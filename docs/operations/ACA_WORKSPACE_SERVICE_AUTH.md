# Workspace service authentication on Azure Container Apps

The private Workspace routes return `503` when `CYRENE_WORKSPACE_SERVICE_AUTH_JSON` is not configured. The deployment workflow can opt in to loading the Platform-provided credential-to-scope map from Azure Key Vault. The map is never stored in this repository or GitHub Actions variables.

## Provisioning prerequisites

1. The Platform deployment owner creates or rotates the JSON map in an Azure Key Vault secret through the approved secret-management path. Do not put its contents in source control, a GitHub variable, a workflow command, or a workflow log.
2. Keep Catalyst's map and secret reference independent from Echo's; do not reuse one Product's scope map for the other.
3. Assign the Catalyst Container App a system-assigned or user-assigned managed identity. Grant that identity `Key Vault Secrets User` on the vault, or the equivalent permission to read the selected secret.
4. Grant the GitHub Actions OIDC deployment principal metadata-only access to the Key Vault secret so the workflow can confirm that the selected secret exists and is enabled. `Key Vault Reader` is the RBAC role for this when the vault uses Azure RBAC. Do not grant the workflow principal secret-content access for this check.
5. Set these GitHub repository **variables** for the environment that owns the app:
   - `WORKSPACE_SERVICE_AUTH_SECRET_URI`: versionless Azure Key Vault secret URI.
   - `WORKSPACE_SERVICE_AUTH_IDENTITY`: `system` or the full resource ID of a user-assigned identity already attached to the app.

These variables contain resource references only. They must not contain the JSON map or any service token. The deployment workflow supports a versionless public Azure Key Vault URI so the app can follow secret rotation.

Before storing or rotating a map, the Platform provisioner must validate each `{tokenSha256, organizationId, workspaceId}` entry against the intended credential and exact scope, including digest encoding/uniqueness and organization/workspace association. A healthy Product revision proves startup and readiness only; it cannot prove that the map assigns the intended scopes.

## Deployment behavior

The GitHub Actions workflow deploys on pushes to `main`, `release`, and `develop`, and on manual dispatch. When both Workspace auth variables are unset, the regular image deployment continues and the workflow leaves ACA auth secrets and environment variables unchanged. With no auth environment variable on the app, private Workspace routes return `503`. Unsetting the two variables later does not remove an already-configured ACA secret reference; removing an existing reference needs a separately reviewed ACA update.

If only one variable is set or either value has an unsupported format, the workflow stops before making ACA changes. When both are set, it first confirms the app has internal ingress, checks that the Key Vault secret exists and is enabled using metadata only, confirms that the selected managed identity is attached to the app, and requires single revision mode. It then registers a uniquely named ACA Key Vault reference and sets `CYRENE_WORKSPACE_SERVICE_AUTH_JSON` to that reference in the same revision update that deploys the image. It does not rewrite a secret used by an older revision, and the workflow never retrieves or prints the secret value.

The Container App identity must be able to read the Key Vault secret. ACA validates and resolves that data-plane access when the reference is applied and the revision starts; the workflow then requires the latest revision to be healthy, use the expected image, and retain internal ingress. Single revision mode keeps the existing revision at 100% traffic until the new revision is ready. If the new revision fails to start or become ready, traffic stays on the existing revision and the workflow fails.

## Failure recovery and cleanup

- A failed variable, Key Vault metadata, identity, ingress, or revision-mode preflight makes no ACA changes. Correct the configuration or prerequisites and rerun the deployment.
- If registering the Key Vault reference succeeds but the image update fails, the run may leave an unused ACA secret reference. The old image and its revision reference are unchanged because each run uses a new ACA secret name. Check the Container App's latest and ready revisions before retrying; a retry uses a new name.
- If a revision update was accepted but the workflow later times out, inspect the latest revision, readiness, image, ingress, and its `CYRENE_WORKSPACE_SERVICE_AUTH_JSON` secret reference before retrying. For example, list revisions without requesting secret values and inspect each revision's auth reference:

  ```bash
  az containerapp revision list --name cyrene-catalyst --resource-group Container-APP \
    --query "[].{revision:name,active:properties.active,health:properties.healthState,image:properties.template.containers[0].image,authRef:properties.template.containers[0].env[?name=='CYRENE_WORKSPACE_SERVICE_AUTH_JSON'].secretRef}" \
    --output json
  ```

  A revision is ready only after startup and readiness checks pass, and single revision mode keeps traffic on the prior ready revision until then.
- The Platform owner must validate the JSON map's scope entries before storing it. A well-formed but incorrect map can pass application readiness, and the deployment workflow deliberately cannot inspect secret contents.
- Remove an unused ACA secret reference only after confirming that no active revision uses it and all inactive revisions that referenced it are deactivated. Use `az containerapp secret list` without `--show-values` to inspect names; then use `az containerapp secret remove --name cyrene-catalyst --resource-group Container-APP --secret-names <unused-secret-name>` only for a confirmed orphan. Never print secret contents during cleanup.

Use a versionless Key Vault secret URI for automatic rotation. Azure Container Apps checks for new secret versions and restarts active revisions that reference the secret through an environment variable. See [Manage secrets in Azure Container Apps](https://learn.microsoft.com/en-us/azure/container-apps/manage-secrets).
