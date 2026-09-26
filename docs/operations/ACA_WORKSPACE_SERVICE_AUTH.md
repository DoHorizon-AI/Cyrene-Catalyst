# Catalyst Workspace service authentication on Azure Container Apps

Catalyst private Workspace routes return `503` until `CYRENE_WORKSPACE_SERVICE_AUTH_JSON` contains a valid credential-to-scope map. The deployment workflow currently does not promote this configuration: it preserves the original image-only path while both auth variables are unset, and fails before checkout or Azure login/write if either variable is non-empty.

## Provisioning prerequisites

1. The Platform deployment owner creates or rotates the JSON map in an Azure Key Vault secret through the approved secret-management path. Do not put map contents or raw bearer tokens in source control, GitHub variables, workflow commands, or logs.
2. Each entry contains only a lowercase SHA-256 token digest and its fixed organization and Workspace scope:

   ```json
   [{"tokenSha256":"<64 lowercase hex characters>","organizationId":"org-id","workspaceId":"workspace-id"}]
   ```

   Validate digest uniqueness and the exact scope against the Platform provisioning record. Do not accept a user, role, organization, or Workspace asserted by an HTTP request. Keep Catalyst's map and Key Vault secret separate from Echo's.
3. Assign the Catalyst Container App a system-assigned or user-assigned managed identity and grant that identity `Key Vault Secrets User` on the map's vault, or equivalent secret-read access.
4. Keep the versionless URI for the Key Vault secret and the selected identity in the reviewed deployment change record. The workflow variable names are `WORKSPACE_SERVICE_AUTH_SECRET_URI` and `WORKSPACE_SERVICE_AUTH_IDENTITY`; do not set them while auth rollout remains disabled. Any non-empty value intentionally stops the workflow.

## Deployment behavior

The workflow deploys on pushes to `main`, `release`, and `develop`, and on manual dispatch. With both auth variables unset, it performs the original image deployment and leaves ACA auth configuration unchanged. Setting either auth variable makes the workflow fail before checkout, Azure login, or any Azure write. The workflow does not register Key Vault references or update the auth environment variable.

The auth rollout is closed because the documented [Container Apps Update API](https://learn.microsoft.com/en-us/rest/api/resource-manager/containerapps/container-apps/update) does not describe an `If-Match`/ETag precondition. The existing opt-in sequence used `az containerapp secret set` followed by `az containerapp update --set-env-vars`; neither write had a conditional version guard. The local CLI implementation reads the app before the secret-set full-resource update and before the template patch, so a concurrent revision could be overwritten by either stale write. A revision snapshot or a GitHub-only lock cannot protect against a portal, IaC, or another out-of-band writer.

Promoting auth therefore requires a manually reviewed deployment under one exclusive lock covering GitHub Actions image deployments, Azure portal changes, IaC, and every other writer to this Container App. Pause normal image deployments while holding that lock. Inspect the current app, latest/ready revisions, image, internal ingress, revision mode, and secret reference names. If the reviewed state changes, stop and re-review rather than applying a stale update. Under the same lock, register a uniquely named Key Vault reference and set `CYRENE_WORKSPACE_SERVICE_AUTH_JSON` to that ACA secret reference. Keep secret contents in Key Vault; never retrieve or print them. If a shared lock cannot cover all writers, leave the private routes disabled.

After the manual update, verify that the active revision retains internal ingress, uses the intended image, references the expected ACA secret name, and becomes healthy. A healthy revision proves startup and readiness only; it does not prove that the map assigns the intended organization and Workspace scopes. During rotation, distinct bearer digests may map to the same scope; verify the new map and Platform caller overlap before switching.

## Failure recovery and cleanup

- An auth-configured Actions run is expected to fail before checkout or Azure changes. Keep both workflow variables unset for the image-only path.
- A failed manual update may leave an unused ACA secret reference. Inspect active revision references by name only, and remove an orphan only after confirming no active revision depends on it.
- If a manual update times out, inspect revision health, image, ingress, and the auth secret reference name. Do not call `az containerapp secret list --show-values` or `listSecrets`.
- Validate the map's digest encoding, uniqueness, and fixed scopes before storing it. Never put map contents or tokens in the change record, command output, or workflow logs.

## 中文说明

Catalyst 私有 Workspace 路由在 `CYRENE_WORKSPACE_SERVICE_AUTH_JSON` 配置有效的凭据范围映射前返回 `503`。当前部署工作流不负责启用该配置：两个认证变量均为空时保留原有镜像发布流程；只要任一变量非空，工作流就在检出代码、Azure 登录或任何 Azure 写操作前失败。

Platform 发布负责人应通过批准的密钥管理流程把 JSON 映射写入 Azure Key Vault。每条映射只包含小写 SHA-256 token 摘要、固定组织和 Workspace 范围；发布前需核对摘要唯一性及准确范围。HTTP 请求不能提供用户、角色、组织或 Workspace 身份。Catalyst 和 Echo 必须使用各自独立的映射与 Key Vault secret。给 Catalyst Container App 分配托管身份，并只授予该身份读取映射 secret 的权限。URI 和身份资源 ID 只记入受控变更记录，不要设置当前工作流中的认证变量；设置任一变量会触发 fail-closed。

当前官方 Container Apps Update API 文档没有说明 `If-Match`/ETag 条件更新。本机 CLI 的 `secret set` 会先读取应用，再提交完整资源更新；`update` 也会先读取应用，再提交 template patch，两者均无条件版本保护。旧 opt-in 流程先执行 `az containerapp secret set`，再执行 `az containerapp update --set-env-vars`。因此认证启用只能由人工在统一排他部署锁下进行；该锁必须覆盖 GitHub Actions 镜像发布、Azure 门户、IaC 和所有其他写入方。持锁期间暂停常规镜像发布，核对应用、最新与就绪 revision、镜像、内部入口、revision 模式和密钥引用名称；状态变化时停止并重新审查。使用新的 Key Vault 引用名，并让 `CYRENE_WORKSPACE_SERVICE_AUTH_JSON` 只引用该 ACA secret。若无法让统一锁覆盖所有写入方，私有路由保持禁用。

人工更新后检查内部入口、镜像、secret 引用名称和 revision 健康状态。健康只证明启动和就绪，不证明映射范围正确；切换前仍需核对 map 与 Platform 调用方的轮换顺序。故障排查只读取密钥引用名称，不读取或打印 Key Vault/ACA 密钥值；确认无活动 revision 依赖后才清理孤立引用。
