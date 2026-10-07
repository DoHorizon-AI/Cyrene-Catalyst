#!/usr/bin/env bash
set -euo pipefail

# Catalyst may use the pinned Platform Artifact SDK only inside its adapter.
# This guard rejects Product-wide SDK leakage, environment bridges, closed
# ArtifactRef.kind vocabularies outside that adapter, and concrete data processing.
guard_path='tooling/ci/check-product-boundary.sh'
violations=0

check_matches() {
  local label=$1
  local pattern=$2
  local matches
  matches="$(git grep -n -i -E "$pattern" -- ":!$guard_path" || true)"
  if [[ -n "$matches" ]]; then
    printf 'Boundary violation (%s):\n%s\n' "$label" "$matches" >&2
    violations=1
  fi
}

check_matches_outside() {
  local label=$1
  local pattern=$2
  local allowed_paths=$3
  local matches
  matches="$(git grep -n -i -E "$pattern" -- ":!$guard_path" \
    | grep -E -v "^(${allowed_paths}):" || true)"
  if [[ -n "$matches" ]]; then
    printf 'Boundary violation (%s):\n%s\n' "$label" "$matches" >&2
    violations=1
  fi
}

check_matches_outside 'Platform git/source checkout' 'Cyrene-Platform[.]git' \
  'pyproject[.]toml|uv[.]lock'
check_matches_outside 'Platform artifact SDK/package' 'cyrene-artifacts|cy_artifacts' \
  'pyproject[.]toml|uv[.]lock|src/cyrene_catalyst/artifacts[.]py'
check_matches 'Platform environment bridge' 'CYRENE_PLATFORM'
check_matches_outside 'closed ArtifactKind code vocabulary' \
  'ArtifactKind[[:space:]]*(::|[.])|class[[:space:]]+ArtifactKind|enum[[:space:]]+ArtifactKind' \
  'src/cyrene_catalyst/artifacts[.]py'

if [[ -e src/cyrene_catalyst/preparation.py ]]; then
  printf 'Boundary violation: local dataset preparation implementation was reintroduced.\n' >&2
  violations=1
fi

if git grep -n -E '^(from|import)[[:space:]]+duckdb([.[:space:]]|$)' -- src; then
  printf 'Boundary violation: concrete dataset processing belongs in Plugins.\n' >&2
  violations=1
fi

if ! grep -q 'dataset.preparation.v1' service.json; then
  printf 'Boundary violation: dataset preparation Plugin dependency is undeclared.\n' >&2
  violations=1
fi

if ! python - <<'PY'
import json
import subprocess
import sys
from pathlib import Path

violations = []
tracked_json = subprocess.run(
    ["git", "ls-files", "*.json"],
    check=True,
    capture_output=True,
    text=True,
).stdout.splitlines()


# Other schemas use `kind` for domain discriminators; only inspect ArtifactRef
# and explicitly named ArtifactKind definitions for a closed vocabulary.
def visit(
    value,
    path: str,
    artifact_ref_context: bool = False,
    artifact_kind_context: bool = False,
) -> None:
    if isinstance(value, dict):
        artifact_ref_context = (
            artifact_ref_context
            or value.get("title") == "ArtifactRef"
            or str(value.get("$id", "")).endswith("/artifact_ref.schema.json")
        )
        artifact_kind_context = (
            artifact_kind_context
            or value.get("title") == "ArtifactKind"
            or path.rsplit(".", maxsplit=1)[-1] in {"ArtifactKind", "artifactKind"}
        )
        properties = value.get("properties")
        kind = properties.get("kind") if isinstance(properties, dict) else None
        if artifact_ref_context and isinstance(kind, dict) and "enum" in kind:
            violations.append(path + ".properties.kind.enum")
        if artifact_kind_context and "enum" in value:
            violations.append(path + ".enum")
        for key, child in value.items():
            visit(
                child,
                f"{path}.{key}",
                artifact_ref_context or key in {"ArtifactRef", "artifactRef"},
                artifact_kind_context,
            )
    elif isinstance(value, list):
        for index, child in enumerate(value):
            visit(child, f"{path}[{index}]", artifact_ref_context, artifact_kind_context)


for name in tracked_json:
    try:
        document = json.loads(Path(name).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        continue
    visit(document, name)

if violations:
    print("Closed ArtifactKind schema detected:", file=sys.stderr)
    print("\n".join(violations), file=sys.stderr)
    raise SystemExit(1)
PY
then
  violations=1
fi

if (( violations != 0 )); then
  exit 1
fi

printf 'Catalyst product boundary guard passed.\n'
