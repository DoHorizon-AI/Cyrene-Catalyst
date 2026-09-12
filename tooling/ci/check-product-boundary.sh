#!/usr/bin/env bash
set -euo pipefail

# Catalyst owns its local Artifact Plane adapter.  This guard keeps future
# changes from silently reintroducing a Platform source checkout, Python SDK,
# environment bridge, or closed ArtifactKind vocabulary.
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

check_matches 'Platform git/source checkout' 'Cyrene-Platform[.]git'
check_matches 'Platform artifact SDK/package' 'cyrene-artifacts|cy_artifacts'
check_matches 'Platform environment bridge' 'CYRENE_PLATFORM'
check_matches 'closed ArtifactKind code vocabulary' \
  'ArtifactKind[[:space:]]*(::|[.])|class[[:space:]]+ArtifactKind|enum[[:space:]]+ArtifactKind'

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


def visit(value, path: str) -> None:
    if isinstance(value, dict):
        kind = value.get("kind")
        if isinstance(kind, dict) and "enum" in kind:
            violations.append(path + ".kind.enum")
        for key, child in value.items():
            visit(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            visit(child, f"{path}[{index}]")


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
