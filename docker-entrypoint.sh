#!/bin/bash
set -euo pipefail

CATALYST_HOME="${CATALYST_HOME:-/data/catalyst}"
ARTIFACT_ROOT="${CATALYST_ARTIFACT_ROOT:-/data/artifacts}"

mkdir -p "${CATALYST_HOME}" "${ARTIFACT_ROOT}"

if [ $# -gt 0 ]; then
    if [[ "$1" == -* ]]; then
        exec cyrene-catalyst serve "$@"
    else
        exec "$@"
    fi
fi

HOST="${CATALYST_HOST:-0.0.0.0}"
PORT="${CATALYST_PORT:-8014}"

echo "[entrypoint] Starting Cyrene Catalyst dataset service on ${HOST}:${PORT}..."
exec cyrene-catalyst serve \
    --home "${CATALYST_HOME}" \
    --artifact-root "${ARTIFACT_ROOT}" \
    --host "${HOST}" \
    --port "${PORT}"
