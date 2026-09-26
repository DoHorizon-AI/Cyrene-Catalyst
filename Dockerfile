# syntax=docker/dockerfile:1.4
# ==============================================================================
# Cyrene Catalyst Production Container Image
# Provides:
#   1. cyrene-catalyst (Dataset preparation, lineage, and publishing API)
# ==============================================================================

FROM python:3.12-slim-bookworm AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/usr/local/bin:$PATH" \
    CATALYST_HOME="/data/catalyst" \
    CATALYST_ARTIFACT_ROOT="/data/artifacts" \
    CATALYST_HOST="0.0.0.0" \
    CATALYST_PORT="8014"

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    ca-certificates \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Pre-install third-party runtime dependencies
RUN pip install --no-cache-dir \
    "fastapi>=0.115.0" \
    "httpx>=0.27.0" \
    "pydantic>=2.0.0" \
    "uvicorn>=0.30.0" \
    "grpcio>=1.60.0" \
    "protobuf>=4.25.0"

# Copy dependency SDKs
COPY Cyrene-Platform/sdk/python/cyrene_artifacts /app/deps/cyrene_artifacts
COPY Cyrene-Plugins-Official/sdk/python/cyrene_plugin_runtime /app/deps/cyrene_plugin_runtime

# Install local monorepo SDKs with --no-deps
RUN pip install --no-cache-dir --no-deps \
    /app/deps/cyrene_artifacts \
    /app/deps/cyrene_plugin_runtime

# Copy Catalyst source
COPY Cyrene-Services/Cyrene-Catalyst/pyproject.toml Cyrene-Services/Cyrene-Catalyst/README.md /app/catalyst/
COPY Cyrene-Services/Cyrene-Catalyst/src /app/catalyst/src

# Install Catalyst package with --no-deps
RUN pip install --no-cache-dir --no-deps /app/catalyst

# Create data directories and non-root user
RUN mkdir -p /data/catalyst /data/artifacts && \
    useradd -u 10001 -m -s /bin/bash cyrene && \
    chown -R cyrene:cyrene /data /app

# Copy entrypoint script
COPY Cyrene-Services/Cyrene-Catalyst/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

USER cyrene

EXPOSE 8014

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
  CMD curl -f http://127.0.0.1:8014/healthz || exit 1

ENTRYPOINT ["docker-entrypoint.sh"]
