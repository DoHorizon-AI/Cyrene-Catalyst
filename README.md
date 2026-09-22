# Catalyst

Data ingestion, cleaning, transformation, structuring, and Dataset creation service for the Cyrene AI software matrix.

---

## Authoritative Documentation & Contracts
- **Product API & Pipeline Specification**: [`docs/API.md`](docs/API.md)
- **Repository Lifecycle & Boundaries**: [`docs/REPOSITORY-LIFECYCLE.md`](docs/REPOSITORY-LIFECYCLE.md)
- **Plugin Dependencies**: [`PLUGIN_DEPENDENCIES.md`](PLUGIN_DEPENDENCIES.md)
- **Dependency and SBOM Record**: [`docs/DEPENDENCIES.md`](docs/DEPENDENCIES.md)
- **Security Policy**: [`SECURITY.md`](SECURITY.md)
- **Contribution Guide**: [`CONTRIBUTING.md`](CONTRIBUTING.md)
- **License**: [`LICENSE`](LICENSE)
- **Service Manifest**: [`service.json`](service.json)

---

## Current Status
Catalyst currently provides a runnable Product API for Dataset and DatasetVersion
creation, deterministic preparation, and explicit handoff to Yield or Echo.
The checked-in sample is synthetic and exists only for local verification. The
historical third-party game-dialogue corpus is excluded from the canonical
source tree and is retained only in the private history archive; this repository
does not grant permission to redistribute it.

The runtime invokes the Plugins-owned `dataset.preparation.v1` capability
through a Product `DataPreparationPort` and persists provider-neutral `ArtifactRef` values. Catalyst
publishes and verifies those references through its replaceable local
`LocalArtifactPlane` adapter backed by the pinned Platform Artifact SDK.
Product payloads go directly to Yield or Echo and never through a Platform
business proxy. Missing or invalid Plugin bindings fail closed. The Plugin owns
JSON/JSONL/text/CSV/Parquet parsing and all JSONL/CSV/Parquet export writing;
Catalyst verifies receipts and publishes immutable references. Catalyst does
not own Platform training/model contracts.

## Clean-root publication posture / Clean-root 公开发布拓扑

This repository is the clean-root source payload for the public Catalyst
release. Its canonical public history starts at the parentless `main` root
commit created from this snapshot. The former complete repository history is
retained only in the private `Cyrene-Catalyst-history-archive`; it is not part of
the public source, a dependency, or a release input.

The GitHub repository and pinned source revisions are publicly reachable, so an
anonymous clone can resolve the locked graph. Binary distribution remains
blocked until the Plugins package license metadata is explicitly declared and
reviewed. Public source visibility, binary distribution, hosted CI, and local
verification are separate gates; none is implied by another.

本仓库是 Catalyst 公开 release 的 clean-root 源码内容；公开规范历史从由此快照
创建的无父 `main` 根提交开始。原完整提交历史仅保留在私有的
`Cyrene-Catalyst-history-archive` 中，不属于公开源码、依赖或 release 输入。

GitHub 仓库与锁定的源码修订版均已公开可达，匿名 clone 可以解析锁定依赖图。
二进制分发仍须等待 Plugins 包明确声明并完成许可证审查。公开源码可见性、二进制
分发、Hosted CI 与本地验证是彼此独立的门禁，不能相互替代。

## Local verification / 本地验证

```bash
uv sync --locked --group dev
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy
uv run pytest -q
```
