# Repository Lifecycle: cyrene-catalyst

This document provides self-contained lifecycle, boundary, and authority specifications for **cyrene-catalyst**.

---

## 1. Repository Purpose & Ownership
**cyrene-catalyst** is classified as **`PUBLIC_PRODUCT`** with desired publication
visibility **`public`**. This clean-root source payload becomes the canonical
public history at the parentless `main` root commit. The former complete history
is retained only in the private `Cyrene-Catalyst-history-archive`; it is not a
public source, dependency, or release input. The live GitHub repository is
public.

**cyrene-catalyst** 属于 **`PUBLIC_PRODUCT`**，目标公开可见性为 **`public`**。本
clean-root 源码内容将在无父 `main` 根提交处成为公开规范历史。原完整历史仅保留在
私有的 `Cyrene-Catalyst-history-archive` 中，不属于公开源码、依赖或 release 输入。
当前 GitHub 仓库已公开。

### What This Repository OWNS:
- Dataset and DatasetVersion lifecycle state
- Data preparation, transformation, quality, and Product lineage
- Explicit handoff requests to owning Products such as Yield and Echo

### What This Repository DOES NOT OWN:
- Kernel execution substrate
- Platform Artifact Plane implementation or generic artifact provider code
- Capability payload contracts, generated capability SDKs, or capability TCKs
- Platform capability resolution, ModelVersion state, or training execution

---

## 2. Classification & Release Units
- **Lifecycle Class**: `PUBLIC_PRODUCT`
- **Source Owner**: Dialogue & Synthesis Team
- **Independent Build Unit**: `Yes` (Build tools: `uv` / `hatchling`)
- **Package / Artifact Units**: `cyrene-catalyst` wheel
- **Deployable Unit**: `Yes` (local Catalyst Product API)
- **Product Unit**: `Yes (Dataset lifecycle Product)`
- **User Distribution Unit**: `Yes (component release)`
- **Multi-Repository Dependency**: `Yes` for direct Yield/Echo handoffs; no build-time Platform dependency

---

## 3. Authorities & Delivery Boundaries
- **CI Authority**: `github` (GitHub Actions owns automatic source and contract checks)
- **Release Role**: `COMPONENT_RELEASE`
- **Release Authority**: `github_releases`
- **Deployment Authority**: `dohorizon_azure` (manual/internal only in this repository)
- **Distribution Profiles**: `dialogue_ext`

---

## 4. Public / Private Trust Boundary & Access Matrix
- **Target repository access**: Public source, public checks, and no private token requirement for ordinary local checks.
- **Live hosting state**: The GitHub repository is public.
- **Dependency boundary**: The pinned Plugins revision is public and immutable; its package metadata does not yet grant a license, so binary publication remains blocked.
- **Product boundary**: Catalyst owns its replaceable local Artifact Plane adapter and exchanges only provider-neutral `ArtifactRef` values. Product payloads and engine calls stay within Catalyst or use direct Product/Plugin handoffs.
- **Internal / Delivery Maintainer**: Azure DevOps is reserved for manual deployment or private integration work; it is not the automatic source CI authority.

## 5. Public release readiness / 公开发布准备度

The source tree is public, but it is not yet a complete binary release. Before
binary publication, maintainers must resolve the Plugins license metadata,
verify that the vendored Yield snapshot remains byte-identical
to the reachable owner ref recorded in `contracts/vendor/yield-product-v1/PROVENANCE.md`
(currently Yield `main@6fea8f835ce2561aaed4b0d9996856f6a3ef1ee6`), and run the
GitHub Actions workflows at the exact release SHA. Local tests and a manual Azure
run do not replace those gates. The former third-party game-dialogue corpus is
intentionally excluded from this source tree and is not approved for public
redistribution.

源码树已经公开，但还不是完整的二进制 release。二进制发布前，维护者必须补齐
Plugins 许可证元数据，并核验
`contracts/vendor/yield-product-v1/PROVENANCE.md` 记录的 vendored Yield 快照仍与
owner 可达引用逐字节一致（当前为 Yield `main@6fea8f835ce2561aaed4b0d9996856f6a3ef1ee6`），
同时在精确 release SHA 上运行 GitHub Actions。本地测试或手动 Azure 运行不能替代这些
门禁。历史第三方游戏对话语料已明确排除在本源码树之外，也未获准公开再分发。

---

## 6. Participation in a Complete Cyrene Distribution
This repository does **not** distribute standalone release zip files directly to general end users. Instead, its verified component artifacts are referenced by exact commit and digest in the official **Cyrene Distribution ReleaseLock** (BOM) under the `dialogue_ext` profile(s).

---

## 7. Verification & Governance Links
- **Local Verification**: Run `uv sync --locked --group dev`, then `uv run ruff check src tests`, `uv run mypy`, and `uv run pytest -q`.
- **Canonical Architecture Docs**: See [`Cyrene-Platform/docs/start-here/00-what-is-cyrene.md`](https://github.com/DoHorizon-AI/Cyrene-Platform/blob/main/docs/start-here/00-what-is-cyrene.md)
- **Release Topology**: See [`Cyrene-Platform/docs/release/release-topology.md`](https://github.com/DoHorizon-AI/Cyrene-Platform/blob/main/docs/release/release-topology.md)
- **CI Trust Model**: See [`Cyrene-Platform/docs/governance/ci-trust-model.md`](https://github.com/DoHorizon-AI/Cyrene-Platform/blob/main/docs/governance/ci-trust-model.md)

## 8. Versioning & Tag Strategy
- **Versioning Scheme**: `semver` (SemVer)
- **Version Scope**: `repository`
- **Tag Strategy**: `repository`
- **Canonical Tag Pattern**: `v{version}`
- **Tag Immutability**: Published tags are permanent and immutable. Defective releases require patch increments.

## 9. Branch Model & Promotion
- **Canonical Branch (`main`)**: The clean-root default branch and the base for daily development and pull requests. It must remain green.
- **Working branches**: Feature and fix branches are short-lived and merge back to `main` through review.
- **Release Source**: Official component releases and tags are created strictly from `main`.

## 10. Publication and evidence boundaries / 公开与证据边界

Public source visibility does not grant rights to excluded third-party data or
unresolved dependencies. Binary distribution additionally requires a release
SBOM and license closure. Hosted CI is evidence for a particular commit only;
local tests, manual Azure runs, and source visibility cannot substitute for it.

公开源码可见性不会授予被排除的第三方数据或未解决依赖的权利。二进制分发还需要
release SBOM 与完整许可证闭包。Hosted CI 只证明特定提交；本地测试、手动 Azure
运行与源码可见性不能相互替代。
