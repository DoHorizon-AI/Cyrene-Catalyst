# Dependency and SBOM record / 依赖与 SBOM 记录

This page is the release entry point for Catalyst's dependency and license
inventory. It is intentionally a record of the current manifests and lockfile,
not a legal opinion.

本页是 Catalyst 依赖与许可证清单的发布入口。它记录当前 manifest 与锁文件，
不构成法律意见。

## Authoritative inputs / 权威输入

- Runtime and development declarations: [`pyproject.toml`](../pyproject.toml).
- Resolved Python graph and hashes: [`uv.lock`](../uv.lock).
- Repository license grant: [`LICENSE`](../LICENSE).
- Direct dependency summary: [`THIRD_PARTY_NOTICES.md`](../THIRD_PARTY_NOTICES.md).

The lockfile is the source of truth for the exact resolved version. The pinned
Plugins revision `4d6f80f1117ab8f7ba389eb9b9049085335ffe2b` is publicly reachable,
and an anonymous clean clone can resolve it. The upstream package manifests do
not yet declare a license, so Catalyst cannot claim complete binary-publication
license closure.

锁文件是精确解析版本的事实来源。当前
锁文件是精确解析版本的事实来源。锁定的 Plugins 修订版
`4d6f80f1117ab8f7ba389eb9b9049085335ffe2b` 已公开可达，匿名 clean clone 可以解析；
但上游包 manifest 尚未声明许可证，因此 Catalyst 仍不能宣称二进制公开发布的许可证
闭包完整。

## SBOM procedure / SBOM 流程

At release time, export the locked graph without resolving new versions and feed
the result to the organization's approved SPDX or CycloneDX generator. Attach
the generated JSON to the release and record its digest beside the release
tag; do not hand-edit a generated SBOM. The input commands are:

发布时应在不重新解析版本的前提下导出锁定依赖图，并交给组织批准的 SPDX 或
CycloneDX 生成器。生成的 JSON 应作为 release 附件，并在 release tag 旁记录摘要；
不要手工编辑生成的 SBOM。输入命令为：

```bash
uv export --locked --format requirements-txt --no-dev > /tmp/cyrene-catalyst-runtime.txt
uv export --locked --format requirements-txt > /tmp/cyrene-catalyst-all.txt
```

The generated SBOM must include both files' package URLs, exact versions, source
URLs/revisions, hashes where available, and license expressions. The current
repository does not commit a generated SBOM because the approved generator and
release artifact store are external controls.

生成的 SBOM 必须包含两个导出文件中的 PURL、精确版本、源 URL/修订版、可用摘要
以及许可证表达式。本仓库不提交生成后的 SBOM，因为批准的生成器与 release 制品
存储属于外部控制。

## Publication boundaries / 公开边界

Making the source repository public is separate from distributing a wheel or
other binary. Binary distribution requires a release SBOM, resolved dependency
licenses, and an explicit decision for every non-registry source. Hosted CI is
also a separate evidence gate for the exact release commit; local tests do not
replace hosted CI.

公开源码仓库与分发 wheel 或其他二进制是不同门禁。二进制分发必须具备 release
SBOM、完整的已解析依赖许可证记录，并为每个非 registry 源作出明确结论。Hosted CI
同样是针对精确 release 提交的独立证据门禁；本地测试不能替代 Hosted CI。

## Review gates / 评审门禁

Before binary publication, maintainers must verify that every direct dependency
and every non-registry source has an explicit license record and that the
generated SBOM matches the tagged `uv.lock`. Anonymous Plugins reachability is
now verified; the remaining unresolved entries are listed in
[`THIRD_PARTY_NOTICES.md`](../THIRD_PARTY_NOTICES.md).

公开前，维护者必须确认每个直接依赖和每个非 registry 源都有明确许可证记录，
Plugins 依赖可匿名访问，并确认生成的 SBOM 与 tag 对应的 `uv.lock` 一致。当前
未解决条目列于 [`THIRD_PARTY_NOTICES.md`](../THIRD_PARTY_NOTICES.md)。
---
<!-- Chinese Translation / 中文翻译 -->

## 权威输入中文对照

- 运行时和开发依赖声明：[`pyproject.toml`](../pyproject.toml)。
- 已解析的 Python 依赖图及摘要：[`uv.lock`](../uv.lock)。
- 仓库许可证授权：[`LICENSE`](../LICENSE)。
- 直接依赖摘要：[`THIRD_PARTY_NOTICES.md`](../THIRD_PARTY_NOTICES.md)。
