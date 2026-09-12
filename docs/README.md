# Catalyst Documentation

Catalyst 文档索引 / Documentation index for Catalyst.

## Repository scope / 仓库范围

Catalyst is the runnable Cyrene Dataset/Preparation lifecycle Product. It owns state, policy, lineage, publication, and handoffs while invoking reusable preparation through `dataset.preparation.v1`.

Catalyst 是可运行的 Cyrene Dataset/Preparation 生命周期 Product，负责状态、策略、血缘、发布与交接，并通过 `dataset.preparation.v1` 调用可复用整理能力。

Concrete parsing and transformation implementations are intentionally absent from this repository and live in Cyrene-Plugins-Official.

具体解析与变换实现有意不放在本仓库，而位于 Cyrene-Plugins-Official。

## Reading map / 阅读地图

| Path | Responsibility / 职责 |
|---|---|
| [`architecture/overview.md`](architecture/overview.md) | System boundary, lifecycle, and data flow / 系统边界、生命周期与数据流 |
| [`architecture/tool-system.md`](architecture/tool-system.md) | Capability seams and ownership boundaries / 能力接缝与职责边界 |
| [`architecture/mcp-integration.md`](architecture/mcp-integration.md) | MCP-facing integration posture and future adapter boundary / MCP 集成现状与未来适配边界 |
| [`modules/catalyst/README.md`](modules/catalyst/README.md) | Repository module map and recommended reading order / 仓库模块地图与推荐阅读顺序 |
| [`glossary.md`](glossary.md) | Bilingual domain vocabulary / 双语领域术语 |
| [`faq.md`](faq.md) | Common questions and troubleshooting / 常见问题与排障指南 |
| [`API.md`](API.md) | Implemented Product/API contract / 已实现的 Product/API 契约 |
| [`REPOSITORY-LIFECYCLE.md`](REPOSITORY-LIFECYCLE.md) | Existing lifecycle, governance, and release boundaries / 现有生命周期、治理与发布边界 |
| [`DEPENDENCIES.md`](DEPENDENCIES.md) | Dependency license record and SBOM procedure / 依赖许可证记录与 SBOM 流程 |

## Suggested order / 推荐顺序

1. Read [`architecture/overview.md`](architecture/overview.md) to understand the product boundary and lifecycle.
2. Read [`architecture/tool-system.md`](architecture/tool-system.md) for capability ownership and extension points.
3. Read [`modules/catalyst/README.md`](modules/catalyst/README.md) to locate the Product source and contracts.
4. Use [`API.md`](API.md), [`REPOSITORY-LIFECYCLE.md`](REPOSITORY-LIFECYCLE.md), [`glossary.md`](glossary.md), and [`faq.md`](faq.md) as reference documents.

1. 先阅读 [`architecture/overview.md`](architecture/overview.md)，理解产品边界与生命周期。
2. 再阅读 [`architecture/tool-system.md`](architecture/tool-system.md)，理解能力归属与扩展点。
3. 阅读 [`modules/catalyst/README.md`](modules/catalyst/README.md)，定位 Product 源码与契约。
4. 最后按需查阅 [`API.md`](API.md)、[`REPOSITORY-LIFECYCLE.md`](REPOSITORY-LIFECYCLE.md)、[`glossary.md`](glossary.md) 与 [`faq.md`](faq.md)。
