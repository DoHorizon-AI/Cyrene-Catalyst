# Catalyst module / Catalyst 模块

## Purpose / 目录用途

This module page describes the runnable Catalyst Product and its dataset-preparation Plugin boundary.

本模块页说明可运行的 Catalyst Product 及其数据整理插件边界。

The active Product implementation is `src/cyrene_catalyst`; reusable processing is not stored in this repository.

活跃 Product 实现位于 `src/cyrene_catalyst`；可复用处理实现不存放在本仓库。

## Files and responsibilities / 文件与职责

| Path | Responsibility / 职责 |
|---|---|
| [`../../API.md`](../../API.md) | Product objects, lifecycle states, capability consumption, and implementation status / 产品对象、生命周期状态、能力消费与实现状态 |
| [`../../REPOSITORY-LIFECYCLE.md`](../../REPOSITORY-LIFECYCLE.md) | Repository ownership, trust boundary, release topology, and branch model / 仓库归属、信任边界、发布拓扑与分支模型 |
| [`../../../README.md`](../../../README.md) | Repository entry point and authoritative-document links / 仓库入口与权威文档链接 |
| [`../../../service.json`](../../../service.json) | Service identity and declared extension points / 服务身份与已声明扩展点 |
| [`../../../repository-policy.yaml`](../../../repository-policy.yaml) | Repository lifecycle and governance metadata / 仓库生命周期与治理元数据 |
| [`../../../PLUGIN_DEPENDENCIES.md`](../../../PLUGIN_DEPENDENCIES.md) | Documented plugin dependency / 已记录的插件依赖 |
| `../../../src/cyrene_catalyst/` | Product state, API, policy, handoffs, and direct Plugin adapter / Product 状态、API、策略、交接与插件直连适配器 |

## Suggested reading / 推荐阅读

1. `docs/architecture/overview.md` — understand the product boundary and lifecycle.
2. `docs/API.md` — inspect the implemented Product contract and state model.
3. `service.json` and `repository-policy.yaml` — verify declared identity and repository governance.
4. `docs/REPOSITORY-LIFECYCLE.md` — understand release and trust boundaries.
5. `src/cyrene_catalyst/engine.py` — inspect the direct Plugin boundary.

1. `docs/architecture/overview.md` —— 理解产品边界与生命周期。
2. `docs/API.md` —— 查看已实现的 Product 契约与状态模型。
3. `service.json` 与 `repository-policy.yaml` —— 核对服务身份与仓库治理声明。
4. `docs/REPOSITORY-LIFECYCLE.md` —— 理解发布与信任边界。
5. `src/cyrene_catalyst/engine.py` —— 查看插件直连边界。
