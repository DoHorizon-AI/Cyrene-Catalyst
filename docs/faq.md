# FAQ and troubleshooting / 常见问题与排障

## Is Catalyst runnable today? / Catalyst 现在可以运行吗？

Yes. Catalyst provides a Product API and requires a resolved `dataset.preparation.v1` endpoint for processing operations. Missing or invalid Plugin configuration fails closed; it never selects an in-tree processor.

可以。Catalyst 提供 Product API；处理操作需要已解析的 `dataset.preparation.v1` 端点。缺失或无效的插件配置会故障关闭，不会选择树内处理器。

## Where is the active source? / 活跃源码在哪里？

The active Product package is `src/cyrene_catalyst`. Concrete dataset preparation is implemented in `Cyrene-Plugins-Official/plugins/tools/dataset-preparation` and reached through `src/cyrene_catalyst/engine.py`.

活跃 Product 包位于 `src/cyrene_catalyst`。具体数据整理实现位于 `Cyrene-Plugins-Official/plugins/tools/dataset-preparation`，由 `src/cyrene_catalyst/engine.py` 直连调用。

## Which file defines extension-point names? / 哪个文件定义扩展点名称？

Use `service.json` as the authoritative manifest for declared extension points. Use `docs/API.md` and the architecture documents to understand the intended behavior and ownership around those names.

以 `service.json` 作为已声明扩展点的权威清单；以 `docs/API.md` 与架构文档理解这些名称对应的预期行为和职责边界。

## Why does processing fail when the Product API starts? / 为什么 Product API 已启动但处理仍失败？

Product state operations can run without a processor, but inspect/prepare/transform require `CYRENE_DATASET_PREPARATION_CONNECTION_REF`. Verify that Platform activated the package and returned the matching endpoint generation.

Product 状态操作可以在没有处理器时运行，但 inspect/prepare/transform 需要 `CYRENE_DATASET_PREPARATION_CONNECTION_REF`。请确认 Platform 已激活该包并返回匹配代次的端点。

## What should not be added to Catalyst? / Catalyst 不应加入什么？

Do not add model-training loops, inference serving, generic process supervision, or direct hardware probing. Those responsibilities belong to Yield, Reactor, Platform Kernel, and Platform Node Agent respectively.

不要在 Catalyst 中加入模型训练循环、推理服务、通用进程监管或直接硬件探测；这些职责分别归属于 Yield、Reactor、Platform Kernel 与 Platform Node Agent。

## How should a future MCP adapter be debugged? / 未来 MCP 适配器如何排障？

Check the layers in order: protocol parsing, request validation, capability resolution, provider execution, dataset state transition, and artifact publication. Keep transport errors separate from provider and domain errors so the failing boundary is visible.

按以下层次排查：协议解析、请求校验、能力解析、提供方执行、数据集状态转换、制品发布。将传输错误与提供方/领域错误分开，才能看清失败边界。
