# Catalyst Product contract v1

状态 / Status: `REFERENCE_MVP_READY`; distributed reconciliation and remote
engine adapters remain outside this reference implementation.

Catalyst owns durable `Dataset` and immutable `DatasetVersion` resources. A
data-processing engine may validate and transform bytes, but it cannot publish a
version, assign Product state, or become the source of lineage truth.

## Boundary

- Product source of truth: SQLite in the MVP; replaceable through `CatalystStore`.
- Artifact bytes: the provider-neutral `ArtifactRef` wire contract and
  Catalyst's replaceable local Artifact Plane adapter. Catalyst persists only
  references and lineage edges; filesystem locations never cross the Product
  API. The adapter is owned and packaged here, so Catalyst has no Platform
  source or SDK dependency.
- Execution: Catalyst invokes the Plugins-owned `dataset.preparation.v1`
  capability through `DataPreparationPort`. The reference implementation uses a
  DuckDB-backed CSV/Parquet serialization bridge for the pinned provider
  contract, but has no local mapping or transformation fallback. Platform may
  resolve a package and return an opaque `connection_ref`, but it does not carry
  Product payloads.
  执行：Catalyst 通过 `DataPreparationPort` 调用 Plugins 所有的
  `dataset.preparation.v1` 能力。参考实现仅为锁定的提供方契约使用基于 DuckDB
  的 CSV/Parquet 序列化桥接，不包含本地映射或变换回退。Platform 可以解析包并
  返回不透明的 `connection_ref`，但不承载 Product 载荷。
- Kernel: execution evidence stays private to a future adapter; DatasetVersion
  state is not inferred from Kernel operation state.
- Events: emitted from persisted resource changes and are never replayed as the
  Product database.

The local application boundary is documented in `data-processing-port.md`.

## Notifications

After durable commits, Catalyst may publish
`dev.cyrene.catalyst.dataset.created.v1`,
`dev.cyrene.catalyst.dataset.updated.v1`,
`dev.cyrene.catalyst.dataset-version.created.v1`, and
`dev.cyrene.catalyst.dataset-version.updated.v1`. They use the common Product
CloudEvents-compatible envelope and carry only resource URI, resource version,
and change kind. Consumers re-read Catalyst and tolerate duplicates, reordering,
and a newer resource version. The synchronous MVP does not claim a durable
outbox publisher.

## Compatibility and versioning

The HTTP surface is rooted at `/api/v1` and consumes the Workspace
`product-http-v1` compatibility profile, including its deprecation window and
removal policy. The OpenAPI document is pinned to 3.1.2 and JSON Schema to Draft
2020-12.

Errors use RFC 9457 Problem Details with stable `code`, `retryable`, and optional
`resourceRef` extensions. `Idempotency-Key` is a Cyrene-defined request key: the
same key and canonical request body returns the current state of the same
resource, including a resource whose first attempt failed; reuse with a
different body returns `CATALYST_IDEMPOTENCY_CONFLICT`.

## State

`Dataset`: `ACTIVE -> ARCHIVED`.

`DatasetVersion`: `PROCESSING -> PUBLISHED | FAILED`. A published or failed
version is immutable. A retry creates a new version and lineage record.

The MVP intentionally executes synchronously and returns `201`. A future async
adapter may honor RFC 7240 `Prefer: respond-async` and return `202` with
`Location` naming the Product-owned DatasetVersion. It must not expose or create
a second Kernel Operation API.
---
<!-- Chinese Translation / 中文翻译 -->

# Catalyst Product 契约 v1

状态为 `REFERENCE_MVP_READY`；分布式协调和远程引擎适配器不属于此参考实现。

Catalyst 拥有持久化的 `Dataset` 和不可变的 `DatasetVersion` 资源。数据处理引擎可以校验并转换字节，但不能发布版本、分配 Product 状态，也不能成为数据沿袭的事实来源。

## 边界

- Product 事实来源：MVP 使用 SQLite；可通过 `CatalystStore` 替换。
- Artifact 字节：使用 Provider 无关的 `ArtifactRef` 线协议和 Catalyst 可替换的本地 Artifact Plane 适配器。Catalyst 只持久化引用和沿袭边；文件系统位置不会跨越 Product API。适配器由本仓库拥有并打包，因此 Catalyst 不依赖 Platform 源码或 SDK。
- 执行：Catalyst 通过 `DataPreparationPort` 调用 Plugins 所有的 `dataset.preparation.v1` 能力。参考实现针对固定 Provider 契约使用基于 DuckDB 的 CSV/Parquet 序列化桥接，但没有本地映射或转换回退。Platform 可以解析包并返回不透明的 `connection_ref`，但不承载 Product 负载。
- Kernel：执行证据留给未来适配器私有处理；DatasetVersion 状态不会根据 Kernel 操作状态推断。
- 事件：根据已持久化的资源变更发出，永远不会作为 Product 数据库的替代品。

本地应用边界见 `data-processing-port.md`。

## 通知

持久化提交后，Catalyst 可以发布以下通知：`dev.cyrene.catalyst.dataset.created.v1`、`dev.cyrene.catalyst.dataset.updated.v1`、`dev.cyrene.catalyst.dataset-version.created.v1` 和 `dev.cyrene.catalyst.dataset-version.updated.v1`。它们使用兼容 CloudEvents 的通用 Product 信封，只携带资源 URI、资源版本和变更类型。消费者重新读取 Catalyst，并容忍重复、乱序和较新的资源版本。同步 MVP 不宣称提供持久化 outbox 发布器。

## 兼容性与版本

HTTP 接口根路径为 `/api/v1`，并采用 Workspace 的 `product-http-v1` 兼容配置文件，包括弃用窗口和移除策略。OpenAPI 固定为 3.1.2，JSON Schema 固定为 Draft 2020-12。

错误使用 RFC 9457 Problem Details，并带有稳定的 `code`、`retryable` 和可选 `resourceRef` 扩展。`Idempotency-Key` 是 Cyrene 定义的请求键：相同键和规范请求体会返回同一资源的当前状态，包括首次尝试失败的资源；相同键配合不同请求体会返回 `CATALYST_IDEMPOTENCY_CONFLICT`。

## 状态

`Dataset`：`ACTIVE -> ARCHIVED`。

`DatasetVersion`：`PROCESSING -> PUBLISHED | FAILED`。已发布或失败的版本均不可变。重试会创建新版本和新的沿袭记录。

MVP 刻意采用同步执行并返回 `201`。未来的异步适配器可以遵循 RFC 7240 的 `Prefer: respond-async`，并返回 `202`，其中 `Location` 指向 Product 所有的 DatasetVersion。不得暴露或创建第二套 Kernel Operation API。
