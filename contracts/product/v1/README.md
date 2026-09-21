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
