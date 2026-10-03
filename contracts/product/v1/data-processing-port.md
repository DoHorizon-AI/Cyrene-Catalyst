# Catalyst data-preparation port / Catalyst 数据整理端口

`DataPreparationPort` is the internal application boundary for the
Plugins-owned `dataset.preparation.v1` capability. It receives a Product-owned
request and an Artifact Plane-resolved staging reference, then returns typed
measurements and output references for Catalyst to validate and commit. It is
not a Platform business capability or a second Platform registration surface.

`DataPreparationPort` 是 Plugins 所有的 `dataset.preparation.v1` 能力在 Product
内的应用边界。它接收 Product 自有请求与制品平面解析后的暂存引用，返回供
Catalyst 校验和提交的类型化测量与输出引用；它不是 Platform 业务能力，也不是
第二套 Platform 注册表面。

Catalyst invokes the resolved provider directly. The Product contains only a
DuckDB-backed CSV/Parquet serialization bridge for the pinned provider contract;
it does not implement mapping or transformation algorithms. DuckDB is not the
runtime preparation provider.

Catalyst 直接调用已解析的提供方。Product 仅为锁定的提供方契约包含基于 DuckDB
的 CSV/Parquet 序列化桥接，不实现映射或变换算法；DuckDB 不是运行时整理提供方。

The port cannot allocate a DatasetVersion, mutate Product state, publish an
event, select a provider/package, or become Artifact Plane authority.
---
<!-- Chinese Translation / 中文翻译 -->

## 端口权限边界

此端口不能分配 DatasetVersion、修改 Product 状态、发布事件、选择 Provider/包，也不能成为 Artifact Plane 的权威来源。
