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

Catalyst invokes the resolved provider directly. There is no in-tree parser or
transformation implementation. DuckDB is used only by tests to read an exported
JSONL file and verify its contents; it is not the runtime provider.

Catalyst 直接调用已解析的提供方，不包含树内解析或变换实现。DuckDB 只由测试读取
导出的 JSONL 并校验内容，不是运行时提供方。

The port cannot allocate a DatasetVersion, mutate Product state, publish an
event, select a provider/package, or become Artifact Plane authority.
