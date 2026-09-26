# Plugin dependencies

Catalyst requires the Plugins-owned `dataset.preparation.v1` capability for
inspect, prepare, and transform operations. Platform may install/activate the
package and return an opaque `connection_ref`; Catalyst then invokes it directly
with the Plugins-owned `cyrene-plugin-runtime` SDK.

The Product owns Dataset/Preparation state, policy, lineage, publication, and
handoffs. The Plugin owns the payload contract and concrete parsing,
normalization, deduplication, grouping, transformation, and split algorithms.
Missing bindings fail closed; no Product-local processing implementation is
selected as a replacement preparation provider.

The pinned Plugin reads JSON/JSONL/text/CSV/Parquet sources and writes the
verified JSONL/CSV/Parquet export bundle. Catalyst validates the Plugin result
and file receipts, publishes immutable ArtifactRefs, and never imports DuckDB
in Product runtime code. DuckDB remains a development dependency only for
independent export assertions.

锁定的 Plugin 负责读取 JSON/JSONL/text/CSV/Parquet，并写出经验证的
JSONL/CSV/Parquet 导出包。Catalyst 只验证结果与文件回执、发布不可变 ArtifactRef；
Product 运行时代码不再导入 DuckDB。DuckDB 仅作为开发依赖用于独立验证导出物。
---
<!-- Chinese Translation / 中文翻译 -->

## 能力依赖边界

Catalyst 在检查、准备和转换操作中需要 Plugins 所有的 `dataset.preparation.v1` 能力。Platform 可以安装/激活该包并返回不透明的 `connection_ref`；随后 Catalyst 使用 Plugins 所有的 `cyrene-plugin-runtime` SDK 直接调用它。

Product 拥有 Dataset/Preparation 状态、策略、沿袭、发布和交接。Plugin 拥有负载契约以及具体的解析、规范化、去重、分组、转换和拆分算法。缺少 binding 时按 fail-closed 处理；不会选择 Product 本地处理实现来替代 preparation provider。
