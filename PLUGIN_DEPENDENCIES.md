# Plugin dependencies

Catalyst requires the Plugins-owned `dataset.preparation.v1` capability for
inspect, prepare, and transform operations. Platform may install/activate the
package and return an opaque `connection_ref`; Catalyst then invokes it directly
with the Plugins-owned `cyrene-plugin-runtime` SDK.

The Product owns Dataset/Preparation state, policy, lineage, publication, and
handoffs. The Plugin owns the payload contract and concrete parsing,
normalization, deduplication, grouping, transformation, and split algorithms.
Missing bindings fail closed; DuckDB is never selected as a replacement
preparation provider.

Catalyst's DuckDB dependency is limited to the Product's CSV/Parquet wire-format
bridge and tabular export writers. It does not replace the Plugin's mapping,
normalization, deduplication, grouping, transformation, or split algorithms.

Catalyst 的 DuckDB 依赖仅用于 Product 的 CSV/Parquet 数据格式桥接与表格导出写入，
不会替代 Plugin 所有的映射、规范化、去重、分组、变换或划分算法。
