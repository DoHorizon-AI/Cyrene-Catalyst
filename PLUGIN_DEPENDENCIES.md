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

Training curation uses the existing `dataset.preparation.v1.curate_training_records`
and `remap_training_record` methods. Reviewed export uses
`dataset.generation.v1.prepare_training_sft`. Development acceptance pins both
owner Plugins and their model-provider codec package to the immutable source
revision in `uv.lock`; no model is called during deterministic cleaning.

训练数据整理和映射复用 preparation 插件；审核后转换复用 generation 插件。
验收依赖锁定到可追踪的 Plugins 源码修订。确定性清洗不调用模型改写答案。

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
