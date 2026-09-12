# Catalyst → Yield 数据交接接口说明（MVP）

本文档描述 Catalyst 数据整理工具发布产物供 Yield 训练器消费的最小接口。
Catalyst 交付的是 Yield-owned 契约上的一个训练草稿请求；**不自动开始训练**，
也不宣称训练完成。

## 1. 产物权威

- Catalyst 是 `Dataset` / `DatasetVersion` 的产品权威（见 `docs/API.md`）。
- 发布产物由 `LocalArtifactPlane`（CAS）按 `sha256` 内容寻址存储；
  `DatasetVersion.output` 指向导出包 `manifest.json`。
- 不新建第二套数据集或产物权威；Yield 只读消费这些 `ArtifactRef`。

## 2. 导出包结构

一个 `DatasetVersion` 的 `output` 指向 `manifest.json`，其 `files` 字段列出：

| 文件            | media_type          | 行数                       | 说明                          |
|-----------------|---------------------|----------------------------|-------------------------------|
| `train.jsonl`   | `application/jsonl` | 训练样本数                  | 标准训练数据                  |
| `val.jsonl`     | `application/jsonl`  | 验证样本数（可为 0）       | 标准验证数据                  |
| `errors.jsonl`  | `application/jsonl`  | 剔除样本数（带原因，不静默） | 每行含 `rowIndex/reasonCode/field/message/excerpt` |
| `manifest.json` | `application/json`   | —                          | 来源/映射/划分/样本谱系/文件引用 |

`manifest.json` 关键字段：
- `source`：原始导入 `ArtifactRef`（保留原始来源）
- `mapping` / `normalization` / `split`：转换与划分配置（可复现）
- `sampleLineage`：`[{sampleIndex, split, groupKey, sourceRowIndexes}]`（样本关联）
- `duplicates`：去重记录（`duplicateOfSampleIndex`）
- `files`：各导出文件的 `ArtifactRef`（digest/uri/size）
- `schemaFields`：`["instruction","output"]` 或 `["conversations"]`
- `rowCounts`：`{train, val, errors}`

## 3. 训练数据格式（与 Yield-owned 契约相容）

Yield 是训练输入不变量与训练状态的唯一权威（见 Yield 的
`dataset-validation-authority` 说明）；Catalyst 不复制 Yield 内部实现，
仅保证导出的 `train.jsonl` / `val.jsonl` 满足 Yield-owned 契约
`DatasetVersionRef.format = "ALPACA_JSONL"` 下的两种模式之一：

### instruction 模式
每行一个 JSON 对象：
```json
{"instruction":"档案员","output":"早上好，今天的入库记录已就绪。"}
```
- 必需列：`instruction`、`output`（均为非空字符串）
- 可选列：`input`（字符串）
- `schemaFields == ["instruction","output"]`（或含 `input`）

### conversation 模式（ShareGPT 形）
```json
{"conversations":[{"from":"档案员","value":"..."}]}
```
- 必需列：`conversations`（非空列表，元素含 `from`/`value`，`value` 非空）

### 可消费性证明
- 发布时用 DuckDB `read_json(format="newline_delimited")` 真实读取 `train.jsonl`/`val.jsonl` 校验行数（`verify_jsonl_rows`）。
- 测试 `tests/test_preparation.py::test_end_to_end_publish_and_consumable_export` 用 DuckDB
  读取导出，并断言每行的 `instruction`/`output` 为非空字符串。
- `tests/test_yield_contract.py` 以 Yield-owned 契约快照校验实际发出的
  `CreateTrainingDraft` 请求体与目标确认结构；不依赖相邻 Yield 检出。
- 不把本地通过等同于远端通过；未在 Yield 真实训练器上执行训练。

## 4. 划分防泄漏

- 划分按 `groupKey` 整组分配（`groupBy` 字段，如会话 `scene`），同组必同侧。
- 指派为确定性哈希：`sha256("catalyst-split-v1:" + groupKey)` 取前 8 字节 `% 10000`，小于 `trainRatio*10000` 入训练集。
- 去重在划分前完成（按规范内容 sha256 去重，保留首次出现，重复记入 `duplicates` 不静默丢弃）。

## 5. “发送到 Yield”的真实语义

两个入口都只创建训练草稿，**不自动开始训练**：

- `POST /api/v1/preparations/{preparationId}/yield-draft`
- `POST /api/v1/dataset-versions/{versionId}/actions/send-to-yield`

Catalyst 直接调用 Yield Product API 的 `POST /api/v1/training-drafts`，请求体为
Yield-owned `CreateTrainingDraft`（`name` + `datasetVersion` 引用），并携带稳定交接身份：

```
Idempotency-Key: catalyst-version:{versionId}:{resourceVersion}
```

成功时 Yield 返回 `201` 与 `TrainingDraft`（`state ∈ {DRAFT, PREPARED, STARTED}`）。
Catalyst 将其投影为：

```json
{"status":"DRAFT","targetResource":{"uri":"cyrene://yield/training-drafts/…","id":"…","resourceVersion":1},"openIn":"<yield-url>/api/v1/training-drafts/…"}
```

`status` 只反映 Yield 已确认的草稿状态；Catalyst 不会调用
`POST /api/v1/training-drafts/{draft_id}/actions/start`。`targetResource.id` 与
响应 `state` 必须同时被确认，否则按失败处理。

### 失败语义（fail closed）

| 情况 | 结果 |
| --- | --- |
| 未配置 Yield URL | `503 CATALYST_YIELD_NOT_CONNECTED`，附可检查的 `draft` |
| Yield 不可达 / 拒绝 / 未确认目标 | `502 CATALYST_YIELD_HANDOFF_FAILED`（`retryable`），重试使用同一交接身份 |

Catalyst 不伪造发送成功，也不会在内置路径静默替代 Yield。普通 build/test 使用
`contracts/vendor/yield-product-v1/` 的只读契约快照（溯源见其中 `PROVENANCE.md`），
不要求相邻 Yield 检出。

## 6. Yield 侧接手接口（已落地）

Yield 已提供并维护训练草稿入口：

```
POST /api/v1/training-drafts        # 创建草稿（Catalyst 的唯一调用点）
GET  /api/v1/training-drafts/{draft_id}
PATCH /api/v1/training-drafts/{draft_id}
POST /api/v1/training-drafts/{draft_id}/actions/start   # 仅 Yield 运营/其他 owner 触发
```

`format` 固定为 `ALPACA_JSONL`，`datasetVersion.artifact` /
`datasetVersion.validationArtifact` 为 `ArtifactRef`，`provenanceRefs` 携带 Catalyst 侧
来源引用。契约细节以 Yield-owned `contracts/product/v1/openapi.yaml` 为准，本仓库快照路径
`contracts/vendor/yield-product-v1/openapi.yaml`。

## 7. 边界与不在本轮范围

- PDF / Word / PowerPoint / Excel 仅在 `preparation.detect_format` 识别魔数并返回 `CATALYST_IMPORT_FORMAT_UNSUPPORTED`。未来的文档导入 Plugin 可以直接通过 Catalyst 的 Product/Plugin 契约接入，本轮不实现解析。
- 复杂合成数据、自动标注、大型质量平台不在本轮范围。
- 嵌套字段路径映射（如行内已含 `conversations` 列表的 ShareGPT 导入）本轮不支持，映射仅针对顶层字段；对话组装按行级 + `groupBy` 连续段。
