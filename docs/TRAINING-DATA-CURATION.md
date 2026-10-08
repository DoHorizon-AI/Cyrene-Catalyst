# Training data curation / 训练数据整理

Catalyst v0.3 reuses Sources, ProcessingRuns, immutable ContentRevisions,
the Review Queue, DatasetVersions, and the Artifact Plane. Format recognition,
deterministic normalization, remapping, and export stay in Plugins. Echo and
Platform do not gain new business responsibilities.

Catalyst v0.3 复用来源、持久化任务、不可变修订、审核队列、数据版本和制品机制。
格式识别、清洗、映射与转换由 Plugins 负责；Client 仅增加必要的功能交互。

## Workflow / 操作流程

1. Import JSON or JSONL files using the existing batch import endpoint.
2. Create a `curateTrainingData` run with selected SourceRevision IDs and a
   versioned `curation` recipe. Explicitly grant the applicable source policies.
   Missing permission never implies permission to train.
3. Inspect the resulting per-source parse reports and paged training records.
   Diagnostics retain malformed raw lines and unsupported content. A single
   JSONL error does not discard the other records.
4. Filter the existing Review Queue by issue type or issue code. Compare raw and
   normalized records; remap fields or roles, approve corrections, or exclude
   records. Each edit creates an immutable revision with processing history.
5. Acknowledge the issues and approve the complete ContentRevision. A queue
   acknowledgement alone does not authorize a problematic record for training.
6. Run `prepareSft` with `outputFormat` set to `sft`, `messages`, or
   `promptCompletion`, then use the existing publish and export endpoints.

导入 → 运行整理 Recipe → 检查诊断 → 审核、修正或排除记录 → 批准完整修订 →
准备训练数据 → 发布不可变版本 → 下载数据包。原始记录和旧修订继续保留。

## Input and output / 输入与输出

| Format | Supported behavior | Explicit limits |
| --- | --- | --- |
| Alpaca | `instruction`, optional `input`, `output`, `system`, and paired `history` | Invalid or incomplete history enters review |
| Prompt-completion | String `prompt` and `completion` | Empty, missing, or wrongly typed fields enter review |
| Messages | Ordered string messages with system/user/assistant roles | Tool calls, media, custom roles, and unknown semantic fields are reported |
| ShareGPT | Ordered `conversations` turns with common role aliases | Unsupported turns or payloads remain in the raw record |
| ChatML | Common `im_start`/`im_end` role-marked sequences | Broken markers or unsupported roles enter review |
| Manual mapping | Versioned field and role mappings, including per-record correction | Mapping cannot rewrite protected lineage or bypass usage policies |

`sft` produces the existing `instruction/input/output` rows with explicit
`system/history`, preserving supported multi-turn conversations for the current
trainer. `messages` preserves the complete ordered conversation. The
`promptCompletion` target accepts only a lossless single-turn user/assistant pair.
An incompatible target fails with a conversion report; it never silently flattens
a conversation or drops a mid-conversation system message.

输出 SFT 保留 `system/history`，messages 保留完整消息序列；prompt-completion 仅支持
能够无损表示的单轮对话。工具、多模态、自定义语义及不兼容转换明确报错，保留原件。

Unicode and newline normalization preserve meaningful answer whitespace.
Exact duplicates are detected deterministically. Length and role-order issues
enter review. Semantic similarity, conflicts, and speculative quality judgements
do not cause automatic deletion. No model rewrites original answers.

## Contracts and accountability / 契约与数量对账

- Existing processing operation: `curateTrainingData`.
- ContentRevision extension: artifact-backed `trainingDataSnapshot` with schema
  `cyrene.training-record.v1`, counts, and an immutable ArtifactRef.
- Record page: `GET /api/v1/content-revisions/{id}/training-records`.
- Record correction: `POST /api/v1/content-revisions/{id}/training-records:edit`.
- Existing preparation operation: `prepareSft`, extended with `outputFormat`.

Every snapshot satisfies `eligible + pendingReview + excluded = total`.
Recognized, format-error, and duplicate-candidate counters describe overlapping
diagnostics and must not be summed as mutually exclusive categories. A failed
source whose remaining JSON array cannot be decoded reports that limitation;
the system does not invent a total for unreadable input.

Published bundles contain `manifest.json`, three split JSONL files, and a
`provenance.jsonl` sidecar. Learned rows contain only the target schema's training
fields. Source IDs, family and conversation lineage, policy, recipe digests,
history, and file digests live in the manifest or sidecar. Connected family and
conversation lineage groups stay within one split. Known exact-content leakage
also blocks publication.

每份快照满足“可用 + 待审核 + 排除 = 总数”。识别、格式错误、重复候选属于可重叠的
诊断统计。发布时验证每个划分、数量、来源和策略；训练字段与内部管理字段分离。

## Reproducible acceptance / 可复现验收

After `uv sync --frozen --group dev`, run:

```bash
uv run python scripts/accept_training_curation.py --output-directory /tmp/catalyst-business
python scripts/verify_training_bundle.py /tmp/catalyst-business/business-sft.zip
uv run python scripts/accept_training_curation_medium.py --output-directory /tmp/catalyst-medium
```

The authored mixed business fixture contains 28 records across four JSON/JSONL
files and all five formats. Its intended reviewed result is 13 eligible and 15
excluded records. The acceptance uses actual owner Plugins over the existing
Direct transport and actual Product HTTP APIs, reopens persistence before review
and before publishing, verifies repeated import/publication, and compares every
exported conversation to its approved snapshot. It is fixed business test data,
not customer-provided production evidence.

The existing source upload API retains each import as an independent source;
identical bytes reuse the same content-addressed artifact. Importing the same
record twice and curating both sources yields one eligible row and one excluded
exact duplicate, with both originals retained. Publication retry uses the
existing `Idempotency-Key` header.

The medium workload synthesizes 20,240 records, including 200 exact duplicates,
20 malformed lines, and 20 unsupported-role rows. Independent verification uses
standard-library ZIP/JSON readers and a temporary SQLite index.

Actual trainer loading is an additional optional acceptance environment:
`scripts/verify_llamafactory_loading.py` requires the current Yield-pinned
`llamafactory==0.9.5`. It invokes the production training Plugin's compile mapping
and the real trainer's dataset loader, checks every aligned turn, and produces a
separate report. It loads no model and starts no training job.

固定语料与中等规模数据均通过真实产品接口和真实插件运行。独立消费者不依赖 Catalyst
解析实现；真实训练器加载另行记录，不将“编译配置成功”当作“训练器读取成功”。
