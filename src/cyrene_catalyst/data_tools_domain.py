"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 data_tools_domain.py                                            │
│  Module: cyrene_catalyst.data_tools_domain                          │
│  Role: Durable source, parse-review, content, and run models.        │
│                                                                     │
│  模块职责：定义 Catalyst Data Tools 的持久化领域模型。                 │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import Field, model_validator

from cyrene_catalyst.domain import ArtifactRef, ContractModel, utc_now


class ProcessingOperation(StrEnum):
    """Operations accepted by the Data Tools workflow. | Data Tools 工作流操作。"""

    PARSE = "parse"
    CURATE_TRAINING_DATA = "curateTrainingData"
    BUILD_KNOWLEDGE = "buildKnowledge"
    PREPARE_SFT = "prepareSft"
    GENERATE_QA = "generateQa"


class ProcessingRunState(StrEnum):
    """Persisted run lifecycle. | 持久化任务生命周期。"""

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    INTERRUPTED = "INTERRUPTED"


class ProcessingStageState(StrEnum):
    """Per-stage outcome, including ambiguous paid-call completion. | 阶段结果。"""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    REUSED = "REUSED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    INTERRUPTED = "INTERRUPTED"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"


class ContentRevisionState(StrEnum):
    """Human review state for one immutable content snapshot. | 内容审核状态。"""

    DRAFT = "DRAFT"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class AnnotationKind(StrEnum):
    """Supported human feedback categories. | 人工标注类别。"""

    NOTE = "NOTE"
    CORRECTION = "CORRECTION"
    APPROVAL = "APPROVAL"
    REJECTION = "REJECTION"


class BlockOrigin(StrEnum):
    """Provenance category for a content block. | 内容块来源类别。"""

    EXTRACTED = "EXTRACTED"
    NORMALIZED = "NORMALIZED"
    HUMAN_EDITED = "HUMAN_EDITED"
    GENERATED = "GENERATED"


class ContentLocator(ContractModel):
    """Stable source locator carried by a content block. | 内容块来源定位信息。"""

    source_pages: list[Annotated[int, Field(ge=1)]] = Field(default_factory=list)
    section_path: list[str] = Field(default_factory=list)
    item_ref: str = ""
    tree_level: int = Field(default=0, ge=0)
    table_index: int | None = Field(default=None, ge=0)
    provenance: list[dict[str, Any]] = Field(default_factory=list)


class ContentPolicy(ContractModel):
    """Explicit output permissions for a block. | 内容块的显式输出许可。"""

    allow_knowledge: bool = False
    allow_training: bool = False
    allowed_principal_refs: list[str] = Field(default_factory=list)
    allowed_use_purposes: list[Literal["knowledge_retrieval", "model_training"]] = Field(
        default_factory=list
    )


class GenerationReceipt(ContractModel):
    """Per-sample generation evidence kept outside learned text. | 逐样本生成回执。"""

    recipe_id: str = Field(min_length=1, max_length=200)
    recipe_version: str = Field(min_length=1, max_length=100)
    recipe_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    binding_id: str = Field(min_length=1, max_length=500)
    model: str = Field(min_length=1, max_length=500)
    budget: dict[str, Any] = Field(default_factory=dict)
    usage: dict[str, Any] = Field(default_factory=dict)
    generated_at: datetime
    source_block_ids: list[Annotated[str, Field(min_length=1, max_length=300)]] = Field(
        min_length=1
    )

    @model_validator(mode="after")
    def validate_source_blocks(self) -> GenerationReceipt:
        """Require unique source IDs in the receipt. | 校验来源 block ID 唯一。"""

        if len(self.source_block_ids) != len(set(self.source_block_ids)):
            raise ValueError("GenerationReceipt sourceBlockIds must be unique.")
        return self


class ProcessingProgress(ContractModel):
    """Compact public stage count. | 精简的公开阶段计数。"""

    completed: int = Field(default=0, ge=0)
    total: int | None = Field(default=None, ge=0)


class ProcessingWarning(ContractModel):
    """Non-fatal conversion or preparation warning. | 非致命转换或准备警告。"""

    code: str = Field(pattern=r"^[A-Z][A-Z0-9_]+$")
    message: str


class ProcessingFailure(ContractModel):
    """Stable failure receipt persisted with a run. | 随运行持久化的稳定失败回执。"""

    code: str = Field(pattern=r"^[A-Z][A-Z0-9_]+$")
    message: str
    retryable: bool


class SourceParseReportState(StrEnum):
    """One source's durable outcome inside a parse batch. | 单来源解析结果。"""

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    WARNING = "WARNING"
    FAILED = "FAILED"
    INTERRUPTED = "INTERRUPTED"
    CANCELLED = "CANCELLED"


class ReviewItemKind(StrEnum):
    """Parser issue category shown in the review queue. | 审核队列问题类型。"""

    PARSER_WARNING = "PARSER_WARNING"
    OCR_WARNING = "OCR_WARNING"
    PARSE_FAILURE = "PARSE_FAILURE"
    UNSUPPORTED_SOURCE = "UNSUPPORTED_SOURCE"
    TRAINING_STRUCTURE = "TRAINING_STRUCTURE"
    TRAINING_DUPLICATE = "TRAINING_DUPLICATE"
    TRAINING_QUALITY = "TRAINING_QUALITY"
    TRAINING_UNSUPPORTED = "TRAINING_UNSUPPORTED"
    TRAINING_LEAKAGE = "TRAINING_LEAKAGE"


class ReviewItemState(StrEnum):
    """Human disposition for a parser or OCR issue. | 解析问题的人工处置状态。"""

    OPEN = "OPEN"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    REJECTED = "REJECTED"


class ReviewItemResolution(StrEnum):
    """Actions available when resolving one review item. | 审核问题处置动作。"""

    ACKNOWLEDGE = "ACKNOWLEDGE"
    REJECT = "REJECT"


class SourceParseReport(ContractModel):
    """Per-source parse receipt that survives batch and process failures. | 逐来源解析报告。"""

    id: UUID
    dataset_id: UUID
    source_revision_id: UUID
    processing_run_id: UUID
    status: SourceParseReportState
    content_revision_id: UUID | None = None
    block_count: int = Field(default=0, ge=0)
    warnings: list[ProcessingWarning] = Field(default_factory=list)
    diagnostics: list[dict[str, Any]] = Field(default_factory=list)
    unsupported_content: list[dict[str, Any]] = Field(default_factory=list)
    review_item_count: int | None = Field(default=None, ge=0)
    diagnostic_counts: dict[str, int] = Field(default_factory=dict)
    failure: ProcessingFailure | None = None
    output_artifacts: list[ArtifactRef] = Field(default_factory=list)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    resource_version: int = Field(default=1, ge=1)

    @model_validator(mode="after")
    def validate_terminal_receipt(self) -> SourceParseReport:
        """Require successful reports to retain the normalized block snapshot."""

        if self.status == SourceParseReportState.FAILED and self.failure is None:
            raise ValueError("Failed SourceParseReports must include a failure receipt.")
        if self.content_revision_id is not None and self.status not in {
            SourceParseReportState.SUCCEEDED,
            SourceParseReportState.WARNING,
            SourceParseReportState.FAILED,
        }:
            raise ValueError("Only terminal source reports can link a ContentRevision.")
        needs_snapshot_artifact = self.content_revision_id is not None or self.status in {
            SourceParseReportState.SUCCEEDED,
            SourceParseReportState.WARNING,
        }
        if needs_snapshot_artifact and not any(
            artifact.kind == "source-parse-blocks" for artifact in self.output_artifacts
        ):
            raise ValueError(
                "Linked or successful SourceParseReports must retain a "
                "source-parse-blocks artifact."
            )
        return self


class ReviewItem(ContractModel):
    """Persisted parser/OCR issue with an explicit human disposition. | 持久化解析问题。"""

    id: UUID
    dataset_id: UUID
    source_parse_report_id: UUID
    source_revision_id: UUID
    processing_run_id: UUID
    content_revision_id: UUID | None = None
    kind: ReviewItemKind
    code: str = Field(min_length=1, max_length=200)
    message: str
    severity: str = Field(min_length=1, max_length=40)
    locator: ContentLocator | None = None
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    record_id: str | None = Field(default=None, min_length=1, max_length=500)
    state: ReviewItemState = ReviewItemState.OPEN
    note: str | None = Field(default=None, max_length=2000)
    created_at: datetime = Field(default_factory=utc_now)
    resolved_at: datetime | None = None
    resource_version: int = Field(default=1, ge=1)

    @model_validator(mode="after")
    def validate_resolution_timestamp(self) -> ReviewItem:
        """Keep issue state and resolution timestamp consistent. | 校验处置时间。"""

        if (self.state == ReviewItemState.OPEN) != (self.resolved_at is None):
            raise ValueError("ReviewItem resolvedAt must match its state.")
        return self


def recipe_digest_for(recipe_version: str, recipe: dict[str, Any]) -> str:
    """Return a stable recipe fingerprint. | 返回稳定的配方摘要。"""

    payload = json.dumps(
        {"recipeVersion": recipe_version, "recipe": recipe},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return f"sha256:{hashlib.sha256(payload.encode('utf-8')).hexdigest()}"


class SourceRevision(ContractModel):
    """Immutable byte revision for a stable logical source. | 逻辑来源的不可变字节版本。"""

    id: UUID
    dataset_id: UUID
    source_id: UUID
    revision: int = Field(ge=1)
    filename: str = Field(min_length=1, max_length=512)
    media_type: str = Field(default="application/octet-stream", max_length=200)
    byte_length: int = Field(ge=0)
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    artifact: ArtifactRef
    created_at: datetime = Field(default_factory=utc_now)
    resource_version: int = Field(default=1, ge=1)

    @model_validator(mode="after")
    def validate_artifact_receipt(self) -> SourceRevision:
        """Keep the byte receipt equal to its immutable ArtifactRef. | 校验制品回执。"""

        if self.digest != self.artifact.digest or self.byte_length != self.artifact.size_bytes:
            raise ValueError("SourceRevision byte receipt must match its ArtifactRef.")
        return self


class ContentBlock(ContractModel):
    """Reviewed source block with locator and lineage metadata. | 可审阅的来源内容块。"""

    id: str = Field(min_length=1, max_length=300)
    source_revision_id: UUID
    source_family_id: str | None = Field(default=None, min_length=1, max_length=300)
    group_id: str | None = Field(default=None, min_length=1, max_length=300)
    conversation_id: str | None = Field(default=None, min_length=1, max_length=300)
    sample_id: str | None = Field(default=None, min_length=1, max_length=500)
    generation_receipt: GenerationReceipt | None = None
    ordinal: int = Field(ge=0)
    kind: str = Field(default="unknown", min_length=1, max_length=80)
    text: str = ""
    locator: ContentLocator = Field(default_factory=ContentLocator)
    origin: BlockOrigin = BlockOrigin.EXTRACTED
    policy: ContentPolicy = Field(default_factory=ContentPolicy)


class TrainingCurationCounts(ContractModel):
    """Reconciled record dispositions and overlapping diagnostics. | 训练整理计数。"""

    total: int = Field(ge=0)
    recognized: int = Field(ge=0)
    format_errors: int = Field(default=0, ge=0)
    duplicate_candidates: int = Field(default=0, ge=0)
    pending_review: int = Field(default=0, ge=0)
    excluded: int = Field(default=0, ge=0)
    eligible: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def validate_dispositions(self) -> TrainingCurationCounts:
        """Require record dispositions to account for every source row."""

        if self.eligible + self.pending_review + self.excluded != self.total:
            raise ValueError("Training record dispositions must sum to total.")
        if any(
            value > self.total
            for value in (self.recognized, self.format_errors, self.duplicate_candidates)
        ):
            raise ValueError("Diagnostic counters cannot exceed total records.")
        return self


class TrainingDataSnapshot(ContractModel):
    """Immutable artifact-backed normalized records for a ContentRevision."""

    schema_version: Literal["cyrene.training-record.v1"]
    artifact: ArtifactRef
    record_count: int = Field(ge=0)
    counts: TrainingCurationCounts

    @model_validator(mode="after")
    def validate_record_count(self) -> TrainingDataSnapshot:
        """Keep the immutable record count equal to its disposition ledger."""

        if self.record_count != self.counts.total:
            raise ValueError("Training snapshot recordCount must equal counts.total.")
        return self


class ContentRevision(ContractModel):
    """Immutable block snapshot and its mutable review projection. | 不可变内容快照与审核投影。"""

    id: UUID
    dataset_id: UUID
    revision: int = Field(ge=1)
    parent_revision_id: UUID | None = None
    source_revision_ids: list[UUID] = Field(default_factory=list)
    blocks: list[ContentBlock] = Field(default_factory=list)
    training_data_snapshot: TrainingDataSnapshot | None = None
    state: ContentRevisionState = ContentRevisionState.DRAFT
    reviewed_at: datetime | None = None
    review_note: str | None = Field(default=None, max_length=2000)
    created_at: datetime = Field(default_factory=utc_now)
    resource_version: int = Field(default=1, ge=1)


class TrainingRecordsPage(ContractModel):
    """One bounded page of raw and normalized training records."""

    revision_id: UUID
    offset: int = Field(ge=0)
    limit: int = Field(ge=1, le=200)
    total: int = Field(ge=0)
    records: list[dict[str, Any]]


class ReviewItemsPage(ContractModel):
    """One bounded page from the existing Review Queue."""

    items: list[ReviewItem]
    total: int = Field(ge=0)
    offset: int = Field(ge=0)
    limit: int = Field(ge=1, le=200)


class Annotation(ContractModel):
    """Feedback tied to a specific published DatasetVersion. | 绑定到具体 DatasetVersion 的反馈。"""

    id: UUID
    dataset_id: UUID
    dataset_version_id: UUID
    content_revision_id: UUID
    block_id: str = Field(min_length=1, max_length=200)
    kind: AnnotationKind
    text: str = Field(default="", max_length=10000)
    created_by: str | None = Field(default=None, max_length=200)
    created_at: datetime = Field(default_factory=utc_now)


class ProcessingStage(ContractModel):
    """One persisted file/workflow stage and its restart evidence. | 持久化处理阶段。"""

    key: str = Field(min_length=1, max_length=120)
    filename: str | None = Field(default=None, max_length=512)
    source_revision_id: UUID | None = None
    state: ProcessingStageState = ProcessingStageState.PENDING
    deterministic: bool = True
    input_digest: str | None = Field(
        default=None,
        pattern=r"^sha256:[0-9a-f]{64}$",
    )
    recipe_digest: str | None = Field(
        default=None,
        pattern=r"^sha256:[0-9a-f]{64}$",
    )
    external_call: bool = False
    call_started_at: datetime | None = None
    call_completed_at: datetime | None = None
    output: dict[str, Any] | None = None
    failure_code: str | None = Field(default=None, pattern=r"^[A-Z][A-Z0-9_]+$")
    failure_message: str | None = None
    retryable: bool = True
    started_at: datetime | None = None
    finished_at: datetime | None = None

    @model_validator(mode="after")
    def disable_cache_for_external_calls(self) -> ProcessingStage:
        """Never reuse a stage that can trigger a paid/external side effect."""

        if self.external_call:
            self.deterministic = False
        return self


class ProcessingRun(ContractModel):
    """Durable operation, recipe, and stage outcomes. | 持久化操作、配方与阶段结果。"""

    id: UUID
    dataset_id: UUID
    operation: ProcessingOperation
    state: ProcessingRunState = ProcessingRunState.QUEUED
    stale: bool = False
    source_revision_ids: list[UUID] = Field(default_factory=list)
    content_revision_id: UUID | None = None
    recipe_version: str = Field(default="1", min_length=1, max_length=80)
    recipe: dict[str, Any] = Field(default_factory=dict)
    recipe_digest: str | None = Field(
        default=None,
        pattern=r"^sha256:[0-9a-f]{64}$",
    )
    stages: list[ProcessingStage] = Field(default_factory=list)
    cancellation_requested: bool = False
    retry_of_run_id: UUID | None = None
    attempt: int = Field(default=1, ge=1)
    progress: ProcessingProgress = Field(default_factory=lambda: ProcessingProgress())
    output_artifacts: list[ArtifactRef] = Field(default_factory=list)
    warnings: list[ProcessingWarning] = Field(default_factory=list)
    failure: ProcessingFailure | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    resource_version: int = Field(default=1, ge=1)

    @model_validator(mode="after")
    def fill_recipe_digest(self) -> ProcessingRun:
        """Fingerprint the recipe whenever callers omit its digest. | 自动计算配方摘要。"""

        expected = recipe_digest_for(self.recipe_version, self.recipe)
        if self.recipe_digest is None:
            self.recipe_digest = expected
        elif self.recipe_digest != expected:
            raise ValueError("recipeDigest must match the persisted recipe and version.")
        return self
