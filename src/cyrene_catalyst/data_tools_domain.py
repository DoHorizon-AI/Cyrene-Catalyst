"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 data_tools_domain.py                                            │
│  Module: cyrene_catalyst.data_tools_domain                          │
│  Role: Durable source, content, annotation, and run models.          │
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


class ContentRevision(ContractModel):
    """Immutable block snapshot and its mutable review projection. | 不可变内容快照与审核投影。"""

    id: UUID
    dataset_id: UUID
    revision: int = Field(ge=1)
    parent_revision_id: UUID | None = None
    source_revision_ids: list[UUID] = Field(default_factory=list)
    blocks: list[ContentBlock] = Field(default_factory=list)
    state: ContentRevisionState = ContentRevisionState.DRAFT
    reviewed_at: datetime | None = None
    review_note: str | None = Field(default=None, max_length=2000)
    created_at: datetime = Field(default_factory=utc_now)
    resource_version: int = Field(default=1, ge=1)


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
