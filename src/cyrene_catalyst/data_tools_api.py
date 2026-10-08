"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 data_tools_api.py                                                │
│  Module: cyrene_catalyst.data_tools_api                              │
│  Role: HTTP routes for reviewable Data Tools resources.              │
│                                                                      │
│  模块职责：提供来源上传、内容审核、异步运行与双 profile 导出 API。       │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, File, Header, Query, Request, UploadFile
from fastapi import Path as ApiPath
from fastapi.responses import Response
from pydantic import Field, model_validator

from cyrene_catalyst.data_tools_domain import (
    ContentBlock,
    ContentPolicy,
    ContentRevision,
    ContentRevisionState,
    ProcessingOperation,
    ProcessingRun,
    ReviewItem,
    ReviewItemResolution,
    SourceParseReport,
    SourceRevision,
)
from cyrene_catalyst.data_tools_domain import (
    TrainingRecordsPage as TrainingRecordsPageModel,
)
from cyrene_catalyst.data_tools_service import (
    MAX_BATCH_REQUEST_BYTES,
    MAX_BATCH_SOURCE_BYTES,
    MAX_BATCH_SOURCES,
    MAX_SOURCE_BYTES,
    DataToolsService,
    _error,
)
from cyrene_catalyst.domain import ContractModel, DatasetVersion
from cyrene_catalyst.errors import CatalystError
from cyrene_catalyst.logging import emit_diagnostic_error
from cyrene_catalyst.trial_auth import trial_principal_from_request

TrainingFieldPath = Annotated[str, Field(min_length=1, max_length=500)]


class TrainingSplitConfig(ContractModel):
    """Source-family split proportions for a reproducible SFT package."""

    train: float = Field(gt=0, lt=1)
    validation: float = Field(ge=0, lt=1)
    test: float = Field(ge=0, lt=1)
    seed: int | None = Field(default=None, ge=0)


class GenerationBudgetConfig(ContractModel):
    """Bounded generation request admitted by the existing QA recipe."""

    max_examples: int = Field(ge=1)
    max_calls: int = Field(ge=1)
    max_input_tokens: int | None = Field(default=None, ge=1)
    max_output_tokens: int | None = Field(default=None, ge=1)


class TrainingCurationRecipeConfig(ContractModel):
    """Versioned deterministic training-data curation recipe."""

    id: str = Field(min_length=1, max_length=200)
    version: str = Field(min_length=1, max_length=100)
    format: Literal["auto", "alpaca", "promptCompletion", "messages", "sharegpt", "chatml"]
    field_mapping: dict[str, TrainingFieldPath] = Field(default_factory=dict, max_length=100)
    role_mapping: dict[str, Literal["system", "user", "assistant"]] = Field(
        default_factory=dict,
        max_length=100,
    )
    max_characters: int = Field(default=100_000, ge=1, le=10_000_000)
    min_characters: int = Field(default=2, ge=0, le=10_000_000)
    unicode_normalization: Literal["NFC", "NFD", "NFKC", "NFKD", "none"] = "NFC"

    @model_validator(mode="after")
    def require_valid_length_range(self) -> TrainingCurationRecipeConfig:
        if self.min_characters > self.max_characters:
            raise ValueError("minCharacters cannot exceed maxCharacters.")
        return self


class ProcessingRunConfig(ContractModel):
    """Strict, additive configuration shared by existing durable operations."""

    sft_mode: Literal["instruction", "conversation"] | None = None
    output_format: Literal["sft", "messages", "promptCompletion"] | None = None
    split: TrainingSplitConfig | None = None
    generation: GenerationBudgetConfig | None = None
    curation: TrainingCurationRecipeConfig | None = None
    source_policies: dict[str, ContentPolicy] = Field(default_factory=dict, max_length=20)


class CreateProcessingRunRequest(ContractModel):
    """Admit a named operation over selected Product-owned inputs."""

    operation: ProcessingOperation
    source_revision_ids: list[UUID] | None = None
    content_revision_id: UUID | None = None
    config: ProcessingRunConfig = Field(default_factory=ProcessingRunConfig)

    @model_validator(mode="after")
    def validate_operation_inputs(self) -> CreateProcessingRunRequest:
        if self.operation.value == "parse" and not self.source_revision_ids:
            raise ValueError("Parse requires at least one sourceRevisionId.")
        if self.operation.value == "curateTrainingData":
            if not self.source_revision_ids:
                raise ValueError("Training curation requires at least one sourceRevisionId.")
            if self.config.curation is None:
                raise ValueError("Training curation requires config.curation.")
        if self.config.source_policies:
            selected_sources = {str(source_id) for source_id in self.source_revision_ids or []}
            if set(self.config.source_policies) - selected_sources:
                raise ValueError("sourcePolicies keys must refer to selected sourceRevisionIds.")
        if (
            self.operation.value in {"buildKnowledge", "prepareSft", "generateQa"}
            and self.content_revision_id is None
        ):
            raise ValueError(f"{self.operation.value} requires contentRevisionId.")
        if self.operation.value == "generateQa" and self.config.generation is None:
            raise ValueError("generateQa requires config.generation.")
        return self


class EditBlockRequest(ContractModel):
    """Create a new immutable content revision from one edit."""

    expected_revision_id: UUID
    text: str | None = Field(default=None, max_length=1_000_000)
    policy: ContentPolicy | None = None

    @model_validator(mode="after")
    def require_edit(self) -> EditBlockRequest:
        if self.text is None and self.policy is None:
            raise ValueError("An edit must include text or policy.")
        return self


class ReviewContentRequest(ContractModel):
    """Record one immutable approval decision."""

    decision: Literal["APPROVE", "REJECT"]
    note: str | None = Field(default=None, max_length=2000)


class PublishDataToolsVersionRequest(ContractModel):
    """Select an approved snapshot and its successful publication runs."""

    content_revision_id: UUID
    knowledge_run_id: UUID | None = None
    sft_run_id: UUID


class ContentBlockPage(ContractModel):
    """Bounded page from one immutable ContentRevision."""

    revision_id: UUID
    offset: int = Field(ge=0)
    limit: int = Field(ge=1, le=200)
    total: int = Field(ge=0)
    blocks: list[ContentBlock]


class BatchUploadError(ContractModel):
    """Sanitized per-file upload failure. | 单文件上传错误回执。"""

    code: str = Field(min_length=1, max_length=120)
    message: str = Field(min_length=1, max_length=2000)
    retryable: bool = False


class BatchSourceUploadItem(ContractModel):
    """Outcome for one multipart part, independent of neighboring files."""

    filename: str = Field(min_length=1, max_length=512)
    source: SourceRevision | None = Field(...)
    error: BatchUploadError | None = Field(...)

    @model_validator(mode="after")
    def require_single_outcome(self) -> BatchSourceUploadItem:
        if (self.source is None) == (self.error is None):
            raise ValueError("A batch source item must contain exactly one of source or error.")
        return self


class BatchSourceUploadResponse(ContractModel):
    """Independent results from one multipart upload request."""

    items: list[BatchSourceUploadItem]


class GeneratedDraftSummary(ContractModel):
    """Small review-queue projection for one generated DRAFT revision."""

    id: UUID
    dataset_id: UUID
    revision: int
    state: Literal["DRAFT"] = "DRAFT"
    block_count: int = Field(ge=0)
    source_revision_ids: list[UUID]
    created_at: datetime
    processing_run_id: UUID | None = None


class ReviewQueueResponse(ContractModel):
    """Parser issues and generated content awaiting human attention."""

    items: list[ReviewItem]
    generated_drafts: list[GeneratedDraftSummary]
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=50, ge=1, le=200)
    total: int = Field(default=0, ge=0)


ReviewQueueKind = Literal[
    "PARSER_WARNING",
    "OCR_WARNING",
    "PARSE_FAILURE",
    "UNSUPPORTED_SOURCE",
    "TRAINING_STRUCTURE",
    "TRAINING_DUPLICATE",
    "TRAINING_QUALITY",
    "TRAINING_UNSUPPORTED",
    "TRAINING_LEAKAGE",
]


class EditTrainingRecordRequest(ContractModel):
    """One explicit human decision for an immutable training record."""

    record_id: str = Field(min_length=1, max_length=300)
    action: Literal["approve", "exclude", "remap"]
    note: str | None = Field(default=None, max_length=2000)
    format: (
        Literal["auto", "alpaca", "promptCompletion", "messages", "sharegpt", "chatml"] | None
    ) = None
    field_mapping: dict[str, TrainingFieldPath] | None = Field(default=None, max_length=100)
    role_mapping: dict[str, Literal["system", "user", "assistant"]] | None = Field(
        default=None,
        max_length=100,
    )
    raw_record: dict[str, Any] | None = None

    @model_validator(mode="after")
    def require_remap_content(self) -> EditTrainingRecordRequest:
        if self.action == "remap" and not any(
            value is not None
            for value in (self.format, self.field_mapping, self.role_mapping, self.raw_record)
        ):
            raise ValueError("A remap edit must include a format, mapping, or corrected rawRecord.")
        return self


class EditTrainingRecordsRequest(ContractModel):
    """Create a child revision from a bounded, optimistic batch of decisions."""

    resource_version: int = Field(ge=1)
    edits: list[EditTrainingRecordRequest] = Field(min_length=1, max_length=200)
    note: str | None = Field(default=None, max_length=2000)

    @model_validator(mode="after")
    def reject_duplicate_record_edits(self) -> EditTrainingRecordsRequest:
        ids = [edit.record_id for edit in self.edits]
        if len(ids) != len(set(ids)):
            raise ValueError("A record may appear only once in one edit batch.")
        return self


class TrainingRecordIssueResponse(ContractModel):
    """One bounded normalization or review diagnostic on a source record."""

    code: str = Field(min_length=1, max_length=200)
    message: str = Field(min_length=1, max_length=2000)
    severity: Literal["warning", "error"]


class TrainingRecordEnvelopeResponse(ContractModel):
    """Source-preserving record shown in the Review Queue and record browser."""

    schema_version: Literal["cyrene.training-record.v1"]
    id: str = Field(min_length=1, max_length=300)
    sample_id: str = Field(min_length=1, max_length=500)
    source_revision_id: UUID
    source_family_id: str = Field(min_length=1, max_length=300)
    conversation_id: str | None = Field(default=None, min_length=1, max_length=300)
    ordinal: int = Field(ge=0)
    locator: dict[str, Any]
    raw_record: Any
    raw_line: str | None
    detected_format: Literal[
        "alpaca", "promptCompletion", "messages", "sharegpt", "chatml", "unknown"
    ]
    normalized: dict[str, Any] | None
    disposition: Literal["eligible", "review", "excluded"]
    issues: list[TrainingRecordIssueResponse]
    content_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    recipe_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    processing_history: list[dict[str, Any]]
    policy: dict[str, Any]


class TrainingRecordsPageResponse(ContractModel):
    """Public bounded page of immutable training record envelopes."""

    revision_id: UUID
    offset: int = Field(ge=0)
    limit: int = Field(ge=1, le=200)
    total: int = Field(ge=0)
    records: list[TrainingRecordEnvelopeResponse]


class ResolveReviewItemRequest(ContractModel):
    """Human disposition for one persisted Parser review item."""

    action: Literal["ACKNOWLEDGE", "REJECT"]
    note: str | None = Field(default=None, max_length=2000)


def build_data_tools_router(service: DataToolsService) -> APIRouter:
    """Bind scoped Data Tools HTTP routes to one Catalyst service instance."""

    router = APIRouter()

    @router.post(
        "/api/v1/datasets/{datasetId}/sources/batch",
        response_model=BatchSourceUploadResponse,
        status_code=200,
    )
    async def upload_sources_batch(
        dataset_id: Annotated[UUID, ApiPath(alias="datasetId")],
        request: Request,
        files: Annotated[
            list[UploadFile],
            File(alias="files[]", min_length=1, max_length=MAX_BATCH_SOURCES),
        ],
    ) -> BatchSourceUploadResponse:
        """Store each source independently while retaining unsupported file bytes."""

        principal = trial_principal_from_request(request)
        service.require_dataset(dataset_id, principal)
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                request_bytes = int(content_length)
            except ValueError:
                raise _error(
                    "CATALYST_SOURCE_BATCH_LENGTH_INVALID",
                    "Batch upload size is invalid",
                    "Provide a valid Content-Length header.",
                    400,
                ) from None
            if request_bytes > MAX_BATCH_REQUEST_BYTES:
                raise _error(
                    "CATALYST_SOURCE_BATCH_TOO_LARGE",
                    "Batch upload is too large",
                    f"A batch request may contain at most {MAX_BATCH_REQUEST_BYTES} bytes.",
                    413,
                )
        if len(files) > MAX_BATCH_SOURCES:
            raise _error(
                "CATALYST_SOURCE_BATCH_TOO_MANY_FILES",
                "Batch contains too many files",
                f"A batch may contain at most {MAX_BATCH_SOURCES} files.",
                413,
            )
        items: list[BatchSourceUploadItem] = []
        batch_bytes = 0
        for upload in files:
            raw_filename = upload.filename or ""
            safe_name = Path(raw_filename.replace("\\", "/")).name
            if not safe_name or safe_name in {".", ".."}:
                items.append(
                    BatchSourceUploadItem(
                        filename=raw_filename[:512] or "upload",
                        source=None,
                        error=BatchUploadError(
                            code="CATALYST_SOURCE_FILENAME_INVALID",
                            message="Filename is invalid.",
                        ),
                    )
                )
                await upload.close()
                continue

            staged = service.artifacts.stage_path(f"batch-upload-{uuid4()}.source")
            total = 0
            oversized = False
            try:
                known_size = upload.size
                if known_size is not None and known_size > MAX_SOURCE_BYTES:
                    items.append(
                        BatchSourceUploadItem(
                            filename=safe_name,
                            source=None,
                            error=BatchUploadError(
                                code="CATALYST_SOURCE_TOO_LARGE",
                                message=(
                                    f"A source file may contain at most {MAX_SOURCE_BYTES} bytes."
                                ),
                            ),
                        )
                    )
                    continue
                if known_size is not None and batch_bytes + known_size > MAX_BATCH_SOURCE_BYTES:
                    items.append(
                        BatchSourceUploadItem(
                            filename=safe_name,
                            source=None,
                            error=BatchUploadError(
                                code="CATALYST_SOURCE_BATCH_TOO_LARGE",
                                message=(
                                    "The accepted files in a batch may contain at most "
                                    f"{MAX_BATCH_SOURCE_BYTES} bytes."
                                ),
                            ),
                        )
                    )
                    continue
                with staged.open("wb") as stream:
                    while chunk := await upload.read(1024 * 1024):
                        total += len(chunk)
                        if total > MAX_SOURCE_BYTES:
                            oversized = True
                            break
                        stream.write(chunk)
                if total > MAX_SOURCE_BYTES:
                    items.append(
                        BatchSourceUploadItem(
                            filename=safe_name,
                            source=None,
                            error=BatchUploadError(
                                code="CATALYST_SOURCE_TOO_LARGE",
                                message=(
                                    f"A source file may contain at most {MAX_SOURCE_BYTES} bytes."
                                ),
                            ),
                        )
                    )
                    continue
                if batch_bytes + total > MAX_BATCH_SOURCE_BYTES:
                    items.append(
                        BatchSourceUploadItem(
                            filename=safe_name,
                            source=None,
                            error=BatchUploadError(
                                code="CATALYST_SOURCE_BATCH_TOO_LARGE",
                                message=(
                                    "The accepted files in a batch may contain at most "
                                    f"{MAX_BATCH_SOURCE_BYTES} bytes."
                                ),
                            ),
                        )
                    )
                    continue
                if oversized:
                    items.append(
                        BatchSourceUploadItem(
                            filename=safe_name,
                            source=None,
                            error=BatchUploadError(
                                code="CATALYST_SOURCE_TOO_LARGE",
                                message=(
                                    f"A source file may contain at most {MAX_SOURCE_BYTES} bytes."
                                ),
                            ),
                        )
                    )
                    continue
                if total == 0:
                    items.append(
                        BatchSourceUploadItem(
                            filename=safe_name,
                            source=None,
                            error=BatchUploadError(
                                code="CATALYST_SOURCE_EMPTY",
                                message="Upload a non-empty source file.",
                            ),
                        )
                    )
                    continue
                source = service.create_source(
                    dataset_id=dataset_id,
                    filename=safe_name,
                    media_type=upload.content_type,
                    staged_path=staged,
                    principal=principal,
                )
                items.append(BatchSourceUploadItem(filename=safe_name, source=source, error=None))
            except CatalystError as exc:
                items.append(
                    BatchSourceUploadItem(
                        filename=safe_name,
                        source=None,
                        error=BatchUploadError(
                            code=exc.code,
                            message=exc.detail[:2000],
                            retryable=exc.retryable,
                        ),
                    )
                )
            except OSError as exc:
                emit_diagnostic_error(
                    "catalyst.batch_source_upload",
                    "CATALYST_SOURCE_UPLOAD_IO_FAILED",
                    "A source part could not be staged or published.",
                    attributes={"error_type": type(exc).__name__},
                )
                batch_bytes += total
                items.append(
                    BatchSourceUploadItem(
                        filename=safe_name,
                        source=None,
                        error=BatchUploadError(
                            code="CATALYST_SOURCE_UPLOAD_IO_FAILED",
                            message="The source could not be staged. Retry this file.",
                            retryable=True,
                        ),
                    )
                )
            finally:
                staged.unlink(missing_ok=True)
                await upload.close()
        return BatchSourceUploadResponse(items=items)

    @router.post(
        "/api/v1/datasets/{datasetId}/sources",
        response_model=SourceRevision,
        response_model_exclude_none=True,
        status_code=201,
    )
    async def upload_source(
        dataset_id: Annotated[UUID, ApiPath(alias="datasetId")],
        request: Request,
        filename: Annotated[str, Query(min_length=1, max_length=300)],
    ) -> SourceRevision:
        principal = trial_principal_from_request(request)
        safe_name = Path(filename.replace("\\", "/")).name
        if not safe_name or safe_name in {".", ".."}:
            raise _error(
                "CATALYST_SOURCE_FILENAME_INVALID",
                "Filename is invalid",
                "Provide a file name.",
                422,
            )
        staged = service.artifacts.stage_path(f"upload-{uuid4()}.source")
        total = 0
        try:
            with staged.open("wb") as stream:
                async for chunk in request.stream():
                    total += len(chunk)
                    if total > MAX_SOURCE_BYTES:
                        raise _error(
                            "CATALYST_SOURCE_TOO_LARGE",
                            "Source is too large",
                            f"A source file may contain at most {MAX_SOURCE_BYTES} bytes.",
                            413,
                        )
                    stream.write(chunk)
            if total == 0:
                raise _error(
                    "CATALYST_SOURCE_EMPTY",
                    "Source is empty",
                    "Upload a non-empty source file.",
                    422,
                )
            return service.create_source(
                dataset_id=dataset_id,
                filename=safe_name,
                media_type=request.headers.get("content-type"),
                staged_path=staged,
                principal=principal,
            )
        finally:
            staged.unlink(missing_ok=True)

    @router.get(
        "/api/v1/datasets/{datasetId}/sources",
        response_model=list[SourceRevision],
        response_model_exclude_none=True,
    )
    def list_sources(
        dataset_id: Annotated[UUID, ApiPath(alias="datasetId")], request: Request
    ) -> list[SourceRevision]:
        return service.list_sources(dataset_id, trial_principal_from_request(request))

    @router.get(
        "/api/v1/datasets/{datasetId}/source-parse-reports",
        response_model=list[SourceParseReport],
        response_model_exclude_none=True,
    )
    def list_source_parse_reports(
        dataset_id: Annotated[UUID, ApiPath(alias="datasetId")],
        request: Request,
        source_revision_id: Annotated[UUID | None, Query(alias="sourceRevisionId")] = None,
        processing_run_id: Annotated[UUID | None, Query(alias="processingRunId")] = None,
    ) -> list[SourceParseReport]:
        return service.list_source_parse_reports(
            dataset_id,
            source_revision_id=source_revision_id,
            processing_run_id=processing_run_id,
            principal=trial_principal_from_request(request),
        )

    @router.get(
        "/api/v1/datasets/{datasetId}/review-queue",
        response_model=ReviewQueueResponse,
        response_model_exclude_none=True,
    )
    def get_review_queue(
        dataset_id: Annotated[UUID, ApiPath(alias="datasetId")],
        request: Request,
        kind: Annotated[ReviewQueueKind | None, Query()] = None,
        code: Annotated[str | None, Query(min_length=1, max_length=200)] = None,
        offset: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
    ) -> ReviewQueueResponse:
        principal = trial_principal_from_request(request)
        page = service.list_review_items_page(
            dataset_id,
            kind=kind,
            code=code,
            offset=offset,
            limit=limit,
            principal=principal,
        )
        generated_drafts = [
            GeneratedDraftSummary(
                id=revision.id,
                dataset_id=revision.dataset_id,
                revision=revision.revision,
                block_count=len(revision.blocks),
                source_revision_ids=revision.source_revision_ids,
                created_at=revision.created_at,
                processing_run_id=processing_run_id,
            )
            for revision, processing_run_id in service.list_generated_drafts(dataset_id, principal)
        ]
        return ReviewQueueResponse(
            items=page.items,
            generated_drafts=generated_drafts,
            offset=page.offset,
            limit=page.limit,
            total=page.total,
        )

    @router.post(
        "/api/v1/review-items/{reviewItemId}/resolve",
        response_model=ReviewItem,
        response_model_exclude_none=True,
    )
    def resolve_review_item(
        review_item_id: Annotated[UUID, ApiPath(alias="reviewItemId")],
        command: ResolveReviewItemRequest,
        request: Request,
    ) -> ReviewItem:
        action = ReviewItemResolution(command.action)
        return service.resolve_review_item(
            review_item_id,
            action,
            note=command.note,
            principal=trial_principal_from_request(request),
        )

    @router.post(
        "/api/v1/datasets/{datasetId}/processing-runs",
        response_model=ProcessingRun,
        response_model_exclude_none=True,
        status_code=202,
    )
    def create_run(
        dataset_id: Annotated[UUID, ApiPath(alias="datasetId")],
        command: CreateProcessingRunRequest,
        request: Request,
        response: Response,
    ) -> ProcessingRun:
        run = service.create_run(
            dataset_id=dataset_id,
            operation=command.operation,
            source_revision_ids=command.source_revision_ids,
            content_revision_id=command.content_revision_id,
            config=command.config.model_dump(by_alias=True, exclude_none=True, exclude_unset=True),
            principal=trial_principal_from_request(request),
        )
        response.headers["Location"] = f"/api/v1/processing-runs/{run.id}"
        return run

    @router.get(
        "/api/v1/datasets/{datasetId}/processing-runs",
        response_model=list[ProcessingRun],
        response_model_exclude_none=True,
    )
    def list_runs(
        dataset_id: Annotated[UUID, ApiPath(alias="datasetId")], request: Request
    ) -> list[ProcessingRun]:
        return service.list_runs(dataset_id, trial_principal_from_request(request))

    @router.get(
        "/api/v1/processing-runs/{runId}",
        response_model=ProcessingRun,
        response_model_exclude_none=True,
    )
    def get_run(run_id: Annotated[UUID, ApiPath(alias="runId")], request: Request) -> ProcessingRun:
        return service.get_run(run_id, trial_principal_from_request(request))

    @router.post(
        "/api/v1/processing-runs/{runId}/cancel",
        response_model=ProcessingRun,
        response_model_exclude_none=True,
    )
    def cancel_run(
        run_id: Annotated[UUID, ApiPath(alias="runId")], request: Request
    ) -> ProcessingRun:
        return service.cancel_run(run_id, trial_principal_from_request(request))

    @router.post(
        "/api/v1/processing-runs/{runId}/retry",
        response_model=ProcessingRun,
        response_model_exclude_none=True,
        status_code=202,
    )
    def retry_run(
        run_id: Annotated[UUID, ApiPath(alias="runId")],
        request: Request,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=200),
    ) -> ProcessingRun:
        return service.retry_run(run_id, idempotency_key, trial_principal_from_request(request))

    @router.get(
        "/api/v1/datasets/{datasetId}/content-revisions",
        response_model=list[ContentRevision],
        response_model_exclude_none=True,
    )
    def list_content_revisions(
        dataset_id: Annotated[UUID, ApiPath(alias="datasetId")], request: Request
    ) -> list[ContentRevision]:
        return service.list_content_revisions(dataset_id, trial_principal_from_request(request))

    @router.get(
        "/api/v1/content-revisions/{revisionId}/blocks",
        response_model=ContentBlockPage,
        response_model_exclude_none=True,
    )
    def get_content_blocks(
        revision_id: Annotated[UUID, ApiPath(alias="revisionId")],
        request: Request,
        offset: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=200)] = 100,
    ) -> ContentBlockPage:
        revision = service.get_content_revision(revision_id, trial_principal_from_request(request))
        return ContentBlockPage(
            revision_id=revision.id,
            offset=offset,
            limit=limit,
            total=len(revision.blocks),
            blocks=revision.blocks[offset : offset + limit],
        )

    @router.get(
        "/api/v1/content-revisions/{revisionId}/training-records",
        operation_id="listTrainingRecords",
        response_model=TrainingRecordsPageResponse,
        response_model_exclude_none=True,
    )
    def get_training_records(
        revision_id: Annotated[UUID, ApiPath(alias="revisionId")],
        request: Request,
        offset: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        disposition: Annotated[Literal["eligible", "review", "excluded"] | None, Query()] = None,
        issue_code: Annotated[
            str | None,
            Query(alias="issueCode", min_length=1, max_length=200),
        ] = None,
    ) -> TrainingRecordsPageModel:
        principal = trial_principal_from_request(request)
        revision = service.get_content_revision(revision_id, principal)
        return service.list_training_records(
            dataset_id=revision.dataset_id,
            revision_id=revision.id,
            offset=offset,
            limit=limit,
            disposition=disposition,
            issue_code=issue_code,
            principal=principal,
        )

    @router.post(
        "/api/v1/content-revisions/{revisionId}/training-records:edit",
        operation_id="editTrainingRecords",
        response_model=ContentRevision,
        response_model_exclude_none=True,
        status_code=201,
    )
    def edit_training_records(
        revision_id: Annotated[UUID, ApiPath(alias="revisionId")],
        command: EditTrainingRecordsRequest,
        request: Request,
    ) -> ContentRevision:
        principal = trial_principal_from_request(request)
        revision = service.get_content_revision(revision_id, principal)
        return service.edit_training_records(
            dataset_id=revision.dataset_id,
            revision_id=revision.id,
            resource_version=command.resource_version,
            edits=[edit.model_dump(by_alias=True, exclude_none=True) for edit in command.edits],
            note=command.note,
            principal=principal,
        )

    @router.post(
        "/api/v1/content-revisions/{revisionId}/blocks/{blockId}/edits",
        response_model=ContentRevision,
        response_model_exclude_none=True,
        status_code=201,
    )
    def edit_content_block(
        revision_id: Annotated[UUID, ApiPath(alias="revisionId")],
        block_id: Annotated[str, ApiPath(alias="blockId", max_length=300)],
        command: EditBlockRequest,
        request: Request,
    ) -> ContentRevision:
        return service.edit_block(
            revision_id=revision_id,
            block_id=block_id,
            expected_revision_id=command.expected_revision_id,
            text=command.text,
            policy=command.policy,
            principal=trial_principal_from_request(request),
        )

    @router.post(
        "/api/v1/content-revisions/{revisionId}/review",
        response_model=ContentRevision,
        response_model_exclude_none=True,
    )
    def review_content(
        revision_id: Annotated[UUID, ApiPath(alias="revisionId")],
        command: ReviewContentRequest,
        request: Request,
    ) -> ContentRevision:
        decision = (
            ContentRevisionState.APPROVED
            if command.decision == "APPROVE"
            else ContentRevisionState.REJECTED
        )
        return service.review_content(
            revision_id=revision_id,
            decision=decision,
            note=command.note,
            principal=trial_principal_from_request(request),
        )

    @router.post(
        "/api/v1/datasets/{datasetId}/data-tools/versions",
        response_model=DatasetVersion,
        response_model_exclude_none=True,
        status_code=201,
    )
    def publish_version(
        dataset_id: Annotated[UUID, ApiPath(alias="datasetId")],
        command: PublishDataToolsVersionRequest,
        request: Request,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=200),
    ) -> DatasetVersion:
        return service.publish_version(
            dataset_id=dataset_id,
            content_revision_id=command.content_revision_id,
            knowledge_run_id=command.knowledge_run_id,
            sft_run_id=command.sft_run_id,
            idempotency_key=idempotency_key,
            principal=trial_principal_from_request(request),
        )

    @router.get(
        "/api/v1/datasets/{datasetId}/data-tools/versions",
        response_model=list[DatasetVersion],
        response_model_exclude_none=True,
    )
    def list_versions(
        dataset_id: Annotated[UUID, ApiPath(alias="datasetId")], request: Request
    ) -> list[DatasetVersion]:
        return service.list_versions(dataset_id, trial_principal_from_request(request))

    @router.get("/api/v1/dataset-versions/{versionId}/data-tools/export")
    def export_version(
        version_id: Annotated[UUID, ApiPath(alias="versionId")],
        request: Request,
        profile: Annotated[Literal["knowledge", "sft"], Query()],
    ) -> Response:
        data, filename = service.export_version(
            version_id, profile, trial_principal_from_request(request)
        )
        return Response(
            content=data,
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    return router
