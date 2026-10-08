"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 data_tools_service.py                                           │
│  Module: cyrene_catalyst.data_tools_service                         │
│  Role: Source, review, run, and dual-profile publication workflow.   │
│                                                                      │
│  模块职责：编排来源、审核、异步处理运行与双 profile 版本发布。           │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import zipfile
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from threading import Event, RLock
from typing import Any, Literal
from uuid import UUID, uuid4, uuid5

from cyrene_catalyst.artifacts import LocalArtifactPlane, sha256_file
from cyrene_catalyst.data_tools_domain import (
    BlockOrigin,
    ContentBlock,
    ContentLocator,
    ContentPolicy,
    ContentRevision,
    ContentRevisionState,
    GenerationReceipt,
    ProcessingFailure,
    ProcessingOperation,
    ProcessingProgress,
    ProcessingRun,
    ProcessingRunState,
    ProcessingStage,
    ProcessingStageState,
    ProcessingWarning,
    ReviewItem,
    ReviewItemKind,
    ReviewItemResolution,
    ReviewItemsPage,
    ReviewItemState,
    SourceParseReport,
    SourceParseReportState,
    SourceRevision,
    TrainingCurationCounts,
    TrainingDataSnapshot,
    TrainingRecordsPage,
    recipe_digest_for,
)
from cyrene_catalyst.data_tools_store import DataToolsStore
from cyrene_catalyst.domain import (
    ArtifactRef,
    DatasetVersion,
    DatasetVersionState,
    DataToolsVersionProjection,
    ImportFormat,
    LineageEdge,
    utc_now,
)
from cyrene_catalyst.engine import DataPreparationPort, SourceInspection
from cyrene_catalyst.errors import CatalystError, DataEngineFailure
from cyrene_catalyst.logging import emit_diagnostic_error
from cyrene_catalyst.processing_runs import ProcessingRunCoordinator, StageExecutionFailure
from cyrene_catalyst.store import CatalystStore
from cyrene_catalyst.workspace_auth import WorkspaceServicePrincipal

DOCUMENT_PARSING_CONNECTION_ENV = "CYRENE_DOCUMENT_PARSING_CONNECTION_REF"
KNOWLEDGE_PREPARATION_CONNECTION_ENV = "CYRENE_KNOWLEDGE_PREPARATION_CONNECTION_REF"
DATASET_GENERATION_CONNECTION_ENV = "CYRENE_DATASET_GENERATION_CONNECTION_REF"
DATASET_PREPARATION_CONNECTION_ENV = "CYRENE_DATASET_PREPARATION_CONNECTION_REF"
MAX_SOURCE_BYTES = 32 * 1024 * 1024
MAX_BATCH_SOURCES = 20
MAX_BATCH_SOURCE_BYTES = 128 * 1024 * 1024
MAX_BATCH_REQUEST_BYTES = 129 * 1024 * 1024
MAX_RESULT_BYTES = 256 * 1024 * 1024
MAX_ZIP_ENTRY_BYTES = 128 * 1024 * 1024
MAX_TRAINING_ROW_BYTES = 16 * 1024 * 1024
_PARSER_CAPABILITY = "document.parsing.v1"
_KNOWLEDGE_CAPABILITY = "dataset.knowledge.v1"
_GENERATION_CAPABILITY = "dataset.generation.v1"
_SOURCE_NAMESPACE = UUID("b2d70d7a-b10c-4caa-a3a0-10cbce05501c")
_BLOCK_NAMESPACE = UUID("43cc3e4d-9882-49fa-93c7-bfb71f96007a")
_REVIEW_NAMESPACE = UUID("9cd0e2d0-60c7-4b37-bccd-cc49a16ac412")
_VERSION_BINDING_ID = "catalyst-data-generation-v1"
_DOCUMENT_PARSER_MEDIA_TYPES = {
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "image/png",
    "image/jpeg",
    "text/csv",
    "text/markdown",
    "text/plain",
}


def _error(
    code: str,
    title: str,
    detail: str,
    status: int,
    *,
    retryable: bool = False,
) -> CatalystError:
    """Create one stable Catalyst Problem Details error. | 创建稳定产品错误。"""

    return CatalystError(
        code=code,
        title=title,
        detail=detail,
        status=status,
        retryable=retryable,
    )


def _canonical_json(value: Any) -> bytes:
    """Serialize JSON deterministically for staged Plugin inputs. | 规范化 JSON。"""

    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    ).encode("utf-8")


def _json_default(value: Any) -> Any:
    """Encode Product scalar types in their stable JSON wire representation."""

    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _digest_bytes(value: bytes) -> str:
    """Return the Artifact SDK-compatible sha256 string. | 返回规范 SHA-256。"""

    return "sha256:" + hashlib.sha256(value).hexdigest()


def _json_object(value: Any, label: str) -> dict[str, Any]:
    """Require an object from an owner-produced JSON document. | 校验 JSON 对象。"""

    if not isinstance(value, dict):
        raise StageExecutionFailure(
            "CATALYST_PLUGIN_OUTPUT_INVALID",
            f"{label} must be a JSON object.",
            retryable=False,
        )
    return value


def _int_field(value: Any, label: str, *, maximum: int = MAX_RESULT_BYTES) -> int:
    """Validate one nonnegative bounded receipt integer. | 校验回执整数。"""

    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise StageExecutionFailure(
            "CATALYST_PLUGIN_RECEIPT_INVALID",
            f"Plugin returned an invalid {label} receipt.",
            retryable=False,
        )
    return value


def _verify_file_receipt(path: Path, digest: Any, size: Any, label: str) -> None:
    """Check a Plugin-written file against its typed digest and size receipt.

    中文:按插件返回的摘要与大小回执验证文件，避免发布截断或被替换的输出。
    """

    expected_size = _int_field(size, f"{label}.size")
    if not isinstance(digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise StageExecutionFailure(
            "CATALYST_PLUGIN_RECEIPT_INVALID",
            f"Plugin returned an invalid {label}.digest receipt.",
            retryable=False,
        )
    if not path.is_file() or path.stat().st_size != expected_size:
        raise StageExecutionFailure(
            "CATALYST_PLUGIN_OUTPUT_INVALID",
            f"Plugin {label} output is missing or truncated.",
            retryable=False,
        )
    if sha256_file(path) != digest:
        raise StageExecutionFailure(
            "CATALYST_PLUGIN_OUTPUT_INVALID",
            f"Plugin {label} output failed digest verification.",
            retryable=False,
        )


def _content_revision_id_for_run(run: ProcessingRun) -> UUID:
    """Require a revision-bound run before dispatching a content operation."""

    if run.content_revision_id is None:
        raise StageExecutionFailure(
            "CATALYST_CONTENT_REVISION_REQUIRED",
            "This operation requires a ContentRevision.",
            retryable=False,
            outcome_unknown=False,
        )
    return run.content_revision_id


def _copy_json(path: Path, value: Any) -> None:
    """Write a deterministic private JSON file for a direct Plugin call."""

    path.write_bytes(_canonical_json(value))


def _simple_warning(code: str, value: Any) -> ProcessingWarning:
    """Project an owner warning into the public run warning shape."""

    message = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    return ProcessingWarning(code=code, message=message[:2000])


def _binding_environment(operation: ProcessingOperation, source: SourceRevision | None) -> str:
    """Select the required configured capability for one processing operation."""

    if operation == ProcessingOperation.PARSE:
        if source is not None and source.media_type in _DOCUMENT_PARSER_MEDIA_TYPES:
            return DOCUMENT_PARSING_CONNECTION_ENV
        return DATASET_PREPARATION_CONNECTION_ENV
    if operation == ProcessingOperation.CURATE_TRAINING_DATA:
        return DATASET_PREPARATION_CONNECTION_ENV
    if operation == ProcessingOperation.BUILD_KNOWLEDGE:
        return KNOWLEDGE_PREPARATION_CONNECTION_ENV
    return DATASET_GENERATION_CONNECTION_ENV


def _detect_source_format(
    path: Path, filename: str, media_type: str | None
) -> tuple[str, ImportFormat]:
    """Classify bytes without rejecting an upload the parser may diagnose later.

    中文:以内容签名为主、文件名和请求媒体类型为辅识别上传格式。
    """

    with path.open("rb") as stream:
        sample = stream.read(65536)
    suffix = Path(filename).suffix.casefold()
    request_type = (media_type or "").split(";", 1)[0].strip().casefold()
    if sample.startswith(b"%PDF-"):
        return "application/pdf", ImportFormat.TEXT
    if sample.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png", ImportFormat.TEXT
    if sample.startswith(b"\xff\xd8\xff"):
        return "image/jpeg", ImportFormat.TEXT
    if sample.lstrip(b"\xef\xbb\xbf \t\r\n").startswith(b"{\\rtf"):
        return "application/rtf", ImportFormat.TEXT
    if zipfile.is_zipfile(path):
        try:
            with zipfile.ZipFile(path) as archive:
                names = set(archive.namelist())
            package_types = {
                ".docx": (
                    "word/document.xml",
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                ),
                ".pptx": (
                    "ppt/presentation.xml",
                    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                ),
                ".xlsx": (
                    "xl/workbook.xml",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                ),
            }
            if "[Content_Types].xml" in names:
                if suffix in package_types:
                    required_entry, detected_type = package_types[suffix]
                    if required_entry in names:
                        return detected_type, ImportFormat.TEXT
                for required_entry, detected_type in package_types.values():
                    if required_entry in names:
                        return detected_type, ImportFormat.TEXT
        except (OSError, zipfile.BadZipFile) as exc:
            emit_diagnostic_error(
                "catalyst.source_format_probe",
                "CATALYST_SOURCE_PACKAGE_PROBE_FAILED",
                "The Office package metadata could not be read; continuing format detection.",
                attributes={
                    "probe": "office_zip_metadata",
                    "error_type": type(exc).__name__,
                },
            )
    if sample.startswith(b"PAR1"):
        return "application/vnd.apache.parquet", ImportFormat.PARQUET
    if suffix in {".parquet", ".pq"} or request_type == "application/vnd.apache.parquet":
        return "application/vnd.apache.parquet", ImportFormat.PARQUET
    if suffix in {".jsonl", ".ndjson"} or request_type in {
        "application/x-ndjson",
        "application/jsonl",
    }:
        return "application/x-ndjson", ImportFormat.JSONL
    if suffix == ".json" or request_type == "application/json":
        return "application/json", ImportFormat.JSON
    if suffix == ".csv" or request_type == "text/csv":
        return "text/csv", ImportFormat.CSV
    if suffix in {".md", ".markdown"} or request_type == "text/markdown":
        return "text/markdown", ImportFormat.TEXT
    if suffix == ".rtf" or request_type == "application/rtf":
        return "application/rtf", ImportFormat.TEXT
    if suffix == ".txt" or request_type == "text/plain":
        return "text/plain", ImportFormat.TEXT

    extension_types = {
        ".pdf": ("application/pdf", ImportFormat.TEXT),
        ".docx": (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ImportFormat.TEXT,
        ),
        ".pptx": (
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            ImportFormat.TEXT,
        ),
        ".xlsx": (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ImportFormat.TEXT,
        ),
        ".png": ("image/png", ImportFormat.TEXT),
        ".jpg": ("image/jpeg", ImportFormat.TEXT),
        ".jpeg": ("image/jpeg", ImportFormat.TEXT),
    }
    if suffix in extension_types:
        return extension_types[suffix]
    try:
        decoded = path.read_text(encoding="utf-8")
    except (UnicodeError, OSError):
        return "application/octet-stream", ImportFormat.TEXT
    nonempty = [line for line in decoded.splitlines() if line.strip()]
    if nonempty:
        try:
            for line in nonempty:
                parsed = json.loads(line)
                if not isinstance(parsed, dict):
                    break
            else:
                return "application/x-ndjson", ImportFormat.JSONL
        except json.JSONDecodeError:
            pass  # diagnostic-allow: JSONL probing falls through to CSV or text classification
    return "application/octet-stream", ImportFormat.TEXT


def _metadata_string(row: dict[str, Any], index: int, *keys: str) -> str | None:
    """Read stable metadata without coercing user-provided JSON values."""

    value = next((row[key] for key in keys if row.get(key) is not None), None)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise StageExecutionFailure(
            "CATALYST_SOURCE_RECORD_UNSUPPORTED",
            f"Structured row {index + 1} metadata {keys[0]} must be a non-empty string.",
            retryable=False,
            outcome_unknown=False,
        )
    return value


class DataToolsService:
    """Own Data Tools workflow state while delegating processing to Plugins.

    中文:管理 Catalyst 独有的数据流程状态，将文件转换交给 Plugins。
    """

    def __init__(
        self,
        *,
        datasets: CatalystStore,
        store: DataToolsStore,
        artifacts: LocalArtifactPlane,
        preparation_engine: DataPreparationPort,
    ) -> None:
        self.datasets = datasets
        self.store = store
        self.artifacts = artifacts
        self.preparation_engine = preparation_engine
        self._publish_lock = RLock()
        self.coordinator = ProcessingRunCoordinator(store, self._run_stage)

    def close(self) -> None:
        """Stop local workers before the persistent Product stores close."""

        self.coordinator.shutdown(wait=True)
        self.store.close()

    def require_dataset(
        self,
        dataset_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> None:
        """Require the canonical Dataset to exist inside the caller's scope."""

        if self.datasets.get_dataset(dataset_id, principal) is None:
            raise _error(
                "CATALYST_DATASET_NOT_FOUND",
                "Dataset not found",
                "No Dataset exists with the requested id in this workspace.",
                404,
            )

    def create_source(
        self,
        *,
        dataset_id: UUID,
        filename: str,
        media_type: str | None,
        staged_path: Path,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> SourceRevision:
        """Publish source bytes and persist a logical source revision."""

        self.require_dataset(dataset_id, principal)
        if staged_path.stat().st_size > MAX_SOURCE_BYTES:
            raise _error(
                "CATALYST_SOURCE_TOO_LARGE",
                "Source is too large",
                f"A source file may contain at most {MAX_SOURCE_BYTES} bytes.",
                413,
            )
        detected_type, _ = _detect_source_format(staged_path, filename, media_type)
        artifact = self.artifacts.publish(staged_path, "dataset")
        now = utc_now()
        source = SourceRevision(
            id=uuid4(),
            dataset_id=dataset_id,
            source_id=uuid4(),
            revision=1,
            filename=filename,
            media_type=detected_type,
            byte_length=artifact.size_bytes,
            digest=artifact.digest,
            artifact=artifact,
            created_at=now,
            resource_version=1,
        )
        try:
            return self.store.create_source(source, principal)
        except LookupError as exc:
            raise _error(
                "CATALYST_DATASET_NOT_FOUND",
                "Dataset not found",
                "No Dataset exists with the requested id in this workspace.",
                404,
            ) from exc

    def list_sources(
        self,
        dataset_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> list[SourceRevision]:
        """List logical source revisions newest-first."""

        self.require_dataset(dataset_id, principal)
        return self.store.list_sources(dataset_id, principal)

    def list_source_parse_reports(
        self,
        dataset_id: UUID,
        *,
        source_revision_id: UUID | None = None,
        processing_run_id: UUID | None = None,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> list[SourceParseReport]:
        """List durable per-source parse outcomes inside the Dataset scope."""

        self.require_dataset(dataset_id, principal)
        return self.store.list_source_parse_reports(
            dataset_id,
            source_revision_id=source_revision_id,
            processing_run_id=processing_run_id,
            principal=principal,
        )

    def list_review_items(
        self,
        dataset_id: UUID,
        *,
        state: ReviewItemState | None = None,
        source_revision_id: UUID | None = None,
        content_revision_id: UUID | None = None,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> list[ReviewItem]:
        """List persisted parser/OCR issues within the Dataset scope."""

        self.require_dataset(dataset_id, principal)
        return self.store.list_review_items(
            dataset_id,
            state=state,
            source_revision_id=source_revision_id,
            content_revision_id=content_revision_id,
            principal=principal,
        )

    def list_review_items_page(
        self,
        dataset_id: UUID,
        *,
        state: ReviewItemState | None = None,
        source_revision_id: UUID | None = None,
        content_revision_id: UUID | None = None,
        kind: str | None = None,
        code: str | None = None,
        offset: int = 0,
        limit: int = 50,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ReviewItemsPage:
        """Return a SQL-paged Review Queue filtered by existing issue fields."""

        self.require_dataset(dataset_id, principal)
        try:
            return self.store.list_review_items_page(
                dataset_id,
                state=state,
                source_revision_id=source_revision_id,
                content_revision_id=content_revision_id,
                kind=kind,
                code=code,
                offset=offset,
                limit=limit,
                principal=principal,
            )
        except ValueError as exc:
            raise _error(
                "CATALYST_REVIEW_QUEUE_PAGE_INVALID",
                "Review Queue page is invalid",
                str(exc),
                422,
            ) from exc

    def list_training_records(
        self,
        dataset_id: UUID,
        revision_id: UUID,
        offset: int = 0,
        limit: int = 50,
        disposition: str | None = None,
        issue_code: str | None = None,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> TrainingRecordsPage:
        """Stream a bounded raw-versus-normalized record page from an immutable snapshot."""

        self.require_dataset(dataset_id, principal)
        if offset < 0 or not 1 <= limit <= 200:
            raise _error(
                "CATALYST_TRAINING_RECORD_PAGE_INVALID",
                "Training record page is invalid",
                "offset must be non-negative and limit must be between 1 and 200.",
                422,
            )
        revision = self.get_content_revision(revision_id, principal)
        if revision.dataset_id != dataset_id or revision.training_data_snapshot is None:
            raise _error(
                "CATALYST_TRAINING_SNAPSHOT_NOT_FOUND",
                "Training snapshot not found",
                "The ContentRevision has no artifact-backed training records.",
                404,
            )
        artifact_path = self.artifacts.resolve(revision.training_data_snapshot.artifact)
        if artifact_path.stat().st_size > MAX_RESULT_BYTES:
            raise _error(
                "CATALYST_TRAINING_SNAPSHOT_TOO_LARGE",
                "Training snapshot exceeds supported size",
                "The saved training record artifact exceeds Catalyst's read limit.",
                413,
            )
        matches: list[dict[str, Any]] = []
        total = 0
        try:
            for envelope in self._iter_training_record_envelopes(artifact_path):
                if disposition is not None and envelope.get("disposition") != disposition:
                    continue
                issues = envelope.get("issues", [])
                if issue_code is not None and not any(
                    isinstance(issue, dict) and issue.get("code") == issue_code for issue in issues
                ):
                    continue
                if offset <= total < offset + limit:
                    matches.append(envelope)
                total += 1
        except (OSError, ValueError) as exc:
            raise _error(
                "CATALYST_TRAINING_SNAPSHOT_INVALID",
                "Training snapshot cannot be read",
                "The immutable training record artifact failed its JSONL checks.",
                500,
            ) from exc
        return TrainingRecordsPage(
            revision_id=revision.id,
            offset=offset,
            limit=limit,
            total=total,
            records=matches,
        )

    def edit_training_records(
        self,
        *,
        dataset_id: UUID,
        revision_id: UUID,
        resource_version: int,
        edits: list[dict[str, Any]],
        note: str | None = None,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ContentRevision:
        """Create an immutable child revision from bounded human record decisions."""

        self.require_dataset(dataset_id, principal)
        current = self.get_content_revision(revision_id, principal)
        if current.dataset_id != dataset_id or current.training_data_snapshot is None:
            raise _error(
                "CATALYST_TRAINING_SNAPSHOT_NOT_FOUND",
                "Training snapshot not found",
                "The ContentRevision has no artifact-backed training records.",
                404,
            )
        if current.state != ContentRevisionState.DRAFT:
            raise _error(
                "CATALYST_CONTENT_REVISION_NOT_EDITABLE",
                "ContentRevision cannot be edited",
                "Create a new curation revision before editing an approved or rejected snapshot.",
                409,
            )
        revisions = self.store.list_content_revisions(dataset_id, principal)
        if not revisions or revisions[0].id != current.id:
            raise _error(
                "CATALYST_CONTENT_REVISION_CONFLICT",
                "Content changed",
                "Reload the latest ContentRevision before editing records.",
                409,
                retryable=True,
            )
        if current.resource_version != resource_version:
            raise _error(
                "CATALYST_CONTENT_REVISION_CONFLICT",
                "Content changed",
                "The expected ContentRevision version is stale.",
                409,
                retryable=True,
            )
        if not edits or len(edits) > 200:
            raise _error(
                "CATALYST_TRAINING_EDIT_BATCH_INVALID",
                "Training edit batch is invalid",
                "Provide between one and 200 record decisions.",
                422,
            )
        edit_by_id: dict[str, dict[str, Any]] = {}
        allowed_keys = {
            "recordId",
            "action",
            "note",
            "format",
            "fieldMapping",
            "roleMapping",
            "rawRecord",
        }
        for edit in edits:
            if not isinstance(edit, dict) or set(edit) - allowed_keys:
                raise _error(
                    "CATALYST_TRAINING_EDIT_INVALID",
                    "Training edit is invalid",
                    "Each edit contains unsupported fields.",
                    422,
                )
            record_id = edit.get("recordId")
            action = edit.get("action")
            if (
                not isinstance(record_id, str)
                or not record_id
                or action
                not in {
                    "approve",
                    "exclude",
                    "remap",
                }
            ):
                raise _error(
                    "CATALYST_TRAINING_EDIT_INVALID",
                    "Training edit is invalid",
                    "Each edit needs a recordId and supported action.",
                    422,
                )
            if record_id in edit_by_id:
                raise _error(
                    "CATALYST_TRAINING_EDIT_INVALID",
                    "Training edit is invalid",
                    "A record may only be edited once in one batch.",
                    422,
                )
            if action == "remap" and not any(
                key in edit for key in ("format", "fieldMapping", "roleMapping", "rawRecord")
            ):
                raise _error(
                    "CATALYST_TRAINING_EDIT_INVALID",
                    "Training edit is invalid",
                    (
                        "A remap edit requires a format, field mapping, role mapping, "
                        "or corrected rawRecord."
                    ),
                    422,
                )
            edit_by_id[record_id] = edit

        source_path = self.artifacts.resolve(current.training_data_snapshot.artifact)
        output_path = self.artifacts.stage_path(f"{uuid4()}.training-records-edited.jsonl")
        recipe, recipe_digest = self._curation_recipe_for_revision(current, principal)
        found: set[str] = set()
        try:
            with output_path.open("wb") as output:
                for envelope in self._iter_training_record_envelopes(source_path):
                    record_id = envelope.get("id")
                    if not isinstance(record_id, str):
                        raise _error(
                            "CATALYST_TRAINING_SNAPSHOT_INVALID",
                            "Training snapshot is invalid",
                            "A record envelope has no stable id.",
                            500,
                        )
                    record_edit = edit_by_id.get(record_id)
                    if record_edit is not None:
                        found.add(record_id)
                        envelope = self._apply_training_record_edit(
                            envelope,
                            record_edit,
                            recipe=recipe,
                            recipe_digest=recipe_digest,
                            note=note,
                        )
                    output.write(_canonical_json(envelope))
                    output.write(b"\n")
            missing = set(edit_by_id) - found
            if missing:
                raise _error(
                    "CATALYST_TRAINING_RECORD_NOT_FOUND",
                    "Training record not found",
                    "At least one recordId is not part of this immutable snapshot.",
                    404,
                )
            if output_path.stat().st_size > MAX_RESULT_BYTES:
                raise _error(
                    "CATALYST_TRAINING_SNAPSHOT_TOO_LARGE",
                    "Training snapshot exceeds supported size",
                    "The edited training record artifact exceeds Catalyst's write limit.",
                    413,
                )
            snapshot_ref = self.artifacts.publish(output_path, "source-parse-blocks")
            counts, per_source = self._count_training_records(output_path)
            child = ContentRevision(
                id=uuid4(),
                dataset_id=dataset_id,
                revision=current.revision + 1,
                parent_revision_id=current.id,
                source_revision_ids=list(current.source_revision_ids),
                training_data_snapshot=TrainingDataSnapshot(
                    schema_version="cyrene.training-record.v1",
                    artifact=snapshot_ref,
                    record_count=counts.total,
                    counts=counts,
                ),
                state=ContentRevisionState.DRAFT,
                created_at=utc_now(),
                resource_version=1,
            )
            now = utc_now()
            edit_digests = [
                {
                    "recordId": record_id,
                    "action": value["action"],
                    "digest": _digest_bytes(_canonical_json(value)),
                }
                for record_id, value in sorted(edit_by_id.items())
            ]
            run_recipe = {
                "operation": ProcessingOperation.CURATE_TRAINING_DATA.value,
                "config": {
                    "humanEdits": edit_digests,
                    "parentContentRevisionId": str(current.id),
                    "curation": recipe,
                    "curationRecipeDigest": recipe_digest,
                },
                "sourceRevisionIds": [str(value) for value in current.source_revision_ids],
                "contentRevisionId": str(current.id),
            }
            processing_run = ProcessingRun(
                id=uuid4(),
                dataset_id=dataset_id,
                operation=ProcessingOperation.CURATE_TRAINING_DATA,
                state=ProcessingRunState.RUNNING,
                source_revision_ids=list(current.source_revision_ids),
                content_revision_id=current.id,
                recipe_version="data-tools-v1",
                recipe=run_recipe,
                recipe_digest=recipe_digest_for("data-tools-v1", run_recipe),
                stages=[
                    ProcessingStage(
                        key="humanRecordReview",
                        filename="training-records.jsonl",
                        state=ProcessingStageState.RUNNING,
                        deterministic=False,
                        input_digest=current.training_data_snapshot.artifact.digest,
                        recipe_digest=recipe_digest_for("data-tools-v1", run_recipe),
                        started_at=now,
                    )
                ],
                progress=ProcessingProgress(completed=0, total=1),
                created_at=now,
                updated_at=now,
                started_at=now,
                resource_version=1,
            )
            self.store.create_run(processing_run, principal)

            reports: list[SourceParseReport] = []
            for source_id, source_summary in per_source.items():
                report_item_count = source_summary["reviewItems"]
                report = SourceParseReport(
                    id=uuid5(_REVIEW_NAMESPACE, f"human-review:{processing_run.id}:{source_id}"),
                    dataset_id=dataset_id,
                    source_revision_id=source_id,
                    processing_run_id=processing_run.id,
                    content_revision_id=child.id,
                    status=(
                        SourceParseReportState.WARNING
                        if report_item_count
                        else SourceParseReportState.SUCCEEDED
                    ),
                    block_count=source_summary["total"],
                    review_item_count=report_item_count,
                    diagnostic_counts=source_summary["diagnosticCounts"],
                    output_artifacts=[snapshot_ref],
                    started_at=now,
                    finished_at=utc_now(),
                    created_at=now,
                    updated_at=utc_now(),
                )
                reports.append(report)

            def create_review_items() -> Any:
                report_by_source = {report.source_revision_id: report for report in reports}
                for envelope in self._iter_training_record_envelopes(output_path):
                    if envelope.get("disposition") != "review":
                        continue
                    source_id = UUID(str(envelope["sourceRevisionId"]))
                    report = report_by_source[source_id]
                    record_id = str(envelope["id"])
                    issues = envelope.get("issues", [])
                    if not issues:
                        issues = [
                            {
                                "code": "HUMAN_REVIEW_REQUIRED",
                                "message": (
                                    "This remapped record needs explicit approval or exclusion."
                                ),
                                "severity": "warning",
                            }
                        ]
                    for index, issue in enumerate(issues):
                        yield ReviewItem(
                            id=uuid5(
                                _REVIEW_NAMESPACE,
                                f"{processing_run.id}:{record_id}:{issue['code']}:{index}",
                            ),
                            dataset_id=dataset_id,
                            source_parse_report_id=report.id,
                            source_revision_id=source_id,
                            processing_run_id=processing_run.id,
                            content_revision_id=child.id,
                            kind=self._training_review_kind(str(issue["code"])),
                            code=str(issue["code"]),
                            message=str(issue["message"])[:2000],
                            severity=str(issue.get("severity", "warning")),
                            locator=self._training_record_locator(envelope.get("locator")),
                            record_id=record_id,
                        )

            try:
                self.store.finalize_training_content_revision_for_worker(
                    child,
                    reports,
                    create_review_items(),
                    principal=principal,
                    parent_revision_id=current.id,
                    resolved_records={
                        record_id: (
                            ReviewItemResolution.REJECT
                            if edit["action"] == "exclude"
                            else ReviewItemResolution.ACKNOWLEDGE
                        )
                        for record_id, edit in edit_by_id.items()
                    },
                    resolution_note=note,
                )
                completed_at = utc_now()
                completed_run = processing_run.model_copy(
                    update={
                        "state": ProcessingRunState.SUCCEEDED,
                        "stages": [
                            processing_run.stages[0].model_copy(
                                update={
                                    "state": ProcessingStageState.COMPLETED,
                                    "output": {
                                        "contentRevisionId": str(child.id),
                                        "artifact": snapshot_ref.model_dump(
                                            by_alias=True,
                                            exclude_none=True,
                                        ),
                                        "recordCount": counts.total,
                                    },
                                    "finished_at": completed_at,
                                }
                            )
                        ],
                        "progress": ProcessingProgress(completed=1, total=1),
                        "output_artifacts": [snapshot_ref],
                        "updated_at": completed_at,
                        "finished_at": completed_at,
                        "resource_version": 2,
                    }
                )
                self.store.save_run(completed_run, principal)
            except (LookupError, ValueError) as exc:
                failure_at = utc_now()
                failed_run = processing_run.model_copy(
                    update={
                        "state": ProcessingRunState.FAILED,
                        "stages": [
                            processing_run.stages[0].model_copy(
                                update={
                                    "state": ProcessingStageState.FAILED,
                                    "failure_code": "CATALYST_TRAINING_EDIT_FAILED",
                                    "failure_message": (
                                        "The edited snapshot could not be finalized."
                                    ),
                                    "retryable": False,
                                    "finished_at": failure_at,
                                }
                            )
                        ],
                        "failure": ProcessingFailure(
                            code="CATALYST_TRAINING_EDIT_FAILED",
                            message="The edited snapshot could not be finalized.",
                            retryable=False,
                        ),
                        "updated_at": failure_at,
                        "finished_at": failure_at,
                        "resource_version": 2,
                    }
                )
                self.store.save_run(failed_run, principal)
                raise _error(
                    "CATALYST_TRAINING_EDIT_FAILED",
                    "Training records could not be updated",
                    "The record decisions could not be saved as a new immutable revision.",
                    409,
                ) from exc
            return child
        except CatalystError:
            raise
        except StageExecutionFailure as exc:
            raise _error(exc.code, "Training record edit failed", str(exc), 422) from exc
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
            raise _error(
                "CATALYST_TRAINING_EDIT_FAILED",
                "Training record edit failed",
                "The immutable training snapshot could not be rewritten safely.",
                422,
            ) from exc
        finally:
            output_path.unlink(missing_ok=True)

    def _curation_recipe_for_revision(
        self,
        revision: ContentRevision,
        principal: WorkspaceServicePrincipal | None,
    ) -> tuple[dict[str, Any], str]:
        """Find the persisted curation recipe that created this snapshot."""

        reports = self.store.list_source_parse_reports(revision.dataset_id, principal=principal)
        for report in reports:
            if report.content_revision_id != revision.id:
                continue
            run = self.store.get_run(report.processing_run_id, principal)
            if run is None or run.operation != ProcessingOperation.CURATE_TRAINING_DATA:
                continue
            config = run.recipe.get("config", {})
            recipe = config.get("curation") if isinstance(config, dict) else None
            if isinstance(recipe, dict):
                curation_digest = config.get("curationRecipeDigest")
                if not isinstance(curation_digest, str):
                    curation_digest = run.recipe_digest or recipe_digest_for(
                        run.recipe_version, run.recipe
                    )
                return recipe, curation_digest
        return (
            {
                "id": "training-curation-v1",
                "version": "1",
                "format": "auto",
                "fieldMapping": {},
                "roleMapping": {},
                "maxCharacters": 100_000,
                "minCharacters": 2,
                "unicodeNormalization": "NFC",
            },
            revision.training_data_snapshot.artifact.digest
            if revision.training_data_snapshot is not None
            else "sha256:" + "0" * 64,
        )

    def _apply_training_record_edit(
        self,
        envelope: dict[str, Any],
        edit: dict[str, Any],
        *,
        recipe: dict[str, Any],
        recipe_digest: str,
        note: str | None,
    ) -> dict[str, Any]:
        """Apply one approve, exclude, or Plugin-owned remap decision."""

        action = str(edit["action"])
        edit_note = edit.get("note") or note
        if action == "approve":
            if envelope.get("disposition") == "excluded":
                raise _error(
                    "CATALYST_TRAINING_RECORD_APPROVAL_INVALID",
                    "Training record cannot be approved",
                    "An excluded record must be remapped before it can enter training.",
                    409,
                )
            if not isinstance(envelope.get("normalized"), dict):
                raise _error(
                    "CATALYST_TRAINING_RECORD_APPROVAL_INVALID",
                    "Training record cannot be approved",
                    "A record without a normalized representation must be remapped or excluded.",
                    409,
                )
            if not self._training_policy_allows(
                envelope.get("policy")
            ) or self._training_record_explicitly_denied(envelope.get("rawRecord")):
                raise _error(
                    "CATALYST_TRAINING_POLICY_VIOLATION",
                    "Training record is not permitted",
                    "Human approval cannot grant training use when policy denies it.",
                    403,
                )
            if any(
                isinstance(issue, dict)
                and (
                    issue.get("severity") == "error"
                    or self._training_issue_requires_exclusion(issue)
                )
                for issue in envelope.get("issues", [])
            ):
                raise _error(
                    "CATALYST_TRAINING_RECORD_APPROVAL_INVALID",
                    "Training record requires correction",
                    "Resolve error-level or unrepresentable issues by remapping or "
                    "excluding this record.",
                    409,
                )
            envelope["disposition"] = "eligible"
        elif action == "exclude":
            envelope["disposition"] = "excluded"
        else:
            previous_issues = envelope.get("issues", [])
            duplicate_blockers = [
                issue
                for issue in previous_issues
                if isinstance(issue, dict) and "DUPLICATE" in str(issue.get("code", "")).upper()
            ]
            response = self._invoke_plugin(
                capability="dataset.preparation.v1",
                environment_name=DATASET_PREPARATION_CONNECTION_ENV,
                method="remap_training_record",
                request={
                    "record": envelope,
                    "recipe": recipe,
                    "recipe_digest": recipe_digest,
                    **({"format": edit["format"]} if edit.get("format") is not None else {}),
                    **(
                        {"field_mapping": edit["fieldMapping"]}
                        if edit.get("fieldMapping") is not None
                        else {}
                    ),
                    **(
                        {"role_mapping": edit["roleMapping"]}
                        if edit.get("roleMapping") is not None
                        else {}
                    ),
                    **(
                        {"raw_record": edit["rawRecord"]}
                        if edit.get("rawRecord") is not None
                        else {}
                    ),
                    **({"note": edit_note} if edit_note else {}),
                },
                cancel_event=Event(),
            )
            if (
                response.get("id") != envelope.get("id")
                or response.get("schemaVersion") != "cyrene.training-record.v1"
                or not isinstance(response.get("issues"), list)
            ):
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_OUTPUT_INVALID",
                    "The remap Plugin changed immutable record identity or lineage.",
                    retryable=False,
                    outcome_unknown=False,
                )
            immutable_fields = (
                "sourceRevisionId",
                "sourceFamilyId",
                "sampleId",
                "conversationId",
                "ordinal",
                "locator",
                "policy",
                "recipeDigest",
            )
            if any(response.get(field) != envelope.get(field) for field in immutable_fields):
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_OUTPUT_INVALID",
                    "The remap Plugin changed immutable record lineage, policy, or "
                    "recipe identity.",
                    retryable=False,
                    outcome_unknown=False,
                )
            if edit.get("rawRecord") is None and response.get("rawRecord") != envelope.get(
                "rawRecord"
            ):
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_OUTPUT_INVALID",
                    "The remap Plugin changed raw content without an explicit correction.",
                    retryable=False,
                    outcome_unknown=False,
                )
            prior_history = envelope.get("processingHistory")
            updated_history = response.get("processingHistory")
            if (
                not isinstance(prior_history, list)
                or not isinstance(updated_history, list)
                or updated_history[: len(prior_history)] != prior_history
                or not any(
                    isinstance(entry, dict) and entry.get("operation") == "humanRemap"
                    for entry in updated_history[len(prior_history) :]
                )
            ):
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_OUTPUT_INVALID",
                    "The remap Plugin did not preserve history and append its human correction.",
                    retryable=False,
                    outcome_unknown=False,
                )
            envelope = response
            if duplicate_blockers:
                # A one-record remap cannot re-run batch deduplication. Keep the prior
                # blocker visible until a batch curation run recomputes duplicate state.
                current_issues = envelope.get("issues", [])
                if not isinstance(current_issues, list):
                    current_issues = []
                existing_codes = {
                    str(issue.get("code")) for issue in current_issues if isinstance(issue, dict)
                }
                current_issues.extend(
                    issue
                    for issue in duplicate_blockers
                    if str(issue.get("code")) not in existing_codes
                )
                envelope["issues"] = current_issues
            if self._training_policy_allows(
                envelope.get("policy")
            ) and not self._training_record_explicitly_denied(envelope.get("rawRecord")):
                envelope["disposition"] = "review"
                if not envelope.get("issues"):
                    envelope["issues"] = [
                        {
                            "code": "HUMAN_REVIEW_REQUIRED",
                            "message": "The remapped record needs explicit approval or exclusion.",
                            "severity": "warning",
                        }
                    ]
            else:
                envelope["disposition"] = "excluded"
        history = envelope.get("processingHistory")
        if not isinstance(history, list):
            history = []
            envelope["processingHistory"] = history
        history.append(
            {
                "operation": "humanReview",
                "mode": action,
                "recipeDigest": recipe_digest,
                "inputDigest": envelope.get("contentDigest"),
                "outputDigest": envelope.get("contentDigest"),
                **({"note": str(edit_note)[:2000]} if edit_note else {}),
            }
        )
        return envelope

    @staticmethod
    def _training_issue_requires_exclusion(issue: dict[str, Any]) -> bool:
        """Keep lossy or unsupported structures out of learned conversational rows."""

        code = issue.get("code")
        return isinstance(code, str) and (
            code.startswith("UNSUPPORTED_") or code.endswith("_UNSUPPORTED")
        )

    def _reconcile_training_snapshot(self, revision: ContentRevision) -> TrainingCurationCounts:
        """Check persisted counters, source membership, and unique IDs from the artifact."""

        snapshot = revision.training_data_snapshot
        if snapshot is None:
            raise ValueError("ContentRevision has no training-data snapshot.")
        path = self.artifacts.resolve(snapshot.artifact)
        counts, per_source = self._count_training_records(path)
        if counts != snapshot.counts or counts.total != snapshot.record_count:
            raise ValueError("Training snapshot counts do not match its immutable artifact.")
        if set(per_source) - set(revision.source_revision_ids):
            raise ValueError("Training snapshot references a source outside the revision.")

        index_path = self.artifacts.stage_path(f"{uuid4()}.training-record-index.sqlite3")
        connection = sqlite3.connect(index_path)
        try:
            connection.execute("CREATE TABLE record_ids (id TEXT PRIMARY KEY)")
            for envelope in self._iter_training_record_envelopes(path):
                record_id = envelope.get("id")
                if not isinstance(record_id, str) or not record_id:
                    raise ValueError("Training record id is missing.")
                try:
                    connection.execute("INSERT INTO record_ids (id) VALUES (?)", (record_id,))
                except sqlite3.IntegrityError as exc:
                    raise ValueError("Training snapshot contains duplicate record ids.") from exc
            connection.commit()
        finally:
            connection.close()
            index_path.unlink(missing_ok=True)
        return counts

    def _verify_training_sft_export(
        self,
        bundle_path: Path,
        files: dict[str, Any],
        response: dict[str, Any],
        run: ProcessingRun,
        revision: ContentRevision,
        output_format: str,
        split_request: dict[str, Any],
    ) -> None:
        """Validate every learned split and reconcile its sidecar to the approved snapshot."""

        snapshot = revision.training_data_snapshot
        if snapshot is None:
            raise ValueError("Training SFT export requires an artifact-backed snapshot.")
        counts = self._reconcile_training_snapshot(revision)
        if counts.pending_review:
            raise ValueError("Training SFT export cannot contain pending-review records.")
        expected_schema = {
            "sft": ("instruction_history", ["instruction", "input", "output", "system", "history"]),
            "messages": ("messages", ["messages"]),
            "promptCompletion": ("prompt_completion", ["prompt", "completion"]),
        }.get(output_format)
        if expected_schema is None:
            raise ValueError("The requested SFT output format is unsupported.")
        schema_name, schema_fields = expected_schema
        expected_counts = {
            "total": counts.total,
            "recognized": counts.recognized,
            "formatErrors": counts.format_errors,
            "duplicateCandidates": counts.duplicate_candidates,
            "pendingReview": counts.pending_review,
            "eligible": counts.eligible,
            "excluded": counts.excluded,
            "policyExcluded": 0,
            "published": counts.eligible,
        }
        reported_counts = _json_object(response.get("counts"), "SFT export count ledger")
        if any(
            _int_field(reported_counts.get(key), f"counts.{key}") != value
            for key, value in expected_counts.items()
        ):
            raise ValueError("SFT export counts do not reconcile to the approved snapshot.")
        if _int_field(response.get("sample_count"), "sample_count") != counts.eligible:
            raise ValueError("SFT sample count does not match approved eligible records.")
        if response.get("output_format") != output_format:
            raise ValueError("SFT exporter returned a different output format.")
        if response.get("recipe_digest") != run.recipe_digest:
            raise ValueError("SFT exporter returned a different processing recipe digest.")

        index_path = self.artifacts.stage_path(f"{uuid4()}.sft-export-index.sqlite3")
        connection = sqlite3.connect(index_path)
        try:
            connection.executescript(
                "CREATE TABLE expected ("
                "record_id TEXT PRIMARY KEY, sample_id TEXT NOT NULL, "
                "source_revision_id TEXT NOT NULL, source_family_id TEXT NOT NULL, "
                "conversation_id TEXT, locator_json TEXT NOT NULL, raw_digest TEXT NOT NULL, "
                "content_digest TEXT NOT NULL, recipe_digest TEXT NOT NULL, "
                "history_json TEXT NOT NULL, policy_json TEXT NOT NULL, "
                "issues_json TEXT NOT NULL, split TEXT);"
                "CREATE TABLE lineage_split ("
                "kind TEXT NOT NULL, lineage_id TEXT NOT NULL, split TEXT NOT NULL, "
                "PRIMARY KEY(kind, lineage_id));"
                "CREATE TABLE recipe_digest (digest TEXT PRIMARY KEY);"
            )
            source_ids = {str(source_id) for source_id in revision.source_revision_ids}
            snapshot_path = self.artifacts.resolve(snapshot.artifact)
            for envelope in self._iter_training_record_envelopes(snapshot_path):
                disposition = envelope.get("disposition")
                if disposition == "excluded":
                    continue
                if disposition != "eligible":
                    raise ValueError("Only approved eligible records may be exported.")
                record_id = envelope.get("id")
                sample_id = envelope.get("sampleId")
                source_id = envelope.get("sourceRevisionId")
                family_id = envelope.get("sourceFamilyId")
                conversation_id = envelope.get("conversationId")
                locator = envelope.get("locator")
                normalized = envelope.get("normalized")
                policy = envelope.get("policy")
                issues = envelope.get("issues", [])
                history = envelope.get("processingHistory")
                content_digest = envelope.get("contentDigest")
                recipe_digest = envelope.get("recipeDigest")
                raw_line = envelope.get("rawLine")
                raw_record = envelope.get("rawRecord")
                if (
                    not isinstance(record_id, str)
                    or not record_id
                    or not isinstance(sample_id, str)
                    or not sample_id
                    or not isinstance(source_id, str)
                    or source_id not in source_ids
                    or not isinstance(family_id, str)
                    or not family_id
                    or (conversation_id is not None and not isinstance(conversation_id, str))
                    or not isinstance(locator, dict)
                    or not isinstance(normalized, dict)
                    or not isinstance(policy, dict)
                    or not isinstance(issues, list)
                    or not isinstance(history, list)
                    or not isinstance(content_digest, str)
                    or not re.fullmatch(r"sha256:[0-9a-f]{64}", content_digest)
                    or not isinstance(recipe_digest, str)
                    or not re.fullmatch(r"sha256:[0-9a-f]{64}", recipe_digest)
                    or (raw_line is not None and not isinstance(raw_line, str))
                ):
                    raise ValueError("Approved training record lineage or provenance is invalid.")
                if content_digest != _digest_bytes(_canonical_json(normalized)):
                    raise ValueError("Approved training record content digest is invalid.")
                raw_digest = _digest_bytes(
                    raw_line.encode("utf-8")
                    if raw_line is not None
                    else _canonical_json(raw_record)
                )
                connection.execute(
                    "INSERT INTO expected VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
                    (
                        record_id,
                        sample_id,
                        source_id,
                        family_id,
                        conversation_id,
                        _canonical_json(locator).decode("utf-8"),
                        raw_digest,
                        content_digest,
                        recipe_digest,
                        _canonical_json(history).decode("utf-8"),
                        _canonical_json(policy).decode("utf-8"),
                        _canonical_json(issues).decode("utf-8"),
                    ),
                )
                connection.execute(
                    "INSERT OR IGNORE INTO recipe_digest (digest) VALUES (?)",
                    (recipe_digest,),
                )
            connection.commit()

            recipe_digests = [
                row[0]
                for row in connection.execute("SELECT digest FROM recipe_digest ORDER BY digest")
            ]
            if response.get("source_recipe_digests") != recipe_digests:
                raise ValueError("SFT source recipe digests do not match the approved snapshot.")

            expected_files = {
                "train.jsonl",
                "validation.jsonl",
                "test.jsonl",
                "provenance.jsonl",
                "manifest.json",
            }
            if set(files) != expected_files:
                raise ValueError("SFT export file receipts are incomplete.")
            row_counts: dict[str, int] = {}
            provenance_counts = {name: 0 for name in ("train", "validation", "test")}
            with zipfile.ZipFile(bundle_path) as archive:
                if set(archive.namelist()) != expected_files:
                    raise ValueError("SFT bundle members differ from the declared file receipts.")
                for split_name in ("train", "validation", "test"):
                    filename = f"{split_name}.jsonl"
                    receipt = _json_object(files.get(filename), f"SFT {filename} receipt")
                    if receipt.get("schema_fields") != schema_fields:
                        raise ValueError(f"SFT {filename} schema receipt is invalid.")
                    count = 0
                    with archive.open(filename) as stream:
                        while True:
                            line = stream.readline(MAX_TRAINING_ROW_BYTES + 1)
                            if not line:
                                break
                            if len(line) > MAX_TRAINING_ROW_BYTES or not line.strip():
                                raise ValueError(f"SFT {filename} contains an invalid row.")
                            row = json.loads(line.decode("utf-8"))
                            if not isinstance(row, dict):
                                raise ValueError(f"SFT {filename} row must be a JSON object.")
                            self._validate_curation_export_row(row, output_format, count + 1)
                            count += 1
                    if _int_field(receipt.get("row_count"), f"{filename}.row_count") != count:
                        raise ValueError(f"SFT {filename} row count differs from its receipt.")
                    row_counts[split_name] = count

                provenance_receipt = _json_object(
                    files.get("provenance.jsonl"), "SFT provenance receipt"
                )
                required_provenance_fields = {
                    "record_id",
                    "sample_id",
                    "source_revision_id",
                    "source_family_id",
                    "conversation_id",
                    "locator",
                    "raw_digest",
                    "content_digest",
                    "curation_recipe_digest",
                    "processing_history",
                    "policy",
                    "issues",
                    "split",
                }
                provenance_count = 0
                with archive.open("provenance.jsonl") as stream:
                    while True:
                        line = stream.readline(MAX_TRAINING_ROW_BYTES + 1)
                        if not line:
                            break
                        if len(line) > MAX_TRAINING_ROW_BYTES or not line.strip():
                            raise ValueError("SFT provenance contains an invalid row.")
                        receipt = json.loads(line.decode("utf-8"))
                        if (
                            not isinstance(receipt, dict)
                            or set(receipt) != required_provenance_fields
                        ):
                            raise ValueError("SFT provenance row has an invalid schema.")
                        export_record_id = receipt.get("record_id")
                        export_split = receipt.get("split")
                        expected = connection.execute(
                            "SELECT sample_id, source_revision_id, source_family_id, "
                            "conversation_id, locator_json, raw_digest, content_digest, "
                            "recipe_digest, history_json, policy_json, issues_json, split "
                            "FROM expected WHERE record_id = ?",
                            (export_record_id,),
                        ).fetchone()
                        if (
                            expected is None
                            or not isinstance(export_split, str)
                            or export_split not in provenance_counts
                            or not isinstance(export_record_id, str)
                            or not export_record_id
                        ):
                            raise ValueError(
                                "SFT provenance refers to an unknown or excluded record."
                            )
                        expected_values = (
                            expected[0],
                            expected[1],
                            expected[2],
                            expected[3],
                            expected[4],
                            expected[5],
                            expected[6],
                            expected[7],
                            expected[8],
                            expected[9],
                            expected[10],
                        )
                        received_values = (
                            receipt.get("sample_id"),
                            receipt.get("source_revision_id"),
                            receipt.get("source_family_id"),
                            receipt.get("conversation_id"),
                            _canonical_json(receipt.get("locator")).decode("utf-8"),
                            receipt.get("raw_digest"),
                            receipt.get("content_digest"),
                            receipt.get("curation_recipe_digest"),
                            _canonical_json(receipt.get("processing_history")).decode("utf-8"),
                            _canonical_json(receipt.get("policy")).decode("utf-8"),
                            _canonical_json(receipt.get("issues")).decode("utf-8"),
                        )
                        if received_values != expected_values:
                            raise ValueError("SFT provenance does not match its approved record.")
                        updated = connection.execute(
                            "UPDATE expected SET split = ? WHERE record_id = ? AND split IS NULL",
                            (export_split, export_record_id),
                        )
                        if updated.rowcount != 1:
                            raise ValueError("SFT provenance duplicates an approved record.")
                        lineage_values = [("family", expected[2]), ("content", expected[6])]
                        if expected[3] is not None:
                            lineage_values.append(("conversation", expected[3]))
                        for kind, lineage_id in lineage_values:
                            prior = connection.execute(
                                "SELECT split FROM lineage_split WHERE kind = ? AND lineage_id = ?",
                                (kind, lineage_id),
                            ).fetchone()
                            if prior is not None and prior[0] != export_split:
                                raise ValueError(
                                    "SFT split leaks a source-family or conversation lineage."
                                )
                            connection.execute(
                                "INSERT OR IGNORE INTO lineage_split (kind, lineage_id, split) "
                                "VALUES (?, ?, ?)",
                                (kind, lineage_id, export_split),
                            )
                        provenance_counts[export_split] += 1
                        provenance_count += 1
                if (
                    _int_field(provenance_receipt.get("row_count"), "provenance.row_count")
                    != provenance_count
                ):
                    raise ValueError("SFT provenance row count differs from its receipt.")
                if connection.execute(
                    "SELECT COUNT(*) FROM expected WHERE split IS NULL"
                ).fetchone()[0]:
                    raise ValueError("SFT provenance omitted an approved eligible record.")
                if provenance_count != counts.eligible:
                    raise ValueError("SFT provenance count does not match approved eligibility.")
                if any(row_counts[name] != provenance_counts[name] for name in row_counts):
                    raise ValueError("SFT learned rows do not reconcile with provenance splits.")

                samples = {
                    "train": row_counts["train"],
                    "validation": row_counts["validation"],
                    "test": row_counts["test"],
                }
                response_split_stats = _json_object(
                    response.get("split_stats"), "SFT split statistics"
                )
                if response_split_stats.get("samples") != samples:
                    raise ValueError("SFT split statistics do not match split JSONL rows.")
                manifest_info = archive.getinfo("manifest.json")
                if manifest_info.file_size > 2 * 1024 * 1024:
                    raise ValueError("SFT manifest exceeds the supported size limit.")
                manifest = _json_object(
                    json.loads(archive.read("manifest.json").decode("utf-8")),
                    "SFT manifest",
                )
                if (
                    manifest.get("schema_version") != "cyrene.sft.bundle.v1"
                    or manifest.get("profile") != "CYRENE_SFT_BUNDLE_V1"
                    or manifest.get("dataset_id") != str(run.dataset_id)
                    or manifest.get("content_revision_id") != str(revision.id)
                    or manifest.get("processing_run_id") != str(run.id)
                    or manifest.get("mode") != output_format
                    or manifest.get("schema") != schema_name
                    or manifest.get("recipe")
                    != {
                        "capability": _GENERATION_CAPABILITY,
                        "method": "prepare_training_sft",
                        "version": run.recipe_version,
                        "digest": run.recipe_digest,
                    }
                    or manifest.get("source_recipe_digests") != recipe_digests
                    or manifest.get("split_ratios")
                    != {key: split_request[key] for key in ("train", "validation", "test")}
                    or manifest.get("split_seed") != split_request.get("seed", 42)
                    or manifest.get("split_stats") != response_split_stats
                ):
                    raise ValueError("SFT manifest lineage or recipe does not match the run.")
                manifest_counts = _json_object(manifest.get("counts"), "SFT manifest counts")
                expected_manifest_counts = {
                    **expected_counts,
                    "train": samples["train"],
                    "validation": samples["validation"],
                    "test": samples["test"],
                }
                if any(
                    _int_field(manifest_counts.get(key), f"manifest.counts.{key}") != value
                    for key, value in expected_manifest_counts.items()
                ):
                    raise ValueError("SFT manifest counts do not match its output rows.")
                manifest_files = _json_object(manifest.get("files"), "SFT manifest files")
                for name in expected_files - {"manifest.json"}:
                    if manifest_files.get(name) != files.get(name):
                        raise ValueError(
                            "SFT manifest file receipts differ from the bundle receipt."
                        )
                for name, expected_rows in {
                    "train.jsonl": row_counts["train"],
                    "validation.jsonl": row_counts["validation"],
                    "test.jsonl": row_counts["test"],
                    "provenance.jsonl": provenance_count,
                }.items():
                    receipt = _json_object(files.get(name), f"SFT {name} receipt")
                    if _int_field(receipt.get("row_count"), f"{name}.row_count") != expected_rows:
                        raise ValueError(f"SFT {name} row count does not match verified content.")
                manifest_receipt = _json_object(files.get("manifest.json"), "SFT manifest receipt")
                if _int_field(manifest_receipt.get("row_count"), "manifest.row_count") != 1:
                    raise ValueError("SFT manifest must be one complete receipt document.")
        finally:
            connection.close()
            index_path.unlink(missing_ok=True)

    def _count_training_records(
        self,
        path: Path,
    ) -> tuple[TrainingCurationCounts, dict[UUID, dict[str, Any]]]:
        """Reconcile dispositions and diagnostics from the edited JSONL artifact."""

        counts: dict[str, int] = {
            "total": 0,
            "recognized": 0,
            "formatErrors": 0,
            "duplicateCandidates": 0,
            "pendingReview": 0,
            "excluded": 0,
            "eligible": 0,
        }
        per_source: dict[UUID, dict[str, Any]] = {}
        for envelope in self._iter_training_record_envelopes(path):
            source_id = UUID(str(envelope["sourceRevisionId"]))
            source_counts = per_source.setdefault(
                source_id,
                {
                    "total": 0,
                    "reviewItems": 0,
                    "diagnosticCounts": {},
                },
            )
            counts["total"] += 1
            source_counts["total"] += 1
            if envelope.get("detectedFormat") not in {None, "unknown"}:
                counts["recognized"] += 1
            issues = envelope.get("issues", [])
            if not isinstance(issues, list) or any(
                not isinstance(issue, dict)
                or not isinstance(issue.get("code"), str)
                or not issue.get("code")
                or not isinstance(issue.get("severity", "warning"), str)
                or issue.get("severity", "warning") not in {"warning", "error"}
                for issue in issues
            ):
                raise ValueError("Training record issues are invalid.")
            if any(
                isinstance(issue, dict)
                and issue.get("code")
                in {
                    "INVALID_JSON",
                    "training.invalid_json",
                    "INVALID_ENCODING",
                    "FORMAT_UNRECOGNIZED",
                    "MESSAGE_STRUCTURE_INVALID",
                    "FIELD_MISSING",
                    "FIELD_TYPE_INVALID",
                }
                for issue in issues
            ):
                counts["formatErrors"] += 1
            disposition = envelope.get("disposition")
            if disposition == "review":
                counts["pendingReview"] += 1
                if not issues:
                    raise ValueError(
                        "Review records must retain at least one issue in the artifact."
                    )
                source_counts["reviewItems"] += len(issues)
                for issue in issues:
                    code = str(issue["code"])
                    source_counts["diagnosticCounts"][code] = (
                        source_counts["diagnosticCounts"].get(code, 0) + 1
                    )
            elif disposition == "excluded":
                counts["excluded"] += 1
            elif disposition == "eligible":
                if not isinstance(envelope.get("normalized"), dict):
                    raise ValueError("Eligible training records require normalized messages.")
                if not self._training_policy_allows(
                    envelope.get("policy")
                ) or self._training_record_explicitly_denied(envelope.get("rawRecord")):
                    raise ValueError("Policy-denied records cannot be eligible for training.")
                if any(
                    issue.get("severity") == "error"
                    or self._training_issue_requires_exclusion(issue)
                    for issue in issues
                ):
                    raise ValueError(
                        "Unresolved or unrepresentable records cannot be eligible for training."
                    )
                counts["eligible"] += 1
            else:
                raise ValueError("Training record disposition is invalid.")
            if any(
                isinstance(issue, dict) and issue.get("code") == "DUPLICATE_EXACT"
                for issue in issues
            ):
                counts["duplicateCandidates"] += 1
        return TrainingCurationCounts.model_validate(counts), per_source

    def resolve_review_item(
        self,
        item_id: UUID,
        action: ReviewItemResolution,
        note: str | None = None,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ReviewItem:
        """Apply a scoped human disposition to one parser/OCR issue."""

        try:
            return self.store.resolve_review_item(item_id, action, note, principal)
        except LookupError as exc:
            raise _error(
                "CATALYST_REVIEW_ITEM_NOT_FOUND",
                "Review item not found",
                "No review item exists with the requested id in this workspace.",
                404,
            ) from exc
        except ValueError as exc:
            raise _error(
                "CATALYST_REVIEW_ITEM_ALREADY_RESOLVED",
                "Review item already resolved",
                "A resolved item cannot receive a different disposition.",
                409,
            ) from exc

    def list_generated_drafts(
        self,
        dataset_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> list[tuple[ContentRevision, UUID | None]]:
        """List generated DRAFT revisions and their producing run when available."""

        self.require_dataset(dataset_id, principal)
        runs = self.store.list_runs(dataset_id, principal)
        run_ids: dict[UUID, UUID] = {}
        for run in runs:
            if run.operation != ProcessingOperation.GENERATE_QA:
                continue
            for stage in run.stages:
                if stage.output is None:
                    continue
                raw_revision_id = stage.output.get("generatedContentRevisionId")
                if isinstance(raw_revision_id, str):
                    try:
                        run_ids.setdefault(UUID(raw_revision_id), run.id)
                    except ValueError:
                        continue
        drafts = []
        for revision in self.store.list_content_revisions(dataset_id, principal):
            if revision.state != ContentRevisionState.DRAFT or not any(
                block.origin == BlockOrigin.GENERATED for block in revision.blocks
            ):
                continue
            drafts.append((revision, run_ids.get(revision.id)))
        return drafts

    def list_content_revisions(
        self,
        dataset_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> list[ContentRevision]:
        """List immutable block snapshots newest-first."""

        self.require_dataset(dataset_id, principal)
        return self.store.list_content_revisions(dataset_id, principal)

    def get_content_revision(
        self,
        revision_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ContentRevision:
        """Read one content snapshot inside the caller's workspace."""

        revision = self.store.get_content_revision(revision_id, principal)
        if revision is None:
            raise _error(
                "CATALYST_CONTENT_REVISION_NOT_FOUND",
                "ContentRevision not found",
                "No ContentRevision exists with the requested id in this workspace.",
                404,
            )
        return revision

    def create_run(
        self,
        *,
        dataset_id: UUID,
        operation: ProcessingOperation,
        source_revision_ids: list[UUID] | None,
        content_revision_id: UUID | None,
        config: dict[str, Any],
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ProcessingRun:
        """Persist and enqueue one selected Product processing operation."""

        self.require_dataset(dataset_id, principal)
        self._validate_run_config(operation, config)
        selected_sources: list[SourceRevision] = []
        revision: ContentRevision | None = None
        if operation in {
            ProcessingOperation.PARSE,
            ProcessingOperation.CURATE_TRAINING_DATA,
        }:
            all_sources = self.store.list_sources(dataset_id, principal)
            if source_revision_ids:
                source_by_id = {item.id: item for item in all_sources}
                selected_ids: set[UUID] = set()
                for source_id in source_revision_ids:
                    source = source_by_id.get(source_id)
                    if source is None:
                        raise _error(
                            "CATALYST_SOURCE_REVISION_NOT_FOUND",
                            "SourceRevision not found",
                            "Every source revision must belong to this Dataset and workspace.",
                            404,
                        )
                    if source.id not in selected_ids:
                        selected_sources.append(source)
                        selected_ids.add(source.id)
            else:
                latest: dict[UUID, SourceRevision] = {}
                for source in all_sources:
                    latest.setdefault(source.source_id, source)
                selected_sources = list(latest.values())
            if not selected_sources:
                raise _error(
                    "CATALYST_SOURCE_REQUIRED",
                    "Source required",
                    "Upload at least one source before starting a parse run.",
                    409,
                )
            if operation == ProcessingOperation.CURATE_TRAINING_DATA:
                prior_revisions = self.store.list_content_revisions(dataset_id, principal)
                revision = prior_revisions[0] if prior_revisions else None
        else:
            if content_revision_id is None:
                raise _error(
                    "CATALYST_CONTENT_REVISION_REQUIRED",
                    "ContentRevision required",
                    "Select an approved ContentRevision before building an output.",
                    422,
                )
            revision = self.get_content_revision(content_revision_id, principal)
            if revision.dataset_id != dataset_id:
                raise _error(
                    "CATALYST_CONTENT_REVISION_NOT_FOUND",
                    "ContentRevision not found",
                    "ContentRevision must belong to the requested Dataset.",
                    404,
                )
            if revision.state != ContentRevisionState.APPROVED:
                raise _error(
                    "CATALYST_CONTENT_REVISION_NOT_APPROVED",
                    "Approval required",
                    "Review and approve this ContentRevision before processing it.",
                    409,
                )
            selected_sources = [
                source
                for source_id in revision.source_revision_ids
                if (source := self.store.get_source(source_id, principal)) is not None
            ]

        # Configuration problems are rejected before durable admission; they are
        # never translated into a source-format error by an async worker.
        if operation == ProcessingOperation.CURATE_TRAINING_DATA:
            source_policies = config.get("sourcePolicies", {})
            selected_source_id_texts = {str(source.id) for source in selected_sources}
            if set(source_policies) - selected_source_id_texts:
                raise _error(
                    "CATALYST_SOURCE_POLICY_INVALID",
                    "Source policy is invalid",
                    "sourcePolicies may only reference selected SourceRevisions.",
                    422,
                )
        for source in (
            selected_sources
            if operation in {ProcessingOperation.PARSE, ProcessingOperation.CURATE_TRAINING_DATA}
            else [None]
        ):
            if source is not None and operation == ProcessingOperation.PARSE:
                if source.media_type == "application/x-ndjson":
                    continue
                if source.media_type not in (
                    _DOCUMENT_PARSER_MEDIA_TYPES
                    | {"application/json", "application/vnd.apache.parquet"}
                ):
                    continue
            self._preflight_binding(_binding_environment(operation, source))

        if operation == ProcessingOperation.GENERATE_QA:
            generation = config.get("generation")
            if not isinstance(generation, dict):
                raise _error(
                    "CATALYST_GENERATION_BUDGET_REQUIRED",
                    "Generation budget required",
                    "generateQa requires explicit maxExamples and maxCalls budgets.",
                    422,
                )
            self._validate_generation_budget(generation)
        if operation in {ProcessingOperation.PREPARE_SFT, ProcessingOperation.GENERATE_QA}:
            self._validate_split(config.get("split"))

        now = utc_now()
        chosen_source_ids = [source.id for source in selected_sources]
        recipe = {
            "operation": operation.value,
            "config": config,
            "sourceRevisionIds": [str(value) for value in chosen_source_ids],
            "contentRevisionId": str(revision.id) if revision else None,
        }
        recipe_digest = recipe_digest_for("data-tools-v1", recipe)
        stage_key = operation.value
        input_digest = self._run_input_digest(selected_sources, revision)
        run = ProcessingRun(
            id=uuid4(),
            dataset_id=dataset_id,
            operation=operation,
            source_revision_ids=chosen_source_ids,
            content_revision_id=revision.id if revision else None,
            recipe_version="data-tools-v1",
            recipe=recipe,
            recipe_digest=recipe_digest,
            stages=[
                ProcessingStage(
                    key=stage_key,
                    filename=stage_key,
                    deterministic=operation != ProcessingOperation.GENERATE_QA,
                    external_call=operation == ProcessingOperation.GENERATE_QA,
                    input_digest=input_digest,
                    recipe_digest=recipe_digest,
                )
            ],
            progress=ProcessingProgress(completed=0, total=1),
            created_at=now,
            updated_at=now,
            resource_version=1,
        )
        try:
            self.store.create_run(run, principal)
            if operation in {
                ProcessingOperation.PARSE,
                ProcessingOperation.CURATE_TRAINING_DATA,
            }:
                for source in selected_sources:
                    report_time = utc_now()
                    self.store.save_source_parse_report(
                        SourceParseReport(
                            id=uuid4(),
                            dataset_id=dataset_id,
                            source_revision_id=source.id,
                            processing_run_id=run.id,
                            status=SourceParseReportState.QUEUED,
                            created_at=report_time,
                            updated_at=report_time,
                        ),
                        principal,
                    )
            return self.coordinator.enqueue(run.id, principal)
        except LookupError as exc:
            raise _error(
                "CATALYST_DATASET_NOT_FOUND",
                "Dataset not found",
                "The requested Dataset is outside this workspace.",
                404,
            ) from exc

    @staticmethod
    def _validate_run_config(
        operation: ProcessingOperation,
        config: dict[str, Any],
    ) -> None:
        """Keep persisted/public recipes limited to non-secret product controls."""

        allowed = {
            ProcessingOperation.PARSE: set(),
            ProcessingOperation.CURATE_TRAINING_DATA: {"curation", "sourcePolicies"},
            ProcessingOperation.BUILD_KNOWLEDGE: set(),
            ProcessingOperation.PREPARE_SFT: {"sftMode", "outputFormat", "split"},
            ProcessingOperation.GENERATE_QA: {"generation", "split"},
        }[operation]
        unknown = set(config) - allowed
        if unknown:
            raise _error(
                "CATALYST_RUN_CONFIG_INVALID",
                "Processing configuration is invalid",
                f"Unsupported configuration fields: {sorted(unknown)}.",
                422,
            )
        if "sftMode" in config and config["sftMode"] not in {"instruction", "conversation"}:
            raise _error(
                "CATALYST_SFT_MODE_INVALID",
                "SFT mode is invalid",
                "sftMode must be instruction or conversation.",
                422,
            )
        if config.get("outputFormat", "sft") not in {"sft", "messages", "promptCompletion"}:
            raise _error(
                "CATALYST_SFT_FORMAT_INVALID",
                "SFT output format is invalid",
                "outputFormat must be sft, messages, or promptCompletion.",
                422,
            )
        if operation == ProcessingOperation.CURATE_TRAINING_DATA:
            curation = config.get("curation")
            if not isinstance(curation, dict):
                raise _error(
                    "CATALYST_CURATION_RECIPE_REQUIRED",
                    "Curation recipe required",
                    "curateTrainingData requires a versioned curation recipe.",
                    422,
                )
            recipe_fields = {
                "id",
                "version",
                "format",
                "fieldMapping",
                "roleMapping",
                "maxCharacters",
                "minCharacters",
                "unicodeNormalization",
            }
            if set(curation) - recipe_fields or not {"id", "version", "format"} <= set(curation):
                raise _error(
                    "CATALYST_CURATION_RECIPE_INVALID",
                    "Curation recipe is invalid",
                    "The curation recipe contains unsupported or missing fields.",
                    422,
                )
            if curation["format"] not in {
                "auto",
                "alpaca",
                "promptCompletion",
                "messages",
                "sharegpt",
                "chatml",
            }:
                raise _error(
                    "CATALYST_CURATION_FORMAT_INVALID",
                    "Curation format is invalid",
                    "The selected curation format is not supported.",
                    422,
                )
            if not isinstance(curation["id"], str) or not curation["id"].strip():
                raise _error(
                    "CATALYST_CURATION_RECIPE_INVALID",
                    "Curation recipe is invalid",
                    "Recipe id must be a non-empty string.",
                    422,
                )
            if not isinstance(curation["version"], str) or not curation["version"].strip():
                raise _error(
                    "CATALYST_CURATION_RECIPE_INVALID",
                    "Curation recipe is invalid",
                    "Recipe version must be a non-empty string.",
                    422,
                )
            for key, default, minimum, maximum in (
                ("maxCharacters", 100_000, 1, 10_000_000),
                ("minCharacters", 2, 0, 10_000_000),
            ):
                value = curation.get(key, default)
                if (
                    isinstance(value, bool)
                    or not isinstance(value, int)
                    or not minimum <= value <= maximum
                ):
                    raise _error(
                        "CATALYST_CURATION_RECIPE_INVALID",
                        "Curation recipe is invalid",
                        f"{key} must be a positive integer no greater than {maximum}.",
                        422,
                    )
            if curation.get("minCharacters", 2) > curation.get("maxCharacters", 100_000):
                raise _error(
                    "CATALYST_CURATION_RECIPE_INVALID",
                    "Curation recipe is invalid",
                    "minCharacters cannot exceed maxCharacters.",
                    422,
                )
            if curation.get("unicodeNormalization", "NFC") not in {
                "NFC",
                "NFD",
                "NFKC",
                "NFKD",
                "none",
            }:
                raise _error(
                    "CATALYST_CURATION_RECIPE_INVALID",
                    "Curation recipe is invalid",
                    "unicodeNormalization must be NFC, NFKC, or none.",
                    422,
                )
            if not isinstance(curation.get("fieldMapping", {}), dict) or not isinstance(
                curation.get("roleMapping", {}), dict
            ):
                raise _error(
                    "CATALYST_CURATION_RECIPE_INVALID",
                    "Curation recipe is invalid",
                    "fieldMapping and roleMapping must be objects.",
                    422,
                )
            source_policies = config.get("sourcePolicies", {})
            if not isinstance(source_policies, dict):
                raise _error(
                    "CATALYST_SOURCE_POLICY_INVALID",
                    "Source policy is invalid",
                    "sourcePolicies must map source revision ids to ContentPolicy objects.",
                    422,
                )
            try:
                for policy in source_policies.values():
                    ContentPolicy.model_validate(policy)
            except ValueError as exc:
                raise _error(
                    "CATALYST_SOURCE_POLICY_INVALID",
                    "Source policy is invalid",
                    "Every source policy must match the existing ContentPolicy contract.",
                    422,
                ) from exc

    @staticmethod
    def _validate_split(value: Any) -> dict[str, float] | None:
        """Validate mutually exclusive source-family split shares."""

        if value is None:
            return None
        if not isinstance(value, dict) or set(value) != {"train", "validation", "test"}:
            raise _error(
                "CATALYST_SPLIT_INVALID",
                "Split is invalid",
                "Split must contain train, validation, and test shares.",
                422,
            )
        shares: dict[str, float] = {}
        for key, share in value.items():
            if (
                isinstance(share, bool)
                or not isinstance(share, (int, float))
                or not 0 <= share <= 1
            ):
                raise _error(
                    "CATALYST_SPLIT_INVALID",
                    "Split is invalid",
                    "Each split share must be between zero and one.",
                    422,
                )
            shares[key] = float(share)
        if abs(sum(shares.values()) - 1.0) > 1e-6:
            raise _error(
                "CATALYST_SPLIT_INVALID",
                "Split is invalid",
                "Train, validation, and test shares must sum to one.",
                422,
            )
        return shares

    @staticmethod
    def _validate_generation_budget(value: dict[str, Any]) -> None:
        """Enforce bounded paid-call limits before any run is admitted."""

        allowed = {"maxExamples", "maxCalls", "maxInputTokens", "maxOutputTokens"}
        if set(value) - allowed or not {"maxExamples", "maxCalls"} <= set(value):
            raise _error(
                "CATALYST_GENERATION_BUDGET_INVALID",
                "Generation budget is invalid",
                "Generation requires maxExamples and maxCalls and accepts no unknown fields.",
                422,
            )
        bounds = {
            "maxExamples": 1000,
            "maxCalls": 1000,
            "maxInputTokens": 1_000_000,
            "maxOutputTokens": 100_000,
        }
        for key, maximum in bounds.items():
            if key not in value:
                continue
            number = value[key]
            if (
                isinstance(number, bool)
                or not isinstance(number, int)
                or not 1 <= number <= maximum
            ):
                raise _error(
                    "CATALYST_GENERATION_BUDGET_INVALID",
                    "Generation budget is invalid",
                    f"{key} must be a positive integer no greater than {maximum}.",
                    422,
                )

    def get_run(
        self,
        run_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ProcessingRun:
        """Read a run and calculate staleness from the current approval."""

        run = self.coordinator.get(run_id, principal)
        if run is None:
            raise _error(
                "CATALYST_PROCESSING_RUN_NOT_FOUND",
                "ProcessingRun not found",
                "No ProcessingRun exists with the requested id in this workspace.",
                404,
            )
        return self.project_run_staleness(run, principal)

    def list_runs(
        self,
        dataset_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> list[ProcessingRun]:
        """List runs newest-first and project current staleness."""

        self.require_dataset(dataset_id, principal)
        return [
            self.project_run_staleness(run, principal)
            for run in self.store.list_runs(dataset_id, principal)
        ]

    def cancel_run(
        self,
        run_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ProcessingRun:
        """Request cooperative cancellation for one scoped run."""

        try:
            run = self.coordinator.cancel(run_id, principal)
        except LookupError as exc:
            raise _error(
                "CATALYST_PROCESSING_RUN_NOT_FOUND",
                "ProcessingRun not found",
                "No ProcessingRun exists with the requested id in this workspace.",
                404,
            ) from exc
        return self.project_run_staleness(run, principal)

    def retry_run(
        self,
        run_id: UUID,
        idempotency_key: str | None,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ProcessingRun:
        """Create or replay a linked retry without repeating ambiguous calls."""

        request_digest = _digest_bytes(_canonical_json({"retryOfRunId": str(run_id)}))
        scope = f"retry-processing-run:{run_id}"
        replay_id = self.datasets.resolve_idempotency(
            scope,
            idempotency_key,
            request_digest,
            principal,
        )
        if replay_id is not None:
            return self.get_run(UUID(replay_id), principal)
        try:
            retried = self.coordinator.retry(run_id, principal)
        except LookupError as exc:
            raise _error(
                "CATALYST_PROCESSING_RUN_NOT_FOUND",
                "ProcessingRun not found",
                "No ProcessingRun exists with the requested id in this workspace.",
                404,
            ) from exc
        except ValueError as exc:
            raise _error(
                "CATALYST_PROCESSING_RUN_RETRY_REJECTED",
                "ProcessingRun cannot be retried",
                str(exc),
                409,
            ) from exc
        self.datasets.remember_idempotency(
            scope=scope,
            key=idempotency_key,
            request_hash=request_digest,
            resource_kind="processing-run",
            resource_id=retried.id,
            principal=principal,
        )
        return self.get_run(retried.id, principal)

    def edit_block(
        self,
        *,
        revision_id: UUID,
        block_id: str,
        expected_revision_id: UUID,
        text: str | None,
        policy: ContentPolicy | None,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ContentRevision:
        """Create a new immutable ContentRevision with one human edit."""

        current = self.get_content_revision(revision_id, principal)
        if current.id != expected_revision_id:
            raise _error(
                "CATALYST_CONTENT_REVISION_CONFLICT",
                "Content changed",
                "Reload the latest ContentRevision before editing this block.",
                409,
                retryable=True,
            )
        revisions = self.store.list_content_revisions(current.dataset_id, principal)
        if not revisions or revisions[0].id != current.id:
            raise _error(
                "CATALYST_CONTENT_REVISION_CONFLICT",
                "Content changed",
                "Only the latest ContentRevision can be edited.",
                409,
                retryable=True,
            )
        block_index = next(
            (index for index, block in enumerate(current.blocks) if block.id == block_id),
            None,
        )
        if block_index is None:
            raise _error(
                "CATALYST_CONTENT_BLOCK_NOT_FOUND",
                "ContentBlock not found",
                "The requested block does not exist in this ContentRevision.",
                404,
            )
        blocks = list(current.blocks)
        original = blocks[block_index]
        update: dict[str, Any] = {"origin": BlockOrigin.HUMAN_EDITED}
        if text is not None:
            update["text"] = text
        if policy is not None:
            update["policy"] = policy
        blocks[block_index] = original.model_copy(update=update)
        return self._create_content_revision(
            dataset_id=current.dataset_id,
            source_revision_ids=current.source_revision_ids,
            parent_revision_id=current.id,
            blocks=blocks,
            principal=principal,
        )

    def review_content(
        self,
        *,
        revision_id: UUID,
        decision: ContentRevisionState,
        note: str | None,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ContentRevision:
        """Approve or reject one draft without changing its immutable blocks."""

        current = self.get_content_revision(revision_id, principal)
        if decision == ContentRevisionState.APPROVED and current.training_data_snapshot is not None:
            try:
                counts = self._reconcile_training_snapshot(current)
            except (CatalystError, OSError, sqlite3.Error, ValueError) as exc:
                raise _error(
                    "CATALYST_TRAINING_SNAPSHOT_INVALID",
                    "Training snapshot is invalid",
                    "The immutable record artifact does not match its saved lineage "
                    "and count ledger.",
                    409,
                ) from exc
            if counts.pending_review > 0:
                raise _error(
                    "CATALYST_TRAINING_RECORDS_PENDING_REVIEW",
                    "Training records require review",
                    "Approve or exclude every pending training record before "
                    "approving the revision.",
                    409,
                )
        if current.state != ContentRevisionState.DRAFT:
            if current.state == decision:
                return current
            raise _error(
                "CATALYST_CONTENT_REVISION_REVIEW_CONFLICT",
                "ContentRevision already reviewed",
                "Create a new ContentRevision before changing an approval decision.",
                409,
            )
        try:
            return self.store.update_content_review(revision_id, decision, note, principal)
        except LookupError as exc:
            raise _error(
                "CATALYST_CONTENT_REVISION_NOT_FOUND",
                "ContentRevision not found",
                "No ContentRevision exists with the requested id in this workspace.",
                404,
            ) from exc
        except ValueError as exc:
            raise _error(
                "CATALYST_CONTENT_REVIEW_BLOCKED",
                "ContentRevision requires review",
                "Resolve all parser and OCR review items before approving this revision.",
                409,
            ) from exc

    def publish_version(
        self,
        *,
        dataset_id: UUID,
        content_revision_id: UUID,
        knowledge_run_id: UUID | None,
        sft_run_id: UUID,
        idempotency_key: str | None,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> DatasetVersion:
        """Publish SFT alone for curation or both refs for the established dual profile."""

        self.require_dataset(dataset_id, principal)
        request_payload = {
            "contentRevisionId": str(content_revision_id),
            "knowledgeRunId": str(knowledge_run_id),
            "sftRunId": str(sft_run_id),
        }
        request_digest = _digest_bytes(_canonical_json(request_payload))
        scope = f"publish-data-tools-version:{dataset_id}"
        replay_id = self.datasets.resolve_idempotency(
            scope,
            idempotency_key,
            request_digest,
            principal,
        )
        if replay_id is not None:
            replay = self.datasets.get_version(UUID(replay_id), principal)
            if replay is None:
                raise _error(
                    "CATALYST_STATE_CORRUPT",
                    "Product state is inconsistent",
                    "The idempotency ledger references a missing DatasetVersion.",
                    500,
                )
            return self.project_version_staleness(replay, principal)

        with self._publish_lock:
            revision = self.get_content_revision(content_revision_id, principal)
            if revision.dataset_id != dataset_id:
                raise _error(
                    "CATALYST_CONTENT_REVISION_NOT_FOUND",
                    "ContentRevision not found",
                    "ContentRevision must belong to the requested Dataset.",
                    404,
                )
            if revision.state != ContentRevisionState.APPROVED:
                raise _error(
                    "CATALYST_CONTENT_REVISION_NOT_APPROVED",
                    "Approval required",
                    "Approve the selected ContentRevision before publication.",
                    409,
                )
            sft = self.get_run(sft_run_id, principal)
            knowledge: ProcessingRun | None = None
            if knowledge_run_id is not None:
                knowledge = self.get_run(knowledge_run_id, principal)
                self._require_publishable_run(
                    knowledge,
                    dataset_id,
                    revision.id,
                    ProcessingOperation.BUILD_KNOWLEDGE,
                )
            elif revision.training_data_snapshot is None:
                raise _error(
                    "CATALYST_KNOWLEDGE_RUN_REQUIRED",
                    "Knowledge output required",
                    "Only an approved training-curation snapshot may publish an SFT-only version.",
                    422,
                )
            self._require_publishable_run(
                sft,
                dataset_id,
                revision.id,
                ProcessingOperation.PREPARE_SFT,
            )
            knowledge_artifact = (
                self._run_package_artifact(knowledge) if knowledge is not None else None
            )
            sft_artifact = self._run_package_artifact(sft)
            source_revisions = [
                self.store.get_source(source_id, principal)
                for source_id in revision.source_revision_ids
            ]
            sources = [source for source in source_revisions if source is not None]
            if not sources:
                raise _error(
                    "CATALYST_SOURCE_REQUIRED",
                    "Source required",
                    "The approved ContentRevision has no readable source revisions.",
                    409,
                )
            train_artifact, row_count, schema_fields = self._publish_sft_train(
                sft_artifact,
                sft,
                output_format=(
                    sft.recipe.get("config", {}).get("outputFormat", "sft")
                    if revision.training_data_snapshot is not None
                    else None
                ),
            )
            now = utc_now()
            version_id = uuid4()
            lineage: list[LineageEdge] = []
            targets = {sft_artifact.digest, train_artifact.digest}
            if knowledge_artifact is not None:
                targets.add(knowledge_artifact.digest)
            for source in sources:
                for target in targets:
                    if source.digest != target:
                        lineage.append(LineageEdge(from_digest=source.digest, to_digest=target))
            version = DatasetVersion(
                id=version_id,
                dataset_id=dataset_id,
                version=self.datasets.next_version(dataset_id),
                state=DatasetVersionState.PUBLISHED,
                source=sources[0].artifact,
                output=train_artifact,
                engine_binding_id=_VERSION_BINDING_ID,
                engine_capability_type="dataset.generation.v1",
                lineage=lineage,
                source_refs=[
                    f"cyrene://catalyst/source-revisions/{source.id}" for source in sources
                ],
                row_count=row_count,
                schema_fields=schema_fields,
                data_tools=DataToolsVersionProjection(
                    content_revision_id=revision.id,
                    source_revision_ids=[source.id for source in sources],
                    knowledge_profile=(
                        "CYRENE_KNOWLEDGE_BUNDLE_V1" if knowledge_artifact is not None else None
                    ),
                    knowledge_artifact=knowledge_artifact,
                    sft_artifact=sft_artifact,
                    stale=False,
                ),
                created_at=now,
                updated_at=now,
                resource_version=1,
            )
            self.datasets.save_version(version)
            self.datasets.remember_idempotency(
                scope=scope,
                key=idempotency_key,
                request_hash=request_digest,
                resource_kind="dataset-version",
                resource_id=version.id,
                principal=principal,
            )
            return self.project_version_staleness(version, principal)

    @staticmethod
    def _require_publishable_run(
        run: ProcessingRun,
        dataset_id: UUID,
        revision_id: UUID,
        operation: ProcessingOperation,
    ) -> None:
        """Reject failed, stale, foreign, or wrong-operation output runs."""

        if (
            run.dataset_id != dataset_id
            or run.content_revision_id != revision_id
            or run.operation != operation
        ):
            raise _error(
                "CATALYST_RUN_PUBLICATION_MISMATCH",
                "ProcessingRun does not match publication",
                "Choose successful output runs for this Dataset and approved ContentRevision.",
                409,
            )
        if run.state != ProcessingRunState.SUCCEEDED:
            raise _error(
                "CATALYST_RUN_NOT_SUCCEEDED",
                "ProcessingRun is not complete",
                "A failed, cancelled, interrupted, or running operation cannot be published.",
                409,
            )
        if run.stale:
            raise _error(
                "CATALYST_RUN_STALE",
                "ProcessingRun is stale",
                "Rebuild outputs from the current approved ContentRevision before publication.",
                409,
            )

    def _run_package_artifact(self, run: ProcessingRun) -> ArtifactRef:
        """Read the verified package ref recorded in a completed stage."""

        for stage in run.stages:
            if stage.output is None:
                continue
            raw = stage.output.get("packageArtifact")
            if isinstance(raw, dict):
                try:
                    return ArtifactRef.model_validate(raw)
                except ValueError:
                    break
        raise _error(
            "CATALYST_RUN_OUTPUT_MISSING",
            "ProcessingRun output is missing",
            "The successful run does not contain its expected profile package.",
            500,
        )

    def _publish_sft_train(
        self,
        package: ArtifactRef,
        run: ProcessingRun,
        *,
        output_format: str | None = None,
    ) -> tuple[ArtifactRef, int, list[str]]:
        """Stream the train JSONL from a verified SFT ZIP into an immutable artifact."""

        package_path = self.artifacts.resolve(package)
        staged = self.artifacts.stage_path(f"{uuid4()}.train.jsonl")
        digest = hashlib.sha256()
        count = 0
        schemas: set[str] = set()
        try:
            with zipfile.ZipFile(package_path) as archive:
                names = archive.namelist()
                if len(names) != len(set(names)):
                    raise ValueError("SFT package contains duplicate member names")
                info = archive.getinfo("train.jsonl")
                if info.file_size > MAX_ZIP_ENTRY_BYTES or info.flag_bits & 0x1:
                    raise ValueError("train.jsonl exceeds the supported size or is encrypted")
                with archive.open(info) as member, staged.open("wb") as output:
                    for line_number, line in enumerate(member, start=1):
                        if len(line) > MAX_TRAINING_ROW_BYTES:
                            raise ValueError(f"train row {line_number} exceeds the supported size")
                        if not line.strip():
                            raise ValueError(f"blank JSONL line {line_number}")
                        record = json.loads(line.decode("utf-8"))
                        if not isinstance(record, dict):
                            raise ValueError(f"line {line_number} is not an object")
                        if output_format is not None:
                            self._validate_curation_export_row(record, output_format, line_number)
                        schemas.update(str(key) for key in record)
                        output.write(line)
                        digest.update(line)
                        count += 1
        except (
            OSError,
            KeyError,
            zipfile.BadZipFile,
            UnicodeError,
            json.JSONDecodeError,
            ValueError,
        ) as exc:
            staged.unlink(missing_ok=True)
            raise _error(
                "CATALYST_SFT_PACKAGE_INVALID",
                "SFT package is invalid",
                "train.jsonl must contain one valid JSON object per line.",
                422,
            ) from exc
        if count == 0:
            staged.unlink(missing_ok=True)
            raise _error(
                "CATALYST_SFT_PACKAGE_INVALID",
                "SFT package is invalid",
                "train.jsonl must contain at least one training record.",
                422,
            )
        stage_output = next(
            (stage.output for stage in run.stages if stage.output is not None),
            {},
        )
        reported_count = stage_output.get("trainRowCount") if stage_output else None
        if reported_count is not None and reported_count != count:
            raise _error(
                "CATALYST_SFT_PACKAGE_INVALID",
                "SFT package is invalid",
                "train.jsonl row count does not match the Plugin receipt.",
                422,
            )
        try:
            expected_digest = f"sha256:{digest.hexdigest()}"
            artifact = self.artifacts.publish(staged, "dataset")
        finally:
            staged.unlink(missing_ok=True)
        if artifact.digest != expected_digest:
            raise _error(
                "CATALYST_ARTIFACT_IDENTITY_INVALID",
                "Artifact identity invalid",
                "Artifact SDK returned a train artifact with an unexpected digest.",
                500,
            )
        return artifact, count, sorted(schemas)

    @staticmethod
    def _validate_curation_export_row(
        record: dict[str, Any], output_format: str, line_number: int
    ) -> None:
        """Validate learned rows without admitting provenance or management fields."""

        schemas = {
            "sft": {"instruction", "input", "output", "system", "history"},
            "messages": {"messages"},
            "promptCompletion": {"prompt", "completion"},
        }
        allowed = schemas.get(output_format)
        if allowed is None or set(record) != allowed:
            raise ValueError(f"line {line_number} does not match the requested learned schema")
        if output_format == "messages":
            messages = record["messages"]
            if not isinstance(messages, list) or not messages:
                raise ValueError(f"line {line_number} has no conversation messages")
            expected_role = "user"
            saw_user = False
            saw_assistant = False
            for index, message in enumerate(messages):
                if not isinstance(message, dict) or set(message) != {"role", "content"}:
                    raise ValueError(f"line {line_number} message {index} has unsupported fields")
                role = message.get("role")
                content = message.get("content")
                if not isinstance(content, str) or not content.strip():
                    raise ValueError(f"line {line_number} message {index} is empty")
                if role == "system" and index == 0:
                    continue
                if role != expected_role:
                    raise ValueError(f"line {line_number} conversation roles are out of order")
                saw_user = saw_user or role == "user"
                saw_assistant = saw_assistant or role == "assistant"
                expected_role = "assistant" if expected_role == "user" else "user"
            if expected_role != "user" or not saw_user or not saw_assistant:
                raise ValueError(f"line {line_number} conversation has no final assistant turn")
            return
        if output_format == "promptCompletion":
            if any(not isinstance(record[key], str) or not record[key].strip() for key in allowed):
                raise ValueError(
                    f"line {line_number} prompt/completion fields must be non-empty text"
                )
            return
        if (
            any(
                not isinstance(record[key], str)
                for key in ("instruction", "input", "output", "system")
            )
            or not record["instruction"].strip()
            or not record["output"].strip()
        ):
            raise ValueError(f"line {line_number} SFT text fields are invalid")
        history = record["history"]
        if not isinstance(history, list) or any(
            not isinstance(pair, list)
            or len(pair) != 2
            or any(not isinstance(text, str) or not text.strip() for text in pair)
            for pair in history
        ):
            raise ValueError(f"line {line_number} SFT history is invalid")

    def list_versions(
        self,
        dataset_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> list[DatasetVersion]:
        """Read canonical DatasetVersions with current stale projections."""

        self.require_dataset(dataset_id, principal)
        return [
            self.project_version_staleness(version, principal)
            for version in self.datasets.list_versions(dataset_id, principal)
        ]

    def get_version(
        self,
        version_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> DatasetVersion:
        """Read a canonical DatasetVersion inside its owning scope."""

        version = self.datasets.get_version(version_id, principal)
        if version is None:
            raise _error(
                "CATALYST_VERSION_NOT_FOUND",
                "DatasetVersion not found",
                "No DatasetVersion exists with the requested id in this workspace.",
                404,
            )
        return self.project_version_staleness(version, principal)

    def export_version(
        self,
        version_id: UUID,
        profile: Literal["knowledge", "sft"],
        principal: WorkspaceServicePrincipal | None = None,
    ) -> tuple[bytes, str]:
        """Download one immutable Data Tools package profile."""

        version = self.get_version(version_id, principal)
        if version.data_tools is None:
            raise _error(
                "CATALYST_DATA_TOOLS_EXPORT_NOT_FOUND",
                "Data Tools export not found",
                "This DatasetVersion does not contain Data Tools profile packages.",
                404,
            )
        artifact = (
            version.data_tools.knowledge_artifact
            if profile == "knowledge"
            else version.data_tools.sft_artifact
        )
        if artifact is None:
            raise _error(
                "CATALYST_DATA_TOOLS_EXPORT_NOT_FOUND",
                "Data Tools export not found",
                "This DatasetVersion does not contain the requested profile package.",
                404,
            )
        path = self.artifacts.resolve(artifact)
        try:
            if artifact.size_bytes > MAX_RESULT_BYTES:
                raise OSError("The selected package exceeds the supported download size.")
            data = path.read_bytes()
        except OSError as exc:
            raise _error(
                "CATALYST_ARTIFACT_UNAVAILABLE",
                "Artifact unavailable",
                "The selected package artifact cannot be read.",
                503,
                retryable=True,
            ) from exc
        return data, f"dataset-v{version.version}-{profile}.zip"

    def project_version_staleness(
        self,
        version: DatasetVersion,
        principal: WorkspaceServicePrincipal | None,
    ) -> DatasetVersion:
        """Compute stale status against the newest approved content snapshot."""

        if version.data_tools is None:
            return version
        approved = self._current_approved_revision(version.dataset_id, principal)
        stale = approved is None or approved.id != version.data_tools.content_revision_id
        return version.model_copy(
            update={
                "data_tools": version.data_tools.model_copy(update={"stale": stale}),
            }
        )

    def project_run_staleness(
        self,
        run: ProcessingRun,
        principal: WorkspaceServicePrincipal | None,
    ) -> ProcessingRun:
        """Compute stale status for a run derived from approved content."""

        approved = (
            self._current_approved_revision(run.dataset_id, principal)
            if run.content_revision_id is not None
            else None
        )
        stale = run.content_revision_id is not None and (
            approved is None or approved.id != run.content_revision_id
        )
        warnings = list(run.warnings)
        seen = {(item.code, item.message) for item in warnings}
        for stage in run.stages:
            if not stage.output:
                continue
            for raw in stage.output.get("warnings", []):
                if not isinstance(raw, dict):
                    continue
                code, message = raw.get("code"), raw.get("message")
                if (
                    isinstance(code, str)
                    and isinstance(message, str)
                    and (code, message) not in seen
                ):
                    warnings.append(ProcessingWarning(code=code, message=message))
                    seen.add((code, message))
        return run.model_copy(update={"stale": stale, "warnings": warnings})

    def _current_approved_revision(
        self,
        dataset_id: UUID,
        principal: WorkspaceServicePrincipal | None,
    ) -> ContentRevision | None:
        return next(
            (
                revision
                for revision in self.store.list_content_revisions(dataset_id, principal)
                if revision.state == ContentRevisionState.APPROVED
            ),
            None,
        )

    def _create_content_revision(
        self,
        *,
        dataset_id: UUID,
        source_revision_ids: list[UUID],
        parent_revision_id: UUID | None,
        blocks: list[ContentBlock],
        principal: WorkspaceServicePrincipal | None,
    ) -> ContentRevision:
        revisions = self.store.list_content_revisions(dataset_id, principal)
        revision = ContentRevision(
            id=uuid4(),
            dataset_id=dataset_id,
            revision=(revisions[0].revision + 1) if revisions else 1,
            parent_revision_id=parent_revision_id,
            source_revision_ids=source_revision_ids,
            blocks=blocks,
            state=ContentRevisionState.DRAFT,
            created_at=utc_now(),
            resource_version=1,
        )
        try:
            return self.store.create_content_revision(revision, principal)
        except LookupError as exc:
            raise _error(
                "CATALYST_DATASET_NOT_FOUND",
                "Dataset not found",
                "The requested Dataset is outside this workspace.",
                404,
            ) from exc

    def _preflight_binding(self, environment_name: str) -> None:
        """Validate local capability wiring before admitting a durable run."""

        connection_ref = os.environ.get(environment_name, "").strip()
        if not connection_ref:
            raise _error(
                "CATALYST_PLUGIN_NOT_CONFIGURED",
                "Processing capability is unavailable",
                f"Configure {environment_name} with the resolved local Plugin connection_ref.",
                503,
                retryable=True,
            )
        try:
            from cyrene_plugin_runtime import DirectPluginClient

            client = DirectPluginClient.for_local_connection_ref(connection_ref)
            client.close()
        except ImportError as exc:
            raise _error(
                "CATALYST_PLUGIN_RUNTIME_UNAVAILABLE",
                "Processing capability is unavailable",
                "cyrene-plugin-runtime is not installed in this Catalyst service.",
                503,
                retryable=True,
            ) from exc
        except (TypeError, ValueError) as exc:
            raise _error(
                "CATALYST_PLUGIN_BINDING_INVALID",
                "Processing capability is unavailable",
                f"{environment_name} is not a valid local connection_ref.",
                503,
                retryable=True,
            ) from exc

    def _invoke_plugin(
        self,
        *,
        capability: str,
        environment_name: str,
        method: str,
        request: dict[str, Any],
        cancel_event: Any,
        paid_outcome_unknown: bool = False,
    ) -> dict[str, Any]:
        """Invoke one typed Plugin method through its local DirectPluginClient."""

        connection_ref = os.environ.get(environment_name, "").strip()
        if not connection_ref:
            raise StageExecutionFailure(
                "CATALYST_PLUGIN_NOT_CONFIGURED",
                f"Configure {environment_name} with the resolved local Plugin connection_ref.",
                retryable=True,
                outcome_unknown=False,
            )
        try:
            from cyrene_plugin_runtime import (
                DirectPayload,
                DirectPluginClient,
                DirectPluginDeadlineExceeded,
                DirectPluginFailure,
                DirectPluginInvocationCancelled,
                DirectPluginTransportError,
            )

            client = DirectPluginClient.for_local_connection_ref(connection_ref)
        except ImportError as exc:
            raise StageExecutionFailure(
                "CATALYST_PLUGIN_RUNTIME_UNAVAILABLE",
                "cyrene-plugin-runtime is not installed in this Catalyst service.",
                retryable=True,
                outcome_unknown=False,
            ) from exc
        except (TypeError, ValueError) as exc:
            raise StageExecutionFailure(
                "CATALYST_PLUGIN_BINDING_INVALID",
                f"{environment_name} is not a valid local connection_ref.",
                retryable=True,
                outcome_unknown=False,
            ) from exc
        request_type = f"type.cyrene.io/{capability}.{method}.request"
        response_type = f"type.cyrene.io/{capability}.{method}.response"
        try:
            response = client.invoke(
                capability=capability,
                interface_version="1",
                method=method,
                request=DirectPayload(
                    type_url=request_type,
                    value=_canonical_json(request),
                ),
                deadline_seconds=600.0,
                cancel_event=cancel_event,
            )
        except DirectPluginInvocationCancelled as exc:
            raise StageExecutionFailure(
                "CATALYST_RUN_CANCELLED",
                "The Plugin invocation was cancelled.",
                retryable=False,
                outcome_unknown=False,
            ) from exc
        except DirectPluginFailure as exc:
            raise StageExecutionFailure(
                exc.domain_code or "CATALYST_PLUGIN_FAILED",
                f"The {capability} Plugin rejected the processing request.",
                retryable=exc.retryable,
                outcome_unknown=False,
            ) from exc
        except (DirectPluginDeadlineExceeded, DirectPluginTransportError) as exc:
            raise StageExecutionFailure(
                "CATALYST_PLUGIN_TRANSPORT_FAILED",
                f"The {capability} endpoint did not return a complete result.",
                retryable=not paid_outcome_unknown,
                outcome_unknown=paid_outcome_unknown,
            ) from exc
        finally:
            client.close()
        if response.type_url != response_type:
            raise StageExecutionFailure(
                "CATALYST_PLUGIN_RESPONSE_TYPE_INVALID",
                f"The {capability} endpoint returned an unexpected response type.",
                retryable=False,
                outcome_unknown=paid_outcome_unknown,
            )
        try:
            decoded = json.loads(response.value.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise StageExecutionFailure(
                "CATALYST_PLUGIN_RESPONSE_INVALID",
                f"The {capability} endpoint returned invalid JSON.",
                retryable=False,
                outcome_unknown=paid_outcome_unknown,
            ) from exc
        return _json_object(decoded, "Plugin response")

    @staticmethod
    def _run_input_digest(
        sources: list[SourceRevision],
        revision: ContentRevision | None,
    ) -> str:
        """Digest immutable source or content inputs for run cache identity."""

        parts: dict[str, Any] = {
            "sourceDigests": sorted(source.digest for source in sources),
            "contentRevisionId": str(revision.id) if revision else None,
        }
        if revision is not None:
            parts["blocksDigest"] = _digest_bytes(
                _canonical_json(
                    [
                        block.model_dump(by_alias=True, exclude_none=True)
                        for block in revision.blocks
                    ]
                )
            )
            if revision.training_data_snapshot is not None:
                parts["trainingDataDigest"] = revision.training_data_snapshot.artifact.digest
        return _digest_bytes(_canonical_json(parts))

    def _run_stage(
        self,
        run: ProcessingRun,
        stage: ProcessingStage,
        cancel_event: Event,
    ) -> dict[str, Any]:
        """Dispatch one durable run to the Plugin that owns its transformation."""

        try:
            if run.operation == ProcessingOperation.PARSE:
                return self._parse_sources(run, cancel_event)
            if run.operation == ProcessingOperation.CURATE_TRAINING_DATA:
                return self._curate_training_data(run, cancel_event)
            if run.operation == ProcessingOperation.BUILD_KNOWLEDGE:
                return self._build_knowledge(run, cancel_event)
            if run.operation == ProcessingOperation.PREPARE_SFT:
                return self._prepare_sft(run, cancel_event)
            if run.operation == ProcessingOperation.GENERATE_QA:
                return self._generate_qa(run, cancel_event)
        except CatalystError as exc:
            raise StageExecutionFailure(
                exc.code,
                exc.detail,
                retryable=exc.retryable,
                outcome_unknown=False,
            ) from exc
        raise StageExecutionFailure(
            "CATALYST_OPERATION_UNSUPPORTED",
            f"Unsupported data-tools operation {run.operation.value}.",
            retryable=False,
            outcome_unknown=False,
        )

    def _parse_sources(self, run: ProcessingRun, cancel_event: Any) -> dict[str, Any]:
        """Parse each selected source independently and persist its durable outcome."""

        prior_revisions = self.store.list_content_revisions_for_worker(run.dataset_id)
        previous = prior_revisions[0] if prior_revisions else None
        selected_ids = set(run.source_revision_ids)
        retained_blocks = (
            [block for block in previous.blocks if block.source_revision_id not in selected_ids]
            if previous
            else []
        )
        retained_source_ids = set(previous.source_revision_ids if previous else []) - selected_ids
        selected_sources = [
            self.store.get_source_for_worker(source_id) for source_id in run.source_revision_ids
        ]
        if any(source is None for source in selected_sources):
            raise StageExecutionFailure(
                "CATALYST_SOURCE_REVISION_NOT_FOUND",
                "One selected SourceRevision no longer exists.",
                retryable=False,
                outcome_unknown=False,
            )

        report_by_source = {
            report.source_revision_id: report
            for report in self.store.list_source_parse_reports_for_worker(run.dataset_id, run.id)
        }
        prior_report_by_source: dict[UUID, SourceParseReport] = {}
        if run.retry_of_run_id is not None:
            prior_report_by_source = {
                report.source_revision_id: report
                for report in self.store.list_source_parse_reports_for_worker(
                    run.dataset_id, run.retry_of_run_id
                )
            }

        # ── Phase 1: Resolve this run's report rows before doing file work. ──
        for source in selected_sources:
            assert source is not None
            report = report_by_source.get(source.id)
            if report is None:
                now = utc_now()
                report = self.store.save_source_parse_report_for_worker(
                    SourceParseReport(
                        id=uuid4(),
                        dataset_id=run.dataset_id,
                        source_revision_id=source.id,
                        processing_run_id=run.id,
                        status=SourceParseReportState.QUEUED,
                        created_at=now,
                        updated_at=now,
                    )
                )
            report = self.store.save_source_parse_report_for_worker(
                report.model_copy(
                    update={
                        "status": SourceParseReportState.RUNNING,
                        "started_at": report.started_at or utc_now(),
                        "finished_at": None,
                        "updated_at": utc_now(),
                        "failure": None,
                    }
                )
            )
            report_by_source[source.id] = report

        # ── Phase 2: Parse, snapshot, and persist every source independently. ──
        parsed_blocks: list[ContentBlock] = []
        successful_source_ids: set[UUID] = set()
        output_artifacts: list[ArtifactRef] = []
        warning_entries: list[dict[str, str]] = []
        parser_receipts: list[dict[str, Any]] = []
        structured_imports: list[dict[str, Any]] = []
        pending_review_reports: list[SourceParseReport] = []
        failed_count = 0
        retryable_failures = False
        cancelled = False

        for source in selected_sources:
            assert source is not None
            report = report_by_source[source.id]
            if cancel_event.is_set():
                cancelled = True
                self._finish_source_report(
                    report,
                    SourceParseReportState.CANCELLED,
                    failure=ProcessingFailure(
                        code="CATALYST_RUN_CANCELLED",
                        message="The parse run was cancelled before this source completed.",
                        retryable=False,
                    ),
                )
                continue

            try:
                warnings: list[dict[str, str]]
                reused = self._reuse_prior_source_output(
                    source,
                    prior_report_by_source.get(source.id),
                )
                if reused is not None:
                    source_blocks, prior_report = reused
                    report_artifacts = prior_report.output_artifacts
                    warnings = [
                        {"code": warning.code, "message": warning.message}
                        for warning in prior_report.warnings
                    ]
                    diagnostics = prior_report.diagnostics
                    unsupported_content = prior_report.unsupported_content
                    parser_receipts.append(
                        {
                            "sourceRevisionId": str(source.id),
                            "reusedFromParseReportId": str(prior_report.id),
                            "status": prior_report.status.value.lower(),
                            "blockCount": len(source_blocks),
                        }
                    )
                else:
                    (
                        source_blocks,
                        report_artifacts,
                        warnings,
                        diagnostics,
                        unsupported_content,
                        parse_receipt,
                    ) = self._parse_one_source(source, run, cancel_event)
                    parser_receipts.append(parse_receipt)
                    if (
                        parse_receipt.get("status") == "partial_success"
                        and not warnings
                        and not diagnostics
                        and not unsupported_content
                    ):
                        warnings = [
                            {
                                "code": "CATALYST_PARSER_PARTIAL_SUCCESS",
                                "message": "The Parser reported partial_success.",
                            }
                        ]

                normalized_artifact = self._publish_source_blocks_snapshot(source, source_blocks)
                if not any(
                    artifact.digest == normalized_artifact.digest for artifact in report_artifacts
                ):
                    report_artifacts = [*report_artifacts, normalized_artifact]
                output_artifacts.extend(report_artifacts)
                source_warnings = [
                    ProcessingWarning.model_validate(warning) for warning in warnings
                ]
                status = (
                    SourceParseReportState.WARNING
                    if source_warnings or diagnostics or unsupported_content
                    else SourceParseReportState.SUCCEEDED
                )
                final_report = self._source_report_candidate(
                    report,
                    status,
                    block_count=len(source_blocks),
                    warnings=source_warnings,
                    diagnostics=diagnostics,
                    unsupported_content=unsupported_content,
                    output_artifacts=report_artifacts,
                )
                successful_source_ids.add(source.id)
                parsed_blocks.extend(source_blocks)
                warning_entries.extend(
                    warning.model_dump(by_alias=True, exclude_none=True)
                    for warning in source_warnings
                )
                pending_review_reports.append(final_report)
                if source.media_type == "application/x-ndjson":
                    structured_imports.append(
                        {
                            "sourceRevisionId": str(source.id),
                            "format": "CYRENE_STRUCTURED_SFT_JSONL_V1",
                            "rowCount": len(source_blocks),
                            "sourceArtifact": source.artifact.model_dump(
                                by_alias=True, exclude_none=True
                            ),
                        }
                    )
            except StageExecutionFailure as exc:
                failed_count += 1
                is_cancelled = exc.code == "CATALYST_RUN_CANCELLED" or cancel_event.is_set()
                cancelled = cancelled or is_cancelled
                failure = ProcessingFailure(
                    code=self._stable_failure_code(exc.code),
                    message=(
                        "The source could not be parsed."
                        if exc.code == "CATALYST_SOURCE_PARSE_FAILED"
                        else str(exc)[:2000]
                    ),
                    retryable=exc.retryable,
                )
                retryable_failures = retryable_failures or failure.retryable
                failed_report = self._finish_source_report(
                    report,
                    SourceParseReportState.CANCELLED
                    if is_cancelled
                    else SourceParseReportState.FAILED,
                    failure=failure,
                )
                self._save_report_review_item(
                    failed_report,
                    kind=(
                        ReviewItemKind.UNSUPPORTED_SOURCE
                        if failure.code == "CATALYST_SOURCE_UNSUPPORTED"
                        else ReviewItemKind.PARSE_FAILURE
                    ),
                    code=failure.code,
                    message=failure.message,
                    severity="error",
                    content_revision_id=None,
                    index=0,
                )
            except CatalystError as exc:
                failed_count += 1
                failure = ProcessingFailure(
                    code=self._stable_failure_code(exc.code),
                    message=exc.detail[:2000],
                    retryable=exc.retryable,
                )
                retryable_failures = retryable_failures or failure.retryable
                failed_report = self._finish_source_report(
                    report,
                    SourceParseReportState.FAILED,
                    failure=failure,
                )
                self._save_report_review_item(
                    failed_report,
                    kind=ReviewItemKind.PARSE_FAILURE,
                    code=failure.code,
                    message=failure.message,
                    severity="error",
                    content_revision_id=None,
                    index=0,
                )
            except (OSError, UnicodeError, ValueError) as exc:
                failed_count += 1
                emit_diagnostic_error(
                    "catalyst.source_parse",
                    "CATALYST_SOURCE_PARSE_FAILED",
                    "A source-specific parse step failed; remaining sources will continue.",
                    attributes={
                        "source_revision_id": str(source.id),
                        "error_type": type(exc).__name__,
                    },
                )
                failure = ProcessingFailure(
                    code="CATALYST_SOURCE_PARSE_FAILED",
                    message="The source could not be parsed.",
                    retryable=False,
                )
                failed_report = self._finish_source_report(
                    report,
                    SourceParseReportState.FAILED,
                    failure=failure,
                )
                self._save_report_review_item(
                    failed_report,
                    kind=ReviewItemKind.PARSE_FAILURE,
                    code=failure.code,
                    message=failure.message,
                    severity="error",
                    content_revision_id=None,
                    index=0,
                )

        if cancelled:
            # Persist finished source receipts even when the batch is cancelled; the
            # batch must not strand successful per-source work in RUNNING.
            for report in pending_review_reports:
                saved_report = self.store.save_source_parse_report_for_worker(report)
                self._save_report_review_items(saved_report)
            raise StageExecutionFailure(
                "CATALYST_RUN_CANCELLED",
                "The parse run was cancelled.",
                retryable=False,
                outcome_unknown=False,
            )
        if not successful_source_ids:
            raise StageExecutionFailure(
                "CATALYST_SOURCE_PARSE_FAILED",
                "No selected source could be parsed.",
                retryable=retryable_failures,
                outcome_unknown=False,
            )

        # ── Phase 3: Atomically publish the revision and its review gate. ──
        content_revision: ContentRevision | None = None
        if parsed_blocks:
            content_revision = self._build_worker_revision(
                dataset_id=run.dataset_id,
                source_revision_ids=sorted(
                    retained_source_ids | successful_source_ids,
                    key=str,
                ),
                parent_revision_id=previous.id if previous else None,
                blocks=[*retained_blocks, *parsed_blocks],
            )
            linked_reports = [
                report.model_copy(
                    update={
                        "content_revision_id": content_revision.id,
                        "updated_at": utc_now(),
                    }
                )
                for report in pending_review_reports
            ]
            review_items = [
                item for report in linked_reports for item in self._report_review_items(report)
            ]
            try:
                content_revision = self.store.finalize_parsed_content_revision_for_worker(
                    content_revision,
                    linked_reports,
                    review_items,
                )
            except (LookupError, ValueError) as exc:
                raise StageExecutionFailure(
                    "CATALYST_CONTENT_REVISION_CREATE_FAILED",
                    "The Product could not atomically persist the parser output and review gate.",
                    retryable=False,
                    outcome_unknown=False,
                ) from exc
        else:
            for report in pending_review_reports:
                saved_report = self.store.save_source_parse_report_for_worker(report)
                self._save_report_review_items(saved_report)

        return {
            "outputArtifacts": [
                artifact.model_dump(by_alias=True, exclude_none=True)
                for artifact in output_artifacts
            ],
            "contentRevisionId": (
                str(content_revision.id) if content_revision is not None else None
            ),
            "blockCount": len(content_revision.blocks) if content_revision else 0,
            "successfulSourceRevisionIds": [
                str(value) for value in sorted(successful_source_ids, key=str)
            ],
            "failedSourceCount": failed_count,
            "parserReceipts": parser_receipts,
            "structuredImports": structured_imports,
            "warnings": warning_entries,
        }

    def _curate_training_data(self, run: ProcessingRun, cancel_event: Any) -> dict[str, Any]:
        """Stream mixed structured sources through the preparation Plugin."""

        config = run.recipe.get("config", {})
        curation = config.get("curation", {})
        source_policy_values = config.get("sourcePolicies", {})
        sources = self._worker_sources(run.source_revision_ids)
        source_by_id = {source.id: source for source in sources}
        selected_ids = set(source_by_id)
        previous = (
            self.store.get_content_revision_for_worker(run.content_revision_id)
            if run.content_revision_id is not None
            else None
        )
        retained_source_ids: set[UUID] = set()
        if previous is not None and previous.training_data_snapshot is not None:
            retained_source_ids = set(previous.source_revision_ids) - selected_ids

        report_by_source = {
            report.source_revision_id: report
            for report in self.store.list_source_parse_reports_for_worker(run.dataset_id, run.id)
        }
        all_report_source_ids = selected_ids | retained_source_ids
        for source_id in sorted(all_report_source_ids, key=str):
            report = report_by_source.get(source_id)
            if report is None:
                now = utc_now()
                report = SourceParseReport(
                    id=uuid5(_REVIEW_NAMESPACE, f"curation-report:{run.id}:{source_id}"),
                    dataset_id=run.dataset_id,
                    source_revision_id=source_id,
                    processing_run_id=run.id,
                    status=SourceParseReportState.QUEUED,
                    created_at=now,
                    updated_at=now,
                )
            report = self.store.save_source_parse_report_for_worker(
                report.model_copy(
                    update={
                        "status": SourceParseReportState.RUNNING,
                        "started_at": report.started_at or utc_now(),
                        "finished_at": None,
                        "updated_at": utc_now(),
                        "failure": None,
                    }
                )
            )
            report_by_source[source_id] = report

        def fail_open_reports(code: str, message: str, retryable: bool) -> None:
            """Make every unfinished source receipt terminal when the run fails."""

            for source_id, current_report in list(report_by_source.items()):
                if current_report.status != SourceParseReportState.RUNNING:
                    continue
                failed = current_report.model_copy(
                    update={
                        "status": SourceParseReportState.FAILED,
                        "failure": ProcessingFailure(
                            code=code,
                            message=message[:2000],
                            retryable=retryable,
                        ),
                        "finished_at": utc_now(),
                        "updated_at": utc_now(),
                    }
                )
                self.store.save_source_parse_report_for_worker(failed)
                report_by_source[source_id] = failed

        if cancel_event.is_set():
            fail_open_reports(
                "CATALYST_RUN_CANCELLED",
                "The training-data curation run was cancelled before Plugin invocation.",
                False,
            )
            raise StageExecutionFailure(
                "CATALYST_RUN_CANCELLED",
                "The training-data curation run was cancelled before Plugin invocation.",
                retryable=False,
                outcome_unknown=False,
            )

        checkpoint_key = hashlib.sha256(
            _canonical_json(
                {
                    "datasetId": str(run.dataset_id),
                    "recipeDigest": run.recipe_digest,
                    "sources": sorted(
                        (
                            str(source.id),
                            str(source.source_id),
                            source.digest,
                        )
                        for source in sources
                    ),
                }
            )
        ).hexdigest()
        # A retry is a new ProcessingRun with a new id. Keep the output path
        # stable for this exact dataset/recipe/source set so the Plugin can
        # validate and resume its durable checkpoint instead of truncating it.
        result_path = self.artifacts.stage_path(f"curation-{checkpoint_key}.training-records.jsonl")
        checkpoint_path = self.artifacts.stage_path(f"curation-{checkpoint_key}.checkpoint.json")
        preserve_checkpoint_output = True
        source_requests: list[dict[str, Any]] = []
        for source in sources:
            configured_policy = source_policy_values.get(str(source.id), {})
            policy = ContentPolicy.model_validate(configured_policy)
            source_requests.append(
                {
                    "source_path": str(self.artifacts.resolve(source.artifact).resolve()),
                    "source_revision_id": str(source.id),
                    "source_family_id": str(source.source_id),
                    "filename": source.filename,
                    "policy": policy.model_dump(by_alias=True, exclude_none=True),
                }
            )

        plugin_recipe = {
            "id": curation["id"],
            "version": curation["version"],
            "format": curation["format"],
            "fieldMapping": curation.get("fieldMapping", {}),
            "roleMapping": curation.get("roleMapping", {}),
            "maxCharacters": curation.get("maxCharacters", 100_000),
            "minCharacters": curation.get("minCharacters", 2),
            "unicodeNormalization": curation.get("unicodeNormalization", "NFC"),
        }
        try:
            response = self._invoke_plugin(
                capability="dataset.preparation.v1",
                environment_name=DATASET_PREPARATION_CONNECTION_ENV,
                method="curate_training_records",
                request={
                    "sources": source_requests,
                    "result_path": str(result_path.resolve()),
                    "checkpoint_path": str(checkpoint_path.resolve()),
                    "recipe_digest": run.recipe_digest,
                    "recipe": plugin_recipe,
                },
                cancel_event=cancel_event,
            )
            if (
                response.get("result_path") != str(result_path.resolve())
                or response.get("schema_version") != "cyrene.training-record.v1"
                or response.get("recipe_digest") != run.recipe_digest
            ):
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_RECEIPT_INVALID",
                    "The curation Plugin receipt does not match this run.",
                    retryable=False,
                    outcome_unknown=False,
                )
            _verify_file_receipt(
                result_path,
                response.get("digest"),
                response.get("size_bytes"),
                "training records",
            )
            source_receipts = response.get("sources")
            if not isinstance(source_receipts, list) or len(source_receipts) != len(sources):
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_RECEIPT_INVALID",
                    "The curation Plugin must return one receipt per selected source.",
                    retryable=False,
                    outcome_unknown=False,
                )
            receipt_by_source: dict[UUID, dict[str, Any]] = {}
            for receipt in source_receipts:
                if not isinstance(receipt, dict):
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_RECEIPT_INVALID",
                        "A curation source receipt is invalid.",
                        retryable=False,
                        outcome_unknown=False,
                    )
                try:
                    source_id = UUID(str(receipt.get("source_revision_id")))
                except ValueError as exc:
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_RECEIPT_INVALID",
                        "A curation source receipt has an invalid SourceRevision id.",
                        retryable=False,
                        outcome_unknown=False,
                    ) from exc
                if source_id not in selected_ids or source_id in receipt_by_source:
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_RECEIPT_INVALID",
                        "The curation Plugin returned a duplicate or foreign source receipt.",
                        retryable=False,
                        outcome_unknown=False,
                    )
                if receipt.get("status") not in {
                    "SUCCEEDED",
                    "SUCCEEDED_WITH_WARNINGS",
                    "FAILED",
                }:
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_RECEIPT_INVALID",
                        "A curation source receipt has an invalid terminal status.",
                        retryable=False,
                        outcome_unknown=False,
                    )
                self._validated_histogram(receipt.get("diagnostic_counts", {}), "diagnostic_counts")
                self._validated_histogram(
                    receipt.get("unsupported_counts", {}), "unsupported_counts"
                )
                receipt_by_source[source_id] = receipt

            merged_path = self.artifacts.stage_path(f"{run.id}.training-records-merged.jsonl")
            previous_path: Path | None = None
            if (
                previous is not None
                and previous.training_data_snapshot is not None
                and retained_source_ids
            ):
                previous_path = self.artifacts.resolve(previous.training_data_snapshot.artifact)
            selected_success_ids = {
                source_id
                for source_id, receipt in receipt_by_source.items()
                if receipt.get("status") in {"SUCCEEDED", "WARNING", "SUCCEEDED_WITH_WARNINGS"}
            }
            selected_failed_ids = selected_ids - selected_success_ids
            emitted_rows = any(
                receipt.get("counts", {}).get("total", 0) > 0
                for receipt in receipt_by_source.values()
            )
            if not emitted_rows and not retained_source_ids:
                raise StageExecutionFailure(
                    "CATALYST_TRAINING_DATA_EMPTY",
                    "No source produced a usable training-record snapshot.",
                    retryable=False,
                    outcome_unknown=False,
                )

            if previous_path is not None:
                merged_path = self.artifacts.stage_path(f"{run.id}.training-records-merged.jsonl")
                selected_id_text = {str(source_id) for source_id in selected_ids}
                with merged_path.open("wb") as output:
                    if previous_path is not None:
                        for envelope in self._iter_training_record_envelopes(previous_path):
                            if str(envelope.get("sourceRevisionId")) not in selected_id_text:
                                output.write(_canonical_json(envelope))
                                output.write(b"\n")
                    for envelope in self._iter_training_record_envelopes(result_path):
                        if str(envelope.get("sourceRevisionId")) in selected_id_text:
                            output.write(_canonical_json(envelope))
                            output.write(b"\n")
            else:
                merged_path = result_path

            if merged_path.stat().st_size > MAX_RESULT_BYTES:
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_OUTPUT_TOO_LARGE",
                    "The normalized training snapshot exceeds Catalyst's supported artifact size.",
                    retryable=False,
                    outcome_unknown=False,
                )

            # Validate each row and reconcile dispositions from the actual artifact.
            actual_counts = {
                "total": 0,
                "recognized": 0,
                "formatErrors": 0,
                "duplicateCandidates": 0,
                "pendingReview": 0,
                "excluded": 0,
                "eligible": 0,
            }
            source_counts: dict[UUID, dict[str, int]] = {
                source_id: {
                    "total": 0,
                    "recognized": 0,
                    "formatErrors": 0,
                    "duplicateCandidates": 0,
                    "pendingReview": 0,
                    "excluded": 0,
                    "eligible": 0,
                    "reviewItems": 0,
                }
                for source_id in all_report_source_ids
            }
            diagnostic_counts: dict[UUID, dict[str, int]] = {
                source_id: {} for source_id in source_counts
            }
            unsupported_counts: dict[UUID, dict[str, int]] = {
                source_id: {} for source_id in source_counts
            }
            selected_actual: dict[UUID, dict[str, int]] = {
                source_id: {
                    "total": 0,
                    "recognized": 0,
                    "formatErrors": 0,
                    "duplicateCandidates": 0,
                    "pendingReview": 0,
                    "excluded": 0,
                    "eligible": 0,
                }
                for source_id in selected_ids
            }
            for envelope in self._iter_training_record_envelopes(merged_path):
                source_text = envelope.get("sourceRevisionId")
                try:
                    source_id = UUID(str(source_text))
                except ValueError as exc:
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_OUTPUT_INVALID",
                        "A training record has an invalid source revision id.",
                        retryable=False,
                        outcome_unknown=False,
                    ) from exc
                if source_id not in source_counts:
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_OUTPUT_INVALID",
                        "A training record references a source outside this curation run.",
                        retryable=False,
                        outcome_unknown=False,
                    )
                disposition = envelope.get("disposition")
                if disposition not in {"eligible", "review", "excluded"}:
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_OUTPUT_INVALID",
                        "A training record has an invalid disposition.",
                        retryable=False,
                        outcome_unknown=False,
                    )
                normalized = envelope.get("normalized")
                if disposition == "eligible" and not isinstance(normalized, dict):
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_OUTPUT_INVALID",
                        "A record without normalized content cannot be eligible for training.",
                        retryable=False,
                        outcome_unknown=False,
                    )
                allowed_by_policy = self._training_policy_allows(envelope.get("policy"))
                if self._training_record_explicitly_denied(envelope.get("rawRecord")):
                    allowed_by_policy = False
                if disposition == "eligible" and not allowed_by_policy:
                    raise StageExecutionFailure(
                        "CATALYST_TRAINING_POLICY_VIOLATION",
                        "A policy-disallowed training record was marked eligible by the Plugin.",
                        retryable=False,
                        outcome_unknown=False,
                    )
                if disposition != "excluded" and not allowed_by_policy:
                    raise StageExecutionFailure(
                        "CATALYST_TRAINING_POLICY_VIOLATION",
                        "A policy-disallowed record must be excluded before human approval.",
                        retryable=False,
                        outcome_unknown=False,
                    )
                issues = envelope.get("issues", [])
                if not isinstance(issues, list):
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_OUTPUT_INVALID",
                        "Training record issues must be a list.",
                        retryable=False,
                        outcome_unknown=False,
                    )
                if disposition == "eligible" and any(
                    isinstance(issue, dict)
                    and (
                        issue.get("severity") == "error"
                        or self._training_issue_requires_exclusion(issue)
                    )
                    for issue in issues
                ):
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_OUTPUT_INVALID",
                        "A record with unresolved or unrepresentable issues cannot be eligible.",
                        retryable=False,
                        outcome_unknown=False,
                    )
                if disposition == "review" and not issues:
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_OUTPUT_INVALID",
                        "A pending-review training record must retain a specific issue.",
                        retryable=False,
                        outcome_unknown=False,
                    )
                row_counts = source_counts[source_id]
                row_counts["total"] += 1
                actual_counts["total"] += 1
                detected_format = envelope.get("detectedFormat")
                if detected_format not in {None, "unknown"}:
                    row_counts["recognized"] += 1
                    actual_counts["recognized"] += 1
                row_counts[disposition if disposition != "review" else "pendingReview"] += 1
                actual_counts[disposition if disposition != "review" else "pendingReview"] += 1
                if source_id in selected_actual:
                    selected_actual[source_id]["total"] += 1
                    selected_actual[source_id][
                        disposition if disposition != "review" else "pendingReview"
                    ] += 1
                    if detected_format not in {None, "unknown"}:
                        selected_actual[source_id]["recognized"] += 1
                for issue in issues:
                    if not isinstance(issue, dict):
                        raise StageExecutionFailure(
                            "CATALYST_PLUGIN_OUTPUT_INVALID",
                            "A training record issue is invalid.",
                            retryable=False,
                            outcome_unknown=False,
                        )
                    code = issue.get("code")
                    message = issue.get("message")
                    severity = issue.get("severity", "warning")
                    if not isinstance(code, str) or not code or not isinstance(message, str):
                        raise StageExecutionFailure(
                            "CATALYST_PLUGIN_OUTPUT_INVALID",
                            "A training record issue needs a code and message.",
                            retryable=False,
                            outcome_unknown=False,
                        )
                    if severity not in {"warning", "error"}:
                        raise StageExecutionFailure(
                            "CATALYST_PLUGIN_OUTPUT_INVALID",
                            "A training issue severity must be warning or error.",
                            retryable=False,
                            outcome_unknown=False,
                        )
                    row_counts["reviewItems"] += 1
                    if code.startswith("UNSUPPORTED_"):
                        unsupported = unsupported_counts[source_id]
                        unsupported[code] = unsupported.get(code, 0) + 1
                for code in {str(issue["code"]) for issue in issues}:
                    bucket = diagnostic_counts[source_id]
                    bucket[code] = bucket.get(code, 0) + 1
                if any(issue.get("code") == "DUPLICATE_EXACT" for issue in issues):
                    row_counts["duplicateCandidates"] += 1
                    actual_counts["duplicateCandidates"] += 1
                    if source_id in selected_actual:
                        selected_actual[source_id]["duplicateCandidates"] += 1
                if any(
                    issue.get("code")
                    in {
                        "INVALID_JSON",
                        "training.invalid_json",
                        "INVALID_ENCODING",
                        "FORMAT_UNRECOGNIZED",
                        "MESSAGE_STRUCTURE_INVALID",
                        "FIELD_MISSING",
                        "FIELD_TYPE_INVALID",
                    }
                    for issue in issues
                ):
                    row_counts["formatErrors"] += 1
                    actual_counts["formatErrors"] += 1
                    if source_id in selected_actual:
                        selected_actual[source_id]["formatErrors"] += 1

            aggregate_receipt_counts: dict[str, int] = {
                key: 0
                for key in (
                    "total",
                    "recognized",
                    "formatErrors",
                    "duplicateCandidates",
                    "pendingReview",
                    "excluded",
                    "eligible",
                )
            }
            for _, receipt in receipt_by_source.items():
                reported = receipt.get("counts")
                if not isinstance(reported, dict):
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_RECEIPT_INVALID",
                        "A curation source receipt has no count ledger.",
                        retryable=False,
                        outcome_unknown=False,
                    )
                for key in aggregate_receipt_counts:
                    value = reported.get(key)
                    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                        raise StageExecutionFailure(
                            "CATALYST_PLUGIN_RECEIPT_INVALID",
                            "A curation source count is invalid.",
                            retryable=False,
                            outcome_unknown=False,
                        )
                    aggregate_receipt_counts[key] += value

            for source_id, actual in selected_actual.items():
                reported = receipt_by_source[source_id].get("counts")
                if not isinstance(reported, dict):
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_RECEIPT_INVALID",
                        "A curation source receipt has no count ledger.",
                        retryable=False,
                        outcome_unknown=False,
                    )
                for key in (
                    "total",
                    "recognized",
                    "formatErrors",
                    "duplicateCandidates",
                    "eligible",
                    "pendingReview",
                    "excluded",
                ):
                    if reported.get(key) != actual[key]:
                        raise StageExecutionFailure(
                            "CATALYST_PLUGIN_RECEIPT_INVALID",
                            "Curation source counts do not match its normalized record envelopes.",
                            retryable=False,
                            outcome_unknown=False,
                        )

                if reported["total"]:
                    diagnostic_receipt = self._validated_histogram(
                        receipt_by_source[source_id].get("diagnostic_counts", {}),
                        "diagnostic_counts",
                    )
                    if diagnostic_receipt != diagnostic_counts[source_id]:
                        raise StageExecutionFailure(
                            "CATALYST_PLUGIN_RECEIPT_INVALID",
                            "Curation diagnostic counts do not match the record envelopes.",
                            retryable=False,
                            outcome_unknown=False,
                        )
                    unsupported_receipt = self._validated_histogram(
                        receipt_by_source[source_id].get("unsupported_counts", {}),
                        "unsupported_counts",
                    )
                    if unsupported_receipt != unsupported_counts[source_id]:
                        raise StageExecutionFailure(
                            "CATALYST_PLUGIN_RECEIPT_INVALID",
                            "Curation unsupported-content counts do not match the "
                            "record envelopes.",
                            retryable=False,
                            outcome_unknown=False,
                        )

            reported_counts = response.get("counts")
            if not isinstance(reported_counts, dict):
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_RECEIPT_INVALID",
                    "The curation receipt has no count ledger.",
                    retryable=False,
                    outcome_unknown=False,
                )
            for key in (
                "total",
                "recognized",
                "formatErrors",
                "duplicateCandidates",
                "pendingReview",
                "excluded",
                "eligible",
            ):
                if reported_counts.get(key) != aggregate_receipt_counts[key]:
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_RECEIPT_INVALID",
                        "Curation aggregate counts do not match the normalized record envelopes.",
                        retryable=False,
                        outcome_unknown=False,
                    )

            if not actual_counts["total"]:
                raise StageExecutionFailure(
                    "CATALYST_TRAINING_DATA_EMPTY",
                    "The curation Plugin produced no record envelopes.",
                    retryable=False,
                    outcome_unknown=False,
                )
            if (
                actual_counts["eligible"]
                + actual_counts["pendingReview"]
                + actual_counts["excluded"]
                != actual_counts["total"]
            ):
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_OUTPUT_INVALID",
                    "Training-record dispositions do not reconcile to the artifact total.",
                    retryable=False,
                    outcome_unknown=False,
                )
            snapshot_ref = self.artifacts.publish(merged_path, "source-parse-blocks")
            if merged_path != result_path:
                merged_path.unlink(missing_ok=True)
            counts = TrainingCurationCounts.model_validate(actual_counts)
            snapshot_records = TrainingDataSnapshot(
                schema_version="cyrene.training-record.v1",
                artifact=snapshot_ref,
                record_count=actual_counts["total"],
                counts=counts,
            )
            final_source_ids = sorted(
                (
                    source_id
                    for source_id, source_count in source_counts.items()
                    if source_count["total"] > 0
                ),
                key=str,
            )
            revision = ContentRevision(
                id=uuid4(),
                dataset_id=run.dataset_id,
                revision=(previous.revision + 1) if previous is not None else 1,
                parent_revision_id=previous.id if previous is not None else None,
                source_revision_ids=final_source_ids,
                training_data_snapshot=snapshot_records,
                state=ContentRevisionState.DRAFT,
                created_at=utc_now(),
                resource_version=1,
            )
            terminal_reports: list[SourceParseReport] = []
            for source_id in selected_failed_ids:
                if source_counts[source_id]["total"] > 0:
                    continue
                receipt = receipt_by_source[source_id]
                diagnostic_histogram = self._validated_histogram(
                    receipt.get("diagnostic_counts", {}), "diagnostic_counts"
                )
                unsupported_histogram = self._validated_histogram(
                    receipt.get("unsupported_counts", {}), "unsupported_counts"
                )
                failed = report_by_source[source_id].model_copy(
                    update={
                        "status": SourceParseReportState.FAILED,
                        "block_count": 0,
                        "diagnostics": self._histogram_documents(
                            diagnostic_histogram, "diagnostic"
                        ),
                        "unsupported_content": self._histogram_documents(
                            unsupported_histogram, "unsupported"
                        ),
                        "diagnostic_counts": diagnostic_histogram,
                        "output_artifacts": [source_by_id[source_id].artifact],
                        "failure": ProcessingFailure(
                            code="CATALYST_TRAINING_SOURCE_FAILED",
                            message=str(
                                receipt.get("failure")
                                or "The source could not be normalized as training records."
                            )[:2000],
                            retryable=False,
                        ),
                        "finished_at": utc_now(),
                        "updated_at": utc_now(),
                    }
                )
                self.store.save_source_parse_report_for_worker(failed)
                report_by_source[source_id] = failed
            for source_id, report in report_by_source.items():
                source_count = source_counts.get(source_id)
                if source_count is None or source_count["total"] == 0:
                    continue
                has_issues = source_count["reviewItems"] > 0
                receipt = receipt_by_source.get(source_id)
                failed_source = receipt is not None and receipt.get("status") == "FAILED"
                failure = None
                if failed_source:
                    failure_message = receipt.get("failure") if receipt is not None else None
                    failure = ProcessingFailure(
                        code="CATALYST_TRAINING_SOURCE_FAILED",
                        message=str(
                            failure_message
                            or "The source could not be normalized as training records."
                        )[:2000],
                        retryable=False,
                    )
                terminal_reports.append(
                    report.model_copy(
                        update={
                            "status": (
                                SourceParseReportState.FAILED
                                if failed_source
                                else SourceParseReportState.WARNING
                                if has_issues
                                else SourceParseReportState.SUCCEEDED
                            ),
                            "content_revision_id": revision.id,
                            "block_count": source_count["total"],
                            "warnings": [],
                            "diagnostics": self._histogram_documents(
                                diagnostic_counts[source_id], "diagnostic"
                            ),
                            "unsupported_content": self._histogram_documents(
                                unsupported_counts[source_id],
                                "unsupported",
                            ),
                            "review_item_count": source_count["reviewItems"],
                            "diagnostic_counts": diagnostic_counts[source_id],
                            "output_artifacts": [snapshot_ref],
                            "finished_at": utc_now(),
                            "updated_at": utc_now(),
                            "failure": failure,
                        }
                    )
                )

            def review_items() -> Any:
                for envelope in self._iter_training_record_envelopes(
                    merged_path if merged_path.exists() else self.artifacts.resolve(snapshot_ref)
                ):
                    try:
                        source_id = UUID(str(envelope["sourceRevisionId"]))
                    except (KeyError, ValueError) as exc:
                        raise ValueError(
                            "Training record lost its source lineage during finalization."
                        ) from exc
                    report = next(
                        (item for item in terminal_reports if item.source_revision_id == source_id),
                        None,
                    )
                    if report is None:
                        continue
                    record_id = envelope.get("id")
                    if not isinstance(record_id, str) or not record_id:
                        raise ValueError("Training record id is required for Review Queue linkage.")
                    for index, issue in enumerate(envelope.get("issues", [])):
                        code = str(issue["code"])
                        severity = str(issue.get("severity", "warning"))
                        yield ReviewItem(
                            id=uuid5(
                                _REVIEW_NAMESPACE,
                                f"{run.id}:{record_id}:{code}:{index}",
                            ),
                            dataset_id=run.dataset_id,
                            source_parse_report_id=report.id,
                            source_revision_id=source_id,
                            processing_run_id=run.id,
                            content_revision_id=revision.id,
                            kind=self._training_review_kind(code),
                            code=code,
                            message=str(issue["message"])[:2000],
                            severity=severity,
                            locator=self._training_record_locator(envelope.get("locator")),
                            record_id=record_id,
                        )

            try:
                saved = self.store.finalize_training_content_revision_for_worker(
                    revision,
                    terminal_reports,
                    review_items(),
                )
            except (LookupError, ValueError) as exc:
                raise StageExecutionFailure(
                    "CATALYST_TRAINING_REVISION_CREATE_FAILED",
                    "The Product could not atomically save the training snapshot and Review Queue.",
                    retryable=False,
                    outcome_unknown=False,
                ) from exc
            preserve_checkpoint_output = False
            return {
                "contentRevisionId": str(saved.id),
                "trainingDataSnapshot": {
                    "artifact": snapshot_ref.model_dump(by_alias=True, exclude_none=True),
                    "recordCount": counts.total,
                    "counts": counts.model_dump(by_alias=True, exclude_none=True),
                },
                "successfulSourceRevisionIds": [
                    str(source_id)
                    for source_id in final_source_ids
                    if source_id not in selected_failed_ids
                ],
                "failedSourceRevisionIds": [
                    str(source_id)
                    for source_id in selected_failed_ids
                    if source_counts[source_id]["total"] > 0
                ],
                "failedSourceCount": len(selected_failed_ids),
                "outputArtifacts": [snapshot_ref.model_dump(by_alias=True, exclude_none=True)],
                "pluginReceipt": {
                    key: value for key, value in response.items() if key != "sources"
                },
            }
        except StageExecutionFailure as exc:
            fail_open_reports(exc.code, str(exc), exc.retryable)
            raise
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError, KeyError) as exc:
            fail_open_reports(
                "CATALYST_TRAINING_CURATION_FAILED",
                "The training data could not be safely normalized and persisted.",
                False,
            )
            raise StageExecutionFailure(
                "CATALYST_TRAINING_CURATION_FAILED",
                "The training data could not be safely normalized and persisted.",
                retryable=False,
                outcome_unknown=False,
            ) from exc
        finally:
            if not preserve_checkpoint_output:
                result_path.unlink(missing_ok=True)
                checkpoint_path.unlink(missing_ok=True)
                checkpoint_path.with_name(checkpoint_path.name + ".dedup.sqlite3").unlink(
                    missing_ok=True
                )
            if "merged_path" in locals() and merged_path != result_path:
                merged_path.unlink(missing_ok=True)

    @staticmethod
    def _training_review_kind(code: str) -> ReviewItemKind:
        """Map stable Plugin issue codes into the existing review kind enum."""

        normalized = code.upper()
        if "DUPLICATE" in normalized:
            return ReviewItemKind.TRAINING_DUPLICATE
        if "LEAK" in normalized:
            return ReviewItemKind.TRAINING_LEAKAGE
        if any(value in normalized for value in ("UNSUPPORTED", "TOOL", "MULTIMODAL", "ROLE")):
            return ReviewItemKind.TRAINING_UNSUPPORTED
        if any(value in normalized for value in ("QUALITY", "SHORT", "LONG", "ORDER")):
            return ReviewItemKind.TRAINING_QUALITY
        return ReviewItemKind.TRAINING_STRUCTURE

    @staticmethod
    def _validated_histogram(value: Any, name: str) -> dict[str, int]:
        """Validate a bounded Plugin diagnostic histogram before persistence."""

        if not isinstance(value, dict) or len(value) > 1000:
            raise ValueError(f"{name} must be a bounded diagnostic histogram.")
        output: dict[str, int] = {}
        for code, count in value.items():
            if (
                not isinstance(code, str)
                or not code
                or not isinstance(count, int)
                or isinstance(count, bool)
                or count < 0
            ):
                raise ValueError(f"{name} contains an invalid code or count.")
            if count:
                output[code] = count
        return output

    @classmethod
    def _histogram_documents(cls, value: Any, name: str) -> list[dict[str, Any]]:
        """Turn compact Plugin histograms into persisted diagnostic receipts."""

        histogram = cls._validated_histogram(value, f"{name}_counts")
        return [{"code": code, "count": count} for code, count in sorted(histogram.items())]

    @staticmethod
    def _training_record_locator(value: Any) -> ContentLocator | None:
        """Project a plugin locator into the existing ReviewItem locator."""

        if not isinstance(value, dict):
            return None
        item_ref = value.get("itemRef")
        if not isinstance(item_ref, str):
            return None
        return ContentLocator(item_ref=item_ref)

    @staticmethod
    def _training_policy_allows(value: Any) -> bool:
        """Require an explicit training grant on a normalized record."""

        if not isinstance(value, dict):
            return False
        purposes = value.get("allowedUsePurposes", value.get("allowed_use_purposes", []))
        return (
            value.get("allowTraining", value.get("allow_training")) is True
            and isinstance(purposes, list)
            and "model_training" in purposes
        )

    @staticmethod
    def _training_record_explicitly_denied(value: Any) -> bool:
        """Honor per-record policy fields that narrow the inherited source grant."""

        if not isinstance(value, dict):
            return False
        candidate = value.get("policy")
        policy = candidate if isinstance(candidate, dict) else value
        if policy.get("allowTraining", policy.get("allow_training")) is False:
            return True
        if policy.get("prohibited") is True or policy.get("banned") is True:
            return True
        purposes = policy.get("allowedUsePurposes", policy.get("allowed_use_purposes"))
        return isinstance(purposes, list) and "model_training" not in purposes

    @staticmethod
    def _iter_training_record_envelopes(path: Path) -> Any:
        """Yield validated JSONL envelopes with one bounded line in memory."""

        if path.stat().st_size > MAX_RESULT_BYTES:
            raise ValueError("Training record snapshot exceeds Catalyst's artifact size limit.")
        with path.open("rb") as stream:
            while True:
                line = stream.readline(MAX_TRAINING_ROW_BYTES + 1)
                if not line:
                    return
                if len(line) > MAX_TRAINING_ROW_BYTES:
                    raise ValueError("Training record envelope exceeds the line size limit.")
                try:
                    value = json.loads(line)
                except (UnicodeError, json.JSONDecodeError) as exc:
                    raise ValueError("The curation Plugin produced invalid JSONL.") from exc
                if (
                    not isinstance(value, dict)
                    or value.get("schemaVersion") != "cyrene.training-record.v1"
                ):
                    raise ValueError("The curation Plugin produced an invalid record envelope.")
                yield value

    def _parse_one_source(
        self,
        source: SourceRevision,
        run: ProcessingRun,
        cancel_event: Any,
    ) -> tuple[
        list[ContentBlock],
        list[ArtifactRef],
        list[dict[str, str]],
        list[dict[str, Any]],
        list[dict[str, Any]],
        dict[str, Any],
    ]:
        """Dispatch one source to its canonical parser/import path."""

        path = self.artifacts.resolve(source.artifact)
        if source.media_type in _DOCUMENT_PARSER_MEDIA_TYPES:
            return self._parse_document(source, path, run, cancel_event)
        if source.media_type == "application/x-ndjson":
            records = self._read_structured_jsonl(path)
            blocks = self._rows_to_blocks(source, records)
            return (
                blocks,
                [],
                [],
                [],
                [],
                {
                    "sourceRevisionId": str(source.id),
                    "status": "success",
                    "blockCount": len(blocks),
                    "format": "CYRENE_STRUCTURED_SFT_JSONL_V1",
                },
            )
        if source.media_type in {"application/json", "application/vnd.apache.parquet"}:
            inspection = self._inspect_tabular(source, path)
            blocks = self._rows_to_blocks(source, inspection)
            return (
                blocks,
                [],
                [],
                [],
                [],
                {
                    "sourceRevisionId": str(source.id),
                    "status": "success",
                    "blockCount": len(blocks),
                    "format": inspection.source_format.value,
                },
            )
        raise StageExecutionFailure(
            "CATALYST_SOURCE_UNSUPPORTED",
            "The source format is not supported by any configured parser.",
            retryable=False,
            outcome_unknown=False,
        )

    def _finish_source_report(
        self,
        report: SourceParseReport,
        status: SourceParseReportState,
        *,
        block_count: int = 0,
        warnings: list[ProcessingWarning] | None = None,
        diagnostics: list[dict[str, Any]] | None = None,
        unsupported_content: list[dict[str, Any]] | None = None,
        failure: ProcessingFailure | None = None,
        output_artifacts: list[ArtifactRef] | None = None,
    ) -> SourceParseReport:
        """Persist a terminal report while retaining immutable source identity."""

        candidate = self._source_report_candidate(
            report,
            status,
            block_count=block_count,
            warnings=warnings,
            diagnostics=diagnostics,
            unsupported_content=unsupported_content,
            failure=failure,
            output_artifacts=output_artifacts,
        )
        return self.store.save_source_parse_report_for_worker(candidate)

    @staticmethod
    def _source_report_candidate(
        report: SourceParseReport,
        status: SourceParseReportState,
        *,
        block_count: int = 0,
        warnings: list[ProcessingWarning] | None = None,
        diagnostics: list[dict[str, Any]] | None = None,
        unsupported_content: list[dict[str, Any]] | None = None,
        failure: ProcessingFailure | None = None,
        output_artifacts: list[ArtifactRef] | None = None,
    ) -> SourceParseReport:
        """Build a terminal report in memory for atomic revision finalization."""

        now = utc_now()
        return report.model_copy(
            update={
                "status": status,
                "block_count": block_count,
                "warnings": warnings or [],
                "diagnostics": diagnostics or [],
                "unsupported_content": unsupported_content or [],
                "failure": failure,
                "output_artifacts": output_artifacts or [],
                "finished_at": now,
                "updated_at": now,
            }
        )

    def _publish_source_blocks_snapshot(
        self,
        source: SourceRevision,
        blocks: list[ContentBlock],
    ) -> ArtifactRef:
        """Persist the normalized Product block projection for safe retry reuse."""

        staged = self.artifacts.stage_path(f"{uuid4()}.source-content-blocks.json")
        try:
            staged.write_bytes(
                _canonical_json(
                    {
                        "schemaVersion": "cyrene.source.content-blocks.v1",
                        "sourceRevisionId": str(source.id),
                        "sourceId": str(source.source_id),
                        "sourceDigest": source.digest,
                        "blocks": [
                            block.model_dump(by_alias=True, exclude_none=True) for block in blocks
                        ],
                    }
                )
            )
            return self.artifacts.publish(staged, "source-parse-blocks")
        finally:
            staged.unlink(missing_ok=True)

    def _reuse_prior_source_output(
        self,
        source: SourceRevision,
        prior: SourceParseReport | None,
    ) -> tuple[list[ContentBlock], SourceParseReport] | None:
        """Reuse a successful source snapshot when an explicit retry resumes a batch."""

        if prior is None or prior.status not in {
            SourceParseReportState.SUCCEEDED,
            SourceParseReportState.WARNING,
        }:
            return None
        snapshot = next(
            (
                artifact
                for artifact in prior.output_artifacts
                if artifact.kind == "source-parse-blocks"
            ),
            None,
        )
        if snapshot is None:
            return None
        try:
            document = json.loads(self.artifacts.resolve(snapshot).read_text(encoding="utf-8"))
            if (
                not isinstance(document, dict)
                or document.get("schemaVersion") != "cyrene.source.content-blocks.v1"
                or document.get("sourceRevisionId") != str(source.id)
                or document.get("sourceDigest") != source.digest
                or not isinstance(document.get("blocks"), list)
            ):
                raise ValueError("The source block snapshot does not match its receipt.")
            blocks = [ContentBlock.model_validate(item) for item in document["blocks"]]
            if any(block.source_revision_id != source.id for block in blocks):
                raise ValueError("The source block snapshot contains foreign block lineage.")
            return blocks, prior
        except (CatalystError, OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
            emit_diagnostic_error(
                "catalyst.source_parse_retry",
                "CATALYST_SOURCE_SNAPSHOT_REUSE_FAILED",
                "A prior source snapshot could not be reused; this source will be parsed again.",
                attributes={
                    "source_revision_id": str(source.id),
                    "error_type": type(exc).__name__,
                },
            )
            return None

    @staticmethod
    def _stable_failure_code(code: str) -> str:
        """Keep SourceParseReport failures within the public uppercase taxonomy."""

        return code if re.fullmatch(r"[A-Z][A-Z0-9_]+", code) else "CATALYST_SOURCE_PARSE_FAILED"

    def _save_report_review_items(self, report: SourceParseReport) -> None:
        """Project raw Parser diagnostics into explicit, version-bound review items."""

        for item in self._report_review_items(report):
            self.store.save_review_item_for_worker(item)

    def _report_review_items(self, report: SourceParseReport) -> list[ReviewItem]:
        """Build deterministic issue DTOs before an atomic revision finalization."""

        items: list[ReviewItem] = []
        seen_messages: set[str] = set()
        for index, diagnostic in enumerate(report.diagnostics):
            code = diagnostic.get("code")
            message = diagnostic.get("message")
            if not isinstance(code, str) or not isinstance(message, str):
                continue
            kind_value = diagnostic.get("kind")
            kind = (
                ReviewItemKind.OCR_WARNING if kind_value == "ocr" else ReviewItemKind.PARSER_WARNING
            )
            items.append(
                self._build_report_review_item(
                    report,
                    kind=kind,
                    code=code,
                    message=message,
                    severity=str(diagnostic.get("severity") or "warning"),
                    locator=diagnostic.get("locator"),
                    confidence=diagnostic.get("confidence"),
                    content_revision_id=report.content_revision_id,
                    index=index,
                )
            )
            seen_messages.add(message)

        for index, warning in enumerate(report.warnings):
            if warning.message in seen_messages:
                continue
            items.append(
                self._build_report_review_item(
                    report,
                    kind=ReviewItemKind.PARSER_WARNING,
                    code=warning.code,
                    message=warning.message,
                    severity="warning",
                    content_revision_id=report.content_revision_id,
                    index=len(report.diagnostics) + index,
                )
            )
        for index, unsupported in enumerate(report.unsupported_content):
            code = unsupported.get("code")
            message = unsupported.get("message")
            items.append(
                self._build_report_review_item(
                    report,
                    kind=ReviewItemKind.UNSUPPORTED_SOURCE,
                    code=code if isinstance(code, str) else "parser.unsupported_content",
                    message=(
                        message[:2000]
                        if isinstance(message, str)
                        else "The parser could not fully convert this content."
                    ),
                    severity=str(unsupported.get("severity") or "warning"),
                    locator=unsupported.get("locator"),
                    confidence=unsupported.get("confidence"),
                    content_revision_id=report.content_revision_id,
                    index=len(report.diagnostics) + len(report.warnings) + index,
                )
            )
        return items

    def _save_report_review_item(
        self,
        report: SourceParseReport,
        *,
        kind: ReviewItemKind,
        code: str,
        message: str,
        severity: str,
        content_revision_id: UUID | None,
        index: int,
        locator: Any = None,
        confidence: Any = None,
    ) -> None:
        """Persist one idempotent issue without moving Parser details into learned text."""

        item = self._build_report_review_item(
            report,
            kind=kind,
            code=code,
            message=message,
            severity=severity,
            content_revision_id=content_revision_id,
            index=index,
            locator=locator,
            confidence=confidence,
        )
        self.store.save_review_item_for_worker(item)

    def _build_report_review_item(
        self,
        report: SourceParseReport,
        *,
        kind: ReviewItemKind,
        code: str,
        message: str,
        severity: str,
        content_revision_id: UUID | None,
        index: int,
        locator: Any = None,
        confidence: Any = None,
    ) -> ReviewItem:
        """Build one deterministic issue without moving Parser details into learned text."""

        safe_locator: ContentLocator | None = None
        if isinstance(locator, dict):
            try:
                safe_locator = ContentLocator.model_validate(locator)
            except ValueError as exc:
                emit_diagnostic_error(
                    "catalyst.source_parse_review",
                    "CATALYST_REVIEW_LOCATOR_PROJECTION_FAILED",
                    "A raw Parser issue locator remains in the report but cannot be projected.",
                    attributes={
                        "source_revision_id": str(report.source_revision_id),
                        "error_type": type(exc).__name__,
                    },
                )
        confidence_value = confidence if isinstance(confidence, (int, float)) else None
        if isinstance(confidence_value, bool):
            confidence_value = None
        if confidence_value is not None and not 0 <= confidence_value <= 1:
            confidence_value = None
        item = ReviewItem(
            id=uuid5(
                _REVIEW_NAMESPACE,
                f"{report.id}:{index}:{kind.value}:{code}:{message[:500]}",
            ),
            dataset_id=report.dataset_id,
            source_parse_report_id=report.id,
            source_revision_id=report.source_revision_id,
            processing_run_id=report.processing_run_id,
            content_revision_id=content_revision_id,
            kind=kind,
            code=code[:200] or kind.value,
            message=message[:2000] or "The parser reported an issue.",
            severity=severity[:40] or "warning",
            locator=safe_locator,
            confidence=float(confidence_value) if confidence_value is not None else None,
            state=ReviewItemState.OPEN,
            created_at=report.updated_at,
        )
        return item

    def _parse_document(
        self,
        source: SourceRevision,
        path: Path,
        run: ProcessingRun,
        cancel_event: Any,
    ) -> tuple[
        list[ContentBlock],
        list[ArtifactRef],
        list[dict[str, str]],
        list[dict[str, Any]],
        list[dict[str, Any]],
        dict[str, Any],
    ]:
        """Invoke document.parsing.v1 and verify its three output receipts."""

        payload_path = self.artifacts.stage_path(f"{uuid4()}.docling.json")
        blocks_path = self.artifacts.stage_path(f"{uuid4()}.blocks.json")
        result_path = self.artifacts.stage_path(f"{uuid4()}.parse-result.json")
        cleanup = [payload_path, blocks_path, result_path]
        try:
            response = self._invoke_plugin(
                capability=_PARSER_CAPABILITY,
                environment_name=DOCUMENT_PARSING_CONNECTION_ENV,
                method="parse",
                request={
                    "source_path": str(path.resolve()),
                    "payload_path": str(payload_path.resolve()),
                    "blocks_path": str(blocks_path.resolve()),
                    "result_path": str(result_path.resolve()),
                    "source_ref": f"cyrene://catalyst/sources/{source.source_id}",
                    "source_digest": source.digest,
                    "filename": source.filename,
                    "media_type": source.media_type,
                },
                cancel_event=cancel_event,
            )
            if response.get("source_digest") != source.digest:
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_RECEIPT_INVALID",
                    "Parser receipt does not match the source digest.",
                    retryable=False,
                    outcome_unknown=False,
                )
            self._verify_receipt_fields(response, payload_path, "payload")
            self._verify_receipt_fields(response, blocks_path, "blocks")
            self._verify_receipt_fields(response, result_path, "result")
            blocks_document = json.loads(blocks_path.read_text(encoding="utf-8"))
            if not isinstance(blocks_document, dict) or not isinstance(
                blocks_document.get("blocks"), list
            ):
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_OUTPUT_INVALID",
                    "Parser block index is not a valid document.",
                    retryable=False,
                    outcome_unknown=False,
                )
            if blocks_document.get("source_digest") != source.digest:
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_RECEIPT_INVALID",
                    "Parser block index does not match the source digest.",
                    retryable=False,
                    outcome_unknown=False,
                )
            status = response.get("status")
            if status not in {"success", "partial_success"}:
                raise StageExecutionFailure(
                    "CATALYST_DOCUMENT_PARSE_INCOMPLETE",
                    "Parser did not report a successful or partial conversion.",
                    retryable=False,
                    outcome_unknown=False,
                )
            extracted = [self._content_block(source, item) for item in blocks_document["blocks"]]
            reported_blocks = _int_field(response.get("block_count"), "block_count")
            if reported_blocks != len(extracted):
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_RECEIPT_INVALID",
                    "Parser block count does not match its block index.",
                    retryable=False,
                    outcome_unknown=False,
                )
            refs = [
                self.artifacts.publish(payload_path, "dataset"),
                self.artifacts.publish(blocks_path, "dataset"),
                self.artifacts.publish(result_path, "dataset"),
            ]
            for ref, prefix in zip(refs, ("payload", "blocks", "result"), strict=True):
                if ref.digest != response[f"{prefix}_digest"]:
                    raise StageExecutionFailure(
                        "CATALYST_ARTIFACT_IDENTITY_INVALID",
                        f"Artifact SDK digest did not match the parser {prefix} receipt.",
                        retryable=False,
                        outcome_unknown=False,
                    )
            raw_warnings = response.get("warnings")
            if not isinstance(raw_warnings, list):
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_RECEIPT_INVALID",
                    "Parser warning receipt must be an array.",
                    retryable=False,
                    outcome_unknown=False,
                )
            warnings = self._warning_documents(raw_warnings)
            page_count = response.get("page_count")
            if page_count is not None:
                page_count = _int_field(page_count, "page_count")
            warning_count = _int_field(response.get("warning_count"), "warning_count")
            if warning_count != len(raw_warnings):
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_RECEIPT_INVALID",
                    "Parser warning count does not match its warning list.",
                    retryable=False,
                    outcome_unknown=False,
                )
            raw_diagnostics = response.get("diagnostics", [])
            if not isinstance(raw_diagnostics, list) or any(
                not isinstance(item, dict) for item in raw_diagnostics
            ):
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_RECEIPT_INVALID",
                    "Parser diagnostics must be an array of objects.",
                    retryable=False,
                    outcome_unknown=False,
                )
            raw_unsupported = response.get("unsupported_content", [])
            if not isinstance(raw_unsupported, list) or any(
                not isinstance(item, dict) for item in raw_unsupported
            ):
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_RECEIPT_INVALID",
                    "Parser unsupported_content must be an array of objects.",
                    retryable=False,
                    outcome_unknown=False,
                )
            parse_receipt = {
                "sourceRevisionId": str(source.id),
                "sourceDigest": source.digest,
                "sourceFormat": response.get("source_format"),
                "status": status,
                "conversionProfile": response.get("conversion_profile"),
                "pageCount": page_count,
                "blockCount": reported_blocks,
                "warningCount": warning_count,
                "slideCount": response.get("slide_count"),
                "sheetCount": response.get("sheet_count"),
                "diagnostics": raw_diagnostics,
                "unsupportedContent": raw_unsupported,
                "payloadArtifact": refs[0].model_dump(by_alias=True, exclude_none=True),
                "blocksArtifact": refs[1].model_dump(by_alias=True, exclude_none=True),
                "resultArtifact": refs[2].model_dump(by_alias=True, exclude_none=True),
            }
            return extracted, refs, warnings, raw_diagnostics, raw_unsupported, parse_receipt
        except (OSError, UnicodeError, json.JSONDecodeError, KeyError) as exc:
            raise StageExecutionFailure(
                "CATALYST_PLUGIN_OUTPUT_INVALID",
                "Parser output could not be read or validated.",
                retryable=False,
                outcome_unknown=False,
            ) from exc
        finally:
            for staged in cleanup:
                staged.unlink(missing_ok=True)

    @staticmethod
    def _verify_receipt_fields(
        response: dict[str, Any],
        path: Path,
        prefix: str,
    ) -> None:
        """Apply the capability-specific digest and size receipt fields."""

        _verify_file_receipt(
            path, response.get(f"{prefix}_digest"), response.get(f"{prefix}_size"), prefix
        )

    @staticmethod
    def _read_structured_jsonl(path: Path) -> list[dict[str, Any]]:
        """Read bounded JSONL rows without sending structured SFT data to prose parsers."""

        try:
            lines = path.read_text(encoding="utf-8").splitlines()
            if not lines or len(lines) > 1_000_000 or any(not line.strip() for line in lines):
                raise ValueError("JSONL must contain nonblank rows.")
            rows: list[dict[str, Any]] = []
            for index, line in enumerate(lines, start=1):
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ValueError(f"JSONL row {index} must be an object.")
                rows.append(value)
            return rows
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
            raise StageExecutionFailure(
                "CATALYST_SOURCE_PARSE_FAILED",
                "Structured JSONL could not be read as valid UTF-8 JSON objects.",
                retryable=False,
                outcome_unknown=False,
            ) from exc

    @staticmethod
    def _content_block(source: SourceRevision, raw: Any) -> ContentBlock:
        """Project one parser block onto the Product review contract."""

        value = _json_object(raw, "parser block")
        locator_raw = _json_object(value.get("locator", {}), "parser locator")
        source_pages_raw = locator_raw.get("source_pages", [])
        if not isinstance(source_pages_raw, list) or any(
            isinstance(page, bool) or not isinstance(page, int) or page < 1
            for page in source_pages_raw
        ):
            raise StageExecutionFailure(
                "CATALYST_PLUGIN_OUTPUT_INVALID",
                "Parser returned an invalid source_pages locator.",
                retryable=False,
                outcome_unknown=False,
            )
        provenance_raw = locator_raw.get("provenance", [])
        if not isinstance(provenance_raw, list) or any(
            not isinstance(item, dict) for item in provenance_raw
        ):
            raise StageExecutionFailure(
                "CATALYST_PLUGIN_OUTPUT_INVALID",
                "Parser returned an invalid provenance locator.",
                retryable=False,
                outcome_unknown=False,
            )
        kind = value.get("kind", "unknown")
        text = value.get("text", "")
        item_ref = value.get("id") or locator_raw.get("item_ref")
        if not isinstance(kind, str) or not isinstance(text, str) or not isinstance(item_ref, str):
            raise StageExecutionFailure(
                "CATALYST_PLUGIN_OUTPUT_INVALID",
                "Parser block has an invalid kind, text, or identity.",
                retryable=False,
                outcome_unknown=False,
            )
        identifier = str(uuid5(_BLOCK_NAMESPACE, f"{source.id}:{item_ref}"))
        try:
            return ContentBlock(
                id=identifier,
                source_revision_id=source.id,
                ordinal=_int_field(value.get("ordinal"), "block.ordinal"),
                kind=kind[:80],
                text=text,
                locator=ContentLocator(
                    source_pages=source_pages_raw,
                    section_path=locator_raw.get("section_path", []),
                    item_ref=str(locator_raw.get("item_ref", item_ref))[:512],
                    tree_level=locator_raw.get("tree_level", 0),
                    table_index=locator_raw.get("table_index"),
                    provenance=provenance_raw,
                ),
                origin=BlockOrigin.EXTRACTED,
                policy=ContentPolicy(),
            )
        except (TypeError, ValueError) as exc:
            raise StageExecutionFailure(
                "CATALYST_PLUGIN_OUTPUT_INVALID",
                "Parser returned a block that does not conform to the Product contract.",
                retryable=False,
                outcome_unknown=False,
            ) from exc

    def _inspect_tabular(self, source: SourceRevision, path: Path) -> SourceInspection:
        """Read existing JSONL/conversation/tabular sources through the current Plugin."""

        suffix = Path(source.filename).suffix.casefold()
        format_hint = {
            ".csv": ImportFormat.CSV,
            ".parquet": ImportFormat.PARQUET,
            ".pq": ImportFormat.PARQUET,
            ".json": ImportFormat.JSON,
            ".jsonl": ImportFormat.JSONL,
            ".ndjson": ImportFormat.JSONL,
            ".txt": ImportFormat.TEXT,
            ".md": ImportFormat.TEXT,
        }.get(suffix)
        try:
            return self.preparation_engine.inspect(path, format_hint=format_hint)
        except DataEngineFailure as exc:
            reason = str(exc)
            code = (
                "CATALYST_PLUGIN_NOT_CONFIGURED"
                if "set CYRENE_DATASET_PREPARATION_CONNECTION_REF" in reason
                or "not installed" in reason
                else "CATALYST_SOURCE_PARSE_FAILED"
            )
            raise StageExecutionFailure(
                code,
                "The existing dataset.preparation.v1 parser could not inspect this source.",
                retryable=code == "CATALYST_PLUGIN_NOT_CONFIGURED",
                outcome_unknown=False,
            ) from exc

    @staticmethod
    def _rows_to_blocks(
        source: SourceRevision,
        inspection: SourceInspection | list[dict[str, Any]],
    ) -> list[ContentBlock]:
        """Preserve imported records as structured, reviewable SFT blocks."""

        blocks: list[ContentBlock] = []
        rows = inspection.rows if isinstance(inspection, SourceInspection) else inspection
        for index, row in enumerate(rows):
            is_instruction = isinstance(row.get("instruction"), str) and isinstance(
                row.get("output"), str
            )
            conversations = row.get("conversations")
            is_conversation = isinstance(conversations, list) and bool(conversations)
            messages = row.get("messages")
            is_messages = "messages" in row
            if is_messages and (
                not isinstance(messages, list)
                or not messages
                or any(
                    not isinstance(message, dict)
                    or set(message) != {"role", "content"}
                    or message.get("role") not in {"user", "assistant"}
                    or not isinstance(message.get("content"), str)
                    or not message["content"].strip()
                    for message in messages
                )
            ):
                raise StageExecutionFailure(
                    "CATALYST_SOURCE_RECORD_UNSUPPORTED",
                    f"Structured row {index + 1} messages are invalid.",
                    retryable=False,
                    outcome_unknown=False,
                )
            if not is_instruction and not is_conversation and not is_messages:
                raise StageExecutionFailure(
                    "CATALYST_SOURCE_RECORD_UNSUPPORTED",
                    f"Structured row {index + 1} must contain instruction/output, conversations, "
                    "or messages.",
                    retryable=False,
                    outcome_unknown=False,
                )
            if sum((is_instruction, is_conversation, is_messages)) > 1:
                raise StageExecutionFailure(
                    "CATALYST_SOURCE_RECORD_UNSUPPORTED",
                    f"Structured row {index + 1} contains conflicting learned-data formats.",
                    retryable=False,
                    outcome_unknown=False,
                )
            learned: dict[str, Any]
            if is_instruction:
                if not row["instruction"].strip() or not row["output"].strip():
                    raise StageExecutionFailure(
                        "CATALYST_SOURCE_RECORD_UNSUPPORTED",
                        f"Structured row {index + 1} has empty instruction/output content.",
                        retryable=False,
                        outcome_unknown=False,
                    )
                if "input" in row and not isinstance(row["input"], str):
                    raise StageExecutionFailure(
                        "CATALYST_SOURCE_RECORD_UNSUPPORTED",
                        f"Structured row {index + 1} input must be a string.",
                        retryable=False,
                        outcome_unknown=False,
                    )
                learned = {
                    key: row[key] for key in ("instruction", "input", "output") if key in row
                }
            elif is_conversation:
                assert isinstance(conversations, list)
                if any(
                    not isinstance(message, dict)
                    or set(message) != {"from", "value"}
                    or message.get("from") not in {"human", "gpt"}
                    or not isinstance(message.get("value"), str)
                    or not message["value"].strip()
                    for message in conversations
                ):
                    raise StageExecutionFailure(
                        "CATALYST_SOURCE_RECORD_UNSUPPORTED",
                        f"Structured row {index + 1} conversations are invalid.",
                        retryable=False,
                        outcome_unknown=False,
                    )
                learned = {"conversations": conversations}
            else:
                assert isinstance(messages, list)
                role_map = {"user": "human", "assistant": "gpt"}
                learned = {
                    "conversations": [
                        {"from": role_map[message["role"]], "value": message["content"]}
                        for message in messages
                    ]
                }
            canonical = _canonical_json(learned).decode("utf-8")
            family_values = [
                _metadata_string(row, index, key)
                for key in ("sourceFamilyId", "source_family_id", "sourceFamily", "source_family")
                if row.get(key) is not None
            ]
            if len(set(family_values)) > 1:
                raise StageExecutionFailure(
                    "CATALYST_SOURCE_FAMILY_CONFLICT",
                    f"Structured row {index + 1} has conflicting source-family aliases.",
                    retryable=False,
                    outcome_unknown=False,
                )
            family_value = family_values[0] if family_values else None
            conversation_value = _metadata_string(
                row, index, "conversationId", "conversation_id", "groupId", "group_id", "thread_id"
            )
            sample_value = _metadata_string(row, index, "sampleId", "sample_id")
            acl_value = row.get("_acl", row.get("allowedPrincipalRefs", []))
            if not isinstance(acl_value, list) or any(
                not isinstance(entry, str) or not entry.strip() for entry in acl_value
            ):
                raise StageExecutionFailure(
                    "CATALYST_SOURCE_RECORD_UNSUPPORTED",
                    f"Structured row {index + 1} ACL metadata must be an array "
                    "of non-empty strings.",
                    retryable=False,
                    outcome_unknown=False,
                )
            allowed_principals = list(dict.fromkeys(acl_value))
            blocks.append(
                ContentBlock(
                    id=str(uuid5(_BLOCK_NAMESPACE, f"{source.id}:row:{index}")),
                    source_revision_id=source.id,
                    ordinal=index,
                    kind="code",
                    text=canonical,
                    locator=ContentLocator(item_ref=f"row:{index + 1}"),
                    origin=BlockOrigin.EXTRACTED,
                    group_id=str(conversation_value) if conversation_value else None,
                    source_family_id=str(family_value) if family_value else None,
                    conversation_id=str(conversation_value) if conversation_value else None,
                    sample_id=str(sample_value) if sample_value else None,
                    policy=ContentPolicy(allowed_principal_refs=allowed_principals),
                )
            )
        return blocks

    def _build_knowledge(self, run: ProcessingRun, cancel_event: Any) -> dict[str, Any]:
        """Build a policy-preserving knowledge package from approved blocks."""

        revision_id = _content_revision_id_for_run(run)
        revision = self.store.get_content_revision_for_worker(revision_id)
        if revision is None or revision.state != ContentRevisionState.APPROVED:
            raise StageExecutionFailure(
                "CATALYST_CONTENT_REVISION_NOT_APPROVED",
                "Knowledge preparation requires an approved ContentRevision.",
                retryable=False,
                outcome_unknown=False,
            )
        blocks_path = self.artifacts.stage_path(f"{uuid4()}.approved-blocks.jsonl")
        bundle_path = self.artifacts.stage_path(f"{uuid4()}.knowledge.zip")
        result_path = self.artifacts.stage_path(f"{uuid4()}.knowledge-result.json")
        try:
            self._write_blocks(blocks_path, revision.blocks)
            source_revisions = self._worker_sources(revision.source_revision_ids)
            response = self._invoke_plugin(
                capability=_KNOWLEDGE_CAPABILITY,
                environment_name=KNOWLEDGE_PREPARATION_CONNECTION_ENV,
                method="build",
                request={
                    "blocks_path": str(blocks_path.resolve()),
                    "bundle_path": str(bundle_path.resolve()),
                    "result_path": str(result_path.resolve()),
                    "dataset_id": str(run.dataset_id),
                    "content_revision_id": str(revision.id),
                    "processing_run_id": str(run.id),
                    "source_revisions": [
                        {
                            "source_id": str(source.source_id),
                            "source_revision_id": str(source.id),
                            "revision": source.revision,
                            "digest": source.digest,
                            "artifact": source.artifact.model_dump(
                                by_alias=True, exclude_none=True
                            ),
                        }
                        for source in source_revisions
                    ],
                    "policy": {"use_purpose": "knowledge_retrieval"},
                },
                cancel_event=cancel_event,
            )
            _verify_file_receipt(
                bundle_path,
                response.get("bundle_digest"),
                response.get("bundle_size"),
                "knowledge bundle",
            )
            _verify_file_receipt(
                result_path,
                response.get("result_digest"),
                response.get("result_size"),
                "knowledge result",
            )
            package = self.artifacts.publish(bundle_path, "dataset")
            report = self.artifacts.publish(result_path, "dataset")
            if package.digest != response.get("bundle_digest"):
                raise StageExecutionFailure(
                    "CATALYST_ARTIFACT_IDENTITY_INVALID",
                    "Artifact SDK digest did not match the knowledge package receipt.",
                    retryable=False,
                    outcome_unknown=False,
                )
            return {
                "profile": "CYRENE_KNOWLEDGE_BUNDLE_V1",
                "packageArtifact": package.model_dump(by_alias=True, exclude_none=True),
                "resultArtifact": report.model_dump(by_alias=True, exclude_none=True),
                "chunkCount": _int_field(response.get("chunk_count"), "chunk_count"),
                "sourceCount": _int_field(response.get("source_count"), "source_count"),
                "manifestDigest": response.get("manifest_digest"),
                "warnings": self._warning_documents(response.get("conversion_report")),
                "outputArtifacts": [
                    package.model_dump(by_alias=True, exclude_none=True),
                    report.model_dump(by_alias=True, exclude_none=True),
                ],
            }
        except (OSError, KeyError) as exc:
            raise StageExecutionFailure(
                "CATALYST_PLUGIN_OUTPUT_INVALID",
                "Knowledge Plugin output could not be read or published.",
                retryable=False,
                outcome_unknown=False,
            ) from exc
        finally:
            for staged in (blocks_path, bundle_path, result_path):
                staged.unlink(missing_ok=True)

    def _prepare_sft(self, run: ProcessingRun, cancel_event: Any) -> dict[str, Any]:
        """Build the deterministic SFT package from approved trainable blocks."""

        revision_id = _content_revision_id_for_run(run)
        revision = self.store.get_content_revision_for_worker(revision_id)
        if revision is None or revision.state != ContentRevisionState.APPROVED:
            raise StageExecutionFailure(
                "CATALYST_CONTENT_REVISION_NOT_APPROVED",
                "SFT preparation requires an approved ContentRevision.",
                retryable=False,
                outcome_unknown=False,
            )
        artifact_backed = revision.training_data_snapshot is not None
        blocks_path = (
            None
            if artifact_backed
            else self.artifacts.stage_path(f"{uuid4()}.approved-blocks.json")
        )
        output_dir = (
            self.artifacts.stage_dir(f"{uuid4()}.training-sft") if artifact_backed else None
        )
        bundle_path = (
            output_dir / "bundle.zip"
            if output_dir is not None
            else self.artifacts.stage_path(f"{uuid4()}.sft.zip")
        )
        result_path = (
            output_dir / "result.json"
            if output_dir is not None
            else self.artifacts.stage_path(f"{uuid4()}.sft-result.json")
        )
        try:
            if blocks_path is not None:
                self._write_approved_blocks(blocks_path, revision)
            split = run.recipe.get("config", {}).get("split")
            if split is None:
                split = {"train": 0.8, "validation": 0.1, "test": 0.1}
            if artifact_backed:
                assert revision.training_data_snapshot is not None and output_dir is not None
                try:
                    validated_counts = self._reconcile_training_snapshot(revision)
                except (CatalystError, OSError, sqlite3.Error, ValueError) as exc:
                    raise StageExecutionFailure(
                        "CATALYST_TRAINING_SNAPSHOT_INVALID",
                        "The approved training snapshot failed lineage or count verification.",
                        retryable=False,
                        outcome_unknown=False,
                    ) from exc
                if validated_counts.pending_review:
                    raise StageExecutionFailure(
                        "CATALYST_TRAINING_RECORDS_PENDING_REVIEW",
                        "SFT export requires every training record to be approved or excluded.",
                        retryable=False,
                        outcome_unknown=False,
                    )
                split_request = {**split, "seed": split.get("seed", 42)}
                output_format = run.recipe.get("config", {}).get("outputFormat", "sft")
                response = self._invoke_plugin(
                    capability=_GENERATION_CAPABILITY,
                    environment_name=DATASET_GENERATION_CONNECTION_ENV,
                    method="prepare_training_sft",
                    request={
                        "snapshot_path": str(
                            self.artifacts.resolve(
                                revision.training_data_snapshot.artifact
                            ).resolve()
                        ),
                        "output_dir": str(output_dir.resolve()),
                        "output_format": output_format,
                        "recipe_digest": run.recipe_digest,
                        "recipe_version": run.recipe_version,
                        "split": split_request,
                        "dataset_id": str(run.dataset_id),
                        "content_revision_id": str(revision.id),
                        "processing_run_id": str(run.id),
                    },
                    cancel_event=cancel_event,
                )
                if response.get("bundle_path") != str(bundle_path.resolve()):
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_RECEIPT_INVALID",
                        "The SFT exporter returned a bundle path outside its output directory.",
                        retryable=False,
                        outcome_unknown=False,
                    )
            else:
                assert blocks_path is not None
                response = self._invoke_plugin(
                    capability=_GENERATION_CAPABILITY,
                    environment_name=DATASET_GENERATION_CONNECTION_ENV,
                    method="prepare_sft",
                    request={
                        "blocks_path": str(blocks_path.resolve()),
                        "bundle_path": str(bundle_path.resolve()),
                        "result_path": str(result_path.resolve()),
                        "dataset_id": str(run.dataset_id),
                        "content_revision_id": str(revision.id),
                        "processing_run_id": str(run.id),
                        "mode": run.recipe.get("config", {}).get("sftMode", "instruction"),
                        "split": split,
                    },
                    cancel_event=cancel_event,
                )
            _verify_file_receipt(
                bundle_path,
                response.get("bundle_digest"),
                response.get("bundle_size_bytes"),
                "SFT bundle",
            )
            package = self.artifacts.publish(bundle_path, "dataset")
            if package.digest != response.get("bundle_digest"):
                raise StageExecutionFailure(
                    "CATALYST_ARTIFACT_IDENTITY_INVALID",
                    "Artifact SDK digest did not match the SFT package receipt.",
                    retryable=False,
                    outcome_unknown=False,
                )
            files = _json_object(response.get("files", {}), "SFT files receipt")
            train = _json_object(files.get("train.jsonl", {}), "SFT train receipt")
            allowed_files = {
                "train.jsonl",
                "validation.jsonl",
                "test.jsonl",
                "provenance.jsonl",
                "manifest.json",
            }
            if set(files) != allowed_files:
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_RECEIPT_INVALID",
                    "SFT file receipts do not match the required bundle layout.",
                    retryable=False,
                    outcome_unknown=False,
                )
            try:
                with zipfile.ZipFile(bundle_path) as archive:
                    member_names = archive.namelist()
                    if (
                        len(member_names) != len(set(member_names))
                        or set(member_names) != allowed_files
                    ):
                        raise ValueError("SFT package members do not match the receipt")
                    total_size = 0
                    for name in sorted(allowed_files):
                        receipt = _json_object(files[name], f"SFT {name} receipt")
                        info = archive.getinfo(name)
                        if info.file_size > MAX_ZIP_ENTRY_BYTES or info.flag_bits & 0x1:
                            raise ValueError(f"SFT package member {name} exceeds limits")
                        total_size += info.file_size
                        if total_size > MAX_RESULT_BYTES:
                            raise ValueError("SFT package files exceed the supported total size")
                        member_digest = hashlib.sha256()
                        member_size = 0
                        with archive.open(info) as member:
                            while chunk := member.read(1024 * 1024):
                                member_size += len(chunk)
                                member_digest.update(chunk)
                        if (
                            _int_field(receipt.get("size_bytes"), f"{name}.size_bytes")
                            != member_size
                            or receipt.get("digest") != f"sha256:{member_digest.hexdigest()}"
                        ):
                            raise ValueError(
                                f"SFT package member {name} failed receipt verification"
                            )
            except (OSError, KeyError, ValueError, zipfile.BadZipFile) as exc:
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_OUTPUT_INVALID",
                    "SFT package members failed receipt verification.",
                    retryable=False,
                    outcome_unknown=False,
                ) from exc
            if artifact_backed:
                try:
                    self._verify_training_sft_export(
                        bundle_path,
                        files,
                        response,
                        run,
                        revision,
                        output_format,
                        split_request,
                    )
                except (
                    CatalystError,
                    OSError,
                    sqlite3.Error,
                    UnicodeError,
                    json.JSONDecodeError,
                    ValueError,
                ) as exc:
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_OUTPUT_INVALID",
                        "SFT split rows or provenance do not match the approved snapshot.",
                        retryable=False,
                        outcome_unknown=False,
                    ) from exc
            result_document = self._read_result_document(result_path)
            if result_document != response:
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_RECEIPT_INVALID",
                    "SFT result file and typed response do not match.",
                    retryable=False,
                    outcome_unknown=False,
                )
            report = self.artifacts.publish(result_path, "dataset")
            if not isinstance(train.get("schema_fields", []), list) or any(
                not isinstance(field, str) for field in train.get("schema_fields", [])
            ):
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_RECEIPT_INVALID",
                    "SFT train schema receipt is invalid.",
                    retryable=False,
                    outcome_unknown=False,
                )
            return {
                "profile": "CYRENE_SFT_BUNDLE_V1",
                "packageArtifact": package.model_dump(by_alias=True, exclude_none=True),
                "resultArtifact": report.model_dump(by_alias=True, exclude_none=True),
                "sampleCount": _int_field(response.get("sample_count"), "sample_count"),
                "trainRowCount": _int_field(train.get("row_count"), "train.row_count"),
                "schemaFields": train.get("schema_fields", []),
                "fileReceipts": files,
                "splitStats": response.get("split_stats", {}),
                "warnings": self._warning_documents(response.get("warnings")),
                "outputArtifacts": [
                    package.model_dump(by_alias=True, exclude_none=True),
                    report.model_dump(by_alias=True, exclude_none=True),
                ],
            }
        except OSError as exc:
            raise StageExecutionFailure(
                "CATALYST_PLUGIN_OUTPUT_INVALID",
                "SFT Plugin output could not be read or published.",
                retryable=False,
                outcome_unknown=False,
            ) from exc
        finally:
            for staged in (blocks_path, bundle_path, result_path):
                if staged is not None:
                    staged.unlink(missing_ok=True)
            if output_dir is not None:
                shutil.rmtree(output_dir, ignore_errors=True)

    def _generate_qa(self, run: ProcessingRun, cancel_event: Any) -> dict[str, Any]:
        """Persist generated QA drafts as a new unapproved ContentRevision."""

        revision_id = _content_revision_id_for_run(run)
        revision = self.store.get_content_revision_for_worker(revision_id)
        if revision is None or revision.state != ContentRevisionState.APPROVED:
            raise StageExecutionFailure(
                "CATALYST_CONTENT_REVISION_NOT_APPROVED",
                "QA generation requires an approved ContentRevision.",
                retryable=False,
                outcome_unknown=False,
            )
        blocks_path = self.artifacts.stage_path(f"{uuid4()}.generation-blocks.json")
        drafts_path = self.artifacts.stage_path(f"{uuid4()}.qa-drafts.jsonl")
        provenance_path = self.artifacts.stage_path(f"{uuid4()}.qa-provenance.jsonl")
        result_path = self.artifacts.stage_path(f"{uuid4()}.qa-result.json")
        try:
            self._write_approved_blocks(blocks_path, revision)
            generation = run.recipe.get("config", {}).get("generation", {})
            request = {
                "blocks_path": str(blocks_path.resolve()),
                "drafts_path": str(drafts_path.resolve()),
                "provenance_path": str(provenance_path.resolve()),
                "result_path": str(result_path.resolve()),
                "dataset_id": str(run.dataset_id),
                "content_revision_id": str(revision.id),
                "processing_run_id": str(run.id),
                "generation": {
                    "max_examples": generation["maxExamples"],
                    "max_calls": generation["maxCalls"],
                    **(
                        {"max_input_tokens": generation["maxInputTokens"]}
                        if "maxInputTokens" in generation
                        else {}
                    ),
                    **(
                        {"max_output_tokens": generation["maxOutputTokens"]}
                        if "maxOutputTokens" in generation
                        else {}
                    ),
                },
            }
            if run.recipe.get("config", {}).get("split") is not None:
                request["split"] = run.recipe["config"]["split"]
            response = self._invoke_plugin(
                capability=_GENERATION_CAPABILITY,
                environment_name=DATASET_GENERATION_CONNECTION_ENV,
                method="generate_qa",
                request=request,
                cancel_event=cancel_event,
                paid_outcome_unknown=True,
            )
            _verify_file_receipt(
                drafts_path,
                response.get("drafts_digest"),
                response.get("drafts_size_bytes"),
                "generated drafts",
            )
            _verify_file_receipt(
                provenance_path,
                response.get("provenance_digest"),
                response.get("provenance_size_bytes"),
                "generation provenance",
            )
            draft_rows = self._read_jsonl(drafts_path)
            provenance_rows = self._read_jsonl(provenance_path)
            result_document = self._read_result_document(result_path)
            if result_document != response or len(draft_rows) != len(provenance_rows):
                raise StageExecutionFailure(
                    "CATALYST_PLUGIN_RECEIPT_INVALID",
                    "Generation result, drafts, and provenance receipts do not agree.",
                    retryable=False,
                    outcome_unknown=True,
                )
            provenance_by_id = {
                str(item.get("sample_id", item.get("sampleId"))): item
                for item in provenance_rows
                if isinstance(item, dict)
            }
            generated: list[ContentBlock] = []
            source_by_id = {
                str(item.id): item for item in self._worker_sources(revision.source_revision_ids)
            }
            original_by_id = {block.id: block for block in revision.blocks}
            for index, row in enumerate(draft_rows):
                if not isinstance(row, dict):
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_OUTPUT_INVALID",
                        "Generation draft rows must be JSON objects.",
                        retryable=False,
                        outcome_unknown=True,
                    )
                receipt = provenance_rows[index] if index < len(provenance_rows) else {}
                sample_id = str(
                    receipt.get("sample_id", receipt.get("sampleId"))
                    if isinstance(receipt, dict)
                    else index
                )
                receipt = provenance_by_id.get(sample_id, {})
                citations = receipt.get("citations", [])
                citation = citations[0] if isinstance(citations, list) and citations else {}
                source_revision_id = (
                    citation.get("source_revision_id", citation.get("sourceRevisionId"))
                    if isinstance(citation, dict)
                    else None
                )
                try:
                    source_id = UUID(str(source_revision_id))
                except (TypeError, ValueError):
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_OUTPUT_INVALID",
                        "Generation provenance must identify a SourceRevision for each draft.",
                        retryable=False,
                        outcome_unknown=True,
                    ) from None
                if str(source_id) not in source_by_id:
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_OUTPUT_INVALID",
                        "Generation provenance refers to a source outside the approved revision.",
                        retryable=False,
                        outcome_unknown=True,
                    )
                source_block_id = (
                    citation.get("block_id", citation.get("blockId"))
                    if isinstance(citation, dict)
                    else None
                )
                source_block = original_by_id.get(str(source_block_id))
                question = row.get("instruction", row.get("question"))
                answer = row.get("output", row.get("answer"))
                if (
                    not isinstance(question, str)
                    or not question.strip()
                    or not isinstance(answer, str)
                    or not answer.strip()
                ):
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_OUTPUT_INVALID",
                        "Generation drafts require non-empty instruction and output strings.",
                        retryable=False,
                        outcome_unknown=True,
                    )
                text = _canonical_json(
                    {
                        "instruction": question,
                        "input": row.get("input", ""),
                        "output": answer,
                    }
                ).decode("utf-8")
                generation_receipt = receipt.get(
                    "generation_receipt", receipt.get("generationReceipt")
                )
                if not isinstance(generation_receipt, dict):
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_OUTPUT_INVALID",
                        "Generation provenance is missing its per-sample receipt.",
                        retryable=False,
                        outcome_unknown=True,
                    )
                try:
                    generation_model = GenerationReceipt.model_validate(generation_receipt)
                except ValueError as exc:
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_OUTPUT_INVALID",
                        "Generation provenance receipt does not match the Product contract.",
                        retryable=False,
                        outcome_unknown=True,
                    ) from exc
                if str(source_block_id) not in generation_model.source_block_ids:
                    raise StageExecutionFailure(
                        "CATALYST_PLUGIN_OUTPUT_INVALID",
                        "Generation receipt does not bind the cited source block.",
                        retryable=False,
                        outcome_unknown=True,
                    )
                generated.append(
                    ContentBlock(
                        id=str(uuid5(_BLOCK_NAMESPACE, f"{run.id}:{sample_id}")),
                        source_revision_id=source_id,
                        ordinal=index,
                        kind="training_record",
                        text=text,
                        locator=source_block.locator if source_block else ContentLocator(),
                        origin=BlockOrigin.GENERATED,
                        group_id=(source_block.group_id if source_block else None),
                        source_family_id=(source_block.source_family_id if source_block else None),
                        conversation_id=source_block.conversation_id if source_block else None,
                        sample_id=sample_id,
                        generation_receipt=generation_model,
                        policy=(source_block.policy if source_block else ContentPolicy()),
                    )
                )
            generated_revision = self._new_worker_revision(
                dataset_id=run.dataset_id,
                source_revision_ids=revision.source_revision_ids,
                parent_revision_id=revision.id,
                blocks=[*revision.blocks, *generated],
            )
            draft_artifact = self.artifacts.publish(drafts_path, "dataset")
            provenance_artifact = self.artifacts.publish(provenance_path, "dataset")
            result_artifact = self.artifacts.publish(result_path, "dataset")
            return {
                "generatedContentRevisionId": str(generated_revision.id),
                "draftCount": _int_field(response.get("draft_count"), "draft_count"),
                "provider": response.get("provider", {}),
                "budget": response.get("budget", {}),
                "recipe": response.get("recipe", {}),
                "warnings": self._warning_documents(response.get("warnings")),
                "outputArtifacts": [
                    draft_artifact.model_dump(by_alias=True, exclude_none=True),
                    provenance_artifact.model_dump(by_alias=True, exclude_none=True),
                    result_artifact.model_dump(by_alias=True, exclude_none=True),
                ],
            }
        except OSError as exc:
            raise StageExecutionFailure(
                "CATALYST_PLUGIN_OUTPUT_INVALID",
                "Generation Plugin output could not be read or published.",
                retryable=False,
                outcome_unknown=True,
            ) from exc
        finally:
            for staged in (blocks_path, drafts_path, provenance_path, result_path):
                staged.unlink(missing_ok=True)

    def _new_worker_revision(
        self,
        *,
        dataset_id: UUID,
        source_revision_ids: list[UUID],
        parent_revision_id: UUID | None,
        blocks: list[ContentBlock],
    ) -> ContentRevision:
        """Create a draft snapshot from a trusted in-process worker."""

        revision = self._build_worker_revision(
            dataset_id=dataset_id,
            source_revision_ids=source_revision_ids,
            parent_revision_id=parent_revision_id,
            blocks=blocks,
        )
        try:
            return self.store.create_content_revision_for_worker(revision)
        except (LookupError, ValueError) as exc:
            raise StageExecutionFailure(
                "CATALYST_CONTENT_REVISION_CREATE_FAILED",
                "The Product could not persist the parser output as a new ContentRevision.",
                retryable=False,
                outcome_unknown=False,
            ) from exc

    def _build_worker_revision(
        self,
        *,
        dataset_id: UUID,
        source_revision_ids: list[UUID],
        parent_revision_id: UUID | None,
        blocks: list[ContentBlock],
    ) -> ContentRevision:
        """Construct a draft snapshot without persisting it."""

        revisions = self.store.list_content_revisions_for_worker(dataset_id)
        return ContentRevision(
            id=uuid4(),
            dataset_id=dataset_id,
            revision=(revisions[0].revision + 1) if revisions else 1,
            parent_revision_id=parent_revision_id,
            source_revision_ids=source_revision_ids,
            blocks=blocks,
            state=ContentRevisionState.DRAFT,
            created_at=utc_now(),
            resource_version=1,
        )

    @staticmethod
    def _write_blocks(path: Path, blocks: list[ContentBlock]) -> None:
        """Write one canonical ContentBlock JSONL file for Plugin consumption."""

        with path.open("wb") as stream:
            for block in blocks:
                stream.write(_canonical_json(block.model_dump(by_alias=True, exclude_none=True)))
                stream.write(b"\n")

    @staticmethod
    def _write_approved_blocks(path: Path, revision: ContentRevision) -> None:
        """Write the Plugin's approved-revision envelope without host metadata."""

        _copy_json(
            path,
            {
                "schema_version": "cyrene.content.blocks.v1",
                "content_revision_id": str(revision.id),
                "content_revision_state": revision.state.value,
                "blocks": [
                    block.model_dump(by_alias=True, exclude_none=True) for block in revision.blocks
                ],
            },
        )

    def _worker_sources(self, source_ids: list[UUID]) -> list[SourceRevision]:
        """Read source revisions from the trusted worker scope."""

        sources = [self.store.get_source_for_worker(source_id) for source_id in source_ids]
        if any(source is None for source in sources):
            raise StageExecutionFailure(
                "CATALYST_SOURCE_REVISION_NOT_FOUND",
                "One source revision referenced by the ContentRevision is missing.",
                retryable=False,
                outcome_unknown=False,
            )
        return [source for source in sources if source is not None]

    @staticmethod
    def _read_jsonl(path: Path) -> list[Any]:
        """Read bounded JSONL output from the generation Plugin."""

        if path.stat().st_size > MAX_RESULT_BYTES:
            raise StageExecutionFailure(
                "CATALYST_PLUGIN_OUTPUT_TOO_LARGE",
                "Plugin JSONL output exceeds the supported size.",
                retryable=False,
                outcome_unknown=False,
            )
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
            if any(not line.strip() for line in lines):
                raise ValueError("blank JSONL row")
            return [json.loads(line) for line in lines]
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
            raise StageExecutionFailure(
                "CATALYST_PLUGIN_OUTPUT_INVALID",
                "Plugin output is not valid JSONL.",
                retryable=False,
                outcome_unknown=False,
            ) from exc

    @staticmethod
    def _read_result_document(path: Path) -> dict[str, Any]:
        """Read a bounded Plugin report document before publishing its receipt."""

        if not path.is_file() or path.stat().st_size > 2 * 1024 * 1024:
            raise StageExecutionFailure(
                "CATALYST_PLUGIN_OUTPUT_INVALID",
                "Plugin result document is missing or too large.",
                retryable=False,
                outcome_unknown=False,
            )
        try:
            return _json_object(json.loads(path.read_text(encoding="utf-8")), "Plugin result")
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise StageExecutionFailure(
                "CATALYST_PLUGIN_OUTPUT_INVALID",
                "Plugin result document is not valid JSON.",
                retryable=False,
                outcome_unknown=False,
            ) from exc

    @staticmethod
    def _warning_documents(value: Any) -> list[dict[str, str]]:
        """Normalize bounded Plugin warning projections."""

        if isinstance(value, dict):
            nested = value.get("warnings")
            collected = DataToolsService._warning_documents(nested)
            warning_count = value.get("warningCount")
            if (
                isinstance(warning_count, int)
                and not isinstance(warning_count, bool)
                and warning_count > 0
            ):
                collected.append(
                    {
                        "code": "PLUGIN_CONVERSION_WARNINGS",
                        "message": f"The Plugin reported {warning_count} conversion warning(s).",
                    }
                )
            return collected[:100]
        if not isinstance(value, list):
            return []
        normalized: list[dict[str, str]] = []
        for item in value[:100]:
            if isinstance(item, dict):
                message = item.get("message")
                code = item.get("code", "PLUGIN_WARNING")
            else:
                message = item
                code = "PLUGIN_WARNING"
            if (
                isinstance(message, str)
                and isinstance(code, str)
                and re.fullmatch(r"[A-Z][A-Z0-9_]+", code)
            ):
                normalized.append({"code": code, "message": message[:2000]})
        return normalized
