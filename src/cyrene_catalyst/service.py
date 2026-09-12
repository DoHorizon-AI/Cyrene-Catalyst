"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 service.py                                                      │
│  Module: cyrene_catalyst.service                                    │
│  Role: Product lifecycle authority and engine/artifact orchestration.│
│                                                                     │
│  模块职责：产品生命周期权威及引擎、制品编排。                             │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid4

from cyrene_catalyst.artifacts import LocalArtifactPlane
from cyrene_catalyst.domain import (
    CreateDatasetRequest,
    CreateDatasetVersionRequest,
    Dataset,
    DatasetVersion,
    DatasetVersionState,
    ErrorPreview,
    ExportFile,
    LineageEdge,
    MappingConfig,
    NormalizationConfig,
    NormalizedPreview,
    NormalizedPreviewItem,
    Preparation,
    PreparationReport,
    PreparationState,
    ProductFailure,
    RawPreview,
    SplitConfig,
    utc_now,
)
from cyrene_catalyst.engine import DataPreparationPort, PreparationOutput
from cyrene_catalyst.errors import CatalystError, DataEngineFailure
from cyrene_catalyst.store import CatalystStore

PREPARATION_ENGINE_BINDING_ID = "catalyst-prep-v1"
MAX_IMPORT_BYTES = 64 * 1024 * 1024
MAX_REPORT_ERRORS = 100


def request_hash(request: CreateDatasetRequest | CreateDatasetVersionRequest) -> str:
    """Hash a canonical command body for idempotency. | 为幂等生成规范请求摘要。"""

    body = request.model_dump_json(by_alias=True, exclude_none=True)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _build_report(total_rows: int, output: PreparationOutput) -> PreparationReport:
    """Build the persisted quality report with a capped error list. | 组装报告。"""

    return PreparationReport(
        total_rows=total_rows,
        valid_samples=len(output.samples) + len(output.duplicates),
        error_samples=len(output.errors),
        duplicate_samples=len(output.duplicates),
        unique_samples=len(output.samples),
        group_count=output.group_count,
        errors=output.errors[:MAX_REPORT_ERRORS],
    )


def _schema_fields(mapping: MappingConfig) -> list[str]:
    """Return the export schema fields for the DatasetVersion. | 导出模式字段。"""

    if mapping.mode == "conversation":
        return ["conversations"]
    fields = ["instruction"]
    if mapping.input is not None:
        fields.append("input")
    fields.append("output")
    return fields


class CatalystService:
    """Own Dataset state while delegating bytes and computation. | Dataset 状态权威服务。"""

    def __init__(
        self,
        store: CatalystStore,
        artifacts: LocalArtifactPlane,
        engine: DataPreparationPort,
    ) -> None:
        self.store = store
        self.artifacts = artifacts
        self.engine = engine

    def create_dataset(self, command: CreateDatasetRequest, idempotency_key: str | None) -> Dataset:
        """Create or idempotently replay a Dataset. | 创建或幂等重放 Dataset。"""

        digest = request_hash(command)
        replay_id = self.store.resolve_idempotency("create-dataset", idempotency_key, digest)
        if replay_id is not None:
            replay = self.store.get_dataset(UUID(replay_id))
            if replay is None:
                raise CatalystError(
                    code="CATALYST_STATE_CORRUPT",
                    title="Product state is inconsistent",
                    detail="The idempotency ledger references a missing Dataset.",
                    status=500,
                )
            return replay
        now = utc_now()
        dataset = Dataset(
            id=uuid4(),
            name=command.name,
            description=command.description,
            created_at=now,
            updated_at=now,
            resource_version=1,
        )
        self.store.save_dataset(dataset)
        self.store.remember_idempotency(
            scope="create-dataset",
            key=idempotency_key,
            request_hash=digest,
            resource_kind="dataset",
            resource_id=dataset.id,
        )
        return dataset

    def get_dataset(self, dataset_id: UUID) -> Dataset:
        """Read a Dataset or raise a stable not-found error. | 读取 Dataset。"""

        dataset = self.store.get_dataset(dataset_id)
        if dataset is None:
            raise CatalystError(
                code="CATALYST_DATASET_NOT_FOUND",
                title="Dataset not found",
                detail="No Dataset exists with the requested id.",
                status=404,
            )
        return dataset

    def create_version(
        self,
        dataset_id: UUID,
        command: CreateDatasetVersionRequest,
        idempotency_key: str | None,
    ) -> DatasetVersion:
        """Process bytes and publish an immutable Product version. | 处理并发布不可变版本。"""

        self.get_dataset(dataset_id)
        digest = request_hash(command)
        scope = f"create-version:{dataset_id}"
        replay_id = self.store.resolve_idempotency(scope, idempotency_key, digest)
        if replay_id is not None:
            return self.get_version(UUID(replay_id))

        now = utc_now()
        version = DatasetVersion(
            id=uuid4(),
            dataset_id=dataset_id,
            version=self.store.next_version(dataset_id),
            state=DatasetVersionState.PROCESSING,
            source=command.source,
            engine_binding_id=command.engine_binding_id,
            created_at=now,
            updated_at=now,
            resource_version=1,
        )
        self.store.save_version(version)
        self.store.remember_idempotency(
            scope=scope,
            key=idempotency_key,
            request_hash=digest,
            resource_kind="dataset-version",
            resource_id=version.id,
        )
        staged_path = self.artifacts.stage_path(f"{version.id}.parquet")
        try:
            source_path = self.artifacts.resolve(command.source)
            result = self.engine.transform(source_path, staged_path)
            output = self.artifacts.publish(staged_path, "dataset")
        except (CatalystError, DataEngineFailure) as exc:
            error = self._as_processing_error(exc, version.id)
            failed = version.model_copy(
                update={
                    "state": DatasetVersionState.FAILED,
                    "failure": ProductFailure(
                        code=error.code,
                        message=error.detail,
                        retryable=error.retryable,
                    ),
                    "updated_at": utc_now(),
                    "resource_version": 2,
                }
            )
            self.store.save_version(failed)
            raise error from exc
        finally:
            staged_path.unlink(missing_ok=True)

        published = version.model_copy(
            update={
                "state": DatasetVersionState.PUBLISHED,
                "output": output,
                "lineage": [
                    LineageEdge(from_digest=command.source.digest, to_digest=output.digest)
                ],
                "row_count": result.row_count,
                "schema_fields": result.schema_fields,
                "updated_at": utc_now(),
                "resource_version": 2,
            }
        )
        self.store.save_version(published)
        return published

    def get_version(self, version_id: UUID) -> DatasetVersion:
        """Read a published or failed DatasetVersion. | 读取已发布或失败版本。"""

        version = self.store.get_version(version_id)
        if version is None:
            raise CatalystError(
                code="CATALYST_VERSION_NOT_FOUND",
                title="DatasetVersion not found",
                detail="No DatasetVersion exists with the requested id.",
                status=404,
            )
        return version

    # ── Preparation workflow ────────────────────────────────────────────

    def list_datasets(self) -> list[Dataset]:
        """List Datasets for the UI. | 列出 Dataset。"""

        return self.store.list_datasets()

    def list_preparations(self, dataset_id: UUID) -> list[Preparation]:
        """List Datasets for the UI. | 列出整理会话。"""

        self.get_dataset(dataset_id)
        return self.store.list_preparations(dataset_id)

    def get_preparation(self, preparation_id: UUID) -> Preparation:
        """Read a Preparation or raise a stable not-found error. | 读取整理会话。"""

        preparation = self.store.get_preparation(preparation_id)
        if preparation is None:
            raise CatalystError(
                code="CATALYST_PREPARATION_NOT_FOUND",
                title="Preparation not found",
                detail="No Preparation exists with the requested id.",
                status=404,
            )
        return preparation

    def create_preparation(
        self,
        dataset_id: UUID,
        *,
        name: str,
        filename: str,
        data: bytes,
        idempotency_key: str | None,
    ) -> Preparation:
        """Ingest raw source bytes and stage a preparation. | 摄取字节并建立整理会话。"""

        self.get_dataset(dataset_id)
        scope = f"create-preparation:{dataset_id}"
        request_digest = hashlib.sha256(f"{name}\0{filename}\0".encode() + data).hexdigest()
        replay_id = self.store.resolve_idempotency(scope, idempotency_key, request_digest)
        if replay_id is not None:
            return self.get_preparation(UUID(replay_id))

        if len(data) > MAX_IMPORT_BYTES:
            raise CatalystError(
                code="CATALYST_IMPORT_TOO_LARGE",
                title="Import too large",
                detail=f"Imports are limited to {MAX_IMPORT_BYTES} bytes in this version.",
                status=413,
            )
        source = self.artifacts.ingest_bytes(
            data, f"import-{uuid4().hex}-{Path(filename).name or 'source'}"
        )
        inspection = self.engine.inspect(self.artifacts.resolve(source))
        now = utc_now()
        preparation = Preparation(
            id=uuid4(),
            dataset_id=dataset_id,
            name=name,
            state=PreparationState.STAGED,
            source=source,
            source_filename=filename,
            format=inspection.source_format,
            detected_fields=inspection.detected_fields,
            row_count=inspection.row_count,
            created_at=now,
            updated_at=now,
            resource_version=1,
        )
        self.store.save_preparation(preparation)
        self.store.remember_idempotency(
            scope=scope,
            key=idempotency_key,
            request_hash=request_digest,
            resource_kind="preparation",
            resource_id=preparation.id,
        )
        return preparation

    def preview_raw(self, preparation_id: UUID, *, offset: int, limit: int) -> RawPreview:
        """Preview imported rows. | 预览原始行。"""

        preparation = self.get_preparation(preparation_id)
        rows = self._load_rows(preparation)
        return RawPreview(
            total=len(rows),
            offset=offset,
            limit=limit,
            rows=rows[offset : offset + limit],
        )

    def preview_normalized(
        self, preparation_id: UUID, *, offset: int, limit: int
    ) -> NormalizedPreview:
        """Preview normalized samples with split labels. | 预览规范化样本。"""

        preparation = self._require_mapped(preparation_id)
        output = self._run(preparation)
        items = [
            NormalizedPreviewItem(
                sample_index=sample.index,
                group_key=sample.group_key,
                split=output.assignment.get(sample.index),
                content=sample.content,
                source_row_indexes=sample.source_row_indexes,
            )
            for sample in output.samples
        ]
        return NormalizedPreview(
            total=len(items), offset=offset, limit=limit, items=items[offset : offset + limit]
        )

    def preview_errors(self, preparation_id: UUID, *, offset: int, limit: int) -> ErrorPreview:
        """Preview rejected samples with concrete reasons. | 预览剔除样本。"""

        preparation = self._require_mapped(preparation_id)
        output = self._run(preparation)
        errors = output.errors
        return ErrorPreview(
            total=len(errors),
            offset=offset,
            limit=limit,
            items=errors[offset : offset + limit],
        )

    def configure_mapping(
        self,
        preparation_id: UUID,
        mapping: MappingConfig,
        normalization: NormalizationConfig,
    ) -> Preparation:
        """Apply mapping/normalization and recompute the quality report. | 应用映射并重算报告。"""

        preparation = self.get_preparation(preparation_id)
        self._require_state(
            preparation,
            {PreparationState.STAGED, PreparationState.MAPPED, PreparationState.SPLIT},
            "configure mapping",
        )
        self._validate_mapping_fields(preparation, mapping)
        output = self.engine.prepare(
            self.artifacts.resolve(preparation.source),
            preparation.format,
            mapping,
            normalization,
        )
        report = _build_report(preparation.row_count, output)
        updated = preparation.model_copy(
            update={
                "mapping": mapping,
                "normalization": normalization,
                "report": report,
                "split": None,
                "split_stats": None,
                "state": PreparationState.MAPPED,
                "updated_at": utc_now(),
                "resource_version": preparation.resource_version + 1,
            }
        )
        self.store.save_preparation(updated)
        return updated

    def configure_split(self, preparation_id: UUID, split: SplitConfig) -> Preparation:
        """Assign the deterministic group-level split. | 配置确定性划分。"""

        preparation = self.get_preparation(preparation_id)
        self._require_state(
            preparation,
            {PreparationState.MAPPED, PreparationState.SPLIT},
            "configure split",
        )
        assert preparation.mapping is not None
        assert preparation.normalization is not None
        output = self.engine.prepare(
            self.artifacts.resolve(preparation.source),
            preparation.format,
            preparation.mapping,
            preparation.normalization,
            split,
        )
        stats = output.split_stats
        if stats is None:
            raise DataEngineFailure("dataset preparation Plugin omitted split statistics")
        updated = preparation.model_copy(
            update={
                "split": split,
                "split_stats": stats,
                "state": PreparationState.SPLIT,
                "updated_at": utc_now(),
                "resource_version": preparation.resource_version + 1,
            }
        )
        self.store.save_preparation(updated)
        return updated

    def confirm_preparation(self, preparation_id: UUID) -> Preparation:
        """Record the manual confirmation gate. | 记录人工确认。"""

        preparation = self.get_preparation(preparation_id)
        self._require_state(preparation, {PreparationState.SPLIT}, "confirm")
        updated = preparation.model_copy(
            update={
                "state": PreparationState.CONFIRMED,
                "updated_at": utc_now(),
                "resource_version": preparation.resource_version + 1,
            }
        )
        self.store.save_preparation(updated)
        return updated

    def publish_preparation(
        self, preparation_id: UUID, idempotency_key: str | None
    ) -> tuple[Preparation, DatasetVersion]:
        """Publish the confirmed bundle as an immutable DatasetVersion. | 发布不可变版本。"""

        preparation = self.get_preparation(preparation_id)
        scope = f"publish-preparation:{preparation.id}"
        request_digest = self._publish_request_digest(preparation)
        replay_id = self.store.resolve_idempotency(scope, idempotency_key, request_digest)
        if replay_id is not None:
            return preparation, self.get_version(UUID(replay_id))
        self._require_state(preparation, {PreparationState.CONFIRMED}, "publish")

        assert preparation.mapping is not None  # state guarantees configuration
        assert preparation.normalization is not None
        assert preparation.split is not None
        staging = self.artifacts.stage_dir(f"prep-{preparation.id}")
        output = self.engine.prepare(
            self.artifacts.resolve(preparation.source),
            preparation.format,
            preparation.mapping,
            preparation.normalization,
            preparation.split,
            output_dir=staging,
        )
        assignment = output.assignment
        stats = output.split_stats
        if stats is None:
            raise DataEngineFailure("dataset preparation Plugin omitted split statistics")

        exports: list[ExportFile] = []
        lineage: list[LineageEdge] = []
        file_refs: dict[str, Any] = {}
        export_bundles: list[tuple[str, Literal["application/jsonl"]]] = [
            ("train.jsonl", "application/jsonl"),
            ("val.jsonl", "application/jsonl"),
            ("errors.jsonl", "application/jsonl"),
        ]
        for file_name, media_type in export_bundles:
            path = staging / file_name
            receipt = output.files.get(file_name)
            if not isinstance(receipt, dict):
                raise DataEngineFailure(f"dataset preparation Plugin omitted {file_name}")
            row_count = self._verify_plugin_export(path, file_name, receipt)
            if file_name == "errors.jsonl":
                path.write_bytes(
                    b"".join(
                        error.model_dump_json(by_alias=True).encode("utf-8") + b"\n"
                        for error in output.errors
                    )
                )
                row_count = len(output.errors)
            reference = self.artifacts.publish(path, "dataset")
            exports.append(
                ExportFile(
                    name=file_name,
                    artifact=reference,
                    row_count=row_count,
                    media_type=media_type,
                )
            )
            file_refs[file_name.removesuffix(".jsonl")] = reference.model_dump(
                by_alias=True, exclude_none=True
            )
            lineage.append(
                LineageEdge(from_digest=preparation.source.digest, to_digest=reference.digest)
            )

        manifest = {
            "manifestVersion": 1,
            "provenanceRefs": preparation.source_refs,
            "engineBindingId": PREPARATION_ENGINE_BINDING_ID,
            "source": preparation.source.model_dump(by_alias=True, exclude_none=True),
            "sourceFilename": preparation.source_filename,
            "format": preparation.format.value,
            "mapping": preparation.mapping.model_dump(by_alias=True, exclude_none=True),
            "normalization": preparation.normalization.model_dump(by_alias=True),
            "split": preparation.split.model_dump(by_alias=True),
            "splitStats": stats.model_dump(by_alias=True),
            "schemaFields": _schema_fields(preparation.mapping),
            "rowCounts": {
                "train": stats.train_samples,
                "val": stats.val_samples,
                "errors": len(output.errors),
            },
            "duplicates": output.duplicates,
            "sampleLineage": [
                {
                    "sampleIndex": sample.index,
                    "split": assignment[sample.index],
                    "groupKey": sample.group_key,
                    "sourceRowIndexes": sample.source_row_indexes,
                }
                for sample in output.samples
            ],
            "files": file_refs,
        }
        manifest_path = staging / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        manifest_ref = self.artifacts.publish(manifest_path, "dataset")
        exports.append(
            ExportFile(
                name="manifest.json",
                artifact=manifest_ref,
                row_count=None,
                media_type="application/json",
            )
        )
        lineage.append(
            LineageEdge(from_digest=preparation.source.digest, to_digest=manifest_ref.digest)
        )
        shutil.rmtree(staging, ignore_errors=True)

        now = utc_now()
        version = DatasetVersion(
            id=uuid4(),
            dataset_id=preparation.dataset_id,
            version=self.store.next_version(preparation.dataset_id),
            state=DatasetVersionState.PUBLISHED,
            source=preparation.source,
            output=manifest_ref,
            engine_binding_id=PREPARATION_ENGINE_BINDING_ID,
            lineage=lineage,
            source_refs=preparation.source_refs,
            row_count=stats.train_samples + stats.val_samples,
            schema_fields=_schema_fields(preparation.mapping),
            created_at=now,
            updated_at=now,
            resource_version=1,
        )
        self.store.save_version(version)
        published = preparation.model_copy(
            update={
                "state": PreparationState.PUBLISHED,
                "published_version_id": version.id,
                "exports": exports,
                "updated_at": utc_now(),
                "resource_version": preparation.resource_version + 1,
            }
        )
        self.store.save_preparation(published)
        self.store.remember_idempotency(
            scope=scope,
            key=idempotency_key,
            request_hash=request_digest,
            resource_kind="dataset-version",
            resource_id=version.id,
        )
        return published, version

    @staticmethod
    def _verify_plugin_export(path: Path, file_name: str, receipt: dict[str, Any]) -> int:
        """Verify an owner-written export before projecting it into Product storage."""

        row_count = receipt.get("row_count")
        size = receipt.get("size")
        digest = receipt.get("digest")
        if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
            raise DataEngineFailure(f"Plugin returned an invalid row count for {file_name}")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise DataEngineFailure(f"Plugin returned an invalid size for {file_name}")
        if not isinstance(digest, str) or not digest.startswith("sha256:"):
            raise DataEngineFailure(f"Plugin returned an invalid digest for {file_name}")
        if not path.is_file() or path.stat().st_size != size:
            raise DataEngineFailure(f"Plugin export {file_name} is missing or truncated")
        if f"sha256:{hashlib.sha256(path.read_bytes()).hexdigest()}" != digest:
            raise DataEngineFailure(f"Plugin export {file_name} failed digest verification")
        return row_count

    def resolve_export(self, preparation_id: UUID, file_name: str) -> ExportFile:
        """Resolve one export file for download. | 解析导出文件。"""

        preparation = self.get_preparation(preparation_id)
        for export in preparation.exports:
            if export.name == file_name:
                return export
        raise CatalystError(
            code="CATALYST_EXPORT_NOT_FOUND",
            title="Export not found",
            detail="No export file with the requested name exists for this preparation.",
            status=404,
        )

    # ── Preparation internals ───────────────────────────────────────────

    def _load_rows(self, preparation: Preparation) -> list[dict[str, Any]]:
        """Re-parse rows from the immutable source artifact. | 从源制品重新解析。"""

        inspection = self.engine.inspect(self.artifacts.resolve(preparation.source))
        if inspection.source_format is not preparation.format:
            raise DataEngineFailure("dataset preparation Plugin changed the source format")
        return inspection.rows

    def _run(self, preparation: Preparation) -> PreparationOutput:
        """Re-run the deterministic pipeline for a configured preparation. | 重跑确定性流水线。"""

        assert preparation.mapping is not None  # guarded by _require_mapped
        assert preparation.normalization is not None
        return self.engine.prepare(
            self.artifacts.resolve(preparation.source),
            preparation.format,
            preparation.mapping,
            preparation.normalization,
            preparation.split,
        )

    def _require_mapped(self, preparation_id: UUID) -> Preparation:
        """Load a preparation that has a mapping configured. | 要求已配置映射。"""

        preparation = self.get_preparation(preparation_id)
        if preparation.state in {
            PreparationState.STAGED,
        }:
            raise CatalystError(
                code="CATALYST_PREPARATION_STATE_CONFLICT",
                title="Preparation state conflict",
                detail="Configure a field mapping before requesting normalized samples.",
                status=409,
            )
        return preparation

    @staticmethod
    def _require_state(
        preparation: Preparation, allowed: set[PreparationState], action: str
    ) -> None:
        """Reject actions outside the workflow state machine. | 状态机守卫。"""

        if preparation.state not in allowed:
            raise CatalystError(
                code="CATALYST_PREPARATION_STATE_CONFLICT",
                title="Preparation state conflict",
                detail=(
                    f"Cannot {action} while the preparation is {preparation.state.value}; "
                    f"expected one of {sorted(state.value for state in allowed)}."
                ),
                status=409,
            )

    @staticmethod
    def _publish_request_digest(preparation: Preparation) -> str:
        """Stable digest of publish inputs, independent of outputs. | 发布输入摘要。"""

        payload = json.dumps(
            {
                "preparationId": str(preparation.id),
                "source": preparation.source.digest,
                "mapping": preparation.mapping.model_dump(by_alias=True, exclude_none=True)
                if preparation.mapping
                else None,
                "normalization": preparation.normalization.model_dump(by_alias=True)
                if preparation.normalization
                else None,
                "split": preparation.split.model_dump(by_alias=True) if preparation.split else None,
            },
            sort_keys=True,
            ensure_ascii=False,
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    @staticmethod
    def _validate_mapping_fields(preparation: Preparation, mapping: MappingConfig) -> None:
        """Reject mappings that reference unknown fields. | 拒绝未知字段映射。"""

        referenced: set[str] = set()
        for source in (mapping.instruction, mapping.input, mapping.output):
            if source is not None and source.field is not None:
                referenced.add(source.field)
        for field in (
            mapping.conversation_value_field,
            mapping.conversation_from_field,
            mapping.group_by,
        ):
            if field is not None:
                referenced.add(field)
        unknown = referenced - set(preparation.detected_fields)
        if unknown:
            raise CatalystError(
                code="CATALYST_MAPPING_FIELD_UNKNOWN",
                title="Mapping references unknown fields",
                detail=(
                    f"Fields {sorted(unknown)} do not exist in the imported source; "
                    f"available fields: {preparation.detected_fields}."
                ),
                status=422,
            )

    @staticmethod
    def _as_processing_error(
        exc: CatalystError | DataEngineFailure, version_id: UUID
    ) -> CatalystError:
        resource_ref = f"/api/v1/dataset-versions/{version_id}"
        if isinstance(exc, CatalystError):
            return CatalystError(
                code=exc.code,
                title=exc.title,
                detail=exc.detail,
                status=exc.status,
                retryable=exc.retryable,
                resource_ref=resource_ref,
            )
        return CatalystError(
            code="CATALYST_DATA_PROCESSING_FAILED",
            title="Data processing failed",
            detail=str(exc),
            status=422,
            resource_ref=resource_ref,
        )
