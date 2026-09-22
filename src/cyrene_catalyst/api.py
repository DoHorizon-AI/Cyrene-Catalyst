"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 api.py                                                          │
│  Module: cyrene_catalyst.api                                        │
│  Role: Versioned HTTP adapter for the Catalyst Product contract.     │
│                                                                     │
│  模块职责：Catalyst 产品契约的版本化 HTTP 适配器。                       │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID, uuid4

from fastapi import FastAPI, Header, Query, Request
from fastapi import Path as ApiPath
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, Response

from cyrene_catalyst.artifacts import LocalArtifactPlane
from cyrene_catalyst.domain import (
    ConfigureMappingRequest,
    ConfigureSplitRequest,
    CreateDatasetRequest,
    CreateDatasetVersionRequest,
    Dataset,
    DatasetPreview,
    DatasetVersion,
    ErrorPreview,
    NormalizedPreview,
    Preparation,
    ProblemDetails,
    PublishPreparationResponse,
    RawPreview,
)
from cyrene_catalyst.engine import DataPreparationPort, data_preparation_from_environment
from cyrene_catalyst.errors import CatalystError, DataEngineFailure, map_catalyst_error
from cyrene_catalyst.logging import (
    emit_diagnostic_error,
    parse_w3c_traceparent,
    sanitize_request_id,
)
from cyrene_catalyst.lifecycle import (
    FeedbackImportRequest,
    HandoffReceipt,
    LifecycleActions,
)
from cyrene_catalyst.service import CatalystService
from cyrene_catalyst.store import CatalystStore

_UI_HTML = (Path(__file__).parent / "ui" / "index.html").read_text(encoding="utf-8")


def create_app(
    *,
    database_path: Path,
    artifact_root: Path,
    engine: DataPreparationPort | None = None,
    yield_url: str | None = None,
) -> FastAPI:
    """Build an app with explicit durable adapters. | 使用显式持久化适配器创建应用。"""

    store = CatalystStore(database_path)
    service = CatalystService(
        store=store,
        artifacts=LocalArtifactPlane(artifact_root),
        engine=engine or data_preparation_from_environment(),
    )
    app = FastAPI(title="Cyrene Catalyst Product API", version="1.0.0")
    app.state.catalyst_store = store
    app.state.catalyst_service = service
    lifecycle = LifecycleActions(service, yield_url)
    app.state.lifecycle_actions = lifecycle

    @app.middleware("http")
    async def propagate_trace(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        parsed_trace = parse_w3c_traceparent(request.headers.get("traceparent"))
        if parsed_trace:
            trace_id, span_id = parsed_trace
        else:
            trace_id = uuid4().hex
            span_id = "0000000000000001"

        raw_req_id = request.headers.get("x-request-id")
        request_id = sanitize_request_id(raw_req_id) or f"req-{uuid4().hex[:12]}"

        request.state.trace_id = trace_id
        request.state.span_id = span_id
        request.state.request_id = request_id

        response = await call_next(request)
        response.headers["traceparent"] = f"00-{trace_id}-{span_id}-01"
        response.headers["x-request-id"] = request_id
        return response

    @app.exception_handler(CatalystError)
    async def product_error(request: Request, exc: CatalystError) -> JSONResponse:
        mapping = map_catalyst_error(exc.code)
        canonical_code = mapping["code"]
        trace_id = getattr(request.state, "trace_id", None) or uuid4().hex
        span_id = getattr(request.state, "span_id", "0000000000000001")
        request_id = getattr(request.state, "request_id", None)
        emit_diagnostic_error(
            "catalyst.error",
            canonical_code,
            f"{exc.title}: {exc.detail}",
            trace_id=trace_id,
            span_id=span_id,
            attributes={
                "http.target": request.url.path,
                "http.status_code": exc.status,
                "request_id": request_id,
                "cause_kind": mapping.get("cause_kind"),
                "recovery_action": mapping.get("recovery_action"),
                "legacy_code": exc.code,
            },
        )
        problem = ProblemDetails(
            type=f"https://errors.cyrene.dev/catalyst/{canonical_code.lower()}",
            title=exc.title,
            status=exc.status,
            detail=exc.detail,
            instance=request.url.path,
            code=exc.code,
            retryable=exc.retryable,
            trace_id=trace_id,
            resource_ref=exc.resource_ref,
            request_id=request_id,
            recovery_action=mapping.get("recovery_action"),
        )
        return JSONResponse(
            status_code=exc.status,
            content=problem.model_dump(by_alias=True, exclude_none=True, mode="json"),
            media_type="application/problem+json",
        )

    @app.exception_handler(DataEngineFailure)
    async def engine_error(request: Request, _exc: DataEngineFailure) -> JSONResponse:
        mapping = map_catalyst_error("CATALYST_DATA_PROCESSING_FAILED")
        canonical_code = mapping["code"]
        trace_id = getattr(request.state, "trace_id", None) or uuid4().hex
        span_id = getattr(request.state, "span_id", "0000000000000001")
        request_id = getattr(request.state, "request_id", None)
        emit_diagnostic_error(
            "catalyst.engine_failure",
            canonical_code,
            "The dataset preparation engine rejected the requested data or schema.",
            trace_id=trace_id,
            span_id=span_id,
            attributes={
                "http.target": request.url.path,
                "http.status_code": 422,
                "request_id": request_id,
                "cause_kind": mapping.get("cause_kind"),
                "recovery_action": mapping.get("recovery_action"),
            },
        )
        problem = ProblemDetails(
            type="https://errors.cyrene.dev/catalyst/data-processing-failed",
            title="Data processing failed",
            status=422,
            detail="The dataset preparation engine rejected the requested data or schema.",
            instance=request.url.path,
            code="CATALYST_DATA_PROCESSING_FAILED",
            retryable=False,
            trace_id=trace_id,
            request_id=request_id,
            recovery_action=mapping.get("recovery_action"),
        )
        return JSONResponse(
            status_code=422,
            content=problem.model_dump(by_alias=True, mode="json"),
            media_type="application/problem+json",
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, _exc: RequestValidationError) -> JSONResponse:
        mapping = map_catalyst_error("CATALYST_REQUEST_INVALID")
        canonical_code = mapping["code"]
        trace_id = getattr(request.state, "trace_id", None) or uuid4().hex
        span_id = getattr(request.state, "span_id", "0000000000000001")
        request_id = getattr(request.state, "request_id", None)
        emit_diagnostic_error(
            "catalyst.validation_error",
            canonical_code,
            "The request does not conform to the Catalyst Product API v1 contract.",
            trace_id=trace_id,
            span_id=span_id,
            attributes={
                "http.target": request.url.path,
                "http.status_code": 422,
                "request_id": request_id,
                "cause_kind": mapping.get("cause_kind"),
                "recovery_action": mapping.get("recovery_action"),
            },
        )
        problem = ProblemDetails(
            type="https://errors.cyrene.dev/catalyst/request-invalid",
            title="Request validation failed",
            status=422,
            detail="The request does not conform to the Catalyst Product API v1 contract.",
            instance=request.url.path,
            code="CATALYST_REQUEST_INVALID",
            retryable=False,
            trace_id=trace_id,
            request_id=request_id,
            recovery_action=mapping.get("recovery_action"),
        )
        return JSONResponse(
            status_code=422,
            content=problem.model_dump(by_alias=True, mode="json"),
            media_type="application/problem+json",
        )

    @app.post(
        "/api/v1/datasets",
        response_model=Dataset,
        response_model_exclude_none=True,
        status_code=201,
    )
    def create_dataset(
        command: CreateDatasetRequest,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=200),
    ) -> Dataset:
        return service.create_dataset(command, idempotency_key)

    @app.get(
        "/api/v1/datasets/{datasetId}",
        response_model=Dataset,
        response_model_exclude_none=True,
    )
    def get_dataset(dataset_id: Annotated[UUID, ApiPath(alias="datasetId")]) -> Dataset:
        return service.get_dataset(dataset_id)

    @app.post(
        "/api/v1/datasets/{datasetId}/versions",
        response_model=DatasetVersion,
        response_model_exclude_none=True,
        status_code=201,
    )
    def create_version(
        dataset_id: Annotated[UUID, ApiPath(alias="datasetId")],
        command: CreateDatasetVersionRequest,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=200),
    ) -> DatasetVersion:
        return service.create_version(dataset_id, command, idempotency_key)

    @app.get(
        "/api/v1/dataset-versions/{versionId}",
        response_model=DatasetVersion,
        response_model_exclude_none=True,
    )
    def get_version(version_id: Annotated[UUID, ApiPath(alias="versionId")]) -> DatasetVersion:
        return service.get_version(version_id)

    @app.get(
        "/api/v1/dataset-versions/{versionId}/preview",
        response_model=DatasetPreview,
        response_model_exclude_none=True,
    )
    def preview_version(
        version_id: Annotated[UUID, ApiPath(alias="versionId")],
        limit: Annotated[int, Query(ge=1, le=100)] = 10,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> DatasetPreview:
        return service.preview_version(version_id, limit=limit, offset=offset)

    @app.get("/", include_in_schema=False, response_class=HTMLResponse)
    def root_ui() -> HTMLResponse:
        return HTMLResponse(_UI_HTML)

    @app.get(
        "/api/v1/datasets",
        response_model=list[Dataset],
        response_model_exclude_none=True,
    )
    def list_datasets() -> list[Dataset]:
        return service.list_datasets()

    @app.get(
        "/api/v1/datasets/{datasetId}/versions",
        response_model=list[DatasetVersion],
        response_model_exclude_none=True,
    )
    def list_versions(
        dataset_id: Annotated[UUID, ApiPath(alias="datasetId")],
    ) -> list[DatasetVersion]:
        return service.list_versions(dataset_id)

    @app.get(
        "/api/v1/datasets/{datasetId}/preparations",
        response_model=list[Preparation],
        response_model_exclude_none=True,
    )
    def list_preparations(
        dataset_id: Annotated[UUID, ApiPath(alias="datasetId")],
    ) -> list[Preparation]:
        return service.list_preparations(dataset_id)

    @app.post(
        "/api/v1/datasets/{datasetId}/preparations",
        response_model=Preparation,
        response_model_exclude_none=True,
        status_code=201,
    )
    async def create_preparation(
        dataset_id: Annotated[UUID, ApiPath(alias="datasetId")],
        request: Request,
        name: Annotated[str, Query(min_length=1, max_length=200)],
        filename: Annotated[str, Query(max_length=300)] = "source",
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=200),
    ) -> Preparation:
        data = await request.body()
        return service.create_preparation(
            dataset_id,
            name=name,
            filename=filename,
            data=data,
            idempotency_key=idempotency_key,
            content_type=request.headers.get("content-type"),
        )

    @app.get(
        "/api/v1/preparations/{preparationId}",
        response_model=Preparation,
        response_model_exclude_none=True,
    )
    def get_preparation(
        preparation_id: Annotated[UUID, ApiPath(alias="preparationId")],
    ) -> Preparation:
        return service.get_preparation(preparation_id)

    @app.get("/api/v1/preparations/{preparationId}/samples", response_model=None)
    def preview_samples(
        preparation_id: Annotated[UUID, ApiPath(alias="preparationId")],
        stage: Annotated[Literal["raw", "normalized", "errors"], Query()] = "raw",
        offset: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=200)] = 20,
    ) -> RawPreview | NormalizedPreview | ErrorPreview:
        if stage == "raw":
            return service.preview_raw(preparation_id, offset=offset, limit=limit)
        if stage == "normalized":
            return service.preview_normalized(preparation_id, offset=offset, limit=limit)
        return service.preview_errors(preparation_id, offset=offset, limit=limit)

    @app.patch(
        "/api/v1/preparations/{preparationId}/mapping",
        response_model=Preparation,
        response_model_exclude_none=True,
    )
    def configure_mapping(
        preparation_id: Annotated[UUID, ApiPath(alias="preparationId")],
        command: ConfigureMappingRequest,
    ) -> Preparation:
        return service.configure_mapping(preparation_id, command.mapping, command.normalization)

    @app.patch(
        "/api/v1/preparations/{preparationId}/split",
        response_model=Preparation,
        response_model_exclude_none=True,
    )
    def configure_split(
        preparation_id: Annotated[UUID, ApiPath(alias="preparationId")],
        command: ConfigureSplitRequest,
    ) -> Preparation:
        return service.configure_split(preparation_id, command.split)

    @app.post(
        "/api/v1/preparations/{preparationId}/confirm",
        response_model=Preparation,
        response_model_exclude_none=True,
    )
    def confirm_preparation(
        preparation_id: Annotated[UUID, ApiPath(alias="preparationId")],
    ) -> Preparation:
        return service.confirm_preparation(preparation_id)

    @app.post(
        "/api/v1/preparations/{preparationId}/publish",
        response_model=PublishPreparationResponse,
        response_model_exclude_none=True,
        status_code=201,
    )
    def publish_preparation(
        preparation_id: Annotated[UUID, ApiPath(alias="preparationId")],
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key", max_length=200),
    ) -> PublishPreparationResponse:
        preparation, version = service.publish_preparation(preparation_id, idempotency_key)
        return PublishPreparationResponse(preparation=preparation, dataset_version=version)

    @app.get(
        "/api/v1/preparations/{preparationId}/exports",
        response_model=None,
    )
    def list_exports(
        preparation_id: Annotated[UUID, ApiPath(alias="preparationId")],
    ) -> list[dict[str, object]]:
        preparation = service.get_preparation(preparation_id)
        return [
            {
                "name": export.name,
                "artifact": export.artifact.model_dump(by_alias=True, exclude_none=True),
                "rowCount": export.row_count,
                "mediaType": export.media_type,
            }
            for export in preparation.exports
        ]

    @app.get("/api/v1/preparations/{preparationId}/exports/{fileName}")
    def download_export(
        preparation_id: Annotated[UUID, ApiPath(alias="preparationId")],
        file_name: Annotated[str, ApiPath(alias="fileName")],
    ) -> Response:
        export = service.resolve_export(preparation_id, file_name)
        path = service.artifacts.resolve(export.artifact)
        return Response(
            content=path.read_bytes(),
            media_type=export.media_type,
            headers={"Content-Disposition": f'attachment; filename="{export.name}"'},
        )

    @app.post(
        "/api/v1/preparations/{preparationId}/yield-draft",
        response_model=HandoffReceipt,
        response_model_exclude_none=True,
    )
    def create_yield_draft(
        preparation_id: Annotated[UUID, ApiPath(alias="preparationId")],
    ) -> HandoffReceipt:
        preparation = service.get_preparation(preparation_id)
        if preparation.published_version_id is None:
            raise CatalystError(
                code="CATALYST_VERSION_NOT_PUBLISHED",
                title="DatasetVersion required",
                detail="Publish the selected preparation before sending.",
                status=409,
            )
        return lifecycle.send_to_yield(preparation.published_version_id)

    @app.post(
        "/api/v1/dataset-versions/{versionId}/actions/send-to-yield", response_model=HandoffReceipt
    )
    def send_version(version_id: Annotated[UUID, ApiPath(alias="versionId")]) -> HandoffReceipt:
        return lifecycle.send_to_yield(version_id)

    @app.post("/api/v1/feedback-imports", response_model=HandoffReceipt, status_code=201)
    def import_feedback(
        command: FeedbackImportRequest,
        idempotency_key: str | None = Header(default=None, max_length=200),
    ) -> HandoffReceipt:
        return lifecycle.import_feedback(command, idempotency_key)

    return app
