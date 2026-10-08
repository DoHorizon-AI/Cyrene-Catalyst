"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 test_training_curation_api.py                                    │
│  Module: tests.test_training_curation_api                            │
│  Role: Freeze the v0.3 curation HTTP boundary and its safety checks. │
│                                                                     │
│  模块职责：验证 v0.3 训练数据整理 HTTP 契约与审核边界。                 │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from openapi_spec_validator import validate as validate_openapi_spec
from openapi_spec_validator.readers import read_from_filename
from pydantic import ValidationError

from cyrene_catalyst import create_app
from cyrene_catalyst.data_tools_api import (
    CreateProcessingRunRequest,
    EditTrainingRecordsRequest,
    PublishDataToolsVersionRequest,
)
from cyrene_catalyst.data_tools_domain import (
    ContentRevision,
    ContentRevisionState,
    ProcessingOperation,
    ProcessingRun,
    ProcessingRunState,
    ReviewItem,
    ReviewItemKind,
    ReviewItemsPage,
    SourceParseReport,
    SourceParseReportState,
    TrainingCurationCounts,
    TrainingDataSnapshot,
    TrainingRecordsPage,
    utc_now,
)


def _approved_policy() -> dict[str, Any]:
    return {
        "allowKnowledge": False,
        "allowTraining": True,
        "allowedPrincipalRefs": [],
        "allowedUsePurposes": ["model_training"],
    }


def _content_revision(dataset_id: UUID, revision_id: UUID) -> ContentRevision:
    return ContentRevision(
        id=revision_id,
        dataset_id=dataset_id,
        revision=1,
        source_revision_ids=[],
        state=ContentRevisionState.DRAFT,
        blocks=[],
        created_at=datetime.now(UTC),
        resource_version=1,
    )


def test_curation_run_request_requires_source_recipe_and_scoped_policy() -> None:
    source_id = uuid4()
    request = CreateProcessingRunRequest.model_validate(
        {
            "operation": "curateTrainingData",
            "sourceRevisionIds": [str(source_id)],
            "config": {
                "curation": {
                    "id": "training-curation-v1",
                    "version": "1",
                    "format": "auto",
                    "fieldMapping": {"output": "answer"},
                    "roleMapping": {"human": "user"},
                    "unicodeNormalization": "NFC",
                },
                "sourcePolicies": {str(source_id): _approved_policy()},
            },
        }
    )
    assert request.operation == ProcessingOperation.CURATE_TRAINING_DATA
    assert request.config.source_policies[str(source_id)].allow_training

    with pytest.raises(ValidationError):
        CreateProcessingRunRequest.model_validate(
            {
                "operation": "curateTrainingData",
                "sourceRevisionIds": [str(source_id)],
                "config": {
                    "curation": {
                        "id": "training-curation-v1",
                        "version": "1",
                        "format": "auto",
                        "fieldMapping": {"output": ""},
                    },
                },
            }
        )

    with pytest.raises(ValidationError, match="sourceRevisionId"):
        CreateProcessingRunRequest.model_validate(
            {
                "operation": "curateTrainingData",
                "config": {"curation": {"id": "x", "version": "1", "format": "auto"}},
            }
        )
    with pytest.raises(ValidationError, match="sourcePolicies keys"):
        CreateProcessingRunRequest.model_validate(
            {
                "operation": "curateTrainingData",
                "sourceRevisionIds": [str(source_id)],
                "config": {
                    "curation": {"id": "x", "version": "1", "format": "auto"},
                    "sourcePolicies": {str(uuid4()): _approved_policy()},
                },
            }
        )


def test_training_record_edit_batch_is_bounded_and_unambiguous() -> None:
    with pytest.raises(ValidationError, match="remap edit"):
        EditTrainingRecordsRequest.model_validate(
            {"resourceVersion": 1, "edits": [{"recordId": "r1", "action": "remap"}]}
        )
    with pytest.raises(ValidationError, match="only once"):
        EditTrainingRecordsRequest.model_validate(
            {
                "resourceVersion": 1,
                "edits": [
                    {"recordId": "r1", "action": "approve"},
                    {"recordId": "r1", "action": "exclude"},
                ],
            }
        )
    with pytest.raises(ValidationError):
        EditTrainingRecordsRequest.model_validate(
            {
                "resourceVersion": 1,
                "edits": [{"recordId": "r1", "action": "approve", "unexpected": True}],
            }
        )
    with pytest.raises(ValidationError):
        EditTrainingRecordsRequest.model_validate(
            {
                "resourceVersion": 1,
                "edits": [
                    {
                        "recordId": "r1",
                        "action": "remap",
                        "fieldMapping": {"output": ""},
                    }
                ],
            }
        )


def test_sft_only_publication_request_omits_knowledge_run() -> None:
    request = PublishDataToolsVersionRequest.model_validate(
        {"contentRevisionId": str(uuid4()), "sftRunId": str(uuid4())}
    )
    assert request.knowledge_run_id is None


def test_training_record_and_review_queue_routes_forward_bounded_filters(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
    )
    dataset_id = uuid4()
    revision_id = uuid4()
    revision = _content_revision(dataset_id, revision_id)
    service = app.state.data_tools_service
    calls: dict[str, Any] = {}
    record = {
        "schemaVersion": "cyrene.training-record.v1",
        "id": "record-1",
        "sampleId": "sample-1",
        "sourceRevisionId": str(uuid4()),
        "sourceFamilyId": "family-1",
        "ordinal": 0,
        "locator": {"itemRef": "line:1"},
        "rawRecord": {"prompt": "Question", "completion": "Answer"},
        "rawLine": None,
        "detectedFormat": "promptCompletion",
        "normalized": {"messages": [{"role": "user", "content": "Question"}]},
        "disposition": "review",
        "issues": [
            {"code": "training.short_answer", "message": "Review answer.", "severity": "warning"}
        ],
        "contentDigest": "sha256:" + "a" * 64,
        "recipeDigest": "sha256:" + "b" * 64,
        "processingHistory": [{"operation": "normalize", "recipeDigest": "sha256:" + "b" * 64}],
        "policy": _approved_policy(),
    }
    page = TrainingRecordsPage(
        revision_id=revision_id,
        offset=2,
        limit=3,
        total=5,
        records=[record],
    )

    monkeypatch.setattr(service, "get_content_revision", lambda *_args, **_kwargs: revision)

    def list_records(**kwargs: Any) -> TrainingRecordsPage:
        calls["records"] = kwargs
        return page

    def list_queue(*_args: Any, **kwargs: Any) -> ReviewItemsPage:
        calls["queue"] = kwargs
        return ReviewItemsPage(items=[], total=7, offset=1, limit=2)

    monkeypatch.setattr(service, "list_training_records", list_records, raising=False)
    monkeypatch.setattr(service, "list_review_items_page", list_queue, raising=False)
    monkeypatch.setattr(service, "list_generated_drafts", lambda *_args, **_kwargs: [])

    try:
        with TestClient(app) as client:
            queue_response = client.get(
                f"/api/v1/datasets/{dataset_id}/review-queue",
                params={
                    "kind": "TRAINING_STRUCTURE",
                    "code": "training.role",
                    "offset": 1,
                    "limit": 2,
                },
            )
            assert queue_response.status_code == 200
            assert queue_response.json()["total"] == 7
            assert queue_response.json()["offset"] == 1
            assert calls["queue"]["kind"] == "TRAINING_STRUCTURE"
            assert calls["queue"]["code"] == "training.role"

            records_response = client.get(
                f"/api/v1/content-revisions/{revision_id}/training-records",
                params={
                    "offset": 2,
                    "limit": 3,
                    "disposition": "review",
                    "issueCode": "training.short_answer",
                },
            )
            assert records_response.status_code == 200
            assert records_response.json()["records"][0]["rawRecord"]["completion"] == "Answer"
            assert calls["records"]["dataset_id"] == dataset_id
            assert calls["records"]["offset"] == 2
            assert calls["records"]["disposition"] == "review"
            assert calls["records"]["issue_code"] == "training.short_answer"

            invalid_filter = client.get(
                f"/api/v1/datasets/{dataset_id}/review-queue",
                params={"kind": "NOT_A_REVIEW_KIND"},
            )
            assert invalid_filter.status_code == 422
    finally:
        app.state.data_tools_service.close()
        app.state.catalyst_service.close()


def test_v03_public_dtos_and_openapi_match_runtime(tmp_path: Path) -> None:
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
    )
    product_root = Path(__file__).parents[1] / "contracts/product/v1"
    schema = json.loads((product_root / "data-tools.schema.json").read_text())
    contract, base_uri = read_from_filename(str(product_root / "openapi.yaml"))
    runtime = app.openapi()

    validate_openapi_spec(contract, base_uri=base_uri)
    component_pairs = {
        "ContentRevision": "ContentRevision",
        "TrainingRecordsPageResponse": "TrainingRecordsPage",
        "TrainingRecordEnvelopeResponse": "TrainingRecordEnvelope",
        "TrainingRecordIssueResponse": "TrainingRecordIssue",
        "ReviewItem": "ReviewItem",
        "ReviewQueueResponse": "ReviewQueueResponse",
        "EditTrainingRecordRequest": "EditTrainingRecordRequest",
        "EditTrainingRecordsRequest": "EditTrainingRecordsRequest",
        "PublishDataToolsVersionRequest": "PublishDataToolsVersionRequest",
    }
    for runtime_name, contract_name in component_pairs.items():
        assert set(runtime["components"]["schemas"][runtime_name]["properties"]) == set(
            schema["$defs"][contract_name]["properties"]
        )
    config_properties = schema["$defs"]["CreateProcessingRunRequest"]["properties"]["config"][
        "properties"
    ]
    assert set(runtime["components"]["schemas"]["ProcessingRunConfig"]["properties"]) == set(
        config_properties
    )
    assert set(
        runtime["components"]["schemas"]["TrainingCurationRecipeConfig"]["properties"]
    ) == set(config_properties["curation"]["properties"])

    records_path = "/api/v1/content-revisions/{revisionId}/training-records"
    assert runtime["paths"][records_path]["get"]["operationId"] == "listTrainingRecords"
    assert contract["paths"][records_path]["get"]["operationId"] == "listTrainingRecords"
    edit_path = "/api/v1/content-revisions/{revisionId}/training-records:edit"
    assert runtime["paths"][edit_path]["post"]["operationId"] == "editTrainingRecords"
    assert contract["paths"][edit_path]["post"]["operationId"] == "editTrainingRecords"

    app.state.data_tools_service.close()
    app.state.catalyst_service.close()


def test_training_edit_route_creates_child_and_never_uses_queue_acknowledgement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
    )
    dataset_id = uuid4()
    revision_id = uuid4()
    revision = _content_revision(dataset_id, revision_id)
    child = ContentRevision(
        id=uuid4(),
        dataset_id=dataset_id,
        revision=2,
        parent_revision_id=revision_id,
        source_revision_ids=[],
        state=ContentRevisionState.DRAFT,
        blocks=[],
        created_at=datetime.now(UTC),
        resource_version=1,
    )
    service = app.state.data_tools_service
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(service, "get_content_revision", lambda *_args, **_kwargs: revision)

    def edit_records(**kwargs: Any) -> ContentRevision:
        calls.append(kwargs)
        return child

    monkeypatch.setattr(service, "edit_training_records", edit_records, raising=False)

    try:
        with TestClient(app) as client:
            response = client.post(
                f"/api/v1/content-revisions/{revision_id}/training-records:edit",
                json={
                    "resourceVersion": 1,
                    "edits": [
                        {
                            "recordId": "record-1",
                            "action": "remap",
                            "format": "promptCompletion",
                            "fieldMapping": {"prompt": "question", "completion": "answer"},
                        }
                    ],
                    "note": "Mapped the source fields after inspection.",
                },
            )
            assert response.status_code == 201
            assert response.json()["parentRevisionId"] == str(revision_id)
            assert calls[0]["resource_version"] == 1
            assert calls[0]["edits"][0]["recordId"] == "record-1"

            acknowledgement_is_not_an_edit = client.post(
                f"/api/v1/content-revisions/{revision_id}/training-records:edit",
                json={
                    "resourceVersion": 1,
                    "edits": [{"recordId": "record-1", "action": "ACKNOWLEDGE"}],
                },
            )
            assert acknowledgement_is_not_an_edit.status_code == 422

            invalid_remap = client.post(
                f"/api/v1/content-revisions/{revision_id}/training-records:edit",
                json={"resourceVersion": 1, "edits": [{"recordId": "record-2", "action": "remap"}]},
            )
            assert invalid_remap.status_code == 422
            assert len(calls) == 1
    finally:
        app.state.data_tools_service.close()
        app.state.catalyst_service.close()


def test_http_acknowledgement_does_not_approve_invalid_training_records(
    tmp_path: Path,
) -> None:
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
    )
    service = app.state.data_tools_service

    try:
        with TestClient(app) as client:
            dataset = client.post("/api/v1/datasets", json={"name": "training-review-gate"})
            assert dataset.status_code == 201
            dataset_id = UUID(dataset.json()["id"])
            upload = client.post(
                f"/api/v1/datasets/{dataset_id}/sources/batch",
                files=[
                    (
                        "files[]",
                        ("malformed.jsonl", b'{"messages":[]\n', "application/x-ndjson"),
                    )
                ],
            )
            assert upload.status_code == 200
            source_id = UUID(upload.json()["items"][0]["source"]["id"])

            record_id = "invalid-record-1"
            run = ProcessingRun(
                id=uuid4(),
                dataset_id=dataset_id,
                operation=ProcessingOperation.CURATE_TRAINING_DATA,
                state=ProcessingRunState.SUCCEEDED,
                source_revision_ids=[source_id],
                recipe={"curation": {"id": "training-curation-v1", "version": "1"}},
                started_at=utc_now(),
                finished_at=utc_now(),
            )
            service.store.create_run(run)

            record = {
                "schemaVersion": "cyrene.training-record.v1",
                "id": record_id,
                "sampleId": f"{source_id}:0",
                "conversationId": None,
                "sourceRevisionId": str(source_id),
                "sourceFamilyId": f"source:{source_id}",
                "ordinal": 0,
                "locator": {"itemRef": "line:1"},
                "rawRecord": {"messages": []},
                "rawLine": '{"messages":[]',
                "detectedFormat": "unknown",
                "normalized": {"messages": []},
                "disposition": "review",
                "issues": [
                    {
                        "code": "training.invalid_json",
                        "message": "The source line is malformed JSON.",
                        "severity": "error",
                    }
                ],
                "contentDigest": "sha256:" + "a" * 64,
                "recipeDigest": "sha256:" + "b" * 64,
                "processingHistory": [
                    {
                        "operation": "normalize",
                        "mode": "deterministic",
                        "recipeDigest": "sha256:" + "b" * 64,
                    }
                ],
                "policy": {
                    "allowKnowledge": False,
                    "allowTraining": True,
                    "allowedPrincipalRefs": [],
                    "allowedUsePurposes": ["model_training"],
                },
            }
            staged = service.artifacts.stage_path(f"training-records-{uuid4()}.jsonl")
            staged.write_text(json.dumps(record) + "\n", encoding="utf-8")
            record_artifact = service.artifacts.publish(staged, "source-parse-blocks")
            staged.unlink(missing_ok=True)

            revision_id = uuid4()
            counts = TrainingCurationCounts(
                total=1,
                recognized=0,
                format_errors=1,
                duplicate_candidates=0,
                pending_review=1,
                excluded=0,
                eligible=0,
            )
            revision = ContentRevision(
                id=revision_id,
                dataset_id=dataset_id,
                revision=1,
                source_revision_ids=[source_id],
                training_data_snapshot=TrainingDataSnapshot(
                    schema_version="cyrene.training-record.v1",
                    artifact=record_artifact,
                    record_count=1,
                    counts=counts,
                ),
                created_at=utc_now(),
                resource_version=1,
            )
            report_id = uuid4()
            report = SourceParseReport(
                id=report_id,
                dataset_id=dataset_id,
                source_revision_id=source_id,
                processing_run_id=run.id,
                status=SourceParseReportState.WARNING,
                content_revision_id=revision_id,
                review_item_count=1,
                diagnostic_counts={"training.invalid_json": 1},
                diagnostics=[
                    {
                        "kind": "training",
                        "code": "training.invalid_json",
                        "message": "The source line is malformed JSON.",
                        "severity": "error",
                    }
                ],
                output_artifacts=[record_artifact],
                created_at=utc_now(),
                updated_at=utc_now(),
            )
            review_item = ReviewItem(
                id=uuid4(),
                dataset_id=dataset_id,
                source_parse_report_id=report_id,
                source_revision_id=source_id,
                processing_run_id=run.id,
                content_revision_id=revision_id,
                kind=ReviewItemKind.TRAINING_STRUCTURE,
                code="training.invalid_json",
                message="The source line is malformed JSON.",
                severity="error",
                record_id=record_id,
            )
            service.store.finalize_parsed_content_revision_for_worker(
                revision,
                [report],
                [review_item],
            )

            acknowledgement = client.post(
                f"/api/v1/review-items/{review_item.id}/resolve",
                json={"action": "ACKNOWLEDGE", "note": "Inspected the malformed row."},
            )
            assert acknowledgement.status_code == 200
            assert acknowledgement.json()["state"] == "ACKNOWLEDGED"

            page = client.get(
                f"/api/v1/content-revisions/{revision_id}/training-records",
                params={"disposition": "review"},
            )
            assert page.status_code == 200
            assert page.json()["total"] == 1
            assert page.json()["records"][0]["disposition"] == "review"

            approval = client.post(
                f"/api/v1/content-revisions/{revision_id}/review",
                json={"decision": "APPROVE"},
            )
            assert approval.status_code == 409
            assert approval.json()["code"] == "CATALYST_TRAINING_RECORDS_PENDING_REVIEW"
    finally:
        service.close()
        app.state.catalyst_service.close()
