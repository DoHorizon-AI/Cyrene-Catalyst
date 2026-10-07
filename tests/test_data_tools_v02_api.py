"""HTTP and import coverage for Catalyst Data Tools v0.2."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from cyrene_catalyst import create_app
from cyrene_catalyst.api import BatchUploadBodyLimitMiddleware
from cyrene_catalyst.data_tools_domain import ContentBlock, SourceRevision
from cyrene_catalyst.data_tools_service import DataToolsService
from cyrene_catalyst.domain import ArtifactRef
from cyrene_catalyst.errors import CatalystError
from cyrene_catalyst.processing_runs import StageExecutionFailure


def _source_revision() -> SourceRevision:
    digest = "sha256:" + "b" * 64
    artifact = ArtifactRef(
        uri="artifact://sha256/" + "b" * 64,
        digest=digest,
        size_bytes=64,
        kind="catalyst.source.original.v1",
    )
    return SourceRevision(
        id=uuid4(),
        dataset_id=uuid4(),
        source_id=uuid4(),
        revision=1,
        filename="source-conversations.jsonl",
        media_type="application/x-ndjson",
        byte_length=artifact.size_bytes,
        digest=artifact.digest,
        artifact=artifact,
    )


def test_batch_upload_returns_required_nullable_outcomes(tmp_path: Path) -> None:
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
    )
    try:
        with TestClient(app) as client:
            dataset_response = client.post("/api/v1/datasets", json={"name": "batch-test"})
            assert dataset_response.status_code == 201
            dataset_id = dataset_response.json()["id"]
            response = client.post(
                f"/api/v1/datasets/{dataset_id}/sources/batch",
                files=[
                    (
                        "files[]",
                        (
                            "samples.jsonl",
                            b'{"instruction":"Q","output":"A"}\n',
                            "application/x-ndjson",
                        ),
                    ),
                    ("files[]", ("empty.csv", b"", "text/csv")),
                ],
            )

            assert response.status_code == 200
            items = response.json()["items"]
            assert items[0]["filename"] == "samples.jsonl"
            assert items[0]["source"]["mediaType"] == "application/x-ndjson"
            assert items[0]["error"] is None
            assert items[1]["filename"] == "empty.csv"
            assert items[1]["source"] is None
            assert items[1]["error"]["code"] == "CATALYST_SOURCE_EMPTY"

    finally:
        app.state.data_tools_service.close()
        app.state.catalyst_service.close()


def test_batch_body_limit_covers_streamed_requests_without_content_length() -> None:
    received = [
        {"type": "http.request", "body": b"abc", "more_body": True},
        {"type": "http.request", "body": b"def", "more_body": False},
    ]

    async def receive() -> dict[str, object]:
        return received.pop(0)

    async def send(_: dict[str, object]) -> None:
        return None

    async def consume_body(_: dict[str, object], bounded_receive: object, __: object) -> None:
        while True:
            message = await bounded_receive()  # type: ignore[operator]
            if not message.get("more_body"):
                return

    async def run() -> None:
        middleware = BatchUploadBodyLimitMiddleware(consume_body, max_bytes=4)
        scope = {
            "type": "http",
            "method": "POST",
            "path": "/api/v1/datasets/00000000-0000-0000-0000-000000000000/sources/batch",
            "headers": [],
        }
        with pytest.raises(CatalystError) as raised:
            await middleware(scope, receive, send)  # type: ignore[arg-type]
        assert raised.value.status == 413
        assert raised.value.code == "CATALYST_SOURCE_BATCH_TOO_LARGE"

    asyncio.run(run())


def test_structured_messages_keep_management_data_out_of_training_text() -> None:
    fixture = (
        Path(__file__).parents[2]
        / "workspace/tests/fixtures/catalyst-v02/source-conversations.jsonl"
    )
    row = json.loads(fixture.read_text(encoding="utf-8").splitlines()[0])

    block = DataToolsService._rows_to_blocks(_source_revision(), [row])[0]
    learned = json.loads(block.text)

    assert learned == {
        "conversations": [
            {
                "from": "human" if message["role"] == "user" else "gpt",
                "value": message["content"],
            }
            for message in row["messages"]
        ]
    }
    assert block.source_family_id == "family-01"
    assert block.conversation_id == "ORCHID-01-CONV-01"
    assert block.group_id == "ORCHID-01-CONV-01"
    assert block.sample_id == "ORCHID-01-CONV-01-SAMPLE"
    assert block.policy.allowed_principal_refs == ["org:synthetic-itops"]
    assert "ORCHID42_OPERATOR_ONLY_SENTINEL_DO_NOT_TRAIN_20261007" not in block.text
    assert "_ingestBatch" not in block.text


def test_structured_jsonl_rejects_ambiguous_family_and_unknown_message_roles() -> None:
    fixture = (
        Path(__file__).parents[2]
        / "workspace/tests/fixtures/catalyst-v02/source-conversations.jsonl"
    )
    row = json.loads(fixture.read_text(encoding="utf-8").splitlines()[0])
    row["sourceFamilyId"] = "conflicting-family"
    with pytest.raises(StageExecutionFailure, match="conflicting source-family aliases"):
        DataToolsService._rows_to_blocks(_source_revision(), [row])

    row.pop("sourceFamilyId")
    row["messages"][0]["role"] = "system"
    with pytest.raises(StageExecutionFailure, match="messages are invalid"):
        DataToolsService._rows_to_blocks(_source_revision(), [row])


def test_mixed_parse_batch_persists_each_source_outcome(tmp_path: Path) -> None:
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
    )
    try:
        with TestClient(app) as client:
            dataset = client.post("/api/v1/datasets", json={"name": "mixed-parse"}).json()
            dataset_id = dataset["id"]
            upload = client.post(
                f"/api/v1/datasets/{dataset_id}/sources/batch",
                files=[
                    (
                        "files[]",
                        (
                            "samples.jsonl",
                            b'{"instruction":"Question","output":"Answer"}\n',
                            "application/x-ndjson",
                        ),
                    ),
                    ("files[]", ("unknown.bin", b"\x00\xffbinary", "application/octet-stream")),
                ],
            )
            assert upload.status_code == 200
            source_ids = [item["source"]["id"] for item in upload.json()["items"]]

            admitted = client.post(
                f"/api/v1/datasets/{dataset_id}/processing-runs",
                json={"operation": "parse", "sourceRevisionIds": source_ids},
            )
            assert admitted.status_code == 202
            run_id = UUID(admitted.json()["id"])
            run = app.state.data_tools_service.coordinator.wait(run_id, timeout=10)
            assert run.state.value == "SUCCEEDED"

            report_response = client.get(
                f"/api/v1/datasets/{dataset_id}/source-parse-reports",
                params={"processingRunId": str(run_id)},
            )
            assert report_response.status_code == 200
            reports = {report["sourceRevisionId"]: report for report in report_response.json()}
            assert reports[source_ids[0]]["status"] == "SUCCEEDED"
            assert any(
                artifact["kind"] == "source-parse-blocks"
                for artifact in reports[source_ids[0]]["outputArtifacts"]
            )
            assert reports[source_ids[1]]["status"] == "FAILED"
            assert reports[source_ids[1]]["failure"]["code"] == "CATALYST_SOURCE_UNSUPPORTED"

            revisions = client.get(f"/api/v1/datasets/{dataset_id}/content-revisions").json()
            assert len(revisions) == 1
            assert revisions[0]["sourceRevisionIds"] == [source_ids[0]]
            queue = client.get(f"/api/v1/datasets/{dataset_id}/review-queue").json()
            assert len(queue["items"]) == 1
            assert queue["items"][0]["sourceRevisionId"] == source_ids[1]
            assert queue["items"][0].get("contentRevisionId") is None
    finally:
        app.state.data_tools_service.close()
        app.state.catalyst_service.close()


def test_warning_report_and_review_item_block_approval_until_ack(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
    )
    try:
        with TestClient(app) as client:
            dataset = client.post("/api/v1/datasets", json={"name": "warning-gate"}).json()
            dataset_id = dataset["id"]
            upload = client.post(
                f"/api/v1/datasets/{dataset_id}/sources/batch",
                files=[
                    (
                        "files[]",
                        (
                            "samples.jsonl",
                            b'{"instruction":"Question","output":"Answer"}\n',
                            "application/x-ndjson",
                        ),
                    )
                ],
            )
            source = upload.json()["items"][0]["source"]
            source_id = UUID(source["id"])
            service = app.state.data_tools_service

            def parse_with_warning(
                *_: object,
            ) -> tuple[
                list[ContentBlock],
                list[ArtifactRef],
                list[dict[str, str]],
                list[dict[str, object]],
                list[dict[str, object]],
                dict[str, object],
            ]:
                block = ContentBlock(
                    id="warning-block",
                    source_revision_id=source_id,
                    ordinal=0,
                    kind="code",
                    text='{"instruction":"Question","output":"Answer"}',
                )
                return (
                    [block],
                    [],
                    [],
                    [
                        {
                            "code": "ocr.low_confidence",
                            "message": "Verify the uncertain text.",
                            "kind": "ocr",
                            "severity": "warning",
                            "confidence": 0.5,
                        }
                    ],
                    [],
                    {
                        "sourceRevisionId": str(source_id),
                        "status": "partial_success",
                        "blockCount": 1,
                    },
                )

            monkeypatch.setattr(service, "_parse_one_source", parse_with_warning)
            admitted = client.post(
                f"/api/v1/datasets/{dataset_id}/processing-runs",
                json={"operation": "parse", "sourceRevisionIds": [str(source_id)]},
            )
            assert admitted.status_code == 202
            run_id = UUID(admitted.json()["id"])
            run = service.coordinator.wait(run_id, timeout=10)
            assert run.state.value == "SUCCEEDED"
            revision = client.get(f"/api/v1/datasets/{dataset_id}/content-revisions").json()[0]
            revision_id = revision["id"]

            report = client.get(
                f"/api/v1/datasets/{dataset_id}/source-parse-reports",
                params={"processingRunId": str(run_id)},
            ).json()[0]
            assert report["status"] == "WARNING"
            assert report["contentRevisionId"] == revision_id
            queue = client.get(f"/api/v1/datasets/{dataset_id}/review-queue").json()
            assert len(queue["items"]) == 1
            item_id = queue["items"][0]["id"]

            approval = client.post(
                f"/api/v1/content-revisions/{revision_id}/review",
                json={"decision": "APPROVE"},
            )
            assert approval.status_code == 409
            assert approval.json()["code"] == "CATALYST_CONTENT_REVIEW_BLOCKED"

            disposition = client.post(
                f"/api/v1/review-items/{item_id}/resolve",
                json={"action": "ACKNOWLEDGE", "note": "Verified against the source."},
            )
            assert disposition.status_code == 200
            approval = client.post(
                f"/api/v1/content-revisions/{revision_id}/review",
                json={"decision": "APPROVE"},
            )
            assert approval.status_code == 200
            assert approval.json()["state"] == "APPROVED"
    finally:
        app.state.data_tools_service.close()
        app.state.catalyst_service.close()
