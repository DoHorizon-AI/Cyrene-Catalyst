"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 accept_training_curation.py                                     │
│  Role: Exercise mixed training curation through real Product APIs.   │
│  中文：通过真实插件和产品 API 验收导入、审核、恢复与不可变发布。             │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from uuid import UUID

from fastapi.testclient import TestClient
from verify_training_bundle import learned_messages, verify_bundle

from cyrene_catalyst import create_app

_ROOT = Path(__file__).resolve().parents[1]
_FIXTURE = _ROOT / "tests/fixtures/training-curation-v03"


@contextmanager
def owner_endpoints() -> Any:
    """Start the actual owner Plugins over their existing Direct contract. | 启动真实插件。"""

    from cyrene_plugin_runtime import serve
    from dataset_generation import DatasetGenerationPlugin
    from dataset_preparation import DatasetPreparationPlugin

    previous: dict[str, str | None] = {}
    servers: list[Any] = []
    try:
        for implementation, capability, environment in (
            (
                DatasetPreparationPlugin(),
                "dataset.preparation.v1",
                "CYRENE_DATASET_PREPARATION_CONNECTION_REF",
            ),
            (
                DatasetGenerationPlugin(),
                "dataset.generation.v1",
                "CYRENE_DATASET_GENERATION_CONNECTION_REF",
            ),
        ):
            server, reference = serve(implementation, capability, "1", "127.0.0.1:0")
            servers.append(server)
            previous[environment] = os.environ.get(environment)
            os.environ[environment] = reference
        yield
    finally:
        for environment, value in previous.items():
            if value is None:
                os.environ.pop(environment, None)
            else:
                os.environ[environment] = value
        for server in servers:
            server.stop(grace=None).wait()


def _json(client: TestClient, method: str, path: str, expected: int = 200, **kwargs: Any) -> Any:
    """Require the expected HTTP outcome and return its payload. | 校验 HTTP 结果。"""

    response = client.request(method, path, **kwargs)
    if response.status_code != expected:
        raise AssertionError(f"{method} {path}: {response.status_code}: {response.text}")
    return response.json()


def _wait(app: Any, response: dict[str, Any], expected: str = "SUCCEEDED") -> dict[str, Any]:
    """Wait for the durable coordinator's terminal result. | 等待持久化任务。"""

    result = app.state.data_tools_service.coordinator.wait(UUID(response["id"]), timeout=60)
    if result.state.value != expected:
        raise AssertionError(
            f"Run did not reach {expected}: {result.model_dump_json(by_alias=True)}"
        )
    return result.model_dump(mode="json", by_alias=True, exclude_none=True)


def _close(app: Any) -> None:
    """Close all Product persistence between real restart phases. | 关闭后重新打开持久化。"""

    app.state.data_tools_service.close()
    app.state.catalyst_service.close()


def _open(output_directory: Path) -> Any:
    """Reopen the same database and shared Artifact Plane. | 打开同一数据库和制品平面。"""

    return create_app(
        database_path=output_directory / "catalyst.sqlite3",
        artifact_root=output_directory / "artifacts",
    )


def _latest(client: TestClient, dataset_id: str) -> dict[str, Any]:
    """Fetch the current immutable ContentRevision. | 获取最新不可变内容修订。"""

    return _json(client, "GET", f"/api/v1/datasets/{dataset_id}/content-revisions")[0]


def _records(client: TestClient, revision: dict[str, Any]) -> list[dict[str, Any]]:
    """Fetch the small fixed fixture using the same paged UI endpoint. | 使用分页端点读取夹具。"""

    page = _json(
        client,
        "GET",
        f"/api/v1/content-revisions/{revision['id']}/training-records",
        params={"limit": 200},
    )
    if page["total"] != 28 or len(page["records"]) != 28:
        raise AssertionError(
            "The fixture records, including broken and blank lines, must reconcile."
        )
    return page["records"]


def run_acceptance(output_directory: Path) -> dict[str, Any]:
    """Run mixed import, human review, restart and publication. | 运行完整验收。"""

    output_directory.mkdir(parents=True, exist_ok=True)
    fixture = json.loads((_FIXTURE / "fixture-manifest.json").read_text(encoding="utf-8"))
    uploads = []
    for entry in fixture["files"]:
        path = _FIXTURE / entry["filename"]
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != entry["sha256"]:
            raise AssertionError("Fixture original digest changed.")
        media_type = "application/json" if path.suffix == ".json" else "application/x-ndjson"
        uploads.append(("files[]", (path.name, data, media_type)))

    with owner_endpoints():
        app = _open(output_directory)
        try:
            with TestClient(app) as client:
                dataset = _json(
                    client,
                    "POST",
                    "/api/v1/datasets",
                    201,
                    json={"name": "Catalyst v0.3 business trial"},
                )
                dataset_id = dataset["id"]
                path = f"/api/v1/datasets/{dataset_id}/sources/batch"
                sources = _json(client, "POST", path, files=uploads)["items"]
                if any(item["error"] for item in sources):
                    raise AssertionError("Every fixed fixture source must be ingested.")
                source_ids = [item["source"]["id"] for item in sources]
                repeated_sources = _json(client, "POST", path, files=uploads)["items"]
                if any(item["error"] for item in repeated_sources):
                    raise AssertionError("Repeated imports must retain every source.")
                repeated_import_preserves_bytes = all(
                    original["source"]["digest"] == repeated["source"]["digest"]
                    and original["source"]["artifact"] == repeated["source"]["artifact"]
                    and original["source"]["id"] != repeated["source"]["id"]
                    for original, repeated in zip(sources, repeated_sources, strict=True)
                )
                if not repeated_import_preserves_bytes:
                    raise AssertionError("Repeated source imports must preserve original bytes.")
                run_config = {
                    "curation": fixture["recipe"],
                    "sourcePolicies": {
                        source_id: {"allowTraining": True, "allowedUsePurposes": ["model_training"]}
                        for source_id in source_ids
                    },
                }
                run = _wait(
                    app,
                    _json(
                        client,
                        "POST",
                        f"/api/v1/datasets/{dataset_id}/processing-runs",
                        202,
                        json={
                            "operation": "curateTrainingData",
                            "sourceRevisionIds": source_ids,
                            "config": run_config,
                        },
                    ),
                )
                original = _latest(client, dataset_id)
                original_records = _records(client, original)
                before_counts = original["trainingDataSnapshot"]["counts"]
                if before_counts["total"] != fixture["totalRecords"]:
                    raise AssertionError("Import record counters must include all original rows.")
                queue = _json(client, "GET", f"/api/v1/datasets/{dataset_id}/review-queue")
                if not queue["items"]:
                    raise AssertionError(
                        "The mixed fixture must produce actionable Review Queue issues."
                    )
                blocked = client.post(
                    f"/api/v1/content-revisions/{original['id']}/review",
                    json={"decision": "APPROVE"},
                )
                if blocked.status_code != 409:
                    raise AssertionError("Unreviewed records must block revision approval.")
                original_digest = original["trainingDataSnapshot"]["artifact"]["digest"]
        finally:
            _close(app)

        app = _open(output_directory)
        try:
            with TestClient(app) as client:
                resumed = _latest(client, dataset_id)
                if resumed["trainingDataSnapshot"]["artifact"]["digest"] != original_digest:
                    raise AssertionError("Restart must retain the original immutable snapshot.")
                recovered_run = _json(client, "GET", f"/api/v1/processing-runs/{run['id']}")
                if recovered_run["state"] != "SUCCEEDED":
                    raise AssertionError("Completed task state must survive restart.")
                edits = []
                excluded = set(fixture["expectedExcludedSamples"])
                for record in original_records:
                    sample_id = record.get("sampleId")
                    if sample_id in {"needs-mapping", "mapped-source"}:
                        mapping = {"instruction": "task", "output": "answer"}
                        if sample_id == "mapped-source":
                            mapping["input"] = "context"
                        edit = {
                            "recordId": record["id"],
                            "action": "remap",
                            "format": "alpaca",
                            "fieldMapping": mapping,
                            "note": "Human verified the legacy task/answer field mapping.",
                        }
                    elif sample_id in excluded or not isinstance(record.get("rawRecord"), dict):
                        edit = {
                            "recordId": record["id"],
                            "action": "exclude",
                            "note": "Human excluded an invalid, prohibited or lossy SFT record.",
                        }
                    else:
                        edit = {
                            "recordId": record["id"],
                            "action": "approve",
                            "note": "Human checked the preserved original and normalized answer.",
                        }
                    edits.append(edit)
                child = _json(
                    client,
                    "POST",
                    f"/api/v1/content-revisions/{resumed['id']}/training-records:edit",
                    expected=201,
                    json={"resourceVersion": resumed["resourceVersion"], "edits": edits},
                )
                child_records = _records(client, child)
                remap_approvals = [
                    {
                        "recordId": record["id"],
                        "action": "approve",
                        "note": "Approve verified mapping.",
                    }
                    for record in child_records
                    if record["disposition"] == "review"
                    and record.get("sampleId") in {"needs-mapping", "mapped-source"}
                ]
                if remap_approvals:
                    child = _json(
                        client,
                        "POST",
                        f"/api/v1/content-revisions/{child['id']}/training-records:edit",
                        expected=201,
                        json={
                            "resourceVersion": child["resourceVersion"],
                            "edits": remap_approvals,
                        },
                    )
                after_counts = child["trainingDataSnapshot"]["counts"]
                if after_counts["eligible"] != fixture["expectedPublishedRecords"]:
                    raise AssertionError(
                        f"Reviewed publication count does not reconcile: {after_counts}"
                    )
                if (
                    after_counts["pendingReview"]
                    or after_counts["eligible"] + after_counts["excluded"] != 28
                ):
                    raise AssertionError(
                        "Every imported record must have an explicit final disposition."
                    )
                # Queue acknowledgement retains issue decisions; excluded rows remain excluded.
                queue = _json(
                    client,
                    "GET",
                    f"/api/v1/datasets/{dataset_id}/review-queue",
                    params={"limit": 200},
                )
                for item in queue["items"]:
                    if item["state"] == "OPEN":
                        _json(
                            client,
                            "POST",
                            f"/api/v1/review-items/{item['id']}/resolve",
                            json={
                                "action": "ACKNOWLEDGE",
                                "note": "Verified the explicit sample decision.",
                            },
                        )
                approved = _json(
                    client,
                    "POST",
                    f"/api/v1/content-revisions/{child['id']}/review",
                    json={
                        "decision": "APPROVE",
                        "note": "Fixed business corpus reviewed for training.",
                    },
                )
                final_records = _records(client, approved)
                approved_digest = approved["trainingDataSnapshot"]["artifact"]["digest"]
                if _records(client, original) != original_records:
                    raise AssertionError("Human correction must not mutate the original snapshot.")
        finally:
            _close(app)

        app = _open(output_directory)
        try:
            with TestClient(app) as client:
                if _latest(client, dataset_id)["state"] != "APPROVED":
                    raise AssertionError("Human approval must survive restart before publishing.")
                consumer_reports = {}
                versions = {}
                for output_format in ("sft", "messages"):
                    exported_run = _wait(
                        app,
                        _json(
                            client,
                            "POST",
                            f"/api/v1/datasets/{dataset_id}/processing-runs",
                            202,
                            json={
                                "operation": "prepareSft",
                                "contentRevisionId": approved["id"],
                                "config": {
                                    "outputFormat": output_format,
                                    "split": {
                                        "train": 0.8,
                                        "validation": 0.1,
                                        "test": 0.1,
                                    },
                                },
                            },
                        ),
                    )
                    publish_body = {
                        "contentRevisionId": approved["id"],
                        "sftRunId": exported_run["id"],
                    }
                    version_record = _json(
                        client,
                        "POST",
                        f"/api/v1/datasets/{dataset_id}/data-tools/versions",
                        201,
                        json=publish_body,
                        headers={"Idempotency-Key": f"publish-{output_format}"},
                    )
                    repeated = _json(
                        client,
                        "POST",
                        f"/api/v1/datasets/{dataset_id}/data-tools/versions",
                        201,
                        json=publish_body,
                        headers={"Idempotency-Key": f"publish-{output_format}"},
                    )
                    if repeated["id"] != version_record["id"]:
                        raise AssertionError(
                            "Retrying publication must return the same immutable version."
                        )
                    response = client.get(
                        f"/api/v1/dataset-versions/{version_record['id']}/data-tools/export",
                        params={"profile": "sft"},
                    )
                    if response.status_code != 200:
                        raise AssertionError(
                            "The published package must be downloadable through Product API."
                        )
                    bundle_path = output_directory / f"business-{output_format}.zip"
                    bundle_path.write_bytes(response.content)
                    result = verify_bundle(bundle_path, fixture["metadataSentinel"])
                    _assert_roundtrip(bundle_path, final_records, output_format)
                    if result["published"] != 13 or result["maxMessageCount"] < 5:
                        raise AssertionError(
                            "All approved samples and multi-turn/system content must survive."
                        )
                    consumer_reports[output_format] = result
                    versions[output_format] = version_record
                if (
                    _latest(client, dataset_id)["trainingDataSnapshot"]["artifact"]["digest"]
                    != approved_digest
                ):
                    raise AssertionError("Publication must not mutate approved content bytes.")
        finally:
            _close(app)

    report = {
        "status": "PASS",
        "fixture": "fixed mixed authored business corpus; no customer data",
        "totalRecords": 28,
        "beforeReview": before_counts,
        "afterReview": after_counts,
        "restartBeforeReview": True,
        "restartBeforePublish": True,
        "taskStateRecovered": True,
        "repeatedImportPreservesBytes": repeated_import_preserves_bytes,
        "repeatedImportCreatesIndependentSources": True,
        "publicationRetryImmutable": True,
        "originalSnapshotImmutable": True,
        "curationRecipeDigest": run["recipeDigest"],
        "datasetId": dataset_id,
        "versions": versions,
        "independentConsumer": consumer_reports,
        "approvedMessages": {
            record["sampleId"]: record["normalized"]["messages"]
            for record in final_records
            if record["disposition"] == "eligible"
        },
    }
    (output_directory / "acceptance-report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return report


def _assert_roundtrip(bundle_path: Path, records: list[dict[str, Any]], output_format: str) -> None:
    """Compare every exported turn against the approved snapshot. | 逐轮对照已批准的快照。"""

    expected = {
        record["sampleId"]: record["normalized"]["messages"]
        for record in records
        if record["disposition"] == "eligible"
    }
    with zipfile.ZipFile(bundle_path) as bundle:
        split_streams = {
            split: bundle.open(f"{split}.jsonl") for split in ("train", "validation", "test")
        }
        try:
            with bundle.open("provenance.jsonl") as stream:
                for line in stream:
                    receipt = json.loads(line)
                    learned = json.loads(next(split_streams[receipt["split"]]))
                    if learned_messages(learned, output_format) != expected[receipt["sample_id"]]:
                        raise AssertionError(
                            "The export lost or changed an approved conversation turn."
                        )
        finally:
            for split_stream in split_streams.values():
                split_stream.close()


def main() -> None:
    """Execute the reproducible Product acceptance workflow. | 可重复产品验收入口。"""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", required=True, type=Path)
    arguments = parser.parse_args()
    report = run_acceptance(arguments.output_directory.resolve())
    print(
        json.dumps(
            {"status": report["status"], "counts": report["afterReview"]}, ensure_ascii=False
        )
    )


if __name__ == "__main__":
    main()
