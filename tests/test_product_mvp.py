"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 test_product_mvp.py                                             │
│  Module: tests.test_product_mvp                                     │
│  Role: Real DuckDB, failure, idempotency, and restart acceptance.    │
│                                                                     │
│  模块职责：验证真实 DuckDB 路径、失败持久化、幂等与重启恢复。                │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import duckdb
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator, FormatChecker
from openapi_spec_validator import validate as validate_openapi_spec
from openapi_spec_validator.readers import read_from_filename
from referencing import Registry, Resource

from cyrene_catalyst import create_app
from cyrene_catalyst.artifacts import LocalArtifactPlane
from cyrene_catalyst.domain import ArtifactRef


def _artifact(path: Path, artifact_root: Path) -> dict[str, Any]:
    """Publish through the shared Platform artifact plane the Product uses.

    中文:通过 Product 共用的 Platform artifact plane 发布。
    """
    # 中文:通过 Product 使用的共享 Platform 制品平面发布。

    reference = LocalArtifactPlane(artifact_root).publish(path, "dataset")
    return reference.model_dump(exclude_none=True)


def _close(app: Any) -> None:
    app.state.catalyst_store.close()


def _data_tools_schema_validator(name: str) -> Draft202012Validator:
    """Load a Product-published data-tools DTO schema and its local ArtifactRef.

    中文：载入 Product data-tools DTO schema 及本地 ArtifactRef 定义。
    """

    product_root = Path(__file__).parents[1] / "contracts/product/v1"
    data_tools = json.loads((product_root / "data-tools.schema.json").read_text())
    artifact_schema = json.loads(
        (product_root / "generated/platform/artifact-ref.schema.json").read_text()
    )
    data_tools_uri = data_tools["$id"]
    artifact_uri = urljoin(data_tools_uri, "./generated/platform/artifact-ref.schema.json")
    registry = (
        Registry()
        .with_resource(data_tools_uri, Resource.from_contents(data_tools))
        .with_resource(artifact_uri, Resource.from_contents(artifact_schema))
    )
    return Draft202012Validator(
        {"$ref": f"{data_tools_uri}#/$defs/{name}"},
        registry=registry,
        format_checker=FormatChecker(),
    )


def test_runtime_paths_match_frozen_openapi(tmp_path: Path) -> None:
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
    )
    contract, _ = read_from_filename(
        str(Path(__file__).parents[1] / "contracts/product/v1/openapi.yaml")
    )
    runtime = app.openapi()
    assert set(runtime["paths"]) == set(contract["paths"])
    v02_routes = {
        "/api/v1/datasets/{datasetId}/sources/batch": "post",
        "/api/v1/datasets/{datasetId}/source-parse-reports": "get",
        "/api/v1/datasets/{datasetId}/review-queue": "get",
        "/api/v1/review-items/{reviewItemId}/resolve": "post",
    }
    for path, method in v02_routes.items():
        assert method in runtime["paths"][path]
        assert method in contract["paths"][path]

    upload_request_schema = runtime["paths"]["/api/v1/datasets/{datasetId}/sources/batch"]["post"][
        "requestBody"
    ]["content"]["multipart/form-data"]["schema"]
    upload_component = runtime["components"]["schemas"][
        upload_request_schema["$ref"].rsplit("/", maxsplit=1)[1]
    ]
    assert upload_component["required"] == ["files[]"]
    assert upload_component["properties"]["files[]"]["maxItems"] == 20

    product_root = Path(__file__).parents[1] / "contracts/product/v1"
    data_tools = json.loads((product_root / "data-tools.schema.json").read_text())
    for name in (
        "BatchUploadError",
        "BatchSourceUploadItem",
        "BatchSourceUploadResponse",
        "SourceParseReport",
        "ReviewItem",
        "GeneratedDraftSummary",
        "ReviewQueueResponse",
        "ResolveReviewItemRequest",
    ):
        assert set(runtime["components"]["schemas"][name]["properties"]) == set(
            data_tools["$defs"][name]["properties"]
        )
    assert set(runtime["components"]["schemas"]["BatchSourceUploadItem"]["required"]) == set(
        data_tools["$defs"]["BatchSourceUploadItem"]["required"]
    )
    _close(app)


def test_v02_batch_report_and_review_contracts_match_openapi() -> None:
    """Freeze v0.2 HTTP routes and validate representative DTOs. | 冻结 v0.2 路由与 DTO。"""

    product_root = Path(__file__).parents[1] / "contracts/product/v1"
    contract, base = read_from_filename(str(product_root / "openapi.yaml"))
    batch_path = "/api/v1/datasets/{datasetId}/sources/batch"
    report_path = "/api/v1/datasets/{datasetId}/source-parse-reports"
    queue_path = "/api/v1/datasets/{datasetId}/review-queue"
    resolve_path = "/api/v1/review-items/{reviewItemId}/resolve"

    assert batch_path in contract["paths"]
    batch = contract["paths"][batch_path]["post"]
    assert batch["operationId"] == "uploadSourceRevisionsBatch"
    multipart = batch["requestBody"]["content"]["multipart/form-data"]
    assert multipart["schema"]["required"] == ["files[]"]
    assert multipart["schema"]["properties"]["files[]"]["maxItems"] == 20
    assert multipart["schema"]["properties"]["files[]"]["items"]["format"] == "binary"
    assert (
        batch["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
        == "./data-tools.schema.json#/$defs/BatchSourceUploadResponse"
    )

    report = contract["paths"][report_path]["get"]
    assert report["operationId"] == "listSourceParseReports"
    assert {parameter["name"] for parameter in report["parameters"]} == {
        "sourceRevisionId",
        "processingRunId",
    }
    report_schema = report["responses"]["200"]["content"]["application/json"]["schema"]
    assert report_schema["type"] == "array"
    assert report_schema["items"]["$ref"] == "./data-tools.schema.json#/$defs/SourceParseReport"

    queue = contract["paths"][queue_path]["get"]
    assert queue["operationId"] == "getDatasetReviewQueue"
    assert (
        queue["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
        == "./data-tools.schema.json#/$defs/ReviewQueueResponse"
    )

    resolve = contract["paths"][resolve_path]["post"]
    assert resolve["operationId"] == "resolveReviewItem"
    assert (
        resolve["requestBody"]["content"]["application/json"]["schema"]["$ref"]
        == "./data-tools.schema.json#/$defs/ResolveReviewItemRequest"
    )
    validate_openapi_spec(contract, base_uri=base)

    data_tools = json.loads((product_root / "data-tools.schema.json").read_text())
    Draft202012Validator.check_schema(data_tools)
    digest = "a" * 64
    artifact = {
        "uri": f"artifact://sha256/{digest}",
        "digest": f"sha256:{digest}",
        "size_bytes": 32,
        "kind": "source-parse-blocks",
    }
    source = {
        "id": "0d9444f9-96c5-42d2-aef3-116cc72f8775",
        "datasetId": "319b41e0-b69e-4d71-b835-319913a43d0b",
        "sourceId": "1ba9929d-132f-4294-ad8d-76df6c4cb0cc",
        "revision": 1,
        "filename": "deck.pptx",
        "mediaType": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "byteLength": 32,
        "digest": f"sha256:{digest}",
        "artifact": {**artifact, "kind": "catalyst.source.original.v1"},
        "createdAt": "2026-10-07T12:00:00Z",
        "resourceVersion": 1,
    }
    batch_response = {"items": [{"filename": "deck.pptx", "source": source, "error": None}]}
    assert _data_tools_schema_validator("BatchSourceUploadResponse").is_valid(batch_response)
    invalid_item = {"items": [{"filename": "empty.pdf", "source": None, "error": None}]}
    assert not _data_tools_schema_validator("BatchSourceUploadResponse").is_valid(invalid_item)

    report_response = {
        "id": "7125ad79-0c15-4491-9994-06c0a657f071",
        "datasetId": source["datasetId"],
        "sourceRevisionId": source["id"],
        "processingRunId": "42c92d0b-2460-416a-bb74-e4b4a68302c4",
        "status": "WARNING",
        "contentRevisionId": "a53f5578-8860-4a1f-b21a-73cac5e8556a",
        "blockCount": 1,
        "warnings": [],
        "diagnostics": [
            {
                "code": "ocr.low_confidence",
                "message": "Recognized text is below the configured confidence threshold.",
                "kind": "ocr",
                "severity": "warning",
                "locator": {
                    "source_pages": [1],
                    "section_path": [],
                    "item_ref": "page/1/region/2",
                    "tree_level": 0,
                    "provenance": [
                        {
                            "engine": "tesseract-cli",
                            "language": "eng",
                            "text_origin": "ocr",
                        }
                    ],
                },
                "confidence": 0.41,
            }
        ],
        "unsupportedContent": [],
        "outputArtifacts": [artifact],
        "createdAt": "2026-10-07T12:00:00Z",
        "updatedAt": "2026-10-07T12:00:03Z",
        "resourceVersion": 2,
    }
    report_validator = _data_tools_schema_validator("SourceParseReport")
    assert report_validator.is_valid(report_response)
    invalid_report = {**report_response, "outputArtifacts": []}
    assert not report_validator.is_valid(invalid_report)

    review_item = {
        "id": "b59f0417-1018-4388-9e7c-f4c75457bb0e",
        "datasetId": source["datasetId"],
        "sourceParseReportId": report_response["id"],
        "sourceRevisionId": source["id"],
        "processingRunId": report_response["processingRunId"],
        "contentRevisionId": report_response["contentRevisionId"],
        "kind": "OCR_WARNING",
        "code": "ocr.low_confidence",
        "message": "Review OCR text.",
        "severity": "warning",
        "locator": {
            "sourcePages": [1],
            "sectionPath": [],
            "itemRef": "page/1/region/2",
            "treeLevel": 0,
            "provenance": [{"engine": "tesseract-cli", "text_origin": "ocr"}],
        },
        "confidence": 0.41,
        "state": "OPEN",
        "createdAt": "2026-10-07T12:00:00Z",
        "resourceVersion": 1,
    }
    queue_response = {
        "items": [review_item],
        "generatedDrafts": [
            {
                "id": "a53f5578-8860-4a1f-b21a-73cac5e8556a",
                "datasetId": source["datasetId"],
                "revision": 2,
                "state": "DRAFT",
                "blockCount": 1,
                "sourceRevisionIds": [source["id"]],
                "createdAt": "2026-10-07T12:00:03Z",
                "processingRunId": report_response["processingRunId"],
            }
        ],
    }
    assert _data_tools_schema_validator("ReviewQueueResponse").is_valid(queue_response)
    assert _data_tools_schema_validator("ResolveReviewItemRequest").is_valid(
        {"action": "ACKNOWLEDGE", "note": "Reviewed OCR text."}
    )
    assert not _data_tools_schema_validator("ResolveReviewItemRequest").is_valid(
        {"action": "acknowledge"}
    )


def test_artifact_kind_is_an_open_producer_owned_string() -> None:
    digest = "sha256:" + "a" * 64
    reference = ArtifactRef(
        uri="artifact://sha256/" + "a" * 64,
        digest=digest,
        size_bytes=1,
        kind="catalyst.dataset.normalized.v1",
    )
    assert reference.kind == "catalyst.dataset.normalized.v1"

    schema = json.loads(
        (
            Path(__file__).parents[1]
            / "contracts/product/v1/generated/platform/artifact-ref.schema.json"
        ).read_text()
    )
    kind_schema = schema["properties"]["kind"]
    assert "enum" not in kind_schema
    assert kind_schema["minLength"] == 1
    assert kind_schema["maxLength"] == 128


def test_product_boundary_guard_allows_non_artifact_kind_enums() -> None:
    product_root = Path(__file__).parents[1]
    data_tools = json.loads(
        (product_root / "contracts/product/v1/data-tools.schema.json").read_text()
    )
    assert data_tools["$defs"]["ParserDiagnostic"]["properties"]["kind"]["enum"] == [
        "parser",
        "ocr",
    ]
    assert data_tools["$defs"]["ReviewItem"]["properties"]["kind"]["enum"] == [
        "PARSER_WARNING",
        "OCR_WARNING",
        "PARSE_FAILURE",
        "UNSUPPORTED_SOURCE",
    ]

    result = subprocess.run(
        ["bash", str(product_root / "tooling/ci/check-product-boundary.sh")],
        cwd=product_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_duckdb_publish_idempotency_and_restart(tmp_path: Path) -> None:
    source = tmp_path / "source.jsonl"
    source.write_text('{"prompt":"one","score":1}\n{"prompt":"two","score":2}\n')
    database = tmp_path / "catalyst.sqlite3"
    artifact_root = tmp_path / "artifacts"
    app = create_app(
        database_path=database,
        artifact_root=artifact_root,
    )

    with TestClient(app) as client:
        dataset_response = client.post(
            "/api/v1/datasets",
            headers={"Idempotency-Key": "dataset-demo"},
            json={"name": "acceptance-data", "description": "real JSONL"},
        )
        assert dataset_response.status_code == 201
        dataset = dataset_response.json()

        version_response = client.post(
            f"/api/v1/datasets/{dataset['id']}/versions",
            headers={"Idempotency-Key": "version-demo"},
            json={"source": _artifact(source, artifact_root), "engineBindingId": "local-duckdb"},
        )
        assert version_response.status_code == 201
        version = version_response.json()
        assert version["state"] == "PUBLISHED"
        assert version["engineCapabilityType"] == "dataset.preparation.v1"
        assert version["rowCount"] == 2
        assert version["schemaFields"] == ["prompt", "score"]
        assert version["lineage"] == [
            {
                "fromDigest": version["source"]["digest"],
                "toDigest": version["output"]["digest"],
                "relation": "DERIVED_FROM",
            }
        ]

        replay = client.post(
            f"/api/v1/datasets/{dataset['id']}/versions",
            headers={"Idempotency-Key": "version-demo"},
            json={"source": _artifact(source, artifact_root), "engineBindingId": "local-duckdb"},
        )
        assert replay.status_code == 201
        assert replay.json()["id"] == version["id"]

    parquet_path = LocalArtifactPlane(artifact_root).resolve(
        ArtifactRef.model_validate(version["output"])
    )
    assert parquet_path.is_file()
    with duckdb.connect() as connection:
        count = connection.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(parquet_path)]
        ).fetchone()
    assert count == (2,)
    _close(app)

    restarted = create_app(
        database_path=database,
        artifact_root=artifact_root,
    )
    with TestClient(restarted) as client:
        persisted = client.get(f"/api/v1/dataset-versions/{version['id']}")
        assert persisted.status_code == 200
        assert persisted.json() == version

    schema = json.loads(
        (Path(__file__).parents[1] / "contracts/product/v1/dataset-version.schema.json").read_text()
    )
    artifact_schema = json.loads(
        (
            Path(__file__).parents[1]
            / "contracts/product/v1/generated/platform/artifact-ref.schema.json"
        ).read_text()
    )
    registry = Registry().with_resource(
        artifact_schema["$id"], Resource.from_contents(artifact_schema)
    )
    Draft202012Validator(
        schema,
        registry=registry,
        format_checker=FormatChecker(),
    ).validate(version)
    _close(restarted)


def test_invalid_json_persists_failed_version_and_problem(tmp_path: Path) -> None:
    source = tmp_path / "broken.jsonl"
    source.write_text('{"prompt": [}\n')
    database = tmp_path / "catalyst.sqlite3"
    artifact_root = tmp_path / "artifacts"
    app = create_app(
        database_path=database,
        artifact_root=artifact_root,
    )

    with TestClient(app) as client:
        dataset = client.post("/api/v1/datasets", json={"name": "broken"}).json()
        command = {
            "source": _artifact(source, artifact_root),
            "engineBindingId": "local-duckdb",
        }
        response = client.post(
            f"/api/v1/datasets/{dataset['id']}/versions",
            headers={"Idempotency-Key": "broken-version"},
            json=command,
        )
        assert response.status_code == 422
        assert response.headers["content-type"].startswith("application/problem+json")
        problem = response.json()
        assert problem["code"] == "CATALYST_DATA_PROCESSING_FAILED"
        assert problem["retryable"] is False
        assert problem["traceId"] in response.headers["traceparent"]

        persisted = client.get(problem["resourceRef"])
        assert persisted.status_code == 200
        assert persisted.json()["state"] == "FAILED"
        assert persisted.json()["failure"]["code"] == "CATALYST_DATA_PROCESSING_FAILED"

        replay = client.post(
            f"/api/v1/datasets/{dataset['id']}/versions",
            headers={"Idempotency-Key": "broken-version"},
            json=command,
        )
        assert replay.status_code == 201
        assert replay.json() == persisted.json()

    _close(app)


def test_idempotency_key_reuse_with_different_body_is_rejected(tmp_path: Path) -> None:
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
    )
    with TestClient(app) as client:
        first = client.post(
            "/api/v1/datasets",
            headers={"Idempotency-Key": "same-key"},
            json={"name": "first"},
        )
        assert first.status_code == 201
        conflict = client.post(
            "/api/v1/datasets",
            headers={"Idempotency-Key": "same-key"},
            json={"name": "different"},
        )
        assert conflict.status_code == 409
        assert conflict.json()["code"] == "CATALYST_IDEMPOTENCY_CONFLICT"
    _close(app)


def test_unresolved_artifact_identity_is_denied_without_path_leak(tmp_path: Path) -> None:
    source = tmp_path / "outside.jsonl"
    source.write_text('{"value":1}\n')
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "isolated-artifacts",
    )
    with TestClient(app) as client:
        dataset = client.post("/api/v1/datasets", json={"name": "scope"}).json()
        response = client.post(
            f"/api/v1/datasets/{dataset['id']}/versions",
            json={
                "source": {
                    "uri": f"artifact://sha256/{'f' * 64}",
                    "digest": f"sha256:{'f' * 64}",
                    "size_bytes": source.stat().st_size,
                    "kind": "dataset",
                },
                "engineBindingId": "local-duckdb",
            },
        )
        assert response.status_code == 422
        assert response.json()["code"] == "CATALYST_ARTIFACT_UNAVAILABLE"
        assert str(source) not in response.text
    _close(app)
