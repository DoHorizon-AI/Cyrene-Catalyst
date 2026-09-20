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
from pathlib import Path
from typing import Any

import duckdb
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator, FormatChecker
from openapi_spec_validator.readers import read_from_filename
from referencing import Registry, Resource

from cyrene_catalyst import create_app
from cyrene_catalyst.artifacts import LocalArtifactPlane
from cyrene_catalyst.domain import ArtifactRef


def _artifact(path: Path, artifact_root: Path) -> dict[str, Any]:
    """Publish through the shared Platform artifact plane the Product uses."""

    reference = LocalArtifactPlane(artifact_root).publish(path, "dataset")
    return reference.model_dump(exclude_none=True)


def _close(app: Any) -> None:
    app.state.catalyst_store.close()


def test_runtime_paths_match_frozen_openapi(tmp_path: Path) -> None:
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
    )
    contract, _ = read_from_filename(
        str(Path(__file__).parents[1] / "contracts/product/v1/openapi.yaml")
    )
    assert set(app.openapi()["paths"]) == set(contract["paths"])
    _close(app)


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
