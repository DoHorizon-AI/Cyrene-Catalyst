"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 test_capability_status.py                                       │
│  Module: tests.test_capability_status                               │
│  Role: Public capability configuration and fail-closed acceptance.  │
│                                                                     │
│  模块职责：验证能力配置状态、无可选插件启动与结构化缺失错误。              │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from cyrene_catalyst import create_app


def test_health_and_configuration_status_without_plugin_refs(tmp_path: Path, monkeypatch) -> None:
    """Start without Plugin refs and report configuration without activation claims."""

    environment_variables = (
        "CYRENE_DATASET_PREPARATION_CONNECTION_REF",
        "CYRENE_DOCUMENT_PARSING_CONNECTION_REF",
        "CYRENE_KNOWLEDGE_PREPARATION_CONNECTION_REF",
        "CYRENE_DATASET_GENERATION_CONNECTION_REF",
    )
    for environment_variable in environment_variables:
        monkeypatch.delenv(environment_variable, raising=False)

    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
    )
    with TestClient(app) as client:
        assert client.get("/healthz").json() == {"status": "ok"}

        response = client.get("/api/v1/system/capabilities")
        assert response.status_code == 200
        report = response.json()
        assert report["semantics"] == "configuration-only"
        assert report["activationVerified"] is False
        assert {item["id"] for item in report["capabilities"]} == {
            "dataset.preparation.v1",
            "document.parsing.v1",
            "dataset.knowledge.v1",
            "dataset.generation.v1",
        }
        assert all(item["supported"] is True for item in report["capabilities"])
        assert all(item["configured"] is False for item in report["capabilities"])


def test_missing_document_parser_returns_structured_problem(tmp_path: Path, monkeypatch) -> None:
    """Reject a document parse before admission when its optional Plugin ref is absent."""

    monkeypatch.delenv("CYRENE_DOCUMENT_PARSING_CONNECTION_REF", raising=False)
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
    )
    with TestClient(app) as client:
        dataset = client.post("/api/v1/datasets", json={"name": "missing-parser"})
        assert dataset.status_code == 201
        dataset_id = dataset.json()["id"]

        upload = client.post(
            f"/api/v1/datasets/{dataset_id}/sources/batch",
            files=[("files[]", ("notes.pdf", b"%PDF-1.4\n", "application/pdf"))],
        )
        assert upload.status_code == 200, upload.text
        source = upload.json()["items"][0]["source"]

        response = client.post(
            f"/api/v1/datasets/{dataset_id}/processing-runs",
            json={"operation": "parse", "sourceRevisionIds": [source["id"]]},
        )
        assert response.status_code == 503
        assert response.headers["content-type"].startswith("application/problem+json")
        problem = response.json()
        assert problem["status"] == 503
        assert problem["code"] == "CATALYST_PLUGIN_NOT_CONFIGURED"
        assert problem["retryable"] is True
        assert "CYRENE_DOCUMENT_PARSING_CONNECTION_REF" in problem["detail"]
