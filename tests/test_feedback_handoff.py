"""Explicit feedback import conflict and restart contract. | 显式反馈导入冲突与重启契约。"""

from pathlib import Path

from fastapi.testclient import TestClient

from cyrene_catalyst.api import create_app
from cyrene_catalyst.artifacts import LocalArtifactPlane


def test_feedback_import_replay_preserves_one_unpublished_preparation(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    artifacts = LocalArtifactPlane(root)
    artifact = artifacts.ingest_bytes(
        b'{"instruction":"q","input":"","output":"corrected"}\n',
        name="feedback.jsonl",
    )
    database = tmp_path / "catalyst.db"
    app = create_app(database_path=database, artifact_root=root)
    source_id = "11111111-1111-4111-8111-111111111111"
    payload = {
        "sourceRef": {
            "uri": f"cyrene://echo/feedback-sets/{source_id}",
            "id": source_id,
            "resourceVersion": 2,
        },
        "artifact": artifact.model_dump(exclude_none=True),
        "provenanceRefs": ["cyrene://echo/evaluation-runs/selected"],
    }
    headers = {"Idempotency-Key": "selected-feedback"}
    with TestClient(app) as client:
        response = client.post("/api/v1/feedback-imports", json=payload, headers=headers)
        assert response.status_code == 201, response.text
        receipt = response.json()
        assert receipt["status"] == "DRAFT"
        prep = client.get(receipt["openIn"]).json()
        assert prep["state"] == "MAPPED" and prep.get("publishedVersionId") is None
        assert payload["sourceRef"]["uri"] in prep["sourceRefs"]
        assert (
            client.post("/api/v1/feedback-imports", json=payload, headers=headers).json() == receipt
        )
        payload["provenanceRefs"].append("cyrene://echo/evaluation-runs/different")
        assert (
            client.post("/api/v1/feedback-imports", json=payload, headers=headers).status_code
            == 409
        )
        payload["provenanceRefs"].pop()
    app.state.catalyst_store.close()
    restarted = create_app(database_path=database, artifact_root=root)
    with TestClient(restarted) as client:
        assert (
            client.post("/api/v1/feedback-imports", json=payload, headers=headers).json() == receipt
        )
    restarted.state.catalyst_store.close()
