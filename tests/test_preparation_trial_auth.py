"""Trial bearer contract tests for scoped Dataset preparations."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from fastapi.testclient import TestClient

from cyrene_catalyst import create_app
from cyrene_catalyst.workspace_auth import WorkspaceServiceAuthenticator

TOKEN_A = "catalyst-trial-workspace-a-token-0123456789"
TOKEN_B = "catalyst-trial-workspace-b-token-9876543210"


def _authenticator() -> WorkspaceServiceAuthenticator:
    entries = [
        (TOKEN_A, "data-tools-trial", "data-tools"),
        (TOKEN_B, "other-org", "other-workspace"),
    ]
    config = [
        {
            "tokenSha256": hashlib.sha256(token.encode("ascii")).hexdigest(),
            "organizationId": organization_id,
            "workspaceId": workspace_id,
        }
        for token, organization_id, workspace_id in entries
    ]
    return WorkspaceServiceAuthenticator.from_json(json.dumps(config))


def _create_dataset(client: TestClient, token: str, name: str) -> str:
    response = client.post(
        "/api/v1/datasets",
        headers={"Authorization": f"Bearer {token}"},
        json={"name": name},
    )
    assert response.status_code == 201, response.text
    dataset_id = response.json()["id"]
    assert isinstance(dataset_id, str)
    return dataset_id


def _upload_preparation(client: TestClient, token: str, dataset_id: str, name: str) -> str:
    response = client.post(
        f"/api/v1/datasets/{dataset_id}/preparations?name={name}&filename=records.jsonl",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/x-ndjson",
        },
        content=(
            b'{"speaker":"a","text":"hello","scene":"s1"}\n'
            b'{"speaker":"a","text":"again","scene":"s1"}\n'
            b'{"speaker":"b","text":"world","scene":"s2"}\n'
            b'{"speaker":"b","text":"done","scene":"s2"}\n'
        ),
    )
    assert response.status_code == 201, response.text
    preparation_id = response.json()["id"]
    assert isinstance(preparation_id, str)
    return preparation_id


def test_trial_bearer_can_prepare_only_its_workspace_dataset(tmp_path: Path) -> None:
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
        trial_authenticator=_authenticator(),
    )
    headers_a = {"Authorization": f"Bearer {TOKEN_A}"}
    headers_b = {"Authorization": f"Bearer {TOKEN_B}"}

    try:
        with TestClient(app) as client:
            assert (
                client.post("/api/v1/datasets", json={"name": "missing-token"}).status_code == 401
            )

            dataset_a = _create_dataset(client, TOKEN_A, "trial-dataset-a")
            dataset_b = _create_dataset(client, TOKEN_B, "trial-dataset-b")
            preparation_a = _upload_preparation(client, TOKEN_A, dataset_a, "trial-a")
            preparation_b = _upload_preparation(client, TOKEN_B, dataset_b, "trial-b")

            listed_a = client.get(f"/api/v1/datasets/{dataset_a}/preparations", headers=headers_a)
            assert listed_a.status_code == 200, listed_a.text
            assert [row["id"] for row in listed_a.json()] == [preparation_a]
            fetched_a = client.get(f"/api/v1/preparations/{preparation_a}", headers=headers_a)
            assert fetched_a.status_code == 200, fetched_a.text
            assert fetched_a.json()["datasetId"] == dataset_a
            preview_a = client.get(
                f"/api/v1/preparations/{preparation_a}/samples?stage=raw", headers=headers_a
            )
            assert preview_a.status_code == 200, preview_a.text
            mapping = {
                "mode": "instruction",
                "instruction": {"field": "speaker"},
                "output": {"field": "text"},
                "groupBy": "scene",
            }
            normalization = {
                "trimWhitespace": True,
                "collapseWhitespace": True,
                "unicodeNfc": True,
            }
            mapped = client.patch(
                f"/api/v1/preparations/{preparation_a}/mapping",
                headers=headers_a,
                json={"mapping": mapping, "normalization": normalization},
            )
            assert mapped.status_code == 200, mapped.text
            split = client.patch(
                f"/api/v1/preparations/{preparation_a}/split",
                headers=headers_a,
                json={"split": {"trainRatio": 0.5}},
            )
            assert split.status_code == 200, split.text
            confirmed = client.post(
                f"/api/v1/preparations/{preparation_a}/confirm", headers=headers_a
            )
            assert confirmed.status_code == 200, confirmed.text
            published = client.post(
                f"/api/v1/preparations/{preparation_a}/publish",
                headers={**headers_a, "Idempotency-Key": "trial-publish-a"},
            )
            assert published.status_code == 201, published.text
            version_id = published.json()["datasetVersion"]["id"]
            replay = client.post(
                f"/api/v1/preparations/{preparation_a}/publish",
                headers={**headers_a, "Idempotency-Key": "trial-publish-a"},
            )
            assert replay.status_code == 201, replay.text
            assert replay.json()["datasetVersion"]["id"] == version_id
            exports = client.get(f"/api/v1/preparations/{preparation_a}/exports", headers=headers_a)
            assert exports.status_code == 200, exports.text
            download = client.get(
                f"/api/v1/preparations/{preparation_a}/exports/train.jsonl", headers=headers_a
            )
            assert download.status_code == 200, download.text
            assert download.content

            listed_b = client.get(f"/api/v1/datasets/{dataset_b}/preparations", headers=headers_b)
            assert listed_b.status_code == 200, listed_b.text
            assert [row["id"] for row in listed_b.json()] == [preparation_b]
            fetched_b = client.get(f"/api/v1/preparations/{preparation_b}", headers=headers_b)
            assert fetched_b.status_code == 200, fetched_b.text

            assert (
                client.get(
                    f"/api/v1/datasets/{dataset_a}/preparations", headers=headers_b
                ).status_code
                == 404
            )
            assert (
                client.get(
                    f"/api/v1/datasets/{dataset_b}/preparations", headers=headers_a
                ).status_code
                == 404
            )
            assert (
                client.get(f"/api/v1/preparations/{preparation_a}", headers=headers_b).status_code
                == 404
            )
            assert (
                client.get(f"/api/v1/preparations/{preparation_b}", headers=headers_a).status_code
                == 404
            )
            assert client.get(f"/api/v1/preparations/{preparation_a}").status_code == 401
            assert (
                client.get(
                    f"/api/v1/preparations/{preparation_a}/samples?stage=raw", headers=headers_b
                ).status_code
                == 404
            )
            assert (
                client.get(
                    f"/api/v1/preparations/{preparation_a}/exports", headers=headers_b
                ).status_code
                == 404
            )
            assert (
                client.get(
                    f"/api/v1/preparations/{preparation_a}/exports/train.jsonl",
                    headers=headers_b,
                ).status_code
                == 404
            )
            denied_upload = client.post(
                f"/api/v1/datasets/{dataset_a}/preparations?name=wrong-workspace&filename=records.jsonl",
                headers={
                    **headers_b,
                    "Content-Type": "application/x-ndjson",
                },
                content=b'{"instruction":"x","output":"y"}\n',
            )
            assert denied_upload.status_code == 404
    finally:
        app.state.catalyst_store.close()


def test_preparation_routes_remain_open_when_trial_auth_is_unconfigured(tmp_path: Path) -> None:
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
        trial_authenticator=WorkspaceServiceAuthenticator.from_json(None),
    )
    try:
        with TestClient(app) as client:
            created = client.post("/api/v1/datasets", json={"name": "legacy-dataset"})
            assert created.status_code == 201, created.text
            dataset_id = created.json()["id"]
            uploaded = client.post(
                f"/api/v1/datasets/{dataset_id}/preparations?name=legacy&filename=records.jsonl",
                headers={"Content-Type": "application/x-ndjson"},
                content=b'{"instruction":"hello","output":"world"}\n',
            )
            assert uploaded.status_code == 201, uploaded.text
            preparation_id = uploaded.json()["id"]
            listed = client.get(f"/api/v1/datasets/{dataset_id}/preparations")
            assert listed.status_code == 200, listed.text
            assert [row["id"] for row in listed.json()] == [preparation_id]
            fetched = client.get(f"/api/v1/preparations/{preparation_id}")
            assert fetched.status_code == 200, fetched.text
    finally:
        app.state.catalyst_store.close()
