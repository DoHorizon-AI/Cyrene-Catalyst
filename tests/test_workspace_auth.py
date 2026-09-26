"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 test_workspace_auth.py                                          │
│  Module: tests.test_workspace_auth                                  │
│  Role: Workspace token, scope isolation, and migration acceptance.   │
│                                                                     │
│  模块职责：验证 Workspace 凭据、隔离边界与旧库迁移。                    │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from cyrene_catalyst import create_app
from cyrene_catalyst.domain import CreateDatasetRequest, Dataset
from cyrene_catalyst.service import request_hash
from cyrene_catalyst.workspace_auth import (
    WorkspaceServiceAuthConfigError,
    WorkspaceServiceAuthenticator,
    WorkspaceServicePrincipal,
)

TOKEN_A = "catalyst-workspace-a-token-" + "a" * 40
TOKEN_A_ROTATED = "catalyst-workspace-a-token-" + "b" * 40
TOKEN_B = "catalyst-workspace-b-token-" + "c" * 40


def _auth_config(*tokens: tuple[str, str, str]) -> str:
    return json.dumps(
        [
            {
                "tokenSha256": hashlib.sha256(token.encode("ascii")).hexdigest(),
                "organizationId": organization_id,
                "workspaceId": workspace_id,
            }
            for token, organization_id, workspace_id in tokens
        ]
    )


def _app(tmp_path: Path, auth_config: str | None = None, *, yield_url: str | None = None):
    authenticator = WorkspaceServiceAuthenticator.from_json(auth_config)
    return create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
        yield_url=yield_url,
        workspace_authenticator=authenticator,
    )


def _close(app) -> None:
    app.state.catalyst_store.close()


def test_workspace_authenticator_supports_rotation_and_rejects_bad_maps() -> None:
    authenticator = WorkspaceServiceAuthenticator.from_json(
        _auth_config(
            (TOKEN_A, "org-a", "workspace-a"),
            (TOKEN_A_ROTATED, "org-a", "workspace-a"),
        )
    )

    expected = WorkspaceServicePrincipal("org-a", "workspace-a")
    assert authenticator.authenticate(f"Bearer {TOKEN_A}") == expected
    assert authenticator.authenticate(f"Bearer {TOKEN_A_ROTATED}") == expected
    assert authenticator.authenticate(f"Bearer {'x' * 31}") is None
    assert authenticator.authenticate(f"Bearer {TOKEN_B}") is None
    assert not WorkspaceServiceAuthenticator.from_json(None).configured

    with pytest.raises(WorkspaceServiceAuthConfigError):
        WorkspaceServiceAuthenticator.from_json("not-json")
    with pytest.raises(WorkspaceServiceAuthConfigError):
        WorkspaceServiceAuthenticator.from_json(
            _auth_config((TOKEN_A, "org-a", "workspace-a"), (TOKEN_A, "org-b", "workspace-b"))
        )


def test_private_dataset_routes_fail_closed_and_partition_all_reads_and_replays(
    tmp_path: Path,
) -> None:
    app = _app(
        tmp_path,
        _auth_config(
            (TOKEN_A, "org-a", "workspace-a"),
            (TOKEN_B, "org-b", "workspace-b"),
        ),
    )
    headers_a = {"Authorization": f"Bearer {TOKEN_A}"}
    headers_b = {"Authorization": f"Bearer {TOKEN_B}"}

    try:
        with TestClient(app) as client:
            unauthenticated = client.get("/internal/workspace/v1/datasets")
            assert unauthenticated.status_code == 401
            assert unauthenticated.headers["www-authenticate"] == "Bearer"
            assert (
                client.post(
                    "/internal/workspace/v1/datasets",
                    headers={"Authorization": f"Bearer {'x' * 31}"},
                    json={"name": "too-short-token"},
                ).status_code
                == 401
            )

            first = client.post(
                "/internal/workspace/v1/datasets",
                headers={**headers_a, "Idempotency-Key": "shared-key"},
                json={"name": "workspace-a-data"},
            )
            assert first.status_code == 201
            first_id = first.json()["id"]
            replay = client.post(
                "/internal/workspace/v1/datasets",
                headers={**headers_a, "Idempotency-Key": "shared-key"},
                json={"name": "workspace-a-data"},
            )
            assert replay.status_code == 201
            assert replay.json()["id"] == first_id

            second = client.post(
                "/internal/workspace/v1/datasets",
                headers={**headers_b, "Idempotency-Key": "shared-key"},
                json={"name": "workspace-b-data"},
            )
            assert second.status_code == 201
            second_id = second.json()["id"]
            conflict = client.post(
                "/internal/workspace/v1/datasets",
                headers={**headers_a, "Idempotency-Key": "shared-key"},
                json={"name": "different-body"},
            )
            assert conflict.status_code == 409

            assert [
                item["id"]
                for item in client.get("/internal/workspace/v1/datasets", headers=headers_a).json()
            ] == [first_id]
            assert [
                item["id"]
                for item in client.get("/internal/workspace/v1/datasets", headers=headers_b).json()
            ] == [second_id]

            legacy = client.post("/api/v1/datasets", json={"name": "legacy-data"})
            assert legacy.status_code == 201
            legacy_id = legacy.json()["id"]
            assert [item["id"] for item in client.get("/api/v1/datasets").json()] == [legacy_id]
            assert client.get(f"/api/v1/datasets/{first_id}").status_code == 404
            assert client.get(f"/api/v1/datasets/{second_id}").status_code == 404
            assert [
                item["id"]
                for item in client.get("/internal/workspace/v1/datasets", headers=headers_a).json()
            ] == [first_id]
    finally:
        _close(app)


def test_missing_private_auth_configuration_returns_503_without_changing_legacy_api(
    tmp_path: Path,
) -> None:
    app = _app(tmp_path)
    try:
        with TestClient(app) as client:
            private = client.get("/internal/workspace/v1/datasets")
            assert private.status_code == 503
            legacy = client.post("/api/v1/datasets", json={"name": "legacy-only"})
            assert legacy.status_code == 201
            assert [item["id"] for item in client.get("/api/v1/datasets").json()] == [
                legacy.json()["id"]
            ]
    finally:
        _close(app)


def test_existing_unscoped_rows_migrate_without_workspace_assignment(tmp_path: Path) -> None:
    database = tmp_path / "catalyst.sqlite3"
    now = datetime(2024, 1, 1, tzinfo=UTC)
    request = CreateDatasetRequest(name="historical-dataset")
    dataset = Dataset(
        id=uuid4(),
        name=request.name,
        created_at=now,
        updated_at=now,
        resource_version=1,
    )
    digest = request_hash(request)
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE datasets(id TEXT PRIMARY KEY, document TEXT NOT NULL);
        CREATE TABLE dataset_versions(
            id TEXT PRIMARY KEY, dataset_id TEXT NOT NULL, version INTEGER NOT NULL,
            document TEXT NOT NULL, UNIQUE(dataset_id, version)
        );
        CREATE TABLE preparations(
            id TEXT PRIMARY KEY, dataset_id TEXT NOT NULL, document TEXT NOT NULL
        );
        CREATE TABLE idempotency(
            scope TEXT NOT NULL, key TEXT NOT NULL, request_hash TEXT NOT NULL,
            resource_kind TEXT NOT NULL, resource_id TEXT NOT NULL, PRIMARY KEY(scope, key)
        );
        """
    )
    connection.execute(
        "INSERT INTO datasets(id, document) VALUES (?, ?)",
        (str(dataset.id), dataset.model_dump_json(by_alias=True, exclude_none=True)),
    )
    connection.execute(
        "INSERT INTO idempotency(scope, key, request_hash, resource_kind, resource_id) "
        "VALUES ('create-dataset', 'historical-key', ?, 'dataset', ?)",
        (digest, str(dataset.id)),
    )
    connection.commit()
    connection.close()

    app = _app(
        tmp_path,
        _auth_config((TOKEN_A, "org-a", "workspace-a")),
    )
    try:
        with TestClient(app) as client:
            assert [item["id"] for item in client.get("/api/v1/datasets").json()] == [
                str(dataset.id)
            ]
            assert (
                client.get(
                    "/internal/workspace/v1/datasets",
                    headers={"Authorization": f"Bearer {TOKEN_A}"},
                ).json()
                == []
            )
            legacy_replay = client.post(
                "/api/v1/datasets",
                headers={"Idempotency-Key": "historical-key"},
                json={"name": request.name},
            )
            assert legacy_replay.status_code == 201
            assert legacy_replay.json()["id"] == str(dataset.id)

            private_create = client.post(
                "/internal/workspace/v1/datasets",
                headers={
                    "Authorization": f"Bearer {TOKEN_A}",
                    "Idempotency-Key": "historical-key",
                },
                json={"name": request.name},
            )
            assert private_create.status_code == 201
            assert private_create.json()["id"] != str(dataset.id)
    finally:
        _close(app)


def test_legacy_parent_and_child_reads_hide_workspace_datasets(tmp_path: Path) -> None:
    app = _app(
        tmp_path,
        _auth_config((TOKEN_A, "org-a", "workspace-a")),
        yield_url="http://127.0.0.1:8093",
    )
    try:
        with TestClient(app) as client:
            created = client.post(
                "/internal/workspace/v1/datasets",
                headers={"Authorization": f"Bearer {TOKEN_A}"},
                json={"name": "private-parent"},
            )
            dataset_id = created.json()["id"]
            preparation_id = str(uuid4())
            version_id = str(uuid4())
            store = app.state.catalyst_store
            with store._lock, store._connection:
                store._connection.execute(
                    "INSERT INTO preparations(id, dataset_id, document) VALUES (?, ?, '{}')",
                    (preparation_id, dataset_id),
                )
                store._connection.execute(
                    "INSERT INTO dataset_versions(id, dataset_id, version, document) "
                    "VALUES (?, ?, 1, '{}')",
                    (version_id, dataset_id),
                )

            assert client.get(f"/api/v1/datasets/{dataset_id}").status_code == 404
            assert client.get(f"/api/v1/datasets/{dataset_id}/versions").status_code == 404
            assert client.get(f"/api/v1/datasets/{dataset_id}/preparations").status_code == 404
            assert client.get(f"/api/v1/dataset-versions/{version_id}").status_code == 404
            assert client.get(f"/api/v1/dataset-versions/{version_id}/preview").status_code == 404
            assert (
                client.post(
                    f"/api/v1/dataset-versions/{version_id}/actions/send-to-yield"
                ).status_code
                == 404
            )
            assert client.get(f"/api/v1/preparations/{preparation_id}").status_code == 404
            assert client.get(f"/api/v1/preparations/{preparation_id}/samples").status_code == 404
            assert client.get(f"/api/v1/preparations/{preparation_id}/exports").status_code == 404
            assert (
                client.get(f"/api/v1/preparations/{preparation_id}/exports/train.jsonl").status_code
                == 404
            )
            assert client.post(f"/api/v1/preparations/{preparation_id}/publish").status_code == 404
            assert (
                client.post(
                    f"/api/v1/datasets/{dataset_id}/preparations?name=attempt",
                    content=b"data",
                    headers={"Content-Type": "application/octet-stream"},
                ).status_code
                == 404
            )
    finally:
        _close(app)
