"""Security, workspace isolation, and XSS hardening tests (Phase 5)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from fastapi.testclient import TestClient

from cyrene_catalyst import create_app
from cyrene_catalyst.workspace_auth import WorkspaceServiceAuthenticator

TOKEN_ORG_A = "token-org-a-" + "a" * 40
TOKEN_ORG_B = "token-org-b-" + "b" * 40


def _make_app(tmp_path: Path, token_env: dict[str, str] | None = None):
    auth_config = [
        {
            "tokenSha256": hashlib.sha256(TOKEN_ORG_A.encode("ascii")).hexdigest(),
            "organizationId": "org-alpha",
            "workspaceId": "ws-alpha",
        },
        {
            "tokenSha256": hashlib.sha256(TOKEN_ORG_B.encode("ascii")).hexdigest(),
            "organizationId": "org-beta",
            "workspaceId": "ws-beta",
        },
    ]
    authenticator = WorkspaceServiceAuthenticator.from_json(json.dumps(auth_config))
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
        workspace_authenticator=authenticator,
    )
    return app


def test_cross_instance_and_workspace_isolation(tmp_path: Path) -> None:
    """A resource created by tenant A must return 404 to a different workspace.

    Tenant A is org-alpha/ws-alpha; tenant B is org-beta/ws-beta.
    """
    app = _make_app(tmp_path)
    with TestClient(app) as client:
        # 1. Tenant A creates a dataset in private workspace
        headers_a = {"Authorization": f"Bearer {TOKEN_ORG_A}"}
        create_res_a = client.post(
            "/internal/workspace/v1/datasets",
            json={"name": "Dataset A", "description": "Private dataset for Org Alpha"},
            headers=headers_a,
        )
        assert create_res_a.status_code == 201
        dataset_a_id = create_res_a.json()["id"]

        # 2. Tenant B creates a dataset in private workspace
        headers_b = {"Authorization": f"Bearer {TOKEN_ORG_B}"}
        create_res_b = client.post(
            "/internal/workspace/v1/datasets",
            json={"name": "Dataset B", "description": "Private dataset for Org Beta"},
            headers=headers_b,
        )
        assert create_res_b.status_code == 201
        dataset_b_id = create_res_b.json()["id"]

        # 3. Tenant A can only list Tenant A's dataset
        list_a = client.get("/internal/workspace/v1/datasets", headers=headers_a).json()
        assert [item["id"] for item in list_a] == [dataset_a_id]

        # 4. Tenant B can only list Tenant B's dataset
        list_b = client.get("/internal/workspace/v1/datasets", headers=headers_b).json()
        assert [item["id"] for item in list_b] == [dataset_b_id]

        # 5. Bad token returns 401
        res_bad = client.get(
            "/internal/workspace/v1/datasets",
            headers={"Authorization": "Bearer totally-invalid-token"},
        )
        assert res_bad.status_code == 401

        # 6. Public route without workspace token cannot access private workspace dataset -> 404
        res_public_a = client.get(f"/api/v1/datasets/{dataset_a_id}")
        assert res_public_a.status_code == 404
        res_public_b = client.get(f"/api/v1/datasets/{dataset_b_id}")
        assert res_public_b.status_code == 404

    app.state.catalyst_service.close()


def test_unauthorized_artifact_download_path_traversal(tmp_path: Path) -> None:
    """Path traversal sequences in export file download must be rejected with 404."""
    app = _make_app(tmp_path)
    with TestClient(app) as client:
        create_res = client.post(
            "/api/v1/datasets",
            json={"name": "Dataset For Download", "description": "Test"},
        )
        assert create_res.status_code == 201
        dataset_id = create_res.json()["id"]

        prep_res = client.post(
            f"/api/v1/datasets/{dataset_id}/preparations?name=my-prep&filename=test.txt",
            content=b"Sample document text for test",
            headers={"Content-Type": "text/plain"},
        )
        assert prep_res.status_code == 201
        prep_id = prep_res.json()["id"]

        # Encoded traversal attempt: ..%2F..%2F..%2Fetc%2Fpasswd -> 404
        bad_export = client.get(
            f"/api/v1/preparations/{prep_id}/exports/..%2F..%2F..%2Fetc%2Fpasswd",
        )
        assert bad_export.status_code == 404

        # Nonexistent export -> 404 with CATALYST_EXPORT_NOT_FOUND
        not_found_export = client.get(
            f"/api/v1/preparations/{prep_id}/exports/nonexistent-file.jsonl",
        )
        assert not_found_export.status_code == 404
        assert not_found_export.json()["code"] == "CATALYST_EXPORT_NOT_FOUND"

    app.state.catalyst_service.close()


def test_xss_in_dataset_name_and_filename(tmp_path: Path) -> None:
    """XSS payloads in names and filenames must be safely handled.

    The test verifies that the content remains JSON and does not execute as script.
    """
    app = _make_app(tmp_path)
    headers_a = {"Authorization": f"Bearer {TOKEN_ORG_A}"}
    xss_payload = '<script>alert("XSS")</script><img src=x onerror=alert(1)>'
    with TestClient(app) as client:
        # Create dataset with XSS in name and description
        create_res = client.post(
            "/api/v1/datasets",
            json={"name": xss_payload, "description": '"><svg onload=alert(2)>'},
            headers=headers_a,
        )
        assert create_res.status_code == 201
        data = create_res.json()
        assert data["name"] == xss_payload

        # Ensure content-type is application/json (not text/html) so browser won't execute it
        assert "application/json" in create_res.headers.get("content-type", "")

    app.state.catalyst_service.close()
