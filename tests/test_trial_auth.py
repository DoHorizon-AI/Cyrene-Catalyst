"""Focused authentication and remote-listener checks for the trial API."""

from __future__ import annotations

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from cyrene_catalyst.trial_auth import (
    TrialAuthConfigError,
    install_trial_auth,
    is_loopback_host,
    trial_authenticator_from_environment,
    trial_principal_from_request,
    validate_trial_listener,
)
from cyrene_catalyst.workspace_auth import WorkspaceServicePrincipal

TOKEN = "data-tools-test-token-0123456789abcdef"


def _authenticator(token: str = TOKEN):
    return trial_authenticator_from_environment({"CYRENE_DATA_TOOLS_TOKEN": token})


def test_trial_auth_rejects_missing_or_invalid_credentials_and_passes_principal() -> None:
    app = FastAPI()
    install_trial_auth(app, authenticator=_authenticator())

    @app.get("/api/v1/report/export")
    def export_report(request: Request) -> dict[str, str | None]:
        principal = trial_principal_from_request(request)
        return {
            "organizationId": principal.organization_id if principal else None,
            "workspaceId": principal.workspace_id if principal else None,
        }

    app.add_api_route("/api/v1/upload", export_report, methods=["POST"])
    app.add_api_route("/api/v1/download", export_report, methods=["GET"])

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    with TestClient(app) as client:
        unauthorized = client.get("/api/v1/report/export")
        assert unauthorized.status_code == 401
        assert unauthorized.headers["www-authenticate"] == "Bearer"
        assert TOKEN not in unauthorized.text
        assert client.post("/api/v1/upload").status_code == 401
        assert client.get("/api/v1/download").status_code == 401
        invalid = client.get(
            "/api/v1/report/export",
            headers={"Authorization": "Bearer invalid-token"},
        )
        assert invalid.status_code == 401
        assert client.get("/healthz").status_code == 200
        authorized = client.get(
            "/api/v1/report/export",
            headers={"Authorization": f"Bearer {TOKEN}"},
        )
        assert authorized.status_code == 200
        assert authorized.json() == {
            "organizationId": "data-tools-trial",
            "workspaceId": "data-tools",
        }


def test_trial_auth_uses_one_configured_workspace_identity() -> None:
    authenticator = trial_authenticator_from_environment(
        {
            "CYRENE_DATA_TOOLS_TOKEN": TOKEN,
            "CYRENE_DATA_TOOLS_ORGANIZATION_ID": "org-a",
            "CYRENE_DATA_TOOLS_WORKSPACE_ID": "workspace-a",
        }
    )
    assert authenticator.authenticate(f"Bearer {TOKEN}") == WorkspaceServicePrincipal(
        "org-a", "workspace-a"
    )
    assert authenticator.authenticate("Bearer another-independent-token-123456") is None


def test_unconfigured_loopback_mode_keeps_legacy_requests_open() -> None:
    app = FastAPI()
    install_trial_auth(app, authenticator=trial_authenticator_from_environment({}))

    @app.get("/api/v1/legacy")
    def legacy(request: Request) -> dict[str, bool]:
        return {"hasPrincipal": trial_principal_from_request(request) is not None}

    with TestClient(app) as client:
        assert client.get("/api/v1/legacy").json() == {"hasPrincipal": False}


def test_remote_listener_requires_explicit_opt_in_and_configured_token() -> None:
    empty = trial_authenticator_from_environment({})
    configured = _authenticator()

    assert is_loopback_host("127.0.0.1")
    assert is_loopback_host("localhost")
    assert is_loopback_host("::1")
    assert not is_loopback_host("0.0.0.0")
    assert not validate_trial_listener("127.0.0.1", allow_remote=False, authenticator=empty)
    with pytest.raises(TrialAuthConfigError, match="--allow-remote"):
        validate_trial_listener("0.0.0.0", allow_remote=False, authenticator=configured)
    with pytest.raises(TrialAuthConfigError, match="CYRENE_DATA_TOOLS_TOKEN"):
        validate_trial_listener("0.0.0.0", allow_remote=True, authenticator=empty)
    assert validate_trial_listener("0.0.0.0", allow_remote=True, authenticator=configured)


def test_required_auth_fails_closed_and_rejects_weak_tokens() -> None:
    with pytest.raises(TrialAuthConfigError, match="CYRENE_DATA_TOOLS_TOKEN"):
        install_trial_auth(
            FastAPI(),
            authenticator=trial_authenticator_from_environment({}),
            required=True,
        )
    with pytest.raises(TrialAuthConfigError, match="printable ASCII"):
        trial_authenticator_from_environment({"CYRENE_DATA_TOOLS_TOKEN": "too-short"})
    with pytest.raises(TrialAuthConfigError, match="printable ASCII"):
        trial_authenticator_from_environment({"CYRENE_DATA_TOOLS_TOKEN": "x" * 31 + "\n"})
