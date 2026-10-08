"""Installed-consumer checks that run before any dataset mutation.

中文：验证已安装服务消费器会在写入数据前检查必需能力绑定。
"""

from __future__ import annotations

import httpx
import pytest

from cyrene_catalyst.external_acceptance import AcceptanceFailure, _capability_preflight


def _report(*, generation_configured: bool) -> dict[str, object]:
    return {
        "semantics": "configuration-only",
        "activationVerified": False,
        "capabilities": [
            {
                "id": "dataset.preparation.v1",
                "supported": True,
                "configured": True,
                "configurationEnvironmentVariable": "CYRENE_DATASET_PREPARATION_CONNECTION_REF",
            },
            {
                "id": "dataset.generation.v1",
                "supported": True,
                "configured": generation_configured,
                "configurationEnvironmentVariable": "CYRENE_DATASET_GENERATION_CONNECTION_REF",
            },
        ],
    }


def _client(report: dict[str, object]) -> httpx.Client:
    def respond(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/api/v1/system/capabilities"
        return httpx.Response(200, json=report)

    return httpx.Client(
        base_url="https://catalyst.example",
        transport=httpx.MockTransport(respond),
    )


def test_preflight_requires_both_installed_workflow_capabilities() -> None:
    """Preparation and SFT generation must be configured before fixture upload."""

    with _client(_report(generation_configured=True)) as client:
        report = _capability_preflight(client)

    assert report["activationVerified"] is False


def test_preflight_rejects_missing_generation_before_dataset_creation() -> None:
    """A missing SFT provider fails before the acceptance consumer creates a dataset."""

    with (
        _client(_report(generation_configured=False)) as client,
        pytest.raises(
            AcceptanceFailure,
            match="CYRENE_DATASET_GENERATION_CONNECTION_REF",
        ),
    ):
        _capability_preflight(client)
