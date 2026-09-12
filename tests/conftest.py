"""Direct Plugin endpoint used by Catalyst integration tests.

The fixture imports the owner package installed from Cyrene-Plugins-Official.
It deliberately does not implement dataset preparation inside this Product.
"""

from __future__ import annotations

import os
from typing import Any

_SERVERS: list[Any] = []
_CONNECTION_ENV = "CYRENE_DATASET_PREPARATION_CONNECTION_REF"


def pytest_configure() -> None:
    """Start the canonical dataset.preparation.v1 implementation."""

    try:
        from cyrene_plugin_runtime import serve
        from dataset_preparation import DatasetPreparationPlugin
    except ImportError as exc:  # pragma: no cover - packaging failure has its own message
        raise RuntimeError(
            "Catalyst tests require the pinned Cyrene Plugins runtime and "
            "dataset-preparation owner package"
        ) from exc

    server, connection_ref = serve(
        DatasetPreparationPlugin(),
        "dataset.preparation.v1",
        "1",
        "127.0.0.1:0",
    )
    _SERVERS.append(server)
    os.environ[_CONNECTION_ENV] = connection_ref


def pytest_unconfigure() -> None:
    """Stop the owner endpoint after the test session."""

    os.environ.pop(_CONNECTION_ENV, None)
    for server in _SERVERS:
        server.stop(grace=None).wait()
    _SERVERS.clear()
