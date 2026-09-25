"""Direct Plugin endpoint used by Catalyst integration tests.

The fixture imports the owner package installed from Cyrene-Plugins-Official.
It deliberately does not implement dataset preparation inside this Product.

中文：用于 Catalyst 集成测试的 Direct Plugin endpoint。fixture 会导入从 Cyrene-Plugins-Official 安装的 owner package；它不会在这个 Product 内实现 dataset preparation。
"""
# 中文：Catalyst 集成测试使用的直连 Plugin 端点。此夹具导入从 Cyrene-Plugins-Official 安装的 owner 包；它刻意不在此 Product 中实现 dataset preparation。

from __future__ import annotations

import os
from typing import Any

_SERVERS: list[Any] = []
_CONNECTION_ENV = "CYRENE_DATASET_PREPARATION_CONNECTION_REF"


def pytest_configure() -> None:
    """Start the canonical dataset.preparation.v1 implementation.

    中文：启动 canonical dataset.preparation.v1 实现。
    """
# 中文：启动规范的 dataset.preparation.v1 实现。

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
    """Stop the owner endpoint after the test session.

    中文：在测试会话结束后停止 owner endpoint。
    """
# 中文：测试会话结束后停止 owner 端点。

    os.environ.pop(_CONNECTION_ENV, None)
    for server in _SERVERS:
        server.stop(grace=None).wait()
    _SERVERS.clear()
