"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 __main__.py                                                     │
│  Module: cyrene_catalyst.__main__                                   │
│  Role: Local ASGI entry point for the interactive preparation UI.   │
│                                                                     │
│  模块职责：本地 ASGI 入口，启动整理工具界面。                            │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import os
from pathlib import Path

import uvicorn

from cyrene_catalyst.api import create_app


def main() -> None:
    """Start the local Catalyst preparation UI. | 启动本地整理界面。"""

    base = Path(os.environ.get("CATALYST_HOME", str(Path.cwd() / ".catalyst")))
    base.mkdir(parents=True, exist_ok=True)
    app = create_app(
        database_path=base / "catalyst.sqlite3",
        artifact_root=Path(os.environ.get("CYRENE_ARTIFACT_ROOT", str(base / "artifacts"))),
        yield_url=os.environ.get("CYRENE_YIELD_URL"),
    )
    uvicorn.run(
        app,
        host=os.environ.get("CATALYST_HOST", "127.0.0.1"),
        port=int(os.environ.get("CATALYST_PORT", "8014")),
    )


if __name__ == "__main__":
    main()
