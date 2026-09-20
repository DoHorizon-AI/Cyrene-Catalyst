"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 cli.py                                                          │
│  Module: cyrene_catalyst.cli                                        │
│  Role: Operator entrypoint for data preparation and publishing.     │
│                                                                     │
│  模块职责：数据准备与发布的命令行入口；每个命令只执行一个显式产品动作。       │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlencode

import httpx
import uvicorn

from cyrene_catalyst.api import create_app


def _base_url(value: str) -> str:
    if not value.startswith(("http://", "https://")):
        raise ValueError("CATALYST_URL_INVALID: use an http(s) Product URL")
    return value.rstrip("/")


def _client(arguments: argparse.Namespace) -> httpx.Client:
    headers = {"Accept": "application/json"}
    token = os.environ.get(arguments.token_env) if arguments.token_env else None
    if token:
        headers["Authorization"] = "Bearer " + token
    return httpx.Client(base_url=_base_url(arguments.url), headers=headers, timeout=1800.0)


def _call(client: httpx.Client, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
    response = client.request(method, path, **kwargs)
    if response.status_code >= 300:
        try:
            code = response.json().get("code", "CATALYST_REQUEST_REJECTED")
        except ValueError:
            code = "CATALYST_REQUEST_REJECTED"
        raise SystemExit(f"Catalyst: HTTP {response.status_code}, {code}")
    return cast(dict[str, Any], response.json())


def _serve(arguments: argparse.Namespace) -> int:
    base = arguments.home.expanduser()
    base.mkdir(parents=True, exist_ok=True)
    app = create_app(
        database_path=base / "catalyst.sqlite3",
        artifact_root=arguments.artifact_root or base / "artifacts",
        yield_url=arguments.yield_url,
    )
    uvicorn.run(app, host=arguments.host, port=arguments.port, access_log=False)
    return 0


def _dataset_import(arguments: argparse.Namespace) -> int:
    payload = arguments.file.read_bytes()
    with _client(arguments) as client:
        dataset = _call(client, "POST", "/api/v1/datasets", json={"name": arguments.name})
        query = urlencode({"name": arguments.name, "filename": arguments.file.name})
        preparation = _call(
            client,
            "POST",
            f"/api/v1/datasets/{dataset['id']}/preparations?{query}",
            content=payload,
            headers={"Content-Type": "application/octet-stream"},
        )
    print(json.dumps({"dataset": dataset, "preparation": preparation}, indent=2))
    return 0


def _dataset_map(arguments: argparse.Namespace) -> int:
    with _client(arguments) as client:
        preparation = _call(
            client,
            "PATCH",
            f"/api/v1/preparations/{arguments.preparation}/mapping",
            json={
                "mapping": {
                    "mode": "instruction",
                    "instruction": {"field": arguments.instruction_field},
                    "input": {"field": arguments.input_field},
                    "output": {"field": arguments.output_field},
                },
                "normalization": {},
            },
        )
    print(json.dumps(preparation, indent=2))
    return 0


def _dataset_split(arguments: argparse.Namespace) -> int:
    with _client(arguments) as client:
        preparation = _call(
            client,
            "PATCH",
            f"/api/v1/preparations/{arguments.preparation}/split",
            json={"split": {"trainRatio": arguments.train_ratio}},
        )
    print(json.dumps(preparation, indent=2))
    return 0


def _dataset_publish(arguments: argparse.Namespace) -> int:
    with _client(arguments) as client:
        _call(client, "POST", f"/api/v1/preparations/{arguments.preparation}/confirm")
        published = _call(
            client,
            "POST",
            f"/api/v1/preparations/{arguments.preparation}/publish",
            headers={"Idempotency-Key": "publish:" + arguments.preparation},
        )
    print(json.dumps(published["datasetVersion"], indent=2))
    return 0


def _add_connection_arguments(command: argparse.ArgumentParser) -> None:
    command.add_argument("--url", default="http://127.0.0.1:8014")
    command.add_argument(
        "--token-env",
        help="Name of the environment variable holding the Product credential",
    )


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description="Cyrene Catalyst Product operator CLI")
    commands = value.add_subparsers(dest="command", required=True)

    serve = commands.add_parser("serve", help="Run the Catalyst Product API")
    serve.add_argument("--home", type=Path, default=Path.cwd() / ".catalyst")
    serve.add_argument("--artifact-root", type=Path)
    serve.add_argument("--yield-url")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8014)

    dataset = commands.add_parser("dataset", help="Prepare and publish dataset versions")
    dataset_commands = dataset.add_subparsers(dest="dataset_command", required=True)

    import_command = dataset_commands.add_parser("import")
    import_command.add_argument("file", type=Path)
    import_command.add_argument("--name", required=True)
    _add_connection_arguments(import_command)

    map_command = dataset_commands.add_parser("map")
    map_command.add_argument("--preparation", required=True)
    map_command.add_argument("--instruction-field", default="instruction")
    map_command.add_argument("--input-field", default="input")
    map_command.add_argument("--output-field", default="output")
    _add_connection_arguments(map_command)

    split_command = dataset_commands.add_parser("split")
    split_command.add_argument("--preparation", required=True)
    split_command.add_argument("--train-ratio", type=float, default=1.0)
    _add_connection_arguments(split_command)

    publish_command = dataset_commands.add_parser("publish")
    publish_command.add_argument("--preparation", required=True)
    _add_connection_arguments(publish_command)
    return value


def main() -> int:
    arguments = parser().parse_args()
    if arguments.command == "serve":
        return _serve(arguments)
    handlers = {
        "import": _dataset_import,
        "map": _dataset_map,
        "split": _dataset_split,
        "publish": _dataset_publish,
    }
    return handlers[arguments.dataset_command](arguments)


if __name__ == "__main__":
    sys.exit(main())
