# Third-party notices / 第三方声明

This record covers direct runtime, development, and test dependencies declared
by `pyproject.toml`. Exact resolved versions and hashes are authoritative in
[`uv.lock`](uv.lock). No upstream source is copied into the repository; the
vendor directory contains only a read-only Product contract snapshot, and the
sample data is synthetic.

本记录覆盖 `pyproject.toml` 声明的直接运行时、开发与测试依赖。精确解析版本与
摘要以 [`uv.lock`](uv.lock) 为准。本仓库未复制上游源码；vendor 目录只包含只读的
Product 契约快照，样例数据为合成数据。

## Runtime dependencies / 运行时依赖

| Package | Declared/locked version | Source | SPDX/license evidence | Use |
|---|---:|---|---|---|
| FastAPI | `0.141.1` | PyPI | `MIT` | HTTP adapter and OpenAPI generation |
| Pydantic | `2.13.5` | PyPI | `MIT` | API and domain validation |
| HTTPX | `0.28.1` | PyPI | `BSD-3-Clause` | Outbound Product handoff client |
| Uvicorn | `0.37.0` | PyPI | `BSD-3-Clause` | ASGI server entrypoint |
| `cyrene-plugin-runtime` | `0.2.0` | Plugins Git SHA `4d6f80f1117ab8f7ba389eb9b9049085335ffe2b` | `UNKNOWN` — upstream package has no license field or nearest license file | Direct typed Plugin invocation |

## Development and test dependencies / 开发与测试依赖

| Package | Locked version | SPDX/license evidence | Use |
|---|---:|---|---|
| `jsonschema` | `4.26.0` | `MIT` | Schema checks |
| `openapi-spec-validator` | `0.9.0` | `Apache-2.0` | Contract checks |
| `mypy` | `2.3.1` | `MIT` | Type checks |
| `pytest` | `9.1.1` | `MIT` | Tests |
| `ruff` | `0.16.5` | `MIT` | Lint and format checks |
| DuckDB | `1.5.5` | `MIT` | Independent CSV/Parquet export assertions |
| `cyrene-dataset-preparation` | `0.1.3` | `UNKNOWN` — upstream package has no license field or nearest license file | Plugin endpoint fixture |

## Transitive and unresolved items / 传递依赖与未解决项

The complete transitive graph is locked in `uv.lock` and must be included in a
release SBOM. The two Plugins packages above are pinned to a publicly reachable
immutable Git revision, but their package manifests do not declare a license.
That unresolved license evidence remains a binary-publication blocker and is
not permission to infer Apache-2.0 from the Catalyst license. Resolve it in the
owner packages, then refresh the lockfile and this record.

完整传递依赖图锁定在 `uv.lock` 中，必须纳入 release SBOM。上面的两个 Plugins 包
已锁定到公开可达的不可变 Git 修订版，但其包 manifest 尚未声明许可证；这仍是二进制
公开发布阻塞项，不能因为 Catalyst 的许可证而推断为 Apache-2.0。应由 owner 补齐
许可证声明，然后刷新锁文件与本记录。

See [`docs/DEPENDENCIES.md`](docs/DEPENDENCIES.md) for the SBOM procedure and
publication gate. This file does not grant rights to third-party dependencies;
their upstream license texts and notices control.

关于 SBOM 流程与公开门禁，请参阅 [`docs/DEPENDENCIES.md`](docs/DEPENDENCIES.md)。
本文件不授予第三方依赖任何权利；具体权利以各上游许可证文本与声明为准。
