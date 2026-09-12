# Contributing to Catalyst / 参与 Catalyst 贡献

Thank you for contributing to the Catalyst Product. Please read
[`SECURITY.md`](SECURITY.md), [`LICENSE`](LICENSE), and the public dependency
notes in [`docs/DEPENDENCIES.md`](docs/DEPENDENCIES.md) before opening a change.

感谢参与 Catalyst Product。提交修改前，请先阅读 [`SECURITY.md`](SECURITY.md)、
[`LICENSE`](LICENSE) 以及 [`docs/DEPENDENCIES.md`](docs/DEPENDENCIES.md) 中的公开
依赖说明。

## Scope / 范围

Catalyst owns Dataset/DatasetVersion state, preparation intent, quality policy,
lineage, publication, and explicit handoffs. Reusable parsing, transformation,
training, inference, process supervision, and hardware probing belong to their
owning Product, Platform, or Plugin repository.

Catalyst 负责 Dataset/DatasetVersion 状态、整理意图、质量策略、血缘、发布与明确
交接。可复用解析/变换、训练、推理、进程监管与硬件探测属于对应的 Product、
Platform 或 Plugin 仓库。

## Local setup and checks / 本地设置与检查

```bash
uv sync --locked --group dev
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy
uv run pytest -q
```

The test suite uses the locked Plugin contract and synthetic fixtures. A local
or simulated result is not evidence of hosted CI, a remote Product handoff, or
production storage.

测试套件使用锁定的 Plugin 契约与合成夹具。本地或模拟结果不等同于 Hosted CI、
远程 Product 交接或生产存储证据。

## Change and review rules / 修改与评审规则

- Keep API and schema changes under `/api/v1` and update the relevant contract docs.
- Keep `uv.lock` synchronized with dependency changes and update the third-party
  notice plus SBOM entry.
- New or substantially changed files under `docs/` must include English and
  Chinese text.
- Never add credentials, private dataset samples, generated artifacts, or local
  absolute paths.
- Use a focused branch and a conventional commit; open a pull request against
  `main` and describe local, hosted, and unrun evidence separately. The
  published source starts from the clean-root `main`; the private history
  archive is not a development base.

- `/api/v1` 下的 API 与 schema 修改必须同步更新对应契约文档。
- 依赖变更必须同步 `uv.lock`、第三方声明与 SBOM 入口。
- `docs/` 下新增或大幅修改的文件必须同时包含英文和中文。
- 不得加入凭证、私有数据集样本、生成制品或本机绝对路径。
- 使用聚焦分支与 Conventional Commit；向 `main` 提交 PR，并分别说明本地、
  Hosted 与未运行的证据。公开源码从 clean-root `main` 开始；私有历史归档不是
  开发基线。
