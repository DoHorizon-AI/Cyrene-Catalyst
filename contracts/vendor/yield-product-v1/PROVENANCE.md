# Vendored Yield Product contract v1 (consumer fixture)

本目录是 Catalyst 作为 **consumer** 使用的 Yield-owned Product contract v1 只读快照，
用于离线 consumer contract tests。它不是第二份契约权威，也不得在本地修改。

This directory is a read-only **consumer-side snapshot** of the Yield-owned Product
contract v1. It exists so Catalyst's ordinary build and tests never require a
sibling `Cyrene-Yield` checkout. It is not a second contract authority and must
not be edited in place.

## Provenance / 溯源

- Owner repository: `Cyrene-Yield` (`https://github.com/DoHorizon-AI/Cyrene-Yield.git`)
- Source commit: `bfa3a09e1aa0f627ecb407fdf7deb87e22d79748` (owner-side reviewed
  candidate containing `YLD-002`; this SHA is not reachable from the current
  `origin/develop` ref)
- Remote baseline with byte-identical artifact: `origin/develop`
  `6add08a8e625c57987016a64016b679bcb04e132` (no `contracts/` path changed between
  the two commits)
- Source root in owner repository: `contracts/product/v1/`
- Vendored on: 2026-09-11 (America/New_York)
- Public-clone status: `BLOCKED` until the owner publishes this reviewed snapshot
  at a reachable immutable ref or Catalyst refreshes the snapshot from one.

公开 clone 状态：`BLOCKED`。必须先由 owner 将已审查快照发布到可达的不可变引用，
或由 Catalyst 从可达引用重新刷新快照。

| Vendored path / 本地路径 | Owner path / 源路径 | sha256 |
| --- | --- | --- |
| `openapi.yaml` | `contracts/product/v1/openapi.yaml` | `42adedc40212d7486c282f374ec086af5801ba94d646d54d97f97e9a011fad06` |
| `model-version.schema.json` | `contracts/product/v1/model-version.schema.json` | `b8287ab4558581a2534dee254925c42cb58a09da82f5dd08d4eee1762aafe6b4` |
| `generated/platform/artifact-ref.schema.json` | `contracts/product/v1/generated/platform/artifact-ref.schema.json` | `0cdbbbb161d25844376e7a9706fbd091e7a5b65bbb92196b33c62d3c7b13f7cb` |
| `generated/common/problem-details.schema.json` | `contracts/product/v1/generated/common/problem-details.schema.json` | `dafd8166b493153ec589dd05dddc8a29ee2691a8f9aa3067722e435769aabe5f` |

## Rules / 规则

- 普通 build/test 只读取本快照；不得要求相邻 Yield 检出、绝对路径或 Yield 内部源码。
- Refresh 必须从 Yield owner 仓库的 reviewed SHA 重新整体复制并更新上表摘要；
  不得手工编辑快照内容。
- `tests/test_yield_contract.py` 校验上述摘要，并在快照上运行 consumer contract tests。
- Yield 是 `TrainingDraft`、`DatasetVersionRef`、`ArtifactRef` 投影的契约 owner；
  Catalyst 只在 `POST /api/v1/training-drafts` 这一 consumer 视角消费它。

- Ordinary build/test reads this snapshot only; no sibling Yield checkout, absolute
  path, or Yield implementation source may be required.
- Refresh by re-copying the whole artifact from a reviewed Yield SHA and updating the
  digest table; never hand-edit snapshot content.
- `tests/test_yield_contract.py` verifies these digests and runs the consumer contract
  tests against the snapshot.
- Yield owns `TrainingDraft`, `DatasetVersionRef`, and the `ArtifactRef` projection;
  Catalyst consumes only the `POST /api/v1/training-drafts` boundary.
