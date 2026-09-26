# Vendored Yield Product contract v1 (consumer fixture)

本目录是 Catalyst 作为 **consumer** 使用的 Yield-owned Product contract v1 只读快照，
用于离线 consumer contract tests。它不是第二份契约权威，也不得在本地修改。

This directory is a read-only **consumer-side snapshot** of the Yield-owned Product
contract v1. It exists so Catalyst's ordinary build and tests never require a
sibling `Cyrene-Yield` checkout. It is not a second contract authority and must
not be edited in place.

## Provenance / 溯源

- Owner repository: `Cyrene-Yield` (`https://github.com/DoHorizon-AI/Cyrene-Yield.git`)
- Source commit: `6fea8f835ce2561aaed4b0d9996856f6a3ef1ee6` (the clean-root
  root commit on the owner's canonical `main`; reachable from the current
  `Cyrene-Yield` `main` ref)
- Remote baseline with byte-identical artifact: owner `main`
  `6fea8f835ce2561aaed4b0d9996856f6a3ef1ee6` (all four files below were
  copied from this ref; no sibling Yield checkout is required)
- Source root in owner repository: `contracts/product/v1/`
- Vendored on: 2026-09-12 (America/New_York)
- Snapshot provenance status: `CURRENT_REACHABLE`; the former stale/unreachable
  snapshot blocker is cleared. The owner repository is still private, so
  anonymous provenance inspection remains an external publication consideration.
- Public-clone/release status: `BLOCKED_BY_EXTERNAL_CLOSURE` only by the
  unresolved private/unlicensed Plugins dependency and the separate release
  gates recorded in Catalyst's dependency documentation; ordinary Catalyst
  build/test uses this self-contained snapshot.

快照溯源状态：`CURRENT_REACHABLE`。旧的不可达快照阻塞已解除；来源提交已从
owner 的新 `Cyrene-Yield` `main` 可达。owner 仓库仍为 private，因此匿名溯源
检查仍是外部公开考量。

公开 clone/release 状态：`BLOCKED_BY_EXTERNAL_CLOSURE`，仅受仍为 private 且
许可证未完整声明的 Plugins 依赖及 Catalyst 依赖文档记录的独立 release 门禁影响；
普通 Catalyst build/test 使用本地自包含快照，不要求相邻 Yield 检出。

| Vendored path / 本地路径 | Owner path / 源路径 | sha256 |
| --- | --- | --- |
| `openapi.yaml` | `contracts/product/v1/openapi.yaml` | `42adedc40212d7486c282f374ec086af5801ba94d646d54d97f97e9a011fad06` |
| `model-version.schema.json` | `contracts/product/v1/model-version.schema.json` | `b8287ab4558581a2534dee254925c42cb58a09da82f5dd08d4eee1762aafe6b4` |
| `generated/platform/artifact-ref.schema.json` | `contracts/product/v1/generated/platform/artifact-ref.schema.json` | `c71d57b9a3e7e01f7fde458fdd6387987653ae75d15227f959e1a7ad3d5731b4` |
| `generated/common/problem-details.schema.json` | `contracts/product/v1/generated/common/problem-details.schema.json` | `dafd8166b493153ec589dd05dddc8a29ee2691a8f9aa3067722e435769aabe5f` |

## Rules / 规则

- 普通 build/test 只读取本快照；不得要求相邻 Yield 检出、绝对路径或 Yield 内部源码。
- Refresh 必须从 Yield owner 仓库的 reviewed、可达 SHA 重新整体复制四个既定
  snapshot 文件并更新上表摘要；不得手工编辑快照内容或引入其它 owner 文件。
- `tests/test_yield_contract.py` 校验上述摘要，并在快照上运行 consumer contract tests。
- Yield 是 `TrainingDraft`、`DatasetVersionRef`、`ArtifactRef` 投影的契约 owner；
  Catalyst 只在 `POST /api/v1/training-drafts` 这一 consumer 视角消费它。

- Ordinary build/test reads this snapshot only; no sibling Yield checkout, absolute
  path, or Yield implementation source may be required.
- Refresh by re-copying the four established snapshot files from a reviewed,
  reachable Yield SHA and updating the digest table; never hand-edit snapshot
  content or add other owner files.
- `tests/test_yield_contract.py` verifies these digests and runs the consumer contract
  tests against the snapshot.
- Yield owns `TrainingDraft`, `DatasetVersionRef`, and the `ArtifactRef` projection;
  Catalyst consumes only the `POST /api/v1/training-drafts` boundary.
---
<!-- Chinese Translation / 中文翻译 -->

## 规则补充译文

- 普通 build/test 只读取此快照；不得要求相邻 Yield 检出、绝对路径或 Yield 实现源码。
- 刷新时必须从经过审查且可达的 Yield SHA 重新复制既定的四个快照文件，并更新摘要表；不得手工编辑快照内容或加入其他 owner 文件。
- `tests/test_yield_contract.py` 会校验这些摘要，并基于该快照运行 consumer contract tests。
- Yield 拥有 `TrainingDraft`、`DatasetVersionRef` 和 `ArtifactRef` 投影；Catalyst 仅以 `POST /api/v1/training-drafts` 这一 consumer 边界消费它们。
