# Architecture documentation / 架构文档

This directory explains Catalyst's product boundary, capability model, and integration posture.

本目录说明 Catalyst 的产品边界、能力模型与集成方式。

| File | Responsibility / 职责 |
|---|---|
| [`overview.md`](overview.md) | Product boundary, lifecycle state flow, and artifact data flow / 产品边界、生命周期状态流与制品数据流 |
| [`tool-system.md`](tool-system.md) | Extension points and ownership rules / 扩展点与职责规则 |
| [`mcp-integration.md`](mcp-integration.md) | MCP adapter boundary and current implementation status / MCP 适配边界与当前实现状态 |

## Suggested reading order / 推荐阅读顺序

Read `overview.md` first, then `tool-system.md`, and finally `mcp-integration.md` when working on an external protocol adapter.

先阅读 `overview.md`，再阅读 `tool-system.md`；涉及外部协议适配时，最后阅读 `mcp-integration.md`。
