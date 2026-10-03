# genshin-dialogue-zh — minimal synthetic sample

This directory holds a **minimal synthetic sample** (a few short records).
The original dialogue corpus was removed from this repository before this
sample was added.

## Where is the full corpus?
The complete corpus (162,791 records / 336 JSON / ~30MB, snapshot 2025-11-20)
was removed during the 2026-09 legacy-surface closure and is retained in
restricted, owner-managed storage outside this repository. This repository
intentionally does not record a machine-specific filesystem path.

## ⚠️ Provenance / license
The original corpus shipped with **no license file and no provenance metadata**.
It is third-party game dialogue. Treat the full corpus as **restricted/internal**
until a license + provenance review is completed by the data owner and legal.
Do not promote it to a public repo or a training run without sign-off.

## Record schema (illustrated by sample.json)
```json
[
  {
    "source_title": "<in-game source / quest>",
    "speaker":       "<character name>",
    "text":          "<dialogue line>"
  }
]
```
---
<!-- Chinese Translation / 中文翻译 -->

# genshin-dialogue-zh — 最小合成样本

本目录只包含一个**最小合成样本**（少量简短记录）。添加该样本前，原始对话语料已从本仓库移除。

## 完整语料存放在哪里？

完整语料包含 162,791 条记录、336 个 JSON 文件，约 30 MB，快照日期为 2025-11-20。它在 2026-09 关闭旧功能表面期间从本仓库移除，并保存在仓库外由 owner 管理的受限存储中。本仓库刻意不记录特定机器上的文件系统路径。

## ⚠️ 来源与许可证

原始语料没有随附许可证文件或来源元数据，内容为第三方游戏对话。在数据 owner 与法务完成许可证和来源审查之前，应将完整语料视为**受限/内部数据**。未经批准，不得将其发布到公开仓库或用于训练。

## 记录模式（由 sample.json 示意）

| 字段 | 含义 |
| --- | --- |
| `source_title` | 游戏内来源或任务名称 |
| `speaker` | 角色名称 |
| `text` | 对话内容 |
