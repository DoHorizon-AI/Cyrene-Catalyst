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
