"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 test_preparation.py                                             │
│  Module: tests.test_preparation                                     │
│  Role: Reproducible conversion, split, publish, and export proof.   │
│                                                                     │
│  模块职责：可复现转换、划分、发布与导出可消费性验证。                       │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
from fastapi.testclient import TestClient

from cyrene_catalyst import create_app


def _close(app: Any) -> None:
    app.state.catalyst_store.close()


def _client(tmp_path: Path) -> TestClient:
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
    )
    return TestClient(app)


# ── End-to-end publish and Yield-compatible export proof ───────────────
# 中文：端到端发布和 Yield 兼容导出的验证。


def _make_dataset(client: TestClient, name: str = "prep-dataset") -> str:
    response = client.post("/api/v1/datasets", json={"name": name})
    assert response.status_code == 201
    return response.json()["id"]


def _upload(client: TestClient, dataset_id: str, name: str, body: bytes) -> dict[str, Any]:
    response = client.post(
        f"/api/v1/datasets/{dataset_id}/preparations?name={name}&filename={name}",
        content=body,
        headers={"Content-Type": "application/octet-stream"},
    )
    assert response.status_code == 201, response.text
    return response.json()


def test_end_to_end_publish_and_consumable_export(tmp_path: Path) -> None:
    client = _client(tmp_path)
    with client:
        dataset_id = _make_dataset(client)
        source = (
            (
                b'{"speaker":"\xe4\xb8\x83\xe4\xb8\x83","text":"\xe6\xac\xa2\xe8\xbf\x8e","scene":"s1"}\n'
                b'{"speaker":"\xe4\xb8\x83\xe4\xb8\x83","text":"\xe5\x86\x8d\xe8\xa7\x81","scene":"s1"}\n'
                b'{"speaker":"\xe6\x97\x85\xe8\xa1\x8c\xe8\x80\x85","text":"\xe5\x97\xaf","scene":"s2"}\n'
                b'{"speaker":"\xe6\x97\x85\xe8\xa1\x8c\xe8\x80\x85","text":"   ","scene":"s2"}\n'
            )
            .decode("utf-8")
            .encode("utf-8")
        )
        prep = _upload(client, dataset_id, "demo.jsonl", source)
        assert prep["format"] == "JSONL"
        assert prep["rowCount"] == 4
        assert "speaker" in prep["detectedFields"]

        mapping = {
            "mode": "instruction",
            "instruction": {"field": "speaker"},
            "output": {"field": "text"},
            "groupBy": "scene",
        }
        normalization = {
            "trimWhitespace": True,
            "collapseWhitespace": True,
            "unicodeNfc": True,
        }
        response = client.patch(
            f"/api/v1/preparations/{prep['id']}/mapping",
            json={"mapping": mapping, "normalization": normalization},
        )
        assert response.status_code == 200, response.text
        prep = response.json()
        assert prep["state"] == "MAPPED"
        assert prep["report"]["errorSamples"] == 1
        assert prep["report"]["validSamples"] == 3
        assert prep["report"]["uniqueSamples"] == 3

        split = client.patch(
            f"/api/v1/preparations/{prep['id']}/split",
            json={"split": {"trainRatio": 0.5}},
        ).json()
        assert split["state"] == "SPLIT"
        assert split["splitStats"]["trainSamples"] + split["splitStats"]["valSamples"] == 3

        confirm = client.post(f"/api/v1/preparations/{prep['id']}/confirm").json()
        assert confirm["state"] == "CONFIRMED"

        publish = client.post(
            f"/api/v1/preparations/{prep['id']}/publish",
            headers={"Idempotency-Key": "demo-publish"},
        )
        assert publish.status_code == 201, publish.text
        published = publish.json()
        version = published["datasetVersion"]
        assert version["state"] == "PUBLISHED"
        assert version["engineBindingId"] == "catalyst-prep-v1"
        assert version["rowCount"] == 3
        assert version["schemaFields"] == ["instruction", "output"]
        assert len(version["lineage"]) >= 4  # source -> train/val/errors/manifest | 中文：源数据到训练、验证、错误记录与清单的血缘项

        exports = {
            e["name"]: e for e in client.get(f"/api/v1/preparations/{prep['id']}/exports").json()
        }
        assert {"train.jsonl", "val.jsonl", "errors.jsonl", "manifest.json"} <= set(exports)

        # Idempotent replay with the same key returns the same version and digest.
        # 中文：使用相同密钥幂等重放时，返回相同版本和摘要。
        replay = client.post(
            f"/api/v1/preparations/{prep['id']}/publish",
            headers={"Idempotency-Key": "demo-publish"},
        )
        assert replay.status_code == 201
        assert replay.json()["datasetVersion"]["id"] == version["id"]
        assert replay.json()["datasetVersion"]["output"]["digest"] == version["output"]["digest"]

        # Real read proof: DuckDB consumes the exported training JSONL,
        # and the rows satisfy the instruction-mode invariants carried by
        # the Yield-owned contract (instruction/output non-empty strings).
        # 中文：真实读取验证：DuckDB 读取导出的训练 JSONL，并确认记录满足 Yield 契约中的 instruction 模式约束（instruction/output 均为非空字符串）。
        train_bytes = client.get(f"/api/v1/preparations/{prep['id']}/exports/train.jsonl").content
        train_path = tmp_path / "train.jsonl"
        train_path.write_bytes(train_bytes)
        with duckdb.connect() as con:
            relation = con.read_json(str(train_path), format="newline_delimited")
            columns = relation.columns
            rows = relation.fetchall()
        assert "instruction" in columns and "output" in columns
        for row in rows:
            instruction = row[columns.index("instruction")]
            output = row[columns.index("output")]
            assert isinstance(instruction, str) and instruction.strip()
            assert isinstance(output, str) and output.strip()

        # Error samples are surfaced with reasons, never silently dropped.
        # 中文：错误样本会附带原因显式呈现，绝不会被静默丢弃。
        errors_text = client.get(f"/api/v1/preparations/{prep['id']}/exports/errors.jsonl").text
        error_rows = [json.loads(line) for line in errors_text.splitlines() if line]
        assert error_rows
        assert all(r["reasonCode"] == "EMPTY_FIELD" for r in error_rows)

        # Manifest preserves source, configs, and per-sample lineage.
        # 中文：Manifest 会保留来源、配置和每个样本的沿袭信息。
        manifest = json.loads(
            client.get(f"/api/v1/preparations/{prep['id']}/exports/manifest.json").text
        )
        assert manifest["source"]["digest"] == prep["source"]["digest"]
        assert manifest["mapping"]["mode"] == "instruction"
        assert manifest["split"]["trainRatio"] == 0.5
        assert manifest["sampleLineage"]
        assert manifest["files"]["train"]["digest"] == exports["train.jsonl"]["artifact"]["digest"]

        # Yield draft honestly reports NOT_CONNECTED.
        # 中文：Yield 草稿如实报告 NOT_CONNECTED。
        yield_draft = client.post(f"/api/v1/preparations/{prep['id']}/yield-draft")
        assert yield_draft.status_code == 503
        assert yield_draft.json()["code"] == "CATALYST_YIELD_NOT_CONNECTED"

    _close(client.app)


def test_state_machine_rejects_invalid_transitions(tmp_path: Path) -> None:
    client = _client(tmp_path)
    with client:
        dataset_id = _make_dataset(client)
        prep = _upload(client, dataset_id, "s.jsonl", b'{"q":"a","a":"b"}\n')
        # confirm before mapping/split -> 409
        # 中文：在 mapping/split 之前执行 confirm -> 409。
        assert client.post(f"/api/v1/preparations/{prep['id']}/confirm").status_code == 409
        # split before mapping -> 409
        # 中文：在 mapping 之前执行 split -> 409。
        assert (
            client.patch(
                f"/api/v1/preparations/{prep['id']}/split",
                json={"split": {"trainRatio": 0.9}},
            ).status_code
            == 409
        )
        # mapping referencing unknown field -> 422
        # 中文：mapping 引用了未知字段 -> 422。
        bad = client.patch(
            f"/api/v1/preparations/{prep['id']}/mapping",
            json={
                "mapping": {
                    "mode": "instruction",
                    "instruction": {"field": "nope"},
                    "output": {"field": "a"},
                },
                "normalization": {
                    "trimWhitespace": True,
                    "collapseWhitespace": True,
                    "unicodeNfc": True,
                },
            },
        )
        assert bad.status_code == 422
        assert bad.json()["code"] == "CATALYST_MAPPING_FIELD_UNKNOWN"
    _close(client.app)


def test_text_import_and_literal_instruction(tmp_path: Path) -> None:
    client = _client(tmp_path)
    with client:
        dataset_id = _make_dataset(client)
        prep = _upload(client, dataset_id, "plain.txt", b"hello\nworld\n\n")
        assert prep["format"] == "TEXT"
        assert prep["rowCount"] == 2
        raw = client.get(f"/api/v1/preparations/{prep['id']}/samples?stage=raw")
        assert raw.status_code == 200, raw.text
        assert raw.json()["total"] == 2
        mapped = client.patch(
            f"/api/v1/preparations/{prep['id']}/mapping",
            json={
                "mapping": {
                    "mode": "instruction",
                    "instruction": {"literal": "Respond conversationally."},
                    "output": {"field": "text"},
                },
                "normalization": {
                    "trimWhitespace": True,
                    "collapseWhitespace": True,
                    "unicodeNfc": True,
                },
            },
        ).json()
        assert mapped["report"]["uniqueSamples"] == 2
    _close(client.app)


def test_json_raw_preview_preserves_json_import_format(tmp_path: Path) -> None:
    client = _client(tmp_path)
    with client:
        dataset_id = _make_dataset(client)
        prep = _upload(client, dataset_id, "records.json", b'[{"text":"hello"},{"text":"world"}]')
        assert prep["format"] == "JSON"
        raw = client.get(f"/api/v1/preparations/{prep['id']}/samples?stage=raw")
        assert raw.status_code == 200, raw.text
        assert raw.json()["total"] == 2
    _close(client.app)
