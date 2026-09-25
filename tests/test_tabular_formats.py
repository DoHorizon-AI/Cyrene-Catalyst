"""
┌─────────────────────────────────────────────────────────────────────┐
│  test_tabular_formats.py                                             │
│  Module: tests.test_tabular_formats                                  │
│  Role: CSV/Parquet import, export, MIME, and schema-boundary proof.   │
└─────────────────────────────────────────────────────────────────────┘

中文：验证 CSV/Parquet 导入与导出、MIME 类型及 schema 边界。
"""
# 中文：文件：tests/test_tabular_formats.py；模块：tests.test_tabular_formats；职责：CSV/Parquet 导入、导出、MIME 类型与模式边界验证。

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
from fastapi.testclient import TestClient

from cyrene_catalyst import create_app
from cyrene_catalyst.artifacts import LocalArtifactPlane
from cyrene_catalyst.domain import ImportFormat, SplitStats
from cyrene_catalyst.engine import (
    EngineResult,
    PreparationOutput,
    PreparedSample,
    SourceInspection,
)


def _close(app: Any) -> None:
    app.state.catalyst_store.close()


def _client(tmp_path: Path, *, engine: Any | None = None) -> TestClient:
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
        engine=engine,
    )
    return TestClient(app)


def _dataset(client: TestClient) -> str:
    response = client.post("/api/v1/datasets", json={"name": "tabular-data"})
    assert response.status_code == 201
    return response.json()["id"]


def _upload(client: TestClient, dataset_id: str, filename: str, body: bytes) -> dict[str, Any]:
    response = client.post(
        f"/api/v1/datasets/{dataset_id}/preparations?name={filename}&filename={filename}",
        content=body,
        headers={
            "Content-Type": "text/csv" if filename.endswith(".csv") else "application/octet-stream"
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


def _publish(client: TestClient, preparation: dict[str, Any]) -> dict[str, Any]:
    mapped = client.patch(
        f"/api/v1/preparations/{preparation['id']}/mapping",
        json={
            "mapping": {
                "mode": "instruction",
                "instruction": {"field": "instruction"},
                "output": {"field": "output"},
            },
            "normalization": {},
        },
    )
    assert mapped.status_code == 200, mapped.text
    split = client.patch(
        f"/api/v1/preparations/{preparation['id']}/split",
        json={"split": {"trainRatio": 1.0}},
    )
    assert split.status_code == 200, split.text
    confirmed = client.post(f"/api/v1/preparations/{preparation['id']}/confirm")
    assert confirmed.status_code == 200, confirmed.text
    published = client.post(f"/api/v1/preparations/{preparation['id']}/publish")
    assert published.status_code == 201, published.text
    return published.json()


def test_csv_import_exports_tabular_files_with_correct_content_types(tmp_path: Path) -> None:
    client = _client(tmp_path)
    with client:
        preparation = _upload(
            client,
            _dataset(client),
            "training.csv",
            b"instruction,output\nquestion one,answer one\nquestion two,answer two\n",
        )
        assert preparation["format"] == ImportFormat.CSV.value
        assert preparation["detectedFields"] == ["instruction", "output"]

        published = _publish(client, preparation)
        exports = {item["name"]: item for item in published["preparation"]["exports"]}
        assert {
            "train.csv",
            "val.csv",
            "errors.csv",
            "train.parquet",
            "val.parquet",
            "errors.parquet",
        } <= set(exports)
        assert exports["train.csv"]["mediaType"] == "text/csv"
        assert exports["train.parquet"]["mediaType"] == "application/vnd.apache.parquet"

        csv_response = client.get(f"/api/v1/preparations/{preparation['id']}/exports/train.csv")
        assert csv_response.status_code == 200
        assert csv_response.headers["content-type"].startswith("text/csv")
        assert csv_response.text.splitlines()[0] == "instruction,output"

        parquet_response = client.get(
            f"/api/v1/preparations/{preparation['id']}/exports/train.parquet"
        )
        assert parquet_response.status_code == 200
        assert parquet_response.headers["content-type"] == "application/vnd.apache.parquet"
        parquet_path = tmp_path / "train.parquet"
        parquet_path.write_bytes(parquet_response.content)
        with duckdb.connect() as connection:
            relation = connection.read_parquet(str(parquet_path))
            assert relation.columns == ["instruction", "output"]
            assert relation.aggregate("count(*)").fetchone() == (2,)
    _close(client.app)


def test_parquet_import_is_inspected_and_prepared(tmp_path: Path) -> None:
    source_path = tmp_path / "source.parquet"
    with duckdb.connect() as connection:
        connection.execute(
            "CREATE TABLE source AS SELECT * FROM (VALUES "
            "('question one', 'answer one'), ('question two', 'answer two')"
            ") AS rows(instruction, output)"
        )
        connection.table("source").write_parquet(str(source_path))

    client = _client(tmp_path)
    with client:
        preparation = _upload(
            client,
            _dataset(client),
            "training.parquet",
            source_path.read_bytes(),
        )
        assert preparation["format"] == ImportFormat.PARQUET.value
        assert preparation["rowCount"] == 2
        published = _publish(client, preparation)
        assert published["datasetVersion"]["rowCount"] == 2
    _close(client.app)


def test_csv_artifact_version_transforms_to_parquet(tmp_path: Path) -> None:
    source_path = tmp_path / "source.csv"
    source_path.write_bytes(b"instruction,output\nquestion,answer\n")
    artifact_root = tmp_path / "artifacts"
    source = LocalArtifactPlane(artifact_root).publish(source_path, "dataset")

    client = _client(tmp_path)
    with client:
        dataset_id = _dataset(client)
        response = client.post(
            f"/api/v1/datasets/{dataset_id}/versions",
            json={
                "source": source.model_dump(by_alias=True, exclude_none=True),
                "engineBindingId": "local-duckdb",
            },
        )
        assert response.status_code == 201, response.text
        assert response.json()["rowCount"] == 1
        assert response.json()["schemaFields"] == ["instruction", "output"]
    _close(client.app)


def test_ragged_csv_columns_are_rejected_on_import(tmp_path: Path) -> None:
    client = _client(tmp_path)
    with client:
        dataset_id = _dataset(client)
        response = client.post(
            f"/api/v1/datasets/{dataset_id}/preparations?name=invalid.csv&filename=invalid.csv",
            content=b"instruction,output\nquestion,answer,unexpected\n",
            headers={"Content-Type": "text/csv"},
        )
        assert response.status_code == 422
        assert response.json()["code"] == "CATALYST_IMPORT_INVALID"
    _close(client.app)


def test_csv_media_type_selects_format_without_a_filename(tmp_path: Path) -> None:
    client = _client(tmp_path)
    with client:
        dataset_id = _dataset(client)
        response = client.post(
            f"/api/v1/datasets/{dataset_id}/preparations?name=content-typed-source",
            content=b"instruction,output\nquestion,answer\n",
            headers={"Content-Type": "text/csv"},
        )
        assert response.status_code == 201, response.text
        assert response.json()["format"] == ImportFormat.CSV.value
    _close(client.app)


class _UnknownColumnEngine:
    """Test-only engine projection with an invalid normalized sample schema.

    中文：构造带有无效规范化 sample schema 的 test-only engine projection。
    """
# 中文：仅供测试使用的引擎投影，其中包含无效的规范化样本模式。

    def inspect(self, source: Path, *, format_hint: ImportFormat | None = None) -> SourceInspection:
        del source, format_hint
        return SourceInspection(
            source_format=ImportFormat.JSONL,
            rows=[{"instruction": "question", "output": "answer"}],
            detected_fields=["instruction", "output"],
            row_count=1,
        )

    def prepare(
        self,
        source: Path,
        source_format: ImportFormat,
        mapping: Any,
        normalization: Any,
        split: Any = None,
        *,
        output_dir: Path | None = None,
    ) -> PreparationOutput:
        del source, source_format, mapping, normalization, split, output_dir
        return PreparationOutput(
            samples=[
                PreparedSample(
                    index=1,
                    group_key="row:1",
                    content={"instruction": "question", "output": "answer", "extra": "nope"},
                    source_row_indexes=[1],
                )
            ],
            errors=[],
            duplicates=[],
            assignment={1: "train"},
            split_stats=SplitStats(
                train_ratio=1.0,
                train_samples=1,
                val_samples=0,
                train_groups=1,
                val_groups=0,
            ),
            files={},
        )

    def transform(self, source: Path, destination: Path) -> EngineResult:
        del source, destination
        raise AssertionError("transform is not used by this test")


def test_unknown_export_column_is_rejected_strictly(tmp_path: Path) -> None:
    client = _client(tmp_path, engine=_UnknownColumnEngine())
    with client:
        preparation = _upload(client, _dataset(client), "source.jsonl", b"ignored\n")
        response = client.patch(
            f"/api/v1/preparations/{preparation['id']}/mapping",
            json={
                "mapping": {
                    "mode": "instruction",
                    "instruction": {"field": "instruction"},
                    "output": {"field": "output"},
                },
                "normalization": {},
            },
        )
        assert response.status_code == 422
        assert response.json()["code"] == "CATALYST_SCHEMA_UNKNOWN_COLUMN"
    _close(client.app)
