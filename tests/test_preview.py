"""
┌─────────────────────────────────────────────────────────────────────┐
│  test_preview.py                                                    │
│  Module: tests.test_preview                                         │
│  Role: Dataset version sample preview API test proof.               │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi.testclient import TestClient

from cyrene_catalyst import create_app


def _client(tmp_path: Path) -> TestClient:
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
    )
    return TestClient(app)


def _dataset(client: TestClient, name: str = "preview-dataset") -> str:
    response = client.post("/api/v1/datasets", json={"name": name})
    assert response.status_code == 201
    return response.json()["id"]


def _publish_csv(
    client: TestClient,
    dataset_id: str,
    filename: str,
    csv_bytes: bytes,
    mapping: dict[str, Any] | None = None,
) -> dict[str, Any]:
    prep_res = client.post(
        f"/api/v1/datasets/{dataset_id}/preparations?name={filename}&filename={filename}",
        content=csv_bytes,
        headers={"Content-Type": "text/csv"},
    )
    assert prep_res.status_code == 201, prep_res.text
    prep = prep_res.json()

    if mapping is None:
        mapping = {
            "mode": "instruction",
            "instruction": {"field": "instruction"},
            "output": {"field": "output"},
        }

    mapped = client.patch(
        f"/api/v1/preparations/{prep['id']}/mapping",
        json={"mapping": mapping, "normalization": {}},
    )
    assert mapped.status_code == 200, mapped.text

    split = client.patch(
        f"/api/v1/preparations/{prep['id']}/split",
        json={"split": {"trainRatio": 1.0}},
    )
    assert split.status_code == 200, split.text

    confirmed = client.post(f"/api/v1/preparations/{prep['id']}/confirm")
    assert confirmed.status_code == 200, confirmed.text

    published = client.post(f"/api/v1/preparations/{prep['id']}/publish")
    assert published.status_code == 201, published.text
    return published.json()["datasetVersion"]


def test_dataset_version_preview(tmp_path: Path) -> None:
    client = _client(tmp_path)
    with client:
        dataset_id = _dataset(client)

        csv_content = (
            b"instruction,output\n"
            b"inst 1,out 1\n"
            b"inst 2,out 2\n"
            b"inst 3,out 3\n"
            b"inst 4,out 4\n"
            b"inst 5,out 5\n"
        )
        version = _publish_csv(client, dataset_id, "sample.csv", csv_content)
        version_id = version["id"]

        # Case 1: Normal pagination
        res = client.get(f"/api/v1/dataset-versions/{version_id}/preview?limit=2&offset=0")
        assert res.status_code == 200, res.text
        data = res.json()
        assert data["versionId"] == version_id
        assert data["totalRows"] >= 5
        assert len(data["rows"]) == 2
        assert data["rows"][0]["index"] == 0
        assert data["rows"][0]["mapped"]["instruction"] == "inst 1"
        assert data["rows"][0]["mapped"]["output"] == "out 1"
        assert data["rows"][0]["raw"]["instruction"] == "inst 1"
        assert data["rows"][1]["index"] == 1

        res2 = client.get(f"/api/v1/dataset-versions/{version_id}/preview?limit=2&offset=2")
        assert res2.status_code == 200
        data2 = res2.json()
        assert len(data2["rows"]) == 2
        assert data2["rows"][0]["index"] == 2
        assert data2["rows"][0]["mapped"]["instruction"] == "inst 3"

        # Case 2: Out-of-range offset
        res_oob = client.get(f"/api/v1/dataset-versions/{version_id}/preview?limit=10&offset=500")
        assert res_oob.status_code == 200
        data_oob = res_oob.json()
        assert data_oob["totalRows"] >= 5
        assert len(data_oob["rows"]) == 0

        # Case 3: Limit bounds
        res_min = client.get(f"/api/v1/dataset-versions/{version_id}/preview?limit=1")
        assert res_min.status_code == 200
        assert len(res_min.json()["rows"]) == 1

        res_max = client.get(f"/api/v1/dataset-versions/{version_id}/preview?limit=100")
        assert res_max.status_code == 200

        res_zero = client.get(f"/api/v1/dataset-versions/{version_id}/preview?limit=0")
        assert res_zero.status_code == 422

        res_over = client.get(f"/api/v1/dataset-versions/{version_id}/preview?limit=101")
        assert res_over.status_code == 422

        res_neg = client.get(f"/api/v1/dataset-versions/{version_id}/preview?offset=-1")
        assert res_neg.status_code == 422

        # Case 4: Mapping field coverage (custom column names mapped to instruction/output/input)
        csv_custom = (
            b"col_prompt,col_response,col_context\n"
            b"custom prompt 1,custom response 1,custom context 1\n"
            b"custom prompt 2,custom response 2,custom context 2\n"
        )
        custom_mapping = {
            "mode": "instruction",
            "instruction": {"field": "col_prompt"},
            "output": {"field": "col_response"},
            "input": {"field": "col_context"},
        }
        custom_version = _publish_csv(
            client, dataset_id, "custom.csv", csv_custom, mapping=custom_mapping
        )
        res_custom = client.get(f"/api/v1/dataset-versions/{custom_version['id']}/preview?limit=10")
        assert res_custom.status_code == 200
        data_custom = res_custom.json()
        assert data_custom["totalRows"] >= 2
        assert len(data_custom["rows"]) >= 2
        first_row = data_custom["rows"][0]
        assert first_row["mapped"]["instruction"] == "custom prompt 1"
        assert first_row["mapped"]["output"] == "custom response 1"
        assert first_row["mapped"]["input"] == "custom context 1"


def test_preview_error_cases(tmp_path: Path) -> None:
    client = _client(tmp_path)
    with client:
        # Non-existent version -> 404
        missing_id = uuid4()
        res_404 = client.get(f"/api/v1/dataset-versions/{missing_id}/preview")
        assert res_404.status_code == 404
        assert res_404.json()["code"] == "CATALYST_VERSION_NOT_FOUND"

        # Missing output / unreadable -> 503
        dataset_id = _dataset(client)
        # Create version directly pointing to missing artifact
        fake_digest = "sha256:" + "0" * 64
        fake_uri = "artifact://sha256/" + "0" * 64
        client.post(
            f"/api/v1/datasets/{dataset_id}/versions",
            json={
                "source": {
                    "uri": fake_uri,
                    "digest": fake_digest,
                    "size_bytes": 100,
                    "kind": "dataset",
                },
                "engineBindingId": "local-duckdb",
            },
        )
        # Because the source file doesn't exist, create_version fails with 422
        # But we can test 503 by deleting the parquet file of a published version
        csv_content = b"instruction,output\nq,a\n"
        version = _publish_csv(client, dataset_id, "temp.csv", csv_content)
        v_id = version["id"]
        import shutil

        shutil.rmtree(tmp_path / "artifacts")
        res_503 = client.get(f"/api/v1/dataset-versions/{v_id}/preview")
        assert res_503.status_code == 503
        assert res_503.json()["code"] == "CATALYST_ARTIFACT_UNAVAILABLE"
