"""Exercise bounded, recoverable JSONL curation at a configurable record count.

中文：通过真实产品/插件契约执行中等规模 JSONL 数据、错误行和重复记录验收。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import resource
from pathlib import Path
from typing import Any

from accept_training_curation import _close, _json, _latest, _open, _wait, owner_endpoints
from fastapi.testclient import TestClient
from verify_training_bundle import verify_bundle


def run_medium(output_directory: Path, record_count: int) -> dict[str, Any]:
    """Stream a scale/fault workload through the actual workflow. | 执行规模与故障验收。"""

    output_directory.mkdir(parents=True, exist_ok=True)
    source_path = output_directory / "scale-input.jsonl"
    duplicates = 0
    malformed = 0
    wrong_roles = 0
    with source_path.open("w", encoding="utf-8") as stream:
        for index in range(record_count):
            identity = hashlib.sha256(str(index).encode()).hexdigest()
            row = {
                "sampleId": f"scale-{index}",
                "sourceFamilyId": f"family-{index % 100:03}",
                "instruction": f"Question {identity}",
                "output": f"Answer {identity[::-1]}",
                "_reviewNote": "SCALE_OPERATOR_ONLY_DO_NOT_TRAIN",
            }
            stream.write(json.dumps(row) + "\n")
            if index % 100 == 0:
                stream.write(json.dumps({**row, "sampleId": f"duplicate-{index}"}) + "\n")
                duplicates += 1
            if index % 1000 == 0:
                stream.write('{"instruction":"broken record",\n')
                malformed += 1
                stream.write(
                    json.dumps({"messages": [{"role": "alien", "content": "Review required"}]})
                    + "\n"
                )
                wrong_roles += 1
    total = record_count + duplicates + malformed + wrong_roles
    with owner_endpoints():
        app = _open(output_directory)
        try:
            with TestClient(app) as client:
                dataset = _json(
                    client, "POST", "/api/v1/datasets", 201, json={"name": "Scale trial"}
                )
                dataset_id = dataset["id"]
                source = _json(
                    client,
                    "POST",
                    f"/api/v1/datasets/{dataset_id}/sources?filename=scale-input.jsonl",
                    201,
                    content=source_path.read_bytes(),
                    headers={"content-type": "application/x-ndjson"},
                )
                run = _wait(
                    app,
                    _json(
                        client,
                        "POST",
                        f"/api/v1/datasets/{dataset_id}/processing-runs",
                        202,
                        json={
                            "operation": "curateTrainingData",
                            "sourceRevisionIds": [source["id"]],
                            "config": {
                                "curation": {
                                    "id": "synthetic-scale-v03",
                                    "version": "1",
                                    "format": "auto",
                                    "maxCharacters": 2000,
                                    "minCharacters": 2,
                                },
                                "sourcePolicies": {
                                    source["id"]: {
                                        "allowTraining": True,
                                        "allowedUsePurposes": ["model_training"],
                                    }
                                },
                            },
                        },
                    ),
                )
                revision = _latest(client, dataset_id)
                before = revision["trainingDataSnapshot"]["counts"]
                if before["total"] != total or before["eligible"] != record_count:
                    raise AssertionError(f"Scale curation does not reconcile: {before}")
                records = _json(
                    client,
                    "GET",
                    f"/api/v1/content-revisions/{revision['id']}/training-records",
                    params={"disposition": "review", "limit": 200},
                )
                if records["total"] > 200:
                    raise AssertionError("Synthetic faults unexpectedly require unbounded review.")
                revision = _json(
                    client,
                    "POST",
                    f"/api/v1/content-revisions/{revision['id']}/training-records:edit",
                    expected=201,
                    json={
                        "resourceVersion": revision["resourceVersion"],
                        "edits": [
                            {"recordId": row["id"], "action": "exclude"}
                            for row in records["records"]
                        ],
                    },
                )
                queue = _json(
                    client,
                    "GET",
                    f"/api/v1/datasets/{dataset_id}/review-queue",
                    params={"limit": 200},
                )
                for item in queue["items"]:
                    if item["state"] == "OPEN":
                        _json(
                            client,
                            "POST",
                            f"/api/v1/review-items/{item['id']}/resolve",
                            json={"action": "ACKNOWLEDGE"},
                        )
                revision = _json(
                    client,
                    "POST",
                    f"/api/v1/content-revisions/{revision['id']}/review",
                    json={"decision": "APPROVE"},
                )
                export = _wait(
                    app,
                    _json(
                        client,
                        "POST",
                        f"/api/v1/datasets/{dataset_id}/processing-runs",
                        202,
                        json={
                            "operation": "prepareSft",
                            "contentRevisionId": revision["id"],
                            "config": {"outputFormat": "sft"},
                        },
                    ),
                )
                version_record = _json(
                    client,
                    "POST",
                    f"/api/v1/datasets/{dataset_id}/data-tools/versions",
                    201,
                    json={"contentRevisionId": revision["id"], "sftRunId": export["id"]},
                )
                response = client.get(
                    f"/api/v1/dataset-versions/{version_record['id']}/data-tools/export",
                    params={"profile": "sft"},
                )
                if response.status_code != 200:
                    raise AssertionError(response.text)
                bundle_path = output_directory / "scale-sft.zip"
                bundle_path.write_bytes(response.content)
                verified = verify_bundle(bundle_path, "SCALE_OPERATOR_ONLY_DO_NOT_TRAIN")
                if verified["published"] != record_count:
                    raise AssertionError("Only eligible rows must enter the final scale package.")
        finally:
            _close(app)
    report = {
        "status": "PASS",
        "workload": "synthetic scale and fault workload; not customer data",
        "inputBytes": source_path.stat().st_size,
        "inputRecords": total,
        "expectedValid": record_count,
        "beforeReview": before,
        "afterReview": revision["trainingDataSnapshot"]["counts"],
        "independentConsumer": verified,
        "maxResidentMiB": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
        "processingRunId": run["id"],
    }
    (output_directory / "scale-report.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    return report


def main() -> None:
    """Run the optional medium-scale acceptance workload. | 中等规模验收入口。"""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-directory", required=True, type=Path)
    parser.add_argument("--records", type=int, default=20_000)
    arguments = parser.parse_args()
    if not 1 <= arguments.records <= 50_000:
        parser.error("records must be between 1 and 50000")
    print(json.dumps(run_medium(arguments.output_directory.resolve(), arguments.records)))


if __name__ == "__main__":
    main()
