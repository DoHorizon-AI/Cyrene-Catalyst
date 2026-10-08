"""Accept an installed Catalyst service through its public Product API.

中文：通过公开 Product API 验收已安装的 Catalyst 服务。
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import ipaddress
import json
import os
import time
import zipfile
from contextlib import ExitStack
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

import httpx

from cyrene_catalyst_consumer.training_bundle import learned_messages, verify_bundle

_FIXTURE_PACKAGE = ("fixtures", "training-curation-v03")
_TOKEN_ENVIRONMENT = "CYRENE_DATA_TOOLS_CLIENT_TOKEN"
_EXPECTED_EXCLUSIONS = {
    "duplicate-alpaca",
    "duplicate-prompt",
    "missing-output",
    "wrong-role",
    "empty-answer",
    "overlong",
    "tool-calls",
    "multimodal",
    "incomplete-prompt",
    "wrong-type",
    "role-order",
    "mid-system",
    "policy-prohibited",
}


class AcceptanceFailure(RuntimeError):
    """Raised when the installed Product API fails an acceptance assertion."""


def _fixture_file(filename: str) -> bytes:
    """Read one release-packaged fixture file. | 读取随正式发行包交付的夹具文件。"""

    resource = resources.files("cyrene_catalyst")
    for part in _FIXTURE_PACKAGE:
        resource = resource.joinpath(part)
    return resource.joinpath(filename).read_bytes()


def _request(
    client: httpx.Client,
    method: str,
    path: str,
    *,
    expected: int = 200,
    json_body: dict[str, Any] | None = None,
    files: list[tuple[str, tuple[str, bytes, str]]] | None = None,
    params: dict[str, str | int] | None = None,
    headers: dict[str, str] | None = None,
) -> Any:
    """Require an expected public API response. | 校验公开 API 返回状态。"""

    response = client.request(
        method,
        path,
        json=json_body,
        files=files,
        params=params,
        headers=headers,
    )
    if response.status_code != expected:
        detail = response.text[:1000]
        raise AcceptanceFailure(f"{method} {path}: {response.status_code}: {detail}")
    if response.headers.get("content-type", "").startswith("application/json"):
        return response.json()
    return response


def _load_fixture() -> tuple[dict[str, Any], list[tuple[str, tuple[str, bytes, str]]]]:
    """Verify packaged source bytes before upload. | 上传前核验夹具原件摘要。"""

    manifest = json.loads(_fixture_file("fixture-manifest.json"))
    uploads = []
    for entry in manifest["files"]:
        filename = entry["filename"]
        content = _fixture_file(filename)
        if hashlib.sha256(content).hexdigest() != entry["sha256"]:
            raise AcceptanceFailure(f"Release fixture digest changed: {filename}")
        media_type = "application/json" if filename.endswith(".json") else "application/x-ndjson"
        uploads.append(("files[]", (filename, content, media_type)))
    return manifest, uploads


def _latest(client: httpx.Client, dataset_id: str) -> dict[str, Any]:
    """Read the current immutable content revision. | 读取当前不可变内容修订。"""

    revisions = cast(
        list[dict[str, Any]],
        _request(client, "GET", f"/api/v1/datasets/{dataset_id}/content-revisions"),
    )
    if not revisions:
        raise AcceptanceFailure("Curation did not produce a content revision.")
    return revisions[0]


def _records(client: httpx.Client, revision: dict[str, Any]) -> list[dict[str, Any]]:
    """Read every record in the fixed small fixture. | 读取固定小型夹具中的全部记录。"""

    page = cast(
        dict[str, Any],
        _request(
            client,
            "GET",
            f"/api/v1/content-revisions/{revision['id']}/training-records",
            params={"limit": 200},
        ),
    )
    if page["total"] != 28 or len(page["records"]) != 28:
        raise AcceptanceFailure(
            "The fixture records, including malformed and blank lines, must reconcile."
        )
    return cast(list[dict[str, Any]], page["records"])


def _wait_for_run(client: httpx.Client, run_id: str, timeout: float = 180.0) -> dict[str, Any]:
    """Poll the Product run resource to a terminal state. | 轮询 Product 任务直到终态。"""

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        run = cast(dict[str, Any], _request(client, "GET", f"/api/v1/processing-runs/{run_id}"))
        if run["state"] == "SUCCEEDED":
            return run
        if run["state"] in {"FAILED", "CANCELLED"}:
            raise AcceptanceFailure(f"Product run ended in {run['state']}: {run.get('failure')}")
        time.sleep(1)
    raise AcceptanceFailure(f"Product run {run_id} did not finish within {timeout:.0f} seconds.")


def _capability_preflight(client: httpx.Client) -> dict[str, Any]:
    """Require preparation and generation before service mutation.

    中文：在修改服务前要求 preparation 与 generation 均已配置。
    """

    report = cast(dict[str, Any], _request(client, "GET", "/api/v1/system/capabilities"))
    if (
        report.get("semantics") != "configuration-only"
        or report.get("activationVerified") is not False
    ):
        raise AcceptanceFailure(
            "The capability endpoint must explicitly report configuration only."
        )
    capabilities = report.get("capabilities")
    if not isinstance(capabilities, list):
        raise AcceptanceFailure("The capability endpoint did not return a capability list.")
    for capability_id in ("dataset.preparation.v1", "dataset.generation.v1"):
        capability = next(
            (
                item
                for item in capabilities
                if isinstance(item, dict) and item.get("id") == capability_id
            ),
            None,
        )
        if capability is None or capability.get("supported") is not True:
            raise AcceptanceFailure(
                f"The installed Product does not declare {capability_id} support."
            )
        if capability.get("configured") is not True:
            environment = capability.get("configurationEnvironmentVariable", "connection_ref")
            raise AcceptanceFailure(f"The installer did not configure {environment}.")
    return report


def _review_records(
    client: httpx.Client,
    revision: dict[str, Any],
    records: list[dict[str, Any]],
    fixture: dict[str, Any],
) -> dict[str, Any]:
    """Apply the published authored-example review decisions. | 应用已发布的示例审核决定。"""

    edits = []
    excluded = set(fixture["expectedExcludedSamples"])
    for record in records:
        sample_id = record.get("sampleId")
        if sample_id in {"needs-mapping", "mapped-source"}:
            mapping = {"instruction": "task", "output": "answer"}
            if sample_id == "mapped-source":
                mapping["input"] = "context"
            edits.append(
                {
                    "recordId": record["id"],
                    "action": "remap",
                    "format": "alpaca",
                    "fieldMapping": mapping,
                    "note": "Acceptance reviewer verified the authored legacy field mapping.",
                }
            )
        elif sample_id in excluded or not isinstance(record.get("rawRecord"), dict):
            edits.append(
                {
                    "recordId": record["id"],
                    "action": "exclude",
                    "note": "Acceptance reviewer excluded an invalid or prohibited SFT row.",
                }
            )
        else:
            edits.append(
                {
                    "recordId": record["id"],
                    "action": "approve",
                    "note": "Acceptance reviewer checked this authored example.",
                }
            )
    return cast(
        dict[str, Any],
        _request(
            client,
            "POST",
            f"/api/v1/content-revisions/{revision['id']}/training-records:edit",
            expected=201,
            json_body={"resourceVersion": revision["resourceVersion"], "edits": edits},
        ),
    )


def _reconcile_export(
    bundle_path: Path,
    records: list[dict[str, Any]],
    output_format: str,
) -> int:
    """Compare every exported learned row to approved messages. | 逐条对照导出样本与已批准消息。"""

    expected = {
        record["sampleId"]: record["normalized"]["messages"]
        for record in records
        if record["disposition"] == "eligible"
    }
    observed: set[str] = set()
    with zipfile.ZipFile(bundle_path) as bundle, ExitStack() as stack:
        split_rows = {
            split: stack.enter_context(bundle.open(f"{split}.jsonl"))
            for split in ("train", "validation", "test")
        }
        with bundle.open("provenance.jsonl") as receipts:
            for line in receipts:
                receipt = json.loads(line)
                sample_id = receipt["sample_id"]
                if sample_id in observed or sample_id not in expected:
                    raise AcceptanceFailure(
                        "Export provenance contains an unexpected sample identity."
                    )
                learned_line = split_rows[receipt["split"]].readline()
                if not learned_line:
                    raise AcceptanceFailure("Export provenance points past a learned split.")
                learned = json.loads(learned_line)
                if learned_messages(learned, output_format) != expected[sample_id]:
                    raise AcceptanceFailure("The export changed an approved conversation turn.")
                observed.add(sample_id)
        for stream in split_rows.values():
            if stream.readline():
                raise AcceptanceFailure("A learned row has no matching provenance receipt.")
    if observed != set(expected):
        raise AcceptanceFailure(
            "Approved sample identities do not reconcile with package provenance."
        )
    return len(observed)


def run_acceptance(
    base_url: str, output_directory: Path, token: str | None = None
) -> dict[str, Any]:
    """Exercise installed Product APIs from fixture upload through SFT export.

    中文：从夹具上传到 SFT 导出，端到端调用已安装服务的 Product API。
    """

    parsed = urlsplit(base_url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or parsed.username
        or parsed.password
    ):
        raise ValueError("--base-url must be an HTTP(S) origin without a path.")
    if parsed.scheme == "http":
        hostname = parsed.hostname.lower()
        local = hostname == "localhost"
        with contextlib.suppress(ValueError):
            local = local or ipaddress.ip_address(hostname).is_loopback
        if not local:
            raise ValueError("Non-loopback acceptance targets require HTTPS.")

    fixture, uploads = _load_fixture()
    output_directory.mkdir(parents=True, exist_ok=True)
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    with httpx.Client(base_url=base_url.rstrip("/"), headers=headers, timeout=75) as client:
        capabilities = _capability_preflight(client)
        dataset = _request(
            client,
            "POST",
            "/api/v1/datasets",
            expected=201,
            json_body={
                "name": "Catalyst authored business acceptance "
                + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            },
        )
        dataset_id = dataset["id"]
        source_path = f"/api/v1/datasets/{dataset_id}/sources/batch"
        sources_response = _request(client, "POST", source_path, files=uploads)
        source_items = sources_response["items"]
        if any(item.get("error") for item in source_items):
            raise AcceptanceFailure("Every packaged fixture source must be ingested.")
        source_ids = [item["source"]["id"] for item in source_items]

        run_response = _request(
            client,
            "POST",
            f"/api/v1/datasets/{dataset_id}/processing-runs",
            expected=202,
            json_body={
                "operation": "curateTrainingData",
                "sourceRevisionIds": source_ids,
                "config": {
                    "curation": fixture["recipe"],
                    "sourcePolicies": {
                        source_id: {
                            "allowTraining": True,
                            "allowedUsePurposes": ["model_training"],
                        }
                        for source_id in source_ids
                    },
                },
            },
        )
        curation_run = _wait_for_run(client, run_response["id"])
        original = _latest(client, dataset_id)
        original_records = _records(client, original)
        before_counts = original["trainingDataSnapshot"]["counts"]
        if before_counts["total"] != fixture["totalRecords"]:
            raise AcceptanceFailure("Imported record counters must include all 28 authored rows.")
        queue = _request(client, "GET", f"/api/v1/datasets/{dataset_id}/review-queue")
        if not queue["items"]:
            raise AcceptanceFailure("The mixed authored fixture must produce Review Queue issues.")

        blocked = client.post(
            f"/api/v1/content-revisions/{original['id']}/review",
            json={"decision": "APPROVE"},
        )
        if blocked.status_code != 409:
            raise AcceptanceFailure("Unreviewed records must block content approval.")

        reviewed = _review_records(client, original, original_records, fixture)
        after_counts = reviewed["trainingDataSnapshot"]["counts"]
        if after_counts["eligible"] != fixture["expectedPublishedRecords"]:
            raise AcceptanceFailure(f"Reviewed record counts did not reconcile: {after_counts}")
        if (
            after_counts["pendingReview"]
            or after_counts["eligible"] + after_counts["excluded"] != 28
        ):
            raise AcceptanceFailure("Every authored row must have a final explicit disposition.")

        queue = _request(
            client,
            "GET",
            f"/api/v1/datasets/{dataset_id}/review-queue",
            params={"limit": 200},
        )
        for item in queue["items"]:
            if item["state"] == "OPEN":
                _request(
                    client,
                    "POST",
                    f"/api/v1/review-items/{item['id']}/resolve",
                    json_body={
                        "action": "ACKNOWLEDGE",
                        "note": "Acceptance reviewer verified the explicit sample disposition.",
                    },
                )
        approved = _request(
            client,
            "POST",
            f"/api/v1/content-revisions/{reviewed['id']}/review",
            json_body={
                "decision": "APPROVE",
                "note": "Authored business acceptance fixture reviewed.",
            },
        )
        approved_records = _records(client, approved)

        export_run_response = _request(
            client,
            "POST",
            f"/api/v1/datasets/{dataset_id}/processing-runs",
            expected=202,
            json_body={
                "operation": "prepareSft",
                "contentRevisionId": approved["id"],
                "config": {
                    "outputFormat": "sft",
                    "split": {"train": 0.8, "validation": 0.1, "test": 0.1},
                },
            },
        )
        export_run = _wait_for_run(client, export_run_response["id"])
        publish_body = {"contentRevisionId": approved["id"], "sftRunId": export_run["id"]}
        version = _request(
            client,
            "POST",
            f"/api/v1/datasets/{dataset_id}/data-tools/versions",
            expected=201,
            json_body=publish_body,
            headers={"Idempotency-Key": "authored-business-acceptance-sft"},
        )
        repeated_version = _request(
            client,
            "POST",
            f"/api/v1/datasets/{dataset_id}/data-tools/versions",
            expected=201,
            json_body=publish_body,
            headers={"Idempotency-Key": "authored-business-acceptance-sft"},
        )
        if repeated_version["id"] != version["id"]:
            raise AcceptanceFailure("Retrying publication must return the same immutable version.")

        exported = _request(
            client,
            "GET",
            f"/api/v1/dataset-versions/{version['id']}/data-tools/export",
            params={"profile": "sft"},
        )
        bundle_path = output_directory / "authored-business-sft.zip"
        bundle_path.write_bytes(exported.content)
        independent_result = verify_bundle(bundle_path, fixture["metadataSentinel"])
        reconciled_records = _reconcile_export(bundle_path, approved_records, "sft")
        if (
            independent_result["published"] != fixture["expectedPublishedRecords"]
            or reconciled_records != fixture["expectedPublishedRecords"]
            or independent_result["maxMessageCount"] < 5
        ):
            raise AcceptanceFailure("The independent SFT consumer found an incomplete package.")

    report = {
        "status": "PASS",
        "target": base_url.rstrip("/"),
        "fixture": "release-packaged authored business examples; no customer data",
        "fixtureVersion": fixture["fixtureVersion"],
        "fixtureProvenance": fixture["provenance"],
        "fixtureLicense": fixture["license"],
        "datasetId": dataset_id,
        "totalRecords": fixture["totalRecords"],
        "beforeReview": before_counts,
        "afterReview": after_counts,
        "capabilityConfiguration": capabilities,
        "connectionActivationProvenBy": "successful public API curation and SFT export requests",
        "curationRunId": curation_run["id"],
        "sftRunId": export_run["id"],
        "datasetVersionId": version["id"],
        "publicationRetryImmutable": True,
        "independentConsumer": independent_result,
        "approvedRowsReconciled": reconciled_records,
        "bundle": str(bundle_path.resolve()),
    }
    report_path = output_directory / "acceptance-report.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return report


def main() -> None:
    """Run the installed-service acceptance command. | 运行已安装服务验收命令。"""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True, help="Installed Catalyst Product API origin")
    parser.add_argument("--output-directory", required=True, type=Path)
    arguments = parser.parse_args()
    report = run_acceptance(
        arguments.base_url,
        arguments.output_directory.resolve(),
        token=os.environ.get(_TOKEN_ENVIRONMENT),
    )
    print(
        json.dumps(
            {
                "status": report["status"],
                "datasetId": report["datasetId"],
                "approvedRows": report["approvedRowsReconciled"],
                "bundle": report["bundle"],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
