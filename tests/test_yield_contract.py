"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 test_yield_contract.py                                          │
│  Module: tests.test_yield_contract                                   │
│  Role: Yield-owned contract fixture and consumer handoff proof.      │
│                                                                     │
│  模块职责：校验 Yield-owned 契约快照，并以此验证训练草稿交接的消费者契约。    │
└─────────────────────────────────────────────────────────────────────┘

The vendored fixture under ``contracts/vendor/yield-product-v1`` is a read-only
copy of the Yield-owned contract artifact. Ordinary tests never read the sibling
Cyrene-Yield repository.
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator, FormatChecker
from openapi_spec_validator import validate as validate_openapi_spec
from openapi_spec_validator.readers import read_from_filename
from referencing import Registry, Resource

from cyrene_catalyst import create_app

CONTRACT_ROOT = Path(__file__).parents[1] / "contracts/vendor/yield-product-v1"
PROVENANCE_PATH = CONTRACT_ROOT / "PROVENANCE.md"
DIGEST_ROW = re.compile(
    r"^\|\s*`(?P<path>[^`]+)`\s*\|\s*`[^`]+`\s*\|\s*`(?P<digest>[0-9a-f]{64})`\s*\|"
)
VENDORED_FILES = {
    "openapi.yaml",
    "model-version.schema.json",
    "generated/platform/artifact-ref.schema.json",
    "generated/common/problem-details.schema.json",
}


def _provenance_digests() -> dict[str, str]:
    """Parse the digest table recorded in PROVENANCE.md. | 解析溯源摘要表。"""

    digests: dict[str, str] = {}
    for line in PROVENANCE_PATH.read_text(encoding="utf-8").splitlines():
        match = DIGEST_ROW.match(line)
        if match:
            digests[match.group("path")] = match.group("digest")
    return digests


def _contract_registry() -> tuple[dict[str, Any], str, Registry]:
    """Load the vendored Yield OpenAPI document and its reference registry. | 载入契约快照。"""

    spec, base = read_from_filename(str(CONTRACT_ROOT / "openapi.yaml"))
    artifact_path = CONTRACT_ROOT / "generated/platform/artifact-ref.schema.json"
    artifact_ref = json.loads(artifact_path.read_text(encoding="utf-8"))
    registry = (
        Registry()
        .with_resource(base, Resource.opaque(spec))
        .with_resource(artifact_path.as_uri(), Resource.from_contents(artifact_ref))
    )
    return spec, base, registry


def _schema_validator(schema_name: str) -> Draft202012Validator:
    """Build a validator anchored at one owner-published component schema. | 构建校验器。"""

    _, base, registry = _contract_registry()
    return Draft202012Validator(
        {"$ref": f"{base}#/components/schemas/{schema_name}"},
        registry=registry,
        format_checker=FormatChecker(),
    )


class _YieldStub:
    """Contract-shaped Yield double; tests only, never real runtime evidence. | Yield 测试替身。"""

    def __init__(
        self,
        *,
        response_status: int = 201,
        response_state: str = "DRAFT",
        resource_ref_mismatch: bool = False,
    ) -> None:
        self.response_status = response_status
        self.response_state = response_state
        self.resource_ref_mismatch = resource_ref_mismatch
        self.requests: list[dict[str, Any]] = []
        self.draft_response: dict[str, Any] | None = None
        self._draft_id: str | None = None
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length) or b"{}")
                outer.requests.append(
                    {
                        "method": "POST",
                        "path": self.path,
                        "idempotency_key": self.headers.get("Idempotency-Key"),
                        "body": body,
                    }
                )
                if outer.response_status not in {200, 201}:
                    self._respond(
                        outer.response_status,
                        {
                            "type": "https://errors.cyrene.dev/yield/rejected",
                            "title": "Rejected",
                            "status": outer.response_status,
                            "code": "YIELD_STUB_REJECTED",
                        },
                    )
                    return
                draft_id = outer._draft_id or str(uuid4())
                outer._draft_id = draft_id
                reference_id = str(uuid4()) if outer.resource_ref_mismatch else draft_id
                draft = {
                    "id": draft_id,
                    "resourceRef": {
                        "uri": f"cyrene://yield/training-drafts/{reference_id}",
                        "id": reference_id,
                        "resourceVersion": 1,
                    },
                    "state": outer.response_state,
                    "name": body["name"],
                    "datasetVersion": body["datasetVersion"],
                    "createdAt": "2026-09-11T12:00:00+00:00",
                }
                outer.draft_response = draft
                self._respond(outer.response_status, draft)

            def _respond(self, status: int, payload: dict[str, Any]) -> None:
                data = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, format: str, *args: Any) -> None:
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def paths(self) -> list[str]:
        return [request["path"] for request in self.requests]


def _unreachable_url() -> str:
    """Bind then release a local port so nothing can answer. | 取得确定不可达地址。"""

    import socket

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    return f"http://127.0.0.1:{port}"


def _publish_instruction_version(client: TestClient) -> tuple[str, str]:
    """Publish an instruction-mode DatasetVersion and return ids. | 发布 instruction 版本。"""

    dataset_id = client.post("/api/v1/datasets", json={"name": "yield-handoff"}).json()["id"]
    source = (
        b'{"speaker":"A","text":"hello","scene":"s1"}\n'
        b'{"speaker":"B","text":"world","scene":"s2"}\n'
    )
    preparation = client.post(
        f"/api/v1/datasets/{dataset_id}/preparations?name=demo.jsonl&filename=demo.jsonl",
        content=source,
        headers={"Content-Type": "application/octet-stream"},
    ).json()
    mapped = client.patch(
        f"/api/v1/preparations/{preparation['id']}/mapping",
        json={
            "mapping": {
                "mode": "instruction",
                "instruction": {"field": "speaker"},
                "output": {"field": "text"},
                "groupBy": "scene",
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
    published = client.post(
        f"/api/v1/preparations/{preparation['id']}/publish",
        headers={"Idempotency-Key": "yield-handoff-publish"},
    )
    assert published.status_code == 201, published.text
    return preparation["id"], published.json()["datasetVersion"]["id"]


def test_vendored_yield_contract_digests_match_provenance() -> None:
    """The fixture must stay byte-identical to the recorded Yield snapshot. | 快照摘要校验。"""

    digests = _provenance_digests()
    assert set(digests) == VENDORED_FILES, "PROVENANCE.md must record every vendored file"
    for relative_path, expected in digests.items():
        actual = hashlib.sha256((CONTRACT_ROOT / relative_path).read_bytes()).hexdigest()
        assert actual == expected, f"{relative_path} differs from its recorded Yield digest"


def test_vendored_yield_contract_publishes_the_consumed_operation() -> None:
    """The consumed boundary must be valid openapi with owner-published schemas. | 契约有效性。"""

    spec, base, _ = _contract_registry()
    validate_openapi_spec(spec, base_uri=base)
    operation = spec["paths"]["/api/v1/training-drafts"]["post"]
    assert operation["operationId"] == "import_dataset_api_v1_training_drafts_post"
    assert (
        operation["requestBody"]["content"]["application/json"]["schema"]["$ref"]
        == "#/components/schemas/CreateTrainingDraft"
    )
    created = operation["responses"]["201"]["content"]["application/json"]["schema"]["$ref"]
    assert created == "#/components/schemas/TrainingDraft"


def test_send_to_yield_creates_training_draft_without_starting_training(tmp_path: Path) -> None:
    """A published version produces exactly one contract-shaped draft request. | 创建草稿。"""

    stub = _YieldStub()
    try:
        app = create_app(
            database_path=tmp_path / "catalyst.sqlite3",
            artifact_root=tmp_path / "artifacts",
            yield_url=stub.url,
        )
        with TestClient(app) as client:
            preparation_id, version_id = _publish_instruction_version(client)
            response = client.post(f"/api/v1/preparations/{preparation_id}/yield-draft")
        assert response.status_code == 200, response.text
        receipt = response.json()
        assert receipt["status"] == "DRAFT"

        assert len(stub.requests) == 1
        request = stub.requests[0]
        assert request["path"] == "/api/v1/training-drafts"
        assert request["idempotency_key"] == f"catalyst-version:{version_id}:1"
        # Owner schema accepts the outbound payload exactly as sent.
        request_errors = list(_schema_validator("CreateTrainingDraft").iter_errors(request["body"]))
        assert request_errors == [], [error.message for error in request_errors]
        # The confirmed target resource is the owner's TrainingDraft.
        assert stub.draft_response is not None
        draft_errors = list(_schema_validator("TrainingDraft").iter_errors(stub.draft_response))
        assert draft_errors == [], [error.message for error in draft_errors]
        assert receipt["targetResource"]["id"] == stub.draft_response["id"]
        assert receipt["targetResource"]["uri"] == stub.draft_response["resourceRef"]["uri"]
        assert receipt["openIn"] == f"{stub.url}/api/v1/training-drafts/{stub.draft_response['id']}"
        # Handoff never auto-starts training: no /actions/start request was made.
        assert all("/actions/start" not in path for path in stub.paths())
        app.state.catalyst_store.close()
    finally:
        stub.close()


def test_send_to_yield_replays_the_same_handoff_identity(tmp_path: Path) -> None:
    """A retry reuses the same Idempotency-Key and confirmed draft. | 重试幂等。"""

    stub = _YieldStub()
    try:
        app = create_app(
            database_path=tmp_path / "catalyst.sqlite3",
            artifact_root=tmp_path / "artifacts",
            yield_url=stub.url,
        )
        with TestClient(app) as client:
            preparation_id, version_id = _publish_instruction_version(client)
            path = f"/api/v1/preparations/{preparation_id}/yield-draft"
            first = client.post(path)
            second = client.post(path)
        assert first.status_code == second.status_code == 200
        assert first.json() == second.json()
        assert len(stub.requests) == 2
        expected_key = f"catalyst-version:{version_id}:1"
        assert [request["idempotency_key"] for request in stub.requests] == [
            expected_key,
            expected_key,
        ]
        app.state.catalyst_store.close()
    finally:
        stub.close()


def test_send_to_yield_fails_closed_when_yield_is_unreachable(tmp_path: Path) -> None:
    """A configured but unreachable Yield must fail with a stable error. | 不可达稳定失败。"""

    url = _unreachable_url()
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
        yield_url=url,
    )
    with TestClient(app) as client:
        preparation_id, _ = _publish_instruction_version(client)
        response = client.post(f"/api/v1/preparations/{preparation_id}/yield-draft")
        assert response.status_code == 502
        problem = response.json()
        assert problem["code"] == "CATALYST_YIELD_HANDOFF_FAILED"
        assert problem["retryable"] is True
        # The published version is untouched; nothing was fabricated.
        preparation = client.get(f"/api/v1/preparations/{preparation_id}").json()
        assert preparation["state"] == "PUBLISHED"
        assert preparation["publishedVersionId"] is not None
    app.state.catalyst_store.close()


@pytest.mark.parametrize(
    "stub_kwargs",
    [
        {"response_status": 200},
        {"response_status": 422},
        {"response_state": "COMPLETED"},
        {"resource_ref_mismatch": True},
    ],
)
def test_send_to_yield_fails_closed_on_unconfirmed_target(
    tmp_path: Path, stub_kwargs: dict[str, Any]
) -> None:
    """Rejection or an unconfirmed identity must not become a receipt. | 未确认即失败。"""

    stub = _YieldStub(**stub_kwargs)
    try:
        app = create_app(
            database_path=tmp_path / "catalyst.sqlite3",
            artifact_root=tmp_path / "artifacts",
            yield_url=stub.url,
        )
        with TestClient(app) as client:
            preparation_id, _ = _publish_instruction_version(client)
            response = client.post(f"/api/v1/preparations/{preparation_id}/yield-draft")
        assert response.status_code == 502
        assert response.json()["code"] == "CATALYST_YIELD_HANDOFF_FAILED"
        app.state.catalyst_store.close()
    finally:
        stub.close()
