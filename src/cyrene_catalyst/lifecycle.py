"""
┌─────────────────────────────────────────────────────────────────────┐
│  Module: cyrene_catalyst.lifecycle                                   │
│  Role: Explicit Yield handoff and selected Echo feedback imports.   │
│  模块职责：基于产品引用与制品引用进行显式交接，不触发训练或发布。           │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import hashlib
from threading import RLock
from typing import Literal
from uuid import UUID

import httpx
from pydantic import Field

from cyrene_catalyst.domain import (
    ArtifactRef,
    ContractModel,
    CreateDatasetRequest,
    FieldSource,
    MappingConfig,
    NormalizationConfig,
    PreparationState,
)
from cyrene_catalyst.errors import CatalystError
from cyrene_catalyst.service import CatalystService


class ResourceRef(ContractModel):
    """Versioned identity owned by the named Product. | 产品资源引用。"""

    uri: str = Field(pattern=r"^(https?://|cyrene://)[^\s]+$")
    id: UUID
    resource_version: int = Field(ge=1)


class HandoffReceipt(ContractModel):
    """Confirmed target resource, returned only after target creation. | 目标确认。"""

    status: Literal["DRAFT", "PREPARED", "STARTED"]
    target_resource: ResourceRef
    open_in: str


class FeedbackImportRequest(ContractModel):
    source_ref: ResourceRef
    artifact: ArtifactRef
    dataset_id: UUID | None = None
    provenance_refs: list[str] = Field(default_factory=list, max_length=100)


def _error(code: str, status: int, detail: str) -> CatalystError:
    return CatalystError(
        code=code, title=code, detail=detail, status=status, retryable=status >= 500
    )


class LifecycleActions:
    """Product-specific actions over existing preparation and owning Product APIs."""

    def __init__(
        self, service: CatalystService, yield_url: str | None, client: httpx.Client | None = None
    ) -> None:
        self.service = service
        self.yield_url = yield_url.rstrip("/") if yield_url else None
        self.client = client or httpx.Client(timeout=30, trust_env=False)
        self.lock = RLock()

    def send_to_yield(self, version_id: UUID) -> HandoffReceipt:
        """Send references to an instruction DatasetVersion; never start training."""
        if self.yield_url is None:
            raise _error(
                "CATALYST_YIELD_NOT_CONNECTED",
                503,
                "Configure the Yield Product URL before sending.",
            )
        version = self.service.get_version(version_id)
        preparation = next(
            (
                item
                for item in self.service.list_preparations(version.dataset_id)
                if item.published_version_id == version_id
            ),
            None,
        )
        if (
            preparation is None
            or preparation.mapping is None
            or preparation.mapping.mode != "instruction"
        ):
            raise _error(
                "CATALYST_YIELD_FORMAT_UNSUPPORTED",
                422,
                "V1 supports published instruction/Alpaca JSONL preparations.",
            )
        exports = {item.name: item for item in preparation.exports}
        train = exports.get("train.jsonl")
        validation = exports.get("val.jsonl")
        if train is None or validation is None:
            raise _error(
                "CATALYST_STATE_CORRUPT",
                500,
                "The published DatasetVersion is missing its canonical JSONL exports.",
            )
        if not train.row_count:
            raise _error(
                "CATALYST_YIELD_EMPTY_TRAINING_DATA",
                422,
                "Choose a split with training samples before publishing.",
            )
        source = {
            "id": str(version.id),
            "uri": f"cyrene://catalyst/dataset-versions/{version.id}",
            "resourceVersion": version.resource_version,
            "format": "ALPACA_JSONL",
            "artifact": train.artifact.model_dump(exclude_none=True),
            "validationArtifact": validation.artifact.model_dump(exclude_none=True),
            "provenanceRefs": preparation.source_refs,
        }
        try:
            response = self.client.post(
                self.yield_url + "/api/v1/training-drafts",
                json={
                    "name": preparation.name,
                    "datasetVersion": source,
                },
                headers={
                    "Idempotency-Key": f"catalyst-version:{version.id}:{version.resource_version}"
                },
            )
            response.raise_for_status()
            if response.status_code != 201:
                raise ValueError("Yield did not return the contractually required 201 status")
            target = response.json()
            reference = ResourceRef.model_validate(target["resourceRef"])
            returned_source = target["datasetVersion"]
            if (
                str(reference.id) != target["id"]
                or reference.uri != f"cyrene://yield/training-drafts/{reference.id}"
                or target["state"] not in {"DRAFT", "PREPARED", "STARTED"}
                or returned_source.get("id") != str(version.id)
                or returned_source.get("resourceVersion") != version.resource_version
            ):
                raise ValueError("Invalid target draft identity or state")
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            raise _error(
                "CATALYST_YIELD_HANDOFF_FAILED",
                502,
                "Yield did not confirm a valid training draft. "
                "Retrying uses the same handoff identity.",
            ) from exc
        return HandoffReceipt(
            status=target["state"],
            target_resource=reference,
            open_in=self.yield_url + "/api/v1/training-drafts/" + str(reference.id),
        )

    def import_feedback(
        self, command: FeedbackImportRequest, key: str | None = None
    ) -> HandoffReceipt:
        """Create a preparation from explicitly selected feedback, retaining raw provenance."""
        with self.lock:
            return self._import_feedback(command, key)

    def _import_feedback(self, command: FeedbackImportRequest, key: str | None) -> HandoffReceipt:
        expected_source_uri = f"cyrene://echo/feedback-sets/{command.source_ref.id}"
        if command.source_ref.uri != expected_source_uri:
            raise _error(
                "CATALYST_FEEDBACK_SOURCE_INVALID",
                422,
                "Feedback sourceRef must identify the matching Echo feedback set.",
            )
        key = key or f"echo-feedback:{command.source_ref.id}:{command.source_ref.resource_version}"
        digest = hashlib.sha256(command.model_dump_json().encode()).hexdigest()
        replay = self.service.store.resolve_idempotency("feedback-import", key, digest)
        if replay is not None:
            return self._receipt(UUID(replay))
        source = self.service.artifacts.resolve(command.artifact)
        if (
            not source.is_file()
            or command.artifact.kind != "dataset"
            or source.stat().st_size > 16 * 1024**2
        ):
            raise _error(
                "CATALYST_FEEDBACK_INVALID",
                422,
                "Feedback must reference a verified JSONL dataset Artifact.",
            )
        dataset = (
            self.service.get_dataset(command.dataset_id)
            if command.dataset_id
            else self.service.create_dataset(
                CreateDatasetRequest(name="Echo feedback " + str(command.source_ref.id)),
                key,
            )
        )
        preparation = self.service.create_preparation(
            dataset.id,
            name="Selected Echo feedback",
            filename="feedback.jsonl",
            data=source.read_bytes(),
            idempotency_key=key,
        )
        if preparation.state == PreparationState.STAGED:
            preparation = self.service.configure_mapping(
                preparation.id,
                MappingConfig(
                    mode="instruction",
                    instruction=FieldSource(field="instruction"),
                    input=FieldSource(field="input"),
                    output=FieldSource(field="output"),
                ),
                NormalizationConfig(),
            )
        preparation.source_refs = list(
            dict.fromkeys([command.source_ref.uri, *command.provenance_refs])
        )
        self.service.store.save_preparation(preparation)
        self.service.store.remember_idempotency(
            scope="feedback-import",
            key=key,
            request_hash=digest,
            resource_kind="preparation",
            resource_id=preparation.id,
        )
        return self._receipt(preparation.id)

    def _receipt(self, identifier: UUID) -> HandoffReceipt:
        preparation = self.service.get_preparation(identifier)
        reference = ResourceRef(
            id=preparation.id,
            uri=f"cyrene://catalyst/preparations/{preparation.id}",
            resource_version=preparation.resource_version,
        )
        return HandoffReceipt(
            status="STARTED" if preparation.published_version_id else "DRAFT",
            target_resource=reference,
            open_in=f"/api/v1/preparations/{preparation.id}",
        )
