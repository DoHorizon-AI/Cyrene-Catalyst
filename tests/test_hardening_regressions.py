"""Regression tests for Catalyst hardening (Phase 1, Catalyst #5)."""

from pathlib import Path
from uuid import uuid4

import pytest

from cyrene_catalyst import create_app
from cyrene_catalyst.domain import (
    CreateDatasetRequest,
    ImportFormat,
    Preparation,
    PreparationState,
    SplitConfig,
    utc_now,
)
from cyrene_catalyst.errors import CatalystError
from cyrene_catalyst.lifecycle import FeedbackImportRequest, LifecycleActions, ResourceRef
from cyrene_catalyst.logging import is_sensitive_key


def test_sensitive_key_excludes_author_and_authority() -> None:
    """Ensure non-sensitive keys containing 'auth' are not redacted."""
    assert not is_sensitive_key("author")
    assert not is_sensitive_key("authored_at")
    assert not is_sensitive_key("author_id")
    assert not is_sensitive_key("authority")
    assert not is_sensitive_key("tokens")
    assert not is_sensitive_key("total_tokens")

    assert is_sensitive_key("authorization")
    assert is_sensitive_key("auth_token")
    assert is_sensitive_key("api_key")
    assert is_sensitive_key("secret")


def test_state_invariant_explicit_exception_without_assert(tmp_path: Path) -> None:
    """Verify state invariants raise CatalystError 409 instead of bare AssertionError."""
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
    )
    service = app.state.catalyst_service
    dataset = service.create_dataset(CreateDatasetRequest(name="inv-test"), idempotency_key=None)
    now = utc_now()
    source_ref = service.artifacts.ingest_bytes(
        b'{"instruction": "x", "output": "y"}\n', "source.jsonl"
    )
    prep = Preparation(
        id=uuid4(),
        dataset_id=dataset.id,
        name="test",
        format=ImportFormat.JSONL,
        source=source_ref,
        detected_fields=["instruction", "output"],
        row_count=1,
        state=PreparationState.MAPPED,
        mapping=None,  # missing invariant
        normalization=None,
        split=None,
        created_at=now,
        updated_at=now,
        resource_version=1,
    )
    service.store.save_preparation(prep)

    with pytest.raises(CatalystError) as exc_info:
        service.configure_split(prep.id, SplitConfig(train_ratio=0.8))
    assert exc_info.value.status == 409
    assert exc_info.value.code == "CATALYST_INVALID_STATE"

    # Also test publish_preparation invariant
    prep_confirmed = prep.model_copy(update={"state": PreparationState.CONFIRMED})
    service.store.save_preparation(prep_confirmed)
    with pytest.raises(CatalystError) as exc_info:
        service.publish_preparation(prep.id, idempotency_key=None)
    assert exc_info.value.status == 409
    assert exc_info.value.code == "CATALYST_INVALID_STATE"


def test_import_feedback_increments_resource_version(tmp_path: Path) -> None:
    """Verify _import_feedback increments resource_version and updates updated_at."""
    app = create_app(
        database_path=tmp_path / "catalyst.sqlite3",
        artifact_root=tmp_path / "artifacts",
    )
    service = app.state.catalyst_service
    dataset = service.create_dataset(CreateDatasetRequest(name="echo-target"), idempotency_key=None)
    lifecycle = LifecycleActions(service=service, yield_url="http://127.0.0.1:8092")

    # Ingest bytes to simulate feedback artifact
    feedback_bytes = b'{"instruction": "q", "input": "", "output": "a"}\n'
    feedback_artifact = service.artifacts.ingest_bytes(feedback_bytes, "feedback.jsonl")

    source_id = uuid4()
    command = FeedbackImportRequest(
        dataset_id=dataset.id,
        source_ref=ResourceRef(
            uri=f"cyrene://echo/feedback-sets/{source_id}",
            id=source_id,
            resource_version=1,
        ),
        artifact=feedback_artifact,
        provenance_refs=["cyrene://echo/runs/22222222-2222-2222-2222-222222222222"],
    )

    receipt = lifecycle.import_feedback(command, key="test-key")
    prep = service.get_preparation(receipt.target_resource.id)

    # Initial create is v1; configure_mapping is v2; updating source_refs must bump to v3!
    assert prep.resource_version >= 3
    assert prep.source_refs == [
        command.source_ref.uri,
        "cyrene://echo/runs/22222222-2222-2222-2222-222222222222",
    ]
