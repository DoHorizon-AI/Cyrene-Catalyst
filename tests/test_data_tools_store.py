"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 test_data_tools_store.py                                        │
│  Module: tests.test_data_tools_store                                │
│  Role: Persistence and job lifecycle proof for Data Tools.          │
│                                                                     │
│  模块职责：验证 Data Tools 持久化、重启恢复、取消与重试边界。            │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from threading import Event
from uuid import UUID, uuid4

import pytest

from cyrene_catalyst.data_tools_domain import (
    Annotation,
    AnnotationKind,
    BlockOrigin,
    ContentBlock,
    ContentPolicy,
    ContentRevision,
    ContentRevisionState,
    GenerationReceipt,
    ProcessingOperation,
    ProcessingRun,
    ProcessingRunState,
    ProcessingStage,
    ProcessingStageState,
    SourceRevision,
)
from cyrene_catalyst.data_tools_store import DataToolsStore
from cyrene_catalyst.domain import (
    ArtifactRef,
    Dataset,
    DatasetVersion,
    DatasetVersionState,
    utc_now,
)
from cyrene_catalyst.processing_runs import ProcessingRunCoordinator
from cyrene_catalyst.store import CatalystStore
from cyrene_catalyst.workspace_auth import WorkspaceServicePrincipal


def _artifact(size: int = 12) -> ArtifactRef:
    digest = hashlib.sha256(b"x" * size).hexdigest()
    return ArtifactRef(
        uri=f"artifact://sha256/{digest}",
        digest=f"sha256:{digest}",
        size_bytes=size,
        kind="dataset",
    )


def _dataset(path: Path) -> tuple[CatalystStore, Dataset]:
    catalyst = CatalystStore(path)
    now = utc_now()
    dataset = Dataset(
        id=uuid4(),
        name="data-tools-test",
        created_at=now,
        updated_at=now,
        resource_version=1,
    )
    catalyst.save_dataset(dataset)
    return catalyst, dataset


def _source(dataset_id: UUID) -> SourceRevision:
    artifact = _artifact()
    return SourceRevision(
        id=uuid4(),
        dataset_id=dataset_id,
        source_id=uuid4(),
        revision=1,
        filename="guide.pdf",
        media_type="application/pdf",
        byte_length=artifact.size_bytes,
        digest=artifact.digest,
        artifact=artifact,
    )


def test_sources_revisions_and_version_bound_annotations_survive_reopen(
    tmp_path: Path,
) -> None:
    database = tmp_path / "catalyst.sqlite3"
    catalyst, dataset = _dataset(database)
    data_tools = DataToolsStore(database)
    source = _source(dataset.id)
    data_tools.create_source(source)
    revision = ContentRevision(
        id=uuid4(),
        dataset_id=dataset.id,
        revision=1,
        source_revision_ids=[source.id],
        blocks=[
            ContentBlock(
                id="block-1",
                source_revision_id=source.id,
                ordinal=0,
                kind="training_record",
                text='{"instruction":"Keep the learned text clean.","output":"Stored separately."}',
                source_family_id="family-7",
                group_id="group-2",
                conversation_id="conversation-2",
                sample_id="sample-19",
                generation_receipt=GenerationReceipt(
                    recipe_id="grounded-qa",
                    recipe_version="1",
                    recipe_digest=f"sha256:{hashlib.sha256(b'recipe').hexdigest()}",
                    binding_id="model-provider-trial",
                    model="trial-model",
                    budget={"maxCalls": 1},
                    usage={"completionTokens": 28},
                    generated_at=utc_now(),
                    source_block_ids=["source-block-4"],
                ),
                origin=BlockOrigin.GENERATED,
                policy=ContentPolicy(allow_knowledge=True),
            )
        ],
    )
    data_tools.create_content_revision(revision)
    reviewed = data_tools.update_content_review(
        revision.id,
        ContentRevisionState.APPROVED,
        note="Reviewed for the trial.",
    )
    assert reviewed.state == ContentRevisionState.APPROVED
    now = utc_now()
    version = DatasetVersion(
        id=uuid4(),
        dataset_id=dataset.id,
        version=1,
        state=DatasetVersionState.PUBLISHED,
        source=source.artifact,
        engine_binding_id="data-tools-test",
        source_refs=[str(source.id)],
        created_at=now,
        updated_at=now,
        resource_version=1,
    )
    catalyst.save_version(version)
    annotation = Annotation(
        id=uuid4(),
        dataset_id=dataset.id,
        dataset_version_id=version.id,
        content_revision_id=revision.id,
        block_id="block-1",
        kind=AnnotationKind.NOTE,
        text="Keep this example.",
    )
    data_tools.save_annotation(annotation)
    data_tools.close()
    catalyst.close()

    catalyst = CatalystStore(database)
    data_tools = DataToolsStore(database)
    assert data_tools.get_source(source.id) == source
    reloaded = data_tools.get_content_revision(revision.id)
    assert reloaded is not None
    assert reloaded.state == ContentRevisionState.APPROVED
    assert reloaded.review_note == "Reviewed for the trial."
    assert reloaded.blocks[0].policy.allow_knowledge is True
    reloaded_block = reloaded.blocks[0]
    assert reloaded_block.source_family_id == "family-7"
    assert reloaded_block.group_id == "group-2"
    assert reloaded_block.conversation_id == "conversation-2"
    assert reloaded_block.sample_id == "sample-19"
    assert reloaded_block.generation_receipt is not None
    assert reloaded_block.generation_receipt.source_block_ids == ["source-block-4"]
    assert "generationReceipt" not in reloaded_block.text
    assert (
        reloaded_block.model_dump(by_alias=True)["generationReceipt"]["bindingId"]
        == "model-provider-trial"
    )
    assert data_tools.list_annotations(version.id) == [annotation]
    assert catalyst.get_version(version.id) == version
    data_tools.close()
    catalyst.close()


def test_deterministic_stage_cache_and_running_cancel(tmp_path: Path) -> None:
    database = tmp_path / "catalyst.sqlite3"
    catalyst, dataset = _dataset(database)
    store = DataToolsStore(database)
    calls = 0

    def stage_runner(
        run: ProcessingRun,
        stage: ProcessingStage,
        cancel_event: Event,
    ) -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {"stage": stage.key, "calls": calls}

    coordinator = ProcessingRunCoordinator(store, stage_runner, recover_on_start=False)
    digest = f"sha256:{'a' * 64}"
    source = _source(dataset.id)
    store.create_source(source)
    first = ProcessingRun(
        id=uuid4(),
        dataset_id=dataset.id,
        operation=ProcessingOperation.PARSE,
        source_revision_ids=[source.id],
        recipe={"format": "pdf"},
        stages=[
            ProcessingStage(
                key="parse:source-a",
                source_revision_id=source.id,
                input_digest=digest,
            )
        ],
    )
    store.create_run(first)
    coordinator.enqueue(first.id)
    completed = coordinator.wait(first.id, timeout=5)
    assert completed.state == ProcessingRunState.SUCCEEDED
    assert completed.stages[0].state == ProcessingStageState.COMPLETED

    second = first.model_copy(update={"id": uuid4()})
    store.create_run(second)
    coordinator.enqueue(second.id)
    reused = coordinator.wait(second.id, timeout=5)
    assert reused.state == ProcessingRunState.SUCCEEDED
    assert reused.stages[0].state == ProcessingStageState.REUSED
    assert calls == 1

    second_source = _source(dataset.id)
    store.create_source(second_source)
    distinct_source_run = ProcessingRun(
        id=uuid4(),
        dataset_id=dataset.id,
        operation=ProcessingOperation.PARSE,
        source_revision_ids=[second_source.id],
        recipe={"format": "pdf"},
        stages=[
            ProcessingStage(
                key="parse:source-a",
                source_revision_id=second_source.id,
                input_digest=digest,
            )
        ],
    )
    store.create_run(distinct_source_run)
    coordinator.enqueue(distinct_source_run.id)
    distinct = coordinator.wait(distinct_source_run.id, timeout=5)
    assert distinct.stages[0].state == ProcessingStageState.COMPLETED
    assert calls == 2

    entered = Event()

    def slow_stage(
        run: ProcessingRun,
        stage: ProcessingStage,
        cancel_event: Event,
    ) -> dict[str, object]:
        entered.set()
        cancel_event.wait(timeout=5)
        return {"finished": True}

    slow_coordinator = ProcessingRunCoordinator(store, slow_stage, recover_on_start=False)
    cancellable = ProcessingRun(
        id=uuid4(),
        dataset_id=dataset.id,
        operation=ProcessingOperation.BUILD_KNOWLEDGE,
        stages=[ProcessingStage(key="build:source-a")],
    )
    store.create_run(cancellable)
    slow_coordinator.enqueue(cancellable.id)
    assert entered.wait(timeout=5)
    requested = slow_coordinator.cancel(cancellable.id)
    assert requested.cancellation_requested is True
    cancelled = slow_coordinator.wait(cancellable.id, timeout=5)
    assert cancelled.state == ProcessingRunState.CANCELLED
    cancellation_retry = slow_coordinator.retry(cancellable.id)
    assert cancellation_retry.retry_of_run_id == cancellable.id
    assert (
        slow_coordinator.wait(cancellation_retry.id, timeout=5).state
        == ProcessingRunState.SUCCEEDED
    )
    slow_coordinator.shutdown()
    coordinator.shutdown()
    store.close()
    catalyst.close()


def test_restart_marks_interrupted_and_blocks_ambiguous_paid_retry(tmp_path: Path) -> None:
    database = tmp_path / "catalyst.sqlite3"
    catalyst, dataset = _dataset(database)
    store = DataToolsStore(database)
    now = utc_now()
    interrupted = ProcessingRun(
        id=uuid4(),
        dataset_id=dataset.id,
        operation=ProcessingOperation.PARSE,
        state=ProcessingRunState.RUNNING,
        started_at=now,
        stages=[
            ProcessingStage(
                key="parse:source-a",
                state=ProcessingStageState.RUNNING,
                started_at=now,
            )
        ],
    )
    paid_call = ProcessingRun(
        id=uuid4(),
        dataset_id=dataset.id,
        operation=ProcessingOperation.GENERATE_QA,
        state=ProcessingRunState.RUNNING,
        started_at=now,
        stages=[
            ProcessingStage(
                key="generate:provider-call",
                state=ProcessingStageState.RUNNING,
                deterministic=False,
                external_call=True,
                call_started_at=now,
                started_at=now,
            )
        ],
    )
    store.create_run(interrupted)
    store.create_run(paid_call)
    coordinator = ProcessingRunCoordinator(
        store,
        lambda run, stage, cancel_event: {"done": True},
    )
    recovered = coordinator.get(interrupted.id)
    assert recovered is not None
    assert recovered.state == ProcessingRunState.INTERRUPTED
    assert recovered.stages[0].state == ProcessingStageState.INTERRUPTED
    retried = coordinator.retry(interrupted.id)
    assert retried.retry_of_run_id == interrupted.id
    assert coordinator.wait(retried.id, timeout=5).state == ProcessingRunState.SUCCEEDED

    unknown = coordinator.get(paid_call.id)
    assert unknown is not None
    assert unknown.state == ProcessingRunState.INTERRUPTED
    assert unknown.stages[0].state == ProcessingStageState.OUTCOME_UNKNOWN
    assert unknown.failure is not None and unknown.failure.retryable is False
    with pytest.raises(ValueError, match="provider outcome is unknown"):
        coordinator.retry(paid_call.id)
    coordinator.shutdown()
    store.close()
    catalyst.close()


def test_scoped_dataset_can_run_through_trusted_worker_accessors(tmp_path: Path) -> None:
    database = tmp_path / "catalyst.sqlite3"
    catalyst = CatalystStore(database)
    principal = WorkspaceServicePrincipal(organization_id="org-a", workspace_id="ws-a")
    now = utc_now()
    dataset = Dataset(
        id=uuid4(),
        name="scoped-data-tools-test",
        created_at=now,
        updated_at=now,
        resource_version=1,
    )
    catalyst.create_workspace_dataset(dataset, principal, None, "request-digest")
    store = DataToolsStore(database)
    source = _source(dataset.id)
    store.create_source(source, principal)
    revision = ContentRevision(
        id=uuid4(),
        dataset_id=dataset.id,
        revision=1,
        source_revision_ids=[source.id],
        blocks=[ContentBlock(id="scoped-block", source_revision_id=source.id, ordinal=0)],
    )
    store.create_content_revision_for_worker(revision)
    run = ProcessingRun(
        id=uuid4(),
        dataset_id=dataset.id,
        operation=ProcessingOperation.PARSE,
        source_revision_ids=[source.id],
        stages=[ProcessingStage(key="parse:scoped-source")],
    )
    store.create_run(run, principal)
    coordinator = ProcessingRunCoordinator(
        store,
        lambda current, stage, cancel_event: {"parsed": True},
        recover_on_start=False,
    )
    coordinator.enqueue(run.id, principal)
    assert (
        coordinator.wait(run.id, timeout=5, principal=principal).state
        == ProcessingRunState.SUCCEEDED
    )
    assert store.get_run(run.id) is None
    assert store.get_run(run.id, principal) is not None
    assert store.get_source_for_worker(source.id) == source
    assert store.get_content_revision(revision.id, principal) is not None
    coordinator.shutdown()
    store.close()
    catalyst.close()
