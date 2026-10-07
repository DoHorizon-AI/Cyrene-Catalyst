"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 test_data_tools_parse_reports.py                                 │
│  Module: tests.test_data_tools_parse_reports                         │
│  Role: Per-source parse receipts, review gates, and restart recovery. │
│                                                                     │
│  模块职责：验证逐来源解析报告、审核阻断与重启恢复。                    │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import hashlib
import sqlite3
from datetime import timedelta
from pathlib import Path
from threading import Event, Thread
from uuid import UUID, uuid4

import pytest

from cyrene_catalyst.data_tools_domain import (
    ContentBlock,
    ContentRevision,
    ContentRevisionState,
    ProcessingFailure,
    ProcessingOperation,
    ProcessingRun,
    ProcessingRunState,
    ProcessingStage,
    ProcessingStageState,
    ProcessingWarning,
    ReviewItem,
    ReviewItemKind,
    ReviewItemResolution,
    ReviewItemState,
    SourceParseReport,
    SourceParseReportState,
    SourceRevision,
)
from cyrene_catalyst.data_tools_store import DataToolsStore
from cyrene_catalyst.domain import ArtifactRef, Dataset, utc_now
from cyrene_catalyst.processing_runs import ProcessingRunCoordinator
from cyrene_catalyst.store import CatalystStore
from cyrene_catalyst.workspace_auth import WorkspaceServicePrincipal


def _artifact(data: bytes, kind: str = "dataset") -> ArtifactRef:
    digest = hashlib.sha256(data).hexdigest()
    return ArtifactRef(
        uri=f"artifact://sha256/{digest}",
        digest=f"sha256:{digest}",
        size_bytes=len(data),
        kind=kind,
    )


def _dataset(
    catalyst: CatalystStore,
    name: str,
    principal: WorkspaceServicePrincipal | None = None,
) -> Dataset:
    now = utc_now()
    dataset = Dataset(
        id=uuid4(),
        name=name,
        created_at=now,
        updated_at=now,
        resource_version=1,
    )
    if principal is None:
        catalyst.save_dataset(dataset)
    else:
        catalyst.create_workspace_dataset(dataset, principal, None, str(uuid4()))
    return dataset


def _source(dataset_id: UUID, filename: str) -> SourceRevision:
    artifact = _artifact(filename.encode("utf-8"))
    return SourceRevision(
        id=uuid4(),
        dataset_id=dataset_id,
        source_id=uuid4(),
        revision=1,
        filename=filename,
        media_type="application/octet-stream",
        byte_length=artifact.size_bytes,
        digest=artifact.digest,
        artifact=artifact,
    )


def _run(dataset_id: UUID, source_ids: list[UUID], *, running: bool = False) -> ProcessingRun:
    now = utc_now()
    return ProcessingRun(
        id=uuid4(),
        dataset_id=dataset_id,
        operation=ProcessingOperation.PARSE,
        state=ProcessingRunState.RUNNING if running else ProcessingRunState.QUEUED,
        source_revision_ids=source_ids,
        started_at=now if running else None,
        stages=[
            ProcessingStage(
                key="parse:batch",
                state=ProcessingStageState.RUNNING if running else ProcessingStageState.PENDING,
                started_at=now if running else None,
            )
        ],
    )


def _report(
    dataset_id: UUID,
    source_id: UUID,
    run_id: UUID,
    status: SourceParseReportState,
    *,
    content_revision_id: UUID | None = None,
    failure: ProcessingFailure | None = None,
) -> SourceParseReport:
    now = utc_now()
    artifacts = (
        [_artifact(str(source_id).encode("utf-8"), "source-parse-blocks")]
        if status in {SourceParseReportState.SUCCEEDED, SourceParseReportState.WARNING}
        else []
    )
    return SourceParseReport(
        id=uuid4(),
        dataset_id=dataset_id,
        source_revision_id=source_id,
        processing_run_id=run_id,
        status=status,
        content_revision_id=content_revision_id,
        block_count=0,
        failure=failure,
        output_artifacts=artifacts,
        created_at=now,
        updated_at=now,
        started_at=now if status != SourceParseReportState.QUEUED else None,
        finished_at=(
            now
            if status
            in {
                SourceParseReportState.SUCCEEDED,
                SourceParseReportState.WARNING,
                SourceParseReportState.FAILED,
                SourceParseReportState.INTERRUPTED,
                SourceParseReportState.CANCELLED,
            }
            else None
        ),
    )


def test_per_source_reports_are_independent_scoped_and_migrated(tmp_path: Path) -> None:
    database = tmp_path / "catalyst.sqlite3"
    catalyst = CatalystStore(database)
    principal = WorkspaceServicePrincipal(organization_id="org-a", workspace_id="ws-a")
    other_principal = WorkspaceServicePrincipal(organization_id="org-b", workspace_id="ws-b")
    dataset = _dataset(catalyst, "report-test", principal)
    other_dataset = _dataset(catalyst, "other-report-test", other_principal)
    store = DataToolsStore(database)
    first_source = _source(dataset.id, "unsupported.pptx")
    second_source = _source(dataset.id, "partial.xlsx")
    other_source = _source(other_dataset.id, "other.pdf")
    for source, owner in (
        (first_source, principal),
        (second_source, principal),
        (other_source, other_principal),
    ):
        store.create_source(source, owner)
    run = _run(dataset.id, [first_source.id, second_source.id])
    other_run = _run(other_dataset.id, [other_source.id])
    store.create_run(run, principal)
    store.create_run(other_run, other_principal)

    unsupported = _report(
        dataset.id,
        first_source.id,
        run.id,
        SourceParseReportState.FAILED,
        failure=ProcessingFailure(
            code="CATALYST_SOURCE_UNSUPPORTED",
            message="No supported slide content was found.",
            retryable=False,
        ),
    ).model_copy(
        update={
            "unsupported_content": [{"kind": "pptx_embedded_object", "relationship_id": "rId7"}]
        }
    )
    warning = _report(
        dataset.id,
        second_source.id,
        run.id,
        SourceParseReportState.WARNING,
    ).model_copy(
        update={
            "block_count": 3,
            "warnings": [
                ProcessingWarning(code="OCR_LOW_CONFIDENCE", message="Cell A2 was uncertain.")
            ],
            "diagnostics": [
                {
                    "kind": "ocr",
                    "severity": "warning",
                    "code": "ocr.low_confidence",
                    "message": "OCR confidence is below the review threshold.",
                    "confidence": 0.62,
                    "locator": {"source_pages": [1], "item_ref": "Sheet1!A2"},
                }
            ],
        }
    )
    other_report = _report(
        other_dataset.id,
        other_source.id,
        other_run.id,
        SourceParseReportState.SUCCEEDED,
    )
    assert store.save_source_parse_report(unsupported, principal) == unsupported
    assert store.save_source_parse_report(warning, principal) == warning
    assert store.save_source_parse_report(other_report, other_principal) == other_report
    assert store.save_source_parse_report(warning, principal) == warning

    own_reports = store.list_source_parse_reports(dataset.id, principal=principal)
    assert {item.source_revision_id for item in own_reports} == {
        first_source.id,
        second_source.id,
    }
    assert len(store.list_source_parse_reports(dataset.id, principal=other_principal)) == 0
    assert store.get_source_parse_report(other_report.id, principal) is None
    stored_unsupported = store.get_source_parse_report(unsupported.id, principal)
    assert stored_unsupported is not None
    assert stored_unsupported.failure is not None
    assert stored_unsupported.unsupported_content[0]["relationship_id"] == "rId7"
    stored_warning = store.get_source_parse_report(warning.id, principal)
    assert stored_warning is not None
    assert stored_warning.diagnostics[0]["code"] == "ocr.low_confidence"

    with pytest.raises(sqlite3.IntegrityError):
        store._connection.execute(
            "INSERT INTO data_tool_source_parse_reports "
            "(id,dataset_id,source_revision_id,processing_run_id,"
            "status,created_at,updated_at,document) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                str(uuid4()),
                str(dataset.id),
                str(other_source.id),
                str(run.id),
                SourceParseReportState.FAILED.value,
                utc_now().isoformat(),
                utc_now().isoformat(),
                "{}",
            ),
        )
    store._connection.rollback()

    store.close()
    with sqlite3.connect(database) as connection:
        connection.execute("DROP TABLE data_tool_review_items")
        connection.execute("DROP TABLE data_tool_source_parse_reports")
        connection.execute("DROP INDEX idx_data_tool_sources_dataset_id")
        connection.execute("DROP INDEX idx_data_tool_content_dataset_id")
        connection.execute("DROP INDEX idx_data_tool_runs_dataset_id")
    migrated = DataToolsStore(database)
    assert migrated.get_source(first_source.id, principal) == first_source
    assert migrated.get_source_parse_report(unsupported.id, principal) is None
    assert migrated._connection.execute("PRAGMA foreign_key_check").fetchall() == []
    migrated.close()
    catalyst.close()


def test_source_report_timestamps_remain_monotonic_after_wall_clock_reversal(
    tmp_path: Path,
) -> None:
    database = tmp_path / "report-clock.sqlite3"
    catalyst = CatalystStore(database)
    dataset = _dataset(catalyst, "report-clock")
    store = DataToolsStore(database)
    source = _source(dataset.id, "clock.pdf")
    store.create_source(source)
    run = _run(dataset.id, [source.id])
    store.create_run(run)

    observed_now = utc_now()
    report_created = observed_now + timedelta(seconds=34)
    queued = _report(
        dataset.id,
        source.id,
        run.id,
        SourceParseReportState.QUEUED,
    ).model_copy(update={"created_at": report_created, "updated_at": report_created})
    store.save_source_parse_report(queued)

    running = store.save_source_parse_report(
        queued.model_copy(
            update={
                "status": SourceParseReportState.RUNNING,
                "started_at": observed_now,
                "updated_at": observed_now,
            }
        )
    )
    assert running.status == SourceParseReportState.RUNNING
    assert running.started_at is not None
    assert running.started_at > running.created_at
    assert running.updated_at > queued.updated_at

    failed = store.save_source_parse_report(
        running.model_copy(
            update={
                "status": SourceParseReportState.FAILED,
                "failure": ProcessingFailure(
                    code="CATALYST_SOURCE_PARSE_FAILED",
                    message="The source could not be parsed.",
                    retryable=False,
                ),
                "finished_at": observed_now,
                "updated_at": observed_now,
            }
        )
    )
    assert failed.status == SourceParseReportState.FAILED
    assert failed.started_at is not None
    assert failed.finished_at is not None
    assert failed.finished_at >= failed.started_at
    assert failed.updated_at >= failed.finished_at
    assert failed.updated_at > running.updated_at
    assert store.get_source_parse_report(queued.id) == failed

    store.close()
    catalyst.close()


def test_review_items_block_approval_until_acknowledged(tmp_path: Path) -> None:
    database = tmp_path / "catalyst.sqlite3"
    catalyst = CatalystStore(database)
    dataset = _dataset(catalyst, "review-test")
    store = DataToolsStore(database)
    source = _source(dataset.id, "scanned.pdf")
    store.create_source(source)
    run = _run(dataset.id, [source.id])
    store.create_run(run)
    revision = ContentRevision(
        id=uuid4(),
        dataset_id=dataset.id,
        revision=1,
        source_revision_ids=[source.id],
        blocks=[ContentBlock(id="page-1", source_revision_id=source.id, ordinal=0)],
    )
    report = _report(
        dataset.id,
        source.id,
        run.id,
        SourceParseReportState.WARNING,
    ).model_copy(
        update={
            "warnings": [
                ProcessingWarning(
                    code="OCR_LOW_CONFIDENCE",
                    message="Review the uncertain scan text.",
                )
            ],
            "diagnostics": [
                {
                    "kind": "parser",
                    "severity": "warning",
                    "code": "parser.lossy_table_conversion",
                    "message": "Confirm the table conversion before approval.",
                }
            ],
        }
    )
    item = ReviewItem(
        id=uuid4(),
        dataset_id=dataset.id,
        source_parse_report_id=report.id,
        source_revision_id=source.id,
        processing_run_id=run.id,
        content_revision_id=revision.id,
        kind=ReviewItemKind.OCR_WARNING,
        code="ocr.low_confidence",
        message="Review the uncertain scan text.",
        severity="warning",
        confidence=0.42,
        locator={"sourcePages": [1], "itemRef": "page:1"},
    )
    rejected_item = ReviewItem(
        id=uuid4(),
        dataset_id=dataset.id,
        source_parse_report_id=report.id,
        source_revision_id=source.id,
        processing_run_id=run.id,
        content_revision_id=revision.id,
        kind=ReviewItemKind.PARSER_WARNING,
        code="parser.lossy_table_conversion",
        message="Confirm the table conversion before approval.",
        severity="warning",
    )
    store.finalize_parsed_content_revision_for_worker(
        revision,
        [report],
        [item, rejected_item],
    )
    assert store.has_unresolved_review_items(revision.id) is True
    with pytest.raises(ValueError, match="parser or OCR review items"):
        store.update_content_review(revision.id, ContentRevisionState.APPROVED)

    rejected = store.resolve_review_item(
        rejected_item.id,
        ReviewItemResolution.REJECT,
        note="Reparse the source table.",
    )
    assert rejected.state == ReviewItemState.REJECTED
    with pytest.raises(ValueError, match="parser or OCR review items"):
        store.update_content_review(revision.id, ContentRevisionState.APPROVED)

    acknowledged = store.resolve_review_item(
        item.id,
        ReviewItemResolution.ACKNOWLEDGE,
        note="Checked against the original scan.",
    )
    assert acknowledged.state == ReviewItemState.ACKNOWLEDGED
    assert acknowledged.resolved_at is not None
    assert store.has_unresolved_review_items(revision.id) is True

    retry_run = _run(dataset.id, [source.id])
    store.create_run(retry_run)
    retry_revision = ContentRevision(
        id=uuid4(),
        dataset_id=dataset.id,
        revision=2,
        parent_revision_id=revision.id,
        source_revision_ids=[source.id],
        blocks=[ContentBlock(id="page-1-reparsed", source_revision_id=source.id, ordinal=0)],
    )
    retry_report = _report(
        dataset.id,
        source.id,
        retry_run.id,
        SourceParseReportState.WARNING,
    ).model_copy(
        update={
            "warnings": [
                ProcessingWarning(
                    code="OCR_LOW_CONFIDENCE",
                    message="Confirm the reparsed scan text.",
                )
            ]
        }
    )
    retry_item = ReviewItem(
        id=uuid4(),
        dataset_id=dataset.id,
        source_parse_report_id=retry_report.id,
        source_revision_id=source.id,
        processing_run_id=retry_run.id,
        content_revision_id=retry_revision.id,
        kind=ReviewItemKind.OCR_WARNING,
        code="ocr.low_confidence",
        message="Confirm the reparsed scan text.",
        severity="warning",
    )
    store.finalize_parsed_content_revision_for_worker(
        retry_revision,
        [retry_report],
        [retry_item],
    )
    assert store.has_unresolved_review_items(retry_revision.id) is True
    store.resolve_review_item(retry_item.id, ReviewItemResolution.ACKNOWLEDGE)
    assert store.has_unresolved_review_items(retry_revision.id) is False
    approved = store.update_content_review(retry_revision.id, ContentRevisionState.APPROVED)
    assert approved.state == ContentRevisionState.APPROVED

    store.close()
    catalyst.close()


def test_approval_fails_closed_between_report_link_and_issue_insert(tmp_path: Path) -> None:
    database = tmp_path / "report-item-interleave.sqlite3"
    catalyst = CatalystStore(database)
    dataset = _dataset(catalyst, "report-item-interleave")
    store = DataToolsStore(database)
    source = _source(dataset.id, "warning.pdf")
    store.create_source(source)
    run = _run(dataset.id, [source.id])
    store.create_run(run)
    report = _report(
        dataset.id,
        source.id,
        run.id,
        SourceParseReportState.WARNING,
    ).model_copy(
        update={"warnings": [ProcessingWarning(code="OCR_LOW_CONFIDENCE", message="Check page 1.")]}
    )
    store.save_source_parse_report(report)
    revision = ContentRevision(
        id=uuid4(),
        dataset_id=dataset.id,
        revision=1,
        source_revision_ids=[source.id],
        blocks=[ContentBlock(id="page-1", source_revision_id=source.id, ordinal=0)],
    )
    store.create_content_revision(revision)

    # The report exists before its revision link and its ReviewItem. Both windows
    # must reject approval rather than accepting a partially materialized queue.
    with pytest.raises(ValueError, match="parser or OCR review items"):
        store.update_content_review(revision.id, ContentRevisionState.APPROVED)
    linked = store.save_source_parse_report(
        report.model_copy(update={"content_revision_id": revision.id, "updated_at": utc_now()})
    )
    with pytest.raises(ValueError, match="parser or OCR review items"):
        store.update_content_review(revision.id, ContentRevisionState.APPROVED)

    item = ReviewItem(
        id=uuid4(),
        dataset_id=dataset.id,
        source_parse_report_id=linked.id,
        source_revision_id=source.id,
        processing_run_id=run.id,
        content_revision_id=revision.id,
        kind=ReviewItemKind.OCR_WARNING,
        code="ocr.low_confidence",
        message="Check page 1.",
        severity="warning",
    )
    store.save_review_item(item)
    with pytest.raises(ValueError, match="parser or OCR review items"):
        store.update_content_review(revision.id, ContentRevisionState.APPROVED)
    store.resolve_review_item(item.id, ReviewItemResolution.ACKNOWLEDGE)
    assert store.update_content_review(revision.id, ContentRevisionState.APPROVED).state == (
        ContentRevisionState.APPROVED
    )

    store.close()
    catalyst.close()


def test_atomic_parse_finalizer_serializes_concurrent_approval(tmp_path: Path) -> None:
    database = tmp_path / "atomic-finalizer.sqlite3"
    catalyst = CatalystStore(database)
    dataset = _dataset(catalyst, "atomic-finalizer")
    store = DataToolsStore(database)
    approval_store = DataToolsStore(database)
    source = _source(dataset.id, "warning.docx")
    store.create_source(source)
    run = _run(dataset.id, [source.id])
    store.create_run(run)
    report = _report(
        dataset.id,
        source.id,
        run.id,
        SourceParseReportState.WARNING,
    ).model_copy(
        update={
            "warnings": [ProcessingWarning(code="PARSER_WARNING", message="Inspect the block.")]
        }
    )
    store.save_source_parse_report(report)
    revision = ContentRevision(
        id=uuid4(),
        dataset_id=dataset.id,
        revision=1,
        source_revision_ids=[source.id],
        blocks=[ContentBlock(id="block-1", source_revision_id=source.id, ordinal=0)],
    )
    item = ReviewItem(
        id=uuid4(),
        dataset_id=dataset.id,
        source_parse_report_id=report.id,
        source_revision_id=source.id,
        processing_run_id=run.id,
        content_revision_id=revision.id,
        kind=ReviewItemKind.PARSER_WARNING,
        code="parser.warning",
        message="Inspect the block.",
        severity="warning",
    )
    issue_insert_started = Event()
    allow_issue_insert = Event()
    approval_started = Event()
    original_save = store._save_review_item_locked

    def paused_save(
        item: ReviewItem,
        principal: WorkspaceServicePrincipal | None,
        *,
        trusted_worker: bool,
    ) -> ReviewItem:
        issue_insert_started.set()
        assert allow_issue_insert.wait(timeout=3)
        return original_save(item, principal, trusted_worker=trusted_worker)

    store._save_review_item_locked = paused_save  # type: ignore[method-assign]
    finalize_errors: list[BaseException] = []
    approval_errors: list[BaseException] = []

    def finalize() -> None:
        try:
            store.finalize_parsed_content_revision_for_worker(revision, [report], [item])
        except BaseException as exc:  # pragma: no cover - asserted below
            finalize_errors.append(exc)

    def approve() -> None:
        approval_started.set()
        try:
            approval_store.update_content_review(revision.id, ContentRevisionState.APPROVED)
        except BaseException as exc:
            approval_errors.append(exc)

    finalizer_thread = Thread(target=finalize)
    finalizer_thread.start()
    assert issue_insert_started.wait(timeout=3)
    approval_thread = Thread(target=approve)
    approval_thread.start()
    assert approval_started.wait(timeout=3)
    allow_issue_insert.set()
    finalizer_thread.join(timeout=3)
    approval_thread.join(timeout=3)

    assert not finalizer_thread.is_alive()
    assert not approval_thread.is_alive()
    assert finalize_errors == []
    assert len(approval_errors) == 1
    assert isinstance(approval_errors[0], ValueError)
    assert store.get_content_revision_for_worker(revision.id) is not None
    assert store.list_review_items_for_worker(dataset.id, run.id) == [item]

    store.close()
    approval_store.close()
    catalyst.close()


def test_child_edit_inherits_warning_and_clean_reparse_supersedes_it(tmp_path: Path) -> None:
    database = tmp_path / "revision-lineage.sqlite3"
    catalyst = CatalystStore(database)
    dataset = _dataset(catalyst, "revision-lineage")
    store = DataToolsStore(database)
    source = _source(dataset.id, "lineage.pdf")
    store.create_source(source)
    parse_run = _run(dataset.id, [source.id])
    store.create_run(parse_run)
    report = _report(
        dataset.id,
        source.id,
        parse_run.id,
        SourceParseReportState.WARNING,
    ).model_copy(
        update={
            "diagnostics": [
                {
                    "kind": "ocr",
                    "severity": "warning",
                    "code": "ocr.low_confidence",
                    "message": "Check the OCR output.",
                }
            ]
        }
    )
    parent = ContentRevision(
        id=uuid4(),
        dataset_id=dataset.id,
        revision=1,
        source_revision_ids=[source.id],
        blocks=[ContentBlock(id="scan-1", source_revision_id=source.id, ordinal=0)],
    )
    issue = ReviewItem(
        id=uuid4(),
        dataset_id=dataset.id,
        source_parse_report_id=report.id,
        source_revision_id=source.id,
        processing_run_id=parse_run.id,
        content_revision_id=parent.id,
        kind=ReviewItemKind.OCR_WARNING,
        code="ocr.low_confidence",
        message="Check the OCR output.",
        severity="warning",
    )
    store.save_source_parse_report(report)
    store.finalize_parsed_content_revision_for_worker(parent, [report], [issue])

    edited_child = ContentRevision(
        id=uuid4(),
        dataset_id=dataset.id,
        revision=2,
        parent_revision_id=parent.id,
        source_revision_ids=[source.id],
        blocks=[
            ContentBlock(
                id="scan-1-edited",
                source_revision_id=source.id,
                ordinal=0,
                text="Human-edited block.",
            )
        ],
    )
    store.create_content_revision(edited_child)
    with pytest.raises(ValueError, match="parser or OCR review items"):
        store.update_content_review(edited_child.id, ContentRevisionState.APPROVED)

    reparse_run = _run(dataset.id, [source.id])
    store.create_run(reparse_run)
    clean_reparse = ContentRevision(
        id=uuid4(),
        dataset_id=dataset.id,
        revision=3,
        parent_revision_id=edited_child.id,
        source_revision_ids=[source.id],
        blocks=[ContentBlock(id="scan-1-clean", source_revision_id=source.id, ordinal=0)],
    )
    clean_report = _report(
        dataset.id,
        source.id,
        reparse_run.id,
        SourceParseReportState.SUCCEEDED,
    )
    store.save_source_parse_report(clean_report)
    store.finalize_parsed_content_revision_for_worker(clean_reparse, [clean_report], [])
    assert store.has_unresolved_review_items(clean_reparse.id) is False
    approved = store.update_content_review(clean_reparse.id, ContentRevisionState.APPROVED)
    assert approved.state == ContentRevisionState.APPROVED
    assert store.get_review_item_for_worker(issue.id) == issue

    store.close()
    catalyst.close()


def test_parse_restart_interrupts_only_unfinished_source_reports(tmp_path: Path) -> None:
    database = tmp_path / "catalyst.sqlite3"
    catalyst = CatalystStore(database)
    dataset = _dataset(catalyst, "recovery-test")
    store = DataToolsStore(database)
    completed_source = _source(dataset.id, "good.docx")
    running_source = _source(dataset.id, "slow.xlsx")
    store.create_source(completed_source)
    store.create_source(running_source)
    run = _run(dataset.id, [completed_source.id, running_source.id], running=True)
    store.create_run(run)
    revision = ContentRevision(
        id=uuid4(),
        dataset_id=dataset.id,
        revision=1,
        source_revision_ids=[completed_source.id],
        blocks=[ContentBlock(id="done", source_revision_id=completed_source.id, ordinal=0)],
    )
    store.create_content_revision(revision)
    completed_report = _report(
        dataset.id,
        completed_source.id,
        run.id,
        SourceParseReportState.SUCCEEDED,
        content_revision_id=revision.id,
    )
    unfinished_report = _report(
        dataset.id,
        running_source.id,
        run.id,
        SourceParseReportState.RUNNING,
    )
    store.save_source_parse_report(completed_report)
    store.save_source_parse_report(unfinished_report)

    coordinator = ProcessingRunCoordinator(store, lambda *_: {"done": True})
    recovered = store.get_run_for_worker(run.id)
    assert recovered is not None
    assert recovered.state == ProcessingRunState.INTERRUPTED
    reports = {
        item.source_revision_id: item
        for item in store.list_source_parse_reports_for_worker(dataset.id, run.id)
    }
    assert reports[completed_source.id].status == SourceParseReportState.SUCCEEDED
    assert reports[running_source.id].status == SourceParseReportState.INTERRUPTED
    assert reports[running_source.id].finished_at is not None

    coordinator.shutdown()
    store.close()
    catalyst.close()
