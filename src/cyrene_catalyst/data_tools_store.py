"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 data_tools_store.py                                             │
│  Module: cyrene_catalyst.data_tools_store                           │
│  Role: SQLite persistence for sources, parse review, and runs.       │
│                                                                     │
│  模块职责：持久化来源、解析审核、内容修订与处理运行，不创建 Dataset。   │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from threading import RLock
from uuid import UUID

from cyrene_catalyst.data_tools_domain import (
    Annotation,
    ContentRevision,
    ContentRevisionState,
    ProcessingProgress,
    ProcessingRun,
    ProcessingRunState,
    ProcessingStageState,
    ReviewItem,
    ReviewItemResolution,
    ReviewItemState,
    SourceParseReport,
    SourceParseReportState,
    SourceRevision,
)
from cyrene_catalyst.domain import utc_now
from cyrene_catalyst.workspace_auth import WorkspaceServicePrincipal


class DataToolsStore:
    """Persist Data Tools resources under the existing Catalyst Dataset authority."""

    def __init__(self, database_path: Path) -> None:
        database_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(database_path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._lock = RLock()
        with self._connection:
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS data_tool_source_revisions (
                    id TEXT PRIMARY KEY,
                    dataset_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    document TEXT NOT NULL,
                    UNIQUE(dataset_id, source_id, revision),
                    FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_data_tool_sources_dataset
                    ON data_tool_source_revisions(dataset_id, revision DESC);

                CREATE TABLE IF NOT EXISTS data_tool_content_revisions (
                    id TEXT PRIMARY KEY,
                    dataset_id TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    document TEXT NOT NULL,
                    UNIQUE(dataset_id, revision),
                    FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_data_tool_content_dataset
                    ON data_tool_content_revisions(dataset_id, revision DESC);

                CREATE TABLE IF NOT EXISTS data_tool_annotations (
                    id TEXT PRIMARY KEY,
                    dataset_id TEXT NOT NULL,
                    dataset_version_id TEXT NOT NULL,
                    content_revision_id TEXT NOT NULL,
                    document TEXT NOT NULL,
                    FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
                    FOREIGN KEY(dataset_version_id)
                        REFERENCES dataset_versions(id) ON DELETE CASCADE,
                    FOREIGN KEY(content_revision_id)
                        REFERENCES data_tool_content_revisions(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_data_tool_annotations_version
                    ON data_tool_annotations(dataset_version_id, id);

                CREATE TABLE IF NOT EXISTS data_tool_processing_runs (
                    id TEXT PRIMARY KEY,
                    dataset_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    recipe_digest TEXT NOT NULL,
                    document TEXT NOT NULL,
                    FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_data_tool_runs_dataset
                    ON data_tool_processing_runs(dataset_id, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_data_tool_runs_active
                    ON data_tool_processing_runs(state, updated_at);

                CREATE TABLE IF NOT EXISTS data_tool_stage_cache (
                    dataset_id TEXT NOT NULL,
                    stage_key TEXT NOT NULL,
                    input_digest TEXT NOT NULL,
                    recipe_digest TEXT NOT NULL,
                    output TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(dataset_id, stage_key, input_digest, recipe_digest),
                    FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
                );

                -- Composite parent keys prevent a child receipt from mixing
                -- one Dataset with another Dataset's source, revision, or run.
                CREATE UNIQUE INDEX IF NOT EXISTS idx_data_tool_sources_dataset_id
                    ON data_tool_source_revisions(dataset_id, id);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_data_tool_content_dataset_id
                    ON data_tool_content_revisions(dataset_id, id);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_data_tool_runs_dataset_id
                    ON data_tool_processing_runs(dataset_id, id);

                CREATE TABLE IF NOT EXISTS data_tool_source_parse_reports (
                    id TEXT PRIMARY KEY,
                    dataset_id TEXT NOT NULL,
                    source_revision_id TEXT NOT NULL,
                    processing_run_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    document TEXT NOT NULL,
                    UNIQUE(processing_run_id, source_revision_id),
                    UNIQUE(dataset_id, id),
                    UNIQUE(dataset_id, id, source_revision_id),
                    FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
                    FOREIGN KEY(dataset_id, source_revision_id)
                        REFERENCES data_tool_source_revisions(dataset_id, id) ON DELETE CASCADE,
                    FOREIGN KEY(dataset_id, processing_run_id)
                        REFERENCES data_tool_processing_runs(dataset_id, id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_data_tool_parse_reports_dataset
                    ON data_tool_source_parse_reports(dataset_id, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_data_tool_parse_reports_source
                    ON data_tool_source_parse_reports(
                        dataset_id, source_revision_id, updated_at DESC
                    );
                CREATE INDEX IF NOT EXISTS idx_data_tool_parse_reports_run
                    ON data_tool_source_parse_reports(dataset_id, processing_run_id);

                CREATE TABLE IF NOT EXISTS data_tool_review_items (
                    id TEXT PRIMARY KEY,
                    dataset_id TEXT NOT NULL,
                    source_parse_report_id TEXT NOT NULL,
                    source_revision_id TEXT NOT NULL,
                    processing_run_id TEXT NOT NULL,
                    content_revision_id TEXT,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    document TEXT NOT NULL,
                    UNIQUE(dataset_id, id),
                    FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
                    FOREIGN KEY(dataset_id, source_parse_report_id, source_revision_id)
                        REFERENCES data_tool_source_parse_reports(
                            dataset_id, id, source_revision_id
                        )
                        ON DELETE CASCADE,
                    FOREIGN KEY(dataset_id, source_revision_id)
                        REFERENCES data_tool_source_revisions(dataset_id, id) ON DELETE CASCADE,
                    FOREIGN KEY(dataset_id, processing_run_id)
                        REFERENCES data_tool_processing_runs(dataset_id, id) ON DELETE CASCADE,
                    FOREIGN KEY(dataset_id, content_revision_id)
                        REFERENCES data_tool_content_revisions(dataset_id, id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_data_tool_review_items_revision
                    ON data_tool_review_items(dataset_id, content_revision_id, state, created_at);
                CREATE INDEX IF NOT EXISTS idx_data_tool_review_items_queue
                    ON data_tool_review_items(dataset_id, state, created_at);
                """
            )

    def close(self) -> None:
        """Close the SQLite connection. | 关闭 SQLite 连接。"""

        with self._lock:
            self._connection.close()

    @staticmethod
    def _scope(principal: WorkspaceServicePrincipal | None) -> tuple[str, tuple[str, ...]]:
        """Return dataset ownership SQL and its parameters. | 获取 Dataset 范围条件。"""

        if principal is None:
            return "d.organization_id IS NULL AND d.workspace_id IS NULL", ()
        return (
            "d.organization_id = ? AND d.workspace_id = ?",
            (principal.organization_id, principal.workspace_id),
        )

    def _require_dataset(
        self,
        dataset_id: UUID,
        principal: WorkspaceServicePrincipal | None,
    ) -> None:
        """Reject absent or out-of-scope parent datasets. | 校验父 Dataset 及访问范围。"""

        predicate, parameters = self._scope(principal)
        row = self._connection.execute(
            f"SELECT 1 FROM datasets AS d WHERE d.id = ? AND {predicate}",
            (str(dataset_id), *parameters),
        ).fetchone()
        if row is None:
            raise LookupError("Dataset was not found in the caller's scope.")

    def _require_dataset_for_worker(self, dataset_id: UUID) -> None:
        """Validate a parent without tenant filtering for trusted background work."""

        row = self._connection.execute(
            "SELECT 1 FROM datasets WHERE id = ?",
            (str(dataset_id),),
        ).fetchone()
        if row is None:
            raise LookupError("Dataset was not found.")

    def create_source(
        self,
        source: SourceRevision,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> SourceRevision:
        """Insert an immutable source revision. | 插入不可变来源版本。"""

        document = source.model_dump_json(by_alias=True, exclude_none=True)
        with self._lock, self._connection:
            self._require_dataset(source.dataset_id, principal)
            self._connection.execute(
                "INSERT INTO data_tool_source_revisions"
                "(id,dataset_id,source_id,revision,document) VALUES (?,?,?,?,?)",
                (
                    str(source.id),
                    str(source.dataset_id),
                    str(source.source_id),
                    source.revision,
                    document,
                ),
            )
        return source

    def get_source(
        self,
        source_revision_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> SourceRevision | None:
        """Read a source revision through its Dataset scope. | 按父 Dataset 范围读取来源版本。"""

        predicate, parameters = self._scope(principal)
        with self._lock:
            row = self._connection.execute(
                "SELECT r.document FROM data_tool_source_revisions AS r "
                "JOIN datasets AS d ON d.id = r.dataset_id "
                f"WHERE r.id = ? AND {predicate}",
                (str(source_revision_id), *parameters),
            ).fetchone()
        return SourceRevision.model_validate_json(row["document"]) if row else None

    def get_source_for_worker(self, source_revision_id: UUID) -> SourceRevision | None:
        """Read a source revision for trusted background execution. | 后台执行读取来源。"""

        with self._lock:
            row = self._connection.execute(
                "SELECT r.document FROM data_tool_source_revisions AS r "
                "JOIN datasets AS d ON d.id = r.dataset_id WHERE r.id = ?",
                (str(source_revision_id),),
            ).fetchone()
        return SourceRevision.model_validate_json(row["document"]) if row else None

    def list_sources(
        self,
        dataset_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> list[SourceRevision]:
        """List revisions newest-first within one Dataset. | 按新到旧列出来源版本。"""

        predicate, parameters = self._scope(principal)
        with self._lock:
            rows = self._connection.execute(
                "SELECT r.document FROM data_tool_source_revisions AS r "
                "JOIN datasets AS d ON d.id = r.dataset_id "
                f"WHERE r.dataset_id = ? AND {predicate} ORDER BY r.rowid DESC",
                (str(dataset_id), *parameters),
            ).fetchall()
        return [SourceRevision.model_validate_json(row["document"]) for row in rows]

    def list_sources_for_worker(self, dataset_id: UUID) -> list[SourceRevision]:
        """List source revisions for trusted background snapshot building."""

        with self._lock:
            rows = self._connection.execute(
                "SELECT r.document FROM data_tool_source_revisions AS r "
                "JOIN datasets AS d ON d.id = r.dataset_id "
                "WHERE r.dataset_id = ? ORDER BY r.rowid DESC",
                (str(dataset_id),),
            ).fetchall()
        return [SourceRevision.model_validate_json(row["document"]) for row in rows]

    @staticmethod
    def _report_transitions() -> dict[SourceParseReportState, set[SourceParseReportState]]:
        """Return allowed monotonic parse-report transitions. | 解析报告状态转移。"""

        terminal = {
            SourceParseReportState.SUCCEEDED,
            SourceParseReportState.WARNING,
            SourceParseReportState.FAILED,
            SourceParseReportState.INTERRUPTED,
            SourceParseReportState.CANCELLED,
        }
        return {
            SourceParseReportState.QUEUED: {
                SourceParseReportState.QUEUED,
                SourceParseReportState.RUNNING,
                *terminal,
            },
            SourceParseReportState.RUNNING: {
                SourceParseReportState.RUNNING,
                *terminal,
            },
            **{state: {state} for state in terminal},
        }

    def _validate_report_links(self, report: SourceParseReport) -> None:
        """Validate same-Dataset source, run, and optional revision links."""

        source = self._connection.execute(
            "SELECT dataset_id FROM data_tool_source_revisions WHERE id = ?",
            (str(report.source_revision_id),),
        ).fetchone()
        run_row = self._connection.execute(
            "SELECT dataset_id, document FROM data_tool_processing_runs WHERE id = ?",
            (str(report.processing_run_id),),
        ).fetchone()
        if (
            source is None
            or source["dataset_id"] != str(report.dataset_id)
            or run_row is None
            or run_row["dataset_id"] != str(report.dataset_id)
        ):
            raise ValueError("SourceParseReport source and run must belong to its Dataset.")
        run = ProcessingRun.model_validate_json(run_row["document"])
        if report.source_revision_id not in run.source_revision_ids:
            raise ValueError("SourceParseReport source must be selected by its ProcessingRun.")
        if report.content_revision_id is not None:
            revision_row = self._connection.execute(
                "SELECT dataset_id, document FROM data_tool_content_revisions WHERE id = ?",
                (str(report.content_revision_id),),
            ).fetchone()
            if revision_row is None or revision_row["dataset_id"] != str(report.dataset_id):
                raise ValueError("SourceParseReport ContentRevision must belong to its Dataset.")
            revision = ContentRevision.model_validate_json(revision_row["document"])
            if report.source_revision_id not in revision.source_revision_ids:
                raise ValueError(
                    "SourceParseReport source must be included in its ContentRevision."
                )

    def save_source_parse_report(
        self,
        report: SourceParseReport,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> SourceParseReport:
        """Insert or advance one source's report within the caller's scope."""

        return self._save_source_parse_report(report, principal, trusted_worker=False)

    def save_source_parse_report_for_worker(
        self,
        report: SourceParseReport,
    ) -> SourceParseReport:
        """Persist one source report from a trusted parser worker."""

        return self._save_source_parse_report(report, None, trusted_worker=True)

    def _save_source_parse_report(
        self,
        report: SourceParseReport,
        principal: WorkspaceServicePrincipal | None,
        *,
        trusted_worker: bool,
    ) -> SourceParseReport:
        """Upsert by the immutable run/source pair and enforce lifecycle order."""

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                saved = self._save_source_parse_report_locked(
                    report,
                    principal,
                    trusted_worker=trusted_worker,
                )
                self._connection.commit()
                return saved
            except Exception:
                self._connection.rollback()
                raise

    def _save_source_parse_report_locked(
        self,
        report: SourceParseReport,
        principal: WorkspaceServicePrincipal | None,
        *,
        trusted_worker: bool,
    ) -> SourceParseReport:
        """Upsert one report while the caller owns a write transaction."""

        if trusted_worker:
            self._require_dataset_for_worker(report.dataset_id)
        else:
            self._require_dataset(report.dataset_id, principal)
        self._validate_report_links(report)
        row = self._connection.execute(
            "SELECT id, document FROM data_tool_source_parse_reports "
            "WHERE processing_run_id = ? AND source_revision_id = ?",
            (str(report.processing_run_id), str(report.source_revision_id)),
        ).fetchone()
        if row is None:
            self._connection.execute(
                "INSERT INTO data_tool_source_parse_reports "
                "(id,dataset_id,source_revision_id,processing_run_id,status,"
                "created_at,updated_at,document) VALUES (?,?,?,?,?,?,?,?)",
                (
                    str(report.id),
                    str(report.dataset_id),
                    str(report.source_revision_id),
                    str(report.processing_run_id),
                    report.status.value,
                    report.created_at.isoformat(),
                    report.updated_at.isoformat(),
                    report.model_dump_json(by_alias=True, exclude_none=True),
                ),
            )
            return report

        current = SourceParseReport.model_validate_json(row["document"])
        if current.id != report.id or current.dataset_id != report.dataset_id:
            raise ValueError("SourceParseReport identity cannot change for one run/source pair.")
        if current.created_at != report.created_at:
            raise ValueError("SourceParseReport createdAt is immutable.")
        if report.updated_at < current.updated_at:
            raise ValueError("SourceParseReport updatedAt cannot move backwards.")
        allowed = self._report_transitions()[current.status]
        if report.status not in allowed:
            raise ValueError(
                f"SourceParseReport cannot transition from {current.status.value} "
                f"to {report.status.value}."
            )
        if (
            current.content_revision_id is not None
            and report.content_revision_id != current.content_revision_id
        ):
            raise ValueError("SourceParseReport ContentRevision link is immutable once set.")
        terminal = {
            SourceParseReportState.SUCCEEDED,
            SourceParseReportState.WARNING,
            SourceParseReportState.FAILED,
            SourceParseReportState.INTERRUPTED,
            SourceParseReportState.CANCELLED,
        }
        if current.status in terminal and report.status == current.status:
            expected = current.model_copy(
                update={
                    "content_revision_id": report.content_revision_id,
                    "updated_at": report.updated_at,
                    "resource_version": report.resource_version,
                }
            )
            if report != expected:
                raise ValueError("Terminal SourceParseReport details are immutable.")
        if report == current:
            return current
        saved = report.model_copy(update={"resource_version": current.resource_version + 1})
        self._connection.execute(
            "UPDATE data_tool_source_parse_reports SET status=?,updated_at=?,document=? "
            "WHERE id=? AND dataset_id=?",
            (
                saved.status.value,
                saved.updated_at.isoformat(),
                saved.model_dump_json(by_alias=True, exclude_none=True),
                str(saved.id),
                str(saved.dataset_id),
            ),
        )
        return saved

    def get_source_parse_report(
        self,
        report_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> SourceParseReport | None:
        """Read a report through its parent Dataset ownership. | 按父 Dataset 范围读取报告。"""

        predicate, parameters = self._scope(principal)
        with self._lock:
            row = self._connection.execute(
                "SELECT r.document FROM data_tool_source_parse_reports AS r "
                "JOIN datasets AS d ON d.id = r.dataset_id "
                f"WHERE r.id = ? AND {predicate}",
                (str(report_id), *parameters),
            ).fetchone()
        return SourceParseReport.model_validate_json(row["document"]) if row else None

    def get_source_parse_report_for_worker(
        self,
        report_id: UUID,
    ) -> SourceParseReport | None:
        """Read a report for trusted asynchronous recovery."""

        with self._lock:
            row = self._connection.execute(
                "SELECT r.document FROM data_tool_source_parse_reports AS r "
                "JOIN datasets AS d ON d.id = r.dataset_id WHERE r.id = ?",
                (str(report_id),),
            ).fetchone()
        return SourceParseReport.model_validate_json(row["document"]) if row else None

    def list_source_parse_reports(
        self,
        dataset_id: UUID,
        source_revision_id: UUID | None = None,
        processing_run_id: UUID | None = None,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> list[SourceParseReport]:
        """List scoped per-source reports, optionally filtered by source or run."""

        predicate, parameters = self._scope(principal)
        filters = ["r.dataset_id = ?", predicate]
        values: list[str] = [str(dataset_id), *parameters]
        if source_revision_id is not None:
            filters.append("r.source_revision_id = ?")
            values.append(str(source_revision_id))
        if processing_run_id is not None:
            filters.append("r.processing_run_id = ?")
            values.append(str(processing_run_id))
        with self._lock:
            rows = self._connection.execute(
                "SELECT r.document FROM data_tool_source_parse_reports AS r "
                "JOIN datasets AS d ON d.id = r.dataset_id WHERE "
                + " AND ".join(filters)
                + " ORDER BY r.created_at DESC, r.id",
                values,
            ).fetchall()
        return [SourceParseReport.model_validate_json(row["document"]) for row in rows]

    def list_source_parse_reports_for_worker(
        self,
        dataset_id: UUID,
        run_id: UUID | None = None,
    ) -> list[SourceParseReport]:
        """List reports for trusted batch processing or recovery."""

        with self._lock:
            self._require_dataset_for_worker(dataset_id)
            if run_id is None:
                rows = self._connection.execute(
                    "SELECT document FROM data_tool_source_parse_reports "
                    "WHERE dataset_id = ? ORDER BY created_at DESC, id",
                    (str(dataset_id),),
                ).fetchall()
            else:
                rows = self._connection.execute(
                    "SELECT document FROM data_tool_source_parse_reports "
                    "WHERE dataset_id = ? AND processing_run_id = ? "
                    "ORDER BY created_at DESC, id",
                    (str(dataset_id), str(run_id)),
                ).fetchall()
        return [SourceParseReport.model_validate_json(row["document"]) for row in rows]

    def mark_parse_reports_interrupted_for_worker(
        self,
        run_id: UUID,
        at: datetime | None = None,
    ) -> list[SourceParseReport]:
        """Mark unfinished per-source work interrupted after its run stops."""

        run = self.get_run_for_worker(run_id)
        if run is None:
            return []
        timestamp = at or utc_now()
        active = {
            SourceParseReportState.QUEUED,
            SourceParseReportState.RUNNING,
        }
        updated: list[SourceParseReport] = []
        for report in self.list_source_parse_reports_for_worker(run.dataset_id, run_id):
            if report.status not in active:
                continue
            updated.append(
                self.save_source_parse_report_for_worker(
                    report.model_copy(
                        update={
                            "status": SourceParseReportState.INTERRUPTED,
                            "updated_at": timestamp,
                            "finished_at": timestamp,
                            "resource_version": report.resource_version + 1,
                        }
                    )
                )
            )
        return updated

    def _validate_review_item_links(self, item: ReviewItem) -> None:
        """Validate that an issue matches its report and optional revision."""

        report_row = self._connection.execute(
            "SELECT dataset_id, document FROM data_tool_source_parse_reports WHERE id = ?",
            (str(item.source_parse_report_id),),
        ).fetchone()
        if report_row is None or report_row["dataset_id"] != str(item.dataset_id):
            raise ValueError("ReviewItem report must belong to its Dataset.")
        report = SourceParseReport.model_validate_json(report_row["document"])
        if (
            report.source_revision_id != item.source_revision_id
            or report.processing_run_id != item.processing_run_id
            or report.content_revision_id != item.content_revision_id
        ):
            raise ValueError("ReviewItem lineage must match its SourceParseReport.")
        if item.content_revision_id is not None:
            revision_row = self._connection.execute(
                "SELECT dataset_id, document FROM data_tool_content_revisions WHERE id = ?",
                (str(item.content_revision_id),),
            ).fetchone()
            if revision_row is None or revision_row["dataset_id"] != str(item.dataset_id):
                raise ValueError("ReviewItem ContentRevision must belong to its Dataset.")
            revision = ContentRevision.model_validate_json(revision_row["document"])
            if item.source_revision_id not in revision.source_revision_ids:
                raise ValueError("ReviewItem source must be included in its ContentRevision.")
            if revision.state == ContentRevisionState.APPROVED:
                raise ValueError("ReviewItems cannot be added to an approved ContentRevision.")

    def save_review_item(
        self,
        item: ReviewItem,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ReviewItem:
        """Insert an open issue idempotently within the caller's Dataset scope."""

        return self._save_review_item(item, principal, trusted_worker=False)

    def save_review_item_for_worker(self, item: ReviewItem) -> ReviewItem:
        """Persist one issue from a trusted parser worker."""

        return self._save_review_item(item, None, trusted_worker=True)

    def _save_review_item(
        self,
        item: ReviewItem,
        principal: WorkspaceServicePrincipal | None,
        *,
        trusted_worker: bool,
    ) -> ReviewItem:
        """Insert or return an identical issue, rejecting identity rewrites."""

        if item.state != ReviewItemState.OPEN or item.resolved_at is not None:
            raise ValueError("New ReviewItems must start OPEN.")
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                saved = self._save_review_item_locked(
                    item,
                    principal,
                    trusted_worker=trusted_worker,
                )
                self._connection.commit()
                return saved
            except Exception:
                self._connection.rollback()
                raise

    def _save_review_item_locked(
        self,
        item: ReviewItem,
        principal: WorkspaceServicePrincipal | None,
        *,
        trusted_worker: bool,
    ) -> ReviewItem:
        """Insert one issue while the caller owns a write transaction."""

        if item.state != ReviewItemState.OPEN or item.resolved_at is not None:
            raise ValueError("New ReviewItems must start OPEN.")
        if trusted_worker:
            self._require_dataset_for_worker(item.dataset_id)
        else:
            self._require_dataset(item.dataset_id, principal)
        self._validate_review_item_links(item)
        row = self._connection.execute(
            "SELECT document FROM data_tool_review_items WHERE id = ?",
            (str(item.id),),
        ).fetchone()
        if row is not None:
            current = ReviewItem.model_validate_json(row["document"])
            if current != item:
                raise ValueError("ReviewItem is immutable after creation; use resolve_review_item.")
            return current
        self._connection.execute(
            "INSERT INTO data_tool_review_items "
            "(id,dataset_id,source_parse_report_id,source_revision_id,processing_run_id,"
            "content_revision_id,state,created_at,document) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                str(item.id),
                str(item.dataset_id),
                str(item.source_parse_report_id),
                str(item.source_revision_id),
                str(item.processing_run_id),
                str(item.content_revision_id) if item.content_revision_id else None,
                item.state.value,
                item.created_at.isoformat(),
                item.model_dump_json(by_alias=True, exclude_none=True),
            ),
        )
        return item

    def get_review_item(
        self,
        item_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ReviewItem | None:
        """Read one issue through its parent Dataset ownership. | 按范围读取审核项。"""

        predicate, parameters = self._scope(principal)
        with self._lock:
            row = self._connection.execute(
                "SELECT i.document FROM data_tool_review_items AS i "
                "JOIN datasets AS d ON d.id = i.dataset_id "
                f"WHERE i.id = ? AND {predicate}",
                (str(item_id), *parameters),
            ).fetchone()
        return ReviewItem.model_validate_json(row["document"]) if row else None

    def get_review_item_for_worker(self, item_id: UUID) -> ReviewItem | None:
        """Read one issue for trusted background work."""

        with self._lock:
            row = self._connection.execute(
                "SELECT i.document FROM data_tool_review_items AS i "
                "JOIN datasets AS d ON d.id = i.dataset_id WHERE i.id = ?",
                (str(item_id),),
            ).fetchone()
        return ReviewItem.model_validate_json(row["document"]) if row else None

    def list_review_items(
        self,
        dataset_id: UUID,
        state: ReviewItemState | None = None,
        source_revision_id: UUID | None = None,
        content_revision_id: UUID | None = None,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> list[ReviewItem]:
        """List review issues filtered by Dataset, state, source, or revision."""

        predicate, parameters = self._scope(principal)
        filters = ["i.dataset_id = ?", predicate]
        values: list[str] = [str(dataset_id), *parameters]
        if state is not None:
            filters.append("i.state = ?")
            values.append(state.value)
        if source_revision_id is not None:
            filters.append("i.source_revision_id = ?")
            values.append(str(source_revision_id))
        if content_revision_id is not None:
            filters.append("i.content_revision_id = ?")
            values.append(str(content_revision_id))
        with self._lock:
            rows = self._connection.execute(
                "SELECT i.document FROM data_tool_review_items AS i "
                "JOIN datasets AS d ON d.id = i.dataset_id WHERE "
                + " AND ".join(filters)
                + " ORDER BY i.created_at, i.id",
                values,
            ).fetchall()
        return [ReviewItem.model_validate_json(row["document"]) for row in rows]

    def list_review_items_for_worker(
        self,
        dataset_id: UUID,
        run_id: UUID | None = None,
    ) -> list[ReviewItem]:
        """List review issues for trusted batch execution or recovery."""

        with self._lock:
            self._require_dataset_for_worker(dataset_id)
            if run_id is None:
                rows = self._connection.execute(
                    "SELECT document FROM data_tool_review_items "
                    "WHERE dataset_id = ? ORDER BY created_at, id",
                    (str(dataset_id),),
                ).fetchall()
            else:
                rows = self._connection.execute(
                    "SELECT document FROM data_tool_review_items "
                    "WHERE dataset_id = ? AND processing_run_id = ? ORDER BY created_at, id",
                    (str(dataset_id), str(run_id)),
                ).fetchall()
        return [ReviewItem.model_validate_json(row["document"]) for row in rows]

    def resolve_review_item(
        self,
        item_id: UUID,
        action: ReviewItemResolution,
        note: str | None = None,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ReviewItem:
        """Apply one immutable human disposition to an open review issue."""

        return self._resolve_review_item(
            item_id,
            action,
            note,
            principal,
            trusted_worker=False,
        )

    def resolve_review_item_for_worker(
        self,
        item_id: UUID,
        action: ReviewItemResolution,
        note: str | None = None,
    ) -> ReviewItem:
        """Apply a disposition from a trusted background workflow."""

        return self._resolve_review_item(item_id, action, note, None, trusted_worker=True)

    def _resolve_review_item(
        self,
        item_id: UUID,
        action: ReviewItemResolution,
        note: str | None,
        principal: WorkspaceServicePrincipal | None,
        *,
        trusted_worker: bool,
    ) -> ReviewItem:
        """Resolve under one SQLite write lock with tenant scope enforced."""

        predicate, parameters = self._scope(principal)
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                condition = "i.id = ?"
                values: tuple[str, ...] = (str(item_id),)
                if not trusted_worker:
                    condition += f" AND {predicate}"
                    values = (*values, *parameters)
                row = self._connection.execute(
                    "SELECT i.document FROM data_tool_review_items AS i "
                    "JOIN datasets AS d ON d.id = i.dataset_id WHERE " + condition,
                    values,
                ).fetchone()
                if row is None:
                    raise LookupError("ReviewItem was not found in the caller's scope.")
                current = ReviewItem.model_validate_json(row["document"])
                desired = (
                    ReviewItemState.ACKNOWLEDGED
                    if action == ReviewItemResolution.ACKNOWLEDGE
                    else ReviewItemState.REJECTED
                )
                if current.state == desired:
                    self._connection.commit()
                    return current
                if current.state != ReviewItemState.OPEN:
                    raise ValueError("A resolved ReviewItem cannot change its disposition.")
                updated = current.model_copy(
                    update={
                        "state": desired,
                        "note": note,
                        "resolved_at": utc_now(),
                        "resource_version": current.resource_version + 1,
                    }
                )
                self._connection.execute(
                    "UPDATE data_tool_review_items SET state=?,document=? WHERE id=?",
                    (
                        updated.state.value,
                        updated.model_dump_json(by_alias=True, exclude_none=True),
                        str(item_id),
                    ),
                )
                self._connection.commit()
                return updated
            except Exception:
                self._connection.rollback()
                raise

    def _has_unresolved_review_items_locked(
        self,
        dataset_id: UUID,
        content_revision_id: UUID,
    ) -> bool:
        """Check nearest source lineage issues while holding the write transaction."""

        rows = self._connection.execute(
            "SELECT id,document FROM data_tool_content_revisions WHERE dataset_id = ?",
            (str(dataset_id),),
        ).fetchall()
        revisions = {
            UUID(row["id"]): ContentRevision.model_validate_json(row["document"]) for row in rows
        }
        current = revisions.get(content_revision_id)
        if current is None:
            return True

        lineage: list[ContentRevision] = []
        seen: set[UUID] = set()
        cursor: ContentRevision | None = current
        while cursor is not None:
            if cursor.id in seen:
                return True
            seen.add(cursor.id)
            lineage.append(cursor)
            if cursor.parent_revision_id is None:
                break
            parent = revisions.get(cursor.parent_revision_id)
            if parent is None:
                return True
            cursor = parent

        report_rows = self._connection.execute(
            "SELECT document FROM data_tool_source_parse_reports WHERE dataset_id = ?",
            (str(dataset_id),),
        ).fetchall()
        reports = [SourceParseReport.model_validate_json(row["document"]) for row in report_rows]
        depth_by_revision = {revision.id: depth for depth, revision in enumerate(lineage)}
        for source_id in current.source_revision_ids:
            linked = [
                (report, depth_by_revision[report.content_revision_id])
                for report in reports
                if report.source_revision_id == source_id
                and report.content_revision_id is not None
                and report.content_revision_id in depth_by_revision
            ]
            nearest_depth = min(
                (depth for _, depth in linked),
                default=None,
            )
            nearest_reports = [report for report, depth in linked if depth == nearest_depth]
            nearest = max(
                nearest_reports,
                key=lambda report: (report.created_at, report.updated_at, str(report.id)),
                default=None,
            )
            if nearest is not None and self._report_has_unresolved_issues_locked(nearest):
                return True

            # A newer queued/running/warning report may be between creation of
            # the nearest applicable revision and the current child. It has no
            # revision link yet, so fail closed until its parse outcome is bound.
            if nearest_depth is not None:
                anchor = lineage[nearest_depth]
                anchor_created_at = anchor.created_at
            else:
                prior_source_revision = next(
                    (
                        revision
                        for revision in lineage[1:]
                        if source_id in revision.source_revision_ids
                    ),
                    None,
                )
                anchor_created_at = (
                    prior_source_revision.created_at
                    if prior_source_revision is not None
                    else datetime.min.replace(tzinfo=current.created_at.tzinfo)
                )
            unbound = [
                report
                for report in reports
                if report.source_revision_id == source_id
                and report.content_revision_id is None
                and report.created_at > anchor_created_at
                and report.created_at <= current.created_at
                and report.status
                in {
                    SourceParseReportState.QUEUED,
                    SourceParseReportState.RUNNING,
                    SourceParseReportState.WARNING,
                }
            ]
            if unbound:
                newest = max(
                    unbound,
                    key=lambda report: (report.created_at, report.updated_at, str(report.id)),
                )
                if newest.status != SourceParseReportState.WARNING or (
                    self._report_has_unresolved_issues_locked(newest)
                ):
                    return True

        return False

    def _report_has_unresolved_issues_locked(self, report: SourceParseReport) -> bool:
        """Fail closed for a warning receipt missing queue projections or acknowledgements."""

        expected = self._expected_review_item_count(report)
        rows = self._connection.execute(
            "SELECT state FROM data_tool_review_items WHERE dataset_id = ? "
            "AND source_parse_report_id = ?",
            (str(report.dataset_id), str(report.id)),
        ).fetchall()
        if report.status == SourceParseReportState.WARNING and (
            expected == 0 or len(rows) != expected
        ):
            return True
        return any(row["state"] != ReviewItemState.ACKNOWLEDGED.value for row in rows)

    def has_unresolved_review_items(
        self,
        content_revision_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> bool:
        """Return whether OPEN or REJECTED issues still block approval."""

        predicate, parameters = self._scope(principal)
        with self._lock:
            row = self._connection.execute(
                "SELECT r.dataset_id FROM data_tool_content_revisions AS r "
                "JOIN datasets AS d ON d.id = r.dataset_id "
                f"WHERE r.id = ? AND {predicate}",
                (str(content_revision_id), *parameters),
            ).fetchone()
            if row is None:
                raise LookupError("ContentRevision was not found in the caller's scope.")
            return self._has_unresolved_review_items_locked(
                UUID(row["dataset_id"]), content_revision_id
            )

    def has_unresolved_review_items_for_worker(self, content_revision_id: UUID) -> bool:
        """Return approval blockers for a trusted Product worker."""

        with self._lock:
            row = self._connection.execute(
                "SELECT dataset_id FROM data_tool_content_revisions WHERE id = ?",
                (str(content_revision_id),),
            ).fetchone()
            if row is None:
                raise LookupError("ContentRevision was not found.")
            return self._has_unresolved_review_items_locked(
                UUID(row["dataset_id"]), content_revision_id
            )

    def create_content_revision(
        self,
        revision: ContentRevision,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ContentRevision:
        """Insert an immutable content snapshot after validating its lineage."""

        return self._create_content_revision(revision, principal, trusted_worker=False)

    def create_content_revision_for_worker(
        self,
        revision: ContentRevision,
    ) -> ContentRevision:
        """Insert an immutable snapshot from a trusted asynchronous Product worker."""

        return self._create_content_revision(revision, None, trusted_worker=True)

    def _create_content_revision(
        self,
        revision: ContentRevision,
        principal: WorkspaceServicePrincipal | None,
        *,
        trusted_worker: bool,
    ) -> ContentRevision:
        """Share snapshot validation while keeping trusted access explicit."""

        document = revision.model_dump_json(by_alias=True, exclude_none=True)
        with self._lock, self._connection:
            if trusted_worker:
                self._require_dataset_for_worker(revision.dataset_id)
            else:
                self._require_dataset(revision.dataset_id, principal)
            self._insert_content_revision_locked(revision, document)
        return revision

    def _insert_content_revision_locked(
        self,
        revision: ContentRevision,
        document: str | None = None,
    ) -> None:
        """Insert a revision after validating all same-Dataset lineage links."""

        if revision.parent_revision_id is not None:
            parent = self._connection.execute(
                "SELECT dataset_id FROM data_tool_content_revisions WHERE id = ?",
                (str(revision.parent_revision_id),),
            ).fetchone()
            if parent is None or parent["dataset_id"] != str(revision.dataset_id):
                raise ValueError("ContentRevision parent must belong to the same Dataset.")
        for source_id in revision.source_revision_ids:
            row = self._connection.execute(
                "SELECT dataset_id FROM data_tool_source_revisions WHERE id = ?",
                (str(source_id),),
            ).fetchone()
            if row is None or row["dataset_id"] != str(revision.dataset_id):
                raise ValueError("ContentRevision sources must belong to the same Dataset.")
        allowed_sources = {str(value) for value in revision.source_revision_ids}
        if any(str(block.source_revision_id) not in allowed_sources for block in revision.blocks):
            raise ValueError("Every block must reference one of the revision's sources.")
        snapshot = document or revision.model_dump_json(by_alias=True, exclude_none=True)
        self._connection.execute(
            "INSERT INTO data_tool_content_revisions(id,dataset_id,revision,document) "
            "VALUES (?,?,?,?)",
            (str(revision.id), str(revision.dataset_id), revision.revision, snapshot),
        )

    def finalize_parsed_content_revision_for_worker(
        self,
        revision: ContentRevision,
        reports: list[SourceParseReport],
        review_items: list[ReviewItem],
    ) -> ContentRevision:
        """Atomically publish parsed blocks, report links, and their review issues."""

        if revision.state != ContentRevisionState.DRAFT:
            raise ValueError("Parsed ContentRevisions must start in DRAFT.")
        if not reports:
            raise ValueError("A parsed ContentRevision must include source reports.")
        source_ids = set(revision.source_revision_ids)
        report_ids: set[UUID] = set()
        linked_reports: list[SourceParseReport] = []
        for report in reports:
            if report.dataset_id != revision.dataset_id:
                raise ValueError("Parse reports and ContentRevision must share a Dataset.")
            if report.source_revision_id not in source_ids:
                raise ValueError("Every finalized parse report source must be in the revision.")
            if report.status not in {
                SourceParseReportState.SUCCEEDED,
                SourceParseReportState.WARNING,
            }:
                raise ValueError("Only successful parse reports can be finalized with a revision.")
            if report.id in report_ids:
                raise ValueError("A parse report may only be finalized once per revision.")
            report_ids.add(report.id)
            linked_reports.append(
                report.model_copy(
                    update={
                        "content_revision_id": revision.id,
                        "updated_at": max(report.updated_at, utc_now()),
                    }
                )
            )

        items_by_report: dict[UUID, list[ReviewItem]] = {report_id: [] for report_id in report_ids}
        for item in review_items:
            if (
                item.dataset_id != revision.dataset_id
                or item.content_revision_id != revision.id
                or item.source_parse_report_id not in report_ids
                or item.source_revision_id not in source_ids
            ):
                raise ValueError("ReviewItems must link to this parsed revision and its reports.")
            items_by_report[item.source_parse_report_id].append(item)
        report_by_id = {report.id: report for report in linked_reports}
        for report_id, report in report_by_id.items():
            expected = self._expected_review_item_count(report)
            actual = len(items_by_report[report_id])
            if report.status == SourceParseReportState.WARNING and actual != expected:
                raise ValueError(
                    "Warning parse reports must materialize every review item atomically."
                )
            if report.status == SourceParseReportState.SUCCEEDED and actual != 0:
                raise ValueError("Successful parse reports cannot contain review items.")

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                self._require_dataset_for_worker(revision.dataset_id)
                self._insert_content_revision_locked(revision)
                saved_reports: dict[UUID, SourceParseReport] = {}
                for report in linked_reports:
                    saved = self._save_source_parse_report_locked(
                        report,
                        None,
                        trusted_worker=True,
                    )
                    saved_reports[saved.id] = saved
                for item in review_items:
                    saved_report = saved_reports[item.source_parse_report_id]
                    if item.source_revision_id != saved_report.source_revision_id:
                        raise ValueError("ReviewItem source must match its report.")
                    self._save_review_item_locked(item, None, trusted_worker=True)
                self._connection.commit()
                return revision
            except Exception:
                self._connection.rollback()
                raise

    @staticmethod
    def _expected_review_item_count(report: SourceParseReport) -> int:
        """Count queue projections using the same rules as DataToolsService."""

        diagnostic_messages: set[str] = set()
        count = 0
        for diagnostic in report.diagnostics:
            code = diagnostic.get("code")
            message = diagnostic.get("message")
            if isinstance(code, str) and isinstance(message, str):
                count += 1
                diagnostic_messages.add(message)
        count += sum(warning.message not in diagnostic_messages for warning in report.warnings)
        count += len(report.unsupported_content)
        return count

    def get_content_revision(
        self,
        revision_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ContentRevision | None:
        """Read one immutable content snapshot through Dataset ownership. | 读取内容修订。"""

        predicate, parameters = self._scope(principal)
        with self._lock:
            row = self._connection.execute(
                "SELECT r.document FROM data_tool_content_revisions AS r "
                "JOIN datasets AS d ON d.id = r.dataset_id "
                f"WHERE r.id = ? AND {predicate}",
                (str(revision_id), *parameters),
            ).fetchone()
        return ContentRevision.model_validate_json(row["document"]) if row else None

    def get_content_revision_for_worker(self, revision_id: UUID) -> ContentRevision | None:
        """Read a content snapshot for trusted background execution. | 后台读取内容修订。"""

        with self._lock:
            row = self._connection.execute(
                "SELECT r.document FROM data_tool_content_revisions AS r "
                "JOIN datasets AS d ON d.id = r.dataset_id WHERE r.id = ?",
                (str(revision_id),),
            ).fetchone()
        return ContentRevision.model_validate_json(row["document"]) if row else None

    def list_content_revisions(
        self,
        dataset_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> list[ContentRevision]:
        """List immutable content revisions newest-first. | 按新到旧列出内容修订。"""

        predicate, parameters = self._scope(principal)
        with self._lock:
            rows = self._connection.execute(
                "SELECT r.document FROM data_tool_content_revisions AS r "
                "JOIN datasets AS d ON d.id = r.dataset_id "
                f"WHERE r.dataset_id = ? AND {predicate} ORDER BY r.revision DESC",
                (str(dataset_id), *parameters),
            ).fetchall()
        return [ContentRevision.model_validate_json(row["document"]) for row in rows]

    def list_content_revisions_for_worker(self, dataset_id: UUID) -> list[ContentRevision]:
        """List revisions for trusted parsing/review orchestration. | 后台读取内容修订。"""

        with self._lock:
            rows = self._connection.execute(
                "SELECT r.document FROM data_tool_content_revisions AS r "
                "JOIN datasets AS d ON d.id = r.dataset_id "
                "WHERE r.dataset_id = ? ORDER BY r.revision DESC",
                (str(dataset_id),),
            ).fetchall()
        return [ContentRevision.model_validate_json(row["document"]) for row in rows]

    def update_content_review(
        self,
        revision_id: UUID,
        decision: ContentRevisionState,
        note: str | None = None,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ContentRevision:
        """Update review metadata while preserving immutable block contents."""

        from cyrene_catalyst.domain import utc_now

        predicate, parameters = self._scope(principal)
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._connection.execute(
                    "SELECT r.document FROM data_tool_content_revisions AS r "
                    "JOIN datasets AS d ON d.id = r.dataset_id "
                    f"WHERE r.id = ? AND {predicate}",
                    (str(revision_id), *parameters),
                ).fetchone()
                if row is None:
                    raise LookupError("ContentRevision was not found in the caller's scope.")
                current = ContentRevision.model_validate_json(row["document"])
                if (
                    decision == ContentRevisionState.APPROVED
                    and self._has_unresolved_review_items_locked(current.dataset_id, current.id)
                ):
                    raise ValueError(
                        "ContentRevision cannot be approved while parser or OCR review items "
                        "are OPEN or REJECTED."
                    )
                updated = current.model_copy(
                    update={
                        "state": decision,
                        "reviewed_at": utc_now(),
                        "review_note": note,
                        "resource_version": current.resource_version + 1,
                    }
                )
                self._connection.execute(
                    "UPDATE data_tool_content_revisions SET document = ? WHERE id = ?",
                    (updated.model_dump_json(by_alias=True, exclude_none=True), str(revision_id)),
                )
                self._connection.commit()
                return updated
            except Exception:
                self._connection.rollback()
                raise

    def save_annotation(
        self,
        annotation: Annotation,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> Annotation:
        """Insert feedback bound to a DatasetVersion and its content revision."""

        document = annotation.model_dump_json(by_alias=True, exclude_none=True)
        predicate, parameters = self._scope(principal)
        with self._lock, self._connection:
            self._require_dataset(annotation.dataset_id, principal)
            row = self._connection.execute(
                "SELECT v.dataset_id FROM dataset_versions AS v "
                "JOIN datasets AS d ON d.id = v.dataset_id "
                f"WHERE v.id = ? AND {predicate}",
                (str(annotation.dataset_version_id), *parameters),
            ).fetchone()
            content = self._connection.execute(
                "SELECT dataset_id, document FROM data_tool_content_revisions WHERE id = ?",
                (str(annotation.content_revision_id),),
            ).fetchone()
            if (
                row is None
                or row["dataset_id"] != str(annotation.dataset_id)
                or content is None
                or content["dataset_id"] != str(annotation.dataset_id)
            ):
                raise ValueError("Annotation version and content must belong to its Dataset.")
            revision = ContentRevision.model_validate_json(content["document"])
            if annotation.block_id not in {block.id for block in revision.blocks}:
                raise ValueError("Annotation block does not exist in the referenced revision.")
            self._connection.execute(
                "INSERT INTO data_tool_annotations"
                "(id,dataset_id,dataset_version_id,content_revision_id,document) "
                "VALUES (?,?,?,?,?)",
                (
                    str(annotation.id),
                    str(annotation.dataset_id),
                    str(annotation.dataset_version_id),
                    str(annotation.content_revision_id),
                    document,
                ),
            )
        return annotation

    def list_annotations(
        self,
        dataset_version_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> list[Annotation]:
        """List feedback bound to one visible DatasetVersion. | 按版本列出标注。"""

        predicate, parameters = self._scope(principal)
        with self._lock:
            rows = self._connection.execute(
                "SELECT a.document FROM data_tool_annotations AS a "
                "JOIN datasets AS d ON d.id = a.dataset_id "
                f"WHERE a.dataset_version_id = ? AND {predicate} ORDER BY a.rowid",
                (str(dataset_version_id), *parameters),
            ).fetchall()
        return [Annotation.model_validate_json(row["document"]) for row in rows]

    def create_run(
        self,
        run: ProcessingRun,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ProcessingRun:
        """Insert a durable ProcessingRun before its worker is queued."""

        document = run.model_dump_json(by_alias=True, exclude_none=True)
        with self._lock, self._connection:
            self._require_dataset(run.dataset_id, principal)
            for source_id in run.source_revision_ids:
                row = self._connection.execute(
                    "SELECT dataset_id FROM data_tool_source_revisions WHERE id = ?",
                    (str(source_id),),
                ).fetchone()
                if row is None or row["dataset_id"] != str(run.dataset_id):
                    raise ValueError("ProcessingRun sources must belong to the same Dataset.")
            if run.content_revision_id is not None:
                row = self._connection.execute(
                    "SELECT dataset_id FROM data_tool_content_revisions WHERE id = ?",
                    (str(run.content_revision_id),),
                ).fetchone()
                if row is None or row["dataset_id"] != str(run.dataset_id):
                    raise ValueError("ProcessingRun content must belong to the same Dataset.")
            self._connection.execute(
                "INSERT INTO data_tool_processing_runs"
                "(id,dataset_id,state,updated_at,recipe_digest,document) VALUES (?,?,?,?,?,?)",
                (
                    str(run.id),
                    str(run.dataset_id),
                    run.state.value,
                    run.updated_at.isoformat(),
                    run.recipe_digest,
                    document,
                ),
            )
        return run

    def get_run(
        self,
        run_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ProcessingRun | None:
        """Read a run through its Dataset ownership. | 按父 Dataset 范围读取运行。"""

        predicate, parameters = self._scope(principal)
        with self._lock:
            row = self._connection.execute(
                "SELECT r.document FROM data_tool_processing_runs AS r "
                "JOIN datasets AS d ON d.id = r.dataset_id "
                f"WHERE r.id = ? AND {predicate}",
                (str(run_id), *parameters),
            ).fetchone()
        return ProcessingRun.model_validate_json(row["document"]) if row else None

    def get_run_for_worker(self, run_id: UUID) -> ProcessingRun | None:
        """Read one run for trusted asynchronous execution/recovery. | 后台读取处理运行。"""

        with self._lock:
            row = self._connection.execute(
                "SELECT r.document FROM data_tool_processing_runs AS r "
                "JOIN datasets AS d ON d.id = r.dataset_id WHERE r.id = ?",
                (str(run_id),),
            ).fetchone()
        return ProcessingRun.model_validate_json(row["document"]) if row else None

    def list_runs(
        self,
        dataset_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> list[ProcessingRun]:
        """List a Dataset's runs newest-first. | 按新到旧列出处理运行。"""

        predicate, parameters = self._scope(principal)
        with self._lock:
            rows = self._connection.execute(
                "SELECT r.document FROM data_tool_processing_runs AS r "
                "JOIN datasets AS d ON d.id = r.dataset_id "
                f"WHERE r.dataset_id = ? AND {predicate} ORDER BY r.updated_at DESC",
                (str(dataset_id), *parameters),
            ).fetchall()
        return [ProcessingRun.model_validate_json(row["document"]) for row in rows]

    def list_active_runs(self) -> list[ProcessingRun]:
        """Return queued/running records for worker restart recovery. | 启动恢复投影。"""

        with self._lock:
            rows = self._connection.execute(
                "SELECT document FROM data_tool_processing_runs "
                "WHERE state IN (?, ?) ORDER BY updated_at",
                (
                    ProcessingRunState.QUEUED.value,
                    ProcessingRunState.RUNNING.value,
                ),
            ).fetchall()
        return [ProcessingRun.model_validate_json(row["document"]) for row in rows]

    def save_run(
        self,
        run: ProcessingRun,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ProcessingRun:
        """Persist a lifecycle update for an existing run. | 保存运行状态更新。"""

        document = run.model_dump_json(by_alias=True, exclude_none=True)
        with self._lock, self._connection:
            self._require_dataset(run.dataset_id, principal)
            cursor = self._connection.execute(
                "UPDATE data_tool_processing_runs SET state=?, updated_at=?, "
                "recipe_digest=?, document=? WHERE id=? AND dataset_id=?",
                (
                    run.state.value,
                    run.updated_at.isoformat(),
                    run.recipe_digest,
                    document,
                    str(run.id),
                    str(run.dataset_id),
                ),
            )
            if cursor.rowcount != 1:
                raise LookupError("ProcessingRun was not found in the caller's scope.")
        return run

    def save_run_for_worker(self, run: ProcessingRun) -> ProcessingRun:
        """Persist a run from the trusted worker without weakening API scope checks."""

        document = run.model_dump_json(by_alias=True, exclude_none=True)
        with self._lock, self._connection:
            self._require_dataset_for_worker(run.dataset_id)
            cursor = self._connection.execute(
                "UPDATE data_tool_processing_runs SET state=?, updated_at=?, "
                "recipe_digest=?, document=? WHERE id=? AND dataset_id=?",
                (
                    run.state.value,
                    run.updated_at.isoformat(),
                    run.recipe_digest,
                    document,
                    str(run.id),
                    str(run.dataset_id),
                ),
            )
            if cursor.rowcount != 1:
                raise LookupError("ProcessingRun was not found.")
        return run

    def request_cancel(
        self,
        run_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ProcessingRun:
        """Atomically persist cancellation unless the run already finished."""

        predicate, parameters = self._scope(principal)
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._connection.execute(
                    "SELECT r.document FROM data_tool_processing_runs AS r "
                    "JOIN datasets AS d ON d.id = r.dataset_id "
                    f"WHERE r.id = ? AND {predicate}",
                    (str(run_id), *parameters),
                ).fetchone()
                if row is None:
                    raise LookupError("ProcessingRun was not found in the caller's scope.")
                current = ProcessingRun.model_validate_json(row["document"])
                if current.state in {
                    ProcessingRunState.SUCCEEDED,
                    ProcessingRunState.FAILED,
                    ProcessingRunState.CANCELLED,
                    ProcessingRunState.INTERRUPTED,
                }:
                    self._connection.commit()
                    return current
                if current.cancellation_requested:
                    self._connection.commit()
                    return current

                now = utc_now()
                stages = current.stages
                state: ProcessingRunState = current.state
                finished_at = current.finished_at
                if current.state == ProcessingRunState.QUEUED:
                    stages = [
                        stage.model_copy(
                            update={
                                "state": ProcessingStageState.CANCELLED,
                                "finished_at": now,
                            }
                        )
                        if stage.state == ProcessingStageState.PENDING
                        else stage
                        for stage in current.stages
                    ]
                    state = ProcessingRunState.CANCELLED
                    finished_at = now
                updated = current.model_copy(
                    update={
                        "state": state,
                        "cancellation_requested": True,
                        "stages": stages,
                        "progress": ProcessingProgress(
                            completed=sum(
                                stage.state
                                in {ProcessingStageState.COMPLETED, ProcessingStageState.REUSED}
                                for stage in stages
                            ),
                            total=len(stages),
                        ),
                        "updated_at": now,
                        "finished_at": finished_at,
                        "resource_version": current.resource_version + 1,
                    }
                )
                self._connection.execute(
                    "UPDATE data_tool_processing_runs SET state=?, updated_at=?, document=? "
                    "WHERE id=?",
                    (
                        updated.state.value,
                        now.isoformat(),
                        updated.model_dump_json(by_alias=True, exclude_none=True),
                        str(run_id),
                    ),
                )
                self._connection.commit()
                return updated
            except Exception:
                self._connection.rollback()
                raise

    def finish_run_for_worker(self, run: ProcessingRun) -> ProcessingRun:
        """Atomically publish success only if cancellation did not win the race."""

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._connection.execute(
                    "SELECT document FROM data_tool_processing_runs WHERE id=? AND dataset_id=?",
                    (str(run.id), str(run.dataset_id)),
                ).fetchone()
                if row is None:
                    raise LookupError("ProcessingRun was not found.")
                current = ProcessingRun.model_validate_json(row["document"])
                if current.cancellation_requested:
                    self._connection.commit()
                    return current
                now = utc_now()
                completed = run.model_copy(
                    update={
                        "cancellation_requested": False,
                        "updated_at": now,
                        "resource_version": max(run.resource_version, current.resource_version) + 1,
                    }
                )
                self._connection.execute(
                    "UPDATE data_tool_processing_runs SET state=?, updated_at=?, document=? "
                    "WHERE id=? AND dataset_id=?",
                    (
                        completed.state.value,
                        now.isoformat(),
                        completed.model_dump_json(by_alias=True, exclude_none=True),
                        str(completed.id),
                        str(completed.dataset_id),
                    ),
                )
                self._connection.commit()
                return completed
            except Exception:
                self._connection.rollback()
                raise

    def find_reusable_stage(
        self,
        dataset_id: UUID,
        stage_key: str,
        input_digest: str,
        recipe_digest: str,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> dict[str, object] | None:
        """Find a deterministic successful stage with the same immutable inputs."""

        with self._lock:
            self._require_dataset(dataset_id, principal)
            row = self._connection.execute(
                "SELECT output FROM data_tool_stage_cache WHERE dataset_id=? AND stage_key=? "
                "AND input_digest=? AND recipe_digest=?",
                (str(dataset_id), stage_key, input_digest, recipe_digest),
            ).fetchone()
        return json.loads(row["output"]) if row else None

    def find_reusable_stage_for_worker(
        self,
        dataset_id: UUID,
        stage_key: str,
        input_digest: str,
        recipe_digest: str,
    ) -> dict[str, object] | None:
        """Find cached output during trusted worker execution."""

        with self._lock:
            self._require_dataset_for_worker(dataset_id)
            row = self._connection.execute(
                "SELECT output FROM data_tool_stage_cache WHERE dataset_id=? AND stage_key=? "
                "AND input_digest=? AND recipe_digest=?",
                (str(dataset_id), stage_key, input_digest, recipe_digest),
            ).fetchone()
        return json.loads(row["output"]) if row else None

    def save_stage_cache(
        self,
        dataset_id: UUID,
        stage_key: str,
        input_digest: str,
        recipe_digest: str,
        output: dict[str, object],
        created_at: str,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> None:
        """Cache only completed deterministic stage receipts. | 缓存确定性阶段结果。"""

        serialized = json.dumps(output, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        with self._lock, self._connection:
            self._require_dataset(dataset_id, principal)
            self._connection.execute(
                "INSERT OR REPLACE INTO data_tool_stage_cache"
                "(dataset_id,stage_key,input_digest,recipe_digest,output,created_at) "
                "VALUES (?,?,?,?,?,?)",
                (str(dataset_id), stage_key, input_digest, recipe_digest, serialized, created_at),
            )

    def save_stage_cache_for_worker(
        self,
        dataset_id: UUID,
        stage_key: str,
        input_digest: str,
        recipe_digest: str,
        output: dict[str, object],
        created_at: str,
    ) -> None:
        """Cache deterministic output from trusted worker execution."""

        serialized = json.dumps(output, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        with self._lock, self._connection:
            self._require_dataset_for_worker(dataset_id)
            self._connection.execute(
                "INSERT OR REPLACE INTO data_tool_stage_cache"
                "(dataset_id,stage_key,input_digest,recipe_digest,output,created_at) "
                "VALUES (?,?,?,?,?,?)",
                (str(dataset_id), stage_key, input_digest, recipe_digest, serialized, created_at),
            )
