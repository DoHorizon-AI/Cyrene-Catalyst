"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 data_tools_store.py                                             │
│  Module: cyrene_catalyst.data_tools_store                           │
│  Role: SQLite persistence for Data Tools child resources and runs.  │
│                                                                     │
│  模块职责：持久化来源、内容修订、版本标注与处理运行，不创建 Dataset。   │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import json
import sqlite3
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
            if any(
                str(block.source_revision_id) not in allowed_sources for block in revision.blocks
            ):
                raise ValueError("Every block must reference one of the revision's sources.")
            self._connection.execute(
                "INSERT INTO data_tool_content_revisions(id,dataset_id,revision,document) "
                "VALUES (?,?,?,?)",
                (str(revision.id), str(revision.dataset_id), revision.revision, document),
            )
        return revision

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
