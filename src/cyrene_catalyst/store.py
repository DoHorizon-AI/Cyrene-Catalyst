"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 store.py                                                        │
│  Module: cyrene_catalyst.store                                      │
│  Role: SQLite Product state authority and idempotency ledger.       │
│                                                                     │
│  模块职责：持久化产品资源与幂等账本，不代理 Kernel 状态。                  │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from threading import RLock
from uuid import UUID

from cyrene_catalyst.domain import Dataset, DatasetVersion, Preparation
from cyrene_catalyst.errors import CatalystError
from cyrene_catalyst.workspace_auth import WorkspaceServicePrincipal


class CatalystStore:
    """Durable Product store; never derives state from an engine. | 产品状态权威存储。"""

    def __init__(self, database_path: Path) -> None:
        database_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(database_path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._lock = RLock()
        with self._connection:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS datasets (
                    id TEXT PRIMARY KEY,
                    document TEXT NOT NULL,
                    organization_id TEXT,
                    workspace_id TEXT
                );
                CREATE TABLE IF NOT EXISTS dataset_versions (
                    id TEXT PRIMARY KEY,
                    dataset_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    document TEXT NOT NULL,
                    UNIQUE(dataset_id, version),
                    FOREIGN KEY(dataset_id) REFERENCES datasets(id)
                );
                CREATE TABLE IF NOT EXISTS preparations (
                    id TEXT PRIMARY KEY,
                    dataset_id TEXT NOT NULL,
                    document TEXT NOT NULL,
                    FOREIGN KEY(dataset_id) REFERENCES datasets(id)
                );
                CREATE TABLE IF NOT EXISTS idempotency (
                    scope TEXT NOT NULL,
                    organization_id TEXT NOT NULL,
                    workspace_id TEXT NOT NULL,
                    key TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    resource_kind TEXT NOT NULL,
                    resource_id TEXT NOT NULL,
                    PRIMARY KEY(scope, organization_id, workspace_id, key)
                );
                """
            )
        self._migrate_workspace_scope_schema()

    def _migrate_workspace_scope_schema(self) -> None:
        """Add Workspace ownership without guessing owners for existing rows."""

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                dataset_columns = {
                    str(row["name"])
                    for row in self._connection.execute("PRAGMA table_info(datasets)")
                }
                if "organization_id" not in dataset_columns:
                    self._connection.execute("ALTER TABLE datasets ADD COLUMN organization_id TEXT")
                if "workspace_id" not in dataset_columns:
                    self._connection.execute("ALTER TABLE datasets ADD COLUMN workspace_id TEXT")

                idempotency_info = list(self._connection.execute("PRAGMA table_info(idempotency)"))
                idempotency_columns = {str(row["name"]) for row in idempotency_info}
                primary_key = [
                    str(row["name"])
                    for row in sorted(idempotency_info, key=lambda item: int(item["pk"]))
                    if int(row["pk"]) > 0
                ]
                expected_primary_key = ["scope", "organization_id", "workspace_id", "key"]
                if (
                    not {"organization_id", "workspace_id"}.issubset(idempotency_columns)
                    or primary_key != expected_primary_key
                ):
                    organization_expr = (
                        "organization_id" if "organization_id" in idempotency_columns else "''"
                    )
                    workspace_expr = (
                        "workspace_id" if "workspace_id" in idempotency_columns else "''"
                    )
                    self._connection.execute(
                        """
                        CREATE TABLE idempotency_workspace_new (
                            scope TEXT NOT NULL,
                            organization_id TEXT NOT NULL,
                            workspace_id TEXT NOT NULL,
                            key TEXT NOT NULL,
                            request_hash TEXT NOT NULL,
                            resource_kind TEXT NOT NULL,
                            resource_id TEXT NOT NULL,
                            PRIMARY KEY(scope, organization_id, workspace_id, key)
                        )
                        """
                    )
                    migration_sql = f"""
                        INSERT INTO idempotency_workspace_new(
                            scope, organization_id, workspace_id, key,
                            request_hash, resource_kind, resource_id
                        )
                        SELECT scope, {organization_expr}, {workspace_expr}, key,
                               request_hash, resource_kind, resource_id
                        FROM idempotency
                        """
                    self._connection.execute(migration_sql)
                    self._connection.execute("DROP TABLE idempotency")
                    self._connection.execute(
                        "ALTER TABLE idempotency_workspace_new RENAME TO idempotency"
                    )

                self._connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_datasets_workspace_scope "
                    "ON datasets(organization_id, workspace_id)"
                )
                self._connection.execute("PRAGMA user_version = 1")
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise

    def close(self) -> None:
        """Close the SQLite connection. | 关闭 SQLite 连接。"""

        with self._lock:
            self._connection.close()

    def save_dataset(self, dataset: Dataset) -> None:
        """Upsert a Dataset document. | 写入 Dataset 文档。"""

        document = dataset.model_dump_json(by_alias=True, exclude_none=True)
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT INTO datasets(id, document, organization_id, workspace_id) "
                "VALUES (?, ?, NULL, NULL)",
                (str(dataset.id), document),
            )

    def create_workspace_dataset(
        self,
        dataset: Dataset,
        principal: WorkspaceServicePrincipal,
        idempotency_key: str | None,
        request_hash: str,
    ) -> Dataset:
        """Atomically persist one scoped Dataset and its scoped replay record."""

        document = dataset.model_dump_json(by_alias=True, exclude_none=True)
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                if idempotency_key is not None:
                    replay = self._connection.execute(
                        "SELECT request_hash, resource_id FROM idempotency "
                        "WHERE scope = 'create-dataset' AND organization_id = ? "
                        "AND workspace_id = ? AND key = ?",
                        (principal.organization_id, principal.workspace_id, idempotency_key),
                    ).fetchone()
                    if replay is not None:
                        if replay["request_hash"] != request_hash:
                            raise CatalystError(
                                code="CATALYST_IDEMPOTENCY_CONFLICT",
                                title="Idempotency key conflict",
                                detail=(
                                    "The Idempotency-Key was already used with a different "
                                    "request body."
                                ),
                                status=409,
                            )
                        scoped_dataset = self._connection.execute(
                            "SELECT document FROM datasets WHERE id = ? AND organization_id = ? "
                            "AND workspace_id = ?",
                            (
                                str(replay["resource_id"]),
                                principal.organization_id,
                                principal.workspace_id,
                            ),
                        ).fetchone()
                        if scoped_dataset is None:
                            raise CatalystError(
                                code="CATALYST_STATE_CORRUPT",
                                title="Product state is inconsistent",
                                detail="The idempotency ledger references a missing Dataset.",
                                status=500,
                            )
                        result = Dataset.model_validate_json(scoped_dataset["document"])
                        self._connection.commit()
                        return result

                self._connection.execute(
                    "INSERT INTO datasets(id, document, organization_id, workspace_id) "
                    "VALUES (?, ?, ?, ?)",
                    (
                        str(dataset.id),
                        document,
                        principal.organization_id,
                        principal.workspace_id,
                    ),
                )
                if idempotency_key is not None:
                    self._connection.execute(
                        "INSERT INTO idempotency("
                        "scope, organization_id, workspace_id, key, request_hash, "
                        "resource_kind, resource_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            "create-dataset",
                            principal.organization_id,
                            principal.workspace_id,
                            idempotency_key,
                            request_hash,
                            "dataset",
                            str(dataset.id),
                        ),
                    )
                self._connection.commit()
                return dataset
            except Exception:
                self._connection.rollback()
                raise

    def get_dataset(
        self,
        dataset_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> Dataset | None:
        """Read a Dataset by opaque id. | 按不透明 ID 读取 Dataset。"""

        with self._lock:
            if principal is None:
                row = self._connection.execute(
                    "SELECT document FROM datasets WHERE id = ? "
                    "AND organization_id IS NULL AND workspace_id IS NULL",
                    (str(dataset_id),),
                ).fetchone()
            else:
                row = self._connection.execute(
                    "SELECT document FROM datasets WHERE id = ? AND organization_id = ? "
                    "AND workspace_id = ?",
                    (str(dataset_id), principal.organization_id, principal.workspace_id),
                ).fetchone()
        return Dataset.model_validate_json(row["document"]) if row else None

    def list_datasets(self, principal: WorkspaceServicePrincipal | None = None) -> list[Dataset]:
        """List Datasets in insertion order. | 按插入顺序列出 Dataset。"""

        with self._lock:
            if principal is None:
                rows = self._connection.execute(
                    "SELECT document FROM datasets WHERE organization_id IS NULL "
                    "AND workspace_id IS NULL ORDER BY rowid"
                ).fetchall()
            else:
                rows = self._connection.execute(
                    "SELECT document FROM datasets WHERE organization_id = ? "
                    "AND workspace_id = ? ORDER BY rowid",
                    (principal.organization_id, principal.workspace_id),
                ).fetchall()
        return [Dataset.model_validate_json(row["document"]) for row in rows]

    def save_preparation(self, preparation: Preparation) -> None:
        """Upsert a Preparation document. | 写入 Preparation 文档。"""

        document = preparation.model_dump_json(by_alias=True, exclude_none=True)
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT OR REPLACE INTO preparations(id, dataset_id, document) VALUES (?, ?, ?)",
                (str(preparation.id), str(preparation.dataset_id), document),
            )

    def get_preparation(
        self,
        preparation_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> Preparation | None:
        """Read a Preparation by id inside its owning Dataset scope."""

        with self._lock:
            if principal is None:
                row = self._connection.execute(
                    "SELECT p.document FROM preparations AS p "
                    "JOIN datasets AS d ON d.id = p.dataset_id "
                    "WHERE p.id = ? AND d.organization_id IS NULL AND d.workspace_id IS NULL",
                    (str(preparation_id),),
                ).fetchone()
            else:
                row = self._connection.execute(
                    "SELECT p.document FROM preparations AS p "
                    "JOIN datasets AS d ON d.id = p.dataset_id "
                    "WHERE p.id = ? AND d.organization_id = ? AND d.workspace_id = ?",
                    (str(preparation_id), principal.organization_id, principal.workspace_id),
                ).fetchone()
        return Preparation.model_validate_json(row["document"]) if row else None

    def list_preparations(
        self,
        dataset_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> list[Preparation]:
        """List Preparations inside the owning Dataset scope in insertion order."""

        with self._lock:
            if principal is None:
                rows = self._connection.execute(
                    "SELECT p.document FROM preparations AS p "
                    "JOIN datasets AS d ON d.id = p.dataset_id "
                    "WHERE p.dataset_id = ? AND d.organization_id IS NULL "
                    "AND d.workspace_id IS NULL ORDER BY p.rowid",
                    (str(dataset_id),),
                ).fetchall()
            else:
                rows = self._connection.execute(
                    "SELECT p.document FROM preparations AS p "
                    "JOIN datasets AS d ON d.id = p.dataset_id "
                    "WHERE p.dataset_id = ? AND d.organization_id = ? "
                    "AND d.workspace_id = ? ORDER BY p.rowid",
                    (str(dataset_id), principal.organization_id, principal.workspace_id),
                ).fetchall()
        return [Preparation.model_validate_json(row["document"]) for row in rows]

    def next_version(self, dataset_id: UUID) -> int:
        """Allocate the next per-Dataset version number. | 分配下一版本号。"""

        with self._lock:
            row = self._connection.execute(
                "SELECT COALESCE(MAX(version), 0) + 1 AS value "
                "FROM dataset_versions WHERE dataset_id = ?",
                (str(dataset_id),),
            ).fetchone()
        return int(row["value"])

    def save_version(self, version: DatasetVersion) -> None:
        """Upsert a DatasetVersion document. | 写入 DatasetVersion 文档。"""

        document = version.model_dump_json(by_alias=True, exclude_none=True)
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT OR REPLACE INTO dataset_versions(id, dataset_id, version, document)
                VALUES (?, ?, ?, ?)
                """,
                (str(version.id), str(version.dataset_id), version.version, document),
            )

    def get_version(
        self,
        version_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> DatasetVersion | None:
        """Read a persisted version inside the owning Dataset scope.

        中文:按唯一 Dataset authority 读取版本；提供 principal 时限制到该 Workspace。
        """

        with self._lock:
            if principal is None:
                row = self._connection.execute(
                    "SELECT v.document FROM dataset_versions AS v "
                    "JOIN datasets AS d ON d.id = v.dataset_id "
                    "WHERE v.id = ? AND d.organization_id IS NULL AND d.workspace_id IS NULL",
                    (str(version_id),),
                ).fetchone()
            else:
                row = self._connection.execute(
                    "SELECT v.document FROM dataset_versions AS v "
                    "JOIN datasets AS d ON d.id = v.dataset_id "
                    "WHERE v.id = ? AND d.organization_id = ? AND d.workspace_id = ?",
                    (str(version_id), principal.organization_id, principal.workspace_id),
                ).fetchone()
        return DatasetVersion.model_validate_json(row["document"]) if row else None

    def list_versions(
        self,
        dataset_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> list[DatasetVersion]:
        """List DatasetVersions newest-first inside the owning Dataset scope.

        中文:按新到旧列出版本，并在提供 principal 时校验 Workspace 所有权。
        """

        with self._lock:
            if principal is None:
                rows = self._connection.execute(
                    "SELECT v.document FROM dataset_versions AS v "
                    "JOIN datasets AS d ON d.id = v.dataset_id "
                    "WHERE v.dataset_id = ? AND d.organization_id IS NULL "
                    "AND d.workspace_id IS NULL ORDER BY v.version DESC",
                    (str(dataset_id),),
                ).fetchall()
            else:
                rows = self._connection.execute(
                    "SELECT v.document FROM dataset_versions AS v "
                    "JOIN datasets AS d ON d.id = v.dataset_id "
                    "WHERE v.dataset_id = ? AND d.organization_id = ? "
                    "AND d.workspace_id = ? ORDER BY v.version DESC",
                    (str(dataset_id), principal.organization_id, principal.workspace_id),
                ).fetchall()
        return [DatasetVersion.model_validate_json(row["document"]) for row in rows]

    def list_active_activity_tasks(self) -> list[dict[str, str]]:
        """Return processing DatasetVersions for startup gate reconciliation.

        Product states remain authoritative; the runtime gate receives only a
        separate RUNNING activity projection.
        """

        with self._lock:
            rows = self._connection.execute(
                "SELECT document FROM dataset_versions ORDER BY rowid"
            ).fetchall()
        versions = [DatasetVersion.model_validate_json(row["document"]) for row in rows]
        return [
            {"task_id": str(version.id), "state": "RUNNING"}
            for version in versions
            if version.state.value == "PROCESSING"
        ]

    def resolve_idempotency(
        self,
        scope: str,
        key: str | None,
        request_hash: str,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> str | None:
        """Return a replayed resource id or reject conflicting key reuse. | 解析幂等重放。"""

        if key is None:
            return None
        organization_id = principal.organization_id if principal else ""
        workspace_id = principal.workspace_id if principal else ""
        with self._lock:
            row = self._connection.execute(
                "SELECT request_hash, resource_id FROM idempotency WHERE scope = ? "
                "AND organization_id = ? AND workspace_id = ? AND key = ?",
                (scope, organization_id, workspace_id, key),
            ).fetchone()
        if row is None:
            return None
        if row["request_hash"] != request_hash:
            raise CatalystError(
                code="CATALYST_IDEMPOTENCY_CONFLICT",
                title="Idempotency key conflict",
                detail="The Idempotency-Key was already used with a different request body.",
                status=409,
            )
        return str(row["resource_id"])

    def remember_idempotency(
        self,
        *,
        scope: str,
        key: str | None,
        request_hash: str,
        resource_kind: str,
        resource_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> None:
        """Persist a command-to-resource idempotency mapping. | 持久化命令资源幂等映射。"""

        if key is None:
            return
        organization_id = principal.organization_id if principal else ""
        workspace_id = principal.workspace_id if principal else ""
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO idempotency(
                    scope, organization_id, workspace_id, key, request_hash,
                    resource_kind, resource_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    scope,
                    organization_id,
                    workspace_id,
                    key,
                    request_hash,
                    resource_kind,
                    str(resource_id),
                ),
            )
