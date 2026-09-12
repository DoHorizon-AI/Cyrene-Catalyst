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
                    document TEXT NOT NULL
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
                    key TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    resource_kind TEXT NOT NULL,
                    resource_id TEXT NOT NULL,
                    PRIMARY KEY(scope, key)
                );
                """
            )

    def close(self) -> None:
        """Close the SQLite connection. | 关闭 SQLite 连接。"""

        with self._lock:
            self._connection.close()

    def save_dataset(self, dataset: Dataset) -> None:
        """Upsert a Dataset document. | 写入 Dataset 文档。"""

        document = dataset.model_dump_json(by_alias=True, exclude_none=True)
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT OR REPLACE INTO datasets(id, document) VALUES (?, ?)",
                (str(dataset.id), document),
            )

    def get_dataset(self, dataset_id: UUID) -> Dataset | None:
        """Read a Dataset by opaque id. | 按不透明 ID 读取 Dataset。"""

        with self._lock:
            row = self._connection.execute(
                "SELECT document FROM datasets WHERE id = ?", (str(dataset_id),)
            ).fetchone()
        return Dataset.model_validate_json(row["document"]) if row else None

    def list_datasets(self) -> list[Dataset]:
        """List Datasets in insertion order. | 按插入顺序列出 Dataset。"""

        with self._lock:
            rows = self._connection.execute(
                "SELECT document FROM datasets ORDER BY rowid"
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

    def get_preparation(self, preparation_id: UUID) -> Preparation | None:
        """Read a Preparation by opaque id. | 按不透明 ID 读取 Preparation。"""

        with self._lock:
            row = self._connection.execute(
                "SELECT document FROM preparations WHERE id = ?", (str(preparation_id),)
            ).fetchone()
        return Preparation.model_validate_json(row["document"]) if row else None

    def list_preparations(self, dataset_id: UUID) -> list[Preparation]:
        """List Preparations of one Dataset in insertion order. | 列出 Dataset 的整理会话。"""

        with self._lock:
            rows = self._connection.execute(
                "SELECT document FROM preparations WHERE dataset_id = ? ORDER BY rowid",
                (str(dataset_id),),
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

    def get_version(self, version_id: UUID) -> DatasetVersion | None:
        """Read a persisted success or failure. | 读取持久化成功或失败。"""

        with self._lock:
            row = self._connection.execute(
                "SELECT document FROM dataset_versions WHERE id = ?", (str(version_id),)
            ).fetchone()
        return DatasetVersion.model_validate_json(row["document"]) if row else None

    def resolve_idempotency(self, scope: str, key: str | None, request_hash: str) -> str | None:
        """Return a replayed resource id or reject conflicting key reuse. | 解析幂等重放。"""

        if key is None:
            return None
        with self._lock:
            row = self._connection.execute(
                "SELECT request_hash, resource_id FROM idempotency WHERE scope = ? AND key = ?",
                (scope, key),
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
    ) -> None:
        """Persist a command-to-resource idempotency mapping. | 持久化命令资源幂等映射。"""

        if key is None:
            return
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO idempotency(scope, key, request_hash, resource_kind, resource_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                (scope, key, request_hash, resource_kind, str(resource_id)),
            )
