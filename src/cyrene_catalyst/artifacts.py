"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 artifacts.py                                                    │
│  Module: cyrene_catalyst.artifacts                                  │
│  Role: Replaceable Artifact Plane adapter used by the local MVP.     │
│                                                                     │
│  模块职责：本地 MVP 的可替换 Artifact Plane 适配器。                     │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

from cyrene_catalyst.domain import ArtifactRef
from cyrene_catalyst.errors import CatalystError


def sha256_file(path: Path) -> str:
    """Return the canonical sha256 digest string. | 返回规范 sha256 摘要。"""

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


class LocalArtifactPlane:
    """Filesystem adapter; the Product stores only returned references. | 本地制品适配器。"""

    def __init__(self, root: Path) -> None:
        self.root = root
        self._objects = root / "sha256"
        self._staging = root / ".staging"
        self._objects.mkdir(parents=True, exist_ok=True)
        self._staging.mkdir(parents=True, exist_ok=True)

    def stage_path(self, name: str) -> Path:
        """Return a private staging path for an engine write. | 返回引擎暂存路径。"""

        return self._staging / name

    def stage_dir(self, name: str) -> Path:
        """Return a private staging directory for a bundle write. | 返回导出包暂存目录。"""

        directory = self._staging / name
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def ingest_bytes(self, data: bytes, name: str) -> ArtifactRef:
        """Stage raw import bytes and publish them immutably. | 摄取并发布导入字节。"""

        staged = self.stage_path(name)
        staged.write_bytes(data)
        try:
            return self.publish(staged, "dataset")
        finally:
            staged.unlink(missing_ok=True)

    def resolve(self, reference: ArtifactRef) -> Path:
        """Resolve and verify canonical content identity. | 解析并验证规范内容标识。"""

        digest_hex = reference.digest.removeprefix("sha256:")
        if reference.uri != f"artifact://sha256/{digest_hex}":
            raise CatalystError(
                code="CATALYST_ARTIFACT_IDENTITY_INVALID",
                title="Artifact identity invalid",
                detail="The ArtifactRef URI and digest identify different content.",
                status=422,
            )
        path = self._objects / digest_hex
        if not path.is_file():
            raise CatalystError(
                code="CATALYST_ARTIFACT_UNAVAILABLE",
                title="Artifact unavailable",
                detail="The source artifact cannot be read.",
                status=422,
            )
        if path.stat().st_size != reference.size_bytes or sha256_file(path) != reference.digest:
            raise CatalystError(
                code="CATALYST_ARTIFACT_DIGEST_MISMATCH",
                title="Artifact integrity failure",
                detail="The source size or digest does not match its ArtifactRef.",
                status=422,
            )
        return path

    def publish(
        self,
        staged_path: Path,
        kind: str,
    ) -> ArtifactRef:
        """Publish immutable bytes and return their reference. | 发布不可变字节并返回引用。"""

        digest = sha256_file(staged_path)
        digest_hex = digest.removeprefix("sha256:")
        destination = self._objects / digest_hex
        if not destination.exists():
            shutil.copy2(staged_path, destination)
        return ArtifactRef(
            uri=f"artifact://sha256/{digest_hex}",
            digest=digest,
            size_bytes=destination.stat().st_size,
            kind=kind,
        )
