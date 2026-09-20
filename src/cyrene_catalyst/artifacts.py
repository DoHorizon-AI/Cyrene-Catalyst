"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 artifacts.py                                                    │
│  Module: cyrene_catalyst.artifacts                                  │
│  Role: Replaceable Artifact Plane adapter used by the local MVP.     │
│                                                                     │
│  模块职责：本地 MVP 的可替换 Artifact Plane 适配器。                     │
└─────────────────────────────────────────────────────────────────────┘

The plane delegates every content-addressed operation to the Platform Artifact
SDK so that references published here resolve inside every other Product that
shares the same artifact root.  No Product owns a private blob layout.
| 制品平面把全部内容寻址操作委托给 Platform 制品 SDK，保证本产品发布的引用
| 在同一 artifact root 下的其它产品中同样可解析；任何产品都不得持有私有 blob 布局。
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from cy_artifacts import ArtifactError, ArtifactKind, LocalArtifactProvider
from cy_artifacts import ArtifactRef as PlatformArtifactRef

from cyrene_catalyst.domain import ArtifactRef
from cyrene_catalyst.errors import CatalystError


def sha256_file(path: Path) -> str:
    """Return the canonical sha256 digest string. | 返回规范 sha256 摘要。"""

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _platform(reference: ArtifactRef) -> PlatformArtifactRef:
    """Project the wire reference onto the Platform SDK identity. | 投影到平台制品标识。"""

    return PlatformArtifactRef.from_dict(reference.model_dump(exclude_none=True))


class LocalArtifactPlane:
    """Platform Artifact SDK adapter; the Product stores only returned references. | 制品适配器。"""

    def __init__(self, root: Path) -> None:
        self.root = root
        self._provider = LocalArtifactProvider(root)
        self._staging = root / ".staging"
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
        try:
            resolved = self._provider.resolve(_platform(reference))
        except ArtifactError as exc:
            raise CatalystError(
                code="CATALYST_ARTIFACT_UNAVAILABLE",
                title="Artifact unavailable",
                detail="The source artifact cannot be read.",
                status=422,
            ) from exc
        return Path(resolved.location)

    def publish(
        self,
        staged_path: Path,
        kind: str,
    ) -> ArtifactRef:
        """Publish immutable bytes and return their reference. | 发布不可变字节并返回引用。"""

        try:
            reference = self._provider.publish(staged_path, kind=ArtifactKind(kind))
        except ArtifactError as exc:
            raise CatalystError(
                code="CATALYST_ARTIFACT_UNAVAILABLE",
                title="Artifact unavailable",
                detail="The artifact bytes could not be published.",
                status=422,
            ) from exc
        return ArtifactRef(**reference.to_dict())
