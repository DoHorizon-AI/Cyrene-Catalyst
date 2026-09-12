"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 domain.py                                                       │
│  Module: cyrene_catalyst.domain                                     │
│  Role: Product-owned Dataset and DatasetVersion boundary models.    │
│                                                                     │
│  模块职责：定义 Catalyst 产品资源、状态与稳定序列化契约。                 │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel


def utc_now() -> datetime:
    """Return a timezone-aware timestamp. | 返回带时区的 UTC 时间。"""

    return datetime.now(UTC)


class ContractModel(BaseModel):
    """Base model with the stable camelCase wire representation. | 稳定线格式基类。"""

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        serialize_by_alias=True,
        extra="forbid",
    )


class DatasetState(StrEnum):
    """Product-owned Dataset lifecycle. | Dataset 产品生命周期。"""

    ACTIVE = "ACTIVE"
    ARCHIVED = "ARCHIVED"


class DatasetVersionState(StrEnum):
    """Product-owned immutable version lifecycle. | 不可变版本生命周期。"""

    PROCESSING = "PROCESSING"
    PUBLISHED = "PUBLISHED"
    FAILED = "FAILED"


class ImportFormat(StrEnum):
    """Supported import source formats. | 支持的导入源格式。"""

    JSONL = "JSONL"
    JSON = "JSON"
    TEXT = "TEXT"


class PreparationState(StrEnum):
    """Interactive preparation workflow states. | 交互式整理工作流状态。"""

    STAGED = "STAGED"
    MAPPED = "MAPPED"
    SPLIT = "SPLIT"
    CONFIRMED = "CONFIRMED"
    PUBLISHED = "PUBLISHED"


class ArtifactRef(ContractModel):
    """Provider-neutral ArtifactRef wire projection. | 与提供方无关的制品引用线协议投影。"""

    model_config = ConfigDict(
        alias_generator=None,
        populate_by_name=True,
        serialize_by_alias=True,
        extra="forbid",
    )

    uri: str = Field(pattern=r"^artifact://sha256/[0-9a-f]{64}$")
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    size_bytes: int = Field(ge=0)
    # Artifact kind is an opaque producer-owned category. The wire contract
    # validates its shape but deliberately does not publish a Product vocabulary.
    kind: str = Field(min_length=1, max_length=128)
    manifest_digest: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
        pattern=r"^sha256:[0-9a-f]{64}$",
    )


class LineageEdge(ContractModel):
    """Persisted Product relationship between immutable artifacts. | 产品谱系关系。"""

    from_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    to_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    relation: Literal["DERIVED_FROM"] = "DERIVED_FROM"


class ProductFailure(ContractModel):
    """Failure persisted with the Product resource. | 随产品资源持久化的失败。"""

    code: str = Field(pattern=r"^[A-Z][A-Z0-9_]+$")
    message: str
    retryable: bool


class Dataset(ContractModel):
    """Durable Dataset aggregate. | 持久化 Dataset 聚合。"""

    id: UUID
    name: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=2000)
    state: DatasetState = DatasetState.ACTIVE
    created_at: datetime
    updated_at: datetime
    resource_version: int = Field(ge=1)


class DatasetVersion(ContractModel):
    """Immutable DatasetVersion and its lineage evidence. | 不可变版本及谱系证据。"""

    id: UUID
    dataset_id: UUID
    version: int = Field(ge=1)
    state: DatasetVersionState
    source: ArtifactRef
    output: ArtifactRef | None = None
    engine_binding_id: str = Field(min_length=1, max_length=200)
    engine_capability_type: Literal["dataset.preparation.v1"] = "dataset.preparation.v1"
    lineage: list[LineageEdge] = Field(default_factory=list)
    source_refs: list[str] = Field(default_factory=list)
    row_count: int | None = Field(default=None, ge=0)
    schema_fields: list[str] | None = None
    failure: ProductFailure | None = None
    created_at: datetime
    updated_at: datetime
    resource_version: int = Field(ge=1)


class CreateDatasetRequest(ContractModel):
    """Create-Dataset command body. | 创建 Dataset 请求。"""

    name: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=2000)


class CreateDatasetVersionRequest(ContractModel):
    """Create-version command body. | 创建版本请求。"""

    source: ArtifactRef
    engine_binding_id: str = Field(min_length=1, max_length=200)


class ProblemDetails(ContractModel):
    """RFC 9457 response with stable Cyrene extensions. | RFC 9457 错误响应。"""

    type: str
    title: str
    status: int = Field(ge=400, le=599)
    detail: str
    instance: str
    code: str
    retryable: bool
    trace_id: str
    resource_ref: str | None = None


class EngineResult(ContractModel):
    """Data-engine output before Product publication. | 产品发布前的数据引擎输出。"""

    row_count: int = Field(ge=0)
    schema_fields: list[str]


class FieldSource(ContractModel):
    """Mapping source: an imported field or a literal. | 字段映射来源。"""

    field: str | None = Field(default=None, min_length=1, max_length=200)
    literal: str | None = Field(default=None, max_length=2000)

    def resolve(self, row: dict[str, Any]) -> Any:
        """Return the raw mapped value or None when missing. | 解析映射值。"""

        if self.literal is not None:
            return self.literal
        if self.field is None:
            return None
        return row.get(self.field)


class MappingConfig(ContractModel):
    """Declarative field mapping to the SFT training shape. | 字段映射配置。"""

    mode: Literal["instruction", "conversation"]
    instruction: FieldSource | None = None
    input: FieldSource | None = None
    output: FieldSource | None = None
    conversation_value_field: str | None = Field(default=None, min_length=1, max_length=200)
    conversation_from_field: str | None = Field(default=None, min_length=1, max_length=200)
    group_by: str | None = Field(default=None, min_length=1, max_length=200)


class NormalizationConfig(ContractModel):
    """Text normalization applied to every mapped field. | 文本规范化配置。"""

    trim_whitespace: bool = True
    collapse_whitespace: bool = True
    unicode_nfc: bool = True


class SplitConfig(ContractModel):
    """Deterministic group-level train/validation split. | 分组确定性划分配置。"""

    train_ratio: float = Field(default=0.9, ge=0.0, le=1.0)


class SampleError(ContractModel):
    """One rejected sample with its concrete reason. | 带具体原因的剔除样本。"""

    row_index: int = Field(ge=1)
    reason_code: str = Field(pattern=r"^[A-Z][A-Z0-9_]+$")
    message: str
    field: str | None = None
    excerpt: str = Field(default="", max_length=200)


class PreparationReport(ContractModel):
    """Quality and deduplication evidence for a mapping. | 质量与去重报告。"""

    total_rows: int = Field(ge=0)
    valid_samples: int = Field(ge=0)
    error_samples: int = Field(ge=0)
    duplicate_samples: int = Field(ge=0)
    unique_samples: int = Field(ge=0)
    group_count: int = Field(ge=0)
    errors: list[SampleError] = Field(max_length=100)


class SplitStats(ContractModel):
    """Split outcome evidence. | 划分结果统计。"""

    train_ratio: float
    train_samples: int = Field(ge=0)
    val_samples: int = Field(ge=0)
    train_groups: int = Field(ge=0)
    val_groups: int = Field(ge=0)


class ExportFile(ContractModel):
    """One downloadable standard export inside a bundle. | 导出包内文件。"""

    name: str = Field(pattern=r"^[a-z0-9_-]+\.(jsonl|json)$")
    artifact: ArtifactRef
    row_count: int | None = Field(default=None, ge=0)
    media_type: Literal["application/jsonl", "application/json"]


class Preparation(ContractModel):
    """Interactive preparation workflow anchored to one Dataset. | 整理工作流聚合。"""

    id: UUID
    dataset_id: UUID
    name: str = Field(min_length=1, max_length=200)
    state: PreparationState
    source: ArtifactRef
    source_refs: list[str] = Field(default_factory=list)
    source_filename: str = Field(default="", max_length=300)
    format: ImportFormat
    detected_fields: list[str] = Field(max_length=500)
    row_count: int = Field(ge=0)
    mapping: MappingConfig | None = None
    normalization: NormalizationConfig | None = None
    split: SplitConfig | None = None
    report: PreparationReport | None = None
    split_stats: SplitStats | None = None
    published_version_id: UUID | None = None
    exports: list[ExportFile] = Field(default_factory=list)
    yield_draft: ArtifactRef | None = None
    created_at: datetime
    updated_at: datetime
    resource_version: int = Field(ge=1)


class RawPreview(ContractModel):
    """Paginated raw imported rows. | 原始行分页预览。"""

    total: int = Field(ge=0)
    offset: int = Field(ge=0)
    limit: int = Field(gt=0)
    rows: list[dict[str, Any]]


class NormalizedPreviewItem(ContractModel):
    """One normalized sample with split assignment. | 规范化样本及划分。"""

    sample_index: int = Field(ge=1)
    group_key: str
    split: Literal["train", "val"] | None = None
    content: dict[str, Any]
    source_row_indexes: list[int]


class NormalizedPreview(ContractModel):
    """Paginated normalized samples. | 规范化样本分页预览。"""

    total: int = Field(ge=0)
    offset: int = Field(ge=0)
    limit: int = Field(gt=0)
    items: list[NormalizedPreviewItem]


class ErrorPreview(ContractModel):
    """Paginated rejected samples with concrete reasons. | 剔除样本分页预览。"""

    total: int = Field(ge=0)
    offset: int = Field(ge=0)
    limit: int = Field(gt=0)
    items: list[SampleError]


class ConfigureMappingRequest(ContractModel):
    """Mapping + normalization patch body. | 映射配置请求。"""

    mapping: MappingConfig
    normalization: NormalizationConfig


class ConfigureSplitRequest(ContractModel):
    """Split patch body. | 划分配置请求。"""

    split: SplitConfig


class PublishPreparationResponse(ContractModel):
    """Publish outcome bundling preparation and version. | 发布结果。"""

    preparation: Preparation
    dataset_version: DatasetVersion
