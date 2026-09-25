"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 errors.py                                                       │
│  Module: cyrene_catalyst.errors                                     │
│  Role: Stable Product error taxonomy and adapter failures.          │
│                                                                     │
│  模块职责：定义稳定产品错误码与适配器失败边界。                           │
└─────────────────────────────────────────────────────────────────────┘
"""


class CatalystError(RuntimeError):
    """Typed error safe to expose as RFC 9457 Problem Details. | 可公开的类型化错误。"""

    def __init__(
        self,
        *,
        code: str,
        title: str,
        detail: str,
        status: int,
        retryable: bool = False,
        resource_ref: str | None = None,
    ) -> None:
        super().__init__(detail)
        self.code = code
        self.title = title
        self.detail = detail
        self.status = status
        self.retryable = retryable
        self.resource_ref = resource_ref


class DataEngineFailure(RuntimeError):
    """Sanitized replaceable-engine failure. | 已净化的可替换引擎失败。"""


# ════════════════════════════════════════════════════════════════════════
# Canonical Cyrene Catalyst Error Catalog & Mappings
# Cyrene Catalyst 标准错误目录与映射
# ════════════════════════════════════════════════════════════════════════
CATALYST_ERROR_MAPPINGS: dict[str, dict[str, str]] = {
    "CATALYST_REQUEST_INVALID": {
        "code": "PRODUCT.CATALYST.REQUEST_INVALID",
        "cause_kind": "validation",
        "recovery_action": "fix_configuration",
    },
    "CATALYST_RESOURCE_NOT_FOUND": {
        "code": "PRODUCT.CATALYST.RESOURCE_NOT_FOUND",
        "cause_kind": "not_found",
        "recovery_action": "user_action_required",
    },
    "CATALYST_DATASET_NOT_FOUND": {
        "code": "PRODUCT.CATALYST.DATASET_NOT_FOUND",
        "cause_kind": "not_found",
        "recovery_action": "user_action_required",
    },
    "CATALYST_VERSION_NOT_FOUND": {
        "code": "PRODUCT.CATALYST.VERSION_NOT_FOUND",
        "cause_kind": "not_found",
        "recovery_action": "user_action_required",
    },
    "CATALYST_IDEMPOTENCY_CONFLICT": {
        "code": "PRODUCT.CATALYST.IDEMPOTENCY_CONFLICT",
        "cause_kind": "conflict",
        "recovery_action": "safely_retry",
    },
    "CATALYST_SCHEMA_MISMATCH": {
        "code": "PRODUCT.CATALYST.SCHEMA_MISMATCH",
        "cause_kind": "validation",
        "recovery_action": "fix_configuration",
    },
    "CATALYST_COLUMN_UNKNOWN": {
        "code": "PRODUCT.CATALYST.COLUMN_UNKNOWN",
        "cause_kind": "validation",
        "recovery_action": "fix_configuration",
    },
    "CATALYST_ENGINE_UNAVAILABLE": {
        "code": "PRODUCT.CATALYST.ENGINE_UNAVAILABLE",
        "cause_kind": "infrastructure",
        "recovery_action": "query_state_first",
    },
    "CATALYST_TRANSFORMATION_FAILED": {
        "code": "PRODUCT.CATALYST.TRANSFORMATION_FAILED",
        "cause_kind": "execution",
        "recovery_action": "fix_configuration",
    },
    "CATALYST_DATA_PROCESSING_FAILED": {
        "code": "PRODUCT.CATALYST.DATA_PROCESSING_FAILED",
        "cause_kind": "execution",
        "recovery_action": "fix_configuration",
    },
    "CATALYST_HANDOFF_FAILED": {
        "code": "PRODUCT.CATALYST.HANDOFF_FAILED",
        "cause_kind": "network",
        "recovery_action": "safely_retry",
    },
    "CATALYST_ARTIFACT_DENIED": {
        "code": "PRODUCT.CATALYST.ARTIFACT_DENIED",
        "cause_kind": "permission",
        "recovery_action": "fix_configuration",
    },
}


def map_catalyst_error(raw_code: str) -> dict[str, str]:
    """Map a raw or legacy Catalyst error code to canonical PRODUCT.CATALYST.<REASON>.
    中文：将原始或旧版 Catalyst 错误码映射为规范的 PRODUCT.CATALYST.<REASON>。"""
    if raw_code in CATALYST_ERROR_MAPPINGS:
        return CATALYST_ERROR_MAPPINGS[raw_code]
    normalized = raw_code.upper().replace(" ", "_")
    if not normalized.startswith("PRODUCT.CATALYST."):
        clean_name = normalized.removeprefix("CATALYST_")
        canonical = f"PRODUCT.CATALYST.{clean_name}"
    else:
        canonical = normalized
    return {
        "code": canonical,
        "cause_kind": "unknown",
        "recovery_action": "query_state_first",
    }
