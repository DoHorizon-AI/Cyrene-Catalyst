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
