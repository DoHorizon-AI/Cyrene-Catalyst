"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 __init__.py                                                     │
│  Module: cyrene_catalyst                                            │
│  Role: Public Catalyst Product API construction surface.             │
│                                                                     │
│  模块职责：导出 Catalyst 产品 API 创建入口。                            │
└─────────────────────────────────────────────────────────────────────┘
"""

from cyrene_catalyst.api import create_app

__all__ = ["create_app"]
