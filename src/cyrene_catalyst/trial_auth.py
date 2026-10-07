"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 trial_auth.py                                                    │
│  Module: cyrene_catalyst.trial_auth                                  │
│  Role: Authenticate externally reachable data-tools trial requests.  │
│                                                                      │
│  模块职责：为独立 Catalyst 试用实例提供 Bearer 身份与路由保护。         │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import os
from collections.abc import Mapping

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response
from starlette.types import ASGIApp

from cyrene_catalyst.workspace_auth import (
    WorkspaceServiceAuthenticator,
    WorkspaceServicePrincipal,
)

_TOKEN_ENV = "CYRENE_DATA_TOOLS_TOKEN"
_ORGANIZATION_ENV = "CYRENE_DATA_TOOLS_ORGANIZATION_ID"
_WORKSPACE_ENV = "CYRENE_DATA_TOOLS_WORKSPACE_ID"
_DEFAULT_ORGANIZATION_ID = "data-tools-trial"
_DEFAULT_WORKSPACE_ID = "data-tools"
_MIN_TOKEN_BYTES = 32
_HEALTH_PATH = "/healthz"


class TrialAuthConfigError(ValueError):
    """Raised when trial auth configuration is unsafe. | 试用认证配置不安全时抛出。"""


def trial_authenticator_from_environment(
    environment: Mapping[str, str] | None = None,
) -> WorkspaceServiceAuthenticator:
    """Build one fixed Workspace identity from the trial token environment.

    The token is converted to its SHA-256 digest before the existing Workspace
    authenticator receives it. The clear token is never serialized into config.
    中文：将试用令牌映射到固定工作空间身份，仅向既有认证器传入摘要。
    """

    values = os.environ if environment is None else environment
    token = values.get(_TOKEN_ENV)
    if token is None or not token:
        return WorkspaceServiceAuthenticator.from_json(None)
    try:
        token_bytes = token.encode("ascii")
    except UnicodeEncodeError as exc:
        raise TrialAuthConfigError(
            f"{_TOKEN_ENV} must contain at least 32 printable ASCII bytes"
        ) from exc
    if len(token_bytes) < _MIN_TOKEN_BYTES or any(not 33 <= byte <= 126 for byte in token_bytes):
        raise TrialAuthConfigError(f"{_TOKEN_ENV} must contain at least 32 printable ASCII bytes")

    organization_id = values.get(_ORGANIZATION_ENV, _DEFAULT_ORGANIZATION_ID)
    workspace_id = values.get(_WORKSPACE_ENV, _DEFAULT_WORKSPACE_ID)
    config = json.dumps(
        [
            {
                "tokenSha256": hashlib.sha256(token_bytes).hexdigest(),
                "organizationId": organization_id,
                "workspaceId": workspace_id,
            }
        ],
        separators=(",", ":"),
    )
    try:
        return WorkspaceServiceAuthenticator.from_json(config)
    except ValueError as exc:
        raise TrialAuthConfigError(str(exc)) from exc


def is_loopback_host(host: str) -> bool:
    """Return whether a bind address names loopback. | 判断监听地址是否为回环地址。"""

    if host.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def validate_trial_listener(
    host: str,
    *,
    allow_remote: bool,
    authenticator: WorkspaceServiceAuthenticator,
) -> bool:
    """Validate remote intent and credentials, then return bind mode. | 校验远程监听条件。"""

    remote_listener = not is_loopback_host(host)
    if remote_listener and not allow_remote:
        raise TrialAuthConfigError(
            "non-loopback listeners require --allow-remote and an external TLS terminator"
        )
    if remote_listener and not authenticator.configured:
        raise TrialAuthConfigError(
            f"non-loopback listeners require {_TOKEN_ENV} and an external TLS terminator"
        )
    return remote_listener


def trial_principal_from_request(request: Request) -> WorkspaceServicePrincipal | None:
    """Read the authenticated identity for explicit resource-scope enforcement.

    Public Product handlers must pass this principal into owner-aware service
    and store methods. Authentication alone does not authorize resource access.
    中文：处理器应将此身份传入带资源归属检查的服务和存储方法。
    """

    principal = getattr(request.state, "trial_workspace_principal", None)
    if isinstance(principal, WorkspaceServicePrincipal):
        return principal
    existing_principal = getattr(request.state, "workspace_service_principal", None)
    if isinstance(existing_principal, WorkspaceServicePrincipal):
        return existing_principal
    return None


class TrialAuthMiddleware(BaseHTTPMiddleware):
    """Require a token outside liveness checks. | 除存活检查外要求 Bearer 令牌。"""

    def __init__(
        self,
        app: ASGIApp,
        *,
        authenticator: WorkspaceServiceAuthenticator,
    ) -> None:
        super().__init__(app)
        self._authenticator = authenticator

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        """Authenticate and attach the Workspace identity. | 验证请求并附加工作空间身份。"""

        if request.url.path == _HEALTH_PATH:
            return await call_next(request)
        principal = self._authenticator.authenticate(request.headers.get("authorization"))
        if principal is None:
            response = JSONResponse(
                status_code=401,
                content={
                    "type": "https://errors.cyrene.dev/data-tools/authentication-required",
                    "title": "Trial authentication required",
                    "status": 401,
                    "detail": "A valid data-tools trial bearer token is required.",
                    "instance": request.url.path,
                    "code": "DATA_TOOLS_AUTHENTICATION_REQUIRED",
                },
                media_type="application/problem+json",
            )
            response.headers["WWW-Authenticate"] = "Bearer"
            return response

        request.state.trial_workspace_principal = principal
        request.state.workspace_service_principal = principal
        return await call_next(request)


def install_trial_auth(
    app: FastAPI,
    *,
    authenticator: WorkspaceServiceAuthenticator | None = None,
    required: bool = False,
) -> WorkspaceServiceAuthenticator:
    """Install request authentication and return the active authenticator.

    `required=True` fails app construction when no token is configured, which
    lets remote listeners fail closed before the server accepts connections.
    中文：远程监听缺少令牌时在应用启动前失败。
    """

    active_authenticator = authenticator or trial_authenticator_from_environment()
    if required and not active_authenticator.configured:
        raise TrialAuthConfigError(f"{_TOKEN_ENV} is required for non-loopback trial listeners")
    app.state.trial_authenticator = active_authenticator
    if active_authenticator.configured:
        app.add_middleware(
            TrialAuthMiddleware,
            authenticator=active_authenticator,
        )
    return active_authenticator
