"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 workspace_auth.py                                               │
│  Module: cyrene_catalyst.workspace_auth                             │
│  Role: Authenticate private Workspace service requests.             │
│                                                                     │
│  模块职责：将私有服务凭据映射到固定 Workspace scope。                   │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import dataclass

_TOKEN_DIGEST_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_MAX_SCOPE_VALUE_BYTES = 512
_MIN_TOKEN_BYTES = 32


class WorkspaceServiceAuthConfigError(ValueError):
    """Raised when the server-side Workspace credential map is invalid."""


@dataclass(frozen=True, slots=True)
class WorkspaceServicePrincipal:
    """Trusted organization and Workspace scope resolved from a service token."""

    organization_id: str
    workspace_id: str


@dataclass(frozen=True, slots=True)
class _Credential:
    token_sha256: bytes
    principal: WorkspaceServicePrincipal


class WorkspaceServiceAuthenticator:
    """Resolve high-entropy bearer tokens to immutable Workspace scopes.

    The configuration stores token digests only. Each request token is hashed,
    then compared with every configured digest using constant-time comparison.
    """

    def __init__(self, credentials: tuple[_Credential, ...] = ()) -> None:
        self._credentials = credentials

    @classmethod
    def from_json(cls, raw_config: str | None) -> WorkspaceServiceAuthenticator:
        """Parse the injected JSON map; an absent value creates a deny-all map."""

        if raw_config is None or not raw_config.strip():
            return cls()

        try:
            entries = json.loads(raw_config)
        except json.JSONDecodeError as exc:
            raise WorkspaceServiceAuthConfigError(
                "CYRENE_WORKSPACE_SERVICE_AUTH_JSON is not valid JSON"
            ) from exc
        if not isinstance(entries, list) or not entries:
            raise WorkspaceServiceAuthConfigError(
                "CYRENE_WORKSPACE_SERVICE_AUTH_JSON must be a non-empty JSON array"
            )

        credentials: list[_Credential] = []
        digest_scopes: dict[bytes, WorkspaceServicePrincipal] = {}
        expected_keys = {"tokenSha256", "organizationId", "workspaceId"}
        for entry in entries:
            if not isinstance(entry, dict) or set(entry) != expected_keys:
                raise WorkspaceServiceAuthConfigError(
                    "each Workspace credential entry must contain only tokenSha256, "
                    "organizationId, and workspaceId"
                )

            digest_value = entry["tokenSha256"]
            organization_id = entry["organizationId"]
            workspace_id = entry["workspaceId"]
            if not isinstance(digest_value, str) or not _TOKEN_DIGEST_RE.fullmatch(digest_value):
                raise WorkspaceServiceAuthConfigError(
                    "each tokenSha256 must be a 64-character hexadecimal SHA-256 digest"
                )
            principal = WorkspaceServicePrincipal(
                organization_id=_validate_scope_value(organization_id, "organizationId"),
                workspace_id=_validate_scope_value(workspace_id, "workspaceId"),
            )
            digest = bytes.fromhex(digest_value)
            previous = digest_scopes.get(digest)
            if previous is not None:
                if previous != principal:
                    raise WorkspaceServiceAuthConfigError(
                        "one token digest cannot map to multiple Workspace scopes"
                    )
                raise WorkspaceServiceAuthConfigError(
                    "duplicate token digest entries are not allowed"
                )
            digest_scopes[digest] = principal
            credentials.append(_Credential(token_sha256=digest, principal=principal))

        return cls(tuple(credentials))

    @property
    def configured(self) -> bool:
        """Return whether at least one trusted service credential is configured."""

        return bool(self._credentials)

    def authenticate(self, authorization: str | None) -> WorkspaceServicePrincipal | None:
        """Return the configured principal for a valid Bearer token, otherwise None."""

        if not self._credentials or authorization is None:
            return None
        scheme, separator, token = authorization.partition(" ")
        if (
            not separator
            or scheme.casefold() != "bearer"
            or not token
            or token != token.strip()
            or any(not 33 <= ord(character) <= 126 for character in token)
        ):
            return None
        try:
            token_bytes = token.encode("ascii")
        except UnicodeEncodeError:
            return None  # diagnostic-allow: non-ASCII bearer tokens must fail authentication
        if len(token_bytes) < _MIN_TOKEN_BYTES:
            return None

        candidate = hashlib.sha256(token_bytes).digest()
        match: WorkspaceServicePrincipal | None = None
        for credential in self._credentials:
            if hmac.compare_digest(candidate, credential.token_sha256):
                match = credential.principal
        return match


def _validate_scope_value(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise WorkspaceServiceAuthConfigError(f"{field_name} must be a non-empty trimmed string")
    if len(value.encode("utf-8")) > _MAX_SCOPE_VALUE_BYTES or any(
        ord(character) < 32 or ord(character) == 127 for character in value
    ):
        raise WorkspaceServiceAuthConfigError(f"{field_name} exceeds the supported scope bounds")
    return value
