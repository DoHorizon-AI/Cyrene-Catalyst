"""Fence ordinary restored services before storage or worker initialization.

中文：恢复标记只表示暂停，不赋予启动、迁移 schema 或重放旧任务的权限。
"""

from __future__ import annotations

from pathlib import Path


class RestoreSuspendedError(RuntimeError):
    """Restored or unverifiable path authority cannot start a mutable owner."""


def assert_not_restored(*paths: Path) -> None:
    """Reject any lexical/resolved ancestor marker without reading its contents.

    Missing path authority, unreadable ancestors, symlink loops and malformed,
    directory or dangling markers all fail closed. No marker is changed here.
    """
    if not paths or any(not isinstance(path, Path) for path in paths):
        raise RestoreSuspendedError("RESTORE_SUSPENDED: storage path authority is required")
    try:
        scopes = (*paths, Path.cwd(), Path(__file__))
        candidates = {
            candidate
            for path in scopes
            for candidate in (path.expanduser().absolute(), path.expanduser().resolve())
        }
    except (OSError, RuntimeError, ValueError):
        raise RestoreSuspendedError(
            "RESTORE_SUSPENDED: path authority cannot be verified"
        ) from None
    for candidate in candidates:
        for parent in (candidate, *candidate.parents):
            try:
                (parent / "RESTORE_SUSPENDED.json").lstat()
            except (FileNotFoundError, NotADirectoryError):
                continue
            except OSError:
                raise RestoreSuspendedError(
                    "RESTORE_SUSPENDED: path authority cannot be verified"
                ) from None
            raise RestoreSuspendedError("RESTORE_SUSPENDED: restored state cannot start services")
