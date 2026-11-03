"""Restored ordinary services fail before SQLite/schema/workers/provider dispatch.

All data and the counted provider leaf are synthetic; startup and SQLite are real.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from cyrene_catalyst.api import create_app
from cyrene_catalyst.data_tools_domain import ProcessingOperation, ProcessingRun, ProcessingStage
from cyrene_catalyst.data_tools_store import DataToolsStore
from cyrene_catalyst.domain import Dataset, utc_now
from cyrene_catalyst.processing_runs import ProcessingRunCoordinator
from cyrene_catalyst.restore_guard import RestoreSuspendedError, assert_not_restored
from cyrene_catalyst.store import CatalystStore


def seed(root: Path) -> Path:
    root.mkdir(parents=True)
    database = root / "catalyst.sqlite3"
    datasets = CatalystStore(database)
    tools = DataToolsStore(database)
    now = utc_now()
    dataset = Dataset(
        id=uuid4(),
        name="Synthetic restored queue",
        created_at=now,
        updated_at=now,
        resource_version=1,
    )
    datasets.save_dataset(dataset)
    run = ProcessingRun(
        id=uuid4(),
        dataset_id=dataset.id,
        operation=ProcessingOperation.GENERATE_QA,
        stages=[ProcessingStage(key="generate:restored", external_call=True, deterministic=False)],
    )
    tools.create_run(run)
    (root / "queued-run-id.txt").write_text(str(run.id), encoding="utf-8")
    tools.close()
    datasets.close()
    return database


def snapshot(root: Path) -> dict[str, str]:
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file() and not path.is_symlink()
    }


def marker(root: Path, kind: str) -> None:
    target = root / "RESTORE_SUSPENDED.json"
    if kind == "directory":
        target.mkdir()
    elif kind == "dangling":
        try:
            target.symlink_to(root / "missing-marker-target")
        except OSError:
            pytest.skip("Local symlink creation privilege is required")
    else:
        target.write_text("{malformed restored marker", encoding="utf-8")


@pytest.mark.parametrize("kind", ["malformed", "directory", "dangling"])
@pytest.mark.parametrize("entry", ["app", "cli", "module", "store", "data-store"])
def test_actual_process_rejects_queued_restore_without_any_effect(
    tmp_path: Path, kind: str, entry: str
) -> None:
    restored = tmp_path / "restored"
    database = seed(restored / "state")
    nonce = restored / "restore-nonce.json"
    nonce.write_text('{"nonce":"synthetic-unconsumed"}', encoding="utf-8")
    marker(restored, kind)
    before = snapshot(restored)
    provider_calls = tmp_path / "provider-calls"
    script = """
import pathlib,sys,time
from cyrene_catalyst.data_tools_service import DataToolsService
def provider(self,*args):
    pathlib.Path(sys.argv[3]).write_text('CALLED')
    return {'synthetic':True}
DataToolsService._run_stage=provider
database=pathlib.Path(sys.argv[2])
if sys.argv[1]=='app':
    from cyrene_catalyst import create_app
    app=create_app(database_path=database,artifact_root=database.parent/'new-artifacts')
elif sys.argv[1]=='store':
    from cyrene_catalyst.store import CatalystStore
    CatalystStore(database)
elif sys.argv[1]=='data-store':
    from cyrene_catalyst.data_tools_store import DataToolsStore
    DataToolsStore(database)
elif sys.argv[1]=='cli':
    from cyrene_catalyst.cli import main
    sys.argv=['catalyst','serve','--home',str(database.parent),'--port','0']
    main()
else:
    import cyrene_catalyst.__main__
    cyrene_catalyst.__main__.main()
time.sleep(.25)
"""
    environment = {
        **os.environ,
        "PYTHONDONTWRITEBYTECODE": "1",
        "CATALYST_HOME": str(database.parent),
    }
    environment["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
    result = subprocess.run(
        [sys.executable, "-B", "-c", script, entry, str(database), str(provider_calls)],
        env=environment,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode != 0
    assert "RESTORE_SUSPENDED" in result.stderr, result.stderr
    assert not provider_calls.exists()
    assert snapshot(restored) == before
    assert not (database.parent / "new-artifacts").exists()
    assert nonce.read_text() == '{"nonce":"synthetic-unconsumed"}'


@pytest.mark.parametrize("direction", ["lexical", "resolved"])
def test_both_ancestor_authorities_are_checked(tmp_path: Path, direction: str) -> None:
    marked, clean = tmp_path / "marked", tmp_path / "clean"
    marked.mkdir()
    clean.mkdir()
    marker(marked, "malformed")
    alias = (marked if direction == "lexical" else clean) / "alias"
    try:
        alias.symlink_to(clean if direction == "lexical" else marked, target_is_directory=True)
    except OSError:
        pytest.skip("Local symlink creation privilege is required")
    with pytest.raises(RestoreSuspendedError):
        CatalystStore(alias / "uncreated" / "catalyst.sqlite3")
    assert not (clean / "uncreated").exists()
    assert not (marked / "uncreated").exists()


def test_coordinator_checks_original_paths_before_executor_or_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = seed(tmp_path / "state")
    tools = DataToolsStore(database)
    marker(tmp_path, "malformed")
    before = snapshot(tmp_path)
    effects = []

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        effects.append("worker-or-recovery")
        raise AssertionError("Restored initialization reached side effects")

    monkeypatch.setattr("cyrene_catalyst.processing_runs.ThreadPoolExecutor", forbidden)
    monkeypatch.setattr(ProcessingRunCoordinator, "_recover_interrupted_runs", forbidden)
    try:
        with pytest.raises(RestoreSuspendedError):
            ProcessingRunCoordinator(tools, forbidden)
        with pytest.raises(RestoreSuspendedError):
            ProcessingRunCoordinator(object(), forbidden)  # type: ignore[arg-type]
        assert effects == []
        assert snapshot(tmp_path) == before
    finally:
        tools.close()


def test_app_checks_before_either_store_or_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker(tmp_path, "malformed")
    effects = []

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        effects.append("constructed")
        raise AssertionError("Restored app reached a state constructor")

    for name in ("CatalystStore", "DataToolsStore", "LocalArtifactPlane"):
        monkeypatch.setattr("cyrene_catalyst.api." + name, forbidden)
    with pytest.raises(RestoreSuspendedError):
        create_app(database_path=tmp_path / "new" / "db", artifact_root=tmp_path / "new" / "blobs")
    assert effects == []
    assert not (tmp_path / "new").exists()


def test_missing_or_unverifiable_path_authority_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(RestoreSuspendedError):
        assert_not_restored()
    with pytest.raises(RestoreSuspendedError):
        assert_not_restored(None)  # type: ignore[arg-type]
    monkeypatch.setattr(
        Path, "resolve", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("unreadable"))
    )
    with pytest.raises(RestoreSuspendedError):
        assert_not_restored(Path("state"))


def test_unmarked_actual_app_recovers_the_canonical_queue_normally(tmp_path: Path) -> None:
    database = seed(tmp_path / "clean")
    script = """
import json,pathlib,sys
from uuid import UUID
from cyrene_catalyst.data_tools_service import DataToolsService
calls=[]
def provider(self,*args):
    calls.append('SIMULATED_PROVIDER_LEAF')
    return {'done':True}
DataToolsService._run_stage=provider
from cyrene_catalyst import create_app
app=create_app(database_path=pathlib.Path(sys.argv[1]),artifact_root=pathlib.Path(sys.argv[2]))
service=app.state.data_tools_service
run_id=UUID((pathlib.Path(sys.argv[1]).parent/'queued-run-id.txt').read_text())
completed=service.coordinator.wait(run_id,timeout=5)
assert completed.state.value=='SUCCEEDED'
assert calls==['SIMULATED_PROVIDER_LEAF']
service.close()
app.state.catalyst_service.close()
print(json.dumps({'state':completed.state.value,'providerCalls':len(calls)}))
"""
    environment = {
        **os.environ,
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": str(Path(__file__).parents[1] / "src"),
    }
    result = subprocess.run(
        [sys.executable, "-B", "-c", script, str(database), str(tmp_path / "artifacts")],
        env=environment,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"state": "SUCCEEDED", "providerCalls": 1}
