"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 processing_runs.py                                              │
│  Module: cyrene_catalyst.processing_runs                            │
│  Role: Durable asynchronous run execution and recovery.              │
│                                                                     │
│  模块职责：执行、取消、重试并恢复持久化的 Data Tools 运行。              │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import hashlib
import json
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Event, RLock
from typing import Any, Protocol
from uuid import UUID, uuid4

from cyrene_catalyst.data_tools_domain import (
    ProcessingFailure,
    ProcessingOperation,
    ProcessingProgress,
    ProcessingRun,
    ProcessingRunState,
    ProcessingStage,
    ProcessingStageState,
)
from cyrene_catalyst.data_tools_store import DataToolsStore
from cyrene_catalyst.domain import utc_now
from cyrene_catalyst.workspace_auth import WorkspaceServicePrincipal

_PAID_CALL_UNKNOWN = "CATALYST_PAID_CALL_OUTCOME_UNKNOWN"


class StageRunner(Protocol):
    """Application callback that executes one concrete parse/build/generate stage."""

    def __call__(
        self,
        run: ProcessingRun,
        stage: ProcessingStage,
        cancel_event: Event,
    ) -> dict[str, Any]:
        """Run one stage and return JSON-serializable output. | 执行一个阶段。"""


class StageExecutionFailure(RuntimeError):
    """Known stage failure receipt, optionally marking an ambiguous provider call."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool,
        outcome_unknown: bool | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.outcome_unknown = outcome_unknown


class ProcessingRunCoordinator:
    """Persist lifecycle changes around a bounded asynchronous stage runner."""

    def __init__(
        self,
        store: DataToolsStore,
        stage_runner: StageRunner,
        *,
        max_workers: int = 2,
        recover_on_start: bool = True,
    ) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be at least one.")
        self.store = store
        self.stage_runner = stage_runner
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="catalyst-run",
        )
        self._lock = RLock()
        self._events: dict[UUID, Event] = {}
        self._scheduled: dict[UUID, Future[None]] = {}
        if recover_on_start:
            self._recover_interrupted_runs()

    def enqueue(
        self,
        run_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ProcessingRun:
        """Schedule a durable queued run and return its latest stored projection."""

        run = self.store.get_run(run_id, principal)
        if run is None:
            raise LookupError("ProcessingRun was not found in the caller's scope.")
        self._schedule(run)
        return self.store.get_run(run_id, principal) or run

    def enqueue_for_worker(self, run_id: UUID) -> ProcessingRun:
        """Schedule a queued run during trusted startup recovery."""

        run = self.store.get_run_for_worker(run_id)
        if run is None:
            raise LookupError("ProcessingRun was not found.")
        self._schedule(run)
        return self.store.get_run_for_worker(run_id) or run

    def _schedule(self, run: ProcessingRun) -> None:
        """Submit one queued record once per coordinator instance. | 提交排队运行。"""

        with self._lock:
            if run.state == ProcessingRunState.QUEUED and run.id not in self._scheduled:
                event = Event()
                self._events[run.id] = event
                self._scheduled[run.id] = self._executor.submit(self._execute, run.id, event)

    def get(
        self,
        run_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ProcessingRun | None:
        """Read the latest persisted run state. | 读取最新运行状态。"""

        return self.store.get_run(run_id, principal)

    def cancel(
        self,
        run_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ProcessingRun:
        """Request cooperative cancellation and persist its intent immediately."""

        with self._lock:
            saved = self.store.request_cancel(run_id, principal)
            event = self._events.get(run_id)
            if event is not None:
                event.set()
            return saved

    def retry(
        self,
        run_id: UUID,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ProcessingRun:
        """Create a linked retry run, refusing ambiguous paid-provider outcomes."""

        prior = self.store.get_run(run_id, principal)
        if prior is None:
            raise LookupError("ProcessingRun was not found in the caller's scope.")
        if prior.state not in {
            ProcessingRunState.FAILED,
            ProcessingRunState.INTERRUPTED,
            ProcessingRunState.CANCELLED,
        }:
            raise ValueError("Only failed, interrupted, or cancelled runs can be retried.")
        if any(stage.state == ProcessingStageState.OUTCOME_UNKNOWN for stage in prior.stages):
            raise ValueError("A paid provider outcome is unknown; create a new run after review.")
        if prior.failure is not None and not prior.failure.retryable:
            raise ValueError("This ProcessingRun is not retryable.")

        now = utc_now()
        stages = [
            stage.model_copy(
                update={
                    "state": ProcessingStageState.PENDING,
                    "call_started_at": None,
                    "call_completed_at": None,
                    "failure_code": None,
                    "failure_message": None,
                    "retryable": True,
                    "started_at": None,
                    "finished_at": None,
                    "output": None,
                }
            )
            if stage.state
            in {
                ProcessingStageState.FAILED,
                ProcessingStageState.INTERRUPTED,
                ProcessingStageState.CANCELLED,
            }
            else stage
            for stage in prior.stages
        ]
        retry = prior.model_copy(
            update={
                "id": uuid4(),
                "state": ProcessingRunState.QUEUED,
                "stages": stages,
                "retry_of_run_id": prior.id,
                "attempt": prior.attempt + 1,
                "cancellation_requested": False,
                "failure": None,
                "progress": self._progress(stages),
                "created_at": now,
                "updated_at": now,
                "started_at": None,
                "finished_at": None,
                "resource_version": 1,
            }
        )
        self.store.create_run(retry, principal)
        return self.enqueue(retry.id, principal)

    def wait(
        self,
        run_id: UUID,
        timeout: float | None = None,
        principal: WorkspaceServicePrincipal | None = None,
    ) -> ProcessingRun:
        """Wait for a local worker completion, mainly for integration harnesses."""

        with self._lock:
            future = self._scheduled.get(run_id)
        if future is not None:
            future.result(timeout=timeout)
        run = self.store.get_run(run_id, principal)
        if run is None:
            raise LookupError("ProcessingRun was not found.")
        return run

    def shutdown(self, *, wait: bool = True) -> None:
        """Stop accepting work and optionally wait for active stages. | 停止任务执行器。"""

        self._executor.shutdown(wait=wait, cancel_futures=False)

    def _recover_interrupted_runs(self) -> None:
        """Mark running records interrupted; never replay unfinished provider calls."""

        queued: list[UUID] = []
        for run in self.store.list_active_runs():
            if run.state == ProcessingRunState.QUEUED:
                queued.append(run.id)
                continue
            now = utc_now()
            unknown_paid_call = False
            stages: list[ProcessingStage] = []
            for stage in run.stages:
                if stage.state != ProcessingStageState.RUNNING:
                    stages.append(stage)
                    continue
                stage_unknown = (
                    stage.external_call
                    and stage.call_started_at is not None
                    and stage.call_completed_at is None
                )
                unknown_paid_call = unknown_paid_call or stage_unknown
                stages.append(
                    stage.model_copy(
                        update={
                            "state": (
                                ProcessingStageState.OUTCOME_UNKNOWN
                                if stage_unknown
                                else ProcessingStageState.INTERRUPTED
                            ),
                            "failure_code": _PAID_CALL_UNKNOWN if stage_unknown else None,
                            "failure_message": (
                                "The provider call started before process exit, "
                                "but its result was not saved."
                                if stage_unknown
                                else "The process stopped before this stage completed."
                            ),
                            "retryable": not stage_unknown,
                            "finished_at": now,
                        }
                    )
                )
            failure = ProcessingFailure(
                code=_PAID_CALL_UNKNOWN if unknown_paid_call else "CATALYST_RUN_INTERRUPTED",
                message=(
                    "A generation provider result is unknown and will not be retried automatically."
                    if unknown_paid_call
                    else "The worker process stopped before the run completed."
                ),
                retryable=not unknown_paid_call,
            )
            recovered = run.model_copy(
                update={
                    "state": ProcessingRunState.INTERRUPTED,
                    "stages": stages,
                    "cancellation_requested": False,
                    "progress": self._progress(stages),
                    "failure": failure,
                    "updated_at": now,
                    "finished_at": now,
                    "resource_version": run.resource_version + 1,
                }
            )
            if run.operation == ProcessingOperation.PARSE:
                self.store.mark_parse_reports_interrupted_for_worker(run.id, now)
            self.store.save_run_for_worker(recovered)
        for run_id in queued:
            self.enqueue_for_worker(run_id)

    def _execute(self, run_id: UUID, cancel_event: Event) -> None:
        with self._lock:
            run = self.store.get_run_for_worker(run_id)
            if run is None or run.state != ProcessingRunState.QUEUED:
                return
            now = utc_now()
            run = self._save(
                run.model_copy(
                    update={
                        "state": ProcessingRunState.RUNNING,
                        "started_at": now,
                        "updated_at": now,
                        "resource_version": run.resource_version + 1,
                    }
                )
            )

        for index, stage in enumerate(run.stages):
            if stage.state in {ProcessingStageState.COMPLETED, ProcessingStageState.REUSED}:
                continue
            if cancel_event.is_set() or run.cancellation_requested:
                run = self._cancel_pending(run)
                return

            input_digest = stage.input_digest or self._default_input_digest(run, stage)
            cache_input_digest = self._cache_input_digest(run, stage, input_digest)
            recipe_digest = stage.recipe_digest or run.recipe_digest
            cache_key = f"{run.operation.value}:{stage.key}"
            if stage.deterministic:
                reused = self.store.find_reusable_stage_for_worker(
                    run.dataset_id,
                    cache_key,
                    cache_input_digest,
                    recipe_digest or "",
                )
                if reused is not None:
                    completed_at = utc_now()
                    reused_stage = stage.model_copy(
                        update={
                            "state": ProcessingStageState.REUSED,
                            "input_digest": input_digest,
                            "recipe_digest": recipe_digest,
                            "output": reused,
                            "started_at": completed_at,
                            "finished_at": completed_at,
                        }
                    )
                    run = self._replace_stage(run, index, reused_stage)
                    run = self._append_artifacts(run, reused)
                    continue

            stage_started = utc_now()
            running_stage = stage.model_copy(
                update={
                    "state": ProcessingStageState.RUNNING,
                    "input_digest": input_digest,
                    "recipe_digest": recipe_digest,
                    "call_started_at": stage_started if stage.external_call else None,
                    "call_completed_at": None,
                    "failure_code": None,
                    "failure_message": None,
                    "retryable": True,
                    "started_at": stage_started,
                    "finished_at": None,
                }
            )
            run = self._replace_stage(run, index, running_stage)
            try:
                output = self.stage_runner(run, running_stage, cancel_event)
            except StageExecutionFailure as exc:
                unknown = (
                    exc.outcome_unknown
                    if exc.outcome_unknown is not None
                    else running_stage.external_call and running_stage.call_started_at is not None
                )
                self._fail_stage(
                    run,
                    index,
                    running_stage,
                    code=_PAID_CALL_UNKNOWN if unknown else exc.code,
                    message=(
                        "The generation provider may have completed this paid call; "
                        "automatic retry is blocked."
                        if unknown
                        else str(exc)
                    ),
                    retryable=exc.retryable and not unknown,
                    outcome_unknown=unknown,
                )
                return
            except Exception as exc:
                unknown_value = getattr(exc, "outcome_unknown", None)
                unknown = (
                    bool(unknown_value)
                    if unknown_value is not None
                    else running_stage.external_call and running_stage.call_started_at is not None
                )
                code = getattr(exc, "code", None)
                retryable = getattr(exc, "retryable", not unknown)
                message = getattr(exc, "detail", None) or str(exc) or "Stage execution failed."
                self._fail_stage(
                    run,
                    index,
                    running_stage,
                    code=_PAID_CALL_UNKNOWN if unknown else str(code or "CATALYST_STAGE_FAILED"),
                    message=(
                        "The generation provider may have completed this paid call; "
                        "automatic retry is blocked."
                        if unknown
                        else str(message)
                    ),
                    retryable=bool(retryable) and not unknown,
                    outcome_unknown=unknown,
                )
                return

            completed_at = utc_now()
            completed_stage = running_stage.model_copy(
                update={
                    "state": ProcessingStageState.COMPLETED,
                    "call_completed_at": completed_at if running_stage.external_call else None,
                    "output": output,
                    "finished_at": completed_at,
                }
            )
            run = self._replace_stage(run, index, completed_stage)
            if completed_stage.deterministic:
                self.store.save_stage_cache_for_worker(
                    run.dataset_id,
                    f"{run.operation.value}:{completed_stage.key}",
                    cache_input_digest,
                    completed_stage.recipe_digest or recipe_digest or "",
                    output,
                    completed_at.isoformat(),
                )
            run = self._append_artifacts(run, output)
            run = self._persist(run)

        if cancel_event.is_set() or run.cancellation_requested:
            self._cancel_pending(run)
            return
        now = utc_now()
        completed = self.store.finish_run_for_worker(
            run.model_copy(
                update={
                    "state": ProcessingRunState.SUCCEEDED,
                    "progress": self._progress(run.stages),
                    "updated_at": now,
                    "finished_at": now,
                    "resource_version": run.resource_version + 1,
                }
            )
        )
        if completed.cancellation_requested:
            self._cancel_pending(completed)

    def _fail_stage(
        self,
        run: ProcessingRun,
        index: int,
        stage: ProcessingStage,
        *,
        code: str,
        message: str,
        retryable: bool,
        outcome_unknown: bool,
    ) -> None:
        now = utc_now()
        failed_stage = stage.model_copy(
            update={
                "state": (
                    ProcessingStageState.OUTCOME_UNKNOWN
                    if outcome_unknown
                    else ProcessingStageState.FAILED
                ),
                "failure_code": code,
                "failure_message": message,
                "retryable": retryable,
                "finished_at": now,
            }
        )
        run = self._replace_stage(run, index, failed_stage)
        stages = [
            pending.model_copy(update={"state": ProcessingStageState.CANCELLED, "finished_at": now})
            if pending.state == ProcessingStageState.PENDING
            else pending
            for pending in run.stages
        ]
        failed = run.model_copy(
            update={
                "state": ProcessingRunState.FAILED,
                "stages": stages,
                "progress": self._progress(stages),
                "failure": ProcessingFailure(code=code, message=message, retryable=retryable),
                "updated_at": now,
                "finished_at": now,
                "resource_version": run.resource_version + 1,
            }
        )
        self._save(failed)

    def _cancel_pending(self, run: ProcessingRun) -> ProcessingRun:
        now = utc_now()
        stages = [
            stage.model_copy(update={"state": ProcessingStageState.CANCELLED, "finished_at": now})
            if stage.state == ProcessingStageState.PENDING
            else stage
            for stage in run.stages
        ]
        cancelled = run.model_copy(
            update={
                "state": ProcessingRunState.CANCELLED,
                "cancellation_requested": True,
                "stages": stages,
                "progress": self._progress(stages),
                "updated_at": now,
                "finished_at": now,
                "resource_version": run.resource_version + 1,
            }
        )
        return self._save(cancelled)

    def _replace_stage(
        self,
        run: ProcessingRun,
        index: int,
        stage: ProcessingStage,
    ) -> ProcessingRun:
        latest = self.store.get_run_for_worker(run.id)
        if latest is not None:
            run = run.model_copy(
                update={
                    "cancellation_requested": (
                        run.cancellation_requested or latest.cancellation_requested
                    ),
                    "resource_version": max(run.resource_version, latest.resource_version),
                }
            )
        stages = list(run.stages)
        stages[index] = stage
        updated = run.model_copy(
            update={
                "stages": stages,
                "progress": self._progress(stages),
                "updated_at": utc_now(),
                "resource_version": run.resource_version + 1,
            }
        )
        return self._save(updated)

    def _append_artifacts(self, run: ProcessingRun, output: dict[str, Any]) -> ProcessingRun:
        raw_artifacts = output.get("outputArtifacts", [])
        if not isinstance(raw_artifacts, list):
            return run
        artifact_models = []
        for raw in raw_artifacts:
            if isinstance(raw, dict):
                from cyrene_catalyst.domain import ArtifactRef

                artifact_models.append(ArtifactRef.model_validate(raw))
        known = {artifact.digest for artifact in run.output_artifacts}
        additional = [artifact for artifact in artifact_models if artifact.digest not in known]
        if not additional:
            return run
        return self._persist(
            run.model_copy(update={"output_artifacts": [*run.output_artifacts, *additional]})
        )

    def _persist(self, run: ProcessingRun) -> ProcessingRun:
        now = utc_now()
        latest = self.store.get_run_for_worker(run.id)
        if latest is not None:
            known = {artifact.digest for artifact in run.output_artifacts}
            run = run.model_copy(
                update={
                    "cancellation_requested": (
                        run.cancellation_requested or latest.cancellation_requested
                    ),
                    "output_artifacts": [
                        *run.output_artifacts,
                        *[
                            artifact
                            for artifact in latest.output_artifacts
                            if artifact.digest not in known
                        ],
                    ],
                    "resource_version": max(run.resource_version, latest.resource_version) + 1,
                }
            )
        return self._save(run.model_copy(update={"updated_at": now}))

    def _save(self, run: ProcessingRun) -> ProcessingRun:
        return self.store.save_run_for_worker(run)

    @staticmethod
    def _progress(stages: list[ProcessingStage]) -> ProcessingProgress:
        done = sum(
            stage.state in {ProcessingStageState.COMPLETED, ProcessingStageState.REUSED}
            for stage in stages
        )
        return ProcessingProgress(completed=done, total=len(stages))

    @staticmethod
    def _default_input_digest(run: ProcessingRun, stage: ProcessingStage) -> str:
        """Fingerprint stage identity, immutable inputs, and recipe. | 计算阶段输入摘要。"""

        payload = {
            "stage": stage.key,
            "sources": [str(source_id) for source_id in run.source_revision_ids],
            "contentRevisionId": str(run.content_revision_id) if run.content_revision_id else None,
            "recipeDigest": run.recipe_digest,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return f"sha256:{hashlib.sha256(encoded.encode('utf-8')).hexdigest()}"

    @staticmethod
    def _cache_input_digest(
        run: ProcessingRun,
        stage: ProcessingStage,
        stage_input_digest: str,
    ) -> str:
        """Bind deterministic reuse to logical source and content identities."""

        payload = {
            "stageInputDigest": stage_input_digest,
            "sourceRevisionId": (
                str(stage.source_revision_id) if stage.source_revision_id is not None else None
            ),
            "sourceRevisionIds": (
                []
                if stage.source_revision_id is not None
                else [str(source_id) for source_id in run.source_revision_ids]
            ),
            "contentRevisionId": str(run.content_revision_id) if run.content_revision_id else None,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return f"sha256:{hashlib.sha256(encoded.encode('utf-8')).hexdigest()}"
