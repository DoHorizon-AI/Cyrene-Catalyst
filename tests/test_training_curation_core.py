"""Core artifact-backed training curation and publication checks."""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from cyrene_catalyst.artifacts import LocalArtifactPlane
from cyrene_catalyst.data_tools_domain import (
    ContentPolicy,
    ContentRevision,
    ContentRevisionState,
    ProcessingOperation,
    ProcessingRun,
    SourceParseReport,
    SourceRevision,
    TrainingCurationCounts,
    TrainingDataSnapshot,
)
from cyrene_catalyst.data_tools_service import DataToolsService
from cyrene_catalyst.domain import ArtifactRef
from cyrene_catalyst.errors import CatalystError
from cyrene_catalyst.processing_runs import StageExecutionFailure


def _package(plane: LocalArtifactPlane, row: dict[str, object]) -> ArtifactRef:
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("train.jsonl", json.dumps(row, ensure_ascii=False) + "\n")
    return plane.ingest_bytes(data.getvalue(), "test-sft-package.zip")


def _service(plane: LocalArtifactPlane) -> DataToolsService:
    service = object.__new__(DataToolsService)
    service.artifacts = plane
    return service


def test_messages_export_is_streamed_as_a_learned_only_artifact(tmp_path: Path) -> None:
    plane = LocalArtifactPlane(tmp_path / "artifacts")
    package = _package(
        plane,
        {
            "messages": [
                {"role": "system", "content": "Be precise."},
                {"role": "user", "content": "第一轮"},
                {"role": "assistant", "content": "First answer"},
                {"role": "user", "content": "第二轮"},
                {"role": "assistant", "content": "Second answer"},
            ]
        },
    )

    run = ProcessingRun(
        id=uuid4(),
        dataset_id=uuid4(),
        operation=ProcessingOperation.PREPARE_SFT,
    )
    artifact, count, fields = _service(plane)._publish_sft_train(
        package,
        run,
        output_format="messages",
    )

    assert count == 1
    assert fields == ["messages"]
    published = plane.resolve(artifact).read_bytes()
    assert json.loads(published) == {
        "messages": [
            {"role": "system", "content": "Be precise."},
            {"role": "user", "content": "第一轮"},
            {"role": "assistant", "content": "First answer"},
            {"role": "user", "content": "第二轮"},
            {"role": "assistant", "content": "Second answer"},
        ]
    }
    assert artifact.digest == f"sha256:{hashlib.sha256(published).hexdigest()}"


@pytest.mark.parametrize(
    ("output_format", "row"),
    [
        (
            "messages",
            {"messages": [{"role": "assistant", "content": "orphaned answer"}]},
        ),
        (
            "messages",
            {
                "messages": [
                    {"role": "user", "content": "prompt"},
                    {"role": "assistant", "content": "answer"},
                ],
                "sourceRevisionId": "must stay in provenance",
            },
        ),
        (
            "promptCompletion",
            {"prompt": "question", "completion": "answer", "sourceFamilyId": "private"},
        ),
    ],
)
def test_export_validation_rejects_lossy_or_management_fields(
    output_format: str, row: dict[str, object]
) -> None:
    with pytest.raises(ValueError):
        DataToolsService._validate_curation_export_row(row, output_format, 1)


def test_unsupported_warning_cannot_be_approved_as_training(tmp_path: Path) -> None:
    service = _service(LocalArtifactPlane(tmp_path / "artifacts"))
    envelope: dict[str, object] = {
        "disposition": "review",
        "normalized": {"messages": [{"role": "user", "content": "question"}]},
        "policy": {
            "allowTraining": True,
            "allowedUsePurposes": ["model_training"],
        },
        "issues": [
            {
                "code": "UNSUPPORTED_TOOL_CALL",
                "message": "Tool output cannot be flattened to text.",
                "severity": "warning",
            }
        ],
    }

    with pytest.raises(CatalystError) as error:
        service._apply_training_record_edit(
            envelope,
            {"recordId": "record-1", "action": "approve"},
            recipe={},
            recipe_digest="sha256:" + "a" * 64,
            note=None,
        )

    assert error.value.code == "CATALYST_TRAINING_RECORD_APPROVAL_INVALID"


@pytest.mark.parametrize(
    ("changed_field", "changed_value"),
    [
        ("sourceFamilyId", "attacker-selected-family"),
        (
            "policy",
            {"allowTraining": True, "allowedUsePurposes": ["model_training"]},
        ),
    ],
)
def test_remap_receipt_cannot_change_lineage_or_grant_policy(
    tmp_path: Path, changed_field: str, changed_value: object
) -> None:
    service = _service(LocalArtifactPlane(tmp_path / "artifacts"))
    recipe_digest = "sha256:" + "b" * 64
    original_history = [{"operation": "normalize", "mode": "deterministic"}]
    envelope: dict[str, object] = {
        "schemaVersion": "cyrene.training-record.v1",
        "id": "record:source:1",
        "sourceRevisionId": "source",
        "sourceFamilyId": "source-family",
        "sampleId": "sample-1",
        "conversationId": None,
        "ordinal": 1,
        "locator": {"itemRef": "line:1"},
        "policy": {"allowTraining": False, "allowedUsePurposes": []},
        "recipeDigest": recipe_digest,
        "rawRecord": {"task": "question", "answer": "answer"},
        "rawLine": '{"task":"question","answer":"answer"}',
        "normalized": None,
        "detectedFormat": "unknown",
        "disposition": "review",
        "issues": [{"code": "FORMAT_UNRECOGNIZED", "message": "unrecognized"}],
        "contentDigest": "sha256:" + "c" * 64,
        "processingHistory": original_history,
    }
    response = {
        **envelope,
        "normalized": {"messages": [{"role": "user", "content": "question"}]},
        "detectedFormat": "alpaca",
        "issues": [],
        "processingHistory": [*original_history, {"operation": "humanRemap"}],
        "contentDigest": "sha256:" + "d" * 64,
        changed_field: changed_value,
    }

    def invoke(**_: object) -> dict[str, object]:
        return response

    cast(Any, service)._invoke_plugin = invoke

    with pytest.raises(StageExecutionFailure, match="immutable record lineage"):
        service._apply_training_record_edit(
            envelope,
            {
                "recordId": "record:source:1",
                "action": "remap",
                "format": "alpaca",
                "fieldMapping": {"instruction": "task", "output": "answer"},
            },
            recipe={"id": "curation", "version": "1"},
            recipe_digest=recipe_digest,
            note=None,
        )


def test_sft_history_schema_preserves_all_turns_and_has_no_metadata() -> None:
    DataToolsService._validate_curation_export_row(
        {
            "instruction": "latest question",
            "input": "",
            "output": "latest answer",
            "system": "",
            "history": [["earlier question", "earlier answer"]],
        },
        "sft",
        1,
    )


def test_sft_retry_uses_fresh_export_directory_after_partial_failure(tmp_path: Path) -> None:
    plane = LocalArtifactPlane(tmp_path / "artifacts")
    dataset_id = uuid4()
    revision_id = uuid4()
    source_id = uuid4()
    recipe_digest = "sha256:" + "e" * 64
    raw_record = {"sampleId": "sample-1", "instruction": "question", "output": "answer"}
    raw_line = json.dumps(raw_record, ensure_ascii=False, separators=(",", ":")) + "\n"
    normalized = {
        "messages": [
            {"role": "user", "content": "question"},
            {"role": "assistant", "content": "answer"},
        ]
    }

    def canonical(value: object) -> bytes:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )

    envelope = {
        "schemaVersion": "cyrene.training-record.v1",
        "id": f"record:{source_id}:0",
        "sampleId": "sample-1",
        "sourceRevisionId": str(source_id),
        "sourceFamilyId": "family-1",
        "conversationId": None,
        "ordinal": 0,
        "locator": {"itemRef": "line:1"},
        "rawRecord": raw_record,
        "rawLine": raw_line,
        "detectedFormat": "alpaca",
        "normalized": normalized,
        "disposition": "eligible",
        "issues": [],
        "contentDigest": f"sha256:{hashlib.sha256(canonical(normalized)).hexdigest()}",
        "recipeDigest": recipe_digest,
        "processingHistory": [{"operation": "normalize", "mode": "deterministic"}],
        "policy": {"allowTraining": True, "allowedUsePurposes": ["model_training"]},
    }
    source_snapshot = plane.ingest_bytes(canonical(envelope) + b"\n", "records.jsonl")
    revision = ContentRevision(
        id=revision_id,
        dataset_id=dataset_id,
        revision=1,
        source_revision_ids=[source_id],
        training_data_snapshot=TrainingDataSnapshot(
            schema_version="cyrene.training-record.v1",
            artifact=source_snapshot,
            record_count=1,
            counts=TrainingCurationCounts(total=1, recognized=1, eligible=1),
        ),
        state=ContentRevisionState.APPROVED,
    )
    service = _service(plane)
    service_any = cast(Any, service)
    service_any.store = SimpleNamespace(
        get_content_revision_for_worker=lambda requested_id: (
            revision if requested_id == revision_id else None
        )
    )
    run = ProcessingRun(
        id=uuid4(),
        dataset_id=dataset_id,
        operation=ProcessingOperation.PREPARE_SFT,
        content_revision_id=revision_id,
        recipe={
            "config": {
                "outputFormat": "sft",
                "split": {"train": 0.8, "validation": 0.1, "test": 0.1},
            }
        },
    )
    output_directories: list[Path] = []
    partial_outputs: list[Path] = []
    attempts = 0

    def invoke(**kwargs: object) -> dict[str, object]:
        nonlocal attempts
        attempts += 1
        request = kwargs["request"]
        assert isinstance(request, dict)
        output_dir = Path(str(request["output_dir"]))
        output_directories.append(output_dir)
        if attempts == 1:
            partial = output_dir / "partial.jsonl"
            partial.write_text('{"partial": true}\n', encoding="utf-8")
            partial_outputs.append(partial)
            raise StageExecutionFailure(
                "CATALYST_PLUGIN_FAILED",
                "Simulated exporter interruption after writing a partial file.",
                retryable=True,
                outcome_unknown=False,
            )

        archive_path = output_dir / "bundle.zip"
        learned_row = {
            "instruction": "question",
            "input": "",
            "output": "answer",
            "system": "",
            "history": [],
        }
        provenance_row = {
            "record_id": envelope["id"],
            "sample_id": envelope["sampleId"],
            "source_revision_id": envelope["sourceRevisionId"],
            "source_family_id": envelope["sourceFamilyId"],
            "conversation_id": envelope["conversationId"],
            "locator": envelope["locator"],
            "raw_digest": f"sha256:{hashlib.sha256(raw_line.encode()).hexdigest()}",
            "content_digest": envelope["contentDigest"],
            "curation_recipe_digest": recipe_digest,
            "processing_history": envelope["processingHistory"],
            "policy": envelope["policy"],
            "issues": [],
            "split": "train",
        }
        file_bytes = {
            "train.jsonl": canonical(learned_row) + b"\n",
            "validation.jsonl": b"",
            "test.jsonl": b"",
            "provenance.jsonl": canonical(provenance_row) + b"\n",
        }
        receipts: dict[str, dict[str, object]] = {}
        for name, data in file_bytes.items():
            receipts[name] = {
                "size_bytes": len(data),
                "digest": f"sha256:{hashlib.sha256(data).hexdigest()}",
                "row_count": 1 if name in {"train.jsonl", "provenance.jsonl"} else 0,
                **(
                    {"schema_fields": ["instruction", "input", "output", "system", "history"]}
                    if name.endswith(".jsonl") and name != "provenance.jsonl"
                    else {}
                ),
            }
        split_stats = {
            "algorithm": "family-content-conversation.v1",
            "ratios": {"train": 0.8, "validation": 0.1, "test": 0.1},
            "seed": 42,
            "samples": {"train": 1, "validation": 0, "test": 0},
            "lineage_components": {"train": 1, "validation": 0, "test": 0},
        }
        expected_counts = {
            "total": 1,
            "recognized": 1,
            "formatErrors": 0,
            "duplicateCandidates": 0,
            "pendingReview": 0,
            "eligible": 1,
            "excluded": 0,
            "policyExcluded": 0,
            "published": 1,
        }
        manifest = {
            "schema_version": "cyrene.sft.bundle.v1",
            "profile": "CYRENE_SFT_BUNDLE_V1",
            "dataset_id": str(dataset_id),
            "content_revision_id": str(revision_id),
            "processing_run_id": str(run.id),
            "mode": "sft",
            "schema": "instruction_history",
            "split_ratios": {"train": 0.8, "validation": 0.1, "test": 0.1},
            "split_seed": 42,
            "split_stats": split_stats,
            "counts": {
                **expected_counts,
                "train": 1,
                "validation": 0,
                "test": 0,
            },
            "recipe": {
                "capability": "dataset.generation.v1",
                "method": "prepare_training_sft",
                "version": run.recipe_version,
                "digest": run.recipe_digest,
            },
            "source_recipe_digests": [recipe_digest],
            "files": receipts,
        }
        manifest_bytes = canonical(manifest) + b"\n"
        file_bytes["manifest.json"] = manifest_bytes
        receipts["manifest.json"] = {
            "size_bytes": len(manifest_bytes),
            "digest": f"sha256:{hashlib.sha256(manifest_bytes).hexdigest()}",
            "row_count": 1,
        }
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, data in file_bytes.items():
                archive.writestr(name, data)
        bundle = archive_path.read_bytes()
        result: dict[str, object] = {
            "profile": "CYRENE_SFT_BUNDLE_V1",
            "bundle_path": str(archive_path),
            "bundle_digest": f"sha256:{hashlib.sha256(bundle).hexdigest()}",
            "bundle_size_bytes": len(bundle),
            "sample_count": 1,
            "counts": {
                **expected_counts,
                "review": 0,
            },
            "files": receipts,
            "split_stats": split_stats,
            "recipe_digest": run.recipe_digest,
            "source_recipe_digests": [recipe_digest],
            "output_format": "sft",
            "warnings": [],
        }
        (output_dir / "result.json").write_text(json.dumps(result), encoding="utf-8")
        return result

    service_any._invoke_plugin = invoke

    with pytest.raises(StageExecutionFailure, match="Simulated exporter interruption"):
        service._prepare_sft(run, Event())
    assert not partial_outputs[0].exists()
    result = service._prepare_sft(run, Event())

    assert attempts == 2
    assert output_directories[0] != output_directories[1]
    assert not output_directories[0].exists()
    assert not output_directories[1].exists()
    published = result["packageArtifact"]
    assert isinstance(published, dict)
    artifact = ArtifactRef.model_validate(published)
    assert plane.resolve(artifact).is_file()


def test_curation_retry_resumes_real_plugin_checkpoint_across_run_ids(tmp_path: Path) -> None:
    plugin = pytest.importorskip("training_curation")
    plane = LocalArtifactPlane(tmp_path / "artifacts")
    dataset_id = uuid4()
    source_revision_id = uuid4()
    source_family_id = uuid4()
    source_bytes = b"".join(
        json.dumps(
            {
                "instruction": f"question {index}",
                "input": "",
                "output": f"answer {index}",
            }
        ).encode()
        + b"\n"
        for index in range(3)
    )
    source_artifact = plane.ingest_bytes(source_bytes, "training.jsonl")
    source = SourceRevision(
        id=source_revision_id,
        dataset_id=dataset_id,
        source_id=source_family_id,
        revision=1,
        filename="training.jsonl",
        media_type="application/jsonl",
        byte_length=source_artifact.size_bytes,
        digest=source_artifact.digest,
        artifact=source_artifact,
    )
    source_policy = ContentPolicy(
        allow_training=True,
        allowed_use_purposes=["model_training"],
    )
    recipe = {
        "config": {
            "curation": {
                "id": "training-data-v1",
                "version": "1",
                "format": "auto",
                "fieldMapping": {},
                "roleMapping": {},
                "maxCharacters": 100_000,
                "minCharacters": 2,
                "unicodeNormalization": "NFC",
            },
            "sourcePolicies": {str(source_revision_id): source_policy.model_dump(by_alias=True)},
        }
    }

    class FakeStore:
        def __init__(self) -> None:
            self.reports: dict[UUID, SourceParseReport] = {}
            self.saved_revision: ContentRevision | None = None

        def get_source_for_worker(self, requested_id: UUID) -> SourceRevision | None:
            return source if requested_id == source_revision_id else None

        def list_source_parse_reports_for_worker(
            self, requested_dataset_id: UUID, processing_run_id: UUID
        ) -> list[SourceParseReport]:
            del requested_dataset_id
            return [
                report
                for report in self.reports.values()
                if report.processing_run_id == processing_run_id
            ]

        def save_source_parse_report_for_worker(
            self, report: SourceParseReport
        ) -> SourceParseReport:
            self.reports[report.id] = report
            return report

        def finalize_training_content_revision_for_worker(
            self,
            revision: ContentRevision,
            reports: list[SourceParseReport],
            review_items: object,
        ) -> ContentRevision:
            del review_items
            self.saved_revision = revision
            for report in reports:
                self.reports[report.id] = report
            return revision

    service = _service(plane)
    service_any = cast(Any, service)
    service_any.store = FakeStore()
    attempts: list[tuple[str, str]] = []

    class CancelAfterOneRecord:
        def __init__(self, cancel: bool) -> None:
            self.cancel = cancel
            self.checks = 0

        def is_cancelled(self) -> bool:
            self.checks += 1
            return self.cancel and self.checks == 2

    def invoke(**kwargs: object) -> dict[str, object]:
        request = kwargs["request"]
        assert isinstance(request, dict)
        attempts.append((str(request["result_path"]), str(request["checkpoint_path"])))
        cancellation = CancelAfterOneRecord(cancel=len(attempts) == 1)
        try:
            response = plugin.curate_training_records(**request, cancellation=cancellation)
            return cast(dict[str, object], response)
        except plugin.CurationCancelled as exc:
            raise StageExecutionFailure(
                "CATALYST_RUN_CANCELLED",
                "The test interrupted curation after the first committed record.",
                retryable=True,
                outcome_unknown=False,
            ) from exc

    service_any._worker_sources = lambda source_ids: [source]
    service_any._invoke_plugin = invoke
    first_run = ProcessingRun(
        id=uuid4(),
        dataset_id=dataset_id,
        operation=ProcessingOperation.CURATE_TRAINING_DATA,
        source_revision_ids=[source_revision_id],
        recipe=recipe,
    )
    with pytest.raises(StageExecutionFailure, match="interrupted curation") as interrupted:
        service._curate_training_data(first_run, Event())
    assert interrupted.value.retryable is True
    assert Path(attempts[0][0]).is_file()
    assert Path(attempts[0][1]).is_file()

    retry_run = ProcessingRun(
        id=uuid4(),
        dataset_id=dataset_id,
        operation=ProcessingOperation.CURATE_TRAINING_DATA,
        source_revision_ids=[source_revision_id],
        recipe=recipe,
        retry_of_run_id=first_run.id,
        attempt=2,
    )
    receipt = service._curate_training_data(retry_run, Event())

    assert len(attempts) == 2
    assert attempts[0] == attempts[1]
    assert receipt["pluginReceipt"]["resumed_records"] == 1
    saved_revision = service_any.store.saved_revision
    assert isinstance(saved_revision, ContentRevision)
    assert saved_revision.training_data_snapshot is not None
    assert saved_revision.training_data_snapshot.record_count == 3
    snapshot_path = plane.resolve(saved_revision.training_data_snapshot.artifact)
    assert len(snapshot_path.read_text(encoding="utf-8").splitlines()) == 3
    assert not Path(attempts[0][0]).exists()
    assert not Path(attempts[0][1]).exists()


def test_reimported_source_bytes_are_deduplicated_across_revisions(tmp_path: Path) -> None:
    plugin = pytest.importorskip("training_curation")
    plane = LocalArtifactPlane(tmp_path / "artifacts")
    dataset_id = uuid4()
    source_bytes = (
        json.dumps(
            {
                "sampleId": "reimported-sample",
                "instruction": "What is the answer?",
                "input": "",
                "output": "The answer is 42.",
            },
            separators=(",", ":"),
        ).encode()
        + b"\n"
    )
    source_artifact = plane.ingest_bytes(source_bytes, "training.jsonl")
    sources = [
        SourceRevision(
            id=uuid4(),
            dataset_id=dataset_id,
            source_id=uuid4(),
            revision=1,
            filename="training.jsonl",
            media_type="application/jsonl",
            byte_length=source_artifact.size_bytes,
            digest=source_artifact.digest,
            artifact=source_artifact,
        )
        for _ in range(2)
    ]
    source_policy = ContentPolicy(
        allow_training=True,
        allowed_use_purposes=["model_training"],
    ).model_dump(by_alias=True)
    recipe = {
        "config": {
            "curation": {
                "id": "training-data-v1",
                "version": "1",
                "format": "auto",
                "fieldMapping": {},
                "roleMapping": {},
                "maxCharacters": 100_000,
                "minCharacters": 2,
                "unicodeNormalization": "NFC",
            },
            "sourcePolicies": {str(source.id): source_policy for source in sources},
        }
    }

    class FakeStore:
        def __init__(self) -> None:
            self.sources = {source.id: source for source in sources}
            self.reports: dict[UUID, SourceParseReport] = {}
            self.saved_revision: ContentRevision | None = None

        def get_source_for_worker(self, requested_id: UUID) -> SourceRevision | None:
            return self.sources.get(requested_id)

        def list_source_parse_reports_for_worker(
            self, requested_dataset_id: UUID, processing_run_id: UUID
        ) -> list[SourceParseReport]:
            del requested_dataset_id
            return [
                report
                for report in self.reports.values()
                if report.processing_run_id == processing_run_id
            ]

        def save_source_parse_report_for_worker(
            self, report: SourceParseReport
        ) -> SourceParseReport:
            self.reports[report.id] = report
            return report

        def finalize_training_content_revision_for_worker(
            self,
            revision: ContentRevision,
            reports: list[SourceParseReport],
            review_items: object,
        ) -> ContentRevision:
            del review_items
            self.saved_revision = revision
            for report in reports:
                self.reports[report.id] = report
            return revision

    service = _service(plane)
    service_any = cast(Any, service)
    store = FakeStore()
    service_any.store = store

    def invoke(**kwargs: object) -> dict[str, object]:
        request = kwargs["request"]
        assert isinstance(request, dict)
        response = plugin.curate_training_records(**request)
        return cast(dict[str, object], response)

    service_any._invoke_plugin = invoke
    run = ProcessingRun(
        id=uuid4(),
        dataset_id=dataset_id,
        operation=ProcessingOperation.CURATE_TRAINING_DATA,
        source_revision_ids=[source.id for source in sources],
        recipe=recipe,
    )

    result = service._curate_training_data(run, Event())

    counts = result["trainingDataSnapshot"]["counts"]
    assert counts == {
        "total": 2,
        "recognized": 2,
        "formatErrors": 0,
        "duplicateCandidates": 1,
        "pendingReview": 0,
        "eligible": 1,
        "excluded": 1,
    }
    saved_revision = store.saved_revision
    assert isinstance(saved_revision, ContentRevision)
    assert saved_revision.source_revision_ids == sorted([source.id for source in sources], key=str)
    assert saved_revision.training_data_snapshot is not None
    records = list(
        service._iter_training_record_envelopes(
            plane.resolve(saved_revision.training_data_snapshot.artifact)
        )
    )
    assert {record["sourceRevisionId"] for record in records} == {
        str(source.id) for source in sources
    }
    assert sorted(record["disposition"] for record in records) == ["eligible", "excluded"]
