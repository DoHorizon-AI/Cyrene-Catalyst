"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 engine.py                                                       │
│  Module: cyrene_catalyst.engine                                     │
│  Role: Product adapter for dataset.preparation.v1.                  │
│                                                                     │
│  模块职责：Catalyst 到 Plugins 数据整理能力的标准直连适配器。               │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import csv
import datetime
import decimal
import hashlib
import json
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol

import duckdb
from pydantic import ValidationError

from cyrene_catalyst.domain import (
    EngineResult,
    ImportFormat,
    MappingConfig,
    NormalizationConfig,
    SampleError,
    SplitConfig,
    SplitStats,
)
from cyrene_catalyst.errors import DataEngineFailure

DATASET_PREPARATION_CAPABILITY = "dataset.preparation.v1"
DATASET_PREPARATION_INTERFACE_VERSION = "1"
DATASET_PREPARATION_CONNECTION_ENV = "CYRENE_DATASET_PREPARATION_CONNECTION_REF"
MAX_SOURCE_BYTES = 64 * 1024 * 1024
_TABULAR_FORMATS = {ImportFormat.CSV, ImportFormat.PARQUET}


@dataclass(frozen=True, slots=True)
class SourceInspection:
    """Product projection of one Plugins-owned source inspection."""

    source_format: ImportFormat
    rows: list[dict[str, Any]]
    detected_fields: list[str]
    row_count: int


@dataclass(frozen=True, slots=True)
class PreparedSample:
    """Product projection of one normalized sample."""

    index: int
    group_key: str
    content: dict[str, Any]
    source_row_indexes: list[int]


@dataclass(frozen=True, slots=True)
class PreparationOutput:
    """Product projection of deterministic capability output."""

    samples: list[PreparedSample]
    errors: list[SampleError]
    duplicates: list[dict[str, Any]]
    assignment: dict[int, Literal["train", "val"]]
    split_stats: SplitStats | None
    files: dict[str, dict[str, Any]]

    @property
    def group_count(self) -> int:
        return len({sample.group_key for sample in self.samples})


class DataPreparationPort(Protocol):
    """Catalyst application port with no capability implementation."""

    def inspect(self, source: Path, *, format_hint: ImportFormat | None = None) -> SourceInspection:
        """Inspect a staged source through the canonical Plugin owner."""

    def prepare(
        self,
        source: Path,
        source_format: ImportFormat,
        mapping: MappingConfig,
        normalization: NormalizationConfig,
        split: SplitConfig | None = None,
        *,
        output_dir: Path | None = None,
    ) -> PreparationOutput:
        """Run mapping/normalization/split through the canonical Plugin owner."""

    def transform(self, source: Path, destination: Path) -> EngineResult:
        """Transform a source artifact through the canonical Plugin owner."""


class DirectPluginDataPreparationPort:
    """Typed adapter for one resolved ``dataset.preparation.v1`` endpoint."""

    def __init__(self, client: Any, *, deadline_seconds: float = 60.0) -> None:
        self._client = client
        self._deadline_seconds = deadline_seconds

    @classmethod
    def from_environment(cls) -> DirectPluginDataPreparationPort:
        """Create the Product adapter from an opaque connection reference."""

        connection_ref = os.environ.get(DATASET_PREPARATION_CONNECTION_ENV, "").strip()
        if not connection_ref:
            raise DataEngineFailure(
                f"set {DATASET_PREPARATION_CONNECTION_ENV} to the resolved Plugin connection_ref"
            )
        try:
            from cyrene_plugin_runtime import DirectPluginClient

            client = DirectPluginClient.for_local_connection_ref(connection_ref)
        except ImportError as exc:
            raise DataEngineFailure("cyrene-plugin-runtime is not installed") from exc
        except (TypeError, ValueError) as exc:
            raise DataEngineFailure(f"invalid Plugin connection_ref: {exc}") from exc
        return cls(client)

    def inspect(self, source: Path, *, format_hint: ImportFormat | None = None) -> SourceInspection:
        """Inspect and validate the Plugin-written row projection."""

        source_format_hint = (
            format_hint if format_hint in _TABULAR_FORMATS else _tabular_format(source)
        )
        with (
            _plugin_source(source, source_format_hint) as plugin_source,
            _result_file() as result_path,
        ):
            receipt = self._invoke(
                "inspect",
                {
                    "source_path": str(plugin_source.resolve()),
                    "result_path": str(result_path),
                },
            )
            document = _read_verified_result(result_path, receipt)
        rows = _mapping_list(document.get("rows"), "rows")
        try:
            inspected_format = ImportFormat(_text(receipt.get("format"), "format"))
        except ValueError as exc:
            raise DataEngineFailure("Plugin returned an unsupported source format") from exc
        detected_fields = _text_list(receipt.get("detected_fields"), "detected_fields")
        row_count = _non_negative_integer(receipt.get("row_count"), "row_count")
        if row_count != len(rows):
            raise DataEngineFailure("Plugin inspection row count is inconsistent")
        _validate_inspection_schema(rows, detected_fields)
        if source_format_hint is not None:
            if inspected_format is not ImportFormat.JSONL:
                raise DataEngineFailure("Tabular source bridge did not produce JSONL")
            inspected_format = source_format_hint
        return SourceInspection(inspected_format, rows, detected_fields, row_count)

    def prepare(
        self,
        source: Path,
        source_format: ImportFormat,
        mapping: MappingConfig,
        normalization: NormalizationConfig,
        split: SplitConfig | None = None,
        *,
        output_dir: Path | None = None,
    ) -> PreparationOutput:
        """Execute preparation and validate every owner-produced projection."""

        source_format_hint = source_format if source_format in _TABULAR_FORMATS else None
        with _plugin_source(source, source_format_hint) as plugin_source:
            request: dict[str, Any] = {
                "source_path": str(plugin_source.resolve()),
                "source_format": (
                    ImportFormat.JSONL.value
                    if source_format_hint is not None
                    else source_format.value
                ),
                "mapping": mapping.model_dump(by_alias=False, exclude_none=True),
                "normalization": normalization.model_dump(by_alias=False),
                "split": split.model_dump(by_alias=False) if split else None,
            }
            if output_dir is not None:
                request["output_dir"] = str(output_dir.resolve())
            with _result_file() as result_path:
                request["result_path"] = str(result_path)
                receipt = self._invoke("prepare", request)
                document = _read_verified_result(result_path, receipt)
        samples = [
            _sample(value, index)
            for index, value in enumerate(_mapping_list(document.get("samples"), "samples"))
        ]
        try:
            errors = [
                SampleError.model_validate(value)
                for value in _mapping_list(document.get("errors"), "errors")
            ]
            split_stats = (
                None
                if document.get("split_stats") is None
                else SplitStats.model_validate(document["split_stats"])
            )
        except ValidationError as exc:
            raise DataEngineFailure("Plugin returned an invalid Product projection") from exc
        duplicates = _mapping_list(document.get("duplicates"), "duplicates")
        raw_assignment = _mapping(document.get("assignment"), "assignment")
        assignment = {
            _positive_integer(key, "assignment key"): _split_value(value)
            for key, value in raw_assignment.items()
        }
        files = _mapping(receipt.get("files"), "files")
        return PreparationOutput(samples, errors, duplicates, assignment, split_stats, files)

    def transform(self, source: Path, destination: Path) -> EngineResult:
        """Execute the Product transform and verify the produced bytes."""

        source_format = _tabular_format(source)
        if source_format in _TABULAR_FORMATS:
            return _transform_tabular(source, destination, source_format)

        receipt = self._invoke(
            "transform",
            {
                "source_path": str(source.resolve()),
                "destination_path": str(destination.resolve()),
            },
        )
        expected_digest = _text(receipt.get("output_digest"), "output_digest")
        expected_size = _non_negative_integer(receipt.get("output_size"), "output_size")
        if not destination.is_file() or destination.stat().st_size != expected_size:
            raise DataEngineFailure("Plugin transform output is missing or truncated")
        if f"sha256:{_sha256_file(destination)}" != expected_digest:
            raise DataEngineFailure("Plugin transform output failed digest verification")
        return EngineResult(
            row_count=_non_negative_integer(receipt.get("row_count"), "row_count"),
            schema_fields=_text_list(receipt.get("schema_fields"), "schema_fields"),
        )

    def _invoke(self, method: str, request: dict[str, Any]) -> dict[str, Any]:
        """Invoke one typed method and decode a strict object response."""

        try:
            from cyrene_plugin_runtime import DirectPayload, DirectPluginError

            response = self._client.invoke(
                capability=DATASET_PREPARATION_CAPABILITY,
                interface_version=DATASET_PREPARATION_INTERFACE_VERSION,
                method=method,
                request=DirectPayload(
                    type_url=f"type.cyrene.io/{DATASET_PREPARATION_CAPABILITY}.{method}.request",
                    value=json.dumps(
                        request, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                    ).encode("utf-8"),
                ),
                deadline_seconds=self._deadline_seconds,
            )
        except ImportError as exc:
            raise DataEngineFailure("cyrene-plugin-runtime is not installed") from exc
        except DirectPluginError as exc:
            raise DataEngineFailure(f"dataset preparation Plugin failed: {exc}") from exc
        expected_type = f"type.cyrene.io/{DATASET_PREPARATION_CAPABILITY}.{method}.response"
        if response.type_url != expected_type:
            raise DataEngineFailure("dataset preparation Plugin returned an unexpected type")
        try:
            document = json.loads(response.value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DataEngineFailure("dataset preparation Plugin returned invalid JSON") from exc
        return _mapping(document, "response")


class UnavailableDataPreparationPort:
    """Fail-closed Product adapter used when no Plugin binding exists."""

    def __init__(self, reason: str) -> None:
        self._reason = reason

    def inspect(self, source: Path, *, format_hint: ImportFormat | None = None) -> SourceInspection:
        del source, format_hint
        raise DataEngineFailure(f"dataset preparation unavailable: {self._reason}")

    def prepare(
        self,
        source: Path,
        source_format: ImportFormat,
        mapping: MappingConfig,
        normalization: NormalizationConfig,
        split: SplitConfig | None = None,
        *,
        output_dir: Path | None = None,
    ) -> PreparationOutput:
        del source, source_format, mapping, normalization, split, output_dir
        raise DataEngineFailure(f"dataset preparation unavailable: {self._reason}")

    def transform(self, source: Path, destination: Path) -> EngineResult:
        del source, destination
        raise DataEngineFailure(f"dataset preparation unavailable: {self._reason}")


def data_preparation_from_environment() -> DataPreparationPort:
    """Resolve the direct Plugin adapter without any local implementation fallback."""

    try:
        return DirectPluginDataPreparationPort.from_environment()
    except DataEngineFailure as exc:
        return UnavailableDataPreparationPort(str(exc))


class _ResultFile:
    def __init__(self) -> None:
        descriptor, name = tempfile.mkstemp(prefix="catalyst-plugin-result-", suffix=".json")
        os.close(descriptor)
        self.path = Path(name).resolve()

    def __enter__(self) -> Path:
        return self.path

    def __exit__(self, *_args: object) -> None:
        self.path.unlink(missing_ok=True)


def _result_file() -> _ResultFile:
    return _ResultFile()


def _read_verified_result(path: Path, receipt: dict[str, Any]) -> dict[str, Any]:
    expected_size = _non_negative_integer(receipt.get("result_size"), "result_size")
    expected_digest = _text(receipt.get("result_digest"), "result_digest")
    if not path.is_file() or path.stat().st_size != expected_size:
        raise DataEngineFailure("Plugin result file is missing or truncated")
    payload = path.read_bytes()
    if f"sha256:{hashlib.sha256(payload).hexdigest()}" != expected_digest:
        raise DataEngineFailure("Plugin result file failed digest verification")
    try:
        return _mapping(json.loads(payload), "result")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DataEngineFailure("Plugin result file is invalid JSON") from exc


def _sample(value: dict[str, Any], index: int) -> PreparedSample:
    return PreparedSample(
        index=_positive_integer(value.get("index"), f"samples[{index}].index"),
        group_key=_text(value.get("group_key"), f"samples[{index}].group_key"),
        content=_mapping(value.get("content"), f"samples[{index}].content"),
        source_row_indexes=[
            _positive_integer(item, f"samples[{index}].source_row_indexes")
            for item in _list(value.get("source_row_indexes"), "source_row_indexes")
        ],
    )


def _mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise DataEngineFailure(f"Plugin {field} must be an object")
    return value


def _mapping_list(value: Any, field: str) -> list[dict[str, Any]]:
    items = _list(value, field)
    if not all(isinstance(item, dict) for item in items):
        raise DataEngineFailure(f"Plugin {field} must contain only objects")
    return items


def _list(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise DataEngineFailure(f"Plugin {field} must be an array")
    return value


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise DataEngineFailure(f"Plugin {field} must be non-empty text")
    return value


def _text_list(value: Any, field: str) -> list[str]:
    items = _list(value, field)
    if not all(isinstance(item, str) for item in items):
        raise DataEngineFailure(f"Plugin {field} must contain only text")
    return items


def _non_negative_integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DataEngineFailure(f"Plugin {field} must be a non-negative integer")
    return value


def _positive_integer(value: Any, field: str) -> int:
    integer = int(value) if isinstance(value, str) and value.isdigit() else value
    if isinstance(integer, bool) or not isinstance(integer, int) or integer < 1:
        raise DataEngineFailure(f"Plugin {field} must be a positive integer")
    return integer


def _split_value(value: Any) -> Literal["train", "val"]:
    if value == "train":
        return "train"
    if value == "val":
        return "val"
    raise DataEngineFailure("Plugin assignment values must be train or val")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _tabular_format(source: Path) -> ImportFormat | None:
    """Return a tabular format hint from a staged source path. | 识别表格格式。"""

    suffix = source.suffix.casefold()
    if suffix == ".csv":
        return ImportFormat.CSV
    if suffix in {".parquet", ".pq"} or _has_parquet_magic(source):
        return ImportFormat.PARQUET
    if _looks_like_csv(source):
        return ImportFormat.CSV
    return None


def _has_parquet_magic(source: Path) -> bool:
    """Check Parquet's leading and trailing magic bytes without parsing it."""

    try:
        size = source.stat().st_size
        if size < 8:
            return False
        with source.open("rb") as stream:
            if stream.read(4) != b"PAR1":
                return False
            stream.seek(-4, os.SEEK_END)
            return stream.read(4) == b"PAR1"
    except OSError:
        return False


def _looks_like_csv(source: Path) -> bool:
    """Recognize header-based CSV artifacts whose CAS path has no suffix."""

    try:
        with source.open("r", encoding="utf-8-sig", newline="") as stream:
            first = stream.read(1)
            while first and first.isspace():
                first = stream.read(1)
            if first in {"{", "["}:
                return False
            stream.seek(0)
            rows = csv.reader(stream)
            header = next(rows, None)
            first_row = next(rows, None)
    except (OSError, UnicodeError, csv.Error):
        return False
    return bool(
        header and first_row and len(header) > 1 and len(header) == len(first_row) and all(header)
    )


@contextmanager
def _plugin_source(source: Path, source_format: ImportFormat | None) -> Iterator[Path]:
    """Bridge tabular bytes to the pinned Plugin's JSONL input contract.

    The bridge only changes the serialization envelope. Mapping, normalization,
    deduplication, splitting, and output ownership remain in the Plugin.
    """

    if source_format not in _TABULAR_FORMATS:
        yield source
        return

    rows = _read_tabular_rows(source, source_format)
    if not rows:
        raise DataEngineFailure("tabular source contains no rows")
    descriptor, name = tempfile.mkstemp(prefix="catalyst-tabular-", suffix=".jsonl")
    os.close(descriptor)
    bridge = Path(name).resolve()
    try:
        bridge.write_bytes(b"".join(_canonical_json(row) + b"\n" for row in rows))
        yield bridge
    finally:
        bridge.unlink(missing_ok=True)


def _read_tabular_rows(source: Path, source_format: ImportFormat) -> list[dict[str, Any]]:
    """Read CSV or Parquet into the Plugin's generic row projection."""

    _validate_tabular_source(source, source_format)
    connection = duckdb.connect()
    try:
        relation = _tabular_relation(connection, source, source_format)
        columns = list(relation.columns)
        _validate_column_names(columns)
        return [
            {column: _json_native(value) for column, value in zip(columns, values, strict=True)}
            for values in relation.fetchall()
        ]
    except duckdb.Error as exc:
        raise DataEngineFailure("tabular source could not be parsed") from exc
    finally:
        connection.close()


def _transform_tabular(
    source: Path, destination: Path, source_format: ImportFormat
) -> EngineResult:
    """Convert a CSV or Parquet source to a verified Parquet artifact."""

    _validate_tabular_source(source, source_format)
    destination.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        relation = _tabular_relation(connection, source, source_format)
        columns = list(relation.columns)
        _validate_column_names(columns)
        row = relation.aggregate("count(*) AS row_count").fetchone()
        relation.write_parquet(str(destination), compression="zstd")
        return EngineResult(
            row_count=int(row[0]) if row is not None else 0,
            schema_fields=columns,
        )
    except duckdb.Error as exc:
        raise DataEngineFailure("tabular source could not be transformed") from exc
    finally:
        connection.close()


def _tabular_relation(
    connection: duckdb.DuckDBPyConnection,
    source: Path,
    source_format: ImportFormat,
) -> duckdb.DuckDBPyRelation:
    """Open one tabular source through DuckDB's typed readers."""

    if source_format is ImportFormat.CSV:
        _validate_csv_header(source)
        return connection.read_csv(str(source), header=True, auto_detect=True)
    if source_format is ImportFormat.PARQUET:
        return connection.read_parquet(str(source))
    raise DataEngineFailure(f"unsupported tabular format: {source_format.value}")


def _validate_tabular_source(source: Path, source_format: ImportFormat) -> None:
    """Apply regular-file, size, and format-specific source checks."""

    if not source.is_file() or source.is_symlink():
        raise DataEngineFailure("tabular source must be a regular file")
    if source.stat().st_size > MAX_SOURCE_BYTES:
        raise DataEngineFailure("tabular source exceeds 64 MiB")
    if source_format is ImportFormat.PARQUET and not _has_parquet_magic(source):
        raise DataEngineFailure("Parquet source has invalid magic bytes")


def _validate_csv_header(source: Path) -> None:
    """Reject blank, duplicate, or ragged CSV columns before DuckDB reads them."""

    try:
        with source.open("r", encoding="utf-8-sig", newline="") as stream:
            rows = csv.reader(stream)
            header = next(rows, None)
            if (
                not header
                or any(not column for column in header)
                or len(set(header)) != len(header)
            ):
                raise DataEngineFailure("CSV source must contain unique non-empty column names")
            for row_index, row in enumerate(rows, start=2):
                if len(row) != len(header):
                    raise DataEngineFailure(
                        f"CSV row {row_index} contains unknown or missing columns"
                    )
    except (OSError, UnicodeError, csv.Error) as exc:
        raise DataEngineFailure("CSV source header is invalid") from exc


def _validate_column_names(columns: list[str]) -> None:
    """Reject schemas DuckDB or the Product contract cannot represent strictly."""

    if (
        not columns
        or any(not isinstance(column, str) or not column for column in columns)
        or len(set(columns)) != len(columns)
    ):
        raise DataEngineFailure("tabular source must contain unique non-empty columns")


def _validate_inspection_schema(rows: list[dict[str, Any]], detected_fields: list[str]) -> None:
    """Reject owner projections containing columns outside their declared schema."""

    if (
        len(detected_fields) > 500
        or len(set(detected_fields)) != len(detected_fields)
        or any(not field for field in detected_fields)
    ):
        raise DataEngineFailure("Plugin returned an invalid detected field schema")
    declared = set(detected_fields)
    for row_index, row in enumerate(rows, start=1):
        unknown = set(row) - declared
        if unknown:
            raise DataEngineFailure(
                f"Plugin row {row_index} contains unknown columns: {sorted(unknown)}"
            )


def _json_native(value: Any) -> Any:
    """Project DuckDB values onto JSON-native values for the Plugin bridge."""

    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, datetime.datetime | datetime.date | datetime.time):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, bytes | bytearray):
        return bytes(value).decode("utf-8", "replace")
    if isinstance(value, dict):
        return {str(key): _json_native(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_native(item) for item in value]
    return str(value)


def _canonical_json(value: Any) -> bytes:
    """Encode bridge rows deterministically."""

    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )


__all__ = [
    "DATASET_PREPARATION_CAPABILITY",
    "DATASET_PREPARATION_CONNECTION_ENV",
    "DATASET_PREPARATION_INTERFACE_VERSION",
    "DataPreparationPort",
    "DirectPluginDataPreparationPort",
    "PreparationOutput",
    "PreparedSample",
    "SourceInspection",
    "UnavailableDataPreparationPort",
    "data_preparation_from_environment",
]
