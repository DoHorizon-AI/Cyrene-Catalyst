"""Malformed-package regressions for the dependency-free SFT consumer.

中文：验证无 Product/Plugin 依赖的 SFT 消费器会拒绝损坏或歧义包。
"""

from __future__ import annotations

import hashlib
import json
import warnings
import zipfile
from pathlib import Path
from typing import Any

import pytest

from cyrene_catalyst_consumer.training_bundle import verify_bundle


def _write_bundle(
    path: Path,
    *,
    files: dict[str, dict[str, Any]] | None = None,
    output_format: Any = "sft",
    manifest_bytes: bytes | None = None,
    extra_member: tuple[str, bytes] | None = None,
    duplicate_member: bool = False,
) -> Path:
    """Build one small valid SFT package before applying a malformed mutation."""

    content = {
        "train.jsonl": b'{"instruction":"Question","output":"Answer"}\n',
        "validation.jsonl": b"",
        "test.jsonl": b"",
        "provenance.jsonl": (
            b'{"source_family_id":"family-1","content_digest":"digest-1","split":"train"}\n'
        ),
    }
    receipts = {
        name: {
            "digest": f"sha256:{hashlib.sha256(data).hexdigest()}",
            "size_bytes": len(data),
            "row_count": int(name in {"train.jsonl", "provenance.jsonl"}),
        }
        for name, data in content.items()
    }
    manifest = {
        "mode": "sft",
        "output_format": output_format,
        "files": files if files is not None else receipts,
    }
    encoded_manifest = manifest_bytes or json.dumps(
        manifest, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in content.items():
            archive.writestr(name, data)
        archive.writestr("manifest.json", encoded_manifest)
        if duplicate_member:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                archive.writestr("provenance.jsonl", content["provenance.jsonl"])
        if extra_member is not None:
            archive.writestr(*extra_member)
    return path


def test_independent_consumer_accepts_hashed_provenance_receipt(tmp_path: Path) -> None:
    """The file table protects provenance bytes and reconciles its row count."""

    result = verify_bundle(_write_bundle(tmp_path / "valid.zip"))

    assert result["status"] == "PASS"
    assert result["published"] == 1
    assert result["splits"] == {"train": 1, "validation": 0, "test": 0}


def test_missing_provenance_file_receipt_is_rejected(tmp_path: Path) -> None:
    """A ZIP provenance member without a manifest receipt is untrusted."""

    path = tmp_path / "missing-provenance-receipt.zip"
    content = {
        "train.jsonl": b'{"instruction":"Question","output":"Answer"}\n',
        "validation.jsonl": b"",
        "test.jsonl": b"",
    }
    receipts = {
        name: {
            "digest": f"sha256:{hashlib.sha256(data).hexdigest()}",
            "size_bytes": len(data),
            "row_count": int(name == "train.jsonl"),
        }
        for name, data in content.items()
    }
    _write_bundle(path, files=receipts)

    with pytest.raises(ValueError, match="file table"):
        verify_bundle(path)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("digest", "sha256:" + "0" * 64, "digest mismatch: provenance.jsonl"),
        ("size_bytes", 999, "size mismatch: provenance.jsonl"),
        ("row_count", 2, "Provenance row count"),
    ],
)
def test_provenance_receipt_must_match_bytes_and_count(
    tmp_path: Path, field: str, value: Any, message: str
) -> None:
    """Corrupt provenance receipts cannot pass independent bundle verification."""

    path = tmp_path / f"bad-provenance-{field}.zip"
    content = {
        "train.jsonl": b'{"instruction":"Question","output":"Answer"}\n',
        "validation.jsonl": b"",
        "test.jsonl": b"",
        "provenance.jsonl": (
            b'{"source_family_id":"family-1","content_digest":"digest-1","split":"train"}\n'
        ),
    }
    receipts = {
        name: {
            "digest": f"sha256:{hashlib.sha256(data).hexdigest()}",
            "size_bytes": len(data),
            "row_count": int(name in {"train.jsonl", "provenance.jsonl"}),
        }
        for name, data in content.items()
    }
    receipts["provenance.jsonl"][field] = value
    _write_bundle(path, files=receipts)

    with pytest.raises(ValueError, match=message):
        verify_bundle(path)


def test_extra_zip_member_is_rejected(tmp_path: Path) -> None:
    """Unreceipted bytes cannot be smuggled beside the approved bundle files."""

    path = _write_bundle(tmp_path / "extra-member.zip", extra_member=("notes.txt", b"extra"))

    with pytest.raises(ValueError, match="members do not match"):
        verify_bundle(path)


def test_duplicate_zip_member_is_rejected(tmp_path: Path) -> None:
    """Ambiguous duplicate ZIP names are rejected before opening by name."""

    path = _write_bundle(tmp_path / "duplicate-member.zip", duplicate_member=True)

    with pytest.raises(ValueError, match="duplicate ZIP members"):
        verify_bundle(path)


def test_invalid_output_format_is_rejected(tmp_path: Path) -> None:
    """Malformed format metadata is not coerced into an SFT schema."""

    path = _write_bundle(tmp_path / "invalid-output-format.zip", output_format=None)

    with pytest.raises(ValueError, match="output format"):
        verify_bundle(path)


def test_duplicate_json_file_receipt_key_is_rejected(tmp_path: Path) -> None:
    """JSON object parsing must not silently replace duplicate file receipts."""

    path = tmp_path / "duplicate-file-receipt.zip"
    train_receipt = json.dumps(
        {
            "digest": "sha256:" + "a" * 64,
            "size_bytes": 1,
            "row_count": 1,
        },
        separators=(",", ":"),
    ).encode("ascii")
    remaining_receipts = {
        name: {"digest": "sha256:" + "0" * 64, "size_bytes": 0, "row_count": 0}
        for name in ("validation.jsonl", "test.jsonl", "provenance.jsonl")
    }
    remaining = b",".join(
        json.dumps(name).encode("ascii")
        + b":"
        + json.dumps(receipt, separators=(",", ":")).encode("ascii")
        for name, receipt in remaining_receipts.items()
    )
    manifest = (
        b'{"mode":"sft","files":{"train.jsonl":'
        + train_receipt
        + b',"train.jsonl":'
        + train_receipt
        + b","
        + remaining
        + b"}}"
    )
    _write_bundle(path, manifest_bytes=manifest)

    with pytest.raises(ValueError, match=r"Duplicate JSON object key: train\.jsonl"):
        verify_bundle(path)
