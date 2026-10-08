"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 verify_training_bundle.py                                       │
│  Role: Independently verify exported training bytes and lineage.     │
│  中文：独立读取训练包，核验文件摘要、严格行结构、数量和划分隔离。           │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import tempfile
import zipfile
from pathlib import Path
from typing import Any


def learned_messages(row: dict[str, Any], output_format: str) -> list[dict[str, str]]:
    """Validate a learned row and reconstruct all message turns. | 校验并还原全部轮次。"""

    if output_format == "messages":
        if set(row) != {"messages"} or not isinstance(row["messages"], list):
            raise ValueError("A messages row must contain only a messages array.")
        messages = row["messages"]
    elif output_format in {"promptCompletion", "prompt_completion"}:
        if set(row) != {"prompt", "completion"} or not all(
            isinstance(value, str) and value.strip() for value in row.values()
        ):
            raise ValueError("A prompt-completion row requires two nonempty strings.")
        messages = [
            {"role": "user", "content": row["prompt"]},
            {"role": "assistant", "content": row["completion"]},
        ]
    else:
        allowed = {"instruction", "input", "output", "history", "system"}
        if set(row) - allowed or not {"instruction", "output"} <= set(row):
            raise ValueError("An SFT row has unknown fields or missing instruction/output.")
        if not all(
            isinstance(row[key], str) and row[key].strip() for key in ("instruction", "output")
        ):
            raise ValueError("An SFT instruction and output must be nonempty strings.")
        if not isinstance(row.get("input", ""), str) or not isinstance(row.get("system", ""), str):
            raise ValueError("SFT input/system must be strings.")
        history = row.get("history", [])
        if not isinstance(history, list):
            raise ValueError("SFT history must be an array.")
        messages = []
        if row.get("system"):
            messages.append({"role": "system", "content": row["system"]})
        for pair in history:
            if (
                not isinstance(pair, list)
                or len(pair) != 2
                or not all(isinstance(value, str) and value.strip() for value in pair)
            ):
                raise ValueError("SFT history requires nonempty [user, assistant] pairs.")
            messages.extend(
                [{"role": "user", "content": pair[0]}, {"role": "assistant", "content": pair[1]}]
            )
        prompt = row["instruction"]
        if row.get("input"):
            prompt += "\n" + row["input"]
        messages.extend(
            [{"role": "user", "content": prompt}, {"role": "assistant", "content": row["output"]}]
        )
    if not messages or any(
        not isinstance(message, dict)
        or set(message) != {"role", "content"}
        or message["role"] not in {"system", "user", "assistant"}
        or not isinstance(message["content"], str)
        or not message["content"].strip()
        for message in messages
    ):
        raise ValueError("A learned conversation has invalid messages.")
    if messages[-1]["role"] != "assistant":
        raise ValueError("A learned conversation must retain its final assistant answer.")
    return messages


def verify_bundle(bundle_path: Path, forbidden_text: str = "") -> dict[str, Any]:
    """Read ZIP entries without Product/Plugin code and reconcile lineage. | 独立对账。"""

    split_counts: dict[str, int] = {}
    max_turns = 0
    total_messages = 0
    with (
        tempfile.TemporaryDirectory(prefix="curation-consumer-") as temporary,
        sqlite3.connect(Path(temporary) / "lineage.sqlite3") as connection,
    ):
        connection.execute(
            "CREATE TABLE lineage (kind TEXT, identity TEXT, split TEXT, "
            "PRIMARY KEY(kind, identity))"
        )
        with zipfile.ZipFile(bundle_path) as bundle:
            manifest = json.loads(bundle.read("manifest.json"))
            output_format = manifest.get("output_format", manifest.get("mode", "sft"))
            for filename, receipt in manifest["files"].items():
                digest = hashlib.sha256()
                size = 0
                with bundle.open(filename) as stream:
                    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(chunk)
                        size += len(chunk)
                expected_digest = receipt.get("digest", receipt.get("sha256"))
                if expected_digest != "sha256:" + digest.hexdigest():
                    raise ValueError(f"File digest mismatch: {filename}")
                if size != receipt["size_bytes"]:
                    raise ValueError(f"File size mismatch: {filename}")
            for split in ("train", "validation", "test"):
                count = 0
                with bundle.open(f"{split}.jsonl") as stream:
                    for line in stream:
                        if forbidden_text and forbidden_text in line.decode("utf-8"):
                            raise ValueError("Management-only text entered a learned row.")
                        messages = learned_messages(json.loads(line), output_format)
                        max_turns = max(max_turns, len(messages))
                        total_messages += len(messages)
                        count += 1
                split_counts[split] = count
                if manifest["files"][f"{split}.jsonl"]["row_count"] != count:
                    raise ValueError("Manifest learned row count does not reconcile.")
            provenance_count = 0
            provenance_split_counts = dict.fromkeys(split_counts, 0)
            with bundle.open("provenance.jsonl") as stream:
                for line in stream:
                    record = json.loads(line)
                    split = record["split"]
                    provenance_split_counts[split] += 1
                    for kind in ("source_family_id", "conversation_id", "content_digest"):
                        identity = record.get(kind)
                        if identity:
                            previous = connection.execute(
                                "SELECT split FROM lineage WHERE kind=? AND identity=?",
                                (kind, identity),
                            ).fetchone()
                            if previous and previous[0] != split:
                                raise ValueError(f"Known lineage leakage: {kind}")
                            connection.execute(
                                "INSERT OR IGNORE INTO lineage VALUES (?, ?, ?)",
                                (kind, identity, split),
                            )
                    provenance_count += 1
            if provenance_split_counts != split_counts:
                raise ValueError("Provenance and learned rows do not reconcile by split.")
            if provenance_count != sum(split_counts.values()):
                raise ValueError("Provenance and learned total counts do not reconcile.")
            return {
                "status": "PASS",
                "outputFormat": output_format,
                "published": provenance_count,
                "splits": split_counts,
                "maxMessageCount": max_turns,
                "totalMessageCount": total_messages,
                "knownLineageLeakage": False,
                "managementFieldsInLearnedRows": False,
            }


def main() -> None:
    """Verify one exported file through the standalone CLI. | 独立校验命令入口。"""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--forbidden-text", default="")
    arguments = parser.parse_args()
    print(json.dumps(verify_bundle(arguments.bundle, arguments.forbidden_text), ensure_ascii=False))


if __name__ == "__main__":
    main()
