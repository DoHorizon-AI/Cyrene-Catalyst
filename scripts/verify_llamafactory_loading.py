"""
┌─────────────────────────────────────────────────────────────────────┐
│  📄 verify_llamafactory_loading.py                                  │
│  Role: Load actual published rows through the pinned trainer.        │
│  中文：通过真实固定版本训练器读取发布数据，不加载模型或启动训练。           │
└─────────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import argparse
import json
import zipfile
from importlib.metadata import version
from pathlib import Path
from typing import Any

from verify_training_bundle import learned_messages


def verify_loading(bundle_path: Path, output_directory: Path) -> dict[str, Any]:
    """Compile the production trainer mapping and check every aligned turn. | 校验训练器映射。"""

    from llama_factory import LlamaFactoryTrainingPlugin
    from llamafactory.data.loader import _load_single_dataset
    from llamafactory.data.parser import get_dataset_list
    from llamafactory.hparams import DataArguments, ModelArguments
    from transformers import Seq2SeqTrainingArguments

    if version("llamafactory") != "0.9.5":
        raise ValueError("Acceptance requires Yield's pinned llamafactory==0.9.5.")
    output_directory.mkdir(parents=True, exist_ok=True)
    results: dict[str, Any] = {}
    with zipfile.ZipFile(bundle_path) as bundle:
        manifest = json.loads(bundle.read("manifest.json"))
        output_format = manifest.get("output_format", manifest.get("mode", "sft"))
        schema = {"sft": "instruction", "messages": "messages"}.get(
            output_format, "prompt_completion"
        )
        for split in ("train", "validation", "test"):
            dataset_path = output_directory / f"{split}.jsonl"
            with bundle.open(f"{split}.jsonl") as source, dataset_path.open("wb") as destination:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    destination.write(chunk)
            with dataset_path.open(encoding="utf-8") as source:
                expected = [learned_messages(json.loads(line), output_format) for line in source]
            if not expected:
                results[split] = {"rows": 0, "status": "EMPTY"}
                continue
            compiled_directory = output_directory / f"compile-{split}"
            specification = {
                "engine": "llamafactory",
                "model": {"path": str(output_directory / "unused-model")},
                "dataset": {
                    "path": str(dataset_path.resolve()),
                    "schema": schema,
                    "format": "jsonl",
                },
                "output_dir": str(compiled_directory.resolve()),
                "stage": "sft",
                "finetuning_type": "lora",
                "hyperparams": {"learning_rate": 0.0002},
                "distributed": {"gpu_count": 0, "world_size": 1, "nnodes": 1, "nproc_per_node": 1},
                "extra": {"template": "qwen"},
            }
            LlamaFactoryTrainingPlugin().compile(specification)
            data_arguments = DataArguments(
                dataset_dir=str(compiled_directory),
                template="qwen",
                overwrite_cache=True,
                preprocessing_num_workers=1,
            )
            attributes = get_dataset_list(["cyrene"], data_arguments.dataset_dir)
            training_arguments = Seq2SeqTrainingArguments(
                output_dir=str(output_directory / "loader-state"),
                use_cpu=True,
                report_to="none",
            )
            loaded = _load_single_dataset(
                attributes[0],
                ModelArguments(model_name_or_path="unused-model", trust_remote_code=False),
                data_arguments,
                training_arguments,
            )
            max_messages = 0
            actual_count = 0
            for index, row in enumerate(loaded):
                actual = []
                if row["_system"]:
                    actual.append({"role": "system", "content": row["_system"]})
                actual.extend(row["_prompt"])
                actual.extend(row["_response"])
                if actual != expected[index]:
                    raise ValueError(f"Trainer changed or lost message turns: {split} row {index}.")
                max_messages = max(max_messages, len(actual))
                actual_count += 1
            if actual_count != len(expected):
                raise ValueError("The actual trainer row count does not reconcile.")
            results[split] = {
                "rows": actual_count,
                "status": "PASS",
                "maxMessageCount": max_messages,
                "schema": schema,
            }
    report = {
        "status": "PASS",
        "trainer": "llamafactory",
        "version": version("llamafactory"),
        "transformers": version("transformers"),
        "torch": version("torch"),
        "modelLoaded": False,
        "trainingStarted": False,
        "splits": results,
    }
    (output_directory / "trainer-loading-report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return report


def main() -> None:
    """Load a package using the optional actual-trainer environment. | 训练器验收入口。"""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--output-directory", required=True, type=Path)
    arguments = parser.parse_args()
    print(
        json.dumps(verify_loading(arguments.bundle, arguments.output_directory), ensure_ascii=False)
    )


if __name__ == "__main__":
    main()
