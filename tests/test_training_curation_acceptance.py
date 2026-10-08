"""Product/owner Plugin acceptance for the fixed mixed training corpus.

中文：通过真实产品 API 和插件契约验收固定混合训练语料。
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_fixed_mixed_training_corpus_through_real_owner_plugins(tmp_path: Path) -> None:
    """Require reviewed, restart-safe, lossless trainable output. | 验收审核、恢复和完整输出。"""

    root = Path(__file__).resolve().parents[1]
    completed = subprocess.run(
        [
            sys.executable,
            str(root / "scripts/accept_training_curation.py"),
            "--output-directory",
            str(tmp_path / "trial"),
        ],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    report = json.loads((tmp_path / "trial/acceptance-report.json").read_text(encoding="utf-8"))
    assert report["totalRecords"] == 28
    assert report["afterReview"]["eligible"] == 13
    assert report["afterReview"]["excluded"] == 15
    assert report["afterReview"]["pendingReview"] == 0
    assert report["restartBeforeReview"] and report["restartBeforePublish"]
    assert report["originalSnapshotImmutable"] and report["publicationRetryImmutable"]
    assert report["repeatedImportPreservesBytes"]
    assert report["repeatedImportCreatesIndependentSources"]
    assert report["independentConsumer"]["sft"]["maxMessageCount"] == 5
    assert report["independentConsumer"]["messages"]["published"] == 13
