from __future__ import annotations

import json
import tempfile
from pathlib import Path

from hpt.checkpoint import (
    read_active_download_sha256,
    should_skip_extract,
    write_extract_checkpoint,
)
from hpt.constants import MANIFEST_JSON_NAME, OUTPUT_SCHEMA_VERSION


def test_checkpoint_skip_when_sha_matches() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        raw = root / "raw" / "h1"
        raw.mkdir(parents=True)
        silver = root / "silver"
        proc = root / "proc"
        manifest = raw / MANIFEST_JSON_NAME
        manifest.write_text(
            json.dumps(
                {
                    "active_download": {
                        "content_sha256": "deadbeef" * 8,
                        "local_path": "x",
                    }
                }
            ),
            encoding="utf-8",
        )
        sj = silver / "h1" / "f.canonical.jsonl"
        sj.parent.mkdir(parents=True)
        sj.write_text('{"a":1}\n', encoding="utf-8")
        write_extract_checkpoint(
            processed_root=proc,
            hospital_key="h1",
            source_content_sha256="deadbeef" * 8,
            silver_jsonl_relpath="h1/f.canonical.jsonl",
        )
        assert (
            read_active_download_sha256(raw_root=root / "raw", hospital_key="h1") == "deadbeef" * 8
        )
        assert should_skip_extract(
            raw_root=root / "raw",
            silver_root=silver,
            processed_root=proc,
            hospital_key="h1",
            force=False,
        )


def test_checkpoint_no_skip_when_force() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        raw = root / "raw" / "h1"
        raw.mkdir(parents=True)
        silver = root / "silver"
        proc = root / "proc"
        (raw / MANIFEST_JSON_NAME).write_text(
            json.dumps({"active_download": {"content_sha256": "aa" * 32}}),
            encoding="utf-8",
        )
        sj = silver / "h1" / "f.canonical.jsonl"
        sj.parent.mkdir(parents=True)
        sj.write_text("{}", encoding="utf-8")
        write_extract_checkpoint(
            processed_root=proc,
            hospital_key="h1",
            source_content_sha256="aa" * 32,
            silver_jsonl_relpath="h1/f.canonical.jsonl",
        )
        assert not should_skip_extract(
            raw_root=root / "raw",
            silver_root=silver,
            processed_root=proc,
            hospital_key="h1",
            force=True,
        )


def test_checkpoint_no_skip_when_extractor_version_changes() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        raw = root / "raw" / "h1"
        raw.mkdir(parents=True)
        silver = root / "silver"
        proc = root / "proc"
        (raw / MANIFEST_JSON_NAME).write_text(
            json.dumps({"active_download": {"content_sha256": "bb" * 32}}),
            encoding="utf-8",
        )
        sj = silver / "h1" / "f.canonical.jsonl"
        sj.parent.mkdir(parents=True)
        sj.write_text("{}", encoding="utf-8")
        cp = proc / "checkpoints" / "extract_h1.json"
        cp.parent.mkdir(parents=True)
        cp.write_text(
            json.dumps(
                {
                    "source_content_sha256": "bb" * 32,
                    "extractor_version": "0.0.0-not-real",
                    "canonical_schema_version": OUTPUT_SCHEMA_VERSION,
                    "silver_jsonl_relpath": "h1/f.canonical.jsonl",
                }
            ),
            encoding="utf-8",
        )
        assert not should_skip_extract(
            raw_root=root / "raw",
            silver_root=silver,
            processed_root=proc,
            hospital_key="h1",
            force=False,
        )
