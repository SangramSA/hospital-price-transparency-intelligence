"""Extract-stage checkpointing: skip re-extract when raw bytes and schema versions are unchanged."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from hpt import __version__ as extractor_version
from hpt.constants import MANIFEST_JSON_NAME, OUTPUT_SCHEMA_VERSION

logger = logging.getLogger(__name__)


def read_active_download_sha256(*, raw_root: Path, hospital_key: str) -> str | None:
    """Return ``content_sha256`` from manifest ``active_download``, if present."""
    manifest = raw_root / hospital_key / MANIFEST_JSON_NAME
    if not manifest.is_file():
        return None
    try:
        data: dict[str, Any] = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("[%s] checkpoint: could not read manifest %s: %s", hospital_key, manifest, e)
        return None
    active = data.get("active_download")
    if not isinstance(active, dict):
        return None
    sha = active.get("content_sha256")
    if sha is None:
        return None
    s = str(sha).strip()
    return s or None


def extract_checkpoint_path(processed_root: Path, hospital_key: str) -> Path:
    return processed_root / "checkpoints" / f"extract_{hospital_key}.json"


def load_extract_checkpoint(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def should_skip_extract(
    *,
    raw_root: Path,
    silver_root: Path,
    processed_root: Path,
    hospital_key: str,
    force: bool,
) -> bool:
    """
    Return True if a prior extract checkpoint matches current manifest hash + versions
    and the recorded silver JSONL still exists.
    """
    if force:
        return False
    current_sha = read_active_download_sha256(raw_root=raw_root, hospital_key=hospital_key)
    if not current_sha:
        return False
    cp_path = extract_checkpoint_path(processed_root, hospital_key)
    prev = load_extract_checkpoint(cp_path)
    if not prev:
        return False
    if str(prev.get("source_content_sha256", "")).strip() != current_sha:
        return False
    if str(prev.get("extractor_version", "")).strip() != extractor_version:
        return False
    if str(prev.get("canonical_schema_version", "")).strip() != OUTPUT_SCHEMA_VERSION:
        return False
    rel = prev.get("silver_jsonl_relpath")
    if not rel or not isinstance(rel, str):
        return False
    silver_path = silver_root / rel
    if not silver_path.is_file():
        return False
    logger.info("[%s] extract: skip (checkpoint matches manifest sha256=%s…)", hospital_key, current_sha[:12])
    return True


def write_extract_checkpoint(
    *,
    processed_root: Path,
    hospital_key: str,
    source_content_sha256: str | None,
    silver_jsonl_relpath: str,
) -> None:
    """Persist checkpoint after a successful extract."""
    out_dir = processed_root / "checkpoints"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = extract_checkpoint_path(processed_root, hospital_key)
    payload = {
        "hospital_key": hospital_key,
        "source_content_sha256": source_content_sha256,
        "extractor_version": extractor_version,
        "canonical_schema_version": OUTPUT_SCHEMA_VERSION,
        "silver_jsonl_relpath": silver_jsonl_relpath,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
