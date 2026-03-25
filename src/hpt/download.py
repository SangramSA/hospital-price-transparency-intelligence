"""Download MRF artifacts from discovery manifest URLs into data/raw/{hospital_key}/."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from hpt.config import (
    default_hospitals_config_path,
    filter_hospitals,
    http_max_retries,
    http_timeout_sec,
    http_user_agent,
    load_hospitals,
    raw_dir,
)
from hpt.constants import MANIFEST_JSON_NAME
from hpt.http_utils import download_to_path, suggested_local_filename
from hpt.models import Hospital

logger = logging.getLogger(__name__)

# Re-export for callers/tests that imported from download.
__all__ = ["download_for_hospital", "run_download", "suggested_local_filename"]

_ARTIFACTS_DIR_NAME = "artifacts"
_MANIFEST_DOWNLOAD_ARTIFACTS_KEY = "download_artifacts"
_MANIFEST_ACTIVE_DOWNLOAD_KEY = "active_download"


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_manifest(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"manifest must be a JSON object: {path}")
    return data


def _write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _manifest_local_path(root: Path, local_path: str | None) -> Path | None:
    if not local_path:
        return None
    candidate = root / local_path
    return candidate if candidate.is_file() else None


def _artifact_relpath(root: Path, artifact_path: Path) -> str:
    try:
        return artifact_path.relative_to(root).as_posix()
    except ValueError:
        return str(artifact_path)


def _build_artifact_record(
    *,
    source_url: str,
    local_path: str,
    content_sha256: str,
    downloaded_at: str,
    status_code: int,
    content_type: str | None,
    etag: str | None,
    last_modified: str | None,
    bytes_written: int,
) -> dict[str, Any]:
    return {
        "source_url": source_url,
        "local_path": local_path,
        "content_sha256": content_sha256,
        "downloaded_at": downloaded_at,
        "http_status": status_code,
        "content_type": content_type,
        "etag": etag,
        "last_modified": last_modified,
        "bytes_written": bytes_written,
    }


def _existing_active_artifact(
    *,
    manifest: dict[str, Any],
    root: Path,
    source_url: str,
) -> tuple[Path, dict[str, Any]] | None:
    active = manifest.get(_MANIFEST_ACTIVE_DOWNLOAD_KEY)
    if isinstance(active, dict):
        if str(active.get("source_url", "")).strip() == source_url:
            p = _manifest_local_path(root, active.get("local_path"))
            if p:
                return (p, active)
    artifacts = manifest.get(_MANIFEST_DOWNLOAD_ARTIFACTS_KEY)
    if not isinstance(artifacts, list):
        return None
    for entry in reversed(artifacts):
        if not isinstance(entry, dict):
            continue
        if str(entry.get("source_url", "")).strip() != source_url:
            continue
        p = _manifest_local_path(root, entry.get("local_path"))
        if not p:
            continue
        manifest[_MANIFEST_ACTIVE_DOWNLOAD_KEY] = entry
        return (p, entry)
    return None


def _promote_to_immutable_artifact(
    *,
    root: Path,
    hospital_key: str,
    downloaded_path: Path,
    content_sha256: str,
) -> Path:
    artifact_dir = root / hospital_key / _ARTIFACTS_DIR_NAME
    artifact_dir.mkdir(parents=True, exist_ok=True)
    immutable_name = f"{content_sha256[:16]}_{downloaded_path.name}"
    immutable_path = artifact_dir / immutable_name
    if immutable_path.exists():
        if downloaded_path != immutable_path and downloaded_path.exists():
            downloaded_path.unlink(missing_ok=True)
        return immutable_path
    downloaded_path.replace(immutable_path)
    return immutable_path


def download_for_hospital(
    hospital: Hospital,
    *,
    raw_root: Path | None = None,
    force: bool = False,
) -> Path | None:
    """
    Read manifest.json; download selected_mrf_url into raw_root/hospital_key/.

    Returns path to downloaded file, or None if skipped/failed.
    """
    root = raw_root or raw_dir()
    hdir = root / hospital.hospital_key
    manifest_path = hdir / MANIFEST_JSON_NAME
    if not manifest_path.is_file():
        logger.error(
            "[%s] download: missing manifest %s — run `hpt discover` first",
            hospital.hospital_key,
            manifest_path,
        )
        return None

    data = _read_manifest(manifest_path)
    url = data.get("selected_mrf_url")
    if not url:
        logger.error("[%s] download: manifest has no selected_mrf_url", hospital.hospital_key)
        return None

    url_str = str(url).strip()
    if not force:
        existing = _existing_active_artifact(manifest=data, root=root, source_url=url_str)
        if existing is not None:
            existing_path, existing_meta = existing
            logger.info(
                "[%s] download: unchanged artifact already present at %s (sha256=%s)",
                hospital.hospital_key,
                existing_path,
                existing_meta.get("content_sha256"),
            )
            _write_manifest(manifest_path, data)
            return existing_path

    logger.info("[%s] download: GET %s -> %s/", hospital.hospital_key, url_str, hdir)
    result = download_to_path(
        url_str,
        hdir,
        force=force,
        timeout_sec=http_timeout_sec(),
        max_retries=http_max_retries(),
        user_agent=http_user_agent(),
    )
    if result.error:
        logger.error(
            "[%s] download: failed (%s bytes): %s",
            hospital.hospital_key,
            result.bytes_written,
            result.error,
        )
        return None

    source_path = result.dest_path
    if not source_path.is_file():
        logger.error(
            "[%s] download: expected local file missing after transfer: %s",
            hospital.hospital_key,
            source_path,
        )
        return None

    content_sha256 = _sha256_file(source_path)
    immutable_path = _promote_to_immutable_artifact(
        root=root,
        hospital_key=hospital.hospital_key,
        downloaded_path=source_path,
        content_sha256=content_sha256,
    )
    artifact_relpath = _artifact_relpath(root, immutable_path)
    downloaded_at = _utc_now_iso()
    artifact_record = _build_artifact_record(
        source_url=url_str,
        local_path=artifact_relpath,
        content_sha256=content_sha256,
        downloaded_at=downloaded_at,
        status_code=result.status_code,
        content_type=result.content_type,
        etag=result.etag,
        last_modified=result.last_modified,
        bytes_written=result.bytes_written,
    )
    existing_artifacts = data.get(_MANIFEST_DOWNLOAD_ARTIFACTS_KEY)
    artifacts: list[dict[str, Any]] = []
    if isinstance(existing_artifacts, list):
        artifacts = [a for a in existing_artifacts if isinstance(a, dict)]
    duplicate_idx: int | None = None
    for idx, entry in enumerate(artifacts):
        if (
            str(entry.get("source_url", "")).strip() == url_str
            and str(entry.get("content_sha256", "")).strip() == content_sha256
        ):
            duplicate_idx = idx
            break
    if duplicate_idx is None:
        artifacts.append(artifact_record)
    else:
        artifacts[duplicate_idx] = artifact_record
    data[_MANIFEST_DOWNLOAD_ARTIFACTS_KEY] = artifacts
    data[_MANIFEST_ACTIVE_DOWNLOAD_KEY] = artifact_record
    _write_manifest(manifest_path, data)

    logger.info(
        "[%s] download: content sha256=%s stored at %s (HTTP %s, etag=%s, last-modified=%s)",
        hospital.hospital_key,
        content_sha256,
        immutable_path,
        result.status_code,
        result.etag,
        result.last_modified,
    )
    if result.skipped:
        logger.info(
            "[%s] download: deduped existing bytes for %s",
            hospital.hospital_key,
            url_str,
        )
    return immutable_path


def run_download(
    *,
    hospital_keys: set[str] | None = None,
    config_path: Path | None = None,
    force: bool = False,
) -> list[tuple[str, Path | None]]:
    """Download for all roster hospitals (or subset). Returns (hospital_key, path_or_none)."""
    path = config_path or default_hospitals_config_path()
    hospitals = load_hospitals(path)
    hospitals = filter_hospitals(hospitals, hospital_keys)
    results: list[tuple[str, Path | None]] = []
    for h in hospitals:
        try:
            p = download_for_hospital(h, force=force)
            results.append((h.hospital_key, p))
        except OSError as e:
            logger.error("[%s] download: I/O error: %s", h.hospital_key, e)
            results.append((h.hospital_key, None))
    return results
