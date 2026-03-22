"""Download MRF artifacts from discovery manifest URLs into data/raw/{hospital_key}/."""

from __future__ import annotations

import json
import logging
from pathlib import Path

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

    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    url = data.get("selected_mrf_url")
    if not url:
        logger.error("[%s] download: manifest has no selected_mrf_url", hospital.hospital_key)
        return None

    url_str = str(url).strip()
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

    if result.skipped:
        logger.info(
            "[%s] download: skipped existing %s",
            hospital.hospital_key,
            result.dest_path,
        )
        return result.dest_path

    logger.info(
        "[%s] download: wrote %s bytes to %s (HTTP %s, content-type=%s)",
        hospital.hospital_key,
        result.bytes_written,
        result.dest_path,
        result.status_code,
        result.content_type,
    )
    return result.dest_path


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
