"""Fetch and parse cms-hpt.txt; resolve MRF URLs and write per-hospital manifests."""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime
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
from hpt.constants import CMS_HPT_LOCAL_NAME, MANIFEST_JSON_NAME
from hpt.http_utils import get_text_with_retries
from hpt.models import CmsHptEntry, DiscoveryManifest, Hospital, MrfUrlSource

logger = logging.getLogger(__name__)

_WS_RE = re.compile(r"\s+")


def _normalize_name(name: str) -> str:
    s = name.strip().lower()
    s = _WS_RE.sub(" ", s)
    return s


def parse_cms_hpt_txt(content: str) -> list[CmsHptEntry]:
    """
    Parse the CMS-mandated key-value block format (see fetched real files).

    Blocks are separated by blank lines; keys include location-name, mrf-url, etc.
    """
    entries: list[CmsHptEntry] = []
    current: dict[str, str] = {}
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line:
            if current:
                ent = _block_to_entry(current)
                if ent:
                    entries.append(ent)
                current = {}
            continue
        if ":" not in line:
            continue
        key, _, rest = line.partition(":")
        k = key.strip().lower()
        v = rest.strip()
        current[k] = v
    if current:
        ent = _block_to_entry(current)
        if ent:
            entries.append(ent)
    return entries


def _block_to_entry(block: dict[str, str]) -> CmsHptEntry | None:
    loc = block.get("location-name", "").strip()
    mrf = block.get("mrf-url", "").strip()
    if not mrf:
        logger.warning("Skipping cms-hpt block with empty mrf-url (location=%r)", loc)
        return None
    src = block.get("source-page-url", "").strip() or None
    cname = block.get("contact-name", "").strip() or None
    cemail = block.get("contact-email", "").strip() or None
    return CmsHptEntry(
        location_name=loc,
        source_page_url=src,
        mrf_url=mrf,
        contact_name=cname,
        contact_email=cemail,
    )


def select_mrf_url(
    entries: list[CmsHptEntry],
    hospital_name: str,
    config_mrf_url: str | None,
) -> tuple[str | None, MrfUrlSource]:
    """Pick the MRF URL for this roster hospital from index entries and config."""
    if not entries:
        return (config_mrf_url, "config_only_no_index")

    target = _normalize_name(hospital_name)
    exact = [e for e in entries if _normalize_name(e.location_name) == target]
    if len(exact) == 1:
        return (exact[0].mrf_url.strip(), "index_exact_match")
    if len(exact) > 1:
        logger.warning(
            "Multiple exact location-name matches for %s; using first mrf-url",
            hospital_name,
        )
        return (exact[0].mrf_url.strip(), "index_exact_match")

    for e in entries:
        n = _normalize_name(e.location_name)
        if not n:
            continue
        if target in n or n in target:
            return (e.mrf_url.strip(), "index_substring_match")

    if len(entries) == 1:
        return (entries[0].mrf_url.strip(), "index_single_entry")

    return (config_mrf_url, "config_fallback_no_match")


def resolved_cms_hpt_index_url(hospital: Hospital) -> str | None:
    """URL for cms-hpt.txt: explicit config wins; no silent guess if explicitly null."""
    if hospital.cms_hpt_index_url:
        return str(hospital.cms_hpt_index_url).strip()
    return None


def discover_one(
    hospital: Hospital,
    *,
    raw_root: Path | None = None,
    dry_run: bool = False,
) -> DiscoveryManifest:
    """
    Fetch cms-hpt.txt when available, parse entries, select mrf_url, write manifest.

    When dry_run is True, no files are written; manifest is still returned.
    """
    root = raw_root or raw_dir()
    raw_hospital_dir = root / hospital.hospital_key
    index_url = resolved_cms_hpt_index_url(hospital)
    now = datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    manifest = DiscoveryManifest(
        hospital_key=hospital.hospital_key,
        hospital_name=hospital.hospital_name,
        state=hospital.state,
        discovered_at=now,
        cms_hpt_index_url=index_url,
        cms_hpt_index_status="skipped_no_url",
        cms_hpt_raw_relpath=None,
        config_mrf_url=hospital.mrf_url,
    )

    entries: list[CmsHptEntry] = []
    if index_url:
        logger.info(
            "[%s] discovery: fetching cms-hpt index %s",
            hospital.hospital_key,
            index_url,
        )
        result = get_text_with_retries(
            index_url,
            timeout_sec=http_timeout_sec(),
            max_retries=http_max_retries(),
            user_agent=http_user_agent(),
        )
        if result.error or result.text is None:
            manifest.cms_hpt_index_status = "http_error"
            manifest.http_error_detail = result.error
            logger.error(
                "[%s] discovery: cms-hpt fetch failed: %s",
                hospital.hospital_key,
                result.error,
            )
        else:
            raw_path = raw_hospital_dir / CMS_HPT_LOCAL_NAME
            if not dry_run:
                raw_hospital_dir.mkdir(parents=True, exist_ok=True)
                raw_path.write_text(result.text, encoding="utf-8")
                manifest.cms_hpt_raw_relpath = f"{hospital.hospital_key}/{CMS_HPT_LOCAL_NAME}"
            else:
                manifest.cms_hpt_raw_relpath = f"{hospital.hospital_key}/{CMS_HPT_LOCAL_NAME}"
            try:
                entries = parse_cms_hpt_txt(result.text)
                manifest.cms_hpt_index_status = "ok"
                manifest.cms_hpt_entries = [e.to_dict() for e in entries]
                logger.info(
                    "[%s] discovery: parsed %s location(s) from cms-hpt.txt",
                    hospital.hospital_key,
                    len(entries),
                )
            except (ValueError, KeyError) as e:
                manifest.cms_hpt_index_status = "parse_error"
                manifest.http_error_detail = str(e)
                logger.exception(
                    "[%s] discovery: failed to parse cms-hpt.txt",
                    hospital.hospital_key,
                )

    selected, source = select_mrf_url(entries, hospital.hospital_name, hospital.mrf_url)
    if selected is None and hospital.mrf_url:
        selected = hospital.mrf_url
        source = "config_override"
    index_failed = manifest.cms_hpt_index_status in ("http_error", "parse_error")
    if index_failed and selected == hospital.mrf_url:
        source = "config_fallback_index_unavailable"

    manifest.selected_mrf_url = selected
    manifest.mrf_url_source = source

    manifest_path = raw_hospital_dir / MANIFEST_JSON_NAME
    if not dry_run:
        raw_hospital_dir.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(manifest.to_json_dict(), indent=2),
            encoding="utf-8",
        )
        logger.info(
            "[%s] discovery: selected mrf_url source=%s url=%s (manifest %s)",
            hospital.hospital_key,
            source,
            selected,
            manifest_path,
        )
    else:
        logger.info(
            "[%s] discovery (dry-run): selected mrf_url source=%s url=%s",
            hospital.hospital_key,
            source,
            selected,
        )

    return manifest


def run_discover(
    *,
    hospital_keys: set[str] | None = None,
    config_path: Path | None = None,
    dry_run: bool = False,
) -> list[DiscoveryManifest]:
    """Discover all (or filtered) hospitals from roster config."""
    path = config_path or default_hospitals_config_path()
    hospitals = load_hospitals(path)
    hospitals = filter_hospitals(hospitals, hospital_keys)
    out: list[DiscoveryManifest] = []
    for h in hospitals:
        try:
            out.append(discover_one(h, dry_run=dry_run))
        except OSError:
            logger.exception("[%s] discovery: I/O error", h.hospital_key)
            raise
    return out
