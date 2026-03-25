"""Typed records for roster, cms-hpt index entries, and discovery manifests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

MrfUrlSource = Literal[
    "index_exact_match",
    "index_substring_match",
    "index_single_entry",
    "config_only_no_index",
    "config_fallback_no_match",
    "config_fallback_index_unavailable",
    "config_override",
]

CmsHptIndexStatus = Literal["ok", "skipped_no_url", "http_error", "parse_error"]


@dataclass(frozen=True)
class Hospital:
    """One row from config/hospitals.yaml."""

    hospital_key: str
    hospital_name: str
    state: str
    # Curated CMS Certification Number (zero-padded 6-digit string) used for CMS joins.
    ccn: str | None
    tier: int
    website_root: str
    cms_hpt_index_url: str | None
    mrf_url: str | None
    source_page_url: str | None


@dataclass(frozen=True)
class CmsHptEntry:
    """One location block from a parsed cms-hpt.txt file."""

    location_name: str
    source_page_url: str | None
    mrf_url: str
    contact_name: str | None
    contact_email: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "location_name": self.location_name,
            "source_page_url": self.source_page_url,
            "mrf_url": self.mrf_url,
            "contact_name": self.contact_name,
            "contact_email": self.contact_email,
        }


@dataclass
class DiscoveryManifest:
    """Written to data/raw/{hospital_key}/manifest.json after discover."""

    hospital_key: str
    hospital_name: str
    state: str
    discovered_at: str  # ISO-8601 UTC
    cms_hpt_index_url: str | None
    cms_hpt_index_status: CmsHptIndexStatus
    cms_hpt_raw_relpath: str | None  # relative to raw root, e.g. nyu/.../cms-hpt.txt
    cms_hpt_entries: list[dict[str, Any]] = field(default_factory=list)
    selected_mrf_url: str | None = None
    mrf_url_source: MrfUrlSource = "config_only_no_index"
    config_mrf_url: str | None = None
    http_error_detail: str | None = None

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "hospital_key": self.hospital_key,
            "hospital_name": self.hospital_name,
            "state": self.state,
            "discovered_at": self.discovered_at,
            "cms_hpt_index_url": self.cms_hpt_index_url,
            "cms_hpt_index_status": self.cms_hpt_index_status,
            "cms_hpt_raw_relpath": self.cms_hpt_raw_relpath,
            "cms_hpt_entries": self.cms_hpt_entries,
            "selected_mrf_url": self.selected_mrf_url,
            "mrf_url_source": self.mrf_url_source,
            "config_mrf_url": self.config_mrf_url,
            "http_error_detail": self.http_error_detail,
        }
