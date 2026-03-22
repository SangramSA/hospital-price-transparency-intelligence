"""Unit tests for cms-hpt.txt parsing and MRF URL selection."""

from __future__ import annotations

from pathlib import Path

from hpt.discovery import parse_cms_hpt_txt, select_mrf_url
from hpt.models import CmsHptEntry


def test_parse_cms_hpt_txt_reads_blocks() -> None:
    text = Path("tests/fixtures/sample_cms_hpt.txt").read_text(encoding="utf-8")
    entries = parse_cms_hpt_txt(text)
    assert len(entries) == 2
    assert entries[0].location_name == "Test Orthopedic Hospital"
    assert entries[0].mrf_url.endswith("test-orthopedic-hospital_standardcharges.csv")
    assert entries[0].contact_email == "jane@example.org"


def test_select_mrf_exact_match() -> None:
    entries = [
        CmsHptEntry("A", None, "https://x/a.csv", None, None),
        CmsHptEntry("Test Orthopedic Hospital", None, "https://x/b.csv", None, None),
    ]
    url, src = select_mrf_url(entries, "Test Orthopedic Hospital", "https://cfg/fallback.csv")
    assert url == "https://x/b.csv"
    assert src == "index_exact_match"


def test_select_mrf_config_when_no_entries() -> None:
    url, src = select_mrf_url([], "Any", "https://cfg/fallback.csv")
    assert url == "https://cfg/fallback.csv"
    assert src == "config_only_no_index"


def test_select_mrf_single_entry() -> None:
    entries = [CmsHptEntry("Other Name", None, "https://x/only.csv", None, None)]
    url, src = select_mrf_url(entries, "Unmatched Name", None)
    assert url == "https://x/only.csv"
    assert src == "index_single_entry"
