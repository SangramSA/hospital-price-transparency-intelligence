"""Unit tests for download filename resolution (Content-Disposition, URL, Content-Type)."""

from __future__ import annotations

from hpt.http_utils import (
    extension_from_content_type,
    parse_content_disposition_filename,
    resolve_download_basename,
    suggested_local_filename,
)


def test_suggested_local_filename_from_path() -> None:
    u = "https://example.org/path/042103612_hospital_standardcharges.json"
    assert suggested_local_filename(u) == "042103612_hospital_standardcharges.json"


def test_suggested_local_filename_aspx_query() -> None:
    u = "https://apps.example.com/PTT/FinalLinks/Reports.aspx?dbName=dbX&type=CSV"
    assert suggested_local_filename(u).endswith(".aspx")


def test_parse_content_disposition_quoted() -> None:
    h = 'attachment; filename="640626874_hospital_standardcharges.csv"'
    assert parse_content_disposition_filename(h) == "640626874_hospital_standardcharges.csv"


def test_parse_content_disposition_rfc5987() -> None:
    h = "attachment; filename*=UTF-8''my%20file%20name.csv"
    assert parse_content_disposition_filename(h) == "my file name.csv"


def test_extension_from_content_type() -> None:
    assert extension_from_content_type("text/csv; charset=utf-8") == ".csv"
    assert extension_from_content_type("application/json") == ".json"
    assert extension_from_content_type("application/octet-stream") is None


def test_resolve_prefers_content_disposition() -> None:
    name = resolve_download_basename(
        content_disposition='attachment; filename="ein_hospital_standardcharges.csv"',
        content_type="text/csv",
        url="https://example.com/MRFDownload/x/y",
    )
    assert name == "ein_hospital_standardcharges.csv"


def test_resolve_adds_extension_from_content_type() -> None:
    name = resolve_download_basename(
        content_disposition=None,
        content_type="text/csv",
        url="https://example.com/MRFDownload/atlanticare/atlanticare",
    )
    assert name == "atlanticare.csv"


def test_resolve_replaces_bin_with_content_type() -> None:
    name = resolve_download_basename(
        content_disposition=None,
        content_type="application/json",
        url="https://example.com",
    )
    assert name == "mrf_download.json"
