"""Unit tests for download manifest hardening and idempotency behavior."""

from __future__ import annotations

import json
from pathlib import Path

from hpt.constants import MANIFEST_JSON_NAME
from hpt.download import download_for_hospital
from hpt.http_utils import StreamDownloadResult
from hpt.models import Hospital


def _hospital() -> Hospital:
    return Hospital(
        hospital_key="test-hospital",
        hospital_name="Test Hospital",
        state="NY",
        ccn="123456",
        tier=1,
        website_root="https://example.org",
        cms_hpt_index_url="https://example.org/cms-hpt.txt",
        mrf_url="https://example.org/mrf.csv",
        source_page_url="https://example.org/prices",
    )


def _write_manifest(root: Path, payload: dict[str, object]) -> Path:
    hdir = root / "test-hospital"
    hdir.mkdir(parents=True, exist_ok=True)
    manifest_path = hdir / MANIFEST_JSON_NAME
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return manifest_path


def test_download_idempotent_rerun_uses_active_manifest_artifact(
    monkeypatch, tmp_path: Path
) -> None:
    raw_root = tmp_path / "raw"
    artifact = raw_root / "test-hospital" / "artifacts" / "abc123_file.csv"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_bytes(b"same-bytes")
    _write_manifest(
        raw_root,
        {
            "hospital_key": "test-hospital",
            "selected_mrf_url": "https://example.org/mrf.csv",
            "download_artifacts": [
                {
                    "source_url": "https://example.org/mrf.csv",
                    "local_path": "test-hospital/artifacts/abc123_file.csv",
                    "content_sha256": "abc123",
                    "downloaded_at": "2026-03-22T08:00:00Z",
                    "http_status": 200,
                }
            ],
            "active_download": {
                "source_url": "https://example.org/mrf.csv",
                "local_path": "test-hospital/artifacts/abc123_file.csv",
                "content_sha256": "abc123",
                "downloaded_at": "2026-03-22T08:00:00Z",
                "http_status": 200,
            },
        },
    )

    def _should_not_download(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("download_to_path should not be called for idempotent rerun")

    monkeypatch.setattr("hpt.download.download_to_path", _should_not_download)
    got = download_for_hospital(_hospital(), raw_root=raw_root)
    assert got == artifact


def test_download_records_transport_metadata_and_content_hash(monkeypatch, tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    hdir = raw_root / "test-hospital"
    hdir.mkdir(parents=True, exist_ok=True)
    staging = hdir / "payload.csv"
    payload = b"payer,rate\nAetna,123.45\n"
    staging.write_bytes(payload)
    _write_manifest(
        raw_root,
        {
            "hospital_key": "test-hospital",
            "selected_mrf_url": "https://example.org/mrf.csv",
        },
    )

    def _fake_download(*args, **kwargs):  # type: ignore[no-untyped-def]
        return StreamDownloadResult(
            url="https://example.org/mrf.csv",
            dest_path=staging,
            status_code=200,
            bytes_written=len(payload),
            content_type="text/csv",
            etag='"abc-etag"',
            last_modified="Mon, 23 Mar 2026 10:00:00 GMT",
            error=None,
            skipped=False,
        )

    monkeypatch.setattr("hpt.download.download_to_path", _fake_download)
    output_path = download_for_hospital(_hospital(), raw_root=raw_root)
    assert output_path is not None
    assert output_path.is_file()
    assert output_path.parent.name == "artifacts"
    assert not staging.exists()

    manifest = json.loads((hdir / MANIFEST_JSON_NAME).read_text(encoding="utf-8"))
    active = manifest["active_download"]
    assert active["source_url"] == "https://example.org/mrf.csv"
    assert active["http_status"] == 200
    assert active["etag"] == '"abc-etag"'
    assert active["last_modified"] == "Mon, 23 Mar 2026 10:00:00 GMT"
    assert active["content_sha256"]
    assert active["local_path"].startswith("test-hospital/artifacts/")
    assert len(manifest["download_artifacts"]) == 1


def test_download_dedupes_same_source_url_and_hash(monkeypatch, tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    hdir = raw_root / "test-hospital"
    hdir.mkdir(parents=True, exist_ok=True)
    _write_manifest(
        raw_root,
        {
            "hospital_key": "test-hospital",
            "selected_mrf_url": "https://example.org/mrf.csv",
        },
    )

    payload = b"x,y\n1,2\n"
    call_count = {"n": 0}

    def _fake_download(*args, **kwargs):  # type: ignore[no-untyped-def]
        call_count["n"] += 1
        staging = hdir / "download.csv"
        staging.write_bytes(payload)
        return StreamDownloadResult(
            url="https://example.org/mrf.csv",
            dest_path=staging,
            status_code=200,
            bytes_written=len(payload),
            content_type="text/csv",
            etag=f"etag-{call_count['n']}",
            last_modified=None,
            error=None,
            skipped=False,
        )

    monkeypatch.setattr("hpt.download.download_to_path", _fake_download)
    first = download_for_hospital(_hospital(), raw_root=raw_root, force=True)
    second = download_for_hospital(_hospital(), raw_root=raw_root, force=True)
    assert first is not None and second is not None
    assert first == second

    manifest = json.loads((hdir / MANIFEST_JSON_NAME).read_text(encoding="utf-8"))
    assert len(manifest["download_artifacts"]) == 1
    assert manifest["active_download"]["etag"] == "etag-2"
