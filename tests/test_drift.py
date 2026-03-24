from __future__ import annotations

from pathlib import Path

from hpt.drift import fingerprint_csv_header, fingerprint_json_charge_item_keys


def test_fingerprint_csv_header_stable() -> None:
    fp = Path("tests/fixtures/sample_wide_tka.csv")
    a = fingerprint_csv_header(fp)
    b = fingerprint_csv_header(fp)
    assert a == b
    assert len(a) == 64


def test_fingerprint_json_nested_fixture() -> None:
    fp = Path("tests/fixtures/sample_json_drg469.json")
    h = fingerprint_json_charge_item_keys(fp)
    assert len(h) == 64
    assert h == fingerprint_json_charge_item_keys(fp)
