from __future__ import annotations

import tempfile
from pathlib import Path

from hpt.csv_encoding import probe_csv_text_encoding
from hpt.extract import select_standard_charges_artifact


def test_probe_csv_prefers_utf8_when_valid() -> None:
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "x.csv"
        p.write_bytes(b"a,b\n1,2\n")
        assert probe_csv_text_encoding(p) in {"utf-8", "utf-8-sig"}


def test_select_artifact_prefers_canonical_over_latin1_reencode() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "hospital"
        art = root / "artifacts"
        art.mkdir(parents=True)
        a = art / "abc123abc123_56_x_standardcharges.csv"
        b = art / "abc123abc123_56_x_standardcharges_latin1_to_utf8.csv"
        a.write_text("a\n", encoding="utf-8")
        b.write_text("b\n", encoding="utf-8")
        chosen = select_standard_charges_artifact(root)
        assert chosen == a
