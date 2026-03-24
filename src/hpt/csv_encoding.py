"""Shared CSV text encoding detection for MRF files (UTF-8 vs Windows-1252 vs Latin-1)."""

from __future__ import annotations

from pathlib import Path

# Order: try strict UTF-8 first, then common US hospital exports, then Latin-1 (never fails per-byte).
CSV_TEXT_ENCODING_ORDER: tuple[str, ...] = (
    "utf-8-sig",
    "utf-8",
    "cp1252",
    "latin-1",
)

_PROBE_BYTES = 512 * 1024


def probe_csv_text_encoding(path: Path) -> str:
    """
    Return an encoding that decodes the start of the file without error.

    Full-file reads use the same encoding; Latin-1 decodes any byte stream, so it is the
    ultimate fallback for pathological files.
    """
    data = path.read_bytes()[:_PROBE_BYTES]
    if not data:
        return "utf-8"
    for enc in CSV_TEXT_ENCODING_ORDER:
        try:
            data.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"
