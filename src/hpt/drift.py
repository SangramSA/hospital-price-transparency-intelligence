"""Drift fingerprints for upstream MRF layout changes (CSV header + JSON key paths)."""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def fingerprint_csv_header(path: Path) -> str:
    """
    SHA-256 of the first text line (header row) using the same encoding probe as parsers.

    Stable across row order changes; changes when columns are renamed/reordered.
    """
    from hpt.csv_encoding import probe_csv_text_encoding

    enc = probe_csv_text_encoding(path)
    with path.open(encoding=enc, newline="") as fh:
        first = fh.readline()
    return hashlib.sha256(first.encode("utf-8")).hexdigest()


def fingerprint_json_charge_item_keys(path: Path) -> str:
    """
    SHA-256 of sorted keys on the first object under ``standard_charge_information``.

    If that path is missing, fall back to sorted top-level JSON keys (first pass).
    """
    import ijson

    keys: list[str] = []
    try:
        with path.open("rb") as fh:
            for item in ijson.items(fh, "standard_charge_information.item"):
                if isinstance(item, dict):
                    keys = sorted(item.keys())
                break
    except OSError as e:
        logger.warning("drift: could not read JSON %s: %s", path, e)
        return hashlib.sha256(b"").hexdigest()

    if keys:
        payload = json.dumps(keys, sort_keys=True).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    top: list[str] = []
    try:
        with path.open("rb") as fh:
            for prefix, event, value in ijson.parse(fh):
                if prefix == "" and event == "map_key" and isinstance(value, str):
                    top.append(value)
    except OSError:
        return hashlib.sha256(b"").hexdigest()
    payload = json.dumps(sorted(set(top)), sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
