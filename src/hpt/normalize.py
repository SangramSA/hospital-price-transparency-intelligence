"""Normalization helpers for canonical extraction outputs."""

from __future__ import annotations

import re
from typing import Any

_WS_RE = re.compile(r"\s+")
_NON_ALNUM_RE = re.compile(r"[^0-9a-z]+")

_NUMERIC_SENTINELS = frozenset({"not payable", "n/a", "na", "null", "none"})


def normalize_ccn(ccn: str | None) -> str | None:
    """Normalize roster CCN to a 6-digit zero-padded string (CMS join key)."""
    if ccn is None:
        return None
    s = str(ccn).strip()
    if s == "":
        return None
    # CCN should be numeric in the roster, but keep defensive conversion.
    if not s.isdigit():
        return s
    if len(s) >= 6:
        return s[-6:]
    return s.zfill(6)


def normalize_payer_name(payer_name: str | None) -> str | None:
    if payer_name is None:
        return None
    s = payer_name.strip()
    if not s:
        return None
    s = s.lower()
    s = _WS_RE.sub(" ", s)
    # Light canonicalization: remove punctuation but keep spaces.
    s = _NON_ALNUM_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s or None


def parse_float_with_dq(value: Any) -> tuple[float | None, bool]:
    """
    Parse a numeric cell; return (value, unparseable_numeric).

    `unparseable_numeric` is True when the cell is non-empty, not a known sentinel,
    and not convertible to float — distinct from blank/missing.
    """
    if value is None:
        return None, False
    if isinstance(value, bool):
        return None, True
    if isinstance(value, int | float):
        return float(value), False
    s = str(value).strip()
    if s == "":
        return None, False
    lowered = s.lower()
    if lowered in _NUMERIC_SENTINELS:
        return None, False
    s2 = s.replace(",", "")
    try:
        return float(s2), False
    except ValueError:
        return None, True


def parse_float_maybe(value: Any) -> float | None:
    """Best-effort float parsing that tolerates blanks and common non-numeric sentinels."""
    parsed, _ = parse_float_with_dq(value)
    return parsed


def normalize_iso_utc_timestamp(ts_utc_iso: str) -> str:
    """Ensure timestamps remain in ISO format; extraction uses UTC now()."""
    # Minimal guard; extraction writes ISO UTC with 'Z'.
    return ts_utc_iso
