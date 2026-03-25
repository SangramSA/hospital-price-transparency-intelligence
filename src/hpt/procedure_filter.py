"""Assessment-scope procedure matching: HCPCS/CPT 27447 and MS-DRG-style 469/470 with safeguards."""

from __future__ import annotations

import re

from hpt.constants import DRG_MAJOR_JOINT_WITH_MCC, DRG_MAJOR_JOINT_WITHOUT_MCC, HCPCS_TKA

_DRG_CODES = {DRG_MAJOR_JOINT_WITH_MCC, DRG_MAJOR_JOINT_WITHOUT_MCC}

# Positive signals for DRG 469/470 rows (hip/knee major joint replacement scope per CMS DRG titles).
# APR-DRG and other taxonomies reuse "469" with unrelated descriptions (e.g. acute kidney injury).
_TKA_DRG_DESCRIPTION_KEYWORDS: tuple[str, ...] = (
    "knee",
    "hip",
    "tka",
    "arthroplasty",
    "joint replacement",
    "lower extremity",
    "replacement or reattachment",
    "femoral",
    "patella",
)


def normalize_billing_code(raw: str) -> str:
    s = raw.strip()
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".", 1)[0]
    return s


def _normalize_code_type(code_type: str | None) -> str:
    if not code_type:
        return ""
    return re.sub(r"\s+", "", code_type.strip().upper())


def description_suggests_major_joint_replacement(description: str | None) -> bool:
    if not description:
        return False
    text = description.casefold()
    return any(k in text for k in _TKA_DRG_DESCRIPTION_KEYWORDS)


def _hcpcs_cpt_types_accept_tka(norm_type: str) -> bool:
    return norm_type in {"HCPCS", "CPT", "HCPCS/CPT"}


def _is_ms_drg(norm_type: str) -> bool:
    return "MS" in norm_type and "DRG" in norm_type


def _is_drg_family(norm_type: str) -> bool:
    return "DRG" in norm_type


def pair_matches_scope(
    code: str,
    code_type: str | None,
    description: str | None,
) -> tuple[str, str | None, str] | None:
    """
    Return (code, code_type, match_method) if this code column matches assessment scope.

    HCPCS/CPT 27447 is accepted on code + type alone. DRG 469/470 requires a DRG-like type and
    a description consistent with major joint replacement, except MS-DRG rows may omit description.
    """
    norm_code = normalize_billing_code(code)
    if not norm_code:
        return None
    nt = _normalize_code_type(code_type)

    if norm_code == HCPCS_TKA and _hcpcs_cpt_types_accept_tka(nt):
        return norm_code, code_type, "hcpcs_exact"

    if norm_code not in _DRG_CODES or not _is_drg_family(nt):
        return None

    desc_stripped = (description or "").strip()
    joint_ok = description_suggests_major_joint_replacement(description)

    if _is_ms_drg(nt):
        if not desc_stripped:
            return norm_code, code_type, "drg_fallback"
        if joint_ok:
            return norm_code, code_type, "drg_fallback"
        return None

    # APR-DRG, plain "DRG", or other DRG taxonomies: never trust code digits alone.
    if joint_ok:
        return norm_code, code_type, "drg_fallback"
    return None


def select_procedure_from_code_columns(
    code_pairs: list[tuple[str, str | None]],
    *,
    description: str | None,
) -> tuple[str, str | None, str] | None:
    """
    Rank matches across code|n slots (stable order: ascending slot index).

    Priority:
    1) HCPCS/CPT 27447
    2) First DRG-scoped 469/470 match
    """
    for code, ctype in code_pairs:
        m = pair_matches_scope(code, ctype, description)
        if m and m[2] == "hcpcs_exact":
            return m
    for code, ctype in code_pairs:
        m = pair_matches_scope(code, ctype, description)
        if m and m[2] == "drg_fallback":
            return m
    return None
