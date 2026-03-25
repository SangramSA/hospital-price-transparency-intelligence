from __future__ import annotations

from hpt.procedure_filter import (
    description_suggests_major_joint_replacement,
    pair_matches_scope,
    select_procedure_from_code_columns,
)


def test_pair_apr_drg_469_kidney_rejected() -> None:
    assert pair_matches_scope("469", "APR-DRG", "Acute Kidney Injury") is None


def test_pair_ms_drg_469_joint_accepted() -> None:
    m = pair_matches_scope(
        "469",
        "MS-DRG",
        "Major Hip And Knee Joint Replacement Or Reattachment Of Lower Extremity With Mcc",
    )
    assert m is not None
    assert m[0] == "469"
    assert m[2] == "drg_fallback"


def test_select_prefers_hcpcs_over_drg_column() -> None:
    pairs = [("469", "MS-DRG"), ("27447", "HCPCS")]
    sel = select_procedure_from_code_columns(pairs, description="Some knee DRG row with HCPCS too")
    assert sel is not None
    assert sel[0] == "27447"
    assert sel[2] == "hcpcs_exact"


def test_description_keyword_hip() -> None:
    assert description_suggests_major_joint_replacement("Total Hip Arthroplasty")
