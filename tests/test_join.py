from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path

from hpt.join import (
    DQ_JOIN_NO_CMS_MATCH,
    DQ_RATIO_MISSING_NEGOTIATED,
    DQ_RATIO_NONCOMPARABLE_RATE_TYPE,
    join_canonical_jsonl_file,
    load_cms_benchmarks_by_ccn,
)


def _write_cms_csv(path: Path) -> None:
    path.write_text(
        (
            "Rndrng_Prvdr_CCN,Rndrng_Prvdr_Org_Name,Rndrng_Prvdr_City,Rndrng_Prvdr_State_Abrvtn,"
            "Rndrng_Prvdr_Zip5,DRG_Cd,DRG_Desc,Tot_Dschrgs,Avg_Submtd_Cvrd_Chrg,Avg_Tot_Pymt_Amt,Avg_Mdcr_Pymt_Amt\n"
            "220088,New England Baptist Hospital,Boston,MA,02120,469,desc,10,30000,0,10000\n"
            "220088,New England Baptist Hospital,Boston,MA,02120,470,desc,30,50000,0,20000\n"
            "310064,Atlanticare Regional Medical Center - City Campus,"
            "Atlantic City,NJ,08401,469,desc,11,131603.3636,0,24778.81818\n"
        ),
        encoding="utf-8",
    )


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    body = "".join(json.dumps(r, sort_keys=True) + "\n" for r in rows)
    path.write_text(body, encoding="utf-8")


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            out.append(json.loads(line))
    return out


def _base_row() -> dict[str, object]:
    return {
        "hospital_key": "new-england-baptist-hospital",
        "ccn": "220088",
        "procedure_code": "27447",
        "procedure_code_type": "CPT",
        "negotiated_amount": 35000.0,
        "charge_methodology": "Case Rate",
        "dq_flags": None,
    }


def test_join_sets_ccn_match_stamps_and_weighted_ratio() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        cms = root / "cms.csv"
        inp = root / "in.jsonl"
        out = root / "out.jsonl"
        _write_cms_csv(cms)
        _write_jsonl(inp, [_base_row()])

        cms_by_ccn, cms_hash = load_cms_benchmarks_by_ccn(cms)
        result = join_canonical_jsonl_file(
            input_path=inp,
            output_path=out,
            cms_by_ccn=cms_by_ccn,
            cms_snapshot_hash=cms_hash,
        )
        rows = _read_jsonl(out)
        assert result.row_count == 1
        assert result.dataset_dq_flags == []
        assert len(rows) == 1
        row = rows[0]
        assert row["cms_match_status"] == "matched_ccn_roster"
        assert row["cms_match_confidence"] == "high"
        assert row["entity_resolution_method"] == "config_ccn"
        assert row["cms_ccn"] == "220088"
        assert row["cms_drg_cd"] == "469|470"
        assert row["cms_tot_dschrgs"] == 40.0
        assert row["cms_avg_mdcr_pymt_amt"] == 17500.0
        assert row["commercial_to_medicare_ratio"] == 2.0
        assert row["cms_snapshot_hash"] == hashlib.sha256(cms.read_bytes()).hexdigest()


def test_join_prefers_exact_drg_benchmark_when_row_is_drg() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        cms = root / "cms.csv"
        inp = root / "in.jsonl"
        out = root / "out.jsonl"
        _write_cms_csv(cms)
        row = _base_row()
        row["procedure_code"] = "469"
        row["procedure_code_type"] = "MS-DRG"
        _write_jsonl(inp, [row])

        cms_by_ccn, cms_hash = load_cms_benchmarks_by_ccn(cms)
        join_canonical_jsonl_file(
            input_path=inp,
            output_path=out,
            cms_by_ccn=cms_by_ccn,
            cms_snapshot_hash=cms_hash,
        )
        joined = _read_jsonl(out)[0]
        assert joined["cms_drg_cd"] == "469"
        assert joined["cms_avg_mdcr_pymt_amt"] == 10000.0
        assert joined["commercial_to_medicare_ratio"] == 3.5


def test_join_leaves_ratio_null_and_sets_dataset_dq_for_noncomparable() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        cms = root / "cms.csv"
        inp = root / "in.jsonl"
        out = root / "out.jsonl"
        _write_cms_csv(cms)
        row = _base_row()
        row["negotiated_amount"] = None
        row["charge_methodology"] = "PERCENT OF TOTAL BILLED CHARGES"
        row["dq_flags"] = "percent_of_charges_noncomparable"
        _write_jsonl(inp, [row])

        cms_by_ccn, cms_hash = load_cms_benchmarks_by_ccn(cms)
        result = join_canonical_jsonl_file(
            input_path=inp,
            output_path=out,
            cms_by_ccn=cms_by_ccn,
            cms_snapshot_hash=cms_hash,
        )
        joined = _read_jsonl(out)[0]
        assert joined["commercial_to_medicare_ratio"] is None
        assert DQ_RATIO_NONCOMPARABLE_RATE_TYPE in result.dataset_dq_flags
        assert DQ_RATIO_MISSING_NEGOTIATED in result.dataset_dq_flags


def test_join_sets_no_match_status_when_ccn_unmatched() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        cms = root / "cms.csv"
        inp = root / "in.jsonl"
        out = root / "out.jsonl"
        _write_cms_csv(cms)
        row = _base_row()
        row["ccn"] = "999999"
        _write_jsonl(inp, [row])

        cms_by_ccn, cms_hash = load_cms_benchmarks_by_ccn(cms)
        result = join_canonical_jsonl_file(
            input_path=inp,
            output_path=out,
            cms_by_ccn=cms_by_ccn,
            cms_snapshot_hash=cms_hash,
        )
        joined = _read_jsonl(out)[0]
        assert joined["cms_match_status"] == "no_match"
        assert joined["cms_match_confidence"] is None
        assert joined["entity_resolution_method"] is None
        assert joined["cms_avg_mdcr_pymt_amt"] is None
        assert joined["commercial_to_medicare_ratio"] is None
        assert DQ_JOIN_NO_CMS_MATCH in result.dataset_dq_flags
