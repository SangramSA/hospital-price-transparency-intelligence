from __future__ import annotations

import json
import tempfile
from pathlib import Path

from hpt.extract import extract_canonical_rows_from_file, iter_canonical_rows_from_file
from hpt.models import Hospital


def _hospital_fixture(*, ccn: str | None = "123") -> Hospital:
    return Hospital(
        hospital_key="test-hospital",
        hospital_name="Test Hospital",
        state="ZZ",
        ccn=ccn,
        tier=1,
        website_root="https://example.org",
        cms_hpt_index_url=None,
        mrf_url="https://example.org/mrf/test_standardcharges.csv",
        source_page_url=None,
    )


def _pick(rows: list[dict], **criteria: object) -> list[dict]:
    out: list[dict] = []
    for r in rows:
        if all(r.get(k) == v for k, v in criteria.items()):
            out.append(r)
    return out


def test_extract_wide_csv_tka() -> None:
    hospital = _hospital_fixture(ccn="123")
    fp = Path("tests/fixtures/sample_wide_tka.csv")
    result = extract_canonical_rows_from_file(hospital, fp)
    assert result.jsonl_path is None
    rows = result.rows

    ccn = "000123"
    negotiated = _pick(
        rows,
        procedure_code="27447",
        rate_type="negotiated",
        payer_name="ACME Health",
    )
    assert len(negotiated) == 1
    r0 = negotiated[0]
    assert r0["ccn"] == ccn
    assert r0["procedure_code_type"] == "CPT"
    assert r0["procedure_description"] == "TOTAL KNEE ARTHROPLASTY"
    assert r0["match_method"] == "hcpcs_exact"
    assert r0["negotiated_amount"] == 500.0
    assert r0["rate_raw"] is None
    assert r0["charge_methodology"] == "Case Rate"
    assert r0["rate_note"] == "Some notes"
    assert r0["payer_name_normalized"] == "acme health"
    assert r0["parser_strategy"].startswith("csv_wide_standardcharges")
    assert isinstance(r0["source_row_index"], int)
    assert r0["extractor_version"] == "0.1.0"
    assert r0["dq_flags"] is None
    assert r0["gross_charge"] == 1000.0
    assert r0["discounted_cash"] == 800.0
    assert r0["deidentified_min"] == 450.0
    assert r0["deidentified_max"] == 550.0
    assert len(rows) == 1


def test_extract_nested_json_drg() -> None:
    hospital = _hospital_fixture(ccn="123")
    fp = Path("tests/fixtures/sample_json_drg469.json")
    # Note: file includes DRG code 469 -> drg_fallback
    result = extract_canonical_rows_from_file(
        hospital,
        fp,
        source_file_url="https://example.org/mrf/test.json",
    )
    assert result.jsonl_path is None
    rows = result.rows

    negotiated = _pick(rows, procedure_code="469", rate_type="negotiated", payer_name="Medicare")
    assert len(negotiated) == 1
    r0 = negotiated[0]
    assert r0["ccn"] == "000123"
    assert r0["procedure_code_type"] == "MS-DRG"
    assert "setting=both" in (r0["rate_note"] or "")
    assert r0["negotiated_amount"] == 17000.0
    assert r0["charge_methodology"] == "other"
    assert r0["match_method"] == "drg_fallback"
    assert r0["parser_strategy"].startswith("json_nested_standard_charge_information")
    assert r0["source_json_path"] == "standard_charge_information[0].standard_charges[0]"
    assert r0["extractor_version"] == "0.1.0"
    assert r0["negotiated_value_source"] == "estimated_amount_fallback"
    assert "negotiated_amount_inferred_from_estimated" in (r0["dq_flags"] or "")
    assert r0["deidentified_min"] == 12345.67
    assert r0["deidentified_max"] == 23456.78
    assert r0["gross_charge"] is None
    assert r0["discounted_cash"] is None
    assert len(rows) == 1


def test_extract_streams_to_jsonl_when_over_threshold() -> None:
    hospital = _hospital_fixture(ccn="123")
    fp = Path("tests/fixtures/sample_wide_tka.csv")
    with tempfile.TemporaryDirectory() as tmp:
        silver = Path(tmp) / "silver"
        result = extract_canonical_rows_from_file(
            hospital,
            fp,
            stream_threshold_bytes=0,
            silver_output_dir=silver,
        )
        assert result.rows == []
        assert result.jsonl_path is not None
        assert result.jsonl_path.is_file()
        lines = result.jsonl_path.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 1
        row0 = json.loads(lines[0])
        assert row0["hospital_key"] == hospital.hospital_key
        assert row0["extractor_version"] == "0.1.0"
        assert row0["parser_strategy"].startswith("csv_wide_standardcharges")


def test_iter_canonical_rows_is_streaming_iterator() -> None:
    hospital = _hospital_fixture(ccn="123")
    fp = Path("tests/fixtures/sample_wide_tka.csv")
    it = iter_canonical_rows_from_file(hospital, fp)
    first = next(it)
    assert first["procedure_code"] == "27447"
    assert first["rate_type"] == "negotiated"


def test_csv_percentage_only_negotiated_dq_flag() -> None:
    """Wide payer-keyed row with percent-only rate emits noncomparable DQ."""
    hospital = _hospital_fixture(ccn="123")
    body = (
        "code|1,code|1|type,description,standard_charge|gross,standard_charge|discounted_cash,"
        "standard_charge|9|ACME Health|negotiated_percentage\n"
        "27447,CPT,TOTAL KNEE ARTHROPLASTY,1000,800,50%\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        fp = Path(tmp) / "pct_only_standardcharges.csv"
        fp.write_text(body, encoding="utf-8")
        result = extract_canonical_rows_from_file(hospital, fp)
        rows = result.rows
    negotiated = _pick(
        rows,
        procedure_code="27447",
        rate_type="negotiated",
        payer_name="ACME Health",
    )
    assert len(negotiated) == 1
    assert negotiated[0]["parser_strategy"].startswith("csv_wide_standardcharges")
    assert negotiated[0]["negotiated_amount"] is None
    assert negotiated[0]["rate_raw"] == "50%"
    assert negotiated[0]["dq_flags"] == "percent_of_charges_noncomparable"


def test_csv_tall_rowwise_negotiated_extraction() -> None:
    hospital = _hospital_fixture(ccn="123")
    body = (
        "hospital_name,last_updated_on,version,location_name,hospital_address,type_2_npi\n"
        "Test Hospital,2026-01-01,2.0.0,Test Hospital Main,1 Main St,1234567890\n"
        "description,code|1,code|1|type,payer_name,plan_name,standard_charge|gross,standard_charge|discounted_cash,"
        "standard_charge|negotiated_dollar,standard_charge|methodology,additional_payer_notes\n"
        "TOTAL KNEE ARTHROPLASTY,27447,CPT,ACME Health,Gold,1200,950,800,Case Rate,Rowwise payer\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        fp = Path(tmp) / "tall_rowwise_standardcharges.csv"
        fp.write_text(body, encoding="utf-8")
        result = extract_canonical_rows_from_file(hospital, fp)
        rows = result.rows
    negotiated = _pick(
        rows, procedure_code="27447", rate_type="negotiated", payer_name="ACME Health"
    )
    assert len(negotiated) == 1
    r0 = negotiated[0]
    assert r0["parser_strategy"].startswith("csv_tall_variant|csv_")
    assert r0["plan_name"] == "Gold"
    assert r0["negotiated_amount"] == 800.0
    assert r0["charge_methodology"] == "Case Rate"
    assert r0["rate_note"] == "Rowwise payer"


def test_csv_tall_estimated_amount_fallback_for_negotiated() -> None:
    hospital = _hospital_fixture(ccn="123")
    body = (
        "description,code|1,code|1|type,payer_name,plan_name,standard_charge|negotiated_dollar,"
        "estimated_amount,standard_charge|negotiated_algorithm\n"
        "TOTAL KNEE ARTHROPLASTY,27447,CPT,ACME Health,Gold,,9876.54,Algorithm text\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        fp = Path(tmp) / "tall_estimated_fallback_standardcharges.csv"
        fp.write_text(body, encoding="utf-8")
        rows = extract_canonical_rows_from_file(hospital, fp).rows
    negotiated = _pick(
        rows, procedure_code="27447", rate_type="negotiated", payer_name="ACME Health"
    )
    assert len(negotiated) == 1
    r0 = negotiated[0]
    assert r0["negotiated_amount"] == 9876.54
    assert r0["negotiated_value_source"] == "estimated_amount_fallback"
    assert "negotiated_amount_inferred_from_estimated" in (r0["dq_flags"] or "")


def test_csv_wide_estimated_amount_fallback_for_negotiated() -> None:
    hospital = _hospital_fixture(ccn="123")
    body = (
        "description,code|1,code|1|type,standard_charge|PayerA|PlanX|negotiated_dollar,"
        "estimated_amount|PayerA|PlanX,standard_charge|PayerA|PlanX|negotiated_algorithm\n"
        "TOTAL KNEE ARTHROPLASTY,27447,CPT,,7654.32,Algo\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        fp = Path(tmp) / "wide_estimated_fallback_standardcharges.csv"
        fp.write_text(body, encoding="utf-8")
        rows = extract_canonical_rows_from_file(hospital, fp).rows
    negotiated = _pick(rows, procedure_code="27447", rate_type="negotiated", payer_name="PayerA")
    assert len(negotiated) == 1
    r0 = negotiated[0]
    assert r0["negotiated_amount"] == 7654.32
    assert r0["negotiated_value_source"] == "estimated_amount_fallback"
    assert "negotiated_amount_inferred_from_estimated" in (r0["dq_flags"] or "")


def test_csv_prefers_hcpcs_27447_from_non_primary_code_column() -> None:
    hospital = _hospital_fixture(ccn="123")
    body = (
        "description,code|1,code|1|type,code|2,code|2|type,standard_charge|gross,"
        "standard_charge|discounted_cash,standard_charge|1|PayerA|negotiated_dollar\n"
        "TOTAL KNEE ARTHROPLASTY,469,CDM,27447,HCPCS,2000,1500,750\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        fp = Path(tmp) / "code2_hcpcs_standardcharges.csv"
        fp.write_text(body, encoding="utf-8")
        result = extract_canonical_rows_from_file(hospital, fp)
        rows = result.rows
    assert len(rows) == 1
    assert rows[0]["procedure_code"] == "27447"
    assert rows[0]["match_method"] == "hcpcs_exact"
    assert rows[0]["negotiated_amount"] == 750.0
    assert rows[0]["gross_charge"] == 2000.0
    assert rows[0]["discounted_cash"] == 1500.0


def test_csv_rejects_apr_drg_469_without_joint_description() -> None:
    hospital = _hospital_fixture(ccn="123")
    body = (
        "description,code|1,code|1|type,standard_charge|1|Payer|negotiated_dollar\n"
        "Acute Kidney Injury,469,APR-DRG,100\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        fp = Path(tmp) / "apr_drg_false_positive_standardcharges.csv"
        fp.write_text(body, encoding="utf-8")
        rows = extract_canonical_rows_from_file(hospital, fp).rows
    assert rows == []


def test_csv_accepts_ms_drg_469_with_empty_description() -> None:
    hospital = _hospital_fixture(ccn="123")
    body = (
        "description,code|1,code|1|type,standard_charge|1|Payer|negotiated_dollar\n"
        ",469,MS-DRG,250\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        fp = Path(tmp) / "ms_drg_empty_desc_standardcharges.csv"
        fp.write_text(body, encoding="utf-8")
        rows = extract_canonical_rows_from_file(hospital, fp).rows
    assert len(rows) == 1
    assert rows[0]["procedure_code"] == "469"
    assert rows[0]["match_method"] == "drg_fallback"


def test_csv_rejects_ms_drg_469_when_description_conflicts() -> None:
    hospital = _hospital_fixture(ccn="123")
    body = (
        "description,code|1,code|1|type,standard_charge|1|Payer|negotiated_dollar\n"
        "Acute Kidney Injury,469,MS-DRG,250\n"
    )
    with tempfile.TemporaryDirectory() as tmp:
        fp = Path(tmp) / "ms_drg_kidney_standardcharges.csv"
        fp.write_text(body, encoding="utf-8")
        rows = extract_canonical_rows_from_file(hospital, fp).rows
    assert rows == []


def test_json_missing_payer_name_flag_on_valid_rows() -> None:
    hospital = _hospital_fixture(ccn="123")
    payload = {
        "standard_charge_information": [
            {
                "description": "Knee DRG",
                "code_information": [{"code": "469", "type": "MS-DRG"}],
                "standard_charges": [
                    {
                        "payers_information": [
                            {"payer_name": "   ", "estimated_amount": 1.0},
                            {
                                "payer_name": "Good Payer",
                                "estimated_amount": 2500.0,
                                "methodology": "other",
                            },
                        ]
                    }
                ],
            }
        ]
    }
    with tempfile.TemporaryDirectory() as tmp:
        fp = Path(tmp) / "bad_payer_standardcharges.json"
        fp.write_text(json.dumps(payload), encoding="utf-8")
        result = extract_canonical_rows_from_file(hospital, fp)
        rows = result.rows
    good = _pick(rows, payer_name="Good Payer", rate_type="negotiated")
    assert len(good) == 1
    assert "missing_payer_name" in (good[0].get("dq_flags") or "")
