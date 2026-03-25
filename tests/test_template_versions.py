from __future__ import annotations

import json
import tempfile
from pathlib import Path

from hpt.template_versions import detect_template_for_file


def test_detect_csv_v2_header() -> None:
    body = (
        "hospital_name,last_updated_on,version,hospital_location,hospital_address,license_number|CA,"
        '"To the best of its knowledge and belief, the hospital has'
        ' included all applicable standard charge information"\n'
        "Example,2025-01-01,2.0.0,Loc,Addr,123,TRUE\n"
        "description,code|1,code|1|type,payer_name,plan_name,standard_charge|negotiated_dollar,standard_charge|gross\n"
        "TOTAL KNEE ARTHROPLASTY,27447,CPT,ACME,Gold,500,1000\n"
    )
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "x.csv"
        p.write_text(body, encoding="utf-8")
        d = detect_template_for_file(p)
    assert d.template_family == "v2"
    assert d.template_version_raw == "2.0.0"
    assert d.format_kind == "csv"


def test_detect_json_v3_attestation() -> None:
    payload = {
        "hospital_name": "X",
        "version": "3.0.0",
        "attestation": {"attestation": "true"},
        "standard_charge_information": [],
    }
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "x.json"
        p.write_text(json.dumps(payload), encoding="utf-8")
        d = detect_template_for_file(p)
    assert d.template_family == "v3"
    assert d.template_version_raw == "3.0.0"
    assert d.is_conformant


def test_detect_family_from_variant_v2_version_string() -> None:
    body = (
        "hospital_name,last_updated_on,version\n"
        "X,2026-01-01,V2.0.0_Wide_CSV_Format\n"
        "description,code|1,code|1|type,payer_name,plan_name,standard_charge|negotiated_dollar,standard_charge|gross\n"
        "TOTAL KNEE ARTHROPLASTY,27447,CPT,ACME,Gold,500,1000\n"
    )
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "x.csv"
        p.write_text(body, encoding="utf-8")
        d = detect_template_for_file(p)
    assert d.template_family == "v2"
