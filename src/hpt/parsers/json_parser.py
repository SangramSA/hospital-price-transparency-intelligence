"""JSON parser for nested hospital transparency files (streaming via ijson)."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import ijson

from hpt.normalize import parse_float_with_dq
from hpt.parsers.csv_parser import (
    ImplantChargeInfo,
    PayerChargeInfo,
    resolve_negotiated_representation,
)
from hpt.procedure_filter import select_procedure_from_code_columns

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JsonHospitalMetadata:
    transparency_hospital_name: str | None
    transparency_address: str | None
    npi_type_2: str | None


@dataclass(frozen=True)
class ProcedureChargeLine:
    hospital_metadata: JsonHospitalMetadata

    procedure_code: str
    procedure_code_type: str | None
    procedure_description: str | None
    match_method: str
    # Stable path (derived during streaming) for the matched standard-charge entry.
    source_json_path: str
    parser_strategy: str

    gross_charge: float | None
    cash: float | None
    deidentified_min: float | None
    deidentified_max: float | None

    payer_charges: list[PayerChargeInfo]
    implant: ImplantChargeInfo
    line_dq_flags: tuple[str, ...] = ()


def _as_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def iter_procedure_charge_lines_from_json(path: Path) -> Iterator[ProcedureChargeLine]:
    """
    Stream-parse a JSON MRF into procedure-level charge lines.

    Assumes JSON layout with:
    - top-level `standard_charge_information` list
    - each item has `code_information` and `standard_charges`
    - `standard_charges` entries include `payers_information` for negotiated values
    """
    parser_strategy = "json_nested_standard_charge_information"

    hospital_metadata = JsonHospitalMetadata(
        transparency_hospital_name=None,
        transparency_address=None,
        npi_type_2=None,
    )

    # JSONL support (best-effort): treat each line as a full standard-charge-information object.
    if path.suffix.lower() == ".jsonl":
        with path.open("r", encoding="utf-8") as f:
            for line_idx, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                yield from _iter_from_json_obj(
                    obj,
                    hospital_metadata,
                    parser_strategy=parser_strategy,
                    source_json_path=f"standard_charge_information[{line_idx}]",
                )
        return

    with path.open("rb") as f:
        for item_idx, info in enumerate(ijson.items(f, "standard_charge_information.item")):
            procedure_description = _as_str(info.get("description")) or None
            code_pairs: list[tuple[str, str | None]] = []
            for ci in info.get("code_information") or []:
                code = _as_str(ci.get("code"))
                if not code:
                    continue
                code_pairs.append((code, _as_str(ci.get("type")) or None))
            selected = select_procedure_from_code_columns(
                code_pairs, description=procedure_description
            )
            if selected is None:
                continue
            match_code, match_type, match_method = selected

            standard_charges = info.get("standard_charges") or []
            if not isinstance(standard_charges, list):
                continue

            implant = ImplantChargeInfo(
                implant_manufacturer=None,
                implant_product=None,
                implant_code=None,
                implant_rate=None,
            )

            for sc_idx, sc in enumerate(standard_charges):
                if not isinstance(sc, dict):
                    continue
                source_json_path = (
                    f"standard_charge_information[{item_idx}].standard_charges[{sc_idx}]"
                )
                line_dq: list[str] = []
                gross_charge, bad_g = parse_float_with_dq(sc.get("gross_charge"))
                if bad_g:
                    line_dq.append("unparseable_numeric")
                cash, bad_c = parse_float_with_dq(sc.get("discounted_cash"))
                if bad_c:
                    line_dq.append("unparseable_numeric")
                deid_min, bad_min = parse_float_with_dq(sc.get("minimum"))
                if bad_min:
                    line_dq.append("unparseable_numeric")
                deid_max, bad_max = parse_float_with_dq(sc.get("maximum"))
                if bad_max:
                    line_dq.append("unparseable_numeric")

                setting = _as_str(sc.get("setting")) or None

                payer_infos = sc.get("payers_information") or []
                payer_charges: list[PayerChargeInfo] = []
                if isinstance(payer_infos, list):
                    for p in payer_infos:
                        if not isinstance(p, dict):
                            continue
                        payer_name = _as_str(p.get("payer_name"))
                        if not payer_name:
                            line_dq.append("missing_payer_name")
                            continue
                        plan_name = _as_str(p.get("plan_name")) or None

                        negotiated_amount, bad_dollar = parse_float_with_dq(
                            p.get("negotiated_dollar")
                        )
                        estimated_amount, bad_est = parse_float_with_dq(p.get("estimated_amount"))
                        payer_dq: list[str] = []
                        if bad_dollar:
                            payer_dq.append("unparseable_numeric")
                        if bad_est:
                            payer_dq.append("unparseable_numeric")
                        methodology = _as_str(p.get("methodology")) or None
                        algorithm = _as_str(p.get("standard_charge_algorithm")) or None
                        additional_notes = _as_str(p.get("additional_payer_notes")) or None

                        rate_note_parts: list[str] = []
                        if setting:
                            rate_note_parts.append(f"setting={setting}")
                        if additional_notes:
                            rate_note_parts.append(additional_notes)
                        negotiated_amount, rate_raw, rate_kind, value_source = (
                            resolve_negotiated_representation(
                                negotiated_amount=negotiated_amount,
                                estimated_amount=estimated_amount,
                                percentage_raw=None,
                                algorithm_raw=algorithm or None,
                            )
                        )
                        if algorithm and negotiated_amount is not None:
                            rate_note_parts.append(f"algorithm={algorithm[:200]}")

                        rate_note = " | ".join(part for part in rate_note_parts if part)

                        payer_charges.append(
                            PayerChargeInfo(
                                payer_name=payer_name,
                                plan_name=plan_name,
                                negotiated_amount=negotiated_amount,
                                rate_raw=rate_raw,
                                charge_methodology=methodology,
                                rate_note=rate_note or None,
                                deidentified_min=None,
                                deidentified_max=None,
                                rate_kind=rate_kind,
                                negotiated_value_source=value_source,
                                parser_dq_flags=tuple(payer_dq),
                            )
                        )

                if not payer_charges:
                    continue
                yield ProcedureChargeLine(
                    hospital_metadata=hospital_metadata,
                    procedure_code=match_code,
                    procedure_code_type=match_type,
                    procedure_description=procedure_description,
                    match_method=match_method,
                    source_json_path=source_json_path,
                    parser_strategy=parser_strategy,
                    gross_charge=gross_charge,
                    cash=cash,
                    deidentified_min=deid_min,
                    deidentified_max=deid_max,
                    payer_charges=payer_charges,
                    implant=implant,
                    line_dq_flags=tuple(dict.fromkeys(line_dq)),
                )


def _iter_from_json_obj(
    obj: dict,
    hospital_metadata: JsonHospitalMetadata,
    *,
    parser_strategy: str,
    source_json_path: str,
) -> Iterator[ProcedureChargeLine]:
    """
    Best-effort: process a dict assumed to be a `standard_charge_information` object.
    """
    procedure_description = _as_str(obj.get("description")) or None
    code_pairs: list[tuple[str, str | None]] = []
    for ci in obj.get("code_information") or []:
        code = _as_str(ci.get("code"))
        if not code:
            continue
        code_pairs.append((code, _as_str(ci.get("type")) or None))
    selected = select_procedure_from_code_columns(code_pairs, description=procedure_description)
    if selected is None:
        return
    match_code, match_type, match_method = selected
    implant = ImplantChargeInfo(
        implant_manufacturer=None,
        implant_product=None,
        implant_code=None,
        implant_rate=None,
    )

    standard_charges = obj.get("standard_charges") or []
    for sc_idx, sc in enumerate(standard_charges if isinstance(standard_charges, list) else []):
        if not isinstance(sc, dict):
            continue
        sc_path = f"{source_json_path}.standard_charges[{sc_idx}]"
        line_dq: list[str] = []
        gross_charge, bad_g = parse_float_with_dq(sc.get("gross_charge"))
        if bad_g:
            line_dq.append("unparseable_numeric")
        cash, bad_c = parse_float_with_dq(sc.get("discounted_cash"))
        if bad_c:
            line_dq.append("unparseable_numeric")
        deid_min, bad_min = parse_float_with_dq(sc.get("minimum"))
        if bad_min:
            line_dq.append("unparseable_numeric")
        deid_max, bad_max = parse_float_with_dq(sc.get("maximum"))
        if bad_max:
            line_dq.append("unparseable_numeric")

        setting = _as_str(sc.get("setting")) or None
        payer_infos = sc.get("payers_information") or []
        payer_charges: list[PayerChargeInfo] = []
        if isinstance(payer_infos, list):
            for p in payer_infos:
                if not isinstance(p, dict):
                    continue
                payer_name = _as_str(p.get("payer_name"))
                if not payer_name:
                    line_dq.append("missing_payer_name")
                    continue
                plan_name = _as_str(p.get("plan_name")) or None
                negotiated_amount, bad_dollar = parse_float_with_dq(p.get("negotiated_dollar"))
                estimated_amount, bad_est = parse_float_with_dq(p.get("estimated_amount"))
                payer_dq: list[str] = []
                if bad_dollar:
                    payer_dq.append("unparseable_numeric")
                if bad_est:
                    payer_dq.append("unparseable_numeric")
                methodology = _as_str(p.get("methodology")) or None
                algorithm = _as_str(p.get("standard_charge_algorithm")) or None
                additional_notes = _as_str(p.get("additional_payer_notes")) or None

                rate_note_parts: list[str] = []
                if setting:
                    rate_note_parts.append(f"setting={setting}")
                if additional_notes:
                    rate_note_parts.append(additional_notes)
                negotiated_amount, rate_raw, rate_kind, value_source = (
                    resolve_negotiated_representation(
                        negotiated_amount=negotiated_amount,
                        estimated_amount=estimated_amount,
                        percentage_raw=None,
                        algorithm_raw=algorithm or None,
                    )
                )
                if algorithm and negotiated_amount is not None:
                    rate_note_parts.append(f"algorithm={algorithm[:200]}")

                payer_charges.append(
                    PayerChargeInfo(
                        payer_name=payer_name,
                        plan_name=plan_name,
                        negotiated_amount=negotiated_amount,
                        rate_raw=rate_raw,
                        charge_methodology=methodology,
                        rate_note=" | ".join(part for part in rate_note_parts if part) or None,
                        deidentified_min=None,
                        deidentified_max=None,
                        rate_kind=rate_kind,
                        negotiated_value_source=value_source,
                        parser_dq_flags=tuple(payer_dq),
                    )
                )

        if not payer_charges:
            continue
        yield ProcedureChargeLine(
            hospital_metadata=hospital_metadata,
            procedure_code=match_code,
            procedure_code_type=match_type,
            procedure_description=procedure_description,
            match_method=match_method,
            source_json_path=sc_path,
            parser_strategy=parser_strategy,
            gross_charge=gross_charge,
            cash=cash,
            deidentified_min=deid_min,
            deidentified_max=deid_max,
            payer_charges=payer_charges,
            implant=implant,
            line_dq_flags=tuple(dict.fromkeys(line_dq)),
        )

