"""CSV parsers for hospital transparency files (wide and tall variants)."""

from __future__ import annotations

import csv
import logging
import re
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from hpt.csv_encoding import CSV_TEXT_ENCODING_ORDER
from hpt.normalize import parse_float_with_dq
from hpt.procedure_filter import select_procedure_from_code_columns

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CsvHospitalMetadata:
    transparency_hospital_name: str | None
    transparency_address: str | None
    npi_type_2: str | None


PayerRateKind = Literal["dollar", "percentage", "algorithm"]
NegotiatedValueSource = Literal[
    "negotiated_dollar",
    "estimated_amount_fallback",
    "negotiated_percentage",
    "negotiated_algorithm",
    "none",
]


@dataclass(frozen=True)
class PayerChargeInfo:
    payer_name: str
    plan_name: str | None

    negotiated_amount: float | None
    rate_raw: str | None  # percentage/algorithm raw string when no dollar amount
    charge_methodology: str | None
    rate_note: str | None

    deidentified_min: float | None
    deidentified_max: float | None

    # Silver DQ: how the negotiated row was represented when dollar amount is absent.
    rate_kind: PayerRateKind = "dollar"
    negotiated_value_source: NegotiatedValueSource = "none"
    parser_dq_flags: tuple[str, ...] = ()


def resolve_negotiated_representation(
    *,
    negotiated_amount: float | None,
    estimated_amount: float | None,
    percentage_raw: str | None,
    algorithm_raw: str | None,
) -> tuple[float | None, str | None, PayerRateKind, NegotiatedValueSource]:
    """
    Generic negotiated value resolution used by all parsers.

    Precedence:
    1) negotiated_dollar
    2) estimated_amount fallback
    3) negotiated_algorithm raw
    4) negotiated_percentage raw
    """
    if negotiated_amount is not None:
        return negotiated_amount, None, "dollar", "negotiated_dollar"
    if estimated_amount is not None:
        return estimated_amount, None, "dollar", "estimated_amount_fallback"
    if algorithm_raw:
        return None, algorithm_raw, "algorithm", "negotiated_algorithm"
    if percentage_raw:
        return None, percentage_raw, "percentage", "negotiated_percentage"
    return None, None, "dollar", "none"


@dataclass(frozen=True)
class ImplantChargeInfo:
    implant_manufacturer: str | None
    implant_product: str | None
    implant_code: str | None
    implant_rate: float | None


@dataclass(frozen=True)
class ProcedureChargeLine:
    hospital_metadata: CsvHospitalMetadata

    procedure_code: str
    procedure_code_type: str | None
    procedure_description: str | None
    match_method: str
    # Index within the post-header CSV data stream (0-based).
    source_row_index: int
    parser_strategy: str

    gross_charge: float | None
    cash: float | None

    payer_charges: list[PayerChargeInfo]
    implant: ImplantChargeInfo
    line_dq_flags: tuple[str, ...] = ()


def _as_str_cell(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalized_header_cell(col: str) -> str:
    # Normalize common spacing variants, e.g. `code | 1` -> `code|1`.
    return "|".join(part.strip() for part in col.strip().lower().split("|"))


def _extract_patient_metadata_from_map(
    header: list[str],
    values: list[str],
) -> CsvHospitalMetadata:
    meta: dict[str, str] = {}
    for k, v in zip(header, values, strict=False):
        meta[k] = v

    # NYU-style metadata header includes location_name/hospital_address/type_2_npi.
    transparency_hospital_name = meta.get("location_name") or meta.get("hospital_name")
    transparency_address = meta.get("hospital_address")
    npi_type_2 = meta.get("type_2_npi")
    if npi_type_2 is not None:
        npi_type_2 = npi_type_2.strip() or None
    return CsvHospitalMetadata(
        transparency_hospital_name=_as_str_cell(transparency_hospital_name) or None,
        transparency_address=_as_str_cell(transparency_address) or None,
        npi_type_2=npi_type_2,
    )


def _classify_csv_table_header(row: list[str]) -> tuple[bool, str | None]:
    """
    Return (is_charge_table_header, parser_strategy).

    `csv_wide_standardcharges` has payer dimensions encoded in column names:
      standard_charge|<payer or id>|<plan/label>|<measure>

    `csv_tall_variant` has row-wise payer columns such as:
      payer_name, plan_name, standard_charge|negotiated_dollar
    """
    normalized = [_normalized_header_cell(c) for c in row]
    has_code = "code|1" in normalized or any(c.startswith("code|1|") for c in normalized)
    has_desc = "description" in normalized
    if not (has_code and has_desc):
        return False, None

    has_tall_payer_cols = (
        "payer_name" in normalized
        and "plan_name" in normalized
        and any(
            c in {"standard_charge|negotiated_dollar", "standard_charge|negotiated_percentage"}
            for c in normalized
        )
    )
    if has_tall_payer_cols:
        return True, "csv_tall_variant"

    has_wide_payer_cols = any(
        c.startswith("standard_charge|")
        and len(c.split("|")) >= 4
        and c.split("|")[-1]
        in {
            "negotiated_dollar",
            "negotiated_percentage",
            "negotiated_algorithm",
            "methodology",
        }
        for c in normalized
    )
    if has_wide_payer_cols:
        return True, "csv_wide_standardcharges"

    # Fallback: treat unknown charge-table shapes as tall-like.
    return True, "csv_tall_variant"


def _code_slot(field: str) -> int | None:
    m = re.fullmatch(r"code\|(\d+)", field)
    return int(m.group(1)) if m else None


def _code_type_slot(field: str) -> int | None:
    m = re.fullmatch(r"code\|(\d+)\|type", field)
    return int(m.group(1)) if m else None


def _derive_wide_payer_name_plan(tokens: list[str]) -> tuple[str, str | None]:
    # Numeric-key schema: standard_charge|999|ACME HEALTH|negotiated_dollar
    if tokens and tokens[0].isdigit():
        payer_name = tokens[1] if len(tokens) >= 2 else tokens[0]
        plan = "|".join(tokens[2:]).strip() or None if len(tokens) > 2 else None
        return payer_name, plan
    # Direct schema: standard_charge|Aetna|ACA|negotiated_dollar
    payer_name = tokens[0] if tokens else "Unknown Payer"
    plan = "|".join(tokens[1:]).strip() or None if len(tokens) > 1 else None
    return payer_name, plan


def iter_procedure_charge_lines_from_csv(path: Path) -> Iterator[ProcedureChargeLine]:
    """
    Stream-parse a wide CSV and yield procedure-level charge lines.

    Notes:
    - This targets the assessment scope (HCPCS 27447 + DRG 469/470 fallback).
    - For "tall" CSV variants, we attempt limited support via explicit payer columns;
      the wide format is the primary implementation.
    """
    meta = CsvHospitalMetadata(
        transparency_hospital_name=None,
        transparency_address=None,
        npi_type_2=None,
    )
    table_header: list[str] | None = None

    # Common header indices.
    code_idx_by_slot: dict[int, int] = {}
    code_type_idx_by_slot: dict[int, int] = {}
    idx_description: int | None = None
    idx_gross_charge: int | None = None
    idx_cash: int | None = None

    # Tall row-wise columns.
    idx_tall_payer_name: int | None = None
    idx_tall_plan_name: int | None = None
    idx_tall_negotiated_dollar: int | None = None
    idx_tall_estimated_amount: int | None = None
    idx_tall_negotiated_percentage: int | None = None
    idx_tall_negotiated_algorithm: int | None = None
    idx_tall_methodology: int | None = None
    idx_tall_additional_notes: int | None = None
    idx_tall_deid_min: int | None = None
    idx_tall_deid_max: int | None = None

    # Wide payer column indices grouped by payer tuple key.
    payer_negotiated_dollar_idx: dict[tuple[str, ...], int] = {}
    payer_estimated_amount_idx: dict[tuple[str, ...], int] = {}
    payer_negotiated_percentage_idx: dict[tuple[str, ...], int] = {}
    payer_negotiated_algorithm_idx: dict[tuple[str, ...], int] = {}
    payer_methodology_idx: dict[tuple[str, ...], int] = {}
    payer_additional_notes_idx: dict[tuple[str, ...], int] = {}
    payer_deid_min_idx: dict[tuple[str, ...], int] = {}
    payer_deid_max_idx: dict[tuple[str, ...], int] = {}

    # Implants (optional).
    idx_implant_manufacturer: int | None = None
    idx_implant_product: int | None = None
    idx_implant_code: int | None = None
    idx_implant_rate: int | None = None

    # Read sequentially; we only materialize the header rows.
    reader: csv._reader
    used_encoding: str | None = None
    parser_strategy: str = "csv_tall_variant"
    for enc in CSV_TEXT_ENCODING_ORDER:
        try:
            with path.open("r", encoding=enc, newline="") as f:
                reader = csv.reader(f)

                # Detect optional metadata header: in NYU files the first row is metadata columns,
                # immediately followed by a values row.
                meta_header: list[str] | None = None
                table_header = None
                for raw_row in reader:
                    row = [c if c is not None else "" for c in raw_row]
                    if not row:
                        continue

                    if table_header is None:
                        if meta_header is None:
                            if (
                                "hospital_name" in row
                                and "location_name" in row
                                and "hospital_address" in row
                            ):
                                meta_header = row
                                values_row = next(reader, None)
                                if values_row:
                                    meta = _extract_patient_metadata_from_map(meta_header, values_row)
                                continue

                        # Table header start (wide or tall-shaped)
                        is_tbl, strategy = _classify_csv_table_header(row)
                        if is_tbl:
                            table_header = row
                            parser_strategy = strategy or "csv_tall_variant"
                            used_encoding = enc
                            break
                        continue
                if table_header is None:
                    continue
                # Header rows can be valid UTF-8 while later rows contain Latin-1/Windows bytes
                # (e.g. smart quotes). Drain the rest of the file to validate this encoding.
                for _ in reader:
                    pass
            break
        except UnicodeDecodeError:
            continue

    if table_header is None or used_encoding is None:
        raise ValueError(f"CSV table header not found: {path}")

    with path.open("r", encoding=used_encoding, newline="") as f:
        reader = csv.reader(f)
        # advance back to the table header by re-running lightweight detection
        meta_header: list[str] | None = None
        for raw_row in reader:
            row = [c if c is not None else "" for c in raw_row]
            if not row:
                continue
            if meta_header is None and (
                "hospital_name" in row and "location_name" in row and "hospital_address" in row
            ):
                meta_header = row
                values_row = next(reader, None)
                if values_row:
                    meta = _extract_patient_metadata_from_map(meta_header, values_row)
                continue
            is_tbl, _ = _classify_csv_table_header(row)
            if is_tbl:
                break

        # Build header indices.
        for i, col in enumerate(table_header):
            c = _normalized_header_cell(col)
            raw_parts = [part.strip() for part in col.strip().split("|")]
            slot = _code_slot(c)
            if slot is not None:
                code_idx_by_slot[slot] = i
                continue
            type_slot = _code_type_slot(c)
            if type_slot is not None:
                code_type_idx_by_slot[type_slot] = i
                continue

            if c == "description":
                idx_description = i
            elif c == "standard_charge|gross":
                idx_gross_charge = i
            elif c == "standard_charge|discounted_cash":
                idx_cash = i
            elif c == "implant_manufacturer":
                idx_implant_manufacturer = i
            elif c == "implant_product":
                idx_implant_product = i
            elif c == "implant_code":
                idx_implant_code = i
            elif c == "implant_rate":
                idx_implant_rate = i
            elif c == "payer_name":
                idx_tall_payer_name = i
            elif c == "plan_name":
                idx_tall_plan_name = i
            elif c == "standard_charge|negotiated_dollar":
                idx_tall_negotiated_dollar = i
            elif c in {"estimated_amount", "standard_charge|estimated_amount"}:
                idx_tall_estimated_amount = i
            elif c == "standard_charge|negotiated_percentage":
                idx_tall_negotiated_percentage = i
            elif c == "standard_charge|negotiated_algorithm":
                idx_tall_negotiated_algorithm = i
            elif c in {"standard_charge|methodology", "methodology"}:
                idx_tall_methodology = i
            elif c in {"additional_payer_notes", "standard_charge|additional_payer_notes"}:
                idx_tall_additional_notes = i
            elif c in {"standard_charge|min", "minimum"}:
                idx_tall_deid_min = i
            elif c in {"standard_charge|max", "maximum"}:
                idx_tall_deid_max = i
            else:
                parts_norm = c.split("|")
                if len(parts_norm) >= 4 and parts_norm[0] == "standard_charge":
                    payer_key = tuple(raw_parts[1:-1])
                    measure = parts_norm[-1]
                    if measure == "negotiated_dollar":
                        payer_negotiated_dollar_idx[payer_key] = i
                    elif measure == "estimated_amount":
                        payer_estimated_amount_idx[payer_key] = i
                    elif measure == "negotiated_percentage":
                        payer_negotiated_percentage_idx[payer_key] = i
                    elif measure == "negotiated_algorithm":
                        payer_negotiated_algorithm_idx[payer_key] = i
                    elif measure == "methodology":
                        payer_methodology_idx[payer_key] = i
                elif len(parts_norm) >= 3 and parts_norm[0] == "additional_payer_notes":
                    payer_key = tuple(raw_parts[1:])
                    payer_additional_notes_idx[payer_key] = i
                elif len(parts_norm) >= 2 and parts_norm[0] in {"10th_percentile", "90th_percentile"}:
                    payer_key = tuple(raw_parts[1:])
                    if parts_norm[0] == "10th_percentile":
                        payer_deid_min_idx[payer_key] = i
                    else:
                        payer_deid_max_idx[payer_key] = i
                elif len(parts_norm) >= 3 and parts_norm[0] == "estimated_amount":
                    payer_key = tuple(raw_parts[1:])
                    payer_estimated_amount_idx[payer_key] = i

        if not code_idx_by_slot:
            raise ValueError(f"CSV missing required `code|n` columns: {path}")

        # Continue reading after header.
        data_row_index = 0
        for raw_row in reader:
            row = [c if c is not None else "" for c in raw_row]
            current_index = data_row_index
            data_row_index += 1
            if not row:
                continue
            procedure_description = (
                _as_str_cell(row[idx_description])
                if idx_description is not None and idx_description < len(row)
                else None
            )
            code_pairs: list[tuple[str, str | None]] = []
            for slot, idx in sorted(code_idx_by_slot.items()):
                code = _as_str_cell(row[idx]) if idx < len(row) else ""
                if not code:
                    continue
                t_idx = code_type_idx_by_slot.get(slot)
                code_type = _as_str_cell(row[t_idx]) if t_idx is not None and t_idx < len(row) else None
                code_pairs.append((code, code_type))
            selected = select_procedure_from_code_columns(
                code_pairs, description=procedure_description
            )
            if selected is None:
                continue
            procedure_code, procedure_code_type, match_method = selected

            line_dq: list[str] = []

            gross_charge = None
            if idx_gross_charge is not None and idx_gross_charge < len(row):
                g, bad_g = parse_float_with_dq(row[idx_gross_charge])
                gross_charge = g
                if bad_g:
                    line_dq.append("unparseable_numeric")

            cash = None
            if idx_cash is not None and idx_cash < len(row):
                c, bad_c = parse_float_with_dq(row[idx_cash])
                cash = c
                if bad_c:
                    line_dq.append("unparseable_numeric")

            implant = ImplantChargeInfo(
                implant_manufacturer=(
                    _as_str_cell(row[idx_implant_manufacturer])
                    if idx_implant_manufacturer is not None and idx_implant_manufacturer < len(row)
                    else None
                ),
                implant_product=(
                    _as_str_cell(row[idx_implant_product])
                    if idx_implant_product is not None and idx_implant_product < len(row)
                    else None
                ),
                implant_code=(
                    _as_str_cell(row[idx_implant_code])
                    if idx_implant_code is not None and idx_implant_code < len(row)
                    else None
                ),
                implant_rate=None,
            )
            if idx_implant_rate is not None and idx_implant_rate < len(row):
                ir, bad_ir = parse_float_with_dq(row[idx_implant_rate])
                implant = ImplantChargeInfo(
                    implant_manufacturer=implant.implant_manufacturer,
                    implant_product=implant.implant_product,
                    implant_code=implant.implant_code,
                    implant_rate=ir,
                )
                if bad_ir:
                    line_dq.append("unparseable_numeric")

            payer_charges: list[PayerChargeInfo] = []
            if parser_strategy == "csv_tall_variant":
                payer_name = (
                    _as_str_cell(row[idx_tall_payer_name])
                    if idx_tall_payer_name is not None and idx_tall_payer_name < len(row)
                    else None
                )
                plan_name = (
                    _as_str_cell(row[idx_tall_plan_name])
                    if idx_tall_plan_name is not None and idx_tall_plan_name < len(row)
                    else None
                )
                if payer_name:
                    negotiated_amount = None
                    estimated_amount = None
                    rate_raw = None
                    payer_dq: list[str] = []

                    if idx_tall_negotiated_dollar is not None and idx_tall_negotiated_dollar < len(row):
                        negotiated_amount, bad_dollar = parse_float_with_dq(row[idx_tall_negotiated_dollar])
                        if bad_dollar:
                            payer_dq.append("unparseable_numeric")
                    if idx_tall_estimated_amount is not None and idx_tall_estimated_amount < len(row):
                        estimated_amount, bad_est = parse_float_with_dq(row[idx_tall_estimated_amount])
                        if bad_est:
                            payer_dq.append("unparseable_numeric")
                    pct_raw = (
                        _as_str_cell(row[idx_tall_negotiated_percentage]) or None
                        if idx_tall_negotiated_percentage is not None
                        and idx_tall_negotiated_percentage < len(row)
                        else None
                    )
                    algo_raw = (
                        _as_str_cell(row[idx_tall_negotiated_algorithm]) or None
                        if idx_tall_negotiated_algorithm is not None
                        and idx_tall_negotiated_algorithm < len(row)
                        else None
                    )
                    negotiated_amount, rate_raw, rate_kind, value_source = (
                        resolve_negotiated_representation(
                            negotiated_amount=negotiated_amount,
                            estimated_amount=estimated_amount,
                            percentage_raw=pct_raw,
                            algorithm_raw=algo_raw,
                        )
                    )

                    methodology = (
                        _as_str_cell(row[idx_tall_methodology]) or None
                        if idx_tall_methodology is not None and idx_tall_methodology < len(row)
                        else None
                    )
                    additional_notes = (
                        _as_str_cell(row[idx_tall_additional_notes]) or None
                        if idx_tall_additional_notes is not None
                        and idx_tall_additional_notes < len(row)
                        else None
                    )
                    deid_min = None
                    if idx_tall_deid_min is not None and idx_tall_deid_min < len(row):
                        deid_min, bad_min = parse_float_with_dq(row[idx_tall_deid_min])
                        if bad_min:
                            line_dq.append("unparseable_numeric")
                    deid_max = None
                    if idx_tall_deid_max is not None and idx_tall_deid_max < len(row):
                        deid_max, bad_max = parse_float_with_dq(row[idx_tall_deid_max])
                        if bad_max:
                            line_dq.append("unparseable_numeric")

                    if negotiated_amount is not None or rate_raw is not None:
                        payer_charges.append(
                            PayerChargeInfo(
                                payer_name=payer_name,
                                plan_name=plan_name,
                                negotiated_amount=negotiated_amount,
                                rate_raw=rate_raw,
                                charge_methodology=methodology,
                                rate_note=additional_notes,
                                deidentified_min=deid_min,
                                deidentified_max=deid_max,
                                rate_kind=rate_kind,
                                negotiated_value_source=value_source,
                                parser_dq_flags=tuple(payer_dq),
                            )
                        )
            else:
                payer_keys: set[tuple[str, ...]] = set()
                payer_keys.update(payer_negotiated_dollar_idx.keys())
                payer_keys.update(payer_estimated_amount_idx.keys())
                payer_keys.update(payer_negotiated_percentage_idx.keys())
                payer_keys.update(payer_negotiated_algorithm_idx.keys())
                payer_keys.update(payer_methodology_idx.keys())
                payer_keys.update(payer_additional_notes_idx.keys())
                payer_keys.update(payer_deid_min_idx.keys())
                payer_keys.update(payer_deid_max_idx.keys())

                for payer_key in sorted(payer_keys):
                    payer_name, plan_name = _derive_wide_payer_name_plan(list(payer_key))
                    negotiated_amount = None
                    estimated_amount = None
                    rate_raw = None
                    payer_dq: list[str] = []

                    idx = payer_negotiated_dollar_idx.get(payer_key)
                    if idx is not None and idx < len(row):
                        negotiated_amount, bad_dollar = parse_float_with_dq(row[idx])
                        if bad_dollar:
                            payer_dq.append("unparseable_numeric")
                    idx = payer_estimated_amount_idx.get(payer_key)
                    if idx is not None and idx < len(row):
                        estimated_amount, bad_est = parse_float_with_dq(row[idx])
                        if bad_est:
                            payer_dq.append("unparseable_numeric")
                    pct_raw: str | None = None
                    idx = payer_negotiated_percentage_idx.get(payer_key)
                    if idx is not None and idx < len(row):
                        pct_raw = _as_str_cell(row[idx]) or None
                    algo_raw: str | None = None
                    idx = payer_negotiated_algorithm_idx.get(payer_key)
                    if idx is not None and idx < len(row):
                        algo_raw = _as_str_cell(row[idx]) or None
                    negotiated_amount, rate_raw, rate_kind, value_source = (
                        resolve_negotiated_representation(
                            negotiated_amount=negotiated_amount,
                            estimated_amount=estimated_amount,
                            percentage_raw=pct_raw,
                            algorithm_raw=algo_raw,
                        )
                    )
                    if negotiated_amount is None and rate_raw is None:
                        continue

                    methodology = None
                    idx = payer_methodology_idx.get(payer_key)
                    if idx is not None and idx < len(row):
                        methodology = _as_str_cell(row[idx]) or None
                    additional_notes = None
                    idx = payer_additional_notes_idx.get(payer_key)
                    if idx is not None and idx < len(row):
                        additional_notes = _as_str_cell(row[idx]) or None
                    deid_min = None
                    idx = payer_deid_min_idx.get(payer_key)
                    if idx is not None and idx < len(row):
                        deid_min, bad_min = parse_float_with_dq(row[idx])
                        if bad_min:
                            line_dq.append("unparseable_numeric")
                    deid_max = None
                    idx = payer_deid_max_idx.get(payer_key)
                    if idx is not None and idx < len(row):
                        deid_max, bad_max = parse_float_with_dq(row[idx])
                        if bad_max:
                            line_dq.append("unparseable_numeric")

                    payer_charges.append(
                        PayerChargeInfo(
                            payer_name=payer_name,
                            plan_name=plan_name,
                            negotiated_amount=negotiated_amount,
                            rate_raw=rate_raw,
                            charge_methodology=methodology,
                            rate_note=additional_notes,
                            deidentified_min=deid_min,
                            deidentified_max=deid_max,
                            rate_kind=rate_kind,
                            negotiated_value_source=value_source,
                            parser_dq_flags=tuple(payer_dq),
                        )
                    )

            if not payer_charges:
                logger.debug("[%s] skipping matched procedure without payer negotiated rows", path.name)
                continue

            yield ProcedureChargeLine(
                hospital_metadata=meta,
                procedure_code=procedure_code,
                procedure_code_type=procedure_code_type,
                procedure_description=procedure_description,
                match_method=match_method,
                source_row_index=current_index,
                parser_strategy=parser_strategy,
                gross_charge=gross_charge,
                cash=cash,
                payer_charges=payer_charges,
                implant=implant,
                line_dq_flags=tuple(dict.fromkeys(line_dq)),
            )

