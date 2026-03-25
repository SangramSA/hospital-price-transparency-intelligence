"""Extract procedure-level payer negotiated rates into the canonical schema."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from hpt import __version__ as package_version
from hpt.config import extract_stream_threshold_bytes, silver_dir
from hpt.models import Hospital
from hpt.normalize import normalize_ccn, normalize_payer_name
from hpt.parsers.csv_parser import PayerChargeInfo, iter_procedure_charge_lines_from_csv
from hpt.parsers.json_parser import iter_procedure_charge_lines_from_json
from hpt.template_versions import detect_template_for_file

logger = logging.getLogger(__name__)

_VALID_MRF_SUFFIXES = frozenset({".csv", ".json", ".jsonl"})


def _iter_standard_charge_artifact_paths(hospital_raw_dir: Path) -> list[Path]:
    """Collect MRF candidates from `data/raw/{key}/` and `.../artifacts/`."""
    roots = [hospital_raw_dir]
    artifacts = hospital_raw_dir / "artifacts"
    if artifacts.is_dir():
        roots.append(artifacts)
    out: list[Path] = []
    for root in roots:
        for p in root.iterdir():
            if (
                p.is_file()
                and "_standardcharges" in p.name
                and p.suffix.lower() in _VALID_MRF_SUFFIXES
            ):
                out.append(p)
    return out


def select_standard_charges_artifact(hospital_raw_dir: Path) -> Path:
    """
    Choose one MRF file to extract.

    Prefers the canonical downloaded name over ad-hoc re-encoded copies (e.g.
    ``*_latin1_to_utf8.csv``) so silver outputs stay stable when encoding is handled in parsers.
    """
    candidates = _iter_standard_charge_artifact_paths(hospital_raw_dir)
    if not candidates:
        raise FileNotFoundError(
            f"No *_standardcharges.(csv|json|jsonl) found under {hospital_raw_dir} (or artifacts/)"
        )

    def _artifact_sort_key(p: Path) -> tuple[int, float]:
        name = p.name.lower()
        penalty = 0
        if "_latin1_to_utf8" in name or "_latin-1_to_utf8" in name:
            penalty += 100
        return (penalty, -p.stat().st_mtime)

    return sorted(candidates, key=_artifact_sort_key)[0]


def _cleanup_stale_silver_canonical_same_prefix(*, silver_hospital_dir: Path, kept: Path) -> None:
    """Remove sibling ``*.canonical.jsonl`` files that share the same 16-hex artifact prefix."""
    if not silver_hospital_dir.is_dir():
        return
    if not kept.name.endswith(".canonical.jsonl"):
        return
    head = kept.name.split("_", 1)[0]
    if len(head) != 16:
        return
    try:
        int(head, 16)
    except ValueError:
        return
    for p in silver_hospital_dir.glob("*.canonical.jsonl"):
        if p == kept:
            continue
        if p.name.startswith(head + "_") and p.name.endswith(".canonical.jsonl"):
            try:
                p.unlink()
                logger.info(
                    "[%s] removed stale silver canonical (same artifact prefix): %s",
                    silver_hospital_dir.name,
                    p.name,
                )
            except OSError as e:
                logger.warning("could not remove stale silver %s: %s", p, e)


CANONICAL_COLUMNS: list[str] = [
    # Identifier and hospital fields
    "hospital_key",
    "hospital_name",
    "state",
    "ccn",
    "ein",
    "npi_type_2",
    "transparency_hospital_name",
    "transparency_address",
    # Procedure fields
    "procedure_code",
    "procedure_code_type",
    "procedure_description",
    "match_method",
    # Payer and rate fields
    "payer_name",
    "payer_name_normalized",
    "plan_name",
    "rate_type",
    "negotiated_amount",
    "currency",
    "rate_raw",
    "negotiated_value_source",
    "charge_methodology",
    "rate_note",
    # Non-negotiated standard charges on the same grain as negotiated (no synthetic payer rows).
    "gross_charge",
    "discounted_cash",
    "deidentified_min",
    "deidentified_max",
    # Implant fields (nullable)
    "implant_manufacturer",
    "implant_product",
    "implant_code",
    "implant_rate",
    # CMS join fields (nullable placeholders until join)
    "cms_ccn",
    "cms_provider_name",
    "cms_city",
    "cms_state",
    "cms_zip5",
    "cms_drg_cd",
    "cms_tot_dschrgs",
    "cms_avg_mdcr_pymt_amt",
    "cms_avg_submtd_cvrd_chrg",
    "commercial_to_medicare_ratio",
    # Match quality and lineage
    "cms_match_status",
    "cms_match_confidence",
    "entity_resolution_method",
    "source_file_url",
    "source_file_name",
    "source_row_index",
    "source_json_path",
    "parser_strategy",
    "template_version_raw",
    "template_family",
    "extractor_version",
    "dq_flags",
    "extracted_at",
    "cms_snapshot_hash",
]


def _ein_from_filename(filename: str) -> str | None:
    # EIN is a 9-digit prefix in most file names (including leading zeros).
    m = re.match(r"^(\d{9})", filename.strip())
    return m.group(1) if m else None


def _utc_now_iso_z() -> str:
    # Canonical output expects ISO-8601 UTC with trailing `Z`.
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _canonical_row_template() -> dict[str, Any]:
    return {k: None for k in CANONICAL_COLUMNS}


def _canonical_base_row(
    *,
    hospital: Hospital,
    file_path: Path,
    source_file_url: str | None,
    transparency_hospital_name: str | None,
    transparency_address: str | None,
    npi_type_2: str | None,
    ein: str | None,
    source_row_index: int | None,
    source_json_path: str | None,
    parser_strategy: str | None,
    template_version_raw: str | None,
    template_family: str | None,
    extractor_version: str,
    extracted_at: str,
) -> dict[str, Any]:
    row = _canonical_row_template()
    row.update(
        {
            "hospital_key": hospital.hospital_key,
            "hospital_name": hospital.hospital_name,
            "state": hospital.state,
            "ccn": normalize_ccn(hospital.ccn),
            "ein": ein,
            "npi_type_2": npi_type_2,
            "transparency_hospital_name": transparency_hospital_name,
            "transparency_address": transparency_address,
            "currency": "USD",
            "cms_match_status": "no_match",
            "source_file_url": source_file_url,
            "source_file_name": file_path.name,
            "source_row_index": source_row_index,
            "source_json_path": source_json_path,
            "parser_strategy": parser_strategy,
            "template_version_raw": template_version_raw,
            "template_family": template_family,
            "extractor_version": extractor_version,
            "dq_flags": None,
            "extracted_at": extracted_at,
        }
    )
    return row


def _emit_negotiated_row(
    base: dict[str, Any],
    *,
    procedure_code: str,
    procedure_code_type: str | None,
    procedure_description: str | None,
    match_method: str,
    payer_name: str,
    plan_name: str | None,
    rate_raw: str | None,
    negotiated_value_source: str | None,
    negotiated_amount: float | None,
    charge_methodology: str | None,
    rate_note: str | None,
    gross_charge: float | None,
    discounted_cash: float | None,
    deidentified_min: float | None,
    deidentified_max: float | None,
    implant: dict[str, Any],
    dq_flags: str | None,
) -> dict[str, Any]:
    out = base.copy()
    out.update(
        {
            "procedure_code": procedure_code,
            "procedure_code_type": procedure_code_type,
            "procedure_description": procedure_description,
            "match_method": match_method,
            "payer_name": payer_name,
            "payer_name_normalized": normalize_payer_name(payer_name),
            "plan_name": plan_name,
            "rate_type": "negotiated",
            "negotiated_amount": negotiated_amount,
            "rate_raw": rate_raw,
            "negotiated_value_source": negotiated_value_source,
            "charge_methodology": charge_methodology,
            "rate_note": rate_note,
            "gross_charge": gross_charge,
            "discounted_cash": discounted_cash,
            "deidentified_min": deidentified_min,
            "deidentified_max": deidentified_max,
            "dq_flags": dq_flags,
        }
    )
    out.update(implant)
    return out


def _negotiated_dq_flags(
    payer: PayerChargeInfo,
    pcl_line_dq: tuple[str, ...],
    template_issues: tuple[str, ...],
) -> str | None:
    ordered: list[str] = []
    seen: set[str] = set()

    def add(flag: str) -> None:
        if flag and flag not in seen:
            seen.add(flag)
            ordered.append(flag)

    for x in pcl_line_dq:
        add(x)
    for x in payer.parser_dq_flags:
        add(x)
    if payer.negotiated_amount is None and payer.rate_raw is not None:
        if payer.rate_kind == "percentage":
            add("percent_of_charges_noncomparable")
        elif payer.rate_kind == "algorithm":
            add("algorithm_only_rate")
    if payer.negotiated_value_source == "estimated_amount_fallback":
        add("negotiated_amount_inferred_from_estimated")
    if payer.negotiated_amount is not None and payer.negotiated_amount == 0:
        add("zero_negotiated_rate")
    for x in template_issues:
        add(f"template_nonconformant:{x}")
    return "|".join(ordered) if ordered else None


def _iter_procedure_charge_lines(path: Path, *, template_family: str) -> Iterator[Any]:
    suf = path.suffix.lower()
    if suf == ".csv":
        # Version-aware routing hook: v2/v3 currently share the same CSV parser core,
        # but are routed explicitly so version-specific behavior can diverge safely.
        if template_family in {"v2", "v3"}:
            yield from iter_procedure_charge_lines_from_csv(path)
            return
        yield from iter_procedure_charge_lines_from_csv(path)
    elif suf in {".json", ".jsonl"}:
        if template_family in {"v2", "v3"}:
            yield from iter_procedure_charge_lines_from_json(path)
            return
        yield from iter_procedure_charge_lines_from_json(path)
    else:
        raise ValueError(f"Unsupported file type for extract: {path}")


def iter_canonical_rows_from_file(
    hospital: Hospital,
    file_path: Path,
    *,
    source_file_url: str | None = None,
) -> Iterator[dict[str, Any]]:
    """
    Stream canonical rows from one transparency file (memory-safe for callers that iterate).
    """
    if source_file_url is None:
        source_file_url = hospital.mrf_url

    extracted_at = _utc_now_iso_z()
    ein = _ein_from_filename(file_path.name)
    template_detection = detect_template_for_file(file_path)

    parser_strategy = None
    if template_detection.template_family != "unknown":
        parser_strategy = template_detection.strategy_suffix

    for pcl in _iter_procedure_charge_lines(
        file_path, template_family=template_detection.template_family
    ):
        src_row = getattr(pcl, "source_row_index", None)
        src_json = getattr(pcl, "source_json_path", None)
        line_dq = getattr(pcl, "line_dq_flags", ())

        effective_strategy = pcl.parser_strategy
        if parser_strategy:
            effective_strategy = f"{pcl.parser_strategy}|{parser_strategy}"
        base = _canonical_base_row(
            hospital=hospital,
            file_path=file_path,
            source_file_url=source_file_url,
            transparency_hospital_name=pcl.hospital_metadata.transparency_hospital_name,
            transparency_address=pcl.hospital_metadata.transparency_address,
            npi_type_2=pcl.hospital_metadata.npi_type_2,
            ein=ein,
            source_row_index=src_row if file_path.suffix.lower() == ".csv" else None,
            source_json_path=src_json if file_path.suffix.lower() in {".json", ".jsonl"} else None,
            parser_strategy=effective_strategy,
            template_version_raw=template_detection.template_version_raw,
            template_family=template_detection.template_family,
            extractor_version=package_version,
            extracted_at=extracted_at,
        )
        implant = {
            "implant_manufacturer": pcl.implant.implant_manufacturer,
            "implant_product": pcl.implant.implant_product,
            "implant_code": pcl.implant.implant_code,
            "implant_rate": pcl.implant.implant_rate,
        }

        line_deid_min = getattr(pcl, "deidentified_min", None)
        line_deid_max = getattr(pcl, "deidentified_max", None)

        for payer in pcl.payer_charges:
            deid_min = payer.deidentified_min
            deid_max = payer.deidentified_max
            if deid_min is None:
                deid_min = line_deid_min
            if deid_max is None:
                deid_max = line_deid_max

            yield _emit_negotiated_row(
                base,
                procedure_code=pcl.procedure_code,
                procedure_code_type=pcl.procedure_code_type,
                procedure_description=pcl.procedure_description,
                match_method=pcl.match_method,
                payer_name=payer.payer_name,
                plan_name=payer.plan_name,
                rate_raw=payer.rate_raw,
                negotiated_value_source=payer.negotiated_value_source,
                negotiated_amount=payer.negotiated_amount,
                charge_methodology=payer.charge_methodology,
                rate_note=payer.rate_note,
                gross_charge=pcl.gross_charge,
                discounted_cash=pcl.cash,
                deidentified_min=deid_min,
                deidentified_max=deid_max,
                implant=implant,
                dq_flags=_negotiated_dq_flags(
                    payer,
                    line_dq,
                    (
                        template_detection.issues
                        if (
                            not template_detection.is_conformant
                            and template_detection.template_version_raw
                        )
                        else ()
                    ),
                ),
            )


@dataclass(frozen=True)
class SilverExtractResult:
    """Silver-stage extract output: in-memory rows and/or a streamed JSONL path."""

    rows: list[dict[str, Any]]
    jsonl_path: Path | None = None


def extract_canonical_rows_from_file(
    hospital: Hospital,
    file_path: Path,
    *,
    source_file_url: str | None = None,
    stream_threshold_bytes: int | None = None,
    silver_output_dir: Path | None = None,
) -> SilverExtractResult:
    """
    Extract canonical rows from a single downloaded transparency file.

    Files at or above ``stream_threshold_bytes`` are written to
    ``{silver_output_dir}/{hospital_key}/{stem}.canonical.jsonl`` without holding all rows
    in memory. Smaller files are returned in ``rows``.
    """
    threshold = (
        stream_threshold_bytes
        if stream_threshold_bytes is not None
        else extract_stream_threshold_bytes()
    )
    size = file_path.stat().st_size
    row_iter = iter_canonical_rows_from_file(hospital, file_path, source_file_url=source_file_url)

    if size >= threshold:
        root = silver_output_dir if silver_output_dir is not None else silver_dir()
        out_path = root / hospital.hospital_key / f"{file_path.stem}.canonical.jsonl"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        n_written = 0
        with out_path.open("w", encoding="utf-8") as out_f:
            for row in row_iter:
                out_f.write(json.dumps(row, sort_keys=True, default=str) + "\n")
                n_written += 1
        _cleanup_stale_silver_canonical_same_prefix(
            silver_hospital_dir=out_path.parent,
            kept=out_path,
        )
        logger.info(
            "[%s] extract: streamed %s rows to %s (source >= %s bytes)",
            hospital.hospital_key,
            n_written,
            out_path,
            threshold,
        )
        return SilverExtractResult(rows=[], jsonl_path=out_path)

    rows = list(row_iter)
    if silver_output_dir is not None and rows:
        root = silver_output_dir
        out_path = root / hospital.hospital_key / f"{file_path.stem}.canonical.jsonl"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as out_f:
            for row in rows:
                out_f.write(json.dumps(row, sort_keys=True, default=str) + "\n")
        _cleanup_stale_silver_canonical_same_prefix(
            silver_hospital_dir=out_path.parent,
            kept=out_path,
        )
        logger.info(
            "[%s] extract: wrote %s rows to %s (small file + silver_output_dir)",
            hospital.hospital_key,
            len(rows),
            out_path,
        )
        return SilverExtractResult(rows=[], jsonl_path=out_path)

    return SilverExtractResult(rows=rows, jsonl_path=None)


def extract_canonical_rows_for_hospital(
    hospital: Hospital,
    *,
    raw_root: Path,
    stream_threshold_bytes: int | None = None,
    silver_output_dir: Path | None = None,
) -> SilverExtractResult:
    """Extract from the downloaded standard-charges file under `data/raw/{hospital_key}/`."""
    hdir = raw_root / hospital.hospital_key
    if not hdir.is_dir():
        raise FileNotFoundError(f"Raw hospital dir not found: {hdir}")

    file_path = select_standard_charges_artifact(hdir)
    logger.info("[%s] extract: using %s", hospital.hospital_key, file_path.name)
    return extract_canonical_rows_from_file(
        hospital,
        file_path,
        stream_threshold_bytes=stream_threshold_bytes,
        silver_output_dir=silver_output_dir,
    )
