"""Gold-prep join stage: deterministic CCN join to CMS knee replacement benchmarks."""

from __future__ import annotations

import csv
import hashlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hpt.config import (
    cms_knee_replacement_csv_path,
    default_hospitals_config_path,
    filter_hospitals,
    load_hospitals,
    processed_dir,
    silver_dir,
)
from hpt.constants import DRG_MAJOR_JOINT_WITH_MCC, DRG_MAJOR_JOINT_WITHOUT_MCC, HCPCS_TKA
from hpt.normalize import normalize_ccn

logger = logging.getLogger(__name__)

_CMS_FIELD_CCN = "Rndrng_Prvdr_CCN"
_CMS_FIELD_PROVIDER_NAME = "Rndrng_Prvdr_Org_Name"
_CMS_FIELD_CITY = "Rndrng_Prvdr_City"
_CMS_FIELD_STATE = "Rndrng_Prvdr_State_Abrvtn"
_CMS_FIELD_ZIP5 = "Rndrng_Prvdr_Zip5"
_CMS_FIELD_DRG = "DRG_Cd"
_CMS_FIELD_TOTAL_DISCHARGES = "Tot_Dschrgs"
_CMS_FIELD_AVG_SUBMITTED = "Avg_Submtd_Cvrd_Chrg"
_CMS_FIELD_AVG_MEDICARE = "Avg_Mdcr_Pymt_Amt"

_CMS_DRG_SCOPE = frozenset({DRG_MAJOR_JOINT_WITH_MCC, DRG_MAJOR_JOINT_WITHOUT_MCC})

DQ_JOIN_NO_CMS_MATCH = "join_no_cms_match"
DQ_RATIO_NONCOMPARABLE_RATE_TYPE = "ratio_noncomparable_rate_type"
DQ_RATIO_MISSING_NEGOTIATED = "ratio_missing_negotiated_amount"
DQ_RATIO_MISSING_CMS_BENCHMARK = "ratio_missing_cms_benchmark"


@dataclass(frozen=True)
class CmsBenchmark:
    ccn: str
    provider_name: str | None
    city: str | None
    state: str | None
    zip5: str | None
    drg_cd: str
    tot_dschrgs: float | None
    avg_mdcr_pymt_amt: float | None
    avg_submtd_cvrd_chrg: float | None


@dataclass(frozen=True)
class _CmsProviderAggregate:
    by_drg: dict[str, CmsBenchmark]
    combined: CmsBenchmark


@dataclass(frozen=True)
class JoinFileResult:
    input_path: Path
    output_path: Path
    row_count: int
    dataset_dq_flags: list[str]
    cms_snapshot_hash: str


def _sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


def _normalize_zip5(value: str | None) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.isdigit():
        return s.zfill(5)
    return s


def _weighted_or_mean(items: list[tuple[float | None, float]]) -> float | None:
    present = [(value, weight) for value, weight in items if value is not None]
    if not present:
        return None
    weighted_items = [(value, weight) for value, weight in present if weight > 0]
    if weighted_items:
        total_weight = sum(weight for _, weight in weighted_items)
        return sum(value * weight for value, weight in weighted_items) / total_weight
    return sum(value for value, _ in present) / len(present)


def _first_non_empty(values: list[str | None]) -> str | None:
    for value in values:
        if value is None:
            continue
        s = value.strip()
        if s:
            return s
    return None


def _build_provider_aggregate(ccn: str, rows: list[dict[str, str]]) -> _CmsProviderAggregate:
    by_drg: dict[str, CmsBenchmark] = {}
    for raw in rows:
        drg = str(raw.get(_CMS_FIELD_DRG, "")).strip()
        if drg not in _CMS_DRG_SCOPE:
            continue
        by_drg[drg] = CmsBenchmark(
            ccn=ccn,
            provider_name=_first_non_empty([raw.get(_CMS_FIELD_PROVIDER_NAME)]),
            city=_first_non_empty([raw.get(_CMS_FIELD_CITY)]),
            state=_first_non_empty([raw.get(_CMS_FIELD_STATE)]),
            zip5=_normalize_zip5(raw.get(_CMS_FIELD_ZIP5)),
            drg_cd=drg,
            tot_dschrgs=_parse_float(raw.get(_CMS_FIELD_TOTAL_DISCHARGES)),
            avg_mdcr_pymt_amt=_parse_float(raw.get(_CMS_FIELD_AVG_MEDICARE)),
            avg_submtd_cvrd_chrg=_parse_float(raw.get(_CMS_FIELD_AVG_SUBMITTED)),
        )
    if not by_drg:
        raise ValueError(f"CMS file has no DRG 469/470 rows for CCN {ccn}")

    benchmarks = [by_drg[k] for k in sorted(by_drg)]
    discharges = [bench.tot_dschrgs or 0.0 for bench in benchmarks]
    drg_cd = "|".join(sorted(by_drg))
    combined = CmsBenchmark(
        ccn=ccn,
        provider_name=_first_non_empty([b.provider_name for b in benchmarks]),
        city=_first_non_empty([b.city for b in benchmarks]),
        state=_first_non_empty([b.state for b in benchmarks]),
        zip5=_first_non_empty([b.zip5 for b in benchmarks]),
        drg_cd=drg_cd,
        tot_dschrgs=sum(discharges) if any(discharges) else None,
        avg_mdcr_pymt_amt=_weighted_or_mean(
            [(bench.avg_mdcr_pymt_amt, discharge) for bench, discharge in zip(benchmarks, discharges)]
        ),
        avg_submtd_cvrd_chrg=_weighted_or_mean(
            [(bench.avg_submtd_cvrd_chrg, discharge) for bench, discharge in zip(benchmarks, discharges)]
        ),
    )
    return _CmsProviderAggregate(by_drg=by_drg, combined=combined)


def load_cms_benchmarks_by_ccn(cms_csv_path: Path) -> tuple[dict[str, _CmsProviderAggregate], str]:
    """Load DRG 469/470 CMS benchmarks, keyed by normalized CCN."""
    if not cms_csv_path.is_file():
        raise FileNotFoundError(f"CMS knee replacement file not found: {cms_csv_path}")
    out: dict[str, list[dict[str, str]]] = {}
    with cms_csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for raw in reader:
            ccn = normalize_ccn(raw.get(_CMS_FIELD_CCN))
            if not ccn:
                continue
            drg = str(raw.get(_CMS_FIELD_DRG, "")).strip()
            if drg not in _CMS_DRG_SCOPE:
                continue
            out.setdefault(ccn, []).append(raw)

    benchmarks = {
        ccn: _build_provider_aggregate(ccn, rows)
        for ccn, rows in out.items()
    }
    return benchmarks, _sha256_file(cms_csv_path)


def _row_has_dq_flag(row: dict[str, Any], flag: str) -> bool:
    raw = row.get("dq_flags")
    if raw is None:
        return False
    flags = [part.strip() for part in str(raw).split("|")]
    return flag in flags


def _methodology_noncomparable(methodology: str | None) -> bool:
    if methodology is None:
        return False
    lowered = methodology.lower()
    return "percent" in lowered or "% of" in lowered


def _select_benchmark_for_row(
    row: dict[str, Any], provider: _CmsProviderAggregate
) -> CmsBenchmark:
    code = str(row.get("procedure_code") or "").strip()
    if code in _CMS_DRG_SCOPE and code in provider.by_drg:
        return provider.by_drg[code]
    if code == HCPCS_TKA:
        return provider.combined
    return provider.combined


def _ratio_noncomparability_reasons(
    *, row: dict[str, Any], benchmark: CmsBenchmark
) -> set[str]:
    reasons: set[str] = set()
    if _row_has_dq_flag(row, "percent_of_charges_noncomparable") or _methodology_noncomparable(
        row.get("charge_methodology")
    ):
        reasons.add(DQ_RATIO_NONCOMPARABLE_RATE_TYPE)
    negotiated = _parse_float(row.get("negotiated_amount"))
    if negotiated is None:
        reasons.add(DQ_RATIO_MISSING_NEGOTIATED)
    if benchmark.avg_mdcr_pymt_amt is None or benchmark.avg_mdcr_pymt_amt <= 0:
        reasons.add(DQ_RATIO_MISSING_CMS_BENCHMARK)
    return reasons


def join_row_to_cms(
    *,
    row: dict[str, Any],
    cms_by_ccn: dict[str, _CmsProviderAggregate],
    cms_snapshot_hash: str,
    dataset_dq_flags: set[str],
) -> dict[str, Any]:
    """Attach CMS fields + match stamps and compute ratio when comparable."""
    out = dict(row)
    out["cms_snapshot_hash"] = cms_snapshot_hash
    out["commercial_to_medicare_ratio"] = None

    ccn = normalize_ccn(out.get("ccn"))
    provider = cms_by_ccn.get(ccn or "")
    if not ccn or provider is None:
        out["cms_match_status"] = "no_match"
        out["cms_match_confidence"] = None
        out["entity_resolution_method"] = None
        out["cms_ccn"] = None
        out["cms_provider_name"] = None
        out["cms_city"] = None
        out["cms_state"] = None
        out["cms_zip5"] = None
        out["cms_drg_cd"] = None
        out["cms_tot_dschrgs"] = None
        out["cms_avg_mdcr_pymt_amt"] = None
        out["cms_avg_submtd_cvrd_chrg"] = None
        dataset_dq_flags.add(DQ_JOIN_NO_CMS_MATCH)
        return out

    benchmark = _select_benchmark_for_row(out, provider)
    out["cms_match_status"] = "matched_ccn_roster"
    out["cms_match_confidence"] = "high"
    out["entity_resolution_method"] = "config_ccn"
    out["cms_ccn"] = benchmark.ccn
    out["cms_provider_name"] = benchmark.provider_name
    out["cms_city"] = benchmark.city
    out["cms_state"] = benchmark.state
    out["cms_zip5"] = benchmark.zip5
    out["cms_drg_cd"] = benchmark.drg_cd
    out["cms_tot_dschrgs"] = benchmark.tot_dschrgs
    out["cms_avg_mdcr_pymt_amt"] = benchmark.avg_mdcr_pymt_amt
    out["cms_avg_submtd_cvrd_chrg"] = benchmark.avg_submtd_cvrd_chrg

    reasons = _ratio_noncomparability_reasons(row=out, benchmark=benchmark)
    if reasons:
        dataset_dq_flags.update(reasons)
        return out

    negotiated = _parse_float(out.get("negotiated_amount"))
    if negotiated is not None and benchmark.avg_mdcr_pymt_amt:
        out["commercial_to_medicare_ratio"] = negotiated / benchmark.avg_mdcr_pymt_amt
    return out


def join_canonical_jsonl_file(
    *,
    input_path: Path,
    output_path: Path,
    cms_by_ccn: dict[str, _CmsProviderAggregate],
    cms_snapshot_hash: str,
) -> JoinFileResult:
    """Join one canonical JSONL file and write joined JSONL output."""
    dataset_flags: set[str] = set()
    row_count = 0
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with (
        input_path.open("r", encoding="utf-8") as in_f,
        output_path.open("w", encoding="utf-8") as out_f,
    ):
        for line in in_f:
            stripped = line.strip()
            if not stripped:
                continue
            raw = json.loads(stripped)
            if not isinstance(raw, dict):
                continue
            joined = join_row_to_cms(
                row=raw,
                cms_by_ccn=cms_by_ccn,
                cms_snapshot_hash=cms_snapshot_hash,
                dataset_dq_flags=dataset_flags,
            )
            out_f.write(json.dumps(joined, sort_keys=True, default=str) + "\n")
            row_count += 1
    return JoinFileResult(
        input_path=input_path,
        output_path=output_path,
        row_count=row_count,
        dataset_dq_flags=sorted(dataset_flags),
        cms_snapshot_hash=cms_snapshot_hash,
    )


def _iter_canonical_jsonl_files(hospital_silver_dir: Path) -> list[Path]:
    return sorted(
        hospital_silver_dir.glob("*.canonical.jsonl"),
        key=lambda p: p.name,
    )


def run_join(
    *,
    hospital_keys: set[str] | None = None,
    config_path: Path | None = None,
    cms_path: Path | None = None,
    silver_root: Path | None = None,
    output_root: Path | None = None,
) -> list[tuple[str, list[JoinFileResult]]]:
    """
    Join all canonical silver JSONL files for selected hospitals.

    Outputs are written under ``{output_root}/joined/{hospital_key}/*.joined.jsonl``.
    """
    cfg = config_path or default_hospitals_config_path()
    hospitals = filter_hospitals(load_hospitals(cfg), hospital_keys)
    cms_csv = cms_path or cms_knee_replacement_csv_path()
    cms_by_ccn, cms_snapshot_hash = load_cms_benchmarks_by_ccn(cms_csv)

    silver = silver_root or silver_dir()
    out_root = output_root or processed_dir()
    results: list[tuple[str, list[JoinFileResult]]] = []
    for hospital in hospitals:
        hospital_in = silver / hospital.hospital_key
        canonical_files = _iter_canonical_jsonl_files(hospital_in)
        joined_results: list[JoinFileResult] = []
        if not canonical_files:
            logger.warning(
                "[%s] join: no canonical silver files found under %s",
                hospital.hospital_key,
                hospital_in,
            )
            results.append((hospital.hospital_key, joined_results))
            continue
        for canonical_file in canonical_files:
            out_file = (
                out_root
                / "joined"
                / hospital.hospital_key
                / canonical_file.name.replace(".canonical.jsonl", ".joined.jsonl")
            )
            file_result = join_canonical_jsonl_file(
                input_path=canonical_file,
                output_path=out_file,
                cms_by_ccn=cms_by_ccn,
                cms_snapshot_hash=cms_snapshot_hash,
            )
            logger.info(
                "[%s] join: %s rows -> %s (dataset_dq_flags=%s)",
                hospital.hospital_key,
                file_result.row_count,
                out_file,
                ",".join(file_result.dataset_dq_flags) if file_result.dataset_dq_flags else "none",
            )
            joined_results.append(file_result)
        results.append((hospital.hospital_key, joined_results))
    return results

