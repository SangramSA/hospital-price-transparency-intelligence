"""Gold export: concatenate joined JSONL into deterministic combined outputs + QA + metadata."""

from __future__ import annotations

import csv
import json
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from hpt import __version__ as pipeline_version
from hpt.config import (
    default_hospitals_config_path,
    filter_hospitals,
    load_hospitals,
    processed_dir,
)
from hpt.constants import (
    COMBINED_CSV_NAME,
    COMBINED_JSONL_NAME,
    EXPORT_METADATA_JSON_NAME,
    OUTPUT_SCHEMA_VERSION,
    QA_SUMMARY_JSON_NAME,
)
from hpt.extract import CANONICAL_COLUMNS

logger = logging.getLogger(__name__)


def _utc_now_iso_z() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _iter_joined_jsonl_paths(*, joined_root: Path, hospital_keys: set[str] | None) -> list[Path]:
    """All ``*.joined.jsonl`` under ``joined_root/{hospital_key}/``, sorted for stability."""
    if not joined_root.is_dir():
        return []
    paths: list[Path] = []
    for hospital_dir in sorted(joined_root.iterdir(), key=lambda p: p.name):
        if not hospital_dir.is_dir():
            continue
        key = hospital_dir.name
        if hospital_keys is not None and key not in hospital_keys:
            continue
        for p in sorted(hospital_dir.glob("*.joined.jsonl"), key=lambda x: x.name):
            paths.append(p)
    return paths


def _row_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    """Deterministic sort: hospital, payer, procedure, rate type, then stable source ids."""
    src_idx = row.get("source_row_index")
    try:
        src_idx_t = int(src_idx) if src_idx is not None else -1
    except (TypeError, ValueError):
        src_idx_t = -1
    return (
        str(row.get("hospital_key") or ""),
        str(row.get("payer_name_normalized") or ""),
        str(row.get("procedure_code") or ""),
        str(row.get("rate_type") or ""),
        str(row.get("source_file_name") or ""),
        src_idx_t,
        str(row.get("source_json_path") or ""),
        str(row.get("plan_name") or ""),
        str(row.get("payer_name") or ""),
    )


def _load_rows_from_jsonl(paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if isinstance(obj, dict):
                    rows.append(obj)
    return rows


def _normalize_row_for_csv(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for col in CANONICAL_COLUMNS:
        out[col] = row.get(col)
    return out


def _count_parser_strategy(rows: list[dict[str, Any]]) -> dict[str, int]:
    c: Counter[str] = Counter()
    for r in rows:
        ps = r.get("parser_strategy")
        if ps is None or str(ps).strip() == "":
            c["(null)"] += 1
        else:
            c[str(ps)] += 1
    return dict(sorted(c.items(), key=lambda x: (-x[1], x[0])))


def _count_dq_flag_tokens(rows: list[dict[str, Any]]) -> dict[str, int]:
    c: Counter[str] = Counter()
    for r in rows:
        raw = r.get("dq_flags")
        if raw is None or str(raw).strip() == "":
            c["(none)"] += 1
            continue
        parts = [p.strip() for p in str(raw).split("|") if p.strip()]
        if not parts:
            c["(empty)"] += 1
        else:
            for p in parts:
                c[p] += 1
    return dict(sorted(c.items(), key=lambda x: (-x[1], x[0])))


def _null_rate(rows: list[dict[str, Any]], key: str) -> float:
    if not rows:
        return 0.0
    n = sum(1 for r in rows if r.get(key) is None)
    return round(n / len(rows), 6)


def _cms_match_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    c: Counter[str] = Counter()
    for r in rows:
        status = r.get("cms_match_status")
        c[str(status) if status is not None else "(null)"] += 1
    return dict(sorted(c.items(), key=lambda x: x[0]))


def _rows_by_hospital(rows: list[dict[str, Any]]) -> dict[str, int]:
    c: Counter[str] = Counter()
    for r in rows:
        hk = r.get("hospital_key")
        c[str(hk) if hk is not None else "(null)"] += 1
    return dict(sorted(c.items(), key=lambda x: x[0]))


def _cms_snapshot_hash(rows: list[dict[str, Any]]) -> str | None:
    for r in rows:
        h = r.get("cms_snapshot_hash")
        if h is not None and str(h).strip():
            return str(h).strip()
    return None


@dataclass(frozen=True)
class ExportResult:
    combined_csv: Path
    combined_jsonl: Path | None
    qa_summary: Path
    export_metadata: Path
    row_count: int


def run_export(
    *,
    joined_root: Path | None = None,
    output_dir: Path | None = None,
    hospital_keys: set[str] | None = None,
    config_path: Path | None = None,
    write_jsonl: bool = False,
) -> ExportResult:
    """
    Read per-hospital ``*.joined.jsonl``, sort deterministically, write ``combined.csv``.

    Also writes ``qa_summary.json`` and ``export_metadata.json`` under ``output_dir``.
    """
    cfg = config_path or default_hospitals_config_path()
    if hospital_keys is not None:
        filter_hospitals(load_hospitals(cfg), hospital_keys)  # validate keys

    out_dir = output_dir or processed_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    root = joined_root if joined_root is not None else out_dir / "joined"

    paths = _iter_joined_jsonl_paths(joined_root=root, hospital_keys=hospital_keys)
    if not paths:
        msg = f"export: no *.joined.jsonl found under {root}"
        logger.error("%s", msg)
        raise FileNotFoundError(msg)

    rows = _load_rows_from_jsonl(paths)
    rows.sort(key=_row_sort_key)
    generated_at = _utc_now_iso_z()
    cms_hash = _cms_snapshot_hash(rows)

    csv_path = out_dir / COMBINED_CSV_NAME
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CANONICAL_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(_normalize_row_for_csv(row))

    jsonl_path: Path | None = None
    if write_jsonl:
        jsonl_path = out_dir / COMBINED_JSONL_NAME
        with jsonl_path.open("w", encoding="utf-8") as jf:
            for row in rows:
                jf.write(json.dumps(row, sort_keys=True, default=str) + "\n")

    qa: dict[str, Any] = {
        "generated_at": generated_at,
        "rows_total": len(rows),
        "rows_by_hospital": _rows_by_hospital(rows),
        "cms_match_status_counts": _cms_match_counts(rows),
        "null_rates": {
            "negotiated_amount": _null_rate(rows, "negotiated_amount"),
            "cms_avg_mdcr_pymt_amt": _null_rate(rows, "cms_avg_mdcr_pymt_amt"),
            "commercial_to_medicare_ratio": _null_rate(rows, "commercial_to_medicare_ratio"),
        },
        "parser_strategy_distribution": _count_parser_strategy(rows),
        "dq_flag_token_counts": _count_dq_flag_tokens(rows),
        "source_joined_files": [str(p) for p in paths],
    }
    qa_path = out_dir / QA_SUMMARY_JSON_NAME
    qa_path.write_text(json.dumps(qa, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    meta = {
        "generated_at": generated_at,
        "pipeline_version": pipeline_version,
        "output_schema_version": OUTPUT_SCHEMA_VERSION,
        "cms_snapshot_hash": cms_hash,
        "row_count": len(rows),
        "combined_csv": COMBINED_CSV_NAME,
        "optional_combined_jsonl": COMBINED_JSONL_NAME if write_jsonl else None,
    }
    meta_path = out_dir / EXPORT_METADATA_JSON_NAME
    meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    logger.info(
        "export: wrote %s rows -> %s, %s, %s",
        len(rows),
        csv_path,
        qa_path.name,
        meta_path.name,
    )
    return ExportResult(
        combined_csv=csv_path,
        combined_jsonl=jsonl_path,
        qa_summary=qa_path,
        export_metadata=meta_path,
        row_count=len(rows),
    )
