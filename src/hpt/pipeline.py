"""Orchestration: batch extract and end-to-end run-all with per-hospital isolation."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from hpt.checkpoint import (
    extract_checkpoint_path,
    load_extract_checkpoint,
    read_active_download_sha256,
    should_skip_extract,
    write_extract_checkpoint,
)
from hpt.config import (
    cms_knee_replacement_csv_path,
    default_hospitals_config_path,
    filter_hospitals,
    load_hospitals,
    processed_dir,
    raw_dir,
    silver_dir,
)
from hpt.export import run_export
from hpt.extract import extract_canonical_rows_for_hospital
from hpt.join import run_join
from hpt.models import Hospital

logger = logging.getLogger(__name__)


def filter_hospitals_by_tier(hospitals: list[Hospital], tier: int | None) -> list[Hospital]:
    if tier is None:
        return hospitals
    return [h for h in hospitals if h.tier == tier]


def resolve_hospital_keys(
    *,
    config_path: Path | None = None,
    hospital_keys: set[str] | None = None,
    tier: int | None = None,
) -> set[str] | None:
    """
    When both ``tier`` and ``hospital_keys`` are None, return None (all hospitals).

    Otherwise return the intersection of explicit keys (if any) and tier filter (if any).
    """
    if tier is None and hospital_keys is None:
        return None
    cfg = config_path or default_hospitals_config_path()
    hs = load_hospitals(cfg)
    hs = filter_hospitals(hs, hospital_keys)
    hs = filter_hospitals_by_tier(hs, tier)
    return {h.hospital_key for h in hs}


@dataclass
class ExtractHospitalResult:
    hospital_key: str
    silver_jsonl: Path | None
    skipped_checkpoint: bool
    error: str | None = None


def run_extract_hospitals(
    *,
    hospital_keys: set[str] | None = None,
    tier: int | None = None,
    config_path: Path | None = None,
    raw_root: Path | None = None,
    silver_root: Path | None = None,
    processed_root: Path | None = None,
    force: bool = False,
) -> list[ExtractHospitalResult]:
    """
    Extract standard charges for each hospital into silver JSONL (per-hospital isolation).

    On success, writes extract checkpoint under ``{processed_root}/checkpoints/`` when
    manifest ``content_sha256`` is available.
    """
    cfg = config_path or default_hospitals_config_path()
    rk = resolve_hospital_keys(config_path=cfg, hospital_keys=hospital_keys, tier=tier)
    hospitals = load_hospitals(cfg)
    hospitals = filter_hospitals(hospitals, rk)
    raw = raw_root or raw_dir()
    silver = silver_root or silver_dir()
    proc = processed_root or processed_dir()

    results: list[ExtractHospitalResult] = []
    for hospital in hospitals:
        key = hospital.hospital_key
        try:
            if should_skip_extract(
                raw_root=raw,
                silver_root=silver,
                processed_root=proc,
                hospital_key=key,
                force=force,
            ):
                cp = extract_checkpoint_path(proc, key)
                prev = load_extract_checkpoint(cp)
                rel = prev.get("silver_jsonl_relpath") if prev else None
                sj = (silver / rel) if isinstance(rel, str) else None
                if sj is not None and not sj.is_file():
                    sj = None
                results.append(
                    ExtractHospitalResult(
                        hospital_key=key,
                        silver_jsonl=sj,
                        skipped_checkpoint=True,
                    )
                )
                continue

            result = extract_canonical_rows_for_hospital(
                hospital,
                raw_root=raw,
                silver_output_dir=silver,
            )
            sha = read_active_download_sha256(raw_root=raw, hospital_key=key)
            out_path = result.jsonl_path
            if out_path is None and result.rows:
                logger.warning(
                    "[%s] extract: no jsonl path despite rows; re-run with silver_output_dir", key
                )
                results.append(
                    ExtractHospitalResult(
                        hospital_key=key,
                        silver_jsonl=None,
                        skipped_checkpoint=False,
                        error="extract produced in-memory rows without JSONL path",
                    )
                )
                continue
            if out_path is not None:
                try:
                    rel = out_path.relative_to(silver)
                except ValueError:
                    rel = Path(key) / out_path.name
                write_extract_checkpoint(
                    processed_root=proc,
                    hospital_key=key,
                    source_content_sha256=sha,
                    silver_jsonl_relpath=rel.as_posix(),
                )
            results.append(
                ExtractHospitalResult(
                    hospital_key=key,
                    silver_jsonl=out_path,
                    skipped_checkpoint=False,
                )
            )
        except OSError as e:
            logger.error("[%s] extract: I/O error: %s", key, e)
            results.append(
                ExtractHospitalResult(
                    hospital_key=key,
                    silver_jsonl=None,
                    skipped_checkpoint=False,
                    error=str(e),
                )
            )
        except ValueError as e:
            logger.error("[%s] extract: %s", key, e)
            results.append(
                ExtractHospitalResult(
                    hospital_key=key,
                    silver_jsonl=None,
                    skipped_checkpoint=False,
                    error=str(e),
                )
            )
    return results


@dataclass
class RunAllSummary:
    discover_ran: bool = False
    download_results: list[tuple[str, Path | None]] = field(default_factory=list)
    extract_results: list[ExtractHospitalResult] = field(default_factory=list)
    join_failed_hospitals: list[str] = field(default_factory=list)
    export_ok: bool = False
    export_error: str | None = None


def run_all(
    *,
    hospital_keys: set[str] | None = None,
    tier: int | None = None,
    config_path: Path | None = None,
    raw_root: Path | None = None,
    silver_root: Path | None = None,
    processed_root: Path | None = None,
    cms_path: Path | None = None,
    skip_discover: bool = False,
    skip_download: bool = False,
    skip_extract: bool = False,
    skip_join: bool = False,
    skip_export: bool = False,
    force_extract: bool = False,
    export_jsonl: bool = False,
) -> RunAllSummary:
    """
    discover → download → extract → join → export.

    Per-hospital failures in extract are recorded; join/export still run for others.
    """
    summary = RunAllSummary()
    cfg = config_path or default_hospitals_config_path()
    proc = processed_root or processed_dir()
    rk = resolve_hospital_keys(config_path=cfg, hospital_keys=hospital_keys, tier=tier)

    if not skip_discover:
        from hpt.discovery import run_discover

        run_discover(hospital_keys=rk, config_path=cfg, dry_run=False)
        summary.discover_ran = True

    if not skip_download:
        from hpt.download import run_download

        summary.download_results = run_download(
            hospital_keys=rk,
            config_path=cfg,
            force=False,
        )

    if not skip_extract:
        summary.extract_results = run_extract_hospitals(
            hospital_keys=hospital_keys,
            tier=tier,
            config_path=cfg,
            raw_root=raw_root,
            silver_root=silver_root,
            processed_root=processed_root,
            force=force_extract,
        )

    if not skip_join:
        join_out = run_join(
            hospital_keys=rk,
            config_path=cfg,
            cms_path=cms_path or cms_knee_replacement_csv_path(),
            silver_root=silver_root,
            output_root=processed_root,
        )
        summary.join_failed_hospitals = [k for k, rows in join_out if not rows]

    if not skip_export:
        try:
            run_export(
                joined_root=proc / "joined",
                output_dir=proc,
                hospital_keys=rk,
                config_path=cfg,
                write_jsonl=export_jsonl,
            )
            summary.export_ok = True
        except FileNotFoundError as e:
            summary.export_error = str(e)
            logger.error("run-all: export failed: %s", e)

    return summary
