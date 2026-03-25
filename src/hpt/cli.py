"""CLI entrypoint: discover, download, extract, join, export, run-all."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from hpt import __version__
from hpt.config import default_hospitals_config_path
from hpt.export import run_export
from hpt.join import run_join
from hpt.pipeline import resolve_hospital_keys, run_all, run_extract_hospitals


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hpt",
        description=(
            "Hospital price transparency pipeline"
            " (discover, download, extract, join, export, run-all)."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    p_disc = sub.add_parser("discover", help="Fetch cms-hpt.txt and write manifest under data/raw/")
    p_disc.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to hospitals.yaml (default: config/hospitals.yaml or HPT_HOSPITALS_CONFIG)",
    )
    p_disc.add_argument(
        "--hospital",
        action="append",
        dest="hospitals",
        metavar="HOSPITAL_KEY",
        help="Limit to one or more hospital_key values (repeatable)",
    )
    p_disc.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve URLs and log actions without writing cms-hpt.txt or manifest.json",
    )
    p_disc.set_defaults(_handler=_cmd_discover)

    p_dl = sub.add_parser(
        "download",
        help="Download MRF from manifest into data/raw/{hospital_key}/",
    )
    p_dl.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to hospitals.yaml (default: config/hospitals.yaml or HPT_HOSPITALS_CONFIG)",
    )
    p_dl.add_argument(
        "--hospital",
        action="append",
        dest="hospitals",
        metavar="HOSPITAL_KEY",
        help="Limit to one or more hospital_key values (repeatable)",
    )
    p_dl.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if the target file already exists",
    )
    p_dl.set_defaults(_handler=_cmd_download)

    p_ext = sub.add_parser(
        "extract",
        help="Parse raw MRF into silver canonical JSONL per hospital",
    )
    p_ext.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to hospitals.yaml (default: config/hospitals.yaml or HPT_HOSPITALS_CONFIG)",
    )
    p_ext.add_argument(
        "--hospital",
        action="append",
        dest="hospitals",
        metavar="HOSPITAL_KEY",
        help="Limit to one or more hospital_key values (repeatable)",
    )
    p_ext.add_argument(
        "--tier",
        type=int,
        default=None,
        metavar="N",
        help="Only hospitals with this tier (1–3)",
    )
    p_ext.add_argument(
        "--raw-dir",
        type=Path,
        default=None,
        help="Raw root (default: data/raw or HPT_RAW_DIR)",
    )
    p_ext.add_argument(
        "--silver-dir",
        type=Path,
        default=None,
        help="Silver output root (default: data/silver or HPT_SILVER_DIR)",
    )
    p_ext.add_argument(
        "--processed-dir",
        type=Path,
        default=None,
        help="Checkpoint root (default: data/processed or HPT_PROCESSED_DIR)",
    )
    p_ext.add_argument(
        "--force",
        action="store_true",
        help="Re-run extract even if checkpoint matches manifest sha256",
    )
    p_ext.set_defaults(_handler=_cmd_extract)

    p_join = sub.add_parser(
        "join",
        help="Join silver canonical rows to CMS knee benchmarks",
    )
    p_join.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to hospitals.yaml (default: config/hospitals.yaml or HPT_HOSPITALS_CONFIG)",
    )
    p_join.add_argument(
        "--hospital",
        action="append",
        dest="hospitals",
        metavar="HOSPITAL_KEY",
        help="Limit to one or more hospital_key values (repeatable)",
    )
    p_join.add_argument(
        "--tier",
        type=int,
        default=None,
        metavar="N",
        help="Only hospitals with this tier (1–3)",
    )
    p_join.add_argument(
        "--cms-path",
        type=Path,
        default=None,
        help=(
            "Path to CMS knee replacement CSV"
            " (default: data/cms_knee_replacement_by_provider.csv"
            " or HPT_CMS_KNEE_CSV_PATH)"
        ),
    )
    p_join.add_argument(
        "--silver-dir",
        type=Path,
        default=None,
        help="Silver input root (default: data/silver or HPT_SILVER_DIR)",
    )
    p_join.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output root for joined JSONL files (default: data/processed or HPT_PROCESSED_DIR)",
    )
    p_join.set_defaults(_handler=_cmd_join)

    p_export = sub.add_parser(
        "export",
        help="Write combined.csv, qa_summary.json, and export_metadata.json from joined JSONL",
    )
    p_export.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to hospitals.yaml (validates --hospital keys if set)",
    )
    p_export.add_argument(
        "--hospital",
        action="append",
        dest="hospitals",
        metavar="HOSPITAL_KEY",
        help="Limit export to these hospitals' joined files (repeatable)",
    )
    p_export.add_argument(
        "--tier",
        type=int,
        default=None,
        metavar="N",
        help="Only include hospitals with this tier (1–3)",
    )
    p_export.add_argument(
        "--joined-root",
        type=Path,
        default=None,
        help=(
            "Directory containing joined/{hospital_key}/"
            "*.joined.jsonl (default: {processed}/joined)"
        ),
    )
    p_export.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for combined + QA (default: data/processed or HPT_PROCESSED_DIR)",
    )
    p_export.add_argument(
        "--jsonl",
        action="store_true",
        help="Also write combined.jsonl alongside combined.csv",
    )
    p_export.set_defaults(_handler=_cmd_export)

    p_ra = sub.add_parser(
        "run-all",
        help="discover → download → extract → join → export (per-hospital isolation for extract)",
    )
    p_ra.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to hospitals.yaml (default: config/hospitals.yaml or HPT_HOSPITALS_CONFIG)",
    )
    p_ra.add_argument(
        "--hospital",
        action="append",
        dest="hospitals",
        metavar="HOSPITAL_KEY",
        help="Limit to one or more hospital_key values (repeatable)",
    )
    p_ra.add_argument(
        "--tier",
        type=int,
        default=None,
        metavar="N",
        help="Only hospitals with this tier (1–3)",
    )
    p_ra.add_argument(
        "--raw-dir",
        type=Path,
        default=None,
        help="Raw root (default: data/raw)",
    )
    p_ra.add_argument(
        "--silver-dir",
        type=Path,
        default=None,
        help="Silver root (default: data/silver)",
    )
    p_ra.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Processed root: joined/, checkpoints, combined.csv (default: data/processed)",
    )
    p_ra.add_argument(
        "--cms-path",
        type=Path,
        default=None,
        help="CMS knee CSV path",
    )
    p_ra.add_argument(
        "--skip-discover",
        action="store_true",
    )
    p_ra.add_argument(
        "--skip-download",
        action="store_true",
    )
    p_ra.add_argument(
        "--skip-extract",
        action="store_true",
    )
    p_ra.add_argument(
        "--skip-join",
        action="store_true",
    )
    p_ra.add_argument(
        "--skip-export",
        action="store_true",
    )
    p_ra.add_argument(
        "--force-extract",
        action="store_true",
        help="Ignore extract checkpoints",
    )
    p_ra.add_argument(
        "--export-jsonl",
        action="store_true",
    )
    p_ra.set_defaults(_handler=_cmd_run_all)

    return parser


def _hospital_key_set(args: argparse.Namespace) -> set[str] | None:
    if not args.hospitals:
        return None
    return set(args.hospitals)


def _cmd_discover(args: argparse.Namespace) -> int:
    from hpt.discovery import run_discover

    keys = _hospital_key_set(args)
    cfg = args.config or default_hospitals_config_path()
    run_discover(hospital_keys=keys, config_path=cfg, dry_run=args.dry_run)
    return 0


def _cmd_download(args: argparse.Namespace) -> int:
    from hpt.download import run_download

    keys = _hospital_key_set(args)
    cfg = args.config or default_hospitals_config_path()
    results = run_download(hospital_keys=keys, config_path=cfg, force=args.force)
    failed = sum(1 for _, p in results if p is None)
    return 1 if failed else 0


def _cmd_extract(args: argparse.Namespace) -> int:
    keys = _hospital_key_set(args)
    cfg = args.config or default_hospitals_config_path()
    results = run_extract_hospitals(
        hospital_keys=keys,
        tier=args.tier,
        config_path=cfg,
        raw_root=args.raw_dir,
        silver_root=args.silver_dir,
        processed_root=args.processed_dir,
        force=args.force,
    )
    failed = sum(1 for r in results if r.error)
    return 1 if failed else 0


def _cmd_join(args: argparse.Namespace) -> int:
    keys = _hospital_key_set(args)
    cfg = args.config or default_hospitals_config_path()
    rk = resolve_hospital_keys(config_path=cfg, hospital_keys=keys, tier=args.tier)
    results = run_join(
        hospital_keys=rk,
        config_path=cfg,
        cms_path=args.cms_path,
        silver_root=args.silver_dir,
        output_root=args.output_dir,
    )
    failed = sum(1 for _, rows in results if not rows)
    return 1 if failed else 0


def _cmd_export(args: argparse.Namespace) -> int:
    log = logging.getLogger("hpt.cli")
    keys = _hospital_key_set(args)
    cfg = args.config or default_hospitals_config_path()
    rk = resolve_hospital_keys(config_path=cfg, hospital_keys=keys, tier=args.tier)
    proc = args.output_dir
    joined_root = args.joined_root
    if joined_root is None and proc is not None:
        joined_root = proc / "joined"
    try:
        run_export(
            joined_root=joined_root,
            output_dir=proc,
            hospital_keys=rk,
            config_path=cfg,
            write_jsonl=args.jsonl,
        )
    except FileNotFoundError as e:
        log.error("%s", e)
        return 1
    return 0


def _cmd_run_all(args: argparse.Namespace) -> int:
    keys = _hospital_key_set(args)
    cfg = args.config or default_hospitals_config_path()
    summary = run_all(
        hospital_keys=keys,
        tier=args.tier,
        config_path=cfg,
        raw_root=args.raw_dir,
        silver_root=args.silver_dir,
        processed_root=args.output_dir,
        cms_path=args.cms_path,
        skip_discover=args.skip_discover,
        skip_download=args.skip_download,
        skip_extract=args.skip_extract,
        skip_join=args.skip_join,
        skip_export=args.skip_export,
        force_extract=args.force_extract,
        export_jsonl=args.export_jsonl,
    )
    if summary.export_error:
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    """Parse CLI arguments. Returns a process exit code."""
    log = logging.getLogger("hpt.cli")
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    parser = _build_parser()
    args = argv if argv is not None else sys.argv[1:]
    if not args:
        parser.print_help()
        return 0
    ns = parser.parse_args(args)
    if getattr(ns, "command", None) is None:
        parser.print_help()
        return 0
    handler = getattr(ns, "_handler", None)
    if handler is None:
        parser.print_help()
        return 0
    try:
        return int(handler(ns))
    except ValueError as e:
        log.error("%s", e)
        return 1
