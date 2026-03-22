"""CLI entrypoint: discover (cms-hpt.txt), download (MRF to raw storage), future extract/join."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from hpt import __version__
from hpt.config import default_hospitals_config_path
from hpt.discovery import run_discover
from hpt.download import run_download


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hpt",
        description="Hospital price transparency pipeline (discover, download, extract, join).",
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

    return parser


def _hospital_key_set(args: argparse.Namespace) -> set[str] | None:
    if not args.hospitals:
        return None
    return set(args.hospitals)


def _cmd_discover(args: argparse.Namespace) -> int:
    keys = _hospital_key_set(args)
    cfg = args.config or default_hospitals_config_path()
    run_discover(hospital_keys=keys, config_path=cfg, dry_run=args.dry_run)
    return 0


def _cmd_download(args: argparse.Namespace) -> int:
    keys = _hospital_key_set(args)
    cfg = args.config or default_hospitals_config_path()
    results = run_download(hospital_keys=keys, config_path=cfg, force=args.force)
    failed = sum(1 for _, p in results if p is None)
    return 1 if failed else 0


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
