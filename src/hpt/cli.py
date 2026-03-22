"""Minimal CLI entrypoint (scaffold): version and help until pipeline subcommands exist."""

from __future__ import annotations

import argparse
import logging
import sys

from hpt import __version__


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
    return parser


def main(argv: list[str] | None = None) -> int:
    """Parse CLI arguments. Returns a process exit code."""
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
    parser.parse_args(args)
    return 0
