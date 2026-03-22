"""Smoke tests for the scaffold CLI."""

from __future__ import annotations

import subprocess
import sys


def test_cli_help_exits_zero() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "hpt"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "Hospital price transparency" in result.stdout


def test_cli_version_exits_zero() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "hpt", "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    out = result.stdout + result.stderr
    assert "0.1.0" in out
