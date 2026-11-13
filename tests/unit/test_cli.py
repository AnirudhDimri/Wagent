"""Smoke tests for the installed public CLI."""

import subprocess
import sys

from typer.testing import CliRunner

from wagent import __version__
from wagent.cli import app


def test_version() -> None:
    result = CliRunner().invoke(app, ["--version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == f"wagent {__version__}"


def test_module_entrypoint() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "wagent", "--version"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout.strip() == f"wagent {__version__}"
