"""Command-line entry point."""

from typing import Annotated

import typer

from wagent import __version__

app = typer.Typer(no_args_is_help=True)


def _version(value: bool) -> None:
    if value:
        typer.echo(f"wagent {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option("--version", callback=_version, is_eager=True, help="Show the version."),
    ] = False,
) -> None:
    """Build reliable WhatsApp agents. Only --version is available in M0."""
