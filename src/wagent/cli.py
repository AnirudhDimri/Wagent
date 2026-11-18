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
    """Manage wagent versions and conversation erasure."""


@app.command()
def purge(
    wa_id: Annotated[str, typer.Option("--wa-id", help="Sender identifier to erase.")],
) -> None:
    """Erase this sender's conversations and persisted LangGraph memory."""
    import asyncio

    from pydantic_settings import BaseSettings, SettingsConfigDict
    from sqlalchemy import select

    from wagent.store.erasure import ErasureService
    from wagent.store.models import Conversation
    from wagent.store.repo import InboxRepository, create_engine

    class DatabaseSettings(BaseSettings):
        model_config = SettingsConfigDict(
            env_file=".env", extra="ignore", hide_input_in_errors=True
        )
        database_url: str = "sqlite+aiosqlite:///./wagent.db"

    async def erase() -> int:
        settings = DatabaseSettings()
        engine = create_engine(settings.database_url)
        try:
            inbox = InboxRepository(engine)
            async with inbox.sessions() as session:
                ids = list(
                    (
                        await session.scalars(
                            select(Conversation.id).where(Conversation.wa_id == wa_id)
                        )
                    ).all()
                )
            service = ErasureService(inbox, settings.database_url)
            count = 0
            for conversation_id in ids:
                count += int(await service.erase(conversation_id))
            return count
        finally:
            await engine.dispose()

    try:
        count = asyncio.run(erase())
    except Exception:
        typer.echo(
            "Erasure incomplete or conversation busy. Retry after active work finishes.", err=True
        )
        raise typer.Exit(1) from None
    typer.echo(f"Erased {count} conversation(s).")
