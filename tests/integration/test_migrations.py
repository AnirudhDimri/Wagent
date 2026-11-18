"""Migration upgrade/downgrade and schema readiness are real DB operations."""

import asyncio
from pathlib import Path

import httpx
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import inspect
from sqlalchemy.engine import Connection

from wagent import Settings, Wagent
from wagent.store.models import Base
from wagent.store.repo import create_engine


async def test_model_schema_matches_migration(gateway: Wagent) -> None:
    def compare(connection: Connection) -> None:
        assert compare_metadata(MigrationContext.configure(connection), Base.metadata) == []

    async with gateway.engine.connect() as connection:
        await connection.run_sync(compare)


async def test_unmigrated_database_not_ready(settings: Settings, tmp_path: Path) -> None:
    settings = settings.model_copy(
        update={
            "database_url": f"sqlite+aiosqlite:///{tmp_path / 'unmigrated.db'}",
        }
    )
    app = Wagent(settings=settings)
    async with app.fastapi_app.router.lifespan_context(app.fastapi_app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app.fastapi_app), base_url="http://test"
        ) as client:
            assert (await client.get("/healthz")).status_code == 200
            assert (await client.get("/readyz")).status_code == 503


async def test_round_trip(database_url: str) -> None:
    await asyncio.to_thread(command.downgrade, Config("alembic.ini"), "base")
    engine = create_engine(database_url)
    try:
        async with engine.connect() as connection:
            names = await connection.run_sync(lambda conn: inspect(conn).get_table_names())
            assert "conversations" not in names
            assert "inbound_messages" not in names
        await asyncio.to_thread(command.upgrade, Config("alembic.ini"), "head")
        async with engine.connect() as connection:
            names = await connection.run_sync(lambda conn: inspect(conn).get_table_names())
            assert {"conversations", "inbound_messages"}.issubset(names)
    finally:
        await engine.dispose()


async def test_custom_webhook_path(settings: Settings) -> None:
    app = Wagent(settings=settings.model_copy(update={"webhook_path": "/meta/events"}))
    async with app.fastapi_app.router.lifespan_context(app.fastapi_app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app.fastapi_app), base_url="http://test"
        ) as client:
            assert (await client.post("/meta/events", content=b"{}")).status_code == 401
            assert (await client.get("/webhook")).status_code == 404


async def test_m3_downgrade_upgrade_preserves_m2_messages(gateway: Wagent) -> None:
    from sqlalchemy import select

    from wagent.gateway.events import parse_events
    from wagent.runners import EchoRunner
    from wagent.store.models import OutboundMessage
    from wagent.testing.payloads import message_payload
    from wagent.worker.loop import Worker

    await gateway.inbox.ingest(
        parse_events(message_payload("keep me"), gateway.settings.wa_phone_id)
    )
    assert await Worker(gateway.inbox, gateway.settings, EchoRunner()).run_once()
    async with gateway.inbox.sessions() as session:
        original = (await session.scalars(select(OutboundMessage))).one()
    await asyncio.to_thread(command.downgrade, Config("alembic.ini"), "0002_worker_outbox")
    await asyncio.to_thread(command.upgrade, Config("alembic.ini"), "head")
    async with gateway.inbox.sessions() as session:
        restored = (await session.scalars(select(OutboundMessage))).one()
        assert restored.id == original.id and restored.body == {"text": "keep me"}
        assert restored.status == "queued" and restored.next_attempt_at is None
