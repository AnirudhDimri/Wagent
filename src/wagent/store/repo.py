"""Async database lifecycle and transactional, conflict-safe ingestion."""

from collections.abc import Sequence
from typing import Any
from uuid import uuid4

from sqlalchemy import case, event, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from wagent.gateway.events import InboundEvent
from wagent.store.models import Conversation, InboundMessage, utc_now


def create_engine(url: str) -> AsyncEngine:
    engine = create_async_engine(url, hide_parameters=True, pool_pre_ping=True)
    if engine.dialect.name == "sqlite":

        @event.listens_for(engine.sync_engine, "connect")
        def sqlite_options(connection: Any, _: Any) -> None:
            cursor = connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA busy_timeout=5000")
            cursor.close()

    return engine


class InboxRepository:
    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine
        self.sessions = async_sessionmaker(engine, expire_on_commit=False)

    async def ready(self) -> None:
        """Require connectivity and the M1 schema, even on an empty database."""
        async with self.sessions() as session:
            await session.execute(text("SELECT 1"))
            await session.execute(select(Conversation).limit(0))
            await session.execute(select(InboundMessage).limit(0))

    async def ingest(self, events: Sequence[InboundEvent]) -> None:
        if not events:
            return
        insert = sqlite_insert if self.engine.dialect.name == "sqlite" else pg_insert
        async with self.sessions.begin() as session:
            # Stable lock order prevents opposing batched deliveries from deadlocking.
            for item in sorted(events, key=lambda e: (e.phone_number_id, e.wa_id)):
                now = utc_now()
                conversation = insert(Conversation).values(
                    id=uuid4(),
                    phone_number_id=item.phone_number_id,
                    wa_id=item.wa_id,
                    thread_id=f"wa:{item.phone_number_id}:{item.wa_id}",
                    last_inbound_at=item.wa_timestamp,
                    created_at=now,
                    updated_at=now,
                )
                conversation = conversation.on_conflict_do_update(
                    index_elements=[Conversation.phone_number_id, Conversation.wa_id],
                    set_={
                        "last_inbound_at": case(
                            (Conversation.last_inbound_at.is_(None), item.wa_timestamp),
                            (Conversation.last_inbound_at < item.wa_timestamp, item.wa_timestamp),
                            else_=Conversation.last_inbound_at,
                        ),
                        "updated_at": now,
                    },
                ).returning(Conversation.id)
                conversation_id = (await session.execute(conversation)).scalar_one()
                inbound = (
                    insert(InboundMessage)
                    .values(
                        wa_message_id=item.wa_message_id,
                        conversation_id=conversation_id,
                        received_at=now,
                        wa_timestamp=item.wa_timestamp,
                        kind=item.kind,
                        text=item.text,
                        media_id=item.media_id,
                        raw=item.raw,
                    )
                    .on_conflict_do_nothing(index_elements=[InboundMessage.wa_message_id])
                )
                await session.execute(inbound)
