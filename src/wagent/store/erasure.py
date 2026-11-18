"""Retryable erasure spanning application rows and LangGraph checkpoints."""

import asyncio
from datetime import timedelta
from uuid import UUID, uuid4

from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from sqlalchemy import delete, exists, inspect, or_, select, update
from sqlalchemy.engine import make_url

from wagent.store.models import Conversation, MessageStatusEvent, OutboundMessage, utc_now
from wagent.store.repo import InboxRepository


class ConversationBusy(Exception):
    pass


class ErasureService:
    def __init__(
        self, inbox: InboxRepository, database_url: str, lease_seconds: float = 120
    ) -> None:
        self.inbox = inbox
        self.database_url = database_url
        self.lease_seconds = lease_seconds

    async def erase_thread(self, thread_id: str) -> None:
        async with self.inbox.engine.connect() as connection:
            tables = await connection.run_sync(lambda conn: inspect(conn).get_table_names())
        if "checkpoints" not in tables:
            return
        url = make_url(self.database_url)
        if url.drivername == "sqlite+aiosqlite":
            async with AsyncSqliteSaver.from_conn_string(url.database or ":memory:") as saver:
                await saver.adelete_thread(thread_id)
        else:
            from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

            async with AsyncPostgresSaver.from_conn_string(
                url.set(drivername="postgresql").render_as_string(hide_password=False)
            ) as saver:
                await saver.adelete_thread(thread_id)

    async def erase(self, conversation_id: UUID) -> bool:
        now = utc_now()
        owner = "erase:" + uuid4().hex
        active_send = exists().where(
            OutboundMessage.conversation_id == conversation_id,
            OutboundMessage.status == "sending",
            or_(
                OutboundMessage.sending_at.is_(None),
                OutboundMessage.sending_at > now - timedelta(seconds=self.lease_seconds),
            ),
        )
        async with self.inbox.sessions.begin() as session:
            conversation = await session.scalar(
                update(Conversation)
                .where(
                    Conversation.id == conversation_id,
                    or_(Conversation.lease_until.is_(None), Conversation.lease_until <= now),
                )
                .values(updated_at=now)
                .returning(Conversation)
            )
            if conversation is None:
                if await session.get(Conversation, conversation_id) is None:
                    return False
                raise ConversationBusy()
            # Recheck sends in a fresh statement after acquiring the conversation
            # lock. PostgreSQL's earlier statement snapshot may predate a sender commit.
            if await session.scalar(select(active_send)):
                raise ConversationBusy()
            conversation.erasing = True
            conversation.lease_owner = owner
            conversation.lease_until = now + timedelta(seconds=self.lease_seconds)
            thread_id = conversation.thread_id
        # No worker/sender/ingest may write this conversation while the marker is set.
        # Keep the marker on error, so another erase call can safely retry.
        try:
            async with asyncio.timeout(self.lease_seconds / 2):
                await self.erase_thread(thread_id)
        except BaseException:
            async with self.inbox.sessions.begin() as session:
                await session.execute(
                    update(Conversation)
                    .where(
                        Conversation.id == conversation_id,
                        Conversation.lease_owner == owner,
                    )
                    .values(lease_owner=None, lease_until=None)
                )
            raise
        async with self.inbox.sessions.begin() as session:
            current = await session.scalar(
                update(Conversation)
                .where(
                    Conversation.id == conversation_id,
                    Conversation.erasing.is_(True),
                    Conversation.lease_owner == owner,
                )
                .values(erasing=True)
                .returning(Conversation)
            )
            if current is None:
                return False
            outgoing = (
                await session.scalars(
                    select(OutboundMessage).where(
                        OutboundMessage.conversation_id == conversation_id
                    )
                )
            ).all()
            ids = [str(row.id) for row in outgoing]
            wamids = [row.wa_message_id for row in outgoing if row.wa_message_id]
            await session.execute(
                delete(MessageStatusEvent).where(
                    MessageStatusEvent.phone_number_id == current.phone_number_id,
                    or_(
                        MessageStatusEvent.tracker.in_(ids),
                        MessageStatusEvent.wa_message_id.in_(wamids),
                    ),
                )
            )
            # Audit/usage/inbound/turn/outbound rows cascade; retain no per-person purge log.
            await session.execute(delete(Conversation).where(Conversation.id == conversation_id))
        return True
