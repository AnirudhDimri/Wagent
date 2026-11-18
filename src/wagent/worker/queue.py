"""Database-backed claims, turn retries, and atomic outbox completion."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from sqlalchemy import exists, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from wagent.outbound.chunking import chunk_text
from wagent.runners.base import AgentReply, turn_id_for
from wagent.settings import Settings
from wagent.store.models import AuditEvent, Conversation, InboundMessage, OutboundMessage, Turn
from wagent.store.repo import InboxRepository
from wagent.worker.debounce import opt_out_match
from wagent.worker.limits import turn_limit_exceeded

ACTIVE = ("pending", "processing")


@dataclass
class Claim:
    conversation: Conversation
    messages: list[InboundMessage]
    turn_id: str
    owner: str


class TurnQueue:
    def __init__(self, inbox: InboxRepository, settings: Settings) -> None:
        self.inbox = inbox
        self.settings = settings

    async def claim(self, now: datetime) -> Claim | None:
        owner = uuid4().hex
        available = or_(Conversation.lease_until.is_(None), Conversation.lease_until <= now)
        has_work = exists().where(
            InboundMessage.conversation_id == Conversation.id,
            InboundMessage.status.in_(ACTIVE),
        )
        delayed = exists().where(
            InboundMessage.conversation_id == Conversation.id,
            InboundMessage.status.in_(ACTIVE),
            InboundMessage.next_attempt_at > now,
        )
        trim = func.btrim if self.inbox.engine.dialect.name == "postgresql" else func.trim
        urgent = exists().where(
            InboundMessage.conversation_id == Conversation.id,
            InboundMessage.status.in_(ACTIVE),
            func.lower(trim(InboundMessage.text, " \t\n\r\v\f")).in_(
                [
                    word.strip().lower()
                    for word in self.settings.opt_out_keywords.split(",")
                    if word.strip()
                ]
            ),
        )
        processing = exists().where(
            InboundMessage.conversation_id == Conversation.id, InboundMessage.status == "processing"
        )

        def pending_time(function: str) -> Any:
            return (
                select(getattr(func, function)(InboundMessage.received_at))
                .where(
                    InboundMessage.conversation_id == Conversation.id,
                    InboundMessage.status == "pending",
                )
                .correlate(Conversation)
                .scalar_subquery()
            )

        ready = or_(
            urgent,
            processing,
            pending_time("max") <= now - timedelta(milliseconds=self.settings.debounce_ms),
            pending_time("min") <= now - timedelta(milliseconds=self.settings.max_debounce_ms),
        )
        async with self.inbox.sessions.begin() as session:
            query = (
                select(Conversation)
                .where(
                    available,
                    has_work,
                    or_(~delayed, urgent),
                    ready,
                    Conversation.erasing.is_(False),
                )
                .order_by(Conversation.created_at, Conversation.id)
                .limit(1)
            )
            if self.inbox.engine.dialect.name == "postgresql":
                query = query.with_for_update(skip_locked=True)
            conversation = (await session.scalars(query)).first()
            if conversation is None:
                return None
            claimed = await session.scalar(
                update(Conversation)
                .where(
                    Conversation.id == conversation.id,
                    available,
                    has_work,
                    or_(~delayed, urgent),
                    ready,
                    Conversation.erasing.is_(False),
                )
                .values(
                    lease_owner=owner,
                    lease_until=now + timedelta(seconds=self.settings.lease_seconds),
                )
                .returning(Conversation.id)
            )
            if claimed is None:
                return None
            rows = list(
                (
                    await session.scalars(
                        select(InboundMessage)
                        .where(
                            InboundMessage.conversation_id == conversation.id,
                            InboundMessage.status.in_(ACTIVE),
                        )
                        .order_by(
                            InboundMessage.wa_timestamp,
                            InboundMessage.received_at,
                            InboundMessage.wa_message_id,
                        )
                    )
                ).all()
            )
            # Preserve the crashed/failed turn's membership when newer messages arrive.
            if not rows:
                conversation.lease_owner = None
                conversation.lease_until = None
                return None
            processing = [row for row in rows if row.status == "processing"]
            if processing and not any(
                opt_out_match(row.text, self.settings.opt_out_keywords) for row in rows
            ):
                previous = processing[0].turn_id
                rows = [row for row in processing if row.turn_id == previous]
            turn_id = turn_id_for(rows)
            if await session.get(Turn, turn_id) is None:
                session.add(
                    Turn(
                        id=turn_id,
                        conversation_id=conversation.id,
                        started_at=now,
                        agent_attempted=False,
                        inbound_ids=[row.wa_message_id for row in rows],
                    )
                )
            for row in rows:
                row.status = "processing"
                row.turn_id = turn_id
            return Claim(conversation, rows, turn_id, owner)

    async def _owned(
        self, session: AsyncSession, claim: Claim, now: datetime
    ) -> Conversation | None:
        # A write locks the row on PostgreSQL and the writer on SQLite. Merely
        # selecting FOR UPDATE would leave SQLite's owner check unfenced.
        owned = await session.scalar(
            update(Conversation)
            .where(
                Conversation.id == claim.conversation.id,
                Conversation.lease_owner == claim.owner,
                Conversation.lease_until > now,
            )
            .values(updated_at=now)
            .returning(Conversation.id)
        )
        return await session.get(Conversation, owned) if owned is not None else None

    async def preflight(self, claim: Claim, now: datetime) -> bool:
        async with self.inbox.sessions.begin() as session:
            conversation = await self._owned(session, claim, now)
            if conversation is None or conversation.erasing:
                return False
            opt_out = any(
                opt_out_match(row.text, self.settings.opt_out_keywords) for row in claim.messages
            )
            status = "skipped"
            reason: str | None = None
            purpose: str | None = None
            text: str | None = None
            if opt_out:
                reason, status = "opt_out", "done"
                if not conversation.opted_out:
                    conversation.opted_out = True
                    purpose, text = (
                        "opt_out",
                        "You have been unsubscribed. We will not send further messages.",
                    )
                    session.add(
                        AuditEvent(
                            conversation_id=conversation.id,
                            type="opt_out",
                            actor="user",
                            at=now,
                            meta={},
                        )
                    )
                await session.execute(
                    update(OutboundMessage)
                    .where(
                        OutboundMessage.conversation_id == conversation.id,
                        OutboundMessage.status == "queued",
                        OutboundMessage.purpose != "opt_out",
                    )
                    .values(status="failed", error_code="opted_out")
                )
            elif conversation.opted_out:
                reason = "opted_out"
            elif self.settings.pause_all or conversation.mode != "bot":
                reason = "paused" if self.settings.pause_all else conversation.mode
            elif await turn_limit_exceeded(
                session, conversation.id, claim.turn_id, now, self.settings.rate_limit_turns_per_min
            ):
                reason = "rate_limited"
                if (
                    conversation.last_rate_notice_at is None
                    or conversation.last_rate_notice_at <= now - timedelta(hours=1)
                ):
                    conversation.last_rate_notice_at = now
                    purpose, text = "rate_notice", "Please slow down and try again in a moment."
            if reason is None:
                await session.execute(
                    update(Turn).where(Turn.id == claim.turn_id).values(agent_attempted=True)
                )
                return True
            if text is not None:
                insert = sqlite_insert if self.inbox.engine.dialect.name == "sqlite" else pg_insert
                key = (
                    f"opt-out:{conversation.id}"
                    if purpose == "opt_out"
                    else f"rate-notice:{claim.turn_id}"
                )
                await session.execute(
                    insert(OutboundMessage)
                    .values(
                        id=uuid4(),
                        conversation_id=conversation.id,
                        turn_id=claim.turn_id,
                        dedupe_key=key,
                        kind="text",
                        body={"text": text},
                        purpose=purpose,
                        created_at=now,
                    )
                    .on_conflict_do_nothing(index_elements=[OutboundMessage.dedupe_key])
                )
            await session.execute(
                update(InboundMessage)
                .where(
                    InboundMessage.wa_message_id.in_([row.wa_message_id for row in claim.messages])
                )
                .values(status=status, last_error=reason, next_attempt_at=None)
            )
            await session.execute(
                update(Turn)
                .where(
                    Turn.conversation_id == conversation.id,
                    Turn.finished_at.is_(None),
                )
                .values(outcome="skipped", finished_at=now)
            )
            conversation.lease_owner, conversation.lease_until = None, None
            return False

    async def complete(
        self,
        claim: Claim,
        reply: AgentReply,
        now: datetime,
        latency_ms: int,
    ) -> bool:
        if reply.buttons or reply.handoff:
            raise ValueError("buttons and handoff are not implemented in M2")
        async with self.inbox.sessions.begin() as session:
            conversation = await self._owned(session, claim, now)
            if conversation is None:
                return False
            if (
                conversation.erasing
                or conversation.opted_out
                or self.settings.pause_all
                or conversation.mode != "bot"
            ):
                reply = AgentReply()
            for index, part in enumerate(chunk_text(reply.text or "")):
                insert = sqlite_insert if self.inbox.engine.dialect.name == "sqlite" else pg_insert
                await session.execute(
                    insert(OutboundMessage)
                    .values(
                        id=uuid4(),
                        conversation_id=conversation.id,
                        turn_id=claim.turn_id,
                        dedupe_key=f"{claim.turn_id}:{index}",
                        kind="text",
                        body={"text": part},
                        created_at=now + timedelta(microseconds=index),
                    )
                    .on_conflict_do_nothing(index_elements=[OutboundMessage.dedupe_key])
                )
            await session.execute(
                update(InboundMessage)
                .where(
                    InboundMessage.wa_message_id.in_([row.wa_message_id for row in claim.messages])
                )
                .values(status="done", next_attempt_at=None, last_error=None)
            )
            await session.execute(
                update(Turn)
                .where(Turn.id == claim.turn_id)
                .values(
                    finished_at=now,
                    outcome="replied" if reply.text else "skipped",
                    latency_ms=latency_ms,
                )
            )
            conversation.lease_owner = None
            conversation.lease_until = None
            return True

    async def fail(self, claim: Claim, error: Exception, now: datetime) -> None:
        async with self.inbox.sessions.begin() as session:
            conversation = await self._owned(session, claim, now)
            if conversation is None:
                return
            attempts = max(row.attempts for row in claim.messages) + 1
            terminal = attempts >= self.settings.max_attempts
            delay = min(300, self.settings.retry_backoff_seconds * 2 ** min(attempts - 1, 20))
            await session.execute(
                update(InboundMessage)
                .where(
                    InboundMessage.wa_message_id.in_([row.wa_message_id for row in claim.messages])
                )
                .values(
                    attempts=attempts,
                    last_error=type(error).__name__,
                    status="failed" if terminal else "processing",
                    next_attempt_at=None if terminal else now + timedelta(seconds=delay),
                )
            )
            if terminal:
                session.add(
                    AuditEvent(
                        conversation_id=conversation.id,
                        type="runner_error",
                        actor="worker",
                        at=now,
                        meta={"turn_id": claim.turn_id, "error": type(error).__name__},
                    )
                )
                await session.execute(
                    update(Turn)
                    .where(Turn.id == claim.turn_id)
                    .values(
                        finished_at=now,
                        outcome="error",
                    )
                )
            conversation.lease_owner = None
            conversation.lease_until = None
