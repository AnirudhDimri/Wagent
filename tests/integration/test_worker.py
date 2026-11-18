"""Durable turns, lease fencing, retries, and recovery."""

import asyncio
from datetime import datetime, timedelta

import pytest
from sqlalchemy import select, update

from wagent import Wagent
from wagent.gateway.events import parse_events
from wagent.runners import AgentReply, EchoRunner, TurnContext
from wagent.store.models import Conversation, InboundMessage, OutboundMessage, Turn, utc_now
from wagent.testing.payloads import message_payload
from wagent.worker.loop import Worker
from wagent.worker.queue import TurnQueue


async def ingest(
    gateway: Wagent,
    text: str = "Hello",
    *,
    message_id: str = "wamid.test",
    wa_id: str = "15551234567",
    timestamp: datetime | None = None,
) -> None:
    payload = message_payload(text, message_id=message_id, wa_id=wa_id, timestamp=timestamp)
    await gateway.inbox.ingest(parse_events(payload, gateway.settings.wa_phone_id))


async def test_worker_orders_messages_and_completes_atomically(gateway: Wagent) -> None:
    now = utc_now()
    await ingest(gateway, "second", message_id="second", timestamp=now)
    await ingest(gateway, "first", message_id="first", timestamp=now - timedelta(seconds=1))
    worker = Worker(gateway.inbox, gateway.settings, EchoRunner())
    assert await worker.run_once()
    assert not await worker.run_once()
    async with gateway.inbox.sessions() as session:
        rows = (await session.scalars(select(InboundMessage))).all()
        assert all(row.status == "done" for row in rows)
        turn = (await session.scalars(select(Turn))).one()
        outbound = (await session.scalars(select(OutboundMessage))).one()
        assert turn.inbound_ids == ["first", "second"]
        assert turn.outcome == "replied" and turn.finished_at is not None
        assert outbound.body == {"text": "first\nsecond"}
        assert outbound.dedupe_key == f"{turn.id}:0"
        conversation = (await session.scalars(select(Conversation))).one()
        assert conversation.lease_owner is None and conversation.lease_until is None


async def test_lease_recovery_preserves_turn_and_fences_old_owner(gateway: Wagent) -> None:
    await ingest(gateway)
    queue = TurnQueue(gateway.inbox, gateway.settings)
    now = utc_now()
    old = await queue.claim(now)
    assert old is not None
    assert await queue.claim(now) is None
    await ingest(gateway, "arrived during crash", message_id="new")
    later = now + timedelta(seconds=gateway.settings.lease_seconds + 1)
    new = await queue.claim(later)
    assert new is not None
    assert new.turn_id == old.turn_id and new.owner != old.owner
    assert [row.wa_message_id for row in new.messages] == ["wamid.test"]
    assert not await queue.complete(old, AgentReply(text="stale"), later, 1)
    assert await queue.complete(new, AgentReply(text="recovered"), later, 1)
    following = await queue.claim(later)
    assert following is not None and following.messages[0].wa_message_id == "new"
    async with gateway.inbox.sessions() as session:
        assert len((await session.scalars(select(OutboundMessage))).all()) == 1


class FailureRunner:
    def __init__(self) -> None:
        self.ids: list[str] = []

    async def run(self, ctx: TurnContext) -> AgentReply:
        self.ids.append(ctx.turn_id)
        raise RuntimeError("secret contents must not be saved")


async def test_runner_failure_backoff_and_max_attempts(gateway: Wagent) -> None:
    settings = gateway.settings.model_copy(update={"max_attempts": 2, "retry_backoff_seconds": 100})
    runner = FailureRunner()
    worker = Worker(gateway.inbox, settings, runner)
    await ingest(gateway)
    assert await worker.run_once()
    assert not await worker.run_once()
    async with gateway.inbox.sessions() as session:
        row = (await session.scalars(select(InboundMessage))).one()
        assert row.attempts == 1 and row.next_attempt_at is not None
        assert row.status == "processing" and row.last_error == "RuntimeError"
    await ingest(gateway, "new", message_id="new")
    assert not await worker.run_once()  # A newer message must not bypass the older retry.
    async with gateway.inbox.sessions.begin() as session:
        await session.execute(
            update(InboundMessage).values(next_attempt_at=utc_now() - timedelta(seconds=1))
        )
    assert await worker.run_once()
    assert len(runner.ids) == 2 and runner.ids[0] == runner.ids[1]
    async with gateway.inbox.sessions() as session:
        row = await session.get(InboundMessage, "wamid.test")
        assert row is not None and row.attempts == 2 and row.status == "failed"
        newer = await session.get(InboundMessage, "new")
        assert newer is not None and newer.status == "pending"
        turn = (await session.scalars(select(Turn))).one()
        assert turn.outcome == "error"
        assert not (await session.scalars(select(OutboundMessage))).all()


class SlowRunner:
    async def run(self, ctx: TurnContext) -> AgentReply:
        await asyncio.sleep(10)
        return AgentReply(text="too late")


async def test_runner_timeout(gateway: Wagent) -> None:
    settings = gateway.settings.model_copy(
        update={"runner_timeout_seconds": 0.02, "max_attempts": 1}
    )
    await ingest(gateway)
    assert await Worker(gateway.inbox, settings, SlowRunner()).run_once()
    async with gateway.inbox.sessions() as session:
        row = (await session.scalars(select(InboundMessage))).one()
        assert row.status == "failed" and row.attempts == 1 and row.last_error == "TimeoutError"
        conversation = (await session.scalars(select(Conversation))).one()
        assert conversation.lease_owner is None


async def test_cancellation_leaves_recoverable_processing(gateway: Wagent) -> None:
    await ingest(gateway)
    worker = Worker(gateway.inbox, gateway.settings, SlowRunner())
    task = asyncio.create_task(worker.run_once())
    async with asyncio.timeout(2):
        while True:
            async with gateway.inbox.sessions() as session:
                row = (await session.scalars(select(InboundMessage))).one()
                if row.status == "processing":
                    break
            await asyncio.sleep(0.005)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with gateway.inbox.sessions() as session:
        row = (await session.scalars(select(InboundMessage))).one()
        assert row.status == "processing" and row.turn_id is not None
    queue = TurnQueue(gateway.inbox, gateway.settings)
    claim = await queue.claim(utc_now() + timedelta(seconds=121))
    assert claim is not None


async def test_no_reply_is_a_successful_skipped_turn(gateway: Wagent) -> None:
    class Silent:
        async def run(self, ctx: TurnContext) -> AgentReply:
            return AgentReply()

    await ingest(gateway)
    assert await Worker(gateway.inbox, gateway.settings, Silent()).run_once()
    async with gateway.inbox.sessions() as session:
        assert (await session.scalars(select(Turn))).one().outcome == "skipped"
        assert (await session.scalars(select(InboundMessage))).one().status == "done"
        assert not (await session.scalars(select(OutboundMessage))).all()


async def test_duplicate_worker_claims_are_exclusive(gateway: Wagent) -> None:
    await ingest(gateway)
    queue = TurnQueue(gateway.inbox, gateway.settings)
    claims = await asyncio.gather(queue.claim(utc_now()), queue.claim(utc_now()))
    assert sum(claim is not None for claim in claims) == 1
