"""M4 debounce, admission limits, pause, opt-out, and raw retention."""

import asyncio
from datetime import timedelta

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import func, select, update

from wagent import Settings, Wagent
from wagent.gateway.events import parse_events
from wagent.outbound.sender import Sender
from wagent.runners import AgentReply, EchoRunner, TurnContext
from wagent.store.models import (
    AuditEvent,
    Conversation,
    InboundMessage,
    OutboundMessage,
    Turn,
    utc_now,
)
from wagent.store.retention import RetentionJob
from wagent.testing.fake_graph_api import FakeGraphAPI
from wagent.testing.payloads import message_payload, signed_payload
from wagent.worker.loop import Worker
from wagent.worker.queue import TurnQueue


async def inbound(gateway: Wagent, text: str, message_id: str) -> None:
    await gateway.inbox.ingest(
        parse_events(message_payload(text, message_id=message_id), gateway.settings.wa_phone_id)
    )


class RecordingEcho(EchoRunner):
    def __init__(self) -> None:
        self.calls = 0

    async def run(self, ctx: TurnContext) -> AgentReply:
        self.calls += 1
        return await super().run(ctx)


async def test_debounce_uses_newest_and_maximum_wait(gateway: Wagent) -> None:
    await inbound(gateway, "one", "one")
    await inbound(gateway, "two", "two")
    now = utc_now()
    async with gateway.inbox.sessions.begin() as session:
        await session.execute(
            update(InboundMessage)
            .where(InboundMessage.wa_message_id == "one")
            .values(received_at=now - timedelta(seconds=2))
        )
        await session.execute(
            update(InboundMessage)
            .where(InboundMessage.wa_message_id == "two")
            .values(received_at=now)
        )
    settings = gateway.settings.model_copy(update={"debounce_ms": 1500, "max_debounce_ms": 5000})
    queue = TurnQueue(gateway.inbox, settings)
    assert await queue.claim(now + timedelta(seconds=1)) is None
    claim = await queue.claim(now + timedelta(seconds=1.5))
    assert claim is not None and len(claim.messages) == 2


async def test_continuous_messages_cannot_delay_beyond_max(gateway: Wagent) -> None:
    await inbound(gateway, "old", "old")
    await inbound(gateway, "new", "new")
    now = utc_now()
    async with gateway.inbox.sessions.begin() as session:
        await session.execute(
            update(InboundMessage)
            .where(InboundMessage.wa_message_id == "old")
            .values(received_at=now - timedelta(seconds=5))
        )
    settings = gateway.settings.model_copy(update={"debounce_ms": 1500, "max_debounce_ms": 5000})
    assert await TurnQueue(gateway.inbox, settings).claim(now) is not None


async def test_three_rapid_messages_make_one_turn_and_reply(settings: Settings) -> None:
    settings = settings.model_copy(
        update={"debounce_ms": 1500, "max_debounce_ms": 5000, "wa_token": SecretStr("test-token")}
    )
    graph, runner = FakeGraphAPI(), RecordingEcho()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=graph.app)) as api:
        app = Wagent(settings=settings, runner=runner, graph_client=api)
        async with app.fastapi_app.router.lifespan_context(app.fastapi_app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app.fastapi_app), base_url="http://test"
            ) as client:
                for text in ["one", "two", "three"]:
                    body, headers = signed_payload(message_payload(text), "local-app-secret")
                    assert (
                        await client.post("/webhook", content=body, headers=headers)
                    ).status_code == 200
                    await asyncio.sleep(0.1)
                assert runner.calls == 0 and not graph.requests
                async with asyncio.timeout(5):
                    for _ in range(500):
                        if graph.requests:
                            break
                        await asyncio.sleep(0.01)
                assert runner.calls == 1 and len(graph.requests) == 1
                assert graph.requests[0]["body"]["text"]["body"] == "one\ntwo\nthree"
                async with app.inbox.sessions() as session:
                    assert await session.scalar(select(func.count()).select_from(Turn)) == 1


async def test_turn_rate_limit_and_one_notice_per_hour(gateway: Wagent) -> None:
    settings = gateway.settings.model_copy(update={"rate_limit_turns_per_min": 1})
    runner = RecordingEcho()
    worker = Worker(gateway.inbox, settings, runner)
    for index in range(3):
        await inbound(gateway, f"message {index}", str(index))
        assert await worker.run_once()
    assert runner.calls == 1
    async with gateway.inbox.sessions() as session:
        rows = (await session.scalars(select(InboundMessage))).all()
        assert sum(row.status == "skipped" for row in rows) == 2
        replies = (await session.scalars(select(OutboundMessage))).all()
        assert sum(row.purpose == "rate_notice" for row in replies) == 1
        assert sum(row.purpose == "agent" for row in replies) == 1


@pytest.mark.parametrize("mode", ["bot", "human", "paused"])
@pytest.mark.parametrize("paused", [False, True])
async def test_opt_out_priority_and_single_confirmation(
    gateway: Wagent,
    mode: str,
    paused: bool,
) -> None:
    await inbound(gateway, " stop ", "stop-1")
    async with gateway.inbox.sessions.begin() as session:
        await session.execute(update(Conversation).values(mode=mode))
    settings = gateway.settings.model_copy(
        update={
            "pause_all": paused,
            "daily_outbound_cap_per_user": 0,
            "wa_token": SecretStr("test-token"),
        }
    )
    runner = RecordingEcho()
    worker = Worker(gateway.inbox, settings, runner)
    assert await worker.run_once()
    graph = FakeGraphAPI()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=graph.app)) as api:
        sender = Sender(gateway.inbox, settings, api)
        assert await sender.run_once()
        for text, message_id in [("STOP", "stop-2"), ("hello", "after-stop")]:
            await inbound(gateway, text, message_id)
            assert await worker.run_once()
        assert not await sender.run_once()
    assert runner.calls == 0 and len(graph.requests) == 1
    assert graph.requests[0]["body"]["type"] == "text"
    async with gateway.inbox.sessions() as session:
        assert (await session.scalars(select(Conversation))).one().opted_out
        assert (await session.scalars(select(AuditEvent).where(AuditEvent.type == "opt_out"))).one()
        assert (await session.scalars(select(OutboundMessage))).one().purpose == "opt_out"
        assert (await session.get(InboundMessage, "after-stop")).status == "skipped"  # type: ignore[union-attr]


async def test_opt_out_cancels_queued_bot_reply(gateway: Wagent) -> None:
    worker = Worker(gateway.inbox, gateway.settings, EchoRunner())
    await inbound(gateway, "old reply", "old")
    assert await worker.run_once()
    await inbound(gateway, "UNSUBSCRIBE", "stop")
    assert await worker.run_once()
    async with gateway.inbox.sessions() as session:
        rows = (await session.scalars(select(OutboundMessage))).all()
        assert sum(row.error_code == "opted_out" for row in rows) == 1
        assert sum(row.purpose == "opt_out" and row.status == "queued" for row in rows) == 1


async def test_stop_preempts_retry_backoff(gateway: Wagent) -> None:
    class Failing:
        async def run(self, ctx: TurnContext) -> AgentReply:
            raise RuntimeError("no user data")

    settings = gateway.settings.model_copy(update={"retry_backoff_seconds": 100})
    worker = Worker(gateway.inbox, settings, Failing())
    await inbound(gateway, "hello", "old")
    assert await worker.run_once()
    await inbound(gateway, "STOP", "new")
    assert await worker.run_once()
    async with gateway.inbox.sessions() as session:
        assert (await session.scalars(select(Conversation))).one().opted_out
        assert all(
            row.status == "done" for row in (await session.scalars(select(InboundMessage))).all()
        )


@pytest.mark.parametrize("mode", ["paused", "human"])
async def test_non_bot_mode_skips_without_agent(gateway: Wagent, mode: str) -> None:
    await inbound(gateway, "hello", "skip")
    async with gateway.inbox.sessions.begin() as session:
        await session.execute(update(Conversation).values(mode=mode))
    runner = RecordingEcho()
    assert await Worker(gateway.inbox, gateway.settings, runner).run_once()
    assert runner.calls == 0
    async with gateway.inbox.sessions() as session:
        assert (await session.scalars(select(InboundMessage))).one().status == "skipped"


async def test_daily_cap_persists_and_resets_on_utc_day(
    gateway: Wagent,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = gateway.settings.model_copy(
        update={"daily_outbound_cap_per_user": 1, "wa_token": SecretStr("test-token")}
    )
    worker = Worker(gateway.inbox, settings, EchoRunner())
    for index in range(2):
        await inbound(gateway, f"reply {index}", str(index))
        assert await worker.run_once()
    clock = [utc_now()]
    monkeypatch.setattr("wagent.outbound.sender.utc_now", lambda: clock[0])
    graph = FakeGraphAPI()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=graph.app)) as api:
        assert await Sender(gateway.inbox, settings, api).run_once()
        assert await Sender(gateway.inbox, settings, api).run_once()
        assert len(graph.requests) == 1
        clock[0] += timedelta(days=1)
        await inbound(gateway, "tomorrow", "tomorrow")
        async with gateway.inbox.sessions.begin() as session:
            await session.execute(update(Conversation).values(last_inbound_at=clock[0]))
        assert await worker.run_once()
        assert await Sender(gateway.inbox, settings, api).run_once()
    assert len(graph.requests) == 2
    async with gateway.inbox.sessions() as session:
        assert (
            await session.scalars(
                select(OutboundMessage).where(OutboundMessage.error_code == "daily_cap")
            )
        ).one()


async def test_retention_removes_only_old_raw_payloads(gateway: Wagent) -> None:
    await inbound(gateway, "old text", "old")
    await inbound(gateway, "new text", "new")
    async with gateway.inbox.sessions.begin() as session:
        await session.execute(
            update(InboundMessage)
            .where(InboundMessage.wa_message_id == "old")
            .values(received_at=utc_now() - timedelta(days=31))
        )
    await RetentionJob(gateway.inbox, gateway.settings).run_once()
    async with gateway.inbox.sessions() as session:
        old = await session.get(InboundMessage, "old")
        new = await session.get(InboundMessage, "new")
        assert old is not None and old.raw is None and old.text == "old text"
        assert new is not None and new.raw is not None


async def test_pause_all_skips_agent_and_drops_already_queued_reply(gateway: Wagent) -> None:
    await inbound(gateway, "queued before pause", "before")
    assert await Worker(gateway.inbox, gateway.settings, EchoRunner()).run_once()
    settings = gateway.settings.model_copy(update={"pause_all": True})
    await inbound(gateway, "during pause", "during")
    runner = RecordingEcho()
    assert await Worker(gateway.inbox, settings, runner).run_once()
    assert runner.calls == 0
    graph = FakeGraphAPI()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=graph.app)) as api:
        assert await Sender(gateway.inbox, settings, api).run_once()
    assert not graph.requests


async def test_cap_counts_unknown_sends_and_retry_does_not_take_second_slot(
    gateway: Wagent,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = gateway.settings.model_copy(
        update={"daily_outbound_cap_per_user": 1, "wa_token": SecretStr("test-token")}
    )
    worker = Worker(gateway.inbox, settings, EchoRunner())
    await inbound(gateway, "first", "first")
    assert await worker.run_once()
    clock = [utc_now()]
    monkeypatch.setattr("wagent.outbound.sender.utc_now", lambda: clock[0])
    calls = 0

    async def transport(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError("pre-write failure")
        raise httpx.ReadTimeout("ambiguous send")

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as api:
        sender = Sender(gateway.inbox, settings, api)
        assert await sender.run_once()
        clock[0] += timedelta(seconds=2)
        assert await sender.run_once()  # Same logical reply retains its slot.
        await inbound(gateway, "second", "second")
        assert await worker.run_once()
        assert await sender.run_once()  # Drops because the unknown reply consumed capacity.
    assert calls == 2
    async with gateway.inbox.sessions() as session:
        assert (
            await session.scalars(
                select(OutboundMessage).where(OutboundMessage.error_code == "daily_cap")
            )
        ).one()
