"""Retry only provable pre-write failures and documented rejections."""

import json
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from typing import Any

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import select

from wagent import Wagent
from wagent.gateway.events import parse_events
from wagent.outbound.sender import Sender
from wagent.runners import EchoRunner
from wagent.store.models import OutboundMessage, utc_now
from wagent.testing.payloads import message_payload
from wagent.worker.loop import Worker


async def enqueue(gateway: Wagent) -> None:
    await gateway.inbox.ingest(parse_events(message_payload(), gateway.settings.wa_phone_id))
    assert await Worker(gateway.inbox, gateway.settings, EchoRunner()).run_once()


@pytest.mark.parametrize("after", ["90", "http-date"])
async def test_retry_after_is_durable_and_honored(
    gateway: Wagent,
    monkeypatch: pytest.MonkeyPatch,
    after: str,
) -> None:
    await enqueue(gateway)
    clock = [utc_now().replace(microsecond=0)]
    start = clock[0]
    monkeypatch.setattr("wagent.outbound.sender.utc_now", lambda: clock[0])
    header = (
        format_datetime(start + timedelta(seconds=90), usegmt=True)
        if after == "http-date"
        else after
    )
    calls = 0

    async def transport(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                429, headers={"Retry-After": header}, json={"error": {"code": 130429}}
            )
        return httpx.Response(200, json={"messages": [{"id": "wamid.retry"}]})

    settings = gateway.settings.model_copy(update={"wa_token": SecretStr("test-token")})
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as api:
        sender = Sender(gateway.inbox, settings, api)
        assert await sender.run_once()
        async with gateway.inbox.sessions() as session:
            row = (await session.scalars(select(OutboundMessage))).one()
            assert row.status == "queued" and row.next_attempt_at == start + timedelta(seconds=90)
        clock[0] = start + timedelta(seconds=89)
        assert not await sender.run_once()
        # A new engine/session must read the persisted retry schedule.
        restarted = Wagent(settings=settings)
        try:
            sender = Sender(restarted.inbox, settings, api)
            clock[0] = start + timedelta(seconds=90)
            assert await sender.run_once()
        finally:
            await restarted.engine.dispose()
    assert calls == 2
    async with gateway.inbox.sessions() as session:
        row = (await session.scalars(select(OutboundMessage))).one()
        assert row.status == "sent" and row.attempts == 2 and row.error_code is None


@pytest.mark.parametrize("code", [4, 80007, 130429, 131056])
async def test_safe_rejection_codes_retry(gateway: Wagent, code: int) -> None:
    await enqueue(gateway)
    settings = gateway.settings.model_copy(update={"wa_token": SecretStr("test-token")})
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(400, json={"error": {"code": code}}))
    ) as api:
        sender = Sender(gateway.inbox, settings, api)
        assert await sender.run_once()
        assert not await sender.run_once()
    async with gateway.inbox.sessions() as session:
        row = (await session.scalars(select(OutboundMessage))).one()
        assert row.status == "queued" and row.error_code == str(code)


async def test_safe_retry_attempts_are_bounded(
    gateway: Wagent, monkeypatch: pytest.MonkeyPatch
) -> None:
    await enqueue(gateway)
    clock = [utc_now()]
    monkeypatch.setattr("wagent.outbound.sender.utc_now", lambda: clock[0])
    settings = gateway.settings.model_copy(
        update={"wa_token": SecretStr("test-token"), "send_max_attempts": 2}
    )

    async def transport(_: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no request bytes written")

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as api:
        sender = Sender(gateway.inbox, settings, api)
        assert await sender.run_once()
        clock[0] += timedelta(minutes=1)
        assert await sender.run_once()
        assert not await sender.run_once()
    async with gateway.inbox.sessions() as session:
        row = (await session.scalars(select(OutboundMessage))).one()
        assert row.status == "failed" and row.attempts == 2 and row.next_attempt_at is None


async def test_window_rechecked_on_retry_and_template_uses_pywa(
    gateway: Wagent,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await enqueue(gateway)
    clock = [utc_now()]
    monkeypatch.setattr("wagent.outbound.sender.utc_now", lambda: clock[0])
    settings = gateway.settings.model_copy(
        update={
            "wa_token": SecretStr("test-token"),
            "wa_waba_id": "waba",
            "out_of_window_policy": "template",
            "fallback_template_name": "reopen",
        }
    )
    requests: list[dict[str, Any]] = []

    async def transport(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        if len(requests) == 1:
            return httpx.Response(429, headers={"Retry-After": "90000"})
        return httpx.Response(
            200,
            json={
                "messaging_product": "whatsapp",
                "contacts": [{"input": "15551234567", "wa_id": "15551234567"}],
                "messages": [{"id": "wamid.template-retry"}],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as api:
        sender = Sender(gateway.inbox, settings, api)
        assert await sender.run_once()
        clock[0] += timedelta(seconds=90001)
        assert await sender.run_once()
    assert [request["type"] for request in requests] == ["text", "template"]
    async with gateway.inbox.sessions() as session:
        row = (await session.scalars(select(OutboundMessage))).one()
        assert row.status == "sent" and row.kind == "template"


async def test_template_429_uses_same_safe_retry_policy(gateway: Wagent) -> None:
    await enqueue(gateway)
    from sqlalchemy import update

    from wagent.store.models import Conversation

    async with gateway.inbox.sessions.begin() as session:
        await session.execute(
            update(Conversation).values(last_inbound_at=datetime(2020, 1, 1, tzinfo=UTC))
        )
    settings = gateway.settings.model_copy(
        update={
            "wa_token": SecretStr("test-token"),
            "wa_waba_id": "waba",
            "out_of_window_policy": "template",
            "fallback_template_name": "reopen",
        }
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(429, headers={"Retry-After": "20"}))
    ) as api:
        assert await Sender(gateway.inbox, settings, api).run_once()
    async with gateway.inbox.sessions() as session:
        row = (await session.scalars(select(OutboundMessage))).one()
        assert row.kind == "template" and row.status == "queued" and row.next_attempt_at is not None


@pytest.mark.parametrize("code", [1, 2, 131000, 131016])
async def test_generic_transient_errors_remain_unknown(gateway: Wagent, code: int) -> None:
    await enqueue(gateway)
    settings = gateway.settings.model_copy(update={"wa_token": SecretStr("test-token")})
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(400, json={"error": {"code": code, "is_transient": True}})
        )
    ) as api:
        sender = Sender(gateway.inbox, settings, api)
        assert await sender.run_once()
        assert not await sender.run_once()
    async with gateway.inbox.sessions() as session:
        row = (await session.scalars(select(OutboundMessage))).one()
        assert row.status == "unknown" and row.attempts == 1
