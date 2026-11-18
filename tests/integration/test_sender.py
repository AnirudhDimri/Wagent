"""Outbox state transitions are persisted independently of HTTP outcomes."""

from datetime import timedelta

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import select, update

from wagent import Wagent
from wagent.gateway.events import parse_events
from wagent.outbound.sender import Sender
from wagent.runners import EchoRunner
from wagent.store.models import Conversation, OutboundMessage, utc_now
from wagent.testing.fake_graph_api import FakeGraphAPI
from wagent.testing.payloads import message_payload
from wagent.worker.loop import Worker


async def enqueue(gateway: Wagent) -> None:
    await gateway.inbox.ingest(parse_events(message_payload("Hello"), gateway.settings.wa_phone_id))
    assert await Worker(gateway.inbox, gateway.settings, EchoRunner()).run_once()


async def test_sending_committed_before_http_and_no_resend(gateway: Wagent) -> None:
    await enqueue(gateway)

    async def transport(request: httpx.Request) -> httpx.Response:
        async with gateway.inbox.sessions() as session:
            row = (await session.scalars(select(OutboundMessage))).one()
            assert row.status == "sending" and row.attempts == 1
        assert request.headers["Authorization"] == "Bearer test-token"
        assert request.url.path == "/v26.0/123456789/messages"
        return httpx.Response(200, json={"messages": [{"id": "wamid.sent"}]})

    settings = gateway.settings.model_copy(update={"wa_token": SecretStr("test-token")})
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        sender = Sender(gateway.inbox, settings, client)
        assert await sender.run_once()
        assert not await sender.run_once()
    async with gateway.inbox.sessions() as session:
        row = (await session.scalars(select(OutboundMessage))).one()
        assert row.status == "sent" and row.wa_message_id == "wamid.sent"
        assert row.sent_at is not None


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (httpx.ReadTimeout("ambiguous"), "unknown"),
        (httpx.WriteError("ambiguous"), "unknown"),
        (httpx.ConnectError("not sent"), "queued"),
    ],
)
async def test_transport_failures_never_blindly_retry(
    gateway: Wagent,
    error: httpx.HTTPError,
    expected: str,
) -> None:
    await enqueue(gateway)
    calls = 0

    async def transport(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise error

    settings = gateway.settings.model_copy(update={"wa_token": SecretStr("test-token")})
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        sender = Sender(gateway.inbox, settings, client)
        assert await sender.run_once()
        assert not await sender.run_once()
    assert calls == 1
    async with gateway.inbox.sessions() as session:
        assert (await session.scalars(select(OutboundMessage))).one().status == expected


@pytest.mark.parametrize(
    ("code", "response", "expected"),
    [
        (500, {}, "unknown"),
        (200, {}, "unknown"),
        (400, {}, "failed"),
        (429, {}, "queued"),
    ],
)
async def test_http_failure_classification(
    gateway: Wagent,
    code: int,
    response: dict[str, str],
    expected: str,
) -> None:
    await enqueue(gateway)
    settings = gateway.settings.model_copy(update={"wa_token": SecretStr("test-token")})
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(code, json=response))
    ) as client:
        sender = Sender(gateway.inbox, settings, client)
        assert await sender.run_once()
        assert not await sender.run_once()
    async with gateway.inbox.sessions() as session:
        assert (await session.scalars(select(OutboundMessage))).one().status == expected


async def test_stale_sending_is_unknown_and_never_replayed(gateway: Wagent) -> None:
    await enqueue(gateway)
    async with gateway.inbox.sessions.begin() as session:
        await session.execute(
            update(OutboundMessage).values(
                status="sending", sending_at=utc_now() - timedelta(seconds=121), attempts=1
            )
        )
    graph = FakeGraphAPI()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=graph.app)) as client:
        assert not await Sender(gateway.inbox, gateway.settings, client).run_once()
    assert graph.requests == []
    async with gateway.inbox.sessions() as session:
        assert (await session.scalars(select(OutboundMessage))).one().status == "unknown"


async def test_closed_window_does_not_send_free_form(gateway: Wagent) -> None:
    await enqueue(gateway)
    async with gateway.inbox.sessions.begin() as session:
        await session.execute(
            update(Conversation).values(last_inbound_at=utc_now() - timedelta(days=2))
        )
    graph = FakeGraphAPI()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=graph.app)) as client:
        assert await Sender(gateway.inbox, gateway.settings, client).run_once()
    assert not graph.requests
    async with gateway.inbox.sessions() as session:
        row = (await session.scalars(select(OutboundMessage))).one()
        assert row.status == "failed" and row.error_code == "window_closed"
