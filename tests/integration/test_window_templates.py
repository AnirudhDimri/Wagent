"""Frozen send-time windows, pywa template contracts, and chunk coalescing."""

from datetime import UTC, datetime, timedelta

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import select, update

from wagent import Wagent
from wagent.gateway.events import parse_events
from wagent.outbound.sender import Sender
from wagent.runners import EchoRunner
from wagent.store.models import AuditEvent, Conversation, OutboundMessage
from wagent.testing.fake_graph_api import FakeGraphAPI
from wagent.testing.payloads import message_payload
from wagent.worker.loop import Worker


async def enqueue(gateway: Wagent, text: str = "Hello") -> None:
    await gateway.inbox.ingest(parse_events(message_payload(text), gateway.settings.wa_phone_id))
    assert await Worker(gateway.inbox, gateway.settings, EchoRunner()).run_once()


@pytest.mark.parametrize(
    ("offset", "policy", "expected"),
    [
        (timedelta(hours=23, minutes=59), "drop", "text"),
        (timedelta(hours=24), "drop", None),
        (timedelta(hours=24, minutes=1), "drop", None),
        (timedelta(hours=24, minutes=1), "template", "template"),
    ],
)
async def test_send_time_window(
    gateway: Wagent,
    monkeypatch: pytest.MonkeyPatch,
    offset: timedelta,
    policy: str,
    expected: str | None,
) -> None:
    await enqueue(gateway)
    last = datetime(2026, 11, 14, 12, tzinfo=UTC)
    async with gateway.inbox.sessions.begin() as session:
        await session.execute(update(Conversation).values(last_inbound_at=last))
    monkeypatch.setattr("wagent.outbound.sender.utc_now", lambda: last + offset)
    settings = gateway.settings.model_copy(
        update={
            "wa_token": SecretStr("test-token"),
            "wa_waba_id": "test-waba",
            "out_of_window_policy": policy,
            "fallback_template_name": "reopen_service",
            "fallback_template_lang": "en_US",
        }
    )
    graph = FakeGraphAPI()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=graph.app)) as client:
        original_headers = dict(client.headers)
        original_url = client.base_url
        sender = Sender(gateway.inbox, settings, client)
        assert await sender.run_once()
        assert not await sender.run_once()
        assert dict(client.headers) == original_headers and client.base_url == original_url
        assert not client.is_closed
    async with gateway.inbox.sessions() as session:
        row = (await session.scalars(select(OutboundMessage))).one()
        if expected is None:
            assert graph.requests == []
            assert row.status == "failed" and row.error_code == "window_closed"
            assert (await session.scalars(select(AuditEvent))).one().type == "outbound_drop"
        else:
            assert row.status == "sent"
            body = graph.requests[0]["body"]
            assert body["type"] == expected
            assert body["biz_opaque_callback_data"] == str(row.id)
            if expected == "template":
                assert body["template"] == {"name": "reopen_service", "language": {"code": "en_US"}}


async def test_10000_character_reply_delivers_ordered_chunks(gateway: Wagent) -> None:
    text = "a" * 3000 + "\n\n" + "b" * 6998
    assert len(text) == 10000
    await enqueue(gateway, text)
    settings = gateway.settings.model_copy(update={"wa_token": SecretStr("test-token")})
    graph = FakeGraphAPI()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=graph.app)) as client:
        sender = Sender(gateway.inbox, settings, client)
        for _ in range(3):
            assert await sender.run_once()
        assert not await sender.run_once()
    parts = [request["body"]["text"]["body"] for request in graph.requests]
    assert "".join(parts) == text
    assert all(len(part) <= 4096 for part in parts)
    async with gateway.inbox.sessions() as session:
        rows = (
            await session.scalars(select(OutboundMessage).order_by(OutboundMessage.created_at))
        ).all()
        assert [row.dedupe_key.rsplit(":", 1)[-1] for row in rows] == ["0", "1", "2"]
        assert all(row.status == "sent" for row in rows)


async def test_closed_window_long_reply_sends_one_template(gateway: Wagent) -> None:
    await enqueue(gateway, "x" * 10000)
    async with gateway.inbox.sessions.begin() as session:
        await session.execute(
            update(Conversation).values(last_inbound_at=datetime(2020, 1, 1, tzinfo=UTC))
        )
    settings = gateway.settings.model_copy(
        update={
            "wa_token": SecretStr("test-token"),
            "wa_waba_id": "test-waba",
            "out_of_window_policy": "template",
            "fallback_template_name": "reopen",
        }
    )
    graph = FakeGraphAPI()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=graph.app)) as client:
        sender = Sender(gateway.inbox, settings, client)
        assert await sender.run_once()
        assert not await sender.run_once()
    assert len(graph.requests) == 1 and graph.requests[0]["body"]["type"] == "template"
    async with gateway.inbox.sessions() as session:
        rows = (await session.scalars(select(OutboundMessage))).all()
        assert sum(row.status == "sent" for row in rows) == 1
        assert sum(row.error_code == "template_coalesced" for row in rows) == 2


async def test_opted_out_user_never_gets_template(gateway: Wagent) -> None:
    await enqueue(gateway)
    async with gateway.inbox.sessions.begin() as session:
        await session.execute(update(Conversation).values(opted_out=True))
    settings = gateway.settings.model_copy(
        update={
            "wa_token": SecretStr("test-token"),
            "wa_waba_id": "test-waba",
            "out_of_window_policy": "template",
            "fallback_template_name": "marketing",
        }
    )
    graph = FakeGraphAPI()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=graph.app)) as client:
        assert await Sender(gateway.inbox, settings, client).run_once()
    assert not graph.requests


async def test_pywa_debug_does_not_log_recipient_or_token(
    gateway: Wagent,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    await enqueue(gateway)
    async with gateway.inbox.sessions.begin() as session:
        await session.execute(
            update(Conversation).values(last_inbound_at=datetime(2020, 1, 1, tzinfo=UTC))
        )
    settings = gateway.settings.model_copy(
        update={
            "wa_token": SecretStr("test-token"),
            "wa_waba_id": "test-waba",
            "out_of_window_policy": "template",
            "fallback_template_name": "reopen",
        }
    )
    graph = FakeGraphAPI()
    with caplog.at_level(logging.DEBUG, logger="pywa.api"):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=graph.app)) as client:
            assert await Sender(gateway.inbox, settings, client).run_once()
    assert len(graph.requests) == 1
    assert "15551234567" not in caplog.text and "test-token" not in caplog.text
    # The filter applies only inside this SDK call, not to the caller's own pywa use.
    with caplog.at_level(logging.DEBUG, logger="pywa.api"):
        logging.getLogger("pywa.api").debug("outside-template-marker")
    assert "outside-template-marker" in caplog.text
