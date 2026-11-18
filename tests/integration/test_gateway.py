"""Exercise real HTTP routes and migrated SQLite persistence together."""

import asyncio
import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Any

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError

from wagent import Settings, Wagent
from wagent.store.models import Conversation, InboundMessage
from wagent.testing.payloads import message_payload, signed_payload

APP_SECRET = "local-app-secret"
VERIFY_TOKEN = "local-verify-token"


async def post(client: httpx.AsyncClient, payload: object) -> httpx.Response:
    body, headers = signed_payload(payload, APP_SECRET)
    return await client.post("/webhook", content=body, headers=headers)


@pytest.mark.parametrize("text", ["Hello", "Olá café 🌍 — नमस्ते"])
async def test_signed_message_is_durable(
    client: httpx.AsyncClient, gateway: Wagent, text: str
) -> None:
    payload = message_payload(text, message_id="wamid.test")
    response = await post(client, payload)
    assert response.status_code == 200
    async with gateway.inbox.sessions() as session:
        row = (await session.scalars(select(InboundMessage))).one()
        conversation = (await session.scalars(select(Conversation))).one()
        assert row.text == text
        assert row.raw is not None and row.raw["id"] == "wamid.test"
        assert row.status == "pending" and row.attempts == 0
        assert row.received_at.tzinfo == UTC and row.wa_timestamp.tzinfo == UTC
        assert conversation.thread_id == "wa:123456789:15551234567"
        assert conversation.mode == "bot" and not conversation.opted_out
        assert conversation.last_inbound_at == row.wa_timestamp


@pytest.mark.parametrize("header", [None, "sha256=" + "0" * 64, "bad", "💥", "0" * 64])
async def test_invalid_signatures_never_persist(
    client: httpx.AsyncClient, gateway: Wagent, header: str | None
) -> None:
    body, headers = signed_payload(message_payload(), APP_SECRET)
    headers.pop("X-Hub-Signature-256")
    if header is not None:
        # httpx requires ASCII HTTP header values; use raw UTF-8 bytes to test rejection.
        raw_headers = [(key.encode(), value.encode()) for key, value in headers.items()]
        raw_headers.append((b"X-Hub-Signature-256", header.encode()))
        response = await client.post("/webhook", content=body, headers=raw_headers)
    else:
        response = await client.post("/webhook", content=body, headers=headers)
    assert response.status_code == 401
    async with gateway.inbox.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(InboundMessage)) == 0
        assert await session.scalar(select(func.count()).select_from(Conversation)) == 0


async def test_tampering_and_raw_serialization(client: httpx.AsyncClient) -> None:
    payload = message_payload("café 🌍")
    body, headers = signed_payload(payload, APP_SECRET)
    assert (await client.post("/webhook", content=body + b" ", headers=headers)).status_code == 401
    different_bytes = json.dumps(payload, ensure_ascii=True, indent=2).encode()
    assert (
        await client.post("/webhook", content=different_bytes, headers=headers)
    ).status_code == 401
    digest = hmac.new(APP_SECRET.encode(), different_bytes, hashlib.sha256).hexdigest()
    headers["X-Hub-Signature-256"] = f"sha256={digest}"
    assert (
        await client.post("/webhook", content=different_bytes, headers=headers)
    ).status_code == 200


@pytest.mark.parametrize(
    ("token", "mode", "expected"),
    [
        (VERIFY_TOKEN, "subscribe", 200),
        ("wrong", "subscribe", 403),
        (VERIFY_TOKEN, "other", 403),
        ("", "subscribe", 403),
    ],
)
async def test_handshake(client: httpx.AsyncClient, token: str, mode: str, expected: int) -> None:
    response = await client.get(
        "/webhook",
        params={
            "hub.mode": mode,
            "hub.verify_token": token,
            "hub.challenge": "12345",
        },
    )
    assert response.status_code == expected
    if expected == 200:
        assert response.text == "12345"
        assert response.headers["content-type"].startswith("text/plain")


async def test_handshake_missing_challenge(client: httpx.AsyncClient) -> None:
    response = await client.get(
        "/webhook",
        params={
            "hub.mode": "subscribe",
            "hub.verify_token": VERIFY_TOKEN,
        },
    )
    assert response.status_code == 400


async def test_duplicate_delivery(client: httpx.AsyncClient, gateway: Wagent) -> None:
    payload = message_payload(message_id="wamid.duplicate")
    for _ in range(3):
        assert (await post(client, payload)).status_code == 200
    async with gateway.inbox.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(InboundMessage)) == 1
        assert await session.scalar(select(func.count()).select_from(Conversation)) == 1


async def test_concurrent_duplicate_delivery(client: httpx.AsyncClient, gateway: Wagent) -> None:
    payload = message_payload(message_id="wamid.concurrent")
    responses = await asyncio.gather(*(post(client, payload) for _ in range(3)))
    assert all(response.status_code == 200 for response in responses)
    async with gateway.inbox.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(InboundMessage)) == 1
        assert await session.scalar(select(func.count()).select_from(Conversation)) == 1


async def test_out_of_order_keeps_latest_window(client: httpx.AsyncClient, gateway: Wagent) -> None:
    first = datetime(2026, 11, 13, 10, 0, tzinfo=UTC)
    latest = first + timedelta(minutes=5)
    for text, timestamp in [("second", latest), ("first", first)]:
        assert (await post(client, message_payload(text, timestamp=timestamp))).status_code == 200
    async with gateway.inbox.sessions() as session:
        rows = (
            await session.scalars(
                select(InboundMessage).order_by(
                    InboundMessage.wa_timestamp, InboundMessage.received_at
                )
            )
        ).all()
        assert [row.text for row in rows] == ["first", "second"]
        conversation = (await session.scalars(select(Conversation))).one()
        assert conversation.last_inbound_at == latest


@pytest.mark.parametrize(
    "payload",
    [
        {},
        [],
        None,
        {"object": "other"},
        {"object": "whatsapp_business_account", "entry": [None, 1, {"changes": "other"}]},
        {
            "object": "whatsapp_business_account",
            "entry": [
                {
                    "changes": [
                        {
                            "field": "unknown",
                            "value": {"anything": True},
                        }
                    ]
                }
            ],
        },
        {
            "object": "whatsapp_business_account",
            "entry": [
                {
                    "changes": [
                        {
                            "field": "messages",
                            "value": {"statuses": [{"status": "delivered"}]},
                        }
                    ]
                }
            ],
        },
    ],
)
async def test_unknown_events_acknowledged(client: httpx.AsyncClient, payload: object) -> None:
    assert (await post(client, payload)).status_code == 200


async def test_invalid_json_authenticated_first(client: httpx.AsyncClient) -> None:
    body = b"{invalid"
    assert (await client.post("/webhook", content=body)).status_code == 401
    digest = hmac.new(APP_SECRET.encode(), body, hashlib.sha256).hexdigest()
    response = await client.post(
        "/webhook", content=body, headers={"X-Hub-Signature-256": f"sha256={digest}"}
    )
    assert response.status_code == 400


async def test_complete_envelope_and_malformed_items(
    client: httpx.AsyncClient, gateway: Wagent
) -> None:
    payload = message_payload("one", message_id="one")
    other = message_payload("two", message_id="two", wa_id="15557654321")
    value = payload["entry"][0]["changes"][0]["value"]
    value["messages"].extend(
        [
            None,
            {"id": "bad", "timestamp": "infinity", "from": "15551234567"},
            {
                "id": "media",
                "from": "15551234567",
                "timestamp": "1794564000",
                "type": "image",
                "image": {"id": "media-id", "mime_type": "image/jpeg"},
            },
            {"id": "future", "from": "15551234567", "timestamp": "1794564000", "type": "future"},
        ]
    )
    payload["entry"][0]["changes"].extend(other["entry"][0]["changes"])
    payload["entry"].extend(message_payload("three", message_id="three")["entry"])
    assert (await post(client, payload)).status_code == 200
    async with gateway.inbox.sessions() as session:
        rows = (await session.scalars(select(InboundMessage))).all()
        assert {row.wa_message_id for row in rows} == {"one", "two", "three", "media", "future"}
        media = next(row for row in rows if row.wa_message_id == "media")
        assert media.kind == "image" and media.media_id == "media-id"
        assert next(row for row in rows if row.wa_message_id == "future").kind == "other"
        assert await session.scalar(select(func.count()).select_from(Conversation)) == 2


async def test_unconfigured_phone_ignored(client: httpx.AsyncClient, gateway: Wagent) -> None:
    assert (await post(client, message_payload(phone_number_id="another-phone"))).status_code == 200
    async with gateway.inbox.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(InboundMessage)) == 0


async def test_fast_ack_with_no_background_agent(client: httpx.AsyncClient) -> None:
    for _ in range(3):
        body, headers = signed_payload(message_payload(), APP_SECRET)
        start = perf_counter()
        response = await client.post("/webhook", content=body, headers=headers)
        elapsed = perf_counter() - start
        assert response.status_code == 200
        assert elapsed < 0.2, f"webhook acknowledgment took {elapsed:.3f}s"


async def test_persists_after_restart(client: httpx.AsyncClient, settings: Settings) -> None:
    assert (await post(client, message_payload(message_id="restart"))).status_code == 200
    restarted = Wagent(settings=settings)
    try:
        async with restarted.inbox.sessions() as session:
            assert (await session.scalars(select(InboundMessage))).one().wa_message_id == "restart"
    finally:
        await restarted.engine.dispose()


async def test_storage_failure_is_retryable(
    client: httpx.AsyncClient, gateway: Wagent, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fail(_: Any, statuses: Any = ()) -> None:
        raise OperationalError("hidden", {}, Exception("hidden"))

    monkeypatch.setattr(gateway.inbox, "ingest", fail)
    response = await post(client, message_payload())
    assert response.status_code == 503
    assert response.json() == {"detail": "Inbox unavailable"}


async def test_health_and_readiness(client: httpx.AsyncClient) -> None:
    assert (await client.get("/healthz")).status_code == 200
    assert (await client.get("/readyz")).status_code == 200


async def test_batch_rolls_back_before_ack(client: httpx.AsyncClient, gateway: Wagent) -> None:
    from sqlalchemy import text

    async with gateway.engine.begin() as connection:
        await connection.execute(
            text(
                "CREATE TRIGGER fail_second BEFORE INSERT ON inbound_messages "
                "WHEN NEW.wa_message_id = 'second' "
                "BEGIN SELECT RAISE(ABORT, 'test storage failure'); END"
            )
        )
    payload = message_payload("one", message_id="first")
    second = message_payload("two", message_id="second")
    payload["entry"].extend(second["entry"])
    assert (await post(client, payload)).status_code == 503
    async with gateway.inbox.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(InboundMessage)) == 0
        assert await session.scalar(select(func.count()).select_from(Conversation)) == 0
    async with gateway.engine.begin() as connection:
        await connection.execute(text("DROP TRIGGER fail_second"))
    assert (await post(client, payload)).status_code == 200
    async with gateway.inbox.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(InboundMessage)) == 2
