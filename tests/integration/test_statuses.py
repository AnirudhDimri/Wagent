"""Signed delivery receipts, status/HTTP races, pricing, and deduplication."""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import func, select

from wagent import Wagent
from wagent.gateway.events import parse_events
from wagent.outbound.sender import Sender
from wagent.runners import EchoRunner
from wagent.store.models import MessageStatusEvent, OutboundMessage, Usage
from wagent.testing.payloads import message_payload, signed_payload, status_payload
from wagent.worker.loop import Worker

PRICE: dict[str, Any] = {
    "billable": True,
    "pricing_model": "PMP",
    "category": "utility",
    "type": "regular",
    "future_field": {"kept": True},
}


async def enqueue(gateway: Wagent) -> OutboundMessage:
    await gateway.inbox.ingest(parse_events(message_payload("hello"), gateway.settings.wa_phone_id))
    assert await Worker(gateway.inbox, gateway.settings, EchoRunner()).run_once()
    async with gateway.inbox.sessions() as session:
        return (await session.scalars(select(OutboundMessage))).one()


async def post(client: httpx.AsyncClient, payload: object) -> None:
    body, headers = signed_payload(payload, "local-app-secret")
    assert (await client.post("/webhook", content=body, headers=headers)).status_code == 200


async def test_statuses_and_pricing_are_idempotent(
    client: httpx.AsyncClient, gateway: Wagent
) -> None:
    row = await enqueue(gateway)
    when = datetime(2026, 11, 14, 12, tzinfo=UTC)
    delivered = status_payload("wamid.status", tracker=str(row.id), pricing=PRICE, timestamp=when)
    await post(client, delivered)
    async with gateway.inbox.sessions() as session:
        usage = (await session.scalars(select(Usage))).one()
        original_id, original_time = usage.id, usage.recorded_at
        assert usage.pricing == PRICE and usage.billable is True
        assert (
            usage.pricing_model == "PMP" and usage.category == "utility" and usage.type == "regular"
        )
    await post(client, delivered)
    read = status_payload("wamid.status", "read", pricing=PRICE, timestamp=when + timedelta(days=1))
    await post(client, read)
    await post(client, read)
    # A late sent receipt must not regress read or create another billable message.
    await post(
        client, status_payload("wamid.status", "sent", timestamp=when - timedelta(seconds=1))
    )
    async with gateway.inbox.sessions() as session:
        outbound = (await session.scalars(select(OutboundMessage))).one()
        assert outbound.status == "read" and outbound.wa_message_id == "wamid.status"
        usage = (await session.scalars(select(Usage))).one()
        assert usage.id == original_id and usage.recorded_at == original_time
        assert await session.scalar(select(func.count()).select_from(MessageStatusEvent)) == 3


async def test_failed_status_records_code_and_never_auto_retries(
    client: httpx.AsyncClient,
    gateway: Wagent,
) -> None:
    row = await enqueue(gateway)
    await post(
        client, status_payload("wamid.failed", "failed", tracker=str(row.id), error_code=131056)
    )
    async with gateway.inbox.sessions() as session:
        row = (await session.scalars(select(OutboundMessage))).one()
        assert row.status == "failed" and row.error_code == "131056"
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: pytest.fail("failed webhook must not trigger a resend")
        )
    ) as api:
        assert not await Sender(gateway.inbox, gateway.settings, api).run_once()


async def test_unknown_send_resolves_using_callback_tracker(
    client: httpx.AsyncClient,
    gateway: Wagent,
) -> None:
    row = await enqueue(gateway)
    settings = gateway.settings.model_copy(update={"wa_token": SecretStr("test-token")})

    async def timeout(_: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("request written, response lost")

    async with httpx.AsyncClient(transport=httpx.MockTransport(timeout)) as api:
        assert await Sender(gateway.inbox, settings, api).run_once()
    async with gateway.inbox.sessions() as session:
        saved = await session.get(OutboundMessage, row.id)
        assert saved is not None and saved.status == "unknown" and saved.wa_message_id is None
    await post(client, status_payload("wamid.resolved", tracker=str(row.id), pricing=PRICE))
    async with gateway.inbox.sessions() as session:
        saved = await session.get(OutboundMessage, row.id)
        assert saved is not None and saved.status == "delivered" and saved.error_code is None
        assert saved.wa_message_id == "wamid.resolved"
        assert (await session.scalars(select(Usage))).one().outbound_id == row.id


@pytest.mark.parametrize("use_tracker", [True, False])
async def test_receipt_before_http_response_is_not_lost_or_regressed(
    client: httpx.AsyncClient,
    gateway: Wagent,
    use_tracker: bool,
) -> None:
    row = await enqueue(gateway)
    settings = gateway.settings.model_copy(update={"wa_token": SecretStr("test-token")})

    async def transport(_: httpx.Request) -> httpx.Response:
        await post(
            client,
            status_payload(
                "wamid.race", tracker=str(row.id) if use_tracker else None, pricing=PRICE
            ),
        )
        return httpx.Response(200, json={"messages": [{"id": "wamid.race"}]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as api:
        assert await Sender(gateway.inbox, settings, api).run_once()
    async with gateway.inbox.sessions() as session:
        assert (await session.scalars(select(OutboundMessage))).one().status == "delivered"
        assert (await session.scalars(select(MessageStatusEvent))).one().applied is True
        assert (await session.scalars(select(Usage))).one().pricing == PRICE


async def test_unmatched_status_is_durable_without_counting_usage(
    client: httpx.AsyncClient,
    gateway: Wagent,
) -> None:
    await post(client, status_payload("wamid.external", pricing=PRICE))
    async with gateway.inbox.sessions() as session:
        receipt = (await session.scalars(select(MessageStatusEvent))).one()
        assert receipt.applied is False and receipt.pricing == PRICE
        assert await session.scalar(select(func.count()).select_from(Usage)) == 0


async def test_absent_billable_is_not_inferred(client: httpx.AsyncClient, gateway: Wagent) -> None:
    row = await enqueue(gateway)
    await post(
        client,
        status_payload(
            "wamid.unknown-pricing",
            tracker=str(row.id),
            pricing={"pricing_model": "PMP", "type": "regular"},
        ),
    )
    async with gateway.inbox.sessions() as session:
        usage = (await session.scalars(select(Usage))).one()
        assert usage.billable is None and usage.category is None
        assert usage.type == "regular"


async def test_concurrent_duplicate_receipts_count_one_message(
    client: httpx.AsyncClient,
    gateway: Wagent,
) -> None:
    row = await enqueue(gateway)
    payload = status_payload("wamid.concurrent-status", tracker=str(row.id), pricing=PRICE)
    await asyncio.gather(*(post(client, payload) for _ in range(3)))
    async with gateway.inbox.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Usage)) == 1
        assert await session.scalar(select(func.count()).select_from(MessageStatusEvent)) == 1


async def test_out_of_order_pricing_and_failure_preserve_delivery_evidence(
    client: httpx.AsyncClient,
    gateway: Wagent,
) -> None:
    row = await enqueue(gateway)
    when = datetime(2026, 11, 14, tzinfo=UTC)
    await post(
        client,
        status_payload(
            "wamid.order",
            "read",
            tracker=str(row.id),
            timestamp=when,
            pricing={"billable": True, "category": "utility"},
        ),
    )
    await post(
        client,
        status_payload(
            "wamid.order",
            "sent",
            timestamp=when - timedelta(seconds=5),
            pricing={"billable": False, "category": "marketing"},
        ),
    )
    await post(client, status_payload("wamid.order", "failed", error_code=131000, timestamp=when))
    async with gateway.inbox.sessions() as session:
        assert (await session.scalars(select(OutboundMessage))).one().status == "read"
        usage = (await session.scalars(select(Usage))).one()
        assert usage.billable is True and usage.category == "utility"


async def test_wrong_phone_and_unknown_status_ignored(
    client: httpx.AsyncClient, gateway: Wagent
) -> None:
    row = await enqueue(gateway)
    await post(client, status_payload("wamid.other", tracker=str(row.id), phone_number_id="other"))
    await post(client, status_payload("wamid.future", "future", tracker=str(row.id)))
    async with gateway.inbox.sessions() as session:
        assert (await session.scalars(select(OutboundMessage))).one().status == "queued"
        assert await session.scalar(select(func.count()).select_from(MessageStatusEvent)) == 0


async def test_later_tracker_also_reconciles_earlier_unmatched_pricing(
    client: httpx.AsyncClient,
    gateway: Wagent,
) -> None:
    row = await enqueue(gateway)
    await post(client, status_payload("wamid.early", "sent", pricing=PRICE))
    await post(client, status_payload("wamid.early", "read", tracker=str(row.id)))
    async with gateway.inbox.sessions() as session:
        assert (await session.scalars(select(Usage))).one().pricing == PRICE
        assert all(
            receipt.applied for receipt in (await session.scalars(select(MessageStatusEvent))).all()
        )
        assert (await session.scalars(select(OutboundMessage))).one().status == "read"
