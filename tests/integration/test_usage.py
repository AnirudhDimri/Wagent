"""Authenticated UTC usage aggregation counts messages, not repeated receipts."""

from datetime import UTC, datetime
from typing import Any

import httpx
from pydantic import SecretStr
from sqlalchemy import select

from wagent import Settings, Wagent
from wagent.gateway.events import parse_events, parse_statuses
from wagent.runners import EchoRunner
from wagent.store.models import OutboundMessage
from wagent.testing.payloads import message_payload, status_payload
from wagent.worker.loop import Worker


async def add_usage(
    gateway: Wagent, index: int, timestamp: datetime, pricing: dict[str, Any]
) -> None:
    await gateway.inbox.ingest(
        parse_events(message_payload(message_id=f"usage-{index}"), gateway.settings.wa_phone_id)
    )
    assert await Worker(gateway.inbox, gateway.settings, EchoRunner()).run_once()
    async with gateway.inbox.sessions() as session:
        row = (
            await session.scalars(
                select(OutboundMessage).order_by(OutboundMessage.created_at.desc())
            )
        ).first()
        assert row is not None
        tracker = str(row.id)
    payload = status_payload(
        f"wamid.usage.{index}", tracker=tracker, timestamp=timestamp, pricing=pricing
    )
    await gateway.inbox.ingest([], parse_statuses(payload, gateway.settings.wa_phone_id))
    await gateway.inbox.ingest([], parse_statuses(payload, gateway.settings.wa_phone_id))


async def test_usage_auth_grouping_and_date_boundaries(gateway: Wagent, settings: Settings) -> None:
    await add_usage(
        gateway,
        1,
        datetime(2026, 11, 14, 12, tzinfo=UTC),
        {"billable": False, "category": "utility", "pricing_model": "PMP"},
    )
    await add_usage(
        gateway,
        2,
        datetime(2026, 11, 15, 12, tzinfo=UTC),
        {"billable": True, "category": "marketing"},
    )
    await add_usage(gateway, 3, datetime(2026, 11, 14, 13, tzinfo=UTC), {"type": "regular"})
    app = Wagent(settings=settings.model_copy(update={"admin_api_key": SecretStr("admin-test")}))
    async with app.fastapi_app.router.lifespan_context(app.fastapi_app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app.fastapi_app), base_url="http://test"
        ) as client:
            assert (await client.get("/admin/usage")).status_code == 401
            assert (
                await client.get("/admin/usage", headers={"Authorization": "Bearer wrong"})
            ).status_code == 401
            client.headers["Authorization"] = "Bearer admin-test"
            response = await client.get("/admin/usage")
            assert response.status_code == 200
            items = response.json()["items"]
            assert sum(item["count"] for item in items) == 3
            assert {item["billable"] for item in items} == {True, False, None}
            response = await client.get(
                "/admin/usage", params={"from": "2026-11-14", "to": "2026-11-15"}
            )
            assert sum(item["count"] for item in response.json()["items"]) == 2
            assert all(item["day"] == "2026-11-14" for item in response.json()["items"])
            response = await client.get(
                "/admin/usage",
                params={
                    "from": "2026-11-14T14:00:00+02:00",
                    "to": "2026-11-14T13:00:00Z",
                },
            )
            assert response.json()["items"] == [
                {"day": "2026-11-14", "category": "utility", "billable": False, "count": 1}
            ]
            for params in [
                {"from": "invalid"},
                {"from": "2026-11-16", "to": "2026-11-14"},
                {"from": "2026-11-14T12:00:00"},
            ]:
                assert (await client.get("/admin/usage", params=params)).status_code == 422


async def test_admin_usage_disabled_without_key(settings: Settings) -> None:
    app = Wagent(settings=settings.model_copy(update={"admin_api_key": None}))
    async with app.fastapi_app.router.lifespan_context(app.fastapi_app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app.fastapi_app), base_url="http://test"
        ) as client:
            assert (await client.get("/admin/usage")).status_code == 404
