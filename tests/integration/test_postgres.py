"""Run the production database contract against CI's disposable PostgreSQL."""

import asyncio
import os
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from alembic import command
from alembic.config import Config
from pydantic import SecretStr
from sqlalchemy import func, select

from examples.clinic_booking.agent import build_graph
from wagent import Settings, Wagent
from wagent.gateway.events import parse_events, parse_statuses
from wagent.outbound.sender import Sender
from wagent.runners import EchoRunner, LangGraphRunner
from wagent.store.models import Conversation, InboundMessage, OutboundMessage, Usage
from wagent.testing.fake_graph_api import FakeGraphAPI
from wagent.testing.payloads import message_payload, signed_payload, status_payload
from wagent.worker.loop import Worker


async def test_postgres_ingestion(monkeypatch: pytest.MonkeyPatch) -> None:
    url = os.environ.get("WAGENT_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("set WAGENT_TEST_POSTGRES_URL to a dedicated test database; CI supplies one")
    monkeypatch.setenv("DATABASE_URL", url)
    await asyncio.to_thread(command.upgrade, Config("alembic.ini"), "head")
    settings = Settings(
        wa_phone_id="123456789",
        wa_app_secret=SecretStr("test-secret"),
        wa_verify_token=SecretStr("test-verify"),
        wa_token=SecretStr("test-token"),
        debounce_ms=0,
        database_url=url,
    )
    gateway = Wagent(settings=settings)
    async with gateway.fastapi_app.router.lifespan_context(gateway.fastapi_app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=gateway.fastapi_app), base_url="http://test"
        ) as client:
            assert (await client.get("/readyz")).status_code == 200
            now = datetime.now(UTC).replace(microsecond=0)
            payload = message_payload("latest", message_id="postgres-latest", timestamp=now)
            body, headers = signed_payload(payload, "test-secret")
            responses = await asyncio.gather(
                *(client.post("/webhook", content=body, headers=headers) for _ in range(10))
            )
            assert all(response.status_code == 200 for response in responses), [
                (response.status_code, response.text) for response in responses
            ]
            older = message_payload(
                "earlier", message_id="postgres-earlier", timestamp=now - timedelta(minutes=1)
            )
            body, headers = signed_payload(older, "test-secret")
            assert (await client.post("/webhook", content=body, headers=headers)).status_code == 200
        async with gateway.inbox.sessions() as session:
            assert await session.scalar(select(func.count()).select_from(Conversation)) == 1
            conversation = (await session.scalars(select(Conversation))).one()
            assert conversation.last_inbound_at == now
            messages = (
                await session.scalars(select(InboundMessage).order_by(InboundMessage.wa_timestamp))
            ).all()
            assert [message.text for message in messages] == ["earlier", "latest"]
            assert all(message.wa_timestamp.tzinfo == UTC for message in messages)

        assert await Worker(gateway.inbox, settings, EchoRunner()).run_once()
        graph = FakeGraphAPI()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=graph.app)) as api:
            sender = Sender(gateway.inbox, settings, api)
            assert await sender.run_once()
            assert graph.requests[0]["body"]["text"]["body"] == "earlier\nlatest"
            for index in range(1, 3):
                runner = LangGraphRunner(build_graph)
                await runner.start(url)
                try:
                    payload = message_payload(f"memory {index}", message_id=f"pg-memory-{index}")
                    await gateway.inbox.ingest(parse_events(payload, settings.wa_phone_id))
                    assert await Worker(gateway.inbox, settings, runner).run_once()
                    assert await sender.run_once()
                    assert f"visit message {index}" in graph.requests[-1]["body"]["text"]["body"]
                finally:
                    await runner.aclose()
            assert not await sender.run_once()

        async with gateway.inbox.sessions() as session:
            outbound = (
                await session.scalars(select(OutboundMessage).order_by(OutboundMessage.created_at))
            ).first()
            assert outbound is not None and outbound.wa_message_id is not None
            receipt = status_payload(
                outbound.wa_message_id,
                pricing={
                    "billable": True,
                    "category": "utility",
                    "pricing_model": "PMP",
                },
            )
        await gateway.inbox.ingest([], parse_statuses(receipt, settings.wa_phone_id))
        await gateway.inbox.ingest([], parse_statuses(receipt, settings.wa_phone_id))
        async with gateway.inbox.sessions() as session:
            assert (await session.scalars(select(Usage))).one().billable is True
        admin = Wagent(
            settings=settings.model_copy(update={"admin_api_key": SecretStr("pg-admin")})
        )
        async with admin.fastapi_app.router.lifespan_context(admin.fastapi_app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=admin.fastapi_app), base_url="http://test"
            ) as client:
                response = await client.get(
                    "/admin/usage", headers={"Authorization": "Bearer pg-admin"}
                )
                assert response.status_code == 200
                assert response.json()["items"][0]["count"] == 1

        from sqlalchemy import text

        from wagent.store.erasure import ErasureService

        stop = message_payload("STOP", message_id="postgres-stop")
        await gateway.inbox.ingest(parse_events(stop, settings.wa_phone_id))
        assert await Worker(gateway.inbox, settings, EchoRunner()).run_once()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=graph.app)) as api:
            assert await Sender(gateway.inbox, settings, api).run_once()
        assert "unsubscribed" in graph.requests[-1]["body"]["text"]["body"]
        assert await ErasureService(gateway.inbox, url).erase(conversation.id)
        async with gateway.inbox.sessions() as session:
            assert await session.scalar(select(func.count()).select_from(Conversation)) == 0
            assert await session.scalar(select(func.count()).select_from(Usage)) == 0
            for table in ["checkpoints", "checkpoint_blobs", "checkpoint_writes"]:
                count = await session.scalar(
                    text(f"SELECT count(*) FROM {table} WHERE thread_id = :thread_id"),
                    {"thread_id": conversation.thread_id},
                )
                assert count == 0
