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

from wagent import Settings, Wagent
from wagent.store.models import Conversation, InboundMessage
from wagent.testing.payloads import message_payload, signed_payload


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
                *(client.post("/webhook", content=body, headers=headers) for _ in range(3))
            )
            assert all(response.status_code == 200 for response in responses)
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
