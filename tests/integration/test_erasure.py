"""Erasure removes application data and real SQLite LangGraph checkpoints."""

import asyncio
from uuid import uuid4

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError
from typer.testing import CliRunner

from examples.clinic_booking.agent import build_graph
from wagent import Settings, Wagent
from wagent.cli import app as cli
from wagent.gateway.events import parse_events, parse_statuses
from wagent.runners import EchoRunner, LangGraphRunner
from wagent.store.erasure import ConversationBusy, ErasureService
from wagent.store.models import (
    AuditEvent,
    Conversation,
    InboundMessage,
    MessageStatusEvent,
    OutboundMessage,
    Turn,
    Usage,
    utc_now,
)
from wagent.testing.payloads import message_payload, status_payload
from wagent.worker.loop import Worker
from wagent.worker.queue import TurnQueue


async def populate(gateway: Wagent, runner: LangGraphRunner | None = None) -> Conversation:
    await gateway.inbox.ingest(
        parse_events(message_payload("remember this"), gateway.settings.wa_phone_id)
    )
    assert await Worker(gateway.inbox, gateway.settings, runner or EchoRunner()).run_once()
    async with gateway.inbox.sessions() as session:
        conversation = (await session.scalars(select(Conversation))).one()
        outgoing = (await session.scalars(select(OutboundMessage))).one()
        receipt = status_payload(
            "wamid.erase",
            tracker=str(outgoing.id),
            pricing={"billable": False, "category": "service"},
        )
    await gateway.inbox.ingest([], parse_statuses(receipt, gateway.settings.wa_phone_id))
    async with gateway.inbox.sessions.begin() as session:
        session.add(
            AuditEvent(
                conversation_id=conversation.id, type="test", actor="test", at=utc_now(), meta={}
            )
        )
    return conversation


async def assert_empty(gateway: Wagent) -> None:
    async with gateway.inbox.sessions() as session:
        for model in [
            Conversation,
            InboundMessage,
            Turn,
            OutboundMessage,
            Usage,
            AuditEvent,
            MessageStatusEvent,
        ]:
            assert await session.scalar(select(func.count()).select_from(model)) == 0


async def test_admin_erases_rows_and_real_checkpoint_thread(
    gateway: Wagent, settings: Settings
) -> None:
    runner = LangGraphRunner(build_graph)
    await runner.start(settings.database_url)
    try:
        conversation = await populate(gateway, runner)
        async with gateway.inbox.sessions() as session:
            assert (
                await session.execute(text("SELECT count(*) FROM checkpoints"))
            ).scalar_one() > 0
        admin = Wagent(
            settings=settings.model_copy(update={"admin_api_key": SecretStr("admin-test")})
        )
        async with admin.fastapi_app.router.lifespan_context(admin.fastapi_app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=admin.fastapi_app), base_url="http://test"
            ) as client:
                path = f"/admin/conversations/{conversation.id}"
                assert (await client.delete(path)).status_code == 401
                client.headers["Authorization"] = "Bearer admin-test"
                assert (await client.delete(path)).status_code == 204
                assert (await client.delete(path)).status_code == 404
                assert (await client.delete(f"/admin/conversations/{uuid4()}")).status_code == 404
        await assert_empty(gateway)
        async with gateway.inbox.sessions() as session:
            for table in ["checkpoints", "writes"]:
                assert (
                    await session.execute(text(f"SELECT count(*) FROM {table}"))
                ).scalar_one() == 0
        state = await runner.graph.aget_state(
            {"configurable": {"thread_id": conversation.thread_id}}
        )
        assert state.values == {}
    finally:
        await runner.aclose()


async def test_live_worker_is_not_erased(gateway: Wagent) -> None:
    await gateway.inbox.ingest(parse_events(message_payload(), gateway.settings.wa_phone_id))
    claim = await TurnQueue(gateway.inbox, gateway.settings).claim(utc_now())
    assert claim is not None
    with pytest.raises(ConversationBusy):
        await ErasureService(gateway.inbox, gateway.settings.database_url).erase(
            claim.conversation.id
        )
    async with gateway.inbox.sessions() as session:
        row = await session.get(Conversation, claim.conversation.id)
        assert row is not None and not row.erasing


async def test_failed_checkpoint_erasure_is_fenced_and_retryable(gateway: Wagent) -> None:
    conversation = await populate(gateway)
    service = ErasureService(gateway.inbox, gateway.settings.database_url)
    original = service.erase_thread

    async def failure(thread_id: str) -> None:
        raise RuntimeError("checkpoint store unavailable")

    service.erase_thread = failure
    with pytest.raises(RuntimeError):
        await service.erase(conversation.id)
    async with gateway.inbox.sessions() as session:
        row = await session.get(Conversation, conversation.id)
        assert row is not None and row.erasing and row.lease_owner is None
    assert await TurnQueue(gateway.inbox, gateway.settings).claim(utc_now()) is None
    with pytest.raises(SQLAlchemyError):
        await gateway.inbox.ingest(
            parse_events(message_payload("retry later"), gateway.settings.wa_phone_id)
        )
    service.erase_thread = original
    assert await service.erase(conversation.id)
    await assert_empty(gateway)


async def test_concurrent_erasure_cannot_delete_a_recreated_thread(gateway: Wagent) -> None:
    conversation = await populate(gateway)
    service = ErasureService(gateway.inbox, gateway.settings.database_url)
    entered, release = asyncio.Event(), asyncio.Event()

    async def blocked(thread_id: str) -> None:
        entered.set()
        await release.wait()

    service.erase_thread = blocked
    task = asyncio.create_task(service.erase(conversation.id))
    try:
        await asyncio.wait_for(entered.wait(), timeout=2)
        with pytest.raises(ConversationBusy):
            await ErasureService(gateway.inbox, gateway.settings.database_url).erase(
                conversation.id
            )
    finally:
        release.set()
        assert await task
    await assert_empty(gateway)


async def test_cli_purge_needs_no_meta_credentials(
    gateway: Wagent, monkeypatch: pytest.MonkeyPatch
) -> None:
    await populate(gateway)
    for name in ["WA_APP_SECRET", "WA_VERIFY_TOKEN", "WA_TOKEN"]:
        monkeypatch.delenv(name, raising=False)
    result = await asyncio.to_thread(CliRunner().invoke, cli, ["purge", "--wa-id", "15551234567"])
    assert result.exit_code == 0, result.output
    assert result.output.strip() == "Erased 1 conversation(s)."
    assert "15551234567" not in result.output
    await assert_empty(gateway)


async def test_live_send_blocks_erasure_until_its_lease_expires(gateway: Wagent) -> None:
    from datetime import timedelta

    from sqlalchemy import update

    conversation = await populate(gateway)
    async with gateway.inbox.sessions.begin() as session:
        await session.execute(
            update(OutboundMessage).values(status="sending", sending_at=utc_now())
        )
    service = ErasureService(gateway.inbox, gateway.settings.database_url)
    with pytest.raises(ConversationBusy):
        await service.erase(conversation.id)
    async with gateway.inbox.sessions.begin() as session:
        await session.execute(
            update(OutboundMessage).values(sending_at=utc_now() - timedelta(seconds=121))
        )
    assert await service.erase(conversation.id)
    await assert_empty(gateway)


async def test_failed_erasure_api_returns_retryable_error_without_private_data(
    gateway: Wagent,
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conversation = await populate(gateway)

    async def fail(self: ErasureService, thread_id: str) -> None:
        raise RuntimeError("15551234567 secret-test-token")

    monkeypatch.setattr(ErasureService, "erase_thread", fail)
    admin = Wagent(settings=settings.model_copy(update={"admin_api_key": SecretStr("admin-test")}))
    async with admin.fastapi_app.router.lifespan_context(admin.fastapi_app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=admin.fastapi_app), base_url="http://test"
        ) as client:
            response = await client.delete(
                f"/admin/conversations/{conversation.id}",
                headers={"Authorization": "Bearer admin-test"},
            )
            assert response.status_code == 503
            assert "15551234567" not in response.text and "secret-test-token" not in response.text
    async with gateway.inbox.sessions() as session:
        row = await session.get(Conversation, conversation.id)
        assert row is not None and row.erasing


async def test_erasure_marker_does_not_block_other_conversations(gateway: Wagent) -> None:
    from sqlalchemy import update

    from wagent.outbound.sender import Sender
    from wagent.testing.fake_graph_api import FakeGraphAPI

    await gateway.inbox.ingest(parse_events(message_payload("first"), gateway.settings.wa_phone_id))
    assert await Worker(gateway.inbox, gateway.settings, EchoRunner()).run_once()
    async with gateway.inbox.sessions.begin() as session:
        await session.execute(update(Conversation).values(erasing=True))
    await gateway.inbox.ingest(
        parse_events(message_payload("second", wa_id="15557654321"), gateway.settings.wa_phone_id)
    )
    assert await Worker(gateway.inbox, gateway.settings, EchoRunner()).run_once()
    graph = FakeGraphAPI()
    settings = gateway.settings.model_copy(update={"wa_token": SecretStr("test-token")})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=graph.app)) as api:
        assert await Sender(gateway.inbox, settings, api).run_once()
    assert len(graph.requests) == 1 and graph.requests[0]["body"]["text"]["body"] == "second"
