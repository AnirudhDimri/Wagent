"""Signed webhook -> background worker -> agent -> fake Graph API."""

import asyncio
from time import perf_counter

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import select

from wagent import Settings, Wagent
from wagent.runners import AgentReply, EchoRunner, TurnContext
from wagent.store.models import InboundMessage, OutboundMessage
from wagent.testing.fake_graph_api import FakeGraphAPI
from wagent.testing.payloads import message_payload, signed_payload


async def wait_for_sends(graph: FakeGraphAPI, count: int) -> None:
    async with asyncio.timeout(3):
        for _ in range(300):
            if len(graph.requests) >= count:
                return
            await asyncio.sleep(0.01)
        raise AssertionError("No outbound request received")


async def test_signed_duplicate_webhooks_produce_one_reply(settings: Settings) -> None:
    class CountingEcho(EchoRunner):
        calls = 0

        async def run(self, ctx: TurnContext) -> AgentReply:
            self.calls += 1
            return await super().run(ctx)

    runner = CountingEcho()
    graph = FakeGraphAPI()
    settings = settings.model_copy(update={"wa_token": SecretStr("test-token")})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=graph.app)) as api:
        app = Wagent(settings=settings, runner=runner, graph_client=api)
        async with app.fastapi_app.router.lifespan_context(app.fastapi_app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app.fastapi_app), base_url="http://test"
            ) as client:
                body, headers = signed_payload(
                    message_payload("Hello 🌍", message_id="unique"), "local-app-secret"
                )
                for _ in range(3):
                    assert (
                        await client.post("/webhook", content=body, headers=headers)
                    ).status_code == 200
                await wait_for_sends(graph, 1)
                async with asyncio.timeout(2):
                    while True:
                        async with app.inbox.sessions() as session:
                            outbound = (await session.scalars(select(OutboundMessage))).one()
                            if outbound.status == "sent":
                                break
                        await asyncio.sleep(0.005)
                assert len(graph.requests) == 1
                assert runner.calls == 1
                assert graph.requests[0]["body"]["text"] == {"body": "Hello 🌍"}
                assert outbound.wa_message_id is not None
                async with app.inbox.sessions() as session:
                    assert (await session.scalars(select(InboundMessage))).one().status == "done"


class BlockingRunner:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def run(self, ctx: TurnContext) -> AgentReply:
        if ctx.messages[0].text == "slow":
            self.started.set()
            await self.release.wait()
        return AgentReply(text=ctx.messages[0].text)


async def test_slow_agent_does_not_block_webhook_or_other_sender(settings: Settings) -> None:
    graph = FakeGraphAPI()
    runner = BlockingRunner()
    settings = settings.model_copy(
        update={"wa_token": SecretStr("test-token"), "worker_poll_seconds": 0.01}
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=graph.app)) as api:
        app = Wagent(settings=settings, runner=runner, graph_client=api)
        async with app.fastapi_app.router.lifespan_context(app.fastapi_app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app.fastapi_app), base_url="http://test"
            ) as client:
                body, headers = signed_payload(message_payload("slow"), "local-app-secret")
                start = perf_counter()
                assert (
                    await client.post("/webhook", content=body, headers=headers)
                ).status_code == 200
                assert perf_counter() - start < 0.2
                await asyncio.wait_for(runner.started.wait(), timeout=2)
                body, headers = signed_payload(message_payload("after slow"), "local-app-secret")
                assert (
                    await client.post("/webhook", content=body, headers=headers)
                ).status_code == 200
                body, headers = signed_payload(
                    message_payload("fast", wa_id="15557654321"), "local-app-secret"
                )
                assert (
                    await client.post("/webhook", content=body, headers=headers)
                ).status_code == 200
                await wait_for_sends(graph, 1)
                assert graph.requests[0]["body"]["text"]["body"] == "fast"
                runner.release.set()
                await wait_for_sends(graph, 3)
                assert [request["body"]["text"]["body"] for request in graph.requests] == [
                    "fast",
                    "slow",
                    "after slow",
                ]


async def test_sqlite_rejects_second_active_worker(settings: Settings) -> None:
    settings = settings.model_copy(update={"wa_token": SecretStr("test-token")})
    graph = FakeGraphAPI()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=graph.app)) as api:
        first = Wagent(settings=settings, runner=EchoRunner(), graph_client=api)
        second = Wagent(settings=settings, runner=EchoRunner(), graph_client=api)
        async with first.fastapi_app.router.lifespan_context(first.fastapi_app):
            with pytest.raises(RuntimeError, match="one wagent worker"):
                async with second.fastapi_app.router.lifespan_context(second.fastapi_app):
                    pass
