"""Real SQLite checkpoint persistence and stable LangGraph message identities."""

import asyncio
from typing import Any

import httpx
from langchain_core.messages import HumanMessage
from pydantic import SecretStr
from sqlalchemy import select

from examples.clinic_booking.agent import build_graph
from wagent import Settings, Wagent
from wagent.gateway.events import parse_events
from wagent.runners import AgentReply, LangGraphRunner, TurnContext
from wagent.store.models import InboundMessage, OutboundMessage, utc_now
from wagent.testing.fake_graph_api import FakeGraphAPI
from wagent.testing.payloads import message_payload, signed_payload


async def test_memory_survives_application_restart(settings: Settings) -> None:
    graph = FakeGraphAPI()
    settings = settings.model_copy(
        update={"wa_token": SecretStr("test-token"), "worker_poll_seconds": 0.01}
    )
    factory_calls: list[bool] = []

    def factory(checkpointer: Any) -> Any:
        factory_calls.append(True)
        return build_graph(checkpointer)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=graph.app)) as api:
        for index, text in enumerate(["I am Ada", "What did I say?"], start=1):
            runner = LangGraphRunner(factory)
            app = Wagent(settings=settings, runner=runner, graph_client=api)
            async with app.fastapi_app.router.lifespan_context(app.fastapi_app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app.fastapi_app), base_url="http://test"
                ) as client:
                    body, headers = signed_payload(
                        message_payload(text, message_id=f"turn-{index}"), "local-app-secret"
                    )
                    assert (
                        await client.post("/webhook", content=body, headers=headers)
                    ).status_code == 200

                    async with asyncio.timeout(5):
                        for _ in range(500):
                            if len(graph.requests) == index:
                                async with app.inbox.sessions() as session:
                                    statuses = list(
                                        (
                                            await session.scalars(select(OutboundMessage.status))
                                        ).all()
                                    )
                                if len(statuses) == index and all(
                                    status == "sent" for status in statuses
                                ):
                                    break
                            await asyncio.sleep(0.01)
                    assert len(graph.requests) == index
                    assert f"visit message {index}" in graph.requests[-1]["body"]["text"]["body"]
                    state: Any = await runner.graph.aget_state(
                        {
                            "configurable": {"thread_id": "wa:123456789:15551234567"},
                        }
                    )
                    human = [
                        message
                        for message in state.values["messages"]
                        if isinstance(message, HumanMessage)
                    ]
                    assert len(human) == index
                    assert human[0].text == "I am Ada"
    assert len(factory_calls) == 2


async def test_retry_replaces_same_human_message_and_isolates_senders(gateway: Wagent) -> None:
    await gateway.inbox.ingest(
        parse_events(message_payload(message_id="same-input"), gateway.settings.wa_phone_id)
    )
    async with gateway.inbox.sessions() as session:
        rows = list((await session.scalars(select(InboundMessage))).all())
    ctx = TurnContext("test-thread", "test-user", rows, True, utc_now())
    runner = LangGraphRunner(build_graph)
    await runner.start(gateway.settings.database_url)
    try:
        first = await runner.run(ctx)
        second = await runner.run(ctx)
        assert first == second and "visit message 1" in (second.text or "")
        state: Any = await runner.graph.aget_state({"configurable": {"thread_id": ctx.thread_id}})
        humans = [
            message for message in state.values["messages"] if isinstance(message, HumanMessage)
        ]
        assert len(humans) == 1 and humans[0].id == ctx.turn_id
        other = TurnContext("other-thread", "other-user", rows, True, utc_now())
        assert "visit message 1" in ((await runner.run(other)).text or "")
    finally:
        await runner.aclose()


async def test_custom_mappers(gateway: Wagent) -> None:
    await gateway.inbox.ingest(parse_events(message_payload(), gateway.settings.wa_phone_id))
    async with gateway.inbox.sessions() as session:
        rows = list((await session.scalars(select(InboundMessage))).all())
    ctx = TurnContext("mapped", "test-user", rows, True, utc_now())
    runner = LangGraphRunner(
        build_graph,
        input_mapper=lambda ctx: {"messages": [HumanMessage(content="custom", id=ctx.turn_id)]},
        output_mapper=lambda _: AgentReply(text="mapped output"),
    )
    await runner.start(gateway.settings.database_url)
    try:
        assert (await runner.run(ctx)).text == "mapped output"
    finally:
        await runner.aclose()
