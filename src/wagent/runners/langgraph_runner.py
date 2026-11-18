"""Run a user-supplied graph with persistent checkpoints and stable message IDs."""

import inspect
from collections.abc import Callable
from contextlib import AsyncExitStack
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from sqlalchemy.engine import make_url

from wagent.runners.base import AgentReply, TurnContext

GraphFactory = Callable[[BaseCheckpointSaver[Any]], Any]
InputMapper = Callable[[TurnContext], dict[str, Any]]
OutputMapper = Callable[[Any], AgentReply]


class LangGraphRunner:
    def __init__(
        self,
        graph_factory: GraphFactory,
        input_mapper: InputMapper | None = None,
        output_mapper: OutputMapper | None = None,
        escalation_tool: bool = False,
    ) -> None:
        if escalation_tool:
            raise ValueError("escalation_tool is available in M5, not M2")
        self.graph_factory = graph_factory
        self.input_mapper = input_mapper
        self.output_mapper = output_mapper
        self.graph: Any = None
        self._stack = AsyncExitStack()

    async def start(self, database_url: str) -> None:
        if self.graph is not None:
            raise RuntimeError("runner is already started")
        url = make_url(database_url)
        try:
            if url.drivername == "sqlite+aiosqlite":
                saver = await self._stack.enter_async_context(
                    AsyncSqliteSaver.from_conn_string(url.database or ":memory:")
                )
            else:
                from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

                saver = await self._stack.enter_async_context(
                    AsyncPostgresSaver.from_conn_string(
                        url.set(drivername="postgresql").render_as_string(hide_password=False)
                    )
                )
            await saver.setup()
            graph = self.graph_factory(saver)
            self.graph = await graph if inspect.isawaitable(graph) else graph
            if not callable(getattr(self.graph, "ainvoke", None)):
                self.graph = None
                raise TypeError("graph_factory must return a compiled graph with ainvoke")
        except BaseException:
            await self._stack.aclose()
            raise

    async def aclose(self) -> None:
        self.graph = None
        await self._stack.aclose()

    async def run(self, ctx: TurnContext) -> AgentReply:
        if self.graph is None:
            raise RuntimeError("LangGraphRunner must be started before use")
        inputs = (
            self.input_mapper(ctx)
            if self.input_mapper
            else {
                "messages": [
                    HumanMessage(
                        content="\n".join(
                            message.text or f"[{message.kind}]" for message in ctx.messages
                        ),
                        id=ctx.turn_id,
                    )
                ]
            }
        )
        result: Any = await self.graph.ainvoke(
            inputs, {"configurable": {"thread_id": ctx.thread_id}}
        )
        if self.output_mapper:
            return self.output_mapper(result)
        for message in reversed(result.get("messages", [])):
            if isinstance(message, AIMessage):
                return AgentReply(text=message.text or None)
        return AgentReply()
