"""A tiny LangGraph agent with a fake model; no external LLM or booking API."""

from typing import Any

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, MessagesState, StateGraph  # pyright: ignore[reportMissingTypeStubs]


def fake_model(state: MessagesState) -> dict[str, list[AIMessage]]:
    messages = [message for message in state["messages"] if isinstance(message, HumanMessage)]
    latest = messages[-1]
    return {
        "messages": [
            AIMessage(
                content=f"Clinic assistant: {latest.text} (visit message {len(messages)})",
                id=f"reply:{latest.id}",
            )
        ]
    }


def build_graph(checkpointer: BaseCheckpointSaver[Any]) -> Any:
    graph: Any = StateGraph(MessagesState)
    graph.add_node("assistant", fake_model)
    graph.add_edge(START, "assistant")
    graph.add_edge("assistant", END)
    return graph.compile(checkpointer=checkpointer)
