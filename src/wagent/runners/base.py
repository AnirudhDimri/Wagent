"""Public runner contract, independent of any LLM framework."""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from wagent.store.models import InboundMessage


def turn_id_for(messages: list[InboundMessage]) -> str:
    encoded = json.dumps(
        sorted(message.wa_message_id for message in messages), separators=(",", ":")
    )
    return hashlib.sha256(encoded.encode()).hexdigest()


@dataclass
class Button:
    id: str
    title: str


@dataclass
class TurnContext:
    thread_id: str
    wa_id: str = field(repr=False)
    messages: list[InboundMessage] = field(repr=False)
    window_open: bool
    now: datetime

    @property
    def turn_id(self) -> str:
        return turn_id_for(self.messages)


@dataclass
class AgentReply:
    text: str | None = None
    buttons: list[Button] | None = None
    handoff: bool = False
    meta: dict[str, Any] = field(default_factory=lambda: dict[str, Any]())


class AgentRunner(Protocol):
    async def run(self, ctx: TurnContext) -> AgentReply: ...
