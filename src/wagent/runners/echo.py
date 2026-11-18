"""Deterministic runner for offline development."""

from wagent.runners.base import AgentReply, TurnContext


class EchoRunner:
    async def run(self, ctx: TurnContext) -> AgentReply:
        return AgentReply(text="\n".join(message.text or "" for message in ctx.messages) or None)
