"""Bounded asynchronous processing, separate from webhook requests."""

import asyncio
import logging
from time import perf_counter

from wagent.logging import hash_phone
from wagent.outbound.window import window_open
from wagent.runners.base import AgentRunner, TurnContext
from wagent.settings import Settings
from wagent.store.models import utc_now
from wagent.store.repo import InboxRepository
from wagent.worker.queue import TurnQueue

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, inbox: InboxRepository, settings: Settings, runner: AgentRunner) -> None:
        self.queue = TurnQueue(inbox, settings)
        self.settings = settings
        self.runner = runner

    async def run_once(self) -> bool:
        claim = await self.queue.claim(utc_now())
        if claim is None:
            return False
        if not await self.queue.preflight(claim, utc_now()):
            return True
        if self.settings.log_salt and self.settings.log_salt.get_secret_value():
            logger.info(
                "Running agent for %s",
                hash_phone(claim.conversation.wa_id, self.settings.log_salt.get_secret_value()),
            )
        start = perf_counter()
        now = utc_now()
        ctx = TurnContext(
            thread_id=claim.conversation.thread_id,
            wa_id=claim.conversation.wa_id,
            messages=claim.messages,
            window_open=window_open(claim.conversation.last_inbound_at, now),
            now=now,
        )
        try:
            async with asyncio.timeout(self.settings.runner_timeout_seconds):
                reply = await self.runner.run(ctx)
            await self.queue.complete(claim, reply, utc_now(), int((perf_counter() - start) * 1000))
        except Exception as error:
            await self.queue.fail(claim, error, utc_now())
        # Cancellation/crashes deliberately leave processing rows behind the lease.
        return True

    async def serve_slot(self) -> None:
        while True:
            try:
                worked = await self.run_once()
            except Exception:
                logger.error("Worker iteration failed; retrying after poll interval")
                worked = False
            if not worked:
                await asyncio.sleep(self.settings.worker_poll_seconds)

    async def serve(self) -> None:
        async with asyncio.TaskGroup() as group:
            for _ in range(self.settings.worker_concurrency):
                group.create_task(self.serve_slot())
