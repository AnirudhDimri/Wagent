"""Periodically remove old raw inbound payloads without deleting message history."""

import asyncio
import logging
from datetime import timedelta

from sqlalchemy import null, update

from wagent.settings import Settings
from wagent.store.models import InboundMessage, utc_now
from wagent.store.repo import InboxRepository

logger = logging.getLogger(__name__)


class RetentionJob:
    def __init__(self, inbox: InboxRepository, settings: Settings) -> None:
        self.inbox = inbox
        self.settings = settings

    async def run_once(self) -> None:
        async with self.inbox.sessions.begin() as session:
            await session.execute(
                update(InboundMessage)
                .where(
                    InboundMessage.received_at
                    < utc_now() - timedelta(days=self.settings.retention_days),
                    InboundMessage.raw.is_not(None),
                )
                .values(raw=null())
            )

    async def serve(self) -> None:
        while True:
            try:
                await self.run_once()
            except Exception:
                logger.error("Raw-payload retention failed; will retry")
            await asyncio.sleep(self.settings.retention_interval_seconds)
