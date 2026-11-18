"""Persistent rolling turn limits and UTC-day outbound reservations."""

from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from wagent.store.models import OutboundMessage, Turn


async def turn_limit_exceeded(
    session: AsyncSession, conversation_id: UUID, turn_id: str, now: datetime, limit: int
) -> bool:
    turn = await session.get(Turn, turn_id)
    if turn is not None and turn.agent_attempted:
        return False  # A retry remains the same admitted logical turn.
    count = await session.scalar(
        select(func.count())
        .select_from(Turn)
        .where(
            Turn.conversation_id == conversation_id,
            Turn.agent_attempted.is_(True),
            Turn.id != turn_id,
            Turn.started_at > now - timedelta(minutes=1),
        )
    )
    return (count or 0) >= limit


async def reserve_outbound(
    session: AsyncSession, row: OutboundMessage, now: datetime, limit: int
) -> bool:
    if row.purpose == "opt_out":
        return True
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if row.cap_reserved_at is not None and day <= row.cap_reserved_at < day + timedelta(days=1):
        return True
    count = await session.scalar(
        select(func.count())
        .select_from(OutboundMessage)
        .where(
            OutboundMessage.conversation_id == row.conversation_id,
            OutboundMessage.cap_reserved_at >= day,
            OutboundMessage.cap_reserved_at < day + timedelta(days=1),
        )
    )
    if (count or 0) >= limit:
        return False
    row.cap_reserved_at = now
    return True
