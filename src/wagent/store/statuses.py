"""Durable status receipts, monotonic state updates, and per-message pricing."""

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from sqlalchemy import or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from wagent.gateway.events import StatusEvent
from wagent.store.models import Conversation, MessageStatusEvent, OutboundMessage, Usage

RANK = {"queued": 0, "sending": 0, "unknown": 0, "sent": 1, "failed": 2, "delivered": 3, "read": 4}


async def _apply(session: AsyncSession, receipt: MessageStatusEvent) -> UUID | None:
    if receipt.applied:
        return None
    try:
        tracker = UUID(receipt.tracker) if receipt.tracker else None
    except ValueError:
        tracker = None
    matching = (
        select(OutboundMessage.id)
        .join(Conversation)
        .where(
            Conversation.phone_number_id == receipt.phone_number_id,
            Conversation.erasing.is_(False),
            (
                OutboundMessage.id == tracker
                if tracker is not None
                else OutboundMessage.wa_message_id == receipt.wa_message_id
            ),
            or_(
                OutboundMessage.wa_message_id.is_(None),
                OutboundMessage.wa_message_id == receipt.wa_message_id,
            ),
        )
    )
    # This conditional write fences sender/status races on both database backends.
    row = await session.scalar(
        update(OutboundMessage)
        .where(OutboundMessage.id.in_(matching))
        .values(status=OutboundMessage.status)
        .returning(OutboundMessage)
    )
    if row is None:
        return None
    row.wa_message_id = receipt.wa_message_id
    if RANK[receipt.status] >= RANK.get(row.status, 0):
        row.status = receipt.status
        row.error_code = receipt.error_code if receipt.status == "failed" else None
    if receipt.status != "failed" and row.sent_at is None:
        row.sent_at = receipt.wa_timestamp
    row.next_attempt_at = None
    if receipt.pricing is not None:
        usage = await session.scalar(select(Usage).where(Usage.outbound_id == row.id))
        if usage is None:
            usage = Usage(
                outbound_id=row.id,
                pricing={},
                recorded_at=receipt.wa_timestamp,
                pricing_at=receipt.wa_timestamp,
            )
            session.add(usage)
        newest = receipt.wa_timestamp >= usage.pricing_at
        raw = receipt.pricing
        fields: dict[str, Any] = {
            "billable": raw.get("billable"),
            "pricing_model": raw.get("pricing_model"),
            "category": raw.get("category"),
            "type": raw.get("type", raw.get("pricing_type")),
        }
        for name, value in fields.items():
            valid = isinstance(value, bool) if name == "billable" else isinstance(value, str)
            if valid and (newest or getattr(usage, name) is None):
                setattr(usage, name, value)
        usage.pricing = {**usage.pricing, **raw} if newest else {**raw, **usage.pricing}
        usage.recorded_at = min(usage.recorded_at, receipt.wa_timestamp)
        usage.pricing_at = max(usage.pricing_at, receipt.wa_timestamp)
    receipt.applied = True
    await session.flush()
    return row.id


async def record_statuses(
    session: AsyncSession,
    events: Sequence[StatusEvent],
    dialect: str,
) -> None:
    insert = sqlite_insert if dialect == "sqlite" else pg_insert
    for event in sorted(events, key=lambda item: (item.wa_message_id, item.wa_timestamp, item.id)):
        await session.execute(
            insert(MessageStatusEvent)
            .values(**event.model_dump())
            .on_conflict_do_nothing(index_elements=[MessageStatusEvent.id])
        )
        receipt = await session.get(MessageStatusEvent, event.id)
        assert receipt is not None
        row_id = await _apply(session, receipt)
        if row_id is not None:
            await reconcile_pending(session, row_id)


async def reconcile_pending(session: AsyncSession, row_id: UUID) -> None:
    row = await session.get(OutboundMessage, row_id)
    if row is None:
        return
    receipts = (
        await session.scalars(
            select(MessageStatusEvent)
            .where(
                MessageStatusEvent.applied.is_(False),
                or_(
                    MessageStatusEvent.wa_message_id == row.wa_message_id,
                    MessageStatusEvent.tracker == str(row_id),
                ),
            )
            .order_by(MessageStatusEvent.wa_timestamp, MessageStatusEvent.id)
        )
    ).all()
    for receipt in receipts:
        await _apply(session, receipt)
