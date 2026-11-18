"""Usage counts derived from reported pricing, with no currency estimates."""

from datetime import UTC, date, datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select

from wagent.admin.auth import admin_auth
from wagent.settings import Settings
from wagent.store.erasure import ConversationBusy, ErasureService
from wagent.store.models import Usage
from wagent.store.repo import InboxRepository


def _boundary(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        if len(value) == 10:
            return datetime.combine(date.fromisoformat(value), datetime.min.time(), UTC)
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None:
            raise ValueError("timezone required")
        return result.astimezone(UTC)
    except ValueError:
        raise HTTPException(422, "Use YYYY-MM-DD or a timezone-aware ISO datetime") from None


def admin_router(settings: Settings, inbox: InboxRepository) -> APIRouter:
    router = APIRouter(prefix="/admin", dependencies=[Depends(admin_auth(settings))])

    @router.get("/usage")
    async def usage(
        from_: Annotated[str | None, Query(alias="from")] = None,
        to: str | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        start, end = _boundary(from_), _boundary(to)
        if start is not None and end is not None and start >= end:
            raise HTTPException(422, "from must be earlier than to")
        timestamp = (
            func.timezone("UTC", Usage.recorded_at)
            if inbox.engine.dialect.name == "postgresql"
            else Usage.recorded_at
        )
        day = func.date(timestamp)
        query = select(day, Usage.category, Usage.billable, func.count(Usage.id))
        if start is not None:
            query = query.where(Usage.recorded_at >= start)
        if end is not None:
            query = query.where(Usage.recorded_at < end)
        query = query.group_by(day, Usage.category, Usage.billable).order_by(day, Usage.category)
        async with inbox.sessions() as session:
            rows = (await session.execute(query)).all()
        return {
            "items": [
                {"day": str(row[0]), "category": row[1], "billable": row[2], "count": row[3]}
                for row in rows
            ]
        }

    @router.delete("/conversations/{conversation_id}", status_code=204)
    async def erase_conversation(conversation_id: UUID) -> None:
        service = ErasureService(inbox, settings.database_url, settings.lease_seconds)
        try:
            erased = await service.erase(conversation_id)
        except ConversationBusy:
            raise HTTPException(409, "Conversation has active work; retry later") from None
        except Exception:
            raise HTTPException(503, "Erasure incomplete; retry to finish") from None
        if not erased:
            raise HTTPException(404, "Conversation not found")

    return router
