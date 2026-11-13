"""Durable inbox schema, with UTC semantics on both supported databases."""

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import JSON, CheckConstraint, DateTime, ForeignKey, Index, UniqueConstraint, Uuid
from sqlalchemy.engine import Dialect
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import TypeDecorator


def utc_now() -> datetime:
    return datetime.now(UTC)


class UTCDateTime(TypeDecorator[datetime]):
    """SQLite drops tzinfo; restore UTC on reads and reject naive writes."""

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone-aware")
        value = value.astimezone(UTC)
        return value.replace(tzinfo=None) if dialect.name == "sqlite" else value

    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class Base(DeclarativeBase):
    pass


class Conversation(Base):
    __tablename__ = "conversations"
    __table_args__ = (
        UniqueConstraint("phone_number_id", "wa_id", name="uq_conversation_sender"),
        CheckConstraint("mode IN ('bot', 'human', 'paused')", name="ck_conversation_mode"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    phone_number_id: Mapped[str]
    wa_id: Mapped[str]
    thread_id: Mapped[str] = mapped_column(unique=True)
    mode: Mapped[str] = mapped_column(default="bot", server_default="bot")
    last_inbound_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    opted_out: Mapped[bool] = mapped_column(default=False, server_default="false")
    locale: Mapped[str | None]
    lease_owner: Mapped[str | None]
    lease_until: Mapped[datetime | None] = mapped_column(UTCDateTime())
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now)


class InboundMessage(Base):
    __tablename__ = "inbound_messages"
    __table_args__ = (
        CheckConstraint(
            "kind IN ('text','audio','image','document','location','interactive','other')",
            name="ck_inbound_kind",
        ),
        CheckConstraint(
            "status IN ('pending','processing','done','failed','skipped')", name="ck_inbound_status"
        ),
        Index("ix_inbound_queue", "conversation_id", "status", "wa_timestamp", "received_at"),
    )

    wa_message_id: Mapped[str] = mapped_column(primary_key=True)
    conversation_id: Mapped[UUID] = mapped_column(
        ForeignKey("conversations.id", ondelete="CASCADE")
    )
    received_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now)
    wa_timestamp: Mapped[datetime] = mapped_column(UTCDateTime())
    kind: Mapped[str]
    text: Mapped[str | None]
    media_id: Mapped[str | None]
    raw: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(default="pending", server_default="pending")
    attempts: Mapped[int] = mapped_column(default=0, server_default="0")
    last_error: Mapped[str | None]
    turn_id: Mapped[str | None]
