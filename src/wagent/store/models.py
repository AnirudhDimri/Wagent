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
    erasing: Mapped[bool] = mapped_column(default=False, server_default="false")
    last_rate_notice_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
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
    next_attempt_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


class Turn(Base):
    __tablename__ = "turns"
    __table_args__ = (
        CheckConstraint(
            "outcome IN ('replied','handoff','skipped','error')", name="ck_turn_outcome"
        ),
    )

    id: Mapped[str] = mapped_column(primary_key=True)
    conversation_id: Mapped[UUID] = mapped_column(
        ForeignKey("conversations.id", ondelete="CASCADE")
    )
    started_at: Mapped[datetime] = mapped_column(UTCDateTime())
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    inbound_ids: Mapped[list[str]] = mapped_column(JSON)
    outcome: Mapped[str | None]
    latency_ms: Mapped[int | None]
    agent_attempted: Mapped[bool] = mapped_column(default=True, server_default="true")


class OutboundMessage(Base):
    __tablename__ = "outbound_messages"
    __table_args__ = (
        CheckConstraint(
            "kind IN ('text','template','interactive','media')", name="ck_outbound_kind"
        ),
        CheckConstraint(
            "status IN ('queued','sending','sent','delivered','read','failed','unknown')",
            name="ck_outbound_status",
        ),
        Index("ix_outbound_queue", "status", "created_at"),
    )

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    conversation_id: Mapped[UUID] = mapped_column(
        ForeignKey("conversations.id", ondelete="CASCADE")
    )
    turn_id: Mapped[str | None] = mapped_column(ForeignKey("turns.id", ondelete="CASCADE"))
    dedupe_key: Mapped[str] = mapped_column(unique=True)
    kind: Mapped[str]
    body: Mapped[dict[str, Any]] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(default="queued", server_default="queued")
    wa_message_id: Mapped[str | None] = mapped_column(unique=True)
    error_code: Mapped[str | None]
    attempts: Mapped[int] = mapped_column(default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now)
    next_attempt_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    sending_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    purpose: Mapped[str] = mapped_column(default="agent", server_default="agent")
    cap_reserved_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    sent_at: Mapped[datetime | None] = mapped_column(UTCDateTime())


class Usage(Base):
    __tablename__ = "usage"
    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    outbound_id: Mapped[UUID] = mapped_column(
        ForeignKey("outbound_messages.id", ondelete="CASCADE"), unique=True
    )
    billable: Mapped[bool | None]
    pricing_model: Mapped[str | None]
    category: Mapped[str | None]
    type: Mapped[str | None]
    pricing: Mapped[dict[str, Any]] = mapped_column(JSON)
    recorded_at: Mapped[datetime] = mapped_column(UTCDateTime(), index=True)
    pricing_at: Mapped[datetime] = mapped_column(UTCDateTime())


class MessageStatusEvent(Base):
    __tablename__ = "message_status_events"
    id: Mapped[str] = mapped_column(primary_key=True)
    phone_number_id: Mapped[str]
    wa_message_id: Mapped[str] = mapped_column(index=True)
    status: Mapped[str]
    wa_timestamp: Mapped[datetime] = mapped_column(UTCDateTime())
    tracker: Mapped[str | None] = mapped_column(index=True)
    pricing: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    error_code: Mapped[str | None]
    applied: Mapped[bool] = mapped_column(default=False, server_default="false")
    received_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now)


class AuditEvent(Base):
    __tablename__ = "audit_events"
    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    conversation_id: Mapped[UUID] = mapped_column(
        ForeignKey("conversations.id", ondelete="CASCADE")
    )
    type: Mapped[str]
    actor: Mapped[str]
    at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utc_now)
    meta: Mapped[dict[str, Any]] = mapped_column(JSON)
