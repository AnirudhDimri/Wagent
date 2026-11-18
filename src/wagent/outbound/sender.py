"""Durable window-aware sends, bounded safe retries, and status reconciliation."""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import httpx
from sqlalchemy import case, exists, or_, select, update
from sqlalchemy.orm import aliased

from wagent.outbound.retries import AMBIGUOUS_CODES, SAFE_REJECTION_CODES, error_code, retry_after
from wagent.outbound.templates import send_template
from wagent.outbound.window import window_open
from wagent.settings import Settings
from wagent.store.models import AuditEvent, Conversation, OutboundMessage, utc_now
from wagent.store.repo import InboxRepository
from wagent.store.statuses import reconcile_pending
from wagent.worker.limits import reserve_outbound

logger = logging.getLogger(__name__)


class Sender:
    def __init__(
        self, inbox: InboxRepository, settings: Settings, client: httpx.AsyncClient
    ) -> None:
        self.inbox = inbox
        self.settings = settings
        self.client = client

    async def recover(self) -> None:
        cutoff = utc_now() - timedelta(seconds=self.settings.lease_seconds)
        async with self.inbox.sessions.begin() as session:
            rows = (
                await session.scalars(
                    update(OutboundMessage)
                    .where(
                        OutboundMessage.status == "sending",
                        or_(
                            OutboundMessage.sending_at <= cutoff,
                            OutboundMessage.sending_at.is_(None),
                        ),
                    )
                    .values(status="unknown", error_code="interrupted_send")
                    .returning(OutboundMessage.id)
                )
            ).all()
            for row_id in rows:
                await reconcile_pending(session, row_id)

    async def _finish(
        self, row_id: UUID, status: str, error: str | None = None, wa_message_id: str | None = None
    ) -> None:
        async with self.inbox.sessions.begin() as session:
            if wa_message_id is not None:
                uncertain = OutboundMessage.status.in_(("sending", "unknown"))
                await session.execute(
                    update(OutboundMessage)
                    .where(
                        OutboundMessage.id == row_id,
                        or_(
                            OutboundMessage.wa_message_id.is_(None),
                            OutboundMessage.wa_message_id == wa_message_id,
                        ),
                    )
                    .values(
                        wa_message_id=wa_message_id,
                        status=case((uncertain, status), else_=OutboundMessage.status),
                        error_code=case((uncertain, error), else_=OutboundMessage.error_code),
                        sent_at=case(
                            (OutboundMessage.sent_at.is_(None), utc_now()),
                            else_=OutboundMessage.sent_at,
                        ),
                        next_attempt_at=None,
                    )
                )
            else:
                await session.execute(
                    update(OutboundMessage)
                    .where(
                        OutboundMessage.id == row_id,
                        OutboundMessage.status == "sending",
                    )
                    .values(status=status, error_code=error, next_attempt_at=None)
                )
            await reconcile_pending(session, row_id)

    async def _retry(self, row_id: UUID, error: str, header: str | None = None) -> None:
        now = utc_now()
        async with self.inbox.sessions.begin() as session:
            row = await session.scalar(
                update(OutboundMessage)
                .where(
                    OutboundMessage.id == row_id,
                    OutboundMessage.status == "sending",
                )
                .values(status=OutboundMessage.status)
                .returning(OutboundMessage)
            )
            if row is None:
                return
            row.error_code = error
            if row.attempts >= self.settings.send_max_attempts:
                row.status = "failed"
                row.next_attempt_at = None
            else:
                backoff = min(
                    300, self.settings.retry_backoff_seconds * 2 ** min(row.attempts - 1, 20)
                )
                delay = max(backoff, retry_after(header, now))
                representable = (datetime.max.replace(tzinfo=UTC) - now).total_seconds() - 1
                row.status = "queued"
                row.next_attempt_at = now + timedelta(seconds=min(delay, representable))
            await session.flush()
            await reconcile_pending(session, row_id)

    async def _response(self, row_id: UUID, response: httpx.Response) -> None:
        code = error_code(response)
        if response.status_code == 429 or (
            400 <= response.status_code < 500 and code in SAFE_REJECTION_CODES
        ):
            await self._retry(row_id, code or "http_429", response.headers.get("Retry-After"))
        elif (
            response.status_code >= 500
            or response.status_code in {408, 409}
            or code in AMBIGUOUS_CODES
        ):
            await self._finish(row_id, "unknown", code or f"http_{response.status_code}")
        elif not response.is_success:
            await self._finish(row_id, "failed", code or f"http_{response.status_code}")
        else:
            try:
                data: Any = response.json()
                message_id: Any = data["messages"][0]["id"]
                if not isinstance(message_id, str) or not message_id:
                    raise ValueError("missing message id")
            except (ValueError, KeyError, IndexError, TypeError):
                await self._finish(row_id, "unknown", "invalid_send_response")
                return
            await self._finish(row_id, "sent", wa_message_id=message_id)

    async def run_once(self) -> bool:
        await self.recover()
        now = utc_now()
        sibling = aliased(OutboundMessage)
        older = exists().where(
            sibling.conversation_id == OutboundMessage.conversation_id,
            or_(
                sibling.status == "sending",
                (sibling.status == "queued")
                & (
                    (sibling.created_at < OutboundMessage.created_at)
                    | (
                        (sibling.created_at == OutboundMessage.created_at)
                        & (sibling.id < OutboundMessage.id)
                    )
                ),
            ),
        )
        due = or_(OutboundMessage.next_attempt_at.is_(None), OutboundMessage.next_attempt_at <= now)
        async with self.inbox.sessions.begin() as session:
            query = (
                select(OutboundMessage)
                .where(
                    OutboundMessage.status == "queued",
                    due,
                    ~older,
                    exists().where(
                        Conversation.id == OutboundMessage.conversation_id,
                        Conversation.erasing.is_(False),
                    ),
                )
                .order_by(OutboundMessage.created_at, OutboundMessage.id)
                .limit(1)
            )
            row = (await session.scalars(query)).first()
            if row is None:
                return False
            # Lock the conversation before its outbox rows: limits, turn completion,
            # and erasure all use this same lock order.
            conversation = await session.scalar(
                update(Conversation)
                .where(
                    Conversation.id == row.conversation_id,
                    Conversation.erasing.is_(False),
                )
                .values(updated_at=now)
                .returning(Conversation)
            )
            if conversation is None:
                return False
            claimed = await session.scalar(
                update(OutboundMessage)
                .where(
                    OutboundMessage.id == row.id,
                    OutboundMessage.status == "queued",
                    due,
                    OutboundMessage.attempts == row.attempts,
                )
                .values(status="sending", sending_at=now, attempts=OutboundMessage.attempts + 1)
                .returning(OutboundMessage.id)
            )
            if claimed is None:
                return False
            reason: str | None = None
            confirmation = row.purpose == "opt_out"
            if not confirmation and (
                self.settings.pause_all or conversation.mode != "bot" or conversation.opted_out
            ):
                reason = "conversation_not_sendable"
            elif not window_open(conversation.last_inbound_at, utc_now()):
                if self.settings.out_of_window_policy == "template" and not confirmation:
                    if row.kind == "text":
                        row.kind = "template"
                        row.body = {
                            "name": self.settings.fallback_template_name,
                            "language": self.settings.fallback_template_lang,
                        }
                else:
                    reason = "window_closed"
                if row.turn_id is not None:
                    await session.execute(
                        update(OutboundMessage)
                        .where(
                            OutboundMessage.turn_id == row.turn_id,
                            OutboundMessage.id != row.id,
                            OutboundMessage.status == "queued",
                        )
                        .values(status="failed", error_code=reason or "template_coalesced")
                    )
            if confirmation and row.kind != "text":
                reason = "confirmation_must_be_free_form"
            if row.kind == "template" and not self.settings.wa_waba_id:
                reason = "missing_waba_id"
            if reason is None and not await reserve_outbound(
                session, row, now, self.settings.daily_outbound_cap_per_user
            ):
                reason = "daily_cap"
            if reason:
                row.status = "failed"
                row.error_code = reason
                session.add(
                    AuditEvent(
                        conversation_id=conversation.id,
                        type="outbound_drop",
                        actor="sender",
                        at=now,
                        meta={"outbound_id": str(row.id), "reason": reason},
                    )
                )
                return True
            row_id, recipient, phone_id = row.id, conversation.wa_id, conversation.phone_number_id
            body, kind = row.body, row.kind
        # All payload conversion and sending state are committed before this call.
        token = self.settings.wa_token
        if token is None or not token.get_secret_value():
            await self._finish(row_id, "failed", "missing_token")
            return True
        try:
            async with asyncio.timeout(self.settings.send_timeout_seconds):
                if kind == "template":
                    message_id = await send_template(
                        self.client,
                        self.settings,
                        phone_id,
                        recipient,
                        body["name"],
                        body["language"],
                        str(row_id),
                    )
                    if not message_id:
                        raise ValueError("missing message id")
                    await self._finish(row_id, "sent", wa_message_id=message_id)
                else:
                    response = await self.client.post(
                        f"https://graph.facebook.com/{self.settings.wa_api_version}/{phone_id}/messages",
                        headers={"Authorization": f"Bearer {token.get_secret_value()}"},
                        json={
                            "messaging_product": "whatsapp",
                            "recipient_type": "individual",
                            "to": recipient,
                            "type": "text",
                            "text": {"body": body["text"]},
                            "biz_opaque_callback_data": str(row_id),
                        },
                        timeout=self.settings.send_timeout_seconds,
                    )
                    await self._response(row_id, response)
        except httpx.HTTPStatusError as error:
            await self._response(row_id, error.response)
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
            await self._retry(row_id, "connect_failed")
        except (httpx.HTTPError, TimeoutError):
            await self._finish(row_id, "unknown", "ambiguous_transport")
        except (ValueError, KeyError, TypeError, IndexError):
            await self._finish(row_id, "unknown", "invalid_send_response")
        return True

    async def serve_slot(self) -> None:
        while True:
            try:
                worked = await self.run_once()
            except Exception:
                logger.error("Sender iteration failed; retrying after poll interval")
                worked = False
            if not worked:
                await asyncio.sleep(self.settings.worker_poll_seconds)

    async def serve(self) -> None:
        async with asyncio.TaskGroup() as group:
            for _ in range(self.settings.worker_concurrency):
                group.create_task(self.serve_slot())
