"""Fast acknowledgments after durable ingestion; no agent work here."""

import hmac
import json

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import PlainTextResponse
from sqlalchemy.exc import SQLAlchemyError

from wagent.gateway.events import parse_events
from wagent.gateway.signature import verify_signature
from wagent.settings import Settings
from wagent.store.repo import InboxRepository


def webhook_router(settings: Settings, inbox: InboxRepository) -> APIRouter:
    router = APIRouter()

    @router.get(settings.webhook_path, response_class=PlainTextResponse)
    async def handshake(request: Request) -> PlainTextResponse:
        query = request.query_params
        token = query.get("hub.verify_token", "")
        expected = settings.wa_verify_token.get_secret_value()
        if query.get("hub.mode") != "subscribe" or not hmac.compare_digest(
            token.encode("utf-8"), expected.encode("utf-8")
        ):
            raise HTTPException(403, "Webhook verification failed")
        challenge = query.get("hub.challenge")
        if challenge is None:
            raise HTTPException(400, "Missing challenge")
        return PlainTextResponse(challenge)

    @router.post(settings.webhook_path, response_class=PlainTextResponse)
    async def ingest(request: Request) -> PlainTextResponse:
        body = await request.body()
        if not verify_signature(
            body,
            request.headers.get("X-Hub-Signature-256"),
            settings.wa_app_secret.get_secret_value(),
        ):
            raise HTTPException(401, "Invalid webhook signature")
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            raise HTTPException(400, "Invalid JSON") from None
        events = parse_events(payload, settings.wa_phone_id)
        try:
            await inbox.ingest(events)
        except SQLAlchemyError:
            # Do not log SQL parameters, payloads, or exception strings.
            raise HTTPException(503, "Inbox unavailable") from None
        return PlainTextResponse("ok")

    return router
