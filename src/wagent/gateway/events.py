"""Normalize all inbound messages in a Cloud API envelope."""

import hashlib
import json
import logging
from datetime import UTC, datetime
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, ValidationError

logger = logging.getLogger(__name__)
MessageKind = Literal["text", "audio", "image", "document", "location", "interactive", "other"]


class InboundEvent(BaseModel):
    """Validated fields required for durable ingestion; raw data stays in storage."""

    model_config = ConfigDict(hide_input_in_errors=True)
    phone_number_id: str = Field(min_length=1)
    wa_id: str = Field(min_length=1)
    wa_message_id: str = Field(min_length=1)
    wa_timestamp: datetime
    kind: MessageKind
    text: str | None = None
    media_id: str | None = None
    raw: dict[str, Any]


def _objects(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [
        cast(dict[str, Any], item) for item in cast(list[object], value) if isinstance(item, dict)
    ]


def parse_events(payload: object, phone_number_id: str) -> list[InboundEvent]:
    """Ignore unknown event types without discarding other messages in the batch."""
    if not isinstance(payload, dict):
        logger.debug("Ignored unknown webhook envelope")
        return []
    payload = cast(dict[str, Any], payload)
    if payload.get("object") != "whatsapp_business_account":
        logger.debug("Ignored unknown webhook envelope")
        return []
    events: list[InboundEvent] = []
    for entry in _objects(payload.get("entry")):
        for change in _objects(entry.get("changes")):
            if change.get("field") != "messages":
                logger.debug("Ignored unknown webhook change")
                continue
            value = change.get("value")
            if not isinstance(value, dict):
                continue
            value = cast(dict[str, Any], value)
            metadata = value.get("metadata")
            if (
                not isinstance(metadata, dict)
                or cast(dict[str, Any], metadata).get("phone_number_id") != phone_number_id
            ):
                continue
            for message in _objects(value.get("messages")):
                try:
                    kind = message.get("type", "other")
                    kind = (
                        kind
                        if kind in {"text", "audio", "image", "document", "location", "interactive"}
                        else "other"
                    )
                    content = message.get(kind)
                    content = cast(dict[str, Any], content) if isinstance(content, dict) else {}
                    timestamp = message.get("timestamp")
                    if not isinstance(timestamp, (str, int)) or isinstance(timestamp, bool):
                        raise ValueError("invalid timestamp")
                    event = InboundEvent.model_validate(
                        {
                            "phone_number_id": phone_number_id,
                            "wa_id": message.get("from"),
                            "wa_message_id": message.get("id"),
                            "wa_timestamp": datetime.fromtimestamp(int(timestamp), UTC),
                            "kind": kind,
                            "text": content.get("body") if kind == "text" else None,
                            "media_id": content.get("id")
                            if kind in {"audio", "image", "document"}
                            else None,
                            "raw": message,
                        }
                    )
                except (ValidationError, ValueError, TypeError, OverflowError, OSError):
                    logger.debug("Ignored malformed inbound event")
                    continue
                events.append(event)
    return events


class StatusEvent(BaseModel):
    model_config = ConfigDict(hide_input_in_errors=True)
    id: str
    phone_number_id: str
    wa_message_id: str = Field(min_length=1)
    status: Literal["sent", "delivered", "read", "failed"]
    wa_timestamp: datetime
    tracker: str | None = None
    pricing: dict[str, Any] | None = None
    error_code: str | None = None


def parse_statuses(payload: object, phone_number_id: str) -> list[StatusEvent]:
    if not isinstance(payload, dict):
        return []
    envelope = cast(dict[str, Any], payload)
    if envelope.get("object") != "whatsapp_business_account":
        return []
    events: list[StatusEvent] = []
    for entry in _objects(envelope.get("entry")):
        for change in _objects(entry.get("changes")):
            value: Any = change.get("value")
            if change.get("field") != "messages" or not isinstance(value, dict):
                continue
            value = cast(dict[str, Any], value)
            metadata: Any = value.get("metadata")
            if not isinstance(metadata, dict):
                continue
            if cast(dict[str, Any], metadata).get("phone_number_id") != phone_number_id:
                continue
            for raw in _objects(value.get("statuses")):
                try:
                    timestamp: Any = raw.get("timestamp")
                    if not isinstance(timestamp, (str, int)) or isinstance(timestamp, bool):
                        raise ValueError("invalid timestamp")
                    pricing: Any = raw.get("pricing")
                    tracker: Any = raw.get("biz_opaque_callback_data")
                    errors = _objects(raw.get("errors"))
                    code: Any = errors[0].get("code") if errors else None
                    digest = hashlib.sha256(
                        json.dumps(
                            [phone_number_id, raw], sort_keys=True, separators=(",", ":")
                        ).encode()
                    ).hexdigest()
                    events.append(
                        StatusEvent.model_validate(
                            {
                                "id": digest,
                                "phone_number_id": phone_number_id,
                                "wa_message_id": raw.get("id"),
                                "status": raw.get("status"),
                                "wa_timestamp": datetime.fromtimestamp(int(timestamp), UTC),
                                "tracker": tracker if isinstance(tracker, str) else None,
                                "pricing": pricing if isinstance(pricing, dict) else None,
                                "error_code": str(code) if isinstance(code, (int, str)) else None,
                            }
                        )
                    )
                except (ValueError, TypeError, OverflowError, OSError):
                    logger.debug("Ignored malformed status event")
    return events
