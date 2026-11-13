"""Normalize all inbound messages in a Cloud API envelope."""

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
