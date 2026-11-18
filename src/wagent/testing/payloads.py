"""Realistic Cloud API payloads, signed without contacting Meta."""

import hashlib
import hmac
import json
from datetime import datetime
from typing import Any
from uuid import uuid4

from wagent.store.models import utc_now


def message_payload(
    text: str = "Hello",
    *,
    message_id: str | None = None,
    wa_id: str = "15551234567",
    phone_number_id: str = "123456789",
    timestamp: datetime | None = None,
) -> dict[str, Any]:
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "987654321",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "messaging_product": "whatsapp",
                            "metadata": {
                                "display_phone_number": "15550000000",
                                "phone_number_id": phone_number_id,
                            },
                            "contacts": [{"profile": {"name": "Test User"}, "wa_id": wa_id}],
                            "messages": [
                                {
                                    "from": wa_id,
                                    "id": message_id or f"wamid.{uuid4().hex}",
                                    "timestamp": str(int((timestamp or utc_now()).timestamp())),
                                    "type": "text",
                                    "text": {"body": text},
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


def signed_payload(payload: object, app_secret: str) -> tuple[bytes, dict[str, str]]:
    """Serialize UTF-8 once and sign precisely the bytes sent by the test client."""
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    signature = hmac.new(app_secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return body, {"Content-Type": "application/json", "X-Hub-Signature-256": f"sha256={signature}"}


def status_payload(
    wa_message_id: str,
    status: str = "delivered",
    *,
    phone_number_id: str = "123456789",
    timestamp: datetime | None = None,
    pricing: dict[str, Any] | None = None,
    tracker: str | None = None,
    error_code: int | None = None,
) -> dict[str, Any]:
    payload = message_payload(phone_number_id=phone_number_id)
    value = payload["entry"][0]["changes"][0]["value"]
    value.pop("messages")
    value.pop("contacts")
    receipt: dict[str, Any] = {
        "id": wa_message_id,
        "status": status,
        "recipient_id": "15551234567",
        "timestamp": str(int((timestamp or utc_now()).timestamp())),
    }
    if pricing is not None:
        receipt["pricing"] = pricing
    if tracker is not None:
        receipt["biz_opaque_callback_data"] = tracker
    if error_code is not None:
        receipt["errors"] = [{"code": error_code, "title": "Test failure"}]
    value["statuses"] = [receipt]
    return payload
