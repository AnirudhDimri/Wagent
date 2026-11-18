"""Local ASGI Graph API stand-in; it never contacts Meta."""

from typing import Any, cast
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request


class FakeGraphAPI:
    """Capture authenticated message and mark-as-read calls for local tests."""

    def __init__(self, token: str = "test-token") -> None:
        self.requests: list[dict[str, Any]] = []
        self.app = FastAPI()

        @self.app.post("/{version}/{phone_number_id}/messages")
        async def messages(version: str, phone_number_id: str, request: Request) -> dict[str, Any]:
            if request.headers.get("Authorization") != f"Bearer {token}":
                raise HTTPException(401, "Invalid test token")
            body = await request.json()
            if not isinstance(body, dict):
                raise HTTPException(400, "Expected an object")
            body = cast(dict[str, Any], body)
            if body.get("messaging_product") != "whatsapp":
                raise HTTPException(400, "Expected a WhatsApp message")
            if body.get("status") == "read" and body.get("message_id"):
                self.requests.append(
                    {"version": version, "phone_number_id": phone_number_id, "body": body}
                )
                return {"success": True}
            kind = body.get("type")
            if kind not in {"text", "template", "interactive"} or not body.get("to"):
                raise HTTPException(400, "Missing type or recipient")
            if not isinstance(body.get(kind), dict):
                raise HTTPException(400, "Missing message content")
            if kind == "text" and not isinstance(body["text"].get("body"), str):
                raise HTTPException(400, "Missing text body")
            if kind == "text" and not 0 < len(body["text"]["body"]) <= 4096:
                raise HTTPException(400, "Text must contain 1 to 4096 characters")
            if kind == "template" and not (
                isinstance(body["template"].get("name"), str)
                and body["template"].get("name")
                and isinstance(body["template"].get("language"), dict)
                and body["template"]["language"].get("code")
            ):
                raise HTTPException(400, "Expected template name and language code")
            self.requests.append(
                {"version": version, "phone_number_id": phone_number_id, "body": body}
            )
            return {
                "messaging_product": "whatsapp",
                "contacts": [{"input": body["to"], "wa_id": body["to"]}],
                "messages": [{"id": f"wamid.fake.{uuid4().hex}"}],
            }
