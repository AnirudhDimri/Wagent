"""The fake API is an offline, authenticated ASGI service."""

import httpx

from wagent.testing.fake_graph_api import FakeGraphAPI


async def test_fake_records_only_valid_authenticated_requests() -> None:
    graph = FakeGraphAPI()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=graph.app), base_url="http://graph"
    ) as client:
        message = {
            "messaging_product": "whatsapp",
            "to": "15551234567",
            "type": "text",
            "text": {"body": "Hello"},
        }
        assert (await client.post("/v26.0/123/messages", json=message)).status_code == 401
        client.headers["Authorization"] = "Bearer test-token"
        assert (await client.post("/v26.0/123/messages", json={})).status_code == 400
        response = await client.post("/v26.0/123/messages", json=message)
        assert response.status_code == 200
        assert response.json()["messages"][0]["id"].startswith("wamid.fake.")
        assert graph.requests == [{"version": "v26.0", "phone_number_id": "123", "body": message}]
        read = {"messaging_product": "whatsapp", "status": "read", "message_id": "wamid.test"}
        assert (await client.post("/v26.0/123/messages", json=read)).json() == {"success": True}
