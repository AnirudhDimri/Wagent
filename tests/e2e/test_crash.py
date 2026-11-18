"""Kill a real sender process after it commits sending and writes the request."""

import asyncio
import sys
from datetime import timedelta
from pathlib import Path

import httpx
from sqlalchemy import select, update

from wagent import Wagent
from wagent.gateway.events import parse_events
from wagent.outbound.sender import Sender
from wagent.runners import EchoRunner
from wagent.store.models import OutboundMessage, utc_now
from wagent.testing.fake_graph_api import FakeGraphAPI
from wagent.testing.payloads import message_payload
from wagent.worker.loop import Worker

CHILD = r"""
import asyncio
import sys
from pathlib import Path
import httpx
from pydantic import SecretStr
from wagent import Settings, Wagent
from wagent.outbound.sender import Sender

async def main():
    settings = Settings(wa_phone_id="123456789", wa_app_secret=SecretStr("secret"),
                        wa_verify_token=SecretStr("verify"), wa_token=SecretStr("test-token"),
                        database_url=sys.argv[1])
    app = Wagent(settings=settings)
    async def accepted(request):
        Path(sys.argv[2]).write_text("request accepted")
        await asyncio.Event().wait()
    async with httpx.AsyncClient(transport=httpx.MockTransport(accepted)) as client:
        await Sender(app.inbox, settings, client).run_once()
asyncio.run(main())
"""


async def test_killed_sender_does_not_send_again(gateway: Wagent, tmp_path: Path) -> None:
    await gateway.inbox.ingest(
        parse_events(message_payload("one reply"), gateway.settings.wa_phone_id)
    )
    assert await Worker(gateway.inbox, gateway.settings, EchoRunner()).run_once()
    marker = tmp_path / "request-written"
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        CHILD,
        gateway.settings.database_url,
        str(marker),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        async with asyncio.timeout(10):
            for _ in range(1000):
                if marker.exists():
                    break
                if process.returncode is not None:
                    raise AssertionError("Sender child exited before writing the request")
                await asyncio.sleep(0.01)
        assert marker.exists()
        async with gateway.inbox.sessions() as session:
            assert (await session.scalars(select(OutboundMessage))).one().status == "sending"
    finally:
        if process.returncode is None:
            process.kill()
        await asyncio.wait_for(process.wait(), timeout=10)
    # Advance the stale-send age rather than sleeping for the production lease.
    async with gateway.inbox.sessions.begin() as session:
        await session.execute(
            update(OutboundMessage).values(
                sending_at=utc_now() - timedelta(seconds=gateway.settings.lease_seconds + 1)
            )
        )
    graph = FakeGraphAPI()
    restarted = Wagent(settings=gateway.settings)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=graph.app)) as client:
            assert not await Sender(restarted.inbox, gateway.settings, client).run_once()
        assert graph.requests == []
        async with restarted.inbox.sessions() as session:
            row = (await session.scalars(select(OutboundMessage))).one()
            assert row.status == "unknown" and row.attempts == 1
    finally:
        await restarted.engine.dispose()
