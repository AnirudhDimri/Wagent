"""Sender labels are stable opaque HMAC values, not raw phone numbers."""

import logging

import pytest
from pydantic import SecretStr

from wagent import Wagent
from wagent.gateway.events import parse_events
from wagent.logging import hash_phone
from wagent.runners import EchoRunner
from wagent.testing.payloads import message_payload
from wagent.worker.loop import Worker


def test_hash_is_salted_and_deterministic() -> None:
    assert hash_phone("15551234567", "salt") == hash_phone("15551234567", "salt")
    assert hash_phone("15551234567", "salt") != hash_phone("15551234567", "other-salt")
    assert "15551234567" not in hash_phone("15551234567", "salt")
    with pytest.raises(ValueError):
        hash_phone("15551234567", "")


async def test_worker_logs_no_phone_or_credentials(
    gateway: Wagent, caplog: pytest.LogCaptureFixture
) -> None:
    settings = gateway.settings.model_copy(
        update={"log_salt": SecretStr("private-salt"), "wa_token": SecretStr("private-token")}
    )
    await gateway.inbox.ingest(parse_events(message_payload(), settings.wa_phone_id))
    with caplog.at_level(logging.INFO, logger="wagent.worker.loop"):
        assert await Worker(gateway.inbox, settings, EchoRunner()).run_once()
    assert hash_phone("15551234567", "private-salt") in caplog.text
    assert "15551234567" not in caplog.text and "private-token" not in caplog.text
    assert "private-salt" not in caplog.text
