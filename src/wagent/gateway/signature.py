"""Verify the exact raw request bytes before parsing JSON."""

import re

from pywa.utils import webhook_updates_validator

_HEADER = re.compile(r"sha256=[0-9a-f]{64}\Z", re.ASCII)


def verify_signature(body: bytes, header: str | None, app_secret: str) -> bool:
    """Reject missing/malformed headers; pywa compares the HMAC in constant time."""
    return bool(
        header
        and _HEADER.fullmatch(header)
        and webhook_updates_validator(
            app_secret=app_secret, request_body=body, x_hub_signature=header
        )
    )
