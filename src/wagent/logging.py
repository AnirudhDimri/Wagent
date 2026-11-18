"""Opaque sender labels: never log a raw WhatsApp identifier."""

import hashlib
import hmac


def hash_phone(wa_id: str, salt: str) -> str:
    if not salt:
        raise ValueError("LOG_SALT must not be empty to hash identifiers")
    return "wa_" + hmac.new(salt.encode(), wa_id.encode(), hashlib.sha256).hexdigest()[:24]
