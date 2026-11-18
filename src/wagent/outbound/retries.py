"""Narrow retry allowlist: a transient error alone does not prove nonacceptance."""

import math
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

SAFE_REJECTION_CODES = {"4", "80007", "130429", "131056"}
AMBIGUOUS_CODES = {"1", "2", "131000", "131016"}


def error_code(response: httpx.Response) -> str | None:
    try:
        data: Any = response.json()
        code: Any = data["error"]["code"]
        return str(code) if isinstance(code, (str, int)) and not isinstance(code, bool) else None
    except (ValueError, KeyError, TypeError):
        return None


def retry_after(value: str | None, now: datetime) -> float:
    if not value:
        return 0
    try:
        seconds = float(value)
        return max(0, seconds) if math.isfinite(seconds) else 0
    except ValueError:
        try:
            target = parsedate_to_datetime(value)
            if target.tzinfo is None:
                target = target.replace(tzinfo=UTC)
            return max(0, (target - now).total_seconds())
        except (ValueError, TypeError, OverflowError):
            return 0
