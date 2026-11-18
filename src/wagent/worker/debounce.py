"""Database timestamps, rather than process timers, drive debounce."""

from datetime import datetime, timedelta


def batch_ready(
    oldest: datetime, newest: datetime, now: datetime, debounce_ms: int, max_debounce_ms: int
) -> bool:
    return newest <= now - timedelta(milliseconds=debounce_ms) or oldest <= now - timedelta(
        milliseconds=max_debounce_ms
    )


def opt_out_match(text: str | None, keywords: str) -> bool:
    return text is not None and text.strip().casefold() in {
        word.strip().casefold() for word in keywords.split(",") if word.strip()
    }
