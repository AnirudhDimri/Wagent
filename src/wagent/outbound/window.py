"""Free-form sends are only allowed during the customer service window."""

from datetime import datetime, timedelta


def window_open(last_inbound_at: datetime | None, now: datetime) -> bool:
    return last_inbound_at is not None and now < last_inbound_at + timedelta(hours=24)
