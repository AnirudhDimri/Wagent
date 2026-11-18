"""Bearer authentication for the currently implemented admin endpoints."""

import hmac
from collections.abc import Callable
from typing import Annotated

from fastapi import Header, HTTPException

from wagent.settings import Settings


def admin_auth(settings: Settings) -> Callable[..., None]:
    def require_admin(authorization: Annotated[str | None, Header()] = None) -> None:
        expected = settings.admin_api_key
        scheme, _, value = (authorization or "").partition(" ")
        if (
            expected is None
            or scheme.lower() != "bearer"
            or not hmac.compare_digest(value.encode(), expected.get_secret_value().encode())
        ):
            raise HTTPException(401, "Unauthorized", headers={"WWW-Authenticate": "Bearer"})

    return require_admin
