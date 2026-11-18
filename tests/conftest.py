"""Use the real Alembic migration for every isolated test database."""

from collections.abc import AsyncGenerator
from pathlib import Path

import httpx
import pytest
from alembic import command
from alembic.config import Config
from pydantic import SecretStr

from wagent import Settings, Wagent

APP_SECRET = "local-app-secret"
VERIFY_TOKEN = "local-verify-token"
PHONE_ID = "123456789"


@pytest.fixture
def database_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    url = f"sqlite+aiosqlite:///{tmp_path / 'inbox.db'}"
    monkeypatch.setenv("DATABASE_URL", url)
    command.upgrade(Config("alembic.ini"), "head")
    return url


@pytest.fixture
def settings(database_url: str) -> Settings:
    return Settings(
        wa_phone_id=PHONE_ID,
        wa_app_secret=SecretStr(APP_SECRET),
        wa_verify_token=SecretStr(VERIFY_TOKEN),
        database_url=database_url,
        debounce_ms=0,
    )


@pytest.fixture
async def gateway(settings: Settings) -> AsyncGenerator[Wagent]:
    gateway = Wagent(settings=settings)
    async with gateway.fastapi_app.router.lifespan_context(gateway.fastapi_app):
        yield gateway


@pytest.fixture
async def client(gateway: Wagent) -> AsyncGenerator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gateway.fastapi_app), base_url="http://test"
    ) as client:
        yield client
