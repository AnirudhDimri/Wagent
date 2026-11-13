"""Application assembly and resource lifecycle for the M1 gateway."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from wagent.gateway.webhook import webhook_router
from wagent.settings import Settings
from wagent.store.repo import InboxRepository, create_engine


class Wagent:
    """Build a gateway. Apply Alembic migrations before serving the application."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings if settings is not None else Settings()  # pyright: ignore[reportCallIssue]
        self.engine = create_engine(self.settings.database_url)
        self.inbox = InboxRepository(self.engine)

        @asynccontextmanager
        async def lifespan(_: FastAPI) -> AsyncGenerator[None]:
            try:
                yield
            finally:
                await self.engine.dispose()

        app = FastAPI(title="wagent", lifespan=lifespan)
        app.include_router(webhook_router(self.settings, self.inbox))

        @app.get("/healthz")
        async def health() -> dict[str, str]:
            return {"status": "ok"}

        @app.get("/readyz")
        async def ready() -> JSONResponse:
            try:
                await self.inbox.ready()
            except SQLAlchemyError:
                return JSONResponse({"status": "unavailable"}, status_code=503)
            return JSONResponse({"status": "ok"})

        self.fastapi_app = app
