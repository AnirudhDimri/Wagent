"""Application assembly, background processing, and resource lifecycle."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from filelock import BaseFileLock, FileLock, Timeout
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError

from wagent.admin.api import admin_router
from wagent.gateway.webhook import webhook_router
from wagent.outbound.sender import Sender
from wagent.runners.base import AgentRunner
from wagent.runners.langgraph_runner import LangGraphRunner
from wagent.settings import Settings
from wagent.store.repo import InboxRepository, create_engine
from wagent.store.retention import RetentionJob
from wagent.worker.loop import Worker


class Wagent:
    """Apply Alembic migrations first. Supply a runner to enable processing."""

    def __init__(
        self,
        settings: Settings | None = None,
        runner: AgentRunner | None = None,
        *,
        graph_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.settings = settings if settings is not None else Settings()  # pyright: ignore[reportCallIssue]
        self.engine = create_engine(self.settings.database_url)
        self.inbox = InboxRepository(self.engine)
        self.worker = Worker(self.inbox, self.settings, runner) if runner is not None else None
        self.sender: Sender | None = None

        @asynccontextmanager
        async def lifespan(_: FastAPI) -> AsyncGenerator[None]:
            tasks: list[asyncio.Task[None]] = []
            client = graph_client
            lock: BaseFileLock | None = None
            runner_started = False
            retention = RetentionJob(self.inbox, self.settings)
            try:
                if runner is not None:
                    await self.inbox.ready()
                    if not self.settings.wa_token or not self.settings.wa_token.get_secret_value():
                        raise ValueError("WA_TOKEN is required when a runner is configured")
                    url = make_url(self.settings.database_url)
                    if url.drivername == "sqlite+aiosqlite" and url.database not in {
                        None,
                        ":memory:",
                    }:
                        lock = FileLock(f"{url.database}.worker.lock")
                        try:
                            lock.acquire(timeout=0)
                        except Timeout:
                            lock = None
                            raise RuntimeError(
                                "SQLite supports only one wagent worker process"
                            ) from None
                    if isinstance(runner, LangGraphRunner):
                        await runner.start(self.settings.database_url)
                        runner_started = True
                    if client is None:
                        client = httpx.AsyncClient()
                    self.sender = Sender(self.inbox, self.settings, client)
                    assert self.worker is not None
                    tasks = [
                        asyncio.create_task(self.worker.serve()),
                        asyncio.create_task(self.sender.serve()),
                    ]
                tasks.append(asyncio.create_task(retention.serve()))
                yield
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                if runner_started and isinstance(runner, LangGraphRunner):
                    await runner.aclose()
                if client is not None and graph_client is None:
                    await client.aclose()
                if lock is not None:
                    lock.release()
                await self.engine.dispose()

        app = FastAPI(title="wagent", lifespan=lifespan)
        app.include_router(webhook_router(self.settings, self.inbox))
        if self.settings.admin_api_key and self.settings.admin_api_key.get_secret_value():
            app.include_router(admin_router(self.settings, self.inbox))

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
