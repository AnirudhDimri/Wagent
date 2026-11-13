"""Alembic async environment; DATABASE_URL overrides the ini default."""

import asyncio
import os

from alembic import context
from dotenv import dotenv_values
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from wagent.store.models import Base

config = context.config
target_metadata = Base.metadata


def database_url() -> str:
    return (
        os.environ.get("DATABASE_URL")
        or dotenv_values(".env").get("DATABASE_URL")
        or config.get_main_option("sqlalchemy.url", "sqlite+aiosqlite:///./wagent.db")
        or "sqlite+aiosqlite:///./wagent.db"
    )


def run_migrations_offline() -> None:
    context.configure(
        url=database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def apply_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    engine = create_async_engine(database_url(), poolclass=pool.NullPool, hide_parameters=True)
    try:
        async with engine.connect() as connection:
            await connection.run_sync(apply_migrations)
    finally:
        await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_async_migrations())
