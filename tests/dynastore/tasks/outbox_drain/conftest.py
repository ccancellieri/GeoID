"""Shared fixtures for ``tests/dynastore/tasks/outbox_drain``.

Mirrors ``tests/dynastore/modules/storage/conftest.py`` — provides a raw
asyncpg connection and a per-test PG schema with ``search_path`` already
set so the DDL helpers and ``PgOutboxStore`` can be exercised directly.
"""
from __future__ import annotations

import os
from typing import AsyncIterator

import pytest_asyncio

from dynastore.tools.identifiers import generate_id_hex


def _resolve_asyncpg_url() -> str:
    url = os.getenv(
        "DATABASE_URL",
        "postgresql://testuser:testpassword@localhost:54320/gis_dev",
    )
    return url.replace("postgresql+asyncpg://", "postgresql://")


@pytest_asyncio.fixture
async def async_conn() -> AsyncIterator[object]:
    """Raw ``asyncpg.Connection`` against the test database."""
    import asyncpg

    conn = await asyncpg.connect(_resolve_asyncpg_url())
    try:
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def async_schema(async_conn) -> AsyncIterator[str]:  # noqa: ANN001
    """Per-test PG schema with ``search_path`` already set on ``async_conn``."""
    schema = f"drain_t_{generate_id_hex()[:10]}"
    await async_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')  # type: ignore[attr-defined]
    await async_conn.execute(f'SET search_path TO "{schema}"')  # type: ignore[attr-defined]
    try:
        yield schema
    finally:
        try:
            await async_conn.execute("RESET search_path")  # type: ignore[attr-defined]
            await async_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')  # type: ignore[attr-defined]
        except Exception:
            pass
