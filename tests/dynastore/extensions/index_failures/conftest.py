"""Fixtures for ``tests/dynastore/extensions/index_failures``.

Mirrors the asyncpg-backed setup from
``tests/dynastore/modules/storage/conftest.py`` so the integration test
can exercise :class:`PgIndexFailureLog` against a real per-tenant
schema without bringing up the full FastAPI app stack (which has no
``api_client`` fixture in this repo).
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
    import asyncpg

    conn = await asyncpg.connect(_resolve_asyncpg_url())
    try:
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def async_schema(async_conn) -> AsyncIterator[str]:  # noqa: ANN001
    schema = f"ifail_t_{generate_id_hex()[:10]}"
    await async_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')  # type: ignore[attr-defined]
    await async_conn.execute(f'SET search_path TO "{schema}"')  # type: ignore[attr-defined]
    try:
        yield schema
    finally:
        try:
            await async_conn.execute("RESET search_path")  # type: ignore[attr-defined]
            await async_conn.execute(  # type: ignore[attr-defined]
                f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'
            )
        except Exception:
            pass
