#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

"""Shared fixtures for tests/dynastore/tasks/retention.

Mirrors tests/dynastore/tasks/outbox_drain/conftest.py — provides a raw
asyncpg connection and a per-test PG schema with the full tasks table
hierarchy (partitioned table + DEFAULT partition) already provisioned,
so the retention function can be exercised end-to-end.
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
    """Raw asyncpg.Connection against the test database.

    Skips the test automatically when the database is not reachable so
    that the integration suite degrades gracefully in environments without
    a local PostgreSQL instance (CI without Docker, unit-only runs).
    """
    import asyncpg

    try:
        conn = await asyncpg.connect(_resolve_asyncpg_url())
    except (OSError, asyncpg.PostgresError, Exception) as exc:
        import pytest as _pytest
        _pytest.skip(f"PostgreSQL not reachable — skipping PG integration test: {exc}")
        return  # unreachable; satisfies type checkers
    try:
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def retention_schema(async_conn) -> AsyncIterator[str]:  # noqa: ANN001
    """Per-test PG schema with tasks + tasks_default provisioned.

    Creates the minimal table structure needed to execute the retention
    function: a PARTITION BY RANGE tasks table and its DEFAULT partition.
    The schema is dropped on teardown.
    """
    schema = f"ret_t_{generate_id_hex()[:10]}"
    conn = async_conn
    await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')  # type: ignore[attr-defined]

    # Minimal tasks table (partition key only — avoids importing the full DDL
    # chain which needs triggers / indexes not required for this test).
    await conn.execute(  # type: ignore[attr-defined]
        f"""
        CREATE TABLE "{schema}".tasks (
            task_id   UUID        NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (timestamp, task_id)
        ) PARTITION BY RANGE (timestamp);
        """
    )
    await conn.execute(  # type: ignore[attr-defined]
        f'CREATE TABLE "{schema}".tasks_default PARTITION OF "{schema}".tasks DEFAULT;'
    )

    try:
        yield schema
    finally:
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')  # type: ignore[attr-defined]
        except Exception:
            pass
