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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Live-PG behavioral test for ``reap_stuck_tasks``'s ``error_message`` text.

Before this fix, ``error_message``'s CASE only checked
``retry_count + 1 >= p_hard_cap`` while ``status``/``finished_at`` gated on
``LEAST(COALESCE(max_retries, p_max_retries), p_hard_cap)``. A row whose own
``max_retries`` was the binding cap (below the platform hard cap — the
common case for Cloud Run jobs, typically ``max_retries=1``) was moved to
``DEAD_LETTER`` but stamped with the generic "heartbeat expired" text instead
of a DLQ-specific reason, misleading anyone reading the row after the fact.

Uses the ``async_conn`` fixture from the sibling retention suite (skips
automatically when no local PostgreSQL is reachable).
"""

from __future__ import annotations

from typing import AsyncIterator

import pytest
import pytest_asyncio

from dynastore.modules.tasks.tasks_module import GLOBAL_TASKS_REAPER_DDL
from dynastore.tools.identifiers import generate_id_hex


@pytest_asyncio.fixture
async def reaper_schema(async_conn) -> AsyncIterator[str]:  # noqa: ANN001
    """Isolated schema carrying just the columns ``reap_stuck_tasks`` reads,
    plus the function itself. Bypasses ``ensure_task_storage_exists`` (which
    hard-refuses any schema other than the global ``tasks`` one) so the test
    can run against a throwaway schema instead of the shared global table.
    """
    schema = f"reap_t_{generate_id_hex()[:10]}"
    conn = async_conn
    await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')  # type: ignore[attr-defined]
    await conn.execute(  # type: ignore[attr-defined]
        f"""
        CREATE TABLE "{schema}".tasks (
            task_id           UUID        NOT NULL,
            timestamp         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            task_type         TEXT        NOT NULL DEFAULT 'noop',
            status             TEXT        NOT NULL DEFAULT 'PENDING',
            retry_count        INT         NOT NULL DEFAULT 0,
            max_retries        INT,
            owner_id           TEXT,
            locked_until       TIMESTAMPTZ,
            last_heartbeat_at  TIMESTAMPTZ,
            finished_at        TIMESTAMPTZ,
            error_message      TEXT,
            PRIMARY KEY (timestamp, task_id)
        ) PARTITION BY RANGE (timestamp);
        """
    )
    await conn.execute(  # type: ignore[attr-defined]
        f'CREATE TABLE "{schema}".tasks_default PARTITION OF "{schema}".tasks DEFAULT;'
    )
    await conn.execute(GLOBAL_TASKS_REAPER_DDL.format(schema=schema))  # type: ignore[attr-defined]

    try:
        yield schema
    finally:
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')  # type: ignore[attr-defined]
        except Exception:
            pass


async def _insert_stuck_row(
    conn, schema: str, *, retry_count: int, max_retries,
) -> str:
    import uuid

    task_id = str(uuid.uuid4())
    await conn.execute(  # type: ignore[attr-defined]
        f"""
        INSERT INTO "{schema}".tasks
            (task_id, task_type, status, retry_count, max_retries,
             owner_id, locked_until)
        VALUES ($1, 'noop', 'ACTIVE', $2, $3, 'stale-worker', NOW() - INTERVAL '1 hour')
        """,
        task_id, retry_count, max_retries,
    )
    return task_id


async def _reap(conn, schema: str, *, p_max_retries: int = 3, p_hard_cap: int = 5):
    await conn.execute(  # type: ignore[attr-defined]
        f'SELECT "{schema}".reap_stuck_tasks($1, $2)', p_max_retries, p_hard_cap,
    )


async def _fetch_row(conn, schema: str, task_id: str):
    return await conn.fetchrow(  # type: ignore[attr-defined]
        f'SELECT status, error_message FROM "{schema}".tasks WHERE task_id = $1',
        task_id,
    )


@pytest.mark.asyncio
async def test_per_row_max_retries_dlq_gets_max_retries_reason(reaper_schema, async_conn):
    """A row whose own ``max_retries`` (below the platform hard cap) is the
    binding cap must be DLQ'd with a max_retries-specific reason, not the
    generic heartbeat-expired text."""
    conn = async_conn
    # max_retries=1: retry_count(0) + 1 >= LEAST(1, 5) → DEAD_LETTER.
    task_id = await _insert_stuck_row(
        conn, reaper_schema, retry_count=0, max_retries=1,
    )

    await _reap(conn, reaper_schema, p_max_retries=3, p_hard_cap=5)

    row = await _fetch_row(conn, reaper_schema, task_id)
    assert row["status"] == "DEAD_LETTER"
    assert "max_retries" in row["error_message"]
    assert "hard retry cap" not in row["error_message"]


@pytest.mark.asyncio
async def test_platform_hard_cap_dlq_gets_hard_cap_reason(reaper_schema, async_conn):
    """A row whose ``max_retries`` is generous (or missing) but crosses the
    platform-wide hard cap must be DLQ'd with the hard-cap reason."""
    conn = async_conn
    # No per-row max_retries: falls back to p_max_retries(3); hard_cap=2 wins.
    task_id = await _insert_stuck_row(
        conn, reaper_schema, retry_count=1, max_retries=None,
    )

    await _reap(conn, reaper_schema, p_max_retries=3, p_hard_cap=2)

    row = await _fetch_row(conn, reaper_schema, task_id)
    assert row["status"] == "DEAD_LETTER"
    assert "hard retry cap" in row["error_message"]


@pytest.mark.asyncio
async def test_plain_requeue_keeps_heartbeat_expired_text(reaper_schema, async_conn):
    """A row still below every cap must requeue to PENDING with the plain
    heartbeat-expired text — unaffected by the DLQ-reason split."""
    conn = async_conn
    task_id = await _insert_stuck_row(
        conn, reaper_schema, retry_count=0, max_retries=5,
    )

    await _reap(conn, reaper_schema, p_max_retries=3, p_hard_cap=5)

    row = await _fetch_row(conn, reaper_schema, task_id)
    assert row["status"] == "PENDING"
    assert "heartbeat expired" in row["error_message"]
