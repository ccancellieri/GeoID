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

"""Live-PG tests for DrainSpawnerService (#2715).

Covers the leader-side RECOVERY tick for the event_drain / storage_drain
outboxes — NOT the co-transactional trigger itself (that is exercised by
``test_event_drain.py`` / ``test_storage_drain.py``). This tick is a no-op
unless the corresponding outbox actually has undrained work, and even then
only unblocks a demonstrably WEDGED existing drain row (stale PENDING, or
ACTIVE with an expired lease) — a healthy in-flight drain still blocks a
fresh spawn, exactly like the hot write-path trigger always has.

Scenarios covered:
1. No backlog in either outbox -> tick spawns nothing (backlog-gated no-op).
2. Backlog in tasks.storage only -> tick spawns storage_drain, not event_drain.
3. Backlog exists AND a live (fresh) PENDING drain row already exists ->
   tick does NOT insert a duplicate (still respects a healthy in-flight drain).
4. Backlog exists AND a WEDGED PENDING drain row exists (older than the
   grace window) -> tick DOES insert a fresh row alongside it (recovery).
5. Backlog exists AND a WEDGED ACTIVE drain row exists (lease expired) ->
   tick DOES insert a fresh row.
6. Backlog exists AND a live ACTIVE drain row exists (lease not expired) ->
   tick does NOT insert (still respects a healthy in-flight drain).
"""
from __future__ import annotations

import os
from typing import Any, AsyncIterator, Dict, List, Tuple
from uuid import uuid4

import pytest
import pytest_asyncio

from dynastore.tools.identifiers import generate_id_hex


def _sa_db_url() -> str:
    url = os.getenv(
        "DATABASE_URL",
        "postgresql://testuser:testpassword@localhost:54320/gis_dev",
    )
    if not url.startswith("postgresql+asyncpg://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


# Minimal flat tables matching the columns the drain-spawner backlog probes
# and trigger helpers touch (verified against GLOBAL_TASKS_TABLE_DDL /
# workclass_ddl.py — catalog_id, not the pre-#2325 schema_name column).
_TASKS_DDL = """
CREATE TABLE IF NOT EXISTS "{schema}".tasks (
    task_id         UUID            NOT NULL,
    catalog_id      VARCHAR(255)    NOT NULL,
    scope           VARCHAR(50)     NOT NULL DEFAULT 'CATALOG',
    caller_id       VARCHAR(255),
    task_type       VARCHAR         NOT NULL,
    type            VARCHAR         NOT NULL DEFAULT 'task',
    execution_mode  VARCHAR         NOT NULL DEFAULT 'ASYNCHRONOUS',
    status          VARCHAR         NOT NULL DEFAULT 'PENDING',
    inputs          JSONB,
    dedup_key       VARCHAR(512),
    timestamp       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    collection_id   VARCHAR(255),
    locked_until    TIMESTAMPTZ,
    PRIMARY KEY (timestamp, task_id)
);
"""

_STORAGE_DDL = """
CREATE TABLE IF NOT EXISTS "{schema}".storage (
    op_id           UUID            NOT NULL,
    day             DATE            NOT NULL DEFAULT CURRENT_DATE,
    catalog_id      TEXT            NOT NULL,
    driver_id       TEXT            NOT NULL,
    collection_id   TEXT,
    entity_kind     TEXT            NOT NULL DEFAULT 'item',
    entity_id       TEXT,
    op              TEXT            NOT NULL,
    status          TEXT            NOT NULL DEFAULT 'ready',
    ready_at        TIMESTAMPTZ     NOT NULL DEFAULT now(),
    op_payload      JSONB           NOT NULL DEFAULT '{{}}'::jsonb,
    idempotency_key TEXT,
    claim_version   INTEGER         NOT NULL DEFAULT 0,
    claimed_by      TEXT,
    claimed_at      TIMESTAMPTZ,
    attempts        INTEGER         NOT NULL DEFAULT 0,
    PRIMARY KEY (day, op_id)
);
"""

_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS "{schema}".events (
    event_id        UUID            NOT NULL,
    day             DATE            NOT NULL DEFAULT CURRENT_DATE,
    shard           SMALLINT        NOT NULL DEFAULT 0,
    catalog_id      TEXT,
    scope           TEXT            NOT NULL DEFAULT 'platform'
                        CHECK (scope = lower(scope)),
    event_type      TEXT            NOT NULL,
    status          TEXT            NOT NULL DEFAULT 'PENDING',
    payload         JSONB           NOT NULL DEFAULT '{{}}'::jsonb,
    locked_until    TIMESTAMPTZ,
    retry_count     INTEGER         NOT NULL DEFAULT 0,
    PRIMARY KEY (day, event_id)
);
"""


@pytest_asyncio.fixture
async def sa_engine():
    sqlalchemy_async = pytest.importorskip(
        "sqlalchemy.ext.asyncio", reason="sqlalchemy[asyncio] not installed",
    )
    create_async_engine = sqlalchemy_async.create_async_engine
    pytest.importorskip("asyncpg", reason="asyncpg not installed")
    from sqlalchemy.pool import NullPool

    engine = create_async_engine(_sa_db_url(), poolclass=NullPool)
    try:
        async with engine.connect() as probe:
            await probe.close()
    except Exception as exc:  # noqa: BLE001
        await engine.dispose()
        pytest.skip(f"Live PG unavailable ({exc!s}); skipping drain spawner tests.")
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def drain_env(
    sa_engine, monkeypatch  # noqa: ANN001
) -> AsyncIterator[Tuple[str, Any]]:
    """Provision a throwaway schema with ``tasks`` + ``storage`` + ``events``.

    Patches ``DYNASTORE_TASK_SCHEMA`` so ``get_task_schema()`` resolves to
    the throwaway schema. Yields ``(task_schema, engine)``.
    """
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )

    token = generate_id_hex()[:10]
    task_schema = f"drain_spawner_{token}"
    monkeypatch.setenv("DYNASTORE_TASK_SCHEMA", task_schema)

    async with managed_transaction(sa_engine) as conn:
        await DQLQuery(
            f'CREATE SCHEMA IF NOT EXISTS "{task_schema}"',
            result_handler=ResultHandler.NONE,
        ).execute(conn)
        await DQLQuery(
            _TASKS_DDL.format(schema=task_schema),
            result_handler=ResultHandler.NONE,
        ).execute(conn)
        await DQLQuery(
            _STORAGE_DDL.format(schema=task_schema),
            result_handler=ResultHandler.NONE,
        ).execute(conn)
        await DQLQuery(
            _EVENTS_DDL.format(schema=task_schema),
            result_handler=ResultHandler.NONE,
        ).execute(conn)

    try:
        yield task_schema, sa_engine
    finally:
        async with managed_transaction(sa_engine) as conn:
            try:
                await DQLQuery(
                    f'DROP SCHEMA IF EXISTS "{task_schema}" CASCADE',
                    result_handler=ResultHandler.NONE,
                ).execute(conn)
            except Exception:  # noqa: BLE001 — best-effort teardown
                pass


async def _fetch_tasks(engine: Any, task_schema: str) -> List[Dict[str, Any]]:
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )
    async with managed_transaction(engine) as conn:
        return await DQLQuery(
            f"SELECT task_type, status, dedup_key FROM {task_schema}.tasks",
            result_handler=ResultHandler.ALL_DICTS,
        ).execute(conn) or []


async def _count_by_type(engine: Any, task_schema: str, task_type: str) -> int:
    return len(
        [
            r for r in await _fetch_tasks(engine, task_schema)
            if r["task_type"] == task_type
        ]
    )


async def _seed_storage_backlog(engine: Any, task_schema: str) -> None:
    """Insert one 'ready' tasks.storage row — the recovery tick's backlog gate."""
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )
    async with managed_transaction(engine) as conn:
        await DQLQuery(
            f"INSERT INTO {task_schema}.storage"
            f" (op_id, catalog_id, driver_id, op, status)"
            f" VALUES (:op_id, 'tenant_a', 'es_driver', 'upsert', 'ready')",
            result_handler=ResultHandler.NONE,
        ).execute(conn, op_id=str(uuid4()))


async def _seed_events_backlog(engine: Any, task_schema: str) -> None:
    """Insert one PENDING tasks.events row — the recovery tick's backlog gate."""
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )
    async with managed_transaction(engine) as conn:
        await DQLQuery(
            f"INSERT INTO {task_schema}.events"
            f" (event_id, catalog_id, event_type, status)"
            f" VALUES (:event_id, 'tenant_a', 'catalog_creation', 'PENDING')",
            result_handler=ResultHandler.NONE,
        ).execute(conn, event_id=str(uuid4()))


async def _seed_drain_task(
    engine: Any,
    task_schema: str,
    *,
    task_type: str,
    status: str,
    timestamp_offset: str = "",  # e.g. "- INTERVAL '10 minutes'"
    locked_until_offset: str = "",  # e.g. "+ INTERVAL '10 minutes'" / "- INTERVAL '1 minute'"
) -> None:
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )
    ts_expr = f"now() {timestamp_offset}" if timestamp_offset else "now()"
    locked_expr = f"now() {locked_until_offset}" if locked_until_offset else "NULL"
    async with managed_transaction(engine) as conn:
        await DQLQuery(
            f"INSERT INTO {task_schema}.tasks"
            f" (task_id, catalog_id, scope, task_type, type, execution_mode,"
            f"  status, dedup_key, timestamp, locked_until)"
            f" VALUES (:task_id, 'platform', 'platform', :task_type,"
            f"         'task', 'ASYNCHRONOUS', :status, :task_type,"
            f"         {ts_expr}, {locked_expr})",
            result_handler=ResultHandler.NONE,
        ).execute(
            conn, task_id=str(uuid4()), task_type=task_type, status=status,
        )


def _make_service(*, wedge_grace_seconds: float = 300.0) -> Any:
    from dynastore.modules.tasks.drain_spawner import DrainSpawnerService
    return DrainSpawnerService(interval_s=120.0, wedge_grace_seconds=wedge_grace_seconds)


def _make_ctx(engine: Any) -> Any:
    import asyncio

    from dynastore.tools.background_service import ServiceContext

    return ServiceContext(
        engine=engine,
        shutdown=asyncio.Event(),
        is_ephemeral=False,
        name="test",
    )


@pytest.mark.asyncio
async def test_tick_with_no_backlog_spawns_nothing(drain_env):
    task_schema, engine = drain_env
    service = _make_service()
    ctx = _make_ctx(engine)

    await service.tick(ctx)

    assert await _fetch_tasks(engine, task_schema) == []


@pytest.mark.asyncio
async def test_tick_spawns_storage_drain_only_when_only_storage_has_backlog(drain_env):
    task_schema, engine = drain_env
    await _seed_storage_backlog(engine, task_schema)

    service = _make_service()
    ctx = _make_ctx(engine)
    await service.tick(ctx)

    rows = await _fetch_tasks(engine, task_schema)
    assert {r["task_type"] for r in rows} == {"storage_drain"}
    assert await _count_by_type(engine, task_schema, "storage_drain") == 1


@pytest.mark.asyncio
async def test_tick_spawns_both_when_both_outboxes_have_backlog(drain_env):
    task_schema, engine = drain_env
    await _seed_storage_backlog(engine, task_schema)
    await _seed_events_backlog(engine, task_schema)

    service = _make_service()
    ctx = _make_ctx(engine)
    await service.tick(ctx)

    rows = await _fetch_tasks(engine, task_schema)
    assert {r["task_type"] for r in rows} == {"event_drain", "storage_drain"}
    assert all(r["status"] == "PENDING" for r in rows)


@pytest.mark.asyncio
async def test_live_pending_row_still_blocks(drain_env):
    """A fresh (non-wedged) PENDING storage_drain row must still block a
    duplicate spawn — the recovery tick must not race a healthy drain."""
    task_schema, engine = drain_env
    await _seed_storage_backlog(engine, task_schema)
    await _seed_drain_task(engine, task_schema, task_type="storage_drain", status="PENDING")

    service = _make_service(wedge_grace_seconds=300.0)
    ctx = _make_ctx(engine)
    await service.tick(ctx)

    assert await _count_by_type(engine, task_schema, "storage_drain") == 1


@pytest.mark.asyncio
async def test_live_active_row_still_blocks(drain_env):
    """An ACTIVE storage_drain row with a lease that has NOT expired must
    still block a duplicate spawn."""
    task_schema, engine = drain_env
    await _seed_storage_backlog(engine, task_schema)
    await _seed_drain_task(
        engine, task_schema, task_type="storage_drain", status="ACTIVE",
        locked_until_offset="+ INTERVAL '10 minutes'",
    )

    service = _make_service(wedge_grace_seconds=300.0)
    ctx = _make_ctx(engine)
    await service.tick(ctx)

    assert await _count_by_type(engine, task_schema, "storage_drain") == 1


@pytest.mark.asyncio
async def test_wedged_pending_row_does_not_block_recovery(drain_env):
    """A PENDING storage_drain row older than the wedge-grace window (no
    dispatcher ever claimed it) must NOT block the recovery tick — this is
    the exact #2715 scenario: a crash-looping/wedged drain silencing every
    later co-transactional trigger insert."""
    task_schema, engine = drain_env
    await _seed_storage_backlog(engine, task_schema)
    await _seed_drain_task(
        engine, task_schema, task_type="storage_drain", status="PENDING",
        timestamp_offset="- INTERVAL '20 minutes'",
    )

    service = _make_service(wedge_grace_seconds=300.0)  # 5 min grace < 20 min age
    ctx = _make_ctx(engine)
    await service.tick(ctx)

    rows = [
        r for r in await _fetch_tasks(engine, task_schema)
        if r["task_type"] == "storage_drain"
    ]
    assert len(rows) == 2  # the wedged row + one fresh recovery row
    assert all(r["status"] == "PENDING" for r in rows)


@pytest.mark.asyncio
async def test_wedged_active_row_does_not_block_recovery(drain_env):
    """An ACTIVE storage_drain row whose lease already expired (owning
    worker died mid-run) must NOT block the recovery tick."""
    task_schema, engine = drain_env
    await _seed_storage_backlog(engine, task_schema)
    await _seed_drain_task(
        engine, task_schema, task_type="storage_drain", status="ACTIVE",
        locked_until_offset="- INTERVAL '10 minutes'",
    )

    service = _make_service(wedge_grace_seconds=300.0)
    ctx = _make_ctx(engine)
    await service.tick(ctx)

    rows = [
        r for r in await _fetch_tasks(engine, task_schema)
        if r["task_type"] == "storage_drain"
    ]
    assert len(rows) == 2  # the wedged ACTIVE row + one fresh recovery row
    statuses = {r["status"] for r in rows}
    assert statuses == {"ACTIVE", "PENDING"}


@pytest.mark.asyncio
async def test_terminal_row_does_not_block_fresh_spawn(drain_env):
    """A COMPLETED row from a prior drain run must not block the next tick
    from spawning a fresh PENDING row (mirrors the terminal-set exclusion
    already covered for the per-write trigger helpers)."""
    task_schema, engine = drain_env
    await _seed_storage_backlog(engine, task_schema)
    await _seed_drain_task(engine, task_schema, task_type="storage_drain", status="COMPLETED")

    service = _make_service()
    ctx = _make_ctx(engine)
    await service.tick(ctx)

    rows = [
        r for r in await _fetch_tasks(engine, task_schema)
        if r["task_type"] == "storage_drain"
    ]
    assert len(rows) == 2  # the seeded COMPLETED row + one fresh PENDING row
    assert {r["status"] for r in rows} == {"COMPLETED", "PENDING"}


@pytest.mark.asyncio
async def test_repeated_ticks_are_idempotent(drain_env):
    """Overlapping/repeated ticks must not pile up duplicate PENDING rows —
    the dedup guard already enforced by the underlying trigger helpers makes
    a second tick a no-op while the first spawn is still live."""
    task_schema, engine = drain_env
    await _seed_storage_backlog(engine, task_schema)
    await _seed_events_backlog(engine, task_schema)

    service = _make_service()
    ctx = _make_ctx(engine)
    for _ in range(3):
        await service.tick(ctx)

    assert await _count_by_type(engine, task_schema, "event_drain") == 1
    assert await _count_by_type(engine, task_schema, "storage_drain") == 1
