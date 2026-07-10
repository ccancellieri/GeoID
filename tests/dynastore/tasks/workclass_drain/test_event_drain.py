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

"""Live-PG tests for EventDrainTask (#1807 PR-5b / #2120).

Each test runs against a per-test throwaway schema; ``DYNASTORE_TASK_SCHEMA``
is patched so every ``get_task_schema()`` call resolves to it.

Scenarios covered:
1. Claim + fence: PENDING rows become PROCESSING with claim_version += 1 and
   owner_id stamped.
2. Disjoint concurrent claims (FOR UPDATE SKIP LOCKED).
3. Stale-claim fence (#1945): ownerA (cv=1) reclaimed as ownerB (cv=2);
   ownerA's mark_done CAS hits 0 rows; ownerB's hits 1.
4. Stale-PROCESSING reclaim: an expired lease (locked_until in the past) is
   reclaimable; a fresh lease is not.
5. Delivery success -> COMPLETED (rows are NOT deleted; retention drops them).
6. Delivery failure -> PENDING with backoff; attempts exhausted -> DEAD_LETTER.
7. No EventBus in the process -> every claimed row retried, never dropped.
8. No listeners for the event_type -> COMPLETED (successful no-op delivery).
9. Drain trigger: emit_event_row inserts exactly ONE pending event_drain
   task row (dedup) per outer transaction and rolls back with it.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple
from uuid import uuid4

import pytest
import pytest_asyncio

from dynastore.tools.identifiers import generate_id_hex
from tests.dynastore.test_utils.engine_mocks import make_fake_async_engine


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _sa_db_url() -> str:
    url = os.getenv(
        "DATABASE_URL",
        "postgresql://testuser:testpassword@localhost:54320/gis_dev",
    )
    if not url.startswith("postgresql+asyncpg://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


# Flat (un-partitioned) events for tests — mirrors the production column
# set including the PR-5b additions (event_type NOT NULL, error_message).
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
    claim_version   INTEGER         NOT NULL DEFAULT 0,
    owner_id        TEXT,
    locked_until    TIMESTAMPTZ,
    retry_count     INTEGER         NOT NULL DEFAULT 0,
    max_retries     INTEGER,
    error_message   TEXT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    processed_at    TIMESTAMPTZ,
    PRIMARY KEY (day, event_id)
);
"""

# Minimal flat tasks table for the trigger test — only the columns written by
# _enqueue_event_drain_trigger (verified against GLOBAL_TASKS_TABLE_DDL).
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
    PRIMARY KEY (timestamp, task_id)
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
        pytest.skip(f"Live PG unavailable ({exc!s}); skipping drain tests.")
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def drain_env(
    sa_engine, monkeypatch  # noqa: ANN001
) -> AsyncIterator[Tuple[str, Any]]:
    """Provision a throwaway schema with events + tasks tables.

    Patches ``DYNASTORE_TASK_SCHEMA`` so ``get_task_schema()`` resolves to the
    throwaway schema. Yields ``(task_schema, engine)``.
    """
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )

    token = generate_id_hex()[:10]
    task_schema = f"pr5b_tasks_{token}"
    monkeypatch.setenv("DYNASTORE_TASK_SCHEMA", task_schema)

    async with managed_transaction(sa_engine) as conn:
        await DQLQuery(
            f'CREATE SCHEMA IF NOT EXISTS "{task_schema}"',
            result_handler=ResultHandler.NONE,
        ).execute(conn)
        await DQLQuery(
            _EVENTS_DDL.format(schema=task_schema),
            result_handler=ResultHandler.NONE,
        ).execute(conn)
        await DQLQuery(
            _TASKS_DDL.format(schema=task_schema),
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _seed_events(
    engine: Any,
    task_schema: str,
    *,
    n: int = 3,
    event_type: str = "catalog_creation",
    scope: str = "platform",
    catalog_id: str = "tenant_a",
    status: str = "PENDING",
    payload: Optional[Dict[str, Any]] = None,
    owner_id: Optional[str] = None,
    locked_until_offset: Optional[str] = None,  # e.g. "- INTERVAL '10 minutes'"
    claim_version: int = 0,
    retry_count: int = 0,
    max_retries: Optional[int] = None,
) -> List[str]:
    """Insert N rows into tasks.events; return list of event_id strings."""
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )

    payload_json = json.dumps(
        payload if payload is not None else {"args": [], "kwargs": {}}
    )
    event_ids = [str(uuid4()) for _ in range(n)]
    for event_id in event_ids:
        locked_expr = "NULL"
        if locked_until_offset is not None:
            locked_expr = f"now() {locked_until_offset}"

        sql = (
            f"INSERT INTO {task_schema}.events"
            f" (event_id, day, shard, catalog_id, scope, event_type, status,"
            f"  payload, claim_version, owner_id, locked_until, retry_count,"
            f"  max_retries)"
            f" VALUES (:event_id, CURRENT_DATE, 0, :catalog_id, :scope,"
            f"         :event_type, :status, CAST(:payload AS jsonb),"
            f"         :claim_version, :owner_id, {locked_expr}, :retry_count,"
            f"         :max_retries)"
        )
        async with managed_transaction(engine) as conn:
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn,
                event_id=event_id,
                catalog_id=catalog_id,
                scope=scope,
                event_type=event_type,
                status=status,
                payload=payload_json,
                claim_version=claim_version,
                owner_id=owner_id,
                retry_count=retry_count,
                max_retries=max_retries,
            )
    return event_ids


async def _fetch_rows(engine: Any, task_schema: str) -> List[Dict[str, Any]]:
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )
    async with managed_transaction(engine) as conn:
        return await DQLQuery(
            f"SELECT event_id, status, claim_version, owner_id, retry_count,"
            f"       locked_until, processed_at, error_message"
            f" FROM {task_schema}.events",
            result_handler=ResultHandler.ALL_DICTS,
        ).execute(conn) or []


async def _fetch_row(
    engine: Any, task_schema: str, event_id: str
) -> Optional[Dict[str, Any]]:
    for r in await _fetch_rows(engine, task_schema):
        if str(r["event_id"]) == event_id:
            return r
    return None


async def _count_tasks(
    engine: Any, task_schema: str, task_type: str = "event_drain"
) -> int:
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )
    async with managed_transaction(engine) as conn:
        return await DQLQuery(
            f"SELECT count(*) FROM {task_schema}.tasks WHERE task_type = :task_type",
            result_handler=ResultHandler.SCALAR,
        ).execute(conn, task_type=task_type) or 0


def _make_task(engine: Any, task_schema: str) -> Any:
    """Construct an EventDrainTask with a small batch size for test control."""
    from dynastore.tasks.workclass_drain.event_drain_task import (
        EventDrainTask,
    )
    return EventDrainTask(batch_size=10, lease_seconds=300)


class _FakeEventBus:
    """Minimal ``EventBusProtocol`` surface for the drain dispatch path.

    Records each ``dispatch_to_listeners`` call; optionally raises to simulate
    a failing handler.
    """

    def __init__(self, *, fail: bool = False, exc: Optional[Exception] = None) -> None:
        self.calls: List[Tuple[str, Any]] = []
        self._fail = fail
        self._exc = exc or RuntimeError("handler boom")

    async def dispatch_to_listeners(self, event_type: str, payload: Any) -> None:
        self.calls.append((event_type, payload))
        if self._fail:
            raise self._exc


# ---------------------------------------------------------------------------
# 1. Claim + fence
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_claim_sets_processing_and_bumps_claim_version(drain_env):
    task_schema, engine = drain_env
    await _seed_events(engine, task_schema, n=2)

    task = _make_task(engine, task_schema)
    owner_id = f"owner:{uuid4()}"

    claimed = await task._claim_batch(
        engine=engine, task_schema=task_schema, owner_id=owner_id,
    )
    assert len(claimed) == 2
    for r in claimed:
        assert r["owner_id"] == owner_id
        assert r["claim_version"] == 1  # started at 0, bumped to 1
        assert r["event_type"] == "catalog_creation"

    # The persisted rows are PROCESSING after claim.
    rows = await _fetch_rows(engine, task_schema)
    assert {r["status"] for r in rows} == {"PROCESSING"}


# ---------------------------------------------------------------------------
# 2. Disjoint concurrent claims
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_disjoint_concurrent_claims(drain_env):
    task_schema, engine = drain_env
    await _seed_events(engine, task_schema, n=4)

    task = _make_task(engine, task_schema)
    owner_a = f"ownerA:{uuid4()}"
    owner_b = f"ownerB:{uuid4()}"

    batch_a = await task._claim_batch(
        engine=engine, task_schema=task_schema, owner_id=owner_a,
    )
    batch_b = await task._claim_batch(
        engine=engine, task_schema=task_schema, owner_id=owner_b,
    )

    ids_a = {str(r["event_id"]) for r in batch_a}
    ids_b = {str(r["event_id"]) for r in batch_b}
    assert ids_a.isdisjoint(ids_b), f"overlap: {ids_a & ids_b}"
    assert len(ids_a | ids_b) == 4


# ---------------------------------------------------------------------------
# 3. Stale-claim fence (#1945)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stale_claim_fence_cas_prevents_double_finalization(drain_env):
    """ownerA claims (cv=1); the row is reclaimed by ownerB (cv=2); ownerA's
    mark_done CAS (cv=1) must be a no-op, ownerB's (cv=2) must win."""
    task_schema, engine = drain_env
    event_ids = await _seed_events(engine, task_schema, n=1)
    event_id = event_ids[0]

    task = _make_task(engine, task_schema)
    owner_a = f"ownerA:{uuid4()}"
    owner_b = f"ownerB:{uuid4()}"

    batch_a = await task._claim_batch(
        engine=engine, task_schema=task_schema, owner_id=owner_a,
    )
    assert len(batch_a) == 1
    row_a = batch_a[0]
    assert row_a["claim_version"] == 1

    # Simulate lease expiry so ownerB can reclaim the PROCESSING row.
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )
    async with managed_transaction(engine) as conn:
        await DQLQuery(
            f"UPDATE {task_schema}.events"
            f" SET locked_until = now() - INTERVAL '1 hour'"
            f" WHERE event_id = :event_id",
            result_handler=ResultHandler.NONE,
        ).execute(conn, event_id=event_id)

    batch_b = await task._claim_batch(
        engine=engine, task_schema=task_schema, owner_id=owner_b,
    )
    assert len(batch_b) == 1
    row_b = batch_b[0]
    assert row_b["claim_version"] == 2

    # ownerA's stale mark_done (cv=1) must match 0 rows.
    await task._mark_done(
        engine=engine, task_schema=task_schema, row=row_a, owner_id=owner_a,
    )
    after_a = await _fetch_row(engine, task_schema, event_id)
    assert after_a is not None
    assert after_a["status"] == "PROCESSING", (
        f"ownerA's stale mark_done should be a no-op; got {after_a['status']}"
    )

    # ownerB's mark_done (cv=2) must succeed.
    await task._mark_done(
        engine=engine, task_schema=task_schema, row=row_b, owner_id=owner_b,
    )
    after_b = await _fetch_row(engine, task_schema, event_id)
    assert after_b is not None
    assert after_b["status"] == "COMPLETED", (
        f"ownerB's mark_done should have won; got {after_b['status']}"
    )


# ---------------------------------------------------------------------------
# 4. Stale-PROCESSING reclaim
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stale_processing_is_reclaimable(drain_env):
    """A PROCESSING row whose lease expired is reclaimable; a fresh one is not."""
    task_schema, engine = drain_env

    stale_id = (await _seed_events(
        engine, task_schema, n=1,
        status="PROCESSING", owner_id="old_owner",
        locked_until_offset="- INTERVAL '1 hour'", claim_version=1,
    ))[0]
    fresh_id = (await _seed_events(
        engine, task_schema, n=1,
        status="PROCESSING", owner_id="fresh_owner",
        locked_until_offset="+ INTERVAL '5 minutes'", claim_version=1,
    ))[0]

    task = _make_task(engine, task_schema)
    owner_id = f"reaper:{uuid4()}"
    batch = await task._claim_batch(
        engine=engine, task_schema=task_schema, owner_id=owner_id,
    )
    claimed_ids = {str(r["event_id"]) for r in batch}

    assert stale_id in claimed_ids, "stale PROCESSING row must be reclaimable"
    assert fresh_id not in claimed_ids, "fresh PROCESSING row must NOT be reclaimable"


# ---------------------------------------------------------------------------
# 5. Delivery success -> COMPLETED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delivery_success_marks_completed(
    drain_env, monkeypatch  # noqa: ANN001
):
    task_schema, engine = drain_env
    await _seed_events(
        engine, task_schema, n=3,
        payload={"args": [1, 2], "kwargs": {"k": "v"}},
    )

    task = _make_task(engine, task_schema)
    bus = _FakeEventBus()
    monkeypatch.setattr(task, "_resolve_event_bus", lambda: bus)

    owner_id = f"owner:{uuid4()}"
    count = await task.drain_once(engine=engine, owner_id=owner_id)
    assert count == 3

    # Each event delivered exactly once, with the decoded payload dict.
    assert len(bus.calls) == 3
    for event_type, payload in bus.calls:
        assert event_type == "catalog_creation"
        assert payload == {"args": [1, 2], "kwargs": {"k": "v"}}

    rows = await _fetch_rows(engine, task_schema)
    assert {r["status"] for r in rows} == {"COMPLETED"}
    assert all(r["processed_at"] is not None for r in rows)
    assert all(r["owner_id"] is None for r in rows)


# ---------------------------------------------------------------------------
# 6. Delivery failure -> retry; exhaustion -> DEAD_LETTER
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delivery_failure_retries_with_backoff(
    drain_env, monkeypatch  # noqa: ANN001
):
    task_schema, engine = drain_env
    await _seed_events(engine, task_schema, n=1, retry_count=0, max_retries=3)

    task = _make_task(engine, task_schema)
    bus = _FakeEventBus(fail=True, exc=RuntimeError("listener failed"))
    monkeypatch.setattr(task, "_resolve_event_bus", lambda: bus)

    owner_id = f"owner:{uuid4()}"
    await task.drain_once(engine=engine, owner_id=owner_id)

    rows = await _fetch_rows(engine, task_schema)
    assert len(rows) == 1
    r = rows[0]
    assert r["status"] == "PENDING"
    assert r["retry_count"] == 1
    assert r["owner_id"] is None
    assert r["error_message"] == "listener failed"
    # locked_until pushed into the future by the backoff curve.
    now = datetime.now(timezone.utc)
    locked = r["locked_until"]
    assert locked is not None
    if locked.tzinfo is None:
        locked = locked.replace(tzinfo=timezone.utc)
    assert locked > now, f"locked_until ({locked}) should be in the future"


@pytest.mark.asyncio
async def test_delivery_failure_exhausted_goes_dead_letter(
    drain_env, monkeypatch  # noqa: ANN001
):
    task_schema, engine = drain_env
    # retry_count already at 2 with max_retries 3 -> next failure is terminal.
    await _seed_events(engine, task_schema, n=1, retry_count=2, max_retries=3)

    task = _make_task(engine, task_schema)
    bus = _FakeEventBus(fail=True)
    monkeypatch.setattr(task, "_resolve_event_bus", lambda: bus)

    owner_id = f"owner:{uuid4()}"
    await task.drain_once(engine=engine, owner_id=owner_id)

    rows = await _fetch_rows(engine, task_schema)
    assert len(rows) == 1
    r = rows[0]
    assert r["status"] == "DEAD_LETTER"
    assert r["retry_count"] == 3
    assert r["locked_until"] is None, "dead-lettered rows carry no retry delay"


@pytest.mark.asyncio
async def test_default_max_retries_applies_when_row_has_none(
    drain_env, monkeypatch  # noqa: ANN001
):
    """A row with NULL max_retries uses the task default (3): retry_count 2 ->
    3 dead-letters."""
    task_schema, engine = drain_env
    await _seed_events(engine, task_schema, n=1, retry_count=2, max_retries=None)

    task = _make_task(engine, task_schema)
    bus = _FakeEventBus(fail=True)
    monkeypatch.setattr(task, "_resolve_event_bus", lambda: bus)

    owner_id = f"owner:{uuid4()}"
    await task.drain_once(engine=engine, owner_id=owner_id)

    r = (await _fetch_rows(engine, task_schema))[0]
    assert r["status"] == "DEAD_LETTER"


# ---------------------------------------------------------------------------
# 7. No EventBus -> retry all (never drop)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_event_bus_retries_all(drain_env, monkeypatch):  # noqa: ANN001
    task_schema, engine = drain_env
    await _seed_events(engine, task_schema, n=3)

    task = _make_task(engine, task_schema)
    monkeypatch.setattr(task, "_resolve_event_bus", lambda: None)

    owner_id = f"owner:{uuid4()}"
    count = await task.drain_once(engine=engine, owner_id=owner_id)
    assert count == 3

    rows = await _fetch_rows(engine, task_schema)
    # Nothing delivered, nothing dropped — all back to PENDING for a capable pod.
    assert {r["status"] for r in rows} == {"PENDING"}
    assert all(r["retry_count"] == 1 for r in rows)
    assert all(
        r["error_message"] == "EventBusProtocol unavailable in drain process"
        for r in rows
    )


# ---------------------------------------------------------------------------
# 8. No listeners for the event_type -> COMPLETED (no-op success)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_listeners_marks_completed(drain_env, monkeypatch):  # noqa: ANN001
    """An event with no registered listeners is a successful no-op delivery —
    the real EventService.dispatch_to_listeners returns without raising, so the
    row is COMPLETED (matching the legacy empty-listener ACK)."""
    task_schema, engine = drain_env
    await _seed_events(engine, task_schema, n=2, event_type="unhandled_type")

    task = _make_task(engine, task_schema)
    # _FakeEventBus with fail=False models "no listeners" — returns w/o raising.
    bus = _FakeEventBus(fail=False)
    monkeypatch.setattr(task, "_resolve_event_bus", lambda: bus)

    owner_id = f"owner:{uuid4()}"
    await task.drain_once(engine=engine, owner_id=owner_id)

    rows = await _fetch_rows(engine, task_schema)
    assert {r["status"] for r in rows} == {"COMPLETED"}


# ---------------------------------------------------------------------------
# 9. Drain trigger (co-transactional dedup'd INSERT into tasks)
# ---------------------------------------------------------------------------


async def _publish(engine: Any, task_schema: str, *, item: str) -> None:
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.tasks.events.events_emit import emit_event_row

    async with managed_transaction(engine) as conn:
        await emit_event_row(
            conn,
            event_type="catalog_creation",
            scope="PLATFORM",
            catalog_id=task_schema,
            collection_id=None,
            identity_id=None,
            payload_str=json.dumps({"args": [item], "kwargs": {}}),
            shard=0,
        )


@pytest.mark.asyncio
async def test_drain_trigger_inserts_one_pending_task_row(drain_env):
    """A single event write inserts exactly one event_drain task row
    via the co-transactional drain trigger."""
    task_schema, engine = drain_env
    await _publish(engine, task_schema, item="a")

    assert await _count_tasks(engine, task_schema) == 1
    # And the event landed in tasks.events with its event_type.
    rows = await _fetch_rows(engine, task_schema)
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_drain_trigger_dedup_multiple_writes_one_row(drain_env):
    """Multiple writes coalesce to ONE pending drain task (dedup guard)."""
    task_schema, engine = drain_env
    for i in range(3):
        await _publish(engine, task_schema, item=f"item_{i}")

    assert await _count_tasks(engine, task_schema) == 1
    assert len(await _fetch_rows(engine, task_schema)) == 3


@pytest.mark.asyncio
async def test_drain_trigger_rolls_back_with_outer_transaction(drain_env):
    """An aborted outer transaction leaves no task row and no event row."""
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.tasks.events.events_emit import emit_event_row

    task_schema, engine = drain_env

    with pytest.raises(RuntimeError, match="simulated abort"):
        async with managed_transaction(engine) as conn:
            await emit_event_row(
                conn,
                event_type="catalog_creation",
                scope="PLATFORM",
                catalog_id=task_schema,
                collection_id=None,
                identity_id=None,
                payload_str=json.dumps({"args": [], "kwargs": {}}),
                shard=0,
            )
            raise RuntimeError("simulated abort")

    assert await _count_tasks(engine, task_schema) == 0
    assert len(await _fetch_rows(engine, task_schema)) == 0


# ---------------------------------------------------------------------------
# 10. Payload coercion (no DB) — valid shapes pass through; malformed shapes
#     degrade to {} but emit a WARNING so corruption is visible.
# ---------------------------------------------------------------------------


def test_coerce_payload_valid_shapes_pass_through():
    from dynastore.tasks.workclass_drain.event_drain_task import (
        EventDrainTask,
    )
    coerce = EventDrainTask._coerce_payload
    assert coerce({"args": [1], "kwargs": {"k": 2}}) == {"args": [1], "kwargs": {"k": 2}}
    assert coerce(None) == {}  # absent payload is a legitimate no-args delivery
    assert coerce('{"args": [3]}') == {"args": [3]}  # JSON string (asyncpg codec)


def test_coerce_payload_malformed_degrades_with_warning(caplog):
    import logging

    from dynastore.tasks.workclass_drain.event_drain_task import (
        EventDrainTask,
    )
    coerce = EventDrainTask._coerce_payload
    with caplog.at_level(logging.WARNING):
        assert coerce("[1, 2, 3]") == {}      # decodes to a non-object
        assert coerce("not valid json") == {}  # undecodable
        assert coerce(12345) == {}             # unexpected column type
    warnings = [r for r in caplog.records if "payload" in r.getMessage()]
    assert len(warnings) == 3, (
        f"each malformed payload must warn; got {[r.getMessage() for r in warnings]}"
    )


# ---------------------------------------------------------------------------
# 11. In-process wall-clock/row drain budget handoff (#2887) — serving tier only
#
# EventDrainTask's ``while True`` loop used to have no escape hatch — a large
# tasks.events backlog could pin a single execution open for hours. If
# cumulative elapsed wall-clock or cumulative claimed rows crosses
# TasksPluginConfig.event_drain_inprocess_max_seconds /
# event_drain_inprocess_max_rows while backlog rows still remain, an
# in-process (serving-tier) run() stops early and hands the remainder off to
# a fresh event_drain execution. The budget is SKIPPED ENTIRELY when this
# execution IS the async-writer Cloud Run Job (app_state.ephemeral_job) — the
# Job always drains to empty, unbounded, exactly like StorageDrainOffloadTask
# (a long single Job execution is no longer a hazard once #2887's Gap-A
# report_failure fix removed the actual OOM mechanism).
# Mirrors StorageDrainTask's byte/wall-clock self-escape test shape.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_hands_off_when_row_budget_exceeded_with_backlog_remaining():
    """A single over-budget batch stops the loop immediately (not drain to
    empty) and calls _handoff_to_offload_job exactly once."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from dynastore.tasks.workclass_drain.event_drain_task import EventDrainTask

    task = EventDrainTask(inprocess_max_rows=3, inprocess_max_seconds=999.0)
    fake_engine = make_fake_async_engine()
    fake_engine.dispose = AsyncMock()

    calls = {"n": 0}

    async def _fake_drain_once(*, engine, owner_id):
        calls["n"] += 1
        return 5  # exceeds the 3-row budget in a single batch; backlog likely remains

    handoff = AsyncMock()

    with (
        patch("sqlalchemy.ext.asyncio.create_async_engine", return_value=fake_engine),
        patch.object(task, "drain_once", new=_fake_drain_once),
        patch.object(task, "_handoff_to_offload_job", new=handoff),
    ):
        report = await task.run(MagicMock())

    handoff.assert_awaited_once_with(fake_engine)
    assert calls["n"] == 1, "must stop after the first over-budget batch, not loop to empty"
    assert report.metrics["drained"] == 5


@pytest.mark.asyncio
async def test_run_hands_off_when_time_budget_exceeded_with_backlog_remaining():
    """Slow delivery that blows the wall-clock budget triggers the same
    handoff, independent of row count (kept well under the row budget)."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock, patch

    from dynastore.tasks.workclass_drain.event_drain_task import EventDrainTask

    task = EventDrainTask(inprocess_max_rows=0, inprocess_max_seconds=0.02)
    fake_engine = make_fake_async_engine()
    fake_engine.dispose = AsyncMock()

    calls = {"n": 0}

    async def _fake_drain_once(*, engine, owner_id):
        calls["n"] += 1
        await asyncio.sleep(0.05)  # simulated slow delivery — well over the 0.02s budget
        return 3

    handoff = AsyncMock()

    with (
        patch("sqlalchemy.ext.asyncio.create_async_engine", return_value=fake_engine),
        patch.object(task, "drain_once", new=_fake_drain_once),
        patch.object(task, "_handoff_to_offload_job", new=handoff),
    ):
        await task.run(MagicMock())

    handoff.assert_awaited_once_with(fake_engine)
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_run_generous_budget_drains_to_empty_without_handoff():
    """A budget nothing in the run ever crosses drains to empty exactly like
    the pre-#2887 loop — no handoff is ever attempted."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from dynastore.tasks.workclass_drain.event_drain_task import EventDrainTask

    task = EventDrainTask(inprocess_max_rows=10**6, inprocess_max_seconds=999.0)
    fake_engine = make_fake_async_engine()
    fake_engine.dispose = AsyncMock()

    calls = {"n": 0}

    async def _fake_drain_once(*, engine, owner_id):
        calls["n"] += 1
        if calls["n"] <= 2:
            return 5
        return 0

    handoff = AsyncMock()

    with (
        patch("sqlalchemy.ext.asyncio.create_async_engine", return_value=fake_engine),
        patch.object(task, "drain_once", new=_fake_drain_once),
        patch.object(task, "_handoff_to_offload_job", new=handoff),
    ):
        report = await task.run(MagicMock())

    handoff.assert_not_awaited()
    assert calls["n"] == 3, "must loop until drain_once reports 0 (drain to empty)"
    assert report.metrics["drained"] == 10


@pytest.mark.asyncio
async def test_handoff_to_offload_job_uses_distinct_dedup_key():
    """_handoff_to_offload_job must enqueue its trigger under a dedup_key
    distinct from the live co-transactional trigger's ("event_drain") — the
    currently-running execution's own row is still non-terminal (ACTIVE), so
    reusing that key would make the handoff a permanent no-op against itself.
    """
    from unittest.mock import AsyncMock, MagicMock, patch

    from dynastore.tasks.workclass_drain.event_drain_task import EventDrainTask

    task = EventDrainTask()
    fake_engine = MagicMock()
    trigger_mock = AsyncMock()

    with (
        patch(
            "dynastore.modules.db_config.query_executor.managed_transaction",
        ) as managed_tx_mock,
        patch(
            "dynastore.modules.tasks.events.events_emit._enqueue_event_drain_trigger",
            new=trigger_mock,
        ),
    ):
        fake_conn = MagicMock()
        managed_tx_mock.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
        managed_tx_mock.return_value.__aexit__ = AsyncMock(return_value=False)

        await task._handoff_to_offload_job(fake_engine)

    trigger_mock.assert_awaited_once_with(fake_conn, dedup_key="event_drain_handoff")


@pytest.mark.asyncio
async def test_run_drains_to_empty_ignoring_budget_when_running_as_async_writer_job():
    """When app_state.ephemeral_job is True (the async-writer Cloud Run Job
    entrypoint, stamped by main_task.py), the in-process budget must be
    skipped entirely — the Job always drains to empty, unbounded, exactly
    like StorageDrainOffloadTask. A budget tiny enough to trip on the very
    first batch must NOT trigger a handoff here."""
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock, patch

    from dynastore.tasks.workclass_drain.event_drain_task import EventDrainTask

    job_app_state = SimpleNamespace(ephemeral_job=True)
    task = EventDrainTask(
        app_state=job_app_state, inprocess_max_rows=1, inprocess_max_seconds=0.001,
    )
    fake_engine = make_fake_async_engine()
    fake_engine.dispose = AsyncMock()

    calls = {"n": 0}

    async def _fake_drain_once(*, engine, owner_id):
        calls["n"] += 1
        if calls["n"] <= 3:
            return 5  # would blow the 1-row / 0.001s budget many times over
        return 0

    handoff = AsyncMock()
    resolve_budget = AsyncMock()

    with (
        patch("sqlalchemy.ext.asyncio.create_async_engine", return_value=fake_engine),
        patch.object(task, "drain_once", new=_fake_drain_once),
        patch.object(task, "_handoff_to_offload_job", new=handoff),
        patch.object(task, "_resolve_inprocess_budget", new=resolve_budget),
    ):
        report = await task.run(MagicMock())

    handoff.assert_not_awaited()
    resolve_budget.assert_not_awaited(), "the Job path must not even resolve the budget config"
    assert calls["n"] == 4, "must loop until drain_once reports 0 (drain to empty)"
    assert report.metrics["drained"] == 15


@pytest.mark.asyncio
async def test_handoff_to_offload_job_logs_when_insert_is_a_noop(caplog):
    """A self-blocked handoff (0 rows inserted — a prior 'event_drain_handoff'
    row is still non-terminal, the no-async-writer-job residual case) must be
    observable via a log line, not swallowed silently."""
    import logging
    from unittest.mock import AsyncMock, MagicMock, patch

    from dynastore.tasks.workclass_drain.event_drain_task import EventDrainTask

    task = EventDrainTask()
    fake_engine = MagicMock()
    trigger_mock = AsyncMock(return_value=0)  # dedup guard blocked the insert

    with (
        patch(
            "dynastore.modules.db_config.query_executor.managed_transaction",
        ) as managed_tx_mock,
        patch(
            "dynastore.modules.tasks.events.events_emit._enqueue_event_drain_trigger",
            new=trigger_mock,
        ),
        caplog.at_level(logging.INFO),
    ):
        fake_conn = MagicMock()
        managed_tx_mock.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
        managed_tx_mock.return_value.__aexit__ = AsyncMock(return_value=False)

        await task._handoff_to_offload_job(fake_engine)

    trigger_mock.assert_awaited_once_with(fake_conn, dedup_key="event_drain_handoff")
    assert any(
        "no-op" in r.getMessage() for r in caplog.records
    ), "a self-blocked handoff must be logged, not silently swallowed"


@pytest.mark.asyncio
async def test_handoff_to_offload_job_logs_success_when_insert_lands(caplog):
    """The success path (1 row inserted) logs the handoff too, distinctly
    from the no-op path above."""
    import logging
    from unittest.mock import AsyncMock, MagicMock, patch

    from dynastore.tasks.workclass_drain.event_drain_task import EventDrainTask

    task = EventDrainTask()
    fake_engine = MagicMock()
    trigger_mock = AsyncMock(return_value=1)

    with (
        patch(
            "dynastore.modules.db_config.query_executor.managed_transaction",
        ) as managed_tx_mock,
        patch(
            "dynastore.modules.tasks.events.events_emit._enqueue_event_drain_trigger",
            new=trigger_mock,
        ),
        caplog.at_level(logging.INFO),
    ):
        fake_conn = MagicMock()
        managed_tx_mock.return_value.__aenter__ = AsyncMock(return_value=fake_conn)
        managed_tx_mock.return_value.__aexit__ = AsyncMock(return_value=False)

        await task._handoff_to_offload_job(fake_engine)

    assert any(
        "handed off remainder" in r.getMessage() for r in caplog.records
    )
    assert not any("no-op" in r.getMessage() for r in caplog.records)
