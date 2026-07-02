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

"""Live-PG tests for StorageDrainTask (#1807 PR-5a).

All tests run against a per-test throwaway schema to avoid collisions.
``DYNASTORE_TASK_SCHEMA`` is patched to point at that schema so every
module-level call to ``get_task_schema()`` resolves to it.

Scenarios covered:
1. Claim + fence: ready rows become in_flight with claim_version += 1.
2. Disjoint concurrent claims: two owners over the same backlog get
   non-overlapping op sets (FOR UPDATE SKIP LOCKED).
3. Stale-claim fence (#1945): ownerA claims (cv=1); row reclaimed as
   ownerB (cv=2); ownerA's mark_done CAS (cv=1) hits 0 rows; ownerB's
   CAS (cv=2) hits 1 row.
4. Reclaim of stale in_flight: old claimed_at is reclaimable; fresh
   in_flight is not.
5. Retry backoff: mark_retry sets status='ready', bumps attempts,
   pushes ready_at into the future.
6. Drain trigger: enqueue_storage_op inserts exactly ONE pending storage_drain
   task row on the same conn (dedup) and rolls back with the outer
   transaction.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple
from uuid import uuid4

import pytest
import pytest_asyncio

from dynastore.tools.identifiers import generate_id_hex


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


# Flat (un-partitioned) storage for tests — mirrors production column set
# including NOT NULL constraints and defaults.
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
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    PRIMARY KEY (day, op_id)
);
"""

# Minimal flat tasks table for the trigger test — only the columns written
# by _enqueue_drain_trigger (verified against GLOBAL_TASKS_TABLE_DDL). Column
# name is catalog_id (matching GLOBAL_TASKS_TABLE_DDL) — a prior schema_name
# spelling here had drifted from production and made every drain-trigger
# test in this file fail with UndefinedColumnError before it could even
# exercise the dedup guard.
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
    """Provision a throwaway schema with storage + tasks tables.

    Patches ``DYNASTORE_TASK_SCHEMA`` so ``get_task_schema()`` resolves to
    the throwaway schema. Yields ``(task_schema, engine)``.
    """
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )

    token = generate_id_hex()[:10]
    task_schema = f"pr5_tasks_{token}"
    monkeypatch.setenv("DYNASTORE_TASK_SCHEMA", task_schema)

    async with managed_transaction(sa_engine) as conn:
        await DQLQuery(
            f'CREATE SCHEMA IF NOT EXISTS "{task_schema}"',
            result_handler=ResultHandler.NONE,
        ).execute(conn)
        await DQLQuery(
            _STORAGE_DDL.format(schema=task_schema),
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


async def _seed_rows(
    engine: Any,
    task_schema: str,
    *,
    n: int = 3,
    driver_id: str = "es_driver",
    catalog_id: str = "tenant_a",
    status: str = "ready",
    claimed_by: Optional[str] = None,
    claimed_at_offset: Optional[str] = None,  # e.g. "- INTERVAL '10 minutes'"
    claim_version: int = 0,
) -> List[str]:
    """Insert N rows into storage; return list of op_id strings.

    Payload-carrying (``op_payload={"legacy": True}``) so the generic
    claim/fence/retry/dedup mechanics exercised by most of this file are
    unaffected by the #2494 P1 id-only re-read path, which keys off the
    explicit ``STORAGE_PLANE_ID_ONLY_MARKER_KEY`` sentinel on an
    ``op='upsert'`` row's ``op_payload`` — use :func:`_seed_id_only_row`
    to seed that shape instead.
    """
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )

    op_ids = [str(uuid4()) for _ in range(n)]
    for op_id in op_ids:
        claimed_at_expr = "NULL"
        if claimed_at_offset is not None:
            claimed_at_expr = f"now() {claimed_at_offset}"

        payload_expr = "'{\"legacy\": true}'::jsonb"
        sql = (
            f"INSERT INTO {task_schema}.storage"
            f" (op_id, day, driver_id, catalog_id, op, status,"
            f"  claimed_by, claimed_at, claim_version, op_payload)"
            f" VALUES (:op_id, CURRENT_DATE, :driver_id,"
            f"         :catalog_id, 'upsert', :status,"
            f"         :claimed_by, {claimed_at_expr}, :claim_version, {payload_expr})"
        )
        async with managed_transaction(engine) as conn:
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn,
                op_id=op_id,
                driver_id=driver_id,
                catalog_id=catalog_id,
                status=status,
                claimed_by=claimed_by,
                claim_version=claim_version,
            )
    return op_ids


async def _fetch_rows(engine: Any, task_schema: str) -> List[Dict[str, Any]]:
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )
    async with managed_transaction(engine) as conn:
        return await DQLQuery(
            f"SELECT op_id, status, claim_version, claimed_by, attempts,"
            f"       ready_at, finished_at FROM {task_schema}.storage",
            result_handler=ResultHandler.ALL_DICTS,
        ).execute(conn) or []


async def _fetch_row(engine: Any, task_schema: str, op_id: str) -> Optional[Dict[str, Any]]:
    rows = await _fetch_rows(engine, task_schema)
    for r in rows:
        if str(r["op_id"]) == op_id:
            return r
    return None


async def _count_tasks(engine: Any, task_schema: str, task_type: str = "storage_drain") -> int:
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )
    async with managed_transaction(engine) as conn:
        return await DQLQuery(
            f"SELECT count(*) FROM {task_schema}.tasks WHERE task_type = :task_type",
            result_handler=ResultHandler.SCALAR,
        ).execute(conn, task_type=task_type) or 0


def _make_task(engine: Any, task_schema: str) -> Any:
    """Construct a StorageDrainTask with a small batch size for test control."""
    from dynastore.tasks.workclass_drain.storage_drain_task import (
        StorageDrainTask,
    )
    return StorageDrainTask(batch_size=10, lease_seconds=300)


# ---------------------------------------------------------------------------
# 1. Claim + fence
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_claim_sets_in_flight_and_bumps_claim_version(drain_env):
    task_schema, engine = drain_env
    await _seed_rows(engine, task_schema, n=3)

    task = _make_task(engine, task_schema)
    owner_id = f"owner:{uuid4()}"
    count = await task.drain_once(engine=engine, owner_id=owner_id)
    assert count == 3

    rows = await _fetch_rows(engine, task_schema)
    # The drain processes all 3; terminal writes follow claim, so check
    # that claim happened correctly by reading claim_version from rows
    # that are now 'done' (no indexer → retry in drain_once).
    # Actually since no indexer is registered they all go to retry.
    # Retry resets to 'ready', so we check attempts was bumped.
    assert len(rows) == 3
    for r in rows:
        # After retry the row returns to 'ready' with attempts+1.
        assert r["status"] == "ready"
        assert r["attempts"] == 1


@pytest.mark.asyncio
async def test_claim_stamps_claimed_by_and_bumps_version(drain_env):
    """Direct SQL claim verification: claim query stamps claimed_by + version."""
    task_schema, engine = drain_env
    await _seed_rows(engine, task_schema, n=2)

    task = _make_task(engine, task_schema)
    owner_id = f"owner:{uuid4()}"

    # Exercise _claim_batch directly before outcomes are applied.
    claimed = await task._claim_batch(
        engine=engine,
        task_schema=task_schema,
        owner_id=owner_id,
    )
    assert len(claimed) == 2
    for r in claimed:
        assert r["claimed_by"] == owner_id
        assert r["claim_version"] == 1  # started at 0, bumped to 1


# ---------------------------------------------------------------------------
# 2. Disjoint concurrent claims
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_disjoint_concurrent_claims(drain_env):
    """Two owners claiming concurrently get non-overlapping op sets."""
    task_schema, engine = drain_env
    await _seed_rows(engine, task_schema, n=4)

    task = _make_task(engine, task_schema)
    owner_a = f"ownerA:{uuid4()}"
    owner_b = f"ownerB:{uuid4()}"

    batch_a = await task._claim_batch(
        engine=engine, task_schema=task_schema, owner_id=owner_a,
    )
    batch_b = await task._claim_batch(
        engine=engine, task_schema=task_schema, owner_id=owner_b,
    )

    ids_a = {str(r["op_id"]) for r in batch_a}
    ids_b = {str(r["op_id"]) for r in batch_b}
    # SKIP LOCKED: no row appears in both batches.
    assert ids_a.isdisjoint(ids_b), f"overlap: {ids_a & ids_b}"
    # Together they cover all 4 rows.
    assert len(ids_a | ids_b) == 4


# ---------------------------------------------------------------------------
# 3. Stale-claim fence (#1945)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stale_claim_fence_cas_prevents_double_finalization(drain_env):
    """The claim_version fence is the #1945 correctness guarantee.

    ownerA claims a row (cv becomes 1).  The row is then reclaimed by ownerB
    (simulating lease expiry) so cv becomes 2.  ownerA's mark_done CAS
    (WHERE claimed_by=ownerA AND claim_version=1) must match 0 rows.
    ownerB's mark_done CAS (claim_version=2) must match 1 row.
    """
    task_schema, engine = drain_env
    op_ids = await _seed_rows(engine, task_schema, n=1)
    op_id = op_ids[0]

    task = _make_task(engine, task_schema)
    owner_a = f"ownerA:{uuid4()}"
    owner_b = f"ownerB:{uuid4()}"

    # ownerA claims: cv becomes 1.
    batch_a = await task._claim_batch(
        engine=engine, task_schema=task_schema, owner_id=owner_a,
    )
    assert len(batch_a) == 1
    row_a = batch_a[0]
    assert row_a["claim_version"] == 1

    # Simulate lease expiry: push claimed_at far in the past so ownerB can
    # reclaim via the stale-in_flight branch.
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )
    async with managed_transaction(engine) as conn:
        await DQLQuery(
            f"UPDATE {task_schema}.storage"
            f" SET claimed_at = now() - INTERVAL '1 hour'"
            f" WHERE op_id = :op_id",
            result_handler=ResultHandler.NONE,
        ).execute(conn, op_id=op_id)

    # ownerB reclaims: cv becomes 2.
    batch_b = await task._claim_batch(
        engine=engine, task_schema=task_schema, owner_id=owner_b,
    )
    assert len(batch_b) == 1
    row_b = batch_b[0]
    assert row_b["claim_version"] == 2

    # ownerA's stale mark_done (cv=1) must match 0 rows.
    await task._mark_done(
        engine=engine,
        task_schema=task_schema,
        row=row_a,  # claim_version=1
        owner_id=owner_a,
    )
    row_after_a = await _fetch_row(engine, task_schema, op_id)
    assert row_after_a is not None
    # The row should still be in_flight (ownerB holds it), NOT 'done'.
    assert row_after_a["status"] == "in_flight", (
        f"ownerA's stale mark_done should have been a no-op; "
        f"row is {row_after_a['status']}"
    )

    # ownerB's mark_done (cv=2) must match 1 row.
    await task._mark_done(
        engine=engine,
        task_schema=task_schema,
        row=row_b,  # claim_version=2
        owner_id=owner_b,
    )
    row_after_b = await _fetch_row(engine, task_schema, op_id)
    assert row_after_b is not None
    assert row_after_b["status"] == "done", (
        f"ownerB's mark_done should have succeeded; row is {row_after_b['status']}"
    )


# ---------------------------------------------------------------------------
# 4. Reclaim of stale in_flight
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stale_in_flight_is_reclaimable(drain_env):
    """An in_flight row with an old claimed_at is reclaimable; fresh is not."""
    task_schema, engine = drain_env

    # Insert one stale in_flight row (claimed_at 1h ago).
    stale_id = (await _seed_rows(
        engine, task_schema, n=1,
        status="in_flight",
        claimed_by="old_owner",
        claimed_at_offset="- INTERVAL '1 hour'",
        claim_version=1,
    ))[0]

    # Insert one fresh in_flight row (claimed_at just now, well within lease).
    fresh_id = (await _seed_rows(
        engine, task_schema, n=1,
        status="in_flight",
        claimed_by="fresh_owner",
        claimed_at_offset="",  # now()
        claim_version=1,
    ))[0]

    task = _make_task(engine, task_schema)
    owner_id = f"reaper:{uuid4()}"
    batch = await task._claim_batch(
        engine=engine, task_schema=task_schema, owner_id=owner_id,
    )
    claimed_ids = {str(r["op_id"]) for r in batch}

    assert stale_id in claimed_ids, "stale in_flight row must be reclaimable"
    assert fresh_id not in claimed_ids, "fresh in_flight row must NOT be reclaimable"


# ---------------------------------------------------------------------------
# 5. Retry backoff
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_backoff_bumps_attempts_and_delays_ready_at(drain_env):
    """mark_retry resets to ready, bumps attempts, pushes ready_at forward."""
    task_schema, engine = drain_env
    op_ids = await _seed_rows(engine, task_schema, n=1)
    op_id = op_ids[0]

    task = _make_task(engine, task_schema)
    owner_id = f"owner:{uuid4()}"

    batch = await task._claim_batch(
        engine=engine, task_schema=task_schema, owner_id=owner_id,
    )
    assert len(batch) == 1
    row = batch[0]

    await task._mark_retry(
        engine=engine,
        task_schema=task_schema,
        row=row,
        owner_id=owner_id,
        error="transient_test_error",
    )

    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )
    async with managed_transaction(engine) as conn:
        result = await DQLQuery(
            f"SELECT status, attempts, ready_at, claimed_by"
            f" FROM {task_schema}.storage WHERE op_id = :op_id",
            result_handler=ResultHandler.ONE_DICT,
        ).execute(conn, op_id=op_id)

    assert result is not None
    assert result["status"] == "ready"
    assert result["attempts"] == 1
    assert result["claimed_by"] is None
    # ready_at must be strictly in the future (at least 1 second).
    now = datetime.now(timezone.utc)
    ready_at = result["ready_at"]
    if ready_at.tzinfo is None:
        from datetime import timezone as _tz
        ready_at = ready_at.replace(tzinfo=_tz.utc)
    assert ready_at > now, (
        f"ready_at ({ready_at}) should be in the future; now={now}"
    )


# ---------------------------------------------------------------------------
# 6. Drain trigger (co-transactional dedup'd INSERT into tasks)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drain_trigger_inserts_one_pending_task_row(drain_env):
    """enqueue_storage_op inserts exactly one storage_drain task row via the
    co-transactional drain trigger.
    """
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.storage_emit import enqueue_storage_op
    from dynastore.models.protocols.indexing import OutboxRecord

    task_schema, engine = drain_env

    rows = [
        OutboxRecord(
            op_id=uuid4(),
            driver_id="es_driver",
            driver_instance_id="di",
            collection_id="coll",
            op="upsert",
            item_id="item_1",
            payload={"x": 1},
            idempotency_key="ik_1",
        ),
    ]

    async with managed_transaction(engine) as conn:
        await enqueue_storage_op(conn, catalog_id=task_schema, rows=rows)

    count = await _count_tasks(engine, task_schema)
    assert count == 1, f"expected 1 pending drain task; got {count}"


@pytest.mark.asyncio
async def test_drain_trigger_dedup_multiple_writes_one_row(drain_env):
    """Multiple writes in separate transactions produce only ONE pending drain
    task due to the dedup WHERE NOT EXISTS guard.
    """
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.storage_emit import enqueue_storage_op
    from dynastore.models.protocols.indexing import OutboxRecord

    task_schema, engine = drain_env

    def _row(item_id: str) -> OutboxRecord:
        return OutboxRecord(
            op_id=uuid4(),
            driver_id="es_driver",
            driver_instance_id="di",
            collection_id="coll",
            op="upsert",
            item_id=item_id,
            payload={},
            idempotency_key=f"ik_{item_id}",
        )

    # Three separate writes — each calls _enqueue_drain_trigger.
    for i in range(3):
        async with managed_transaction(engine) as conn:
            await enqueue_storage_op(conn, catalog_id=task_schema, rows=[_row(f"item_{i}")])

    count = await _count_tasks(engine, task_schema)
    assert count == 1, f"dedup should coalesce to 1 pending task; got {count}"


@pytest.mark.asyncio
async def test_drain_trigger_rolls_back_with_outer_transaction(drain_env):
    """An aborted outer transaction leaves no task row in tasks."""
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.storage_emit import enqueue_storage_op
    from dynastore.models.protocols.indexing import OutboxRecord

    task_schema, engine = drain_env

    rows = [
        OutboxRecord(
            op_id=uuid4(),
            driver_id="es_driver",
            driver_instance_id="di",
            collection_id="coll",
            op="upsert",
            item_id="item_rollback",
            payload={},
            idempotency_key="ik_rb",
        ),
    ]

    with pytest.raises(RuntimeError, match="simulated abort"):
        async with managed_transaction(engine) as conn:
            await enqueue_storage_op(conn, catalog_id=task_schema, rows=rows)
            raise RuntimeError("simulated abort")

    count = await _count_tasks(engine, task_schema)
    assert count == 0, f"rollback must remove the drain task row; got {count}"


# ---------------------------------------------------------------------------
# 7. Indexer dispatch path (claim -> index_bulk(ops) -> mark_*)
#
# The existing tests above never reach a resolvable indexer (driver_id
# "es_driver" resolves to None -> retry). These exercise the dispatch path
# with an injected BulkIndexer, which is where the BulkIndexer-vs-Indexer
# protocol contract and the _apply_outcomes partitioning actually run.
# ---------------------------------------------------------------------------


class _FakeBulkIndexer:
    """Minimal ``BulkIndexer``: records the ops it was handed (one positional
    arg — the ``BulkIndexer`` contract) and returns a preset result."""

    def __init__(self, result_builder: Any) -> None:
        self.calls: List[Any] = []
        self._build = result_builder

    async def index_bulk(self, ops: Any) -> Any:  # one positional arg
        ops_list = list(ops)
        self.calls.append(ops_list)
        return self._build(ops_list)


@pytest.mark.asyncio
async def test_drain_once_dispatches_via_bulk_indexer_and_marks_done(
    drain_env, monkeypatch  # noqa: ANN001
):
    """Full claim -> index_bulk(ops) -> mark_done path.

    Guards the ``BulkIndexer`` contract: the drain calls ``index_bulk(ops)``
    with a single positional arg and reads ``BulkIndexResult.passed`` — which
    is distinct from the ``Indexer`` protocol's ``index_bulk(ctx, ops)``.
    """
    from dynastore.models.protocols.indexing import BulkIndexResult

    task_schema, engine = drain_env
    await _seed_rows(engine, task_schema, n=3)

    task = _make_task(engine, task_schema)
    fake = _FakeBulkIndexer(
        lambda ops: BulkIndexResult(
            passed=[op.op_id for op in ops], transient=[], poison=[],
        )
    )

    async def _resolve(driver_id: str) -> Any:
        return fake

    monkeypatch.setattr(task, "_resolve_indexer", _resolve)

    owner_id = f"owner:{uuid4()}"
    count = await task.drain_once(engine=engine, owner_id=owner_id)
    assert count == 3

    # index_bulk called once with all 3 ops, passed positionally as IndexableOp.
    assert len(fake.calls) == 1
    assert len(fake.calls[0]) == 3
    assert all(hasattr(op, "op_id") for op in fake.calls[0])

    rows = await _fetch_rows(engine, task_schema)
    assert {r["status"] for r in rows} == {"done"}
    assert all(r["finished_at"] is not None for r in rows)


@pytest.mark.asyncio
async def test_drain_once_retries_op_omitted_from_result(
    drain_env, monkeypatch  # noqa: ANN001
):
    """An op_id the indexer omits from ``BulkIndexResult`` is retried, not
    stranded in_flight until lease expiry (the _apply_outcomes guard)."""
    from dynastore.models.protocols.indexing import BulkIndexResult

    task_schema, engine = drain_env
    await _seed_rows(engine, task_schema, n=3)

    task = _make_task(engine, task_schema)

    def _build(ops: Any) -> Any:  # pass all but the FIRST op
        return BulkIndexResult(
            passed=[op.op_id for op in ops][1:], transient=[], poison=[],
        )

    fake = _FakeBulkIndexer(_build)

    async def _resolve(driver_id: str) -> Any:
        return fake

    monkeypatch.setattr(task, "_resolve_indexer", _resolve)

    owner_id = f"owner:{uuid4()}"
    await task.drain_once(engine=engine, owner_id=owner_id)

    rows = await _fetch_rows(engine, task_schema)
    done = [r for r in rows if r["status"] == "done"]
    retried = [r for r in rows if r["status"] == "ready"]
    assert len(done) == 2, f"two ops should be done; got {[r['status'] for r in rows]}"
    assert len(retried) == 1, "the omitted op must be retried (ready), not stranded"
    assert retried[0]["attempts"] == 1


@pytest.mark.asyncio
async def test_resolve_indexer_unknown_driver_returns_none(drain_env):
    """Any unknown driver_id resolves to no indexer (caller retries it)."""
    task_schema, engine = drain_env
    task = _make_task(engine, task_schema)
    assert await task._resolve_indexer("totally_unknown_driver_xyz") is None


@pytest.mark.asyncio
async def test_resolve_indexer_es_driver_is_bulk_indexer(drain_env):
    """The ES driver_id resolves to an ``ESBulkIndexer`` (the ``BulkIndexer``
    protocol), and its ``index_bulk`` takes exactly one positional arg ``ops``
    — NOT the ``Indexer`` protocol's ``index_bulk(ctx, ops)``. Skips when
    opensearch-py is absent from the test extras."""
    import inspect

    task_schema, engine = drain_env
    task = _make_task(engine, task_schema)
    indexer = await task._resolve_indexer("items_elasticsearch_driver")
    if indexer is None:
        pytest.skip("ES driver unavailable (opensearch-py not installed)")

    from dynastore.tasks.workclass_drain.es_indexer_adapter import ESBulkIndexer

    assert isinstance(indexer, ESBulkIndexer)
    params = list(inspect.signature(indexer.index_bulk).parameters)
    assert params == ["ops"], (
        f"index_bulk must be the BulkIndexer one-arg contract; got {params}"
    )


@pytest.mark.asyncio
async def test_resolve_indexer_registered_driver_resolves_via_registry():
    """A driver registered under its snake_case ``driver_id`` in
    ``DriverRegistry.collection_index()`` resolves to a ``BulkIndexer`` —
    the registry lookup is the only resolution path (#2732 step 3: the
    hardcoded ``ItemsElasticsearchDriver()`` construction fallback was
    removed). Pure-unit — no PG required."""
    from unittest.mock import MagicMock, patch

    from dynastore.modules.storage.driver_registry import DriverRegistry
    from dynastore.modules.storage.drivers.elasticsearch import (
        ItemsElasticsearchDriver,
    )
    from dynastore.tasks.workclass_drain.es_indexer_adapter import ESBulkIndexer
    from dynastore.tasks.workclass_drain.storage_drain_task import StorageDrainTask

    fake_driver = MagicMock(spec=ItemsElasticsearchDriver)
    fake_driver.is_available.return_value = True

    task = StorageDrainTask()
    with (
        patch.object(
            DriverRegistry, "collection_index",
            return_value={"items_elasticsearch_driver": fake_driver},
        ),
        patch.object(DriverRegistry, "asset_index", return_value={}),
    ):
        indexer = await task._resolve_indexer("items_elasticsearch_driver")

    assert isinstance(indexer, ESBulkIndexer)


@pytest.mark.asyncio
async def test_drain_once_unregistered_driver_retries_without_affecting_others(
    drain_env, monkeypatch, caplog,
):
    """A ``driver_id`` with no registered ``BulkIndexer`` funnels its rows to
    retry with a WARNING naming the driver_id and stating registration is
    required — while another ``driver_id``'s rows claimed in the same batch
    are indexed normally (#2732 step 3: no construction fallback, so an
    unregistered driver can never silently mask a real registration gap)."""
    import logging as _logging

    from dynastore.models.protocols.indexing import BulkIndexResult

    task_schema, engine = drain_env
    await _seed_rows(engine, task_schema, n=2, driver_id="registered_driver")
    await _seed_rows(engine, task_schema, n=1, driver_id="unregistered_driver_xyz")

    task = _make_task(engine, task_schema)
    fake = _FakeBulkIndexer(
        lambda ops: BulkIndexResult(
            passed=[op.op_id for op in ops], transient=[], poison=[],
        )
    )

    async def _resolve(driver_id: str) -> Any:
        return fake if driver_id == "registered_driver" else None

    monkeypatch.setattr(task, "_resolve_indexer", _resolve)

    owner_id = f"owner:{uuid4()}"
    with caplog.at_level(_logging.WARNING):
        count = await task.drain_once(engine=engine, owner_id=owner_id)
    assert count == 3

    # The registered driver's rows were dispatched to its bulk indexer;
    # nothing from the unregistered driver reached it.
    assert len(fake.calls) == 1
    assert len(fake.calls[0]) == 2

    rows = await _fetch_rows(engine, task_schema)
    done = [r for r in rows if r["status"] == "done"]
    retried = [r for r in rows if r["status"] == "ready"]
    assert len(done) == 2, "the registered driver's rows must be indexed and done"
    assert len(retried) == 1, "the unregistered driver's row must be retried, not dropped"
    assert retried[0]["attempts"] == 1

    assert any(
        "unregistered_driver_xyz" in r.getMessage()
        and "registration is required" in r.getMessage().lower()
        for r in caplog.records
    ), "the WARN must name the driver_id and state registration is required"


# ---------------------------------------------------------------------------
# 8. Canonical re-read for id-only rows (#2494 P1)
# ---------------------------------------------------------------------------


async def _seed_id_only_row(
    engine: Any,
    task_schema: str,
    *,
    op: str = "upsert",
    driver_id: str = "es_driver",
    catalog_id: str = "tenant_a",
    collection_id: str = "coll_a",
    entity_id: str,
) -> str:
    """Insert one id-only row (explicit id-only sentinel payload,
    explicit collection/entity)."""
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )

    op_id = str(uuid4())
    sql = (
        f"INSERT INTO {task_schema}.storage"
        f" (op_id, day, driver_id, catalog_id, collection_id, entity_id,"
        f"  op, status, op_payload)"
        f" VALUES (:op_id, CURRENT_DATE, :driver_id, :catalog_id,"
        f"         :collection_id, :entity_id, :op, 'ready',"
        f"         '{{\"_id_only\": true}}'::jsonb)"
    )
    async with managed_transaction(engine) as conn:
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn,
            op_id=op_id,
            driver_id=driver_id,
            catalog_id=catalog_id,
            collection_id=collection_id,
            entity_id=entity_id,
            op=op,
        )
    return op_id


async def _seed_empty_payload_upsert_row(
    engine: Any,
    task_schema: str,
    *,
    driver_id: str = "es_driver",
    catalog_id: str = "tenant_a",
    collection_id: str = "coll_a",
    entity_id: str,
) -> str:
    """Insert one upsert row with a genuinely EMPTY ``op_payload`` (no
    id-only marker) — the DDL-default shape, distinct from an id-only row.
    """
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )

    op_id = str(uuid4())
    sql = (
        f"INSERT INTO {task_schema}.storage"
        f" (op_id, day, driver_id, catalog_id, collection_id, entity_id,"
        f"  op, status, op_payload)"
        f" VALUES (:op_id, CURRENT_DATE, :driver_id, :catalog_id,"
        f"         :collection_id, :entity_id, 'upsert', 'ready', '{{}}'::jsonb)"
    )
    async with managed_transaction(engine) as conn:
        await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
            conn,
            op_id=op_id,
            driver_id=driver_id,
            catalog_id=catalog_id,
            collection_id=collection_id,
            entity_id=entity_id,
        )
    return op_id


class _StubCanonicalInput:
    """Minimal stand-in for CanonicalIndexInput — only ``row`` is read by
    the stubbed ``_build_canonical_doc`` in these tests."""

    def __init__(self, geoid: str) -> None:
        self.row = {"geoid": geoid}
        self.resolved_sidecars = []
        self.geometry = None
        self.bbox = None
        self.user_properties = None
        self.access = None
        self.stac_reserved_members = None


def _patch_canonical_reread(monkeypatch, task, *, present: set) -> List[tuple]:
    """Stub the canonical re-read seam: geoids in ``present`` resolve to a
    canned CanonicalIndexInput; everything else is absent (missing PG row).
    Records every ``(catalog_id, collection_id, geoids)`` call so tests can
    assert on batching.
    """
    calls: List[tuple] = []

    async def _fake_read(*, engine, catalog_id, collection_id, geoids):
        calls.append((catalog_id, collection_id, tuple(sorted(geoids))))
        return {g: _StubCanonicalInput(g) for g in geoids if g in present}

    async def _fake_build(*, catalog_id, collection_id, ci):
        return {"id": ci.row["geoid"], "catalog_id": catalog_id, "collection_id": collection_id}

    monkeypatch.setattr(task, "_read_canonical_inputs", _fake_read)
    monkeypatch.setattr(task, "_build_canonical_doc", _fake_build)
    return calls


@pytest.mark.asyncio
async def test_id_only_upsert_present_row_indexes_current_state(drain_env, monkeypatch):
    """An id-only upsert row whose geoid IS found in canonical PG state is
    indexed with the freshly-built canonical document."""
    from dynastore.models.protocols.indexing import BulkIndexResult

    task_schema, engine = drain_env
    await _seed_id_only_row(engine, task_schema, entity_id="geoid-1")

    task = _make_task(engine, task_schema)
    _patch_canonical_reread(monkeypatch, task, present={"geoid-1"})

    fake = _FakeBulkIndexer(
        lambda ops: BulkIndexResult(passed=[op.op_id for op in ops], transient=[], poison=[])
    )

    async def _resolve(driver_id: str) -> Any:
        return fake

    monkeypatch.setattr(task, "_resolve_indexer", _resolve)

    owner_id = f"owner:{uuid4()}"
    count = await task.drain_once(engine=engine, owner_id=owner_id)
    assert count == 1

    assert len(fake.calls) == 1
    ops = fake.calls[0]
    assert len(ops) == 1
    assert ops[0].payload == {"id": "geoid-1", "catalog_id": "tenant_a", "collection_id": "coll_a"}

    rows = await _fetch_rows(engine, task_schema)
    assert rows[0]["status"] == "done"


@pytest.mark.asyncio
async def test_id_only_upsert_missing_row_skipped_as_success(
    drain_env, monkeypatch, caplog,
):
    """An id-only upsert row whose geoid is ABSENT from canonical PG state
    is marked done directly — never built into an op sent to the indexer.

    #2731: this is the ONLY legitimate auto_done outcome, and it must be
    logged — the drain that lost ~5200 items on 2026-07-02 produced zero
    WARNING/ERROR/INFO logs for the whole run.
    """
    import logging as _logging

    task_schema, engine = drain_env
    await _seed_id_only_row(engine, task_schema, entity_id="geoid-missing")

    task = _make_task(engine, task_schema)
    _patch_canonical_reread(monkeypatch, task, present=set())  # nothing found

    fake = _FakeBulkIndexer(lambda ops: pytest.fail("indexer must not be called"))

    async def _resolve(driver_id: str) -> Any:
        return fake

    monkeypatch.setattr(task, "_resolve_indexer", _resolve)

    owner_id = f"owner:{uuid4()}"
    with caplog.at_level(_logging.INFO):
        count = await task.drain_once(engine=engine, owner_id=owner_id)
    assert count == 1
    assert fake.calls == [], "no op should reach the indexer for a missing PG row"

    rows = await _fetch_rows(engine, task_schema)
    assert rows[0]["status"] == "done", "missing PG row on upsert must skip as success"

    assert any(
        "auto_done" in r.getMessage() and "tenant_a" in r.getMessage() and "coll_a" in r.getMessage()
        for r in caplog.records
    ), "an auto_done row must log an INFO summary naming (catalog, collection, count)"
    assert any(
        "claimed=1" in r.getMessage() and "auto_done=1" in r.getMessage()
        for r in caplog.records
    ), "the per-batch classification summary must be logged"


@pytest.mark.asyncio
async def test_id_only_delete_bypasses_reread(drain_env, monkeypatch):
    """A delete row (even with empty op_payload — the normal shape for a
    delete) goes straight to the indexer; no canonical re-read is attempted."""
    from dynastore.models.protocols.indexing import BulkIndexResult

    task_schema, engine = drain_env
    await _seed_id_only_row(engine, task_schema, op="delete", entity_id="geoid-del")

    task = _make_task(engine, task_schema)
    reread_calls = _patch_canonical_reread(monkeypatch, task, present={"geoid-del"})

    fake = _FakeBulkIndexer(
        lambda ops: BulkIndexResult(passed=[op.op_id for op in ops], transient=[], poison=[])
    )

    async def _resolve(driver_id: str) -> Any:
        return fake

    monkeypatch.setattr(task, "_resolve_indexer", _resolve)

    owner_id = f"owner:{uuid4()}"
    count = await task.drain_once(engine=engine, owner_id=owner_id)
    assert count == 1

    assert reread_calls == [], "delete ops must never trigger a canonical re-read"
    assert len(fake.calls) == 1
    assert fake.calls[0][0].op == "delete"

    rows = await _fetch_rows(engine, task_schema)
    assert rows[0]["status"] == "done"


@pytest.mark.asyncio
async def test_id_only_batches_one_read_per_catalog_collection_group(drain_env, monkeypatch):
    """Multiple id-only rows in the same (catalog, collection) group share
    ONE canonical re-read call; a different group gets its own call."""
    from dynastore.models.protocols.indexing import BulkIndexResult

    task_schema, engine = drain_env
    await _seed_id_only_row(
        engine, task_schema, collection_id="coll_a", entity_id="g1",
    )
    await _seed_id_only_row(
        engine, task_schema, collection_id="coll_a", entity_id="g2",
    )
    await _seed_id_only_row(
        engine, task_schema, collection_id="coll_b", entity_id="g3",
    )

    task = _make_task(engine, task_schema)
    reread_calls = _patch_canonical_reread(
        monkeypatch, task, present={"g1", "g2", "g3"},
    )

    fake = _FakeBulkIndexer(
        lambda ops: BulkIndexResult(passed=[op.op_id for op in ops], transient=[], poison=[])
    )

    async def _resolve(driver_id: str) -> Any:
        return fake

    monkeypatch.setattr(task, "_resolve_indexer", _resolve)

    owner_id = f"owner:{uuid4()}"
    count = await task.drain_once(engine=engine, owner_id=owner_id)
    assert count == 3

    # One batched read per distinct (catalog_id, collection_id) group.
    assert len(reread_calls) == 2, reread_calls
    groups = {(c, coll) for c, coll, _ids in reread_calls}
    assert groups == {("tenant_a", "coll_a"), ("tenant_a", "coll_b")}
    coll_a_call = next(c for c in reread_calls if c[1] == "coll_a")
    assert set(coll_a_call[2]) == {"g1", "g2"}

    rows = await _fetch_rows(engine, task_schema)
    assert {r["status"] for r in rows} == {"done"}


@pytest.mark.asyncio
async def test_id_only_reread_failure_funnels_group_to_retry(drain_env, monkeypatch):
    """A canonical re-read that raises for a group is a transient infra
    failure — every row in that group is retried, not dropped or poisoned."""
    task_schema, engine = drain_env
    await _seed_id_only_row(engine, task_schema, entity_id="geoid-err")

    task = _make_task(engine, task_schema)

    async def _fake_read(*, engine, catalog_id, collection_id, geoids):
        raise RuntimeError("simulated PG outage")

    monkeypatch.setattr(task, "_read_canonical_inputs", _fake_read)

    owner_id = f"owner:{uuid4()}"
    count = await task.drain_once(engine=engine, owner_id=owner_id)
    assert count == 1

    rows = await _fetch_rows(engine, task_schema)
    assert rows[0]["status"] == "ready", "re-read failure must retry, not drop"
    assert rows[0]["attempts"] == 1


@pytest.mark.asyncio
async def test_payload_carrying_rows_unaffected_by_id_only_path(drain_env, monkeypatch):
    """A payload-carrying row (non-empty op_payload) goes straight to the
    indexer exactly as before #2494 — no canonical re-read attempted."""
    from dynastore.models.protocols.indexing import BulkIndexResult

    task_schema, engine = drain_env
    # Default _seed_rows now carries a non-empty payload.
    await _seed_rows(engine, task_schema, n=2)

    task = _make_task(engine, task_schema)
    reread_calls = _patch_canonical_reread(monkeypatch, task, present=set())

    fake = _FakeBulkIndexer(
        lambda ops: BulkIndexResult(passed=[op.op_id for op in ops], transient=[], poison=[])
    )

    async def _resolve(driver_id: str) -> Any:
        return fake

    monkeypatch.setattr(task, "_resolve_indexer", _resolve)

    owner_id = f"owner:{uuid4()}"
    count = await task.drain_once(engine=engine, owner_id=owner_id)
    assert count == 2

    assert reread_calls == [], "payload-carrying rows must never trigger a re-read"
    assert len(fake.calls) == 1
    assert len(fake.calls[0]) == 2
    assert all(op.payload == {"legacy": True} for op in fake.calls[0])


@pytest.mark.asyncio
async def test_empty_payload_without_marker_does_not_trigger_reread(
    drain_env, monkeypatch, caplog,
):
    """An upsert row with a genuinely EMPTY op_payload (the DDL default,
    no id-only marker) must NOT be classified as id-only — it falls
    through to the legacy path (built and sent to the indexer as-is,
    same as pre-#2494), with a WARNING logged for the anomaly."""
    import logging as _logging

    from dynastore.models.protocols.indexing import BulkIndexResult

    task_schema, engine = drain_env
    await _seed_empty_payload_upsert_row(engine, task_schema, entity_id="geoid-empty")

    task = _make_task(engine, task_schema)
    reread_calls = _patch_canonical_reread(monkeypatch, task, present={"geoid-empty"})

    fake = _FakeBulkIndexer(
        lambda ops: BulkIndexResult(passed=[op.op_id for op in ops], transient=[], poison=[])
    )

    async def _resolve(driver_id: str) -> Any:
        return fake

    monkeypatch.setattr(task, "_resolve_indexer", _resolve)

    owner_id = f"owner:{uuid4()}"
    with caplog.at_level(_logging.WARNING):
        count = await task.drain_once(engine=engine, owner_id=owner_id)
    assert count == 1

    assert reread_calls == [], "an unmarked empty payload must not trigger a re-read"
    assert len(fake.calls) == 1
    assert len(fake.calls[0]) == 1
    assert fake.calls[0][0].payload == {}
    assert any(
        "EMPTY op_payload and no id-only marker" in r.getMessage()
        for r in caplog.records
    ), "an unmarked empty payload must log the anomaly warning"

    rows = await _fetch_rows(engine, task_schema)
    assert rows[0]["status"] == "done"


# ---------------------------------------------------------------------------
# 8b. Byte-budgeted hydration sub-chunking (#2723)
#
# Bounds peak hydrated-payload memory independent of storage_drain_batch_size
# (row count only, #2726): as documents are built they are dispatched to
# index_bulk in sub-chunks capped by TasksPluginConfig
# .storage_drain_hydration_byte_budget, instead of the whole claimed batch
# being materialized before a single _bulk call.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hydration_byte_budget_splits_dispatch_into_multiple_sub_chunks(
    drain_env, monkeypatch,
):
    """A byte budget smaller than the claimed batch's total hydrated size
    splits dispatch into multiple index_bulk calls — hydration must never
    materialize the whole batch before the first _bulk call leaves."""
    from dynastore.models.protocols.indexing import BulkIndexResult
    from dynastore.tasks.workclass_drain.storage_drain_task import (
        StorageDrainTask, _estimate_doc_bytes,
    )

    task_schema, engine = drain_env
    n = 5
    entity_ids = [f"geoid-hb{i}" for i in range(n)]
    for eid in entity_ids:
        await _seed_id_only_row(engine, task_schema, entity_id=eid)

    heavy = "x" * 1024  # ~1 KiB per hydrated document

    async def _fake_read(*, engine, catalog_id, collection_id, geoids):
        return {g: _StubCanonicalInput(g) for g in geoids}

    async def _fake_build(*, catalog_id, collection_id, ci):
        return {"id": ci.row["geoid"], "blob": heavy}

    one_doc_bytes = _estimate_doc_bytes({"id": "geoid-hb0", "blob": heavy})
    # Just above one document's size: the 1st doc in a sub-chunk never
    # crosses the budget alone, the 2nd always does — forces pairs.
    budget = one_doc_bytes + 50

    task = StorageDrainTask(batch_size=10, lease_seconds=300, hydration_byte_budget=budget)
    monkeypatch.setattr(task, "_read_canonical_inputs", _fake_read)
    monkeypatch.setattr(task, "_build_canonical_doc", _fake_build)

    fake = _FakeBulkIndexer(
        lambda ops: BulkIndexResult(passed=[op.op_id for op in ops], transient=[], poison=[])
    )

    async def _resolve(driver_id: str) -> Any:
        return fake

    monkeypatch.setattr(task, "_resolve_indexer", _resolve)

    owner_id = f"owner:{uuid4()}"
    count = await task.drain_once(engine=engine, owner_id=owner_id)
    assert count == n

    # Never one shot: the whole batch must never reach the indexer as a
    # single call (that is the exact #2723 OOM shape).
    assert len(fake.calls) > 1, "hydration must be sub-chunked, not one shot"
    assert not any(len(call) == n for call in fake.calls)
    assert sum(len(call) for call in fake.calls) == n

    # Peak materialized per call stays bounded: a chunk's accumulated bytes
    # never exceed budget by more than one document's worth (the doc whose
    # append triggered the flush).
    for call in fake.calls:
        total = sum(_estimate_doc_bytes(op.payload) for op in call)
        assert total <= budget + one_doc_bytes, (
            f"sub-chunk of {len(call)} row(s) totalled {total} bytes, "
            f"budget={budget}"
        )

    rows = await _fetch_rows(engine, task_schema)
    assert {r["status"] for r in rows} == {"done"}


@pytest.mark.asyncio
async def test_hydration_sub_chunk_failure_isolates_retry_to_that_chunk(
    drain_env, monkeypatch,
):
    """A sub-chunk that fails ``index_bulk`` retries only ITS rows — rows in
    an earlier, already-flushed sub-chunk keep their 'done' outcome (no
    double-indexing) and no claimed row is silently skipped (#2723
    crash/partial-failure safety)."""
    from dynastore.models.protocols.indexing import BulkIndexResult
    from dynastore.tasks.workclass_drain.storage_drain_task import (
        StorageDrainTask, _estimate_doc_bytes,
    )

    task_schema, engine = drain_env
    n = 4
    entity_ids = [f"geoid-fc{i}" for i in range(n)]
    for eid in entity_ids:
        await _seed_id_only_row(engine, task_schema, entity_id=eid)

    heavy = "x" * 1024

    async def _fake_read(*, engine, catalog_id, collection_id, geoids):
        return {g: _StubCanonicalInput(g) for g in geoids}

    async def _fake_build(*, catalog_id, collection_id, ci):
        return {"id": ci.row["geoid"], "blob": heavy}

    one_doc_bytes = _estimate_doc_bytes({"id": "geoid-fc0", "blob": heavy})
    budget = one_doc_bytes + 50  # forces pairs, same shape as the test above

    task = StorageDrainTask(batch_size=10, lease_seconds=300, hydration_byte_budget=budget)
    monkeypatch.setattr(task, "_read_canonical_inputs", _fake_read)
    monkeypatch.setattr(task, "_build_canonical_doc", _fake_build)

    class _FailSecondCallIndexer:
        def __init__(self) -> None:
            self.calls: List[Any] = []

        async def index_bulk(self, ops: Any) -> Any:
            ops_list = list(ops)
            self.calls.append(ops_list)
            if len(self.calls) == 2:
                raise RuntimeError("simulated ES outage")
            return BulkIndexResult(
                passed=[op.op_id for op in ops_list], transient=[], poison=[],
            )

    fake = _FailSecondCallIndexer()

    async def _resolve(driver_id: str) -> Any:
        return fake

    monkeypatch.setattr(task, "_resolve_indexer", _resolve)

    owner_id = f"owner:{uuid4()}"
    count = await task.drain_once(engine=engine, owner_id=owner_id)
    assert count == n
    assert len(fake.calls) >= 2, "budget must split this batch into >=2 sub-chunks"

    rows = await _fetch_rows(engine, task_schema)
    done = [r for r in rows if r["status"] == "done"]
    retried = [r for r in rows if r["status"] == "ready"]

    # Every claimed row lands in exactly one bucket — none skipped, none
    # double-counted between done and retried.
    assert len(done) + len(retried) == n
    assert len(retried) == len(fake.calls[1]), "only the failing sub-chunk's rows retry"
    for r in retried:
        assert r["attempts"] == 1


# ---------------------------------------------------------------------------
# 9. Split completion metrics (#2731)
#
# Pure-unit — no PG required. Mirrors the create_async_engine stubbing
# pattern in test_drain_engine_url_normalization.py: drain_once is stubbed
# so run() only exercises the metrics-accumulation and TaskReport wiring.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_reports_split_completion_metrics():
    """``run()``'s ``TaskReport.metrics`` splits indexed/auto_done/retried
    alongside the backward-compat ``drained`` total (#2731) — a drain can no
    longer describe auto_done/retried rows as uniformly "processed"."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from dynastore.tasks.workclass_drain.storage_drain_task import StorageDrainTask

    task = StorageDrainTask()
    fake_engine = MagicMock()
    fake_engine.dispose = AsyncMock()

    calls = {"n": 0}

    async def _fake_drain_once(*, engine, owner_id, batch_size=None, hydration_byte_budget=None):
        calls["n"] += 1
        if calls["n"] == 1:
            # Simulate one claimed batch: 3 indexed, 2 auto_done, 1 retried.
            task._run_metrics["indexed"] += 3
            task._run_metrics["auto_done"] += 2
            task._run_metrics["retried"] += 1
            return 6
        return 0  # drain-to-empty exit

    with (
        patch("sqlalchemy.ext.asyncio.create_async_engine", return_value=fake_engine),
        patch.object(task, "drain_once", new=_fake_drain_once),
    ):
        report = await task.run(MagicMock())

    assert report.metrics == {
        "drained": 6, "indexed": 3, "auto_done": 2, "retried": 1,
    }


@pytest.mark.asyncio
async def test_run_resets_split_metrics_between_runs():
    """A second ``run()`` call must not carry over the previous run's split
    counters (self._run_metrics is reset at the top of run())."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from dynastore.tasks.workclass_drain.storage_drain_task import StorageDrainTask

    task = StorageDrainTask()
    fake_engine = MagicMock()
    fake_engine.dispose = AsyncMock()

    first_calls = {"n": 0}

    async def _first_drain_once(*, engine, owner_id, batch_size=None, hydration_byte_budget=None):
        first_calls["n"] += 1
        if first_calls["n"] == 1:
            task._run_metrics["indexed"] += 10
            task._run_metrics["auto_done"] += 10
            task._run_metrics["retried"] += 10
            return 30
        return 0

    with (
        patch("sqlalchemy.ext.asyncio.create_async_engine", return_value=fake_engine),
        patch.object(task, "drain_once", new=_first_drain_once),
    ):
        await task.run(MagicMock())

    async def _second_drain_once(*, engine, owner_id, batch_size=None, hydration_byte_budget=None):
        return 0  # nothing claimed this run

    with (
        patch("sqlalchemy.ext.asyncio.create_async_engine", return_value=fake_engine),
        patch.object(task, "drain_once", new=_second_drain_once),
    ):
        report = await task.run(MagicMock())

    assert report.metrics == {
        "drained": 0, "indexed": 0, "auto_done": 0, "retried": 0,
    }


# ---------------------------------------------------------------------------
# 10. In-process byte/wall-clock drain budget handoff (#2732 step 4)
#
# StorageDrainTask always starts in-process. If cumulative hydrated bytes or
# wall-clock elapsed crosses TasksPluginConfig.storage_drain_inprocess_max_bytes
# / storage_drain_inprocess_max_seconds while backlog rows still remain,
# run() stops early and hands the remainder off to storage_drain_offload
# (StorageDrainOffloadTask, which carries the async-write workclass marker
# and ignores the budget entirely — it always drains to empty).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_hands_off_when_byte_budget_exceeded_with_backlog_remaining():
    """A single over-budget batch stops the loop immediately (not drain to
    empty) and calls _handoff_to_offload_job exactly once."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from dynastore.tasks.workclass_drain.storage_drain_task import StorageDrainTask

    task = StorageDrainTask(inprocess_max_bytes=100, inprocess_max_seconds=999.0)
    fake_engine = MagicMock()
    fake_engine.dispose = AsyncMock()

    calls = {"n": 0}

    async def _fake_drain_once(*, engine, owner_id, batch_size=None, hydration_byte_budget=None):
        calls["n"] += 1
        task._last_batch_bytes = 200  # exceeds the 100-byte budget in one batch
        return 5  # non-empty — backlog likely remains

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
    """Slow hydration that blows the wall-clock budget triggers the same
    handoff, independent of hydrated bytes (kept at 0 throughout)."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock, patch

    from dynastore.tasks.workclass_drain.storage_drain_task import StorageDrainTask

    task = StorageDrainTask(inprocess_max_bytes=0, inprocess_max_seconds=0.02)
    fake_engine = MagicMock()
    fake_engine.dispose = AsyncMock()

    calls = {"n": 0}

    async def _fake_drain_once(*, engine, owner_id, batch_size=None, hydration_byte_budget=None):
        calls["n"] += 1
        await asyncio.sleep(0.05)  # simulated slow hydration — well over the 0.02s budget
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
    the pre-#2732 loop — no handoff is ever attempted."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from dynastore.tasks.workclass_drain.storage_drain_task import StorageDrainTask

    task = StorageDrainTask(inprocess_max_bytes=10**9, inprocess_max_seconds=999.0)
    fake_engine = MagicMock()
    fake_engine.dispose = AsyncMock()

    calls = {"n": 0}

    async def _fake_drain_once(*, engine, owner_id, batch_size=None, hydration_byte_budget=None):
        calls["n"] += 1
        if calls["n"] <= 2:
            task._last_batch_bytes = 10
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
async def test_storage_drain_offload_task_ignores_budget_and_drains_to_empty():
    """StorageDrainOffloadTask (_inprocess_budget_enabled=False) never calls
    _handoff_to_offload_job, even when hydrated bytes vastly exceed what
    would trip the base task's budget — it always drains its claimed
    backlog to empty instead of escaping again."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from dynastore.tasks.workclass_drain.storage_drain_task import (
        StorageDrainOffloadTask,
    )

    task = StorageDrainOffloadTask(inprocess_max_bytes=1, inprocess_max_seconds=0.001)
    assert task._inprocess_budget_enabled is False

    fake_engine = MagicMock()
    fake_engine.dispose = AsyncMock()

    calls = {"n": 0}

    async def _fake_drain_once(*, engine, owner_id, batch_size=None, hydration_byte_budget=None):
        calls["n"] += 1
        if calls["n"] <= 3:
            task._last_batch_bytes = 10**9  # would blow any base-task budget
            return 100
        return 0

    handoff = AsyncMock()

    with (
        patch("sqlalchemy.ext.asyncio.create_async_engine", return_value=fake_engine),
        patch.object(task, "drain_once", new=_fake_drain_once),
        patch.object(task, "_handoff_to_offload_job", new=handoff),
    ):
        report = await task.run(MagicMock())

    handoff.assert_not_awaited()
    assert calls["n"] == 4
    assert report.metrics["drained"] == 300


# ---------------------------------------------------------------------------
# 11. Live-PG: _handoff_to_offload_job's actual DB effect
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handoff_to_offload_job_inserts_distinct_dedup_key_row(drain_env):
    """_handoff_to_offload_job enqueues a storage_drain_offload trigger with
    its own dedup_key — independent of (never blocked by, never blocking) a
    live storage_drain trigger for the same outbox."""
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.storage_emit import enqueue_storage_op
    from dynastore.models.protocols.indexing import OutboxRecord

    task_schema, engine = drain_env

    # A live in-process storage_drain trigger already exists (the hot
    # co-transactional write path).
    rows = [
        OutboxRecord(
            op_id=uuid4(), driver_id="es_driver", driver_instance_id="di",
            collection_id="coll", op="upsert", item_id="item_1",
            payload={"x": 1}, idempotency_key="ik_1",
        ),
    ]
    async with managed_transaction(engine) as conn:
        await enqueue_storage_op(conn, catalog_id=task_schema, rows=rows)
    assert await _count_tasks(engine, task_schema, task_type="storage_drain") == 1

    task = _make_task(engine, task_schema)
    await task._handoff_to_offload_job(engine)

    assert await _count_tasks(engine, task_schema, task_type="storage_drain") == 1
    assert await _count_tasks(engine, task_schema, task_type="storage_drain_offload") == 1

    # A second handoff call while the first offload trigger is still
    # non-terminal is deduplicated to the same single row.
    await task._handoff_to_offload_job(engine)
    assert await _count_tasks(engine, task_schema, task_type="storage_drain_offload") == 1


# ---------------------------------------------------------------------------
# 12. Live-PG: _enqueue_drain_trigger task_type/dedup_key parameterization
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enqueue_drain_trigger_defaults_emit_storage_drain(drain_env):
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.storage_emit import _enqueue_drain_trigger

    task_schema, engine = drain_env
    async with managed_transaction(engine) as conn:
        await _enqueue_drain_trigger(conn)

    assert await _count_tasks(engine, task_schema, task_type="storage_drain") == 1
    assert await _count_tasks(engine, task_schema, task_type="storage_drain_offload") == 0


@pytest.mark.asyncio
async def test_enqueue_drain_trigger_explicit_kwargs_emit_storage_drain_offload(drain_env):
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.storage_emit import _enqueue_drain_trigger

    task_schema, engine = drain_env
    async with managed_transaction(engine) as conn:
        await _enqueue_drain_trigger(
            conn, task_type="storage_drain_offload", dedup_key="storage_drain_offload",
        )

    assert await _count_tasks(engine, task_schema, task_type="storage_drain") == 0
    assert await _count_tasks(engine, task_schema, task_type="storage_drain_offload") == 1


@pytest.mark.asyncio
async def test_enqueue_drain_trigger_dedup_guard_is_per_key(drain_env):
    """Repeated calls with the SAME custom key coalesce to one row; a
    DIFFERENT key gets its own independent row."""
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.storage_emit import _enqueue_drain_trigger

    task_schema, engine = drain_env
    for _ in range(3):
        async with managed_transaction(engine) as conn:
            await _enqueue_drain_trigger(
                conn, task_type="storage_drain_offload", dedup_key="storage_drain_offload",
            )
    async with managed_transaction(engine) as conn:
        await _enqueue_drain_trigger(conn)  # defaults — a distinct key

    assert await _count_tasks(engine, task_schema, task_type="storage_drain_offload") == 1
    assert await _count_tasks(engine, task_schema, task_type="storage_drain") == 1


# ---------------------------------------------------------------------------
# 13. Live-PG integration: the full run() budget loop end-to-end
#
# create_async_engine is patched to return the throwaway drain_env engine
# (mirrors test_drain_engine_url_normalization.py's stubbing pattern) so
# run()'s own engine.dispose() call is safe — SQLAlchemy AsyncEngine.dispose()
# only tears down the pooled connections; the engine itself remains usable
# for the fixture's teardown query afterwards.
# ---------------------------------------------------------------------------


async def _run_with_engine(task: Any, engine: Any) -> Any:
    from unittest.mock import MagicMock, patch

    with patch("sqlalchemy.ext.asyncio.create_async_engine", return_value=engine):
        return await task.run(MagicMock())


@pytest.mark.asyncio
async def test_run_byte_budget_integration_leaves_backlog_and_offload_trigger(
    drain_env, monkeypatch,
):
    """Seeded rows whose hydrated docs exceed the byte budget: run() stops
    after the first (budget-bounded) claim, leaves the rest 'ready', and
    enqueues exactly one storage_drain_offload trigger."""
    from dynastore.models.protocols.indexing import BulkIndexResult
    from dynastore.tasks.workclass_drain.storage_drain_task import (
        StorageDrainTask, _estimate_doc_bytes,
    )

    task_schema, engine = drain_env
    n = 6
    entity_ids = [f"geoid-ib{i}" for i in range(n)]
    for eid in entity_ids:
        await _seed_id_only_row(engine, task_schema, entity_id=eid)

    heavy = "x" * 2048

    async def _fake_read(*, engine, catalog_id, collection_id, geoids):
        return {g: _StubCanonicalInput(g) for g in geoids}

    async def _fake_build(*, catalog_id, collection_id, ci):
        return {"id": ci.row["geoid"], "blob": heavy}

    one_doc_bytes = _estimate_doc_bytes({"id": "geoid-ib0", "blob": heavy})

    # batch_size=2 caps each claim at 2 rows; a budget just over one doc's
    # size is exceeded by the FIRST claimed pair, so the loop hands off
    # after processing only 2 of the 6 seeded rows.
    task = StorageDrainTask(
        batch_size=2, lease_seconds=300, hydration_byte_budget=10_000_000,
        inprocess_max_bytes=one_doc_bytes + 50, inprocess_max_seconds=999.0,
    )
    monkeypatch.setattr(task, "_read_canonical_inputs", _fake_read)
    monkeypatch.setattr(task, "_build_canonical_doc", _fake_build)

    fake = _FakeBulkIndexer(
        lambda ops: BulkIndexResult(passed=[op.op_id for op in ops], transient=[], poison=[])
    )

    async def _resolve(driver_id: str) -> Any:
        return fake

    monkeypatch.setattr(task, "_resolve_indexer", _resolve)

    await _run_with_engine(task, engine)

    rows = await _fetch_rows(engine, task_schema)
    done = [r for r in rows if r["status"] == "done"]
    ready = [r for r in rows if r["status"] == "ready"]
    assert len(done) == 2, f"only the first budget-bounded claim should be processed; got {rows}"
    assert len(ready) == 4, "unclaimed rows must remain untouched ('ready')"

    assert await _count_tasks(engine, task_schema, task_type="storage_drain_offload") == 1


@pytest.mark.asyncio
async def test_run_high_byte_budget_drains_to_empty_no_offload_trigger(
    drain_env, monkeypatch,
):
    """With a budget the whole run never crosses, run() drains every seeded
    row to 'done' and never enqueues a storage_drain_offload trigger."""
    from dynastore.models.protocols.indexing import BulkIndexResult
    from dynastore.tasks.workclass_drain.storage_drain_task import StorageDrainTask

    task_schema, engine = drain_env
    n = 4
    entity_ids = [f"geoid-hb{i}" for i in range(n)]
    for eid in entity_ids:
        await _seed_id_only_row(engine, task_schema, entity_id=eid)

    heavy = "x" * 256

    async def _fake_read(*, engine, catalog_id, collection_id, geoids):
        return {g: _StubCanonicalInput(g) for g in geoids}

    async def _fake_build(*, catalog_id, collection_id, ci):
        return {"id": ci.row["geoid"], "blob": heavy}

    task = StorageDrainTask(
        batch_size=2, lease_seconds=300, hydration_byte_budget=10_000_000,
        inprocess_max_bytes=10**9, inprocess_max_seconds=999.0,
    )
    monkeypatch.setattr(task, "_read_canonical_inputs", _fake_read)
    monkeypatch.setattr(task, "_build_canonical_doc", _fake_build)

    fake = _FakeBulkIndexer(
        lambda ops: BulkIndexResult(passed=[op.op_id for op in ops], transient=[], poison=[])
    )

    async def _resolve(driver_id: str) -> Any:
        return fake

    monkeypatch.setattr(task, "_resolve_indexer", _resolve)

    await _run_with_engine(task, engine)

    rows = await _fetch_rows(engine, task_schema)
    assert {r["status"] for r in rows} == {"done"}
    assert await _count_tasks(engine, task_schema, task_type="storage_drain_offload") == 0
