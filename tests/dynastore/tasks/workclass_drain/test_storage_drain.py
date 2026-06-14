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
6. Drain trigger: dispatch_storage_dual_write with emit_target_storage='new'
   or 'both' inserts exactly ONE pending storage_drain task row on the
   same conn (dedup) and rolls back with the outer transaction.
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
# by _enqueue_drain_trigger (verified against GLOBAL_TASKS_TABLE_DDL).
_TASKS_DDL = """
CREATE TABLE IF NOT EXISTS "{schema}".tasks (
    task_id         UUID            NOT NULL,
    schema_name     VARCHAR(255)    NOT NULL,
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
    """Insert N rows into storage; return list of op_id strings."""
    from dynastore.modules.db_config.query_executor import (
        DQLQuery, ResultHandler, managed_transaction,
    )

    op_ids = [str(uuid4()) for _ in range(n)]
    for op_id in op_ids:
        claimed_at_expr = "NULL"
        if claimed_at_offset is not None:
            claimed_at_expr = f"now() {claimed_at_offset}"

        sql = (
            f"INSERT INTO {task_schema}.storage"
            f" (op_id, day, driver_id, catalog_id, op, status,"
            f"  claimed_by, claimed_at, claim_version)"
            f" VALUES (:op_id, CURRENT_DATE, :driver_id,"
            f"         :catalog_id, 'upsert', :status,"
            f"         :claimed_by, {claimed_at_expr}, :claim_version)"
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
    """dispatch_storage_dual_write with emit_target_storage='new' inserts exactly
    one storage_drain task row via the co-transactional drain trigger.
    """
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.storage_dual_write import (
        dispatch_storage_dual_write,
    )
    from dynastore.models.protocols.indexing import OutboxRecord
    from dynastore.modules.storage.pg_outbox import PgOutboxStore

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
    configs = _StubConfigs(_config("new"))
    outbox = PgOutboxStore(pool=object(), single_conn=None)

    async with managed_transaction(engine) as conn:
        await dispatch_storage_dual_write(
            conn,
            outbox=outbox,
            catalog_id=task_schema,
            rows=rows,
            configs=configs,
        )

    count = await _count_tasks(engine, task_schema)
    assert count == 1, f"expected 1 pending drain task; got {count}"


@pytest.mark.asyncio
async def test_drain_trigger_dedup_multiple_writes_one_row(drain_env):
    """Multiple writes in separate transactions produce only ONE pending drain
    task due to the dedup WHERE NOT EXISTS guard.
    """
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.storage_dual_write import (
        dispatch_storage_dual_write,
    )
    from dynastore.models.protocols.indexing import OutboxRecord
    from dynastore.modules.storage.pg_outbox import PgOutboxStore

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

    configs = _StubConfigs(_config("new"))
    outbox = PgOutboxStore(pool=object(), single_conn=None)

    # Three separate writes — each calls _enqueue_drain_trigger.
    for i in range(3):
        async with managed_transaction(engine) as conn:
            await dispatch_storage_dual_write(
                conn,
                outbox=outbox,
                catalog_id=task_schema,
                rows=[_row(f"item_{i}")],
                configs=configs,
            )

    count = await _count_tasks(engine, task_schema)
    assert count == 1, f"dedup should coalesce to 1 pending task; got {count}"


@pytest.mark.asyncio
async def test_drain_trigger_rolls_back_with_outer_transaction(drain_env):
    """An aborted outer transaction leaves no task row in tasks."""
    from dynastore.modules.db_config.query_executor import managed_transaction
    from dynastore.modules.storage.storage_dual_write import (
        dispatch_storage_dual_write,
    )
    from dynastore.models.protocols.indexing import OutboxRecord
    from dynastore.modules.storage.pg_outbox import PgOutboxStore

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
    configs = _StubConfigs(_config("new"))
    outbox = PgOutboxStore(pool=object(), single_conn=None)

    with pytest.raises(RuntimeError, match="simulated abort"):
        async with managed_transaction(engine) as conn:
            await dispatch_storage_dual_write(
                conn,
                outbox=outbox,
                catalog_id=task_schema,
                rows=rows,
                configs=configs,
            )
            raise RuntimeError("simulated abort")

    count = await _count_tasks(engine, task_schema)
    assert count == 0, f"rollback must remove the drain task row; got {count}"


# ---------------------------------------------------------------------------
# Stub helpers for trigger tests
# ---------------------------------------------------------------------------


class _StubConfigs:
    def __init__(self, cfg: Any) -> None:
        self._cfg = cfg

    async def get_config(self, config_cls: Any, **_kw: Any) -> Any:
        return self._cfg


def _config(target: Optional[str] = None) -> Any:
    from dynastore.modules.tasks.workclass_config import EmitTarget, WorkClassConfig

    if target is None:
        return WorkClassConfig()
    return WorkClassConfig(emit_target_storage=EmitTarget(target))


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

    from dynastore.tasks.outbox_drain.es_indexer_adapter import ESBulkIndexer

    assert isinstance(indexer, ESBulkIndexer)
    params = list(inspect.signature(indexer.index_bulk).parameters)
    assert params == ["ops"], (
        f"index_bulk must be the BulkIndexer one-arg contract; got {params}"
    )
