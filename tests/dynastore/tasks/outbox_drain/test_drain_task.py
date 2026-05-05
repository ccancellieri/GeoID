"""OutboxDrainTask tests — claim, dispatch to BulkIndexer.index_bulk,
apply BulkIndexResult per-row outcomes, record failures.

Uses an inline ``_StubFailureLog`` Protocol-conformant fake instead of a
PG-backed ``IndexFailureLog``: the PG implementation is out of scope for
this consumer and ships separately.
"""
from __future__ import annotations

from typing import Any, List, Literal, Optional, Sequence
from uuid import UUID, uuid4

import pytest

from dynastore.models.protocols.indexing import (
    BulkIndexResult,
    IndexFailureRecord,
    IndexableOp,
    OutboxRecord,
)
from dynastore.modules.storage.outbox_ddl import ensure_storage_outbox_asyncpg
from dynastore.modules.storage.pg_outbox import PgOutboxStore
from dynastore.tasks.outbox_drain.drain_task import OutboxDrainTask


class _StubFailureLog:
    """Protocol-conformant stand-in for ``IndexFailureLog``.

    Records every ``record(...)`` call into ``self.records`` so tests can
    assert against the calls without spinning up the PG-backed
    implementation.
    """

    def __init__(self) -> None:
        self.records: List[dict] = []

    async def record(
        self,
        conn: Any,
        *,
        catalog_id: str,
        collection_id: str,
        driver_instance_id: str,
        driver_id: str,
        op_id: UUID,
        item_id: Optional[str],
        op: str,
        attempts: int,
        error_class: str,
        error_message: str,
        status: Literal["retrying", "failed"],
        correlation_id: Optional[str] = None,
    ) -> None:
        self.records.append(
            dict(
                op_id=op_id,
                status=status,
                attempts=attempts,
                error_class=error_class,
                error_message=error_message,
            )
        )

    async def list_failures(
        self, **kwargs: Any,
    ) -> Sequence[IndexFailureRecord]:
        return []


@pytest.mark.asyncio
async def test_drain_processes_ready_rows(async_conn, async_schema):
    """Happy path: 5 ready rows → BulkIndexer reports all passed →
    drain_once returns 5 and rows transition to ``done``."""
    await ensure_storage_outbox_asyncpg(async_conn, async_schema)

    store = PgOutboxStore(single_conn=async_conn)
    failure_log = _StubFailureLog()
    await store.enqueue_bulk(
        async_conn,
        catalog_id=async_schema,
        rows=[
            OutboxRecord(
                op_id=uuid4(),
                driver_id="d",
                driver_instance_id="di",
                collection_id="cc",
                op="upsert",
                item_id=str(i),
                payload={"i": i},
                idempotency_key=str(i),
            )
            for i in range(5)
        ],
    )

    received: List[IndexableOp] = []

    class _Indexer:
        indexer_id = "di"
        preferred_chunk_size = 1000

        async def index_bulk(
            self, ops: Sequence[IndexableOp],
        ) -> BulkIndexResult:
            received.extend(ops)
            return BulkIndexResult(
                passed=[o.op_id for o in ops],
                transient=[],
                poison=[],
            )

    task = OutboxDrainTask(
        driver_id="d",
        indexer=_Indexer(),
        store=store,
        failure_log=failure_log,
        catalog_id=async_schema,
        batch_size=10,
        worker_id="w",
    )
    n = await task.drain_once()
    assert n == 5
    assert len(received) == 5

    rows = await async_conn.fetch("SELECT status FROM storage_outbox")  # type: ignore[attr-defined]
    assert all(r["status"] == "done" for r in rows)
    # No failures should have been recorded for an all-passed batch.
    assert failure_log.records == []


@pytest.mark.asyncio
async def test_drain_records_failure_when_threshold_reached(
    async_conn, async_schema,
):
    """Transient outcome with ``retry_visible_threshold=1`` — the very
    first drain bumps next-attempts to 1, hits the threshold, writes a
    ``retrying`` failure-log entry, and re-schedules the row."""
    await ensure_storage_outbox_asyncpg(async_conn, async_schema)

    store = PgOutboxStore(single_conn=async_conn)
    failure_log = _StubFailureLog()
    op_id = uuid4()
    await store.enqueue_bulk(
        async_conn,
        catalog_id=async_schema,
        rows=[
            OutboxRecord(
                op_id=op_id,
                driver_id="d",
                driver_instance_id="di",
                collection_id="cc",
                op="upsert",
                item_id="i1",
                payload={"i": 1},
                idempotency_key="i1",
            ),
        ],
    )

    class _Flaky:
        indexer_id = "di"
        preferred_chunk_size = 1000

        async def index_bulk(
            self, ops: Sequence[IndexableOp],
        ) -> BulkIndexResult:
            return BulkIndexResult(
                passed=[],
                transient=[(o.op_id, "blip") for o in ops],
                poison=[],
            )

    task = OutboxDrainTask(
        driver_id="d",
        indexer=_Flaky(),
        store=store,
        failure_log=failure_log,
        catalog_id=async_schema,
        batch_size=10,
        worker_id="w",
        retry_visible_threshold=1,
    )
    # First drain: row's next-attempts becomes 1 → meets threshold →
    # ``_StubFailureLog.records`` should grow with a ``retrying`` entry.
    await task.drain_once()
    assert any(
        r["op_id"] == op_id
        and r["status"] == "retrying"
        and r["attempts"] >= 1
        for r in failure_log.records
    )

    # Push ready_at back so the row is claimable again, then drain a
    # second time to confirm classifier still records on subsequent
    # transient outcomes.
    await async_conn.execute(  # type: ignore[attr-defined]
        "UPDATE storage_outbox SET ready_at = now() WHERE op_id = $1", op_id,
    )
    await task.drain_once()
    retrying = [
        r for r in failure_log.records
        if r["op_id"] == op_id and r["status"] == "retrying"
    ]
    assert len(retrying) >= 2


@pytest.mark.asyncio
async def test_drain_marks_poison_and_records_failed(
    async_conn, async_schema,
):
    """Poison outcome → row goes to terminal ``failed`` and the failure
    log gets a ``failed`` entry regardless of the retry threshold."""
    await ensure_storage_outbox_asyncpg(async_conn, async_schema)

    store = PgOutboxStore(single_conn=async_conn)
    failure_log = _StubFailureLog()
    op_id = uuid4()
    await store.enqueue_bulk(
        async_conn,
        catalog_id=async_schema,
        rows=[
            OutboxRecord(
                op_id=op_id,
                driver_id="d",
                driver_instance_id="di",
                collection_id="cc",
                op="upsert",
                item_id="i1",
                payload={},
                idempotency_key="i1",
            ),
        ],
    )

    class _Poison:
        indexer_id = "di"
        preferred_chunk_size = 1000

        async def index_bulk(
            self, ops: Sequence[IndexableOp],
        ) -> BulkIndexResult:
            return BulkIndexResult(
                passed=[],
                transient=[],
                poison=[(o.op_id, "malformed") for o in ops],
            )

    task = OutboxDrainTask(
        driver_id="d",
        indexer=_Poison(),
        store=store,
        failure_log=failure_log,
        catalog_id=async_schema,
        batch_size=10,
        worker_id="w",
    )
    await task.drain_once()

    assert any(
        r["op_id"] == op_id and r["status"] == "failed"
        for r in failure_log.records
    )
    statuses = await async_conn.fetch(  # type: ignore[attr-defined]
        "SELECT status FROM storage_outbox WHERE op_id = $1", op_id,
    )
    assert statuses[0]["status"] == "failed"


# ---------------------------------------------------------------------------
# Framework-instantiation regression: the dynastore tasks framework loads
# every entry-point at startup with zero-arg or app_state-only construction.
# OutboxDrainTask must accept that path without raising — production wires
# concrete collaborators per-tenant before drain_once() is called.
# ---------------------------------------------------------------------------


def test_outbox_drain_task_zero_arg_construction():
    """Framework registers the task with no kwargs; should succeed."""
    task = OutboxDrainTask()
    assert task.driver_id is None
    assert task.indexer is None


def test_outbox_drain_task_app_state_only_construction():
    """Framework also tries factory(app_state=...). Should succeed."""
    task = OutboxDrainTask(app_state=object())
    assert task.driver_id is None


@pytest.mark.asyncio
async def test_outbox_drain_unconfigured_drain_raises():
    """A framework-instantiated placeholder must refuse to drain
    until configuration completes — the error message names the gap."""
    task = OutboxDrainTask()
    with pytest.raises(RuntimeError, match="not configured: missing"):
        await task.drain_once()
