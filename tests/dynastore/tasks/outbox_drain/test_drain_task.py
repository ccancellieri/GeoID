"""OutboxDrainTask tests — claim, dispatch to BulkIndexer.index_bulk,
apply BulkIndexResult per-row outcomes, and emit failure log events
through the canonical ``log_event`` channel.

Failure observability goes through ``log_event`` (the
``/logs/catalogs/{cat}`` surface), not a dedicated index_failure_log
table. Tests monkeypatch ``log_event`` in the drain_task module so they
can assert against captured calls without booting LogService.
"""
from __future__ import annotations

from typing import Any, List, Sequence
from uuid import uuid4

import pytest

from dynastore.models.protocols.indexing import (
    BulkIndexResult,
    IndexableOp,
    OutboxRecord,
)
from dynastore.modules.storage.outbox_ddl import ensure_storage_outbox_asyncpg
from dynastore.modules.storage.pg_outbox import PgOutboxStore
from dynastore.tasks.outbox_drain import drain_task as drain_task_mod
from dynastore.tasks.outbox_drain.drain_task import OutboxDrainTask


def _capture_log_event(monkeypatch: pytest.MonkeyPatch) -> List[dict]:
    """Replace ``log_event`` in drain_task with a capturing stub.

    Returns the mutable list the stub appends each call's kwargs to,
    so individual tests can assert against ``event_type`` / ``level``
    / ``details`` without any LogService wiring.
    """
    captured: List[dict] = []

    async def _stub(**kwargs: Any) -> None:
        captured.append(kwargs)

    monkeypatch.setattr(drain_task_mod, "log_event", _stub)
    return captured


@pytest.mark.asyncio
async def test_drain_processes_ready_rows(
    async_conn, async_schema, monkeypatch,
):
    """Happy path: 5 ready rows → BulkIndexer reports all passed →
    drain_once returns 5, rows transition to ``done``, no log events."""
    await ensure_storage_outbox_asyncpg(async_conn, async_schema)

    captured = _capture_log_event(monkeypatch)
    store = PgOutboxStore(single_conn=async_conn)
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
        catalog_id=async_schema,
        batch_size=10,
        worker_id="w",
    )
    n = await task.drain_once()
    assert n == 5
    assert len(received) == 5

    rows = await async_conn.fetch("SELECT status FROM storage_outbox")  # type: ignore[attr-defined]
    assert all(r["status"] == "done" for r in rows)
    # No failure events for an all-passed batch.
    assert captured == []


@pytest.mark.asyncio
async def test_drain_emits_retry_event_when_threshold_reached(
    async_conn, async_schema, monkeypatch,
):
    """Transient outcome with ``retry_visible_threshold=1`` — first
    drain bumps next-attempts to 1, hits the threshold, and emits an
    ``index_failure_retry`` log event at WARNING level."""
    await ensure_storage_outbox_asyncpg(async_conn, async_schema)

    captured = _capture_log_event(monkeypatch)
    store = PgOutboxStore(single_conn=async_conn)
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
        catalog_id=async_schema,
        batch_size=10,
        worker_id="w",
        retry_visible_threshold=1,
    )
    await task.drain_once()

    retry_events = [
        c for c in captured
        if c.get("event_type") == "index_failure_retry"
    ]
    assert len(retry_events) == 1
    e = retry_events[0]
    assert e["level"] == "WARNING"
    assert e["catalog_id"] == async_schema
    assert e["collection_id"] == "cc"
    details = e["details"]
    assert details["op_id"] == str(op_id)
    assert details["item_id"] == "i1"
    assert details["status"] == "retrying"
    assert details["attempts"] == 1
    assert details["error_class"] == "TransientIndexerError"
    assert details["error_message"] == "blip"

    # Re-claim and drain a second time — the threshold-cross emission
    # repeats on every transient outcome past the threshold.
    await async_conn.execute(  # type: ignore[attr-defined]
        "UPDATE storage_outbox SET ready_at = now() WHERE op_id = $1", op_id,
    )
    await task.drain_once()
    retry_events = [
        c for c in captured
        if c.get("event_type") == "index_failure_retry"
    ]
    assert len(retry_events) == 2


@pytest.mark.asyncio
async def test_drain_marks_poison_and_emits_persistent_event(
    async_conn, async_schema, monkeypatch,
):
    """Poison outcome → row goes to terminal ``failed`` and one
    ``index_failure_persistent`` ERROR event fires regardless of the
    retry threshold."""
    await ensure_storage_outbox_asyncpg(async_conn, async_schema)

    captured = _capture_log_event(monkeypatch)
    store = PgOutboxStore(single_conn=async_conn)
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
        catalog_id=async_schema,
        batch_size=10,
        worker_id="w",
    )
    await task.drain_once()

    persistent_events = [
        c for c in captured
        if c.get("event_type") == "index_failure_persistent"
    ]
    assert len(persistent_events) == 1
    e = persistent_events[0]
    assert e["level"] == "ERROR"
    assert e["catalog_id"] == async_schema
    assert e["collection_id"] == "cc"
    details = e["details"]
    assert details["op_id"] == str(op_id)
    assert details["item_id"] == "i1"
    assert details["status"] == "failed"
    assert details["error_class"] == "PoisonIndexerError"
    assert details["error_message"] == "malformed"

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


def test_outbox_drain_task_type_is_index_drain():
    """task_type must be 'index_drain' after the Phase 0 rename.

    'outbox_drain' collided with the event-consumer routing key.  The new
    name 'index_drain' unambiguously describes this task's role (draining
    the storage_outbox into the ES index).
    """
    assert OutboxDrainTask.task_type == "index_drain", (
        "OutboxDrainTask.task_type must be 'index_drain'; 'outbox_drain' was "
        "the overloaded pre-rename value that collided with EVENT_TASK_KEY."
    )


def test_outbox_drain_task_legacy_alias_present():
    """legacy_task_types must include 'outbox_drain' for the one-release shim.

    The dispatcher's get_task_config() resolves legacy aliases so that rows
    written with task_type='outbox_drain' before the rename are still
    dispatched to OutboxDrainTask.  Remove once all old rows are drained.
    """
    assert hasattr(OutboxDrainTask, "legacy_task_types"), (
        "OutboxDrainTask must declare legacy_task_types for the shim."
    )
    assert "outbox_drain" in OutboxDrainTask.legacy_task_types, (
        "'outbox_drain' must appear in legacy_task_types so that old DB rows "
        "continue to be claimed by OutboxDrainTask."
    )
