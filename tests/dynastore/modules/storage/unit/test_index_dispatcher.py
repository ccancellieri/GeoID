#    Copyright 2026 FAO
#    Licensed under the Apache License, Version 2.0 (the "License");

"""Phase 1 unit tests for :class:`IndexDispatcher`.

Covers the four FailurePolicy branches (FATAL / OUTBOX / WARN / IGNORE)
on a stub :class:`Indexer`, plus single-op vs bulk fan-out.  The outbox
writer is None — Phase 2 will add coverage for the durable enqueue path.
"""

from __future__ import annotations

from typing import List, Optional, Sequence

import pytest

from dynastore.models.protocols.indexer import (
    BulkResult, Indexer, IndexContext, IndexOp,
)
from dynastore.modules.storage.index_dispatcher import (
    IndexDispatcher, IndexerFatal, TaskTableOutboxWriter,
    _bind_named_to_positional, get_index_dispatcher, reset_index_dispatcher,
)
from dynastore.modules.storage.routing_config import (
    FailurePolicy, Operation, OperationDriverEntry, WriteMode,
)


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------


class _StubIndexer:
    """Records calls; can be told to raise on a specific op_type."""

    def __init__(
        self,
        indexer_id: str,
        *,
        raise_on: Optional[str] = None,
        raise_on_ensure: bool = False,
    ) -> None:
        self.indexer_id = indexer_id
        self.raise_on = raise_on
        self.raise_on_ensure = raise_on_ensure
        self.calls: List[IndexOp] = []
        self.bulk_calls: List[Sequence[IndexOp]] = []
        self.ensure_calls: List[IndexContext] = []

    async def ensure_indexer(self, ctx: IndexContext) -> None:
        self.ensure_calls.append(ctx)
        if self.raise_on_ensure:
            raise RuntimeError("stub ensure_indexer failure")

    async def index(self, ctx: IndexContext, op: IndexOp) -> None:
        self.calls.append(op)
        if self.raise_on and op.op_type == self.raise_on:
            raise RuntimeError(f"stub failure on {op.op_type}")

    async def index_bulk(
        self, ctx: IndexContext, ops: Sequence[IndexOp],
    ) -> BulkResult:
        self.bulk_calls.append(ops)
        if self.raise_on:
            raise RuntimeError(f"stub bulk failure ({self.raise_on})")
        return BulkResult(total=len(ops), succeeded=len(ops))


class _StubRouting:
    def __init__(self, entries: List[OperationDriverEntry]) -> None:
        self.operations = {Operation.INDEX: entries}


def _make_dispatcher(
    entries: List[OperationDriverEntry],
    indexers: dict,
) -> IndexDispatcher:
    routing = _StubRouting(entries)

    async def routing_resolver(catalog, collection):
        return routing

    async def indexer_registry(indexer_id):
        return indexers.get(indexer_id)

    return IndexDispatcher(
        routing_resolver=routing_resolver,
        indexer_registry=indexer_registry,
    )


def _entry(driver_id: str, *, on_failure: FailurePolicy) -> OperationDriverEntry:
    return OperationDriverEntry(
        driver_id=driver_id,
        on_failure=on_failure,
        write_mode=WriteMode.SYNC,
        source="auto",
    )


def _ctx() -> IndexContext:
    return IndexContext(catalog="cat-x", collection="col-y", correlation_id="cid-1")


def _op(op_type: str = "upsert", entity_id: str = "item-1") -> IndexOp:
    return IndexOp(
        op_type=op_type,
        entity_type="item",
        entity_id=entity_id,
        payload={"foo": "bar"} if op_type == "upsert" else None,
    )


# ---------------------------------------------------------------------------
# Happy path — no failures
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# ensure_indexer (Phase 2e) — bootstrap before first write
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fan_out_calls_ensure_indexer_once_per_collection():
    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.WARN)],
        indexers={"a": a},
    )
    # Three ops on the same (catalog, collection) — only one ensure call.
    await dispatcher.fan_out(_ctx(), _op(entity_id="i1"))
    await dispatcher.fan_out(_ctx(), _op(entity_id="i2"))
    await dispatcher.fan_out(_ctx(), _op(entity_id="i3"))
    assert len(a.ensure_calls) == 1
    assert len(a.calls) == 3


@pytest.mark.asyncio
async def test_fan_out_re_runs_ensure_for_new_collection():
    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.WARN)],
        indexers={"a": a},
    )
    ctx_b = IndexContext(catalog="cat-x", collection="col-z", correlation_id="c2")
    await dispatcher.fan_out(_ctx(), _op())
    await dispatcher.fan_out(ctx_b, _op())
    # Two distinct collections → two ensure calls.
    assert len(a.ensure_calls) == 2


@pytest.mark.asyncio
async def test_ensure_indexer_failure_with_warn_skips_index_call():
    a = _StubIndexer("a", raise_on_ensure=True)
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.WARN)],
        indexers={"a": a},
    )
    await dispatcher.fan_out(_ctx(), _op())
    # ensure_indexer attempted once; index() never reached.
    assert len(a.ensure_calls) == 1
    assert len(a.calls) == 0


@pytest.mark.asyncio
async def test_ensure_indexer_failure_with_fatal_raises():
    a = _StubIndexer("a", raise_on_ensure=True)
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.FATAL)],
        indexers={"a": a},
    )
    with pytest.raises(IndexerFatal):
        await dispatcher.fan_out(_ctx(), _op())


@pytest.mark.asyncio
async def test_indexer_without_ensure_indexer_method_is_treated_as_ready():
    """Drivers in transition may not yet have ensure_indexer — the
    dispatcher must not block on missing methods.
    """

    class _LegacyIndexer:
        indexer_id = "legacy"
        def __init__(self):
            self.calls: List = []
        async def index(self, ctx, op):
            self.calls.append(op)
        async def index_bulk(self, ctx, ops):
            return BulkResult(total=len(ops), succeeded=len(ops))

    a = _LegacyIndexer()
    dispatcher = _make_dispatcher(
        entries=[_entry("legacy", on_failure=FailurePolicy.WARN)],
        indexers={"legacy": a},
    )
    await dispatcher.fan_out(_ctx(), _op())
    assert len(a.calls) == 1


@pytest.mark.asyncio
async def test_fan_out_calls_every_configured_indexer():
    a = _StubIndexer("a")
    b = _StubIndexer("b")
    dispatcher = _make_dispatcher(
        entries=[
            _entry("a", on_failure=FailurePolicy.WARN),
            _entry("b", on_failure=FailurePolicy.WARN),
        ],
        indexers={"a": a, "b": b},
    )
    await dispatcher.fan_out(_ctx(), _op())
    assert len(a.calls) == 1
    assert len(b.calls) == 1


@pytest.mark.asyncio
async def test_fan_out_skips_when_indexer_not_registered():
    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(
        entries=[
            _entry("a", on_failure=FailurePolicy.WARN),
            _entry("missing", on_failure=FailurePolicy.WARN),
        ],
        indexers={"a": a},  # 'missing' not registered
    )
    await dispatcher.fan_out(_ctx(), _op())
    assert len(a.calls) == 1


# ---------------------------------------------------------------------------
# Failure policies
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fatal_policy_raises_indexer_fatal():
    a = _StubIndexer("a", raise_on="upsert")
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.FATAL)],
        indexers={"a": a},
    )
    with pytest.raises(IndexerFatal) as exc_info:
        await dispatcher.fan_out(_ctx(), _op())
    assert exc_info.value.indexer_id == "a"
    assert exc_info.value.op.op_type == "upsert"


@pytest.mark.asyncio
async def test_warn_policy_swallows_failure_and_continues_to_next():
    a = _StubIndexer("a", raise_on="upsert")
    b = _StubIndexer("b")
    dispatcher = _make_dispatcher(
        entries=[
            _entry("a", on_failure=FailurePolicy.WARN),
            _entry("b", on_failure=FailurePolicy.WARN),
        ],
        indexers={"a": a, "b": b},
    )
    await dispatcher.fan_out(_ctx(), _op())  # must not raise
    assert len(a.calls) == 1
    assert len(b.calls) == 1


@pytest.mark.asyncio
async def test_ignore_policy_silent():
    a = _StubIndexer("a", raise_on="upsert")
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.IGNORE)],
        indexers={"a": a},
    )
    await dispatcher.fan_out(_ctx(), _op())  # must not raise


@pytest.mark.asyncio
async def test_outbox_policy_without_writer_degrades_to_warn(caplog):
    """Phase 1: no OutboxWriter wired → OUTBOX policy degrades to WARN.

    Phase 2 will replace this assertion: with an outbox, the row should
    be enqueued in the same TX.
    """
    a = _StubIndexer("a", raise_on="upsert")
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.OUTBOX)],
        indexers={"a": a},
    )
    import logging as _logging
    with caplog.at_level(_logging.WARNING):
        await dispatcher.fan_out(_ctx(), _op())
    # Two warnings expected: one-time degrade-notice + the actual failure.
    msgs = [r.getMessage() for r in caplog.records]
    assert any("on_failure=outbox" in m and "Phase 2" in m for m in msgs)


# ---------------------------------------------------------------------------
# Bulk fan-out
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fan_out_bulk_returns_per_indexer_results():
    a = _StubIndexer("a")
    b = _StubIndexer("b")
    dispatcher = _make_dispatcher(
        entries=[
            _entry("a", on_failure=FailurePolicy.WARN),
            _entry("b", on_failure=FailurePolicy.WARN),
        ],
        indexers={"a": a, "b": b},
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    results = await dispatcher.fan_out_bulk(_ctx(), ops)
    assert results["a"].succeeded == 2
    assert results["b"].succeeded == 2


# ---------------------------------------------------------------------------
# OutboxWriter — Phase 2a
# ---------------------------------------------------------------------------


class _FakePgConn:
    """Stub connection that records executed SQL for inspection."""

    def __init__(self) -> None:
        self.calls: List[tuple] = []

    async def execute(self, sql, *args):
        self.calls.append((sql, args))


@pytest.mark.asyncio
async def test_outbox_writer_skips_when_no_pg_conn(caplog):
    """When IndexContext has no pg_conn, atomicity can't be guaranteed —
    writer logs a warning and returns without raising.
    """
    writer = TaskTableOutboxWriter(task_schema_resolver=lambda: "tasks")
    import logging as _logging
    with caplog.at_level(_logging.WARNING):
        await writer.enqueue(
            indexer_id="x", ctx=_ctx(), op=_op(), last_error="boom",
        )
    assert any("ctx.pg_conn is None" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_outbox_writer_inserts_task_row_on_caller_conn():
    """Happy path: writer issues INSERT INTO {task_schema}.tasks on the
    caller's connection.  The shape of the SQL + bind args is the load-
    bearing assertion — this is the contract that gives the atomicity
    guarantee.
    """
    conn = _FakePgConn()
    ctx = IndexContext(
        catalog="cat-x", collection="col-y", correlation_id="cid-1",
        pg_conn=conn,
    )
    writer = TaskTableOutboxWriter(task_schema_resolver=lambda: "tasks")

    await writer.enqueue(
        indexer_id="items_elasticsearch_driver",
        ctx=ctx,
        op=_op(),
        last_error="ES timeout",
    )
    assert len(conn.calls) == 1
    sql, args = conn.calls[0]

    # SQL must INSERT into the tasks table with task_type='index_propagation'.
    assert "INSERT INTO tasks.tasks" in sql
    # Named placeholders should have been translated to $N positional args.
    assert "$1" in sql

    # Inspect the bind args by reconstructing the named->positional mapping.
    # We re-bind to verify the inputs JSON contains the indexer_id payload.
    inputs_json = next(
        (a for a in args if isinstance(a, str) and "items_elasticsearch_driver" in a),
        None,
    )
    assert inputs_json is not None
    import json as _json
    inputs = _json.loads(inputs_json)
    assert inputs["indexer_id"] == "items_elasticsearch_driver"
    assert inputs["entity_id"] == "item-1"
    assert inputs["op_type"] == "upsert"
    assert inputs["catalog"] == "cat-x"
    assert inputs["last_error"] == "ES timeout"


@pytest.mark.asyncio
async def test_outbox_dedup_key_is_stable_across_calls():
    """Same (indexer_id, entity_id, op_type) → same dedup_key.  Different
    op_type → different key.  Coalesces concurrent failures of the same
    item into one row, but keeps upsert/delete distinct.
    """
    op_a = _op("upsert", "abc")
    op_b = _op("delete", "abc")
    op_c = _op("upsert", "abc")
    k_a = TaskTableOutboxWriter._dedup_key("ix", op_a)
    k_b = TaskTableOutboxWriter._dedup_key("ix", op_b)
    k_c = TaskTableOutboxWriter._dedup_key("ix", op_c)
    assert k_a == k_c, "same op identity must coalesce"
    assert k_a != k_b, "upsert and delete must stay distinct"


# ---------------------------------------------------------------------------
# Singleton factory
# ---------------------------------------------------------------------------


def test_get_index_dispatcher_returns_singleton():
    """Repeated calls return the same instance; reset clears the cache."""
    reset_index_dispatcher()
    a = get_index_dispatcher()
    b = get_index_dispatcher()
    assert a is b
    reset_index_dispatcher()
    c = get_index_dispatcher()
    assert c is not a


@pytest.mark.asyncio
async def test_default_dispatcher_describe_with_no_routing_returns_empty_indexers():
    """Without ConfigsProtocol in the process the default routing
    resolver yields an empty CollectionRoutingConfig — describe returns
    an empty indexer list rather than blowing up.
    """
    reset_index_dispatcher()
    d = get_index_dispatcher()
    info = await d.describe(IndexContext(catalog="cat", collection="col"))
    assert info["indexers"] == [] or all(
        isinstance(x, dict) for x in info["indexers"]
    )


def test_bind_named_to_positional_handles_repeated_names():
    sql, args = _bind_named_to_positional(
        "SELECT :a, :b, :a, :c", {"a": 1, "b": 2, "c": 3},
    )
    assert sql == "SELECT $1, $2, $1, $3"
    assert args == [1, 2, 3]


# ---------------------------------------------------------------------------
# Dispatcher × OutboxWriter integration (Phase 2a)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_outbox_policy_with_writer_enqueues_on_failure():
    """OUTBOX policy + a real OutboxWriter should write the task row when
    the indexer call fails.  Validates the production durability path
    end-to-end against the dispatcher.
    """
    a = _StubIndexer("a", raise_on="upsert")
    conn = _FakePgConn()
    ctx_with_conn = IndexContext(
        catalog="cat-x", collection="col-y",
        correlation_id="cid-1", pg_conn=conn,
    )
    writer = TaskTableOutboxWriter(task_schema_resolver=lambda: "tasks")

    routing = _StubRouting([_entry("a", on_failure=FailurePolicy.OUTBOX)])

    async def routing_resolver(catalog, collection):
        return routing

    async def indexer_registry(indexer_id):
        return {"a": a}.get(indexer_id)

    dispatcher = IndexDispatcher(
        routing_resolver=routing_resolver,
        indexer_registry=indexer_registry,
        outbox=writer,
    )
    await dispatcher.fan_out(ctx_with_conn, _op())

    # Indexer was attempted once (and raised).
    assert len(a.calls) == 1
    # Outbox row was written on the caller's connection.
    assert len(conn.calls) == 1
    sql, _ = conn.calls[0]
    assert "INSERT INTO tasks.tasks" in sql


@pytest.mark.asyncio
async def test_fan_out_bulk_failure_with_warn_returns_failure_summary():
    a = _StubIndexer("a", raise_on="upsert")
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.WARN)],
        indexers={"a": a},
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    results = await dispatcher.fan_out_bulk(_ctx(), ops)
    assert results["a"].failed == 2
    assert results["a"].failures
