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
    BulkResult, IndexContext, IndexOp,
)
from dynastore.modules.storage.index_dispatcher import (
    IndexDispatcher, IndexerFatal, TaskTableOutboxWriter,
    get_index_dispatcher, reset_index_dispatcher,
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
        self.operations = {Operation.WRITE: entries}


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


def _entry(driver_ref: str, *, on_failure: FailurePolicy) -> OperationDriverEntry:
    return OperationDriverEntry(
        driver_ref=driver_ref,
        on_failure=on_failure,
        write_mode=WriteMode.SYNC,
        secondary_index=True,
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
    await dispatcher.fan_out_bulk(_ctx(), [_op(entity_id="i1")])
    await dispatcher.fan_out_bulk(_ctx(), [_op(entity_id="i2")])
    await dispatcher.fan_out_bulk(_ctx(), [_op(entity_id="i3")])
    assert len(a.ensure_calls) == 1
    assert len(a.bulk_calls) == 3


@pytest.mark.asyncio
async def test_fan_out_re_runs_ensure_for_new_collection():
    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.WARN)],
        indexers={"a": a},
    )
    ctx_b = IndexContext(catalog="cat-x", collection="col-z", correlation_id="c2")
    await dispatcher.fan_out_bulk(_ctx(), [_op()])
    await dispatcher.fan_out_bulk(ctx_b, [_op()])
    # Two distinct collections → two ensure calls.
    assert len(a.ensure_calls) == 2


@pytest.mark.asyncio
async def test_ensure_indexer_failure_with_warn_skips_index_call():
    a = _StubIndexer("a", raise_on_ensure=True)
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.WARN)],
        indexers={"a": a},
    )
    await dispatcher.fan_out_bulk(_ctx(), [_op()])
    # ensure_indexer attempted once; index() never reached.
    assert len(a.ensure_calls) == 1
    assert len(a.bulk_calls) == 0


@pytest.mark.asyncio
async def test_ensure_indexer_failure_with_fatal_raises():
    a = _StubIndexer("a", raise_on_ensure=True)
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.FATAL)],
        indexers={"a": a},
    )
    with pytest.raises(IndexerFatal):
        await dispatcher.fan_out_bulk(_ctx(), [_op()])


@pytest.mark.asyncio
async def test_indexer_without_ensure_indexer_method_is_treated_as_ready():
    """Drivers in transition may not yet have ensure_indexer — the
    dispatcher must not block on missing methods.
    """

    class _LegacyIndexer:
        indexer_id = "legacy"
        def __init__(self):
            self.bulk_calls: List = []
        async def index(self, ctx, op):
            raise AssertionError("dispatcher should call index_bulk only")
        async def index_bulk(self, ctx, ops):
            self.bulk_calls.append(list(ops))
            return BulkResult(total=len(ops), succeeded=len(ops))

    a = _LegacyIndexer()
    dispatcher = _make_dispatcher(
        entries=[_entry("legacy", on_failure=FailurePolicy.WARN)],
        indexers={"legacy": a},
    )
    await dispatcher.fan_out_bulk(_ctx(), [_op()])
    assert len(a.bulk_calls) == 1
    assert len(a.bulk_calls[0]) == 1


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
    await dispatcher.fan_out_bulk(_ctx(), [_op()])
    assert len(a.bulk_calls) == 1
    assert len(b.bulk_calls) == 1


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
    await dispatcher.fan_out_bulk(_ctx(), [_op()])
    assert len(a.bulk_calls) == 1


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
        await dispatcher.fan_out_bulk(_ctx(), [_op()])
    assert exc_info.value.indexer_id == "a"
    assert exc_info.value.op.op_type == "upsert"


def test_indexer_fatal_renders_descriptor_for_empty_batch():
    # _handle_failure_bulk passes ops[0] if ops else None — when an upstream
    # filter rejects every op in a FATAL batch, IndexerFatal is constructed
    # with op=None. PR #610 widened the type; this asserts the descriptor.
    err = IndexerFatal("ix1", None, RuntimeError("boom"))
    msg = str(err)
    assert "<empty batch>" in msg
    assert "ix1" in msg
    assert err.op is None


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
    await dispatcher.fan_out_bulk(_ctx(), [_op()])  # must not raise
    assert len(a.bulk_calls) == 1
    assert len(b.bulk_calls) == 1


@pytest.mark.asyncio
async def test_ignore_policy_silent():
    a = _StubIndexer("a", raise_on="upsert")
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.IGNORE)],
        indexers={"a": a},
    )
    await dispatcher.fan_out_bulk(_ctx(), [_op()])  # must not raise


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
        await dispatcher.fan_out_bulk(_ctx(), [_op()])
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
    """Stub connection that records executed SQL for inspection.

    Shaped for the SQLAlchemy sync ``Connection`` contract because
    ``DQLExecutor`` routes non-SA-async resources through the sync
    workflow (``conn.execute(statement, parameters)``). The fake is
    intentionally NOT a real ``AsyncConnection`` — that keeps the test
    independent of a live engine while exercising the same outbox path
    the production wiring uses.
    """

    def __init__(self) -> None:
        self.calls: List[tuple] = []

    def execute(self, statement, parameters=None):
        # ``statement`` is a SQLAlchemy ``TextClause`` — render to str so
        # asserts can match on the INSERT shape regardless of bind form.
        self.calls.append((str(statement), parameters or {}))
        return None


@pytest.mark.asyncio
async def test_outbox_writer_skips_when_no_pg_conn(caplog):
    """When IndexContext has no pg_conn, atomicity can't be guaranteed —
    writer logs a warning and returns without raising.
    """
    writer = TaskTableOutboxWriter(task_schema_resolver=lambda: "tasks")
    import logging as _logging
    with caplog.at_level(_logging.WARNING):
        await writer.enqueue(
            indexer_id="x", ctx=_ctx(), ops=[_op()], last_error="boom",
        )
    assert any("ctx.pg_conn is None" in r.getMessage() for r in caplog.records)


class _RecordingWriter(TaskTableOutboxWriter):
    """Bypass DQLQuery so chunk emission can be asserted in isolation
    from the SA dialect resolution layer (which the legacy ``_FakePgConn``
    fixture no longer satisfies)."""

    def __init__(self) -> None:
        super().__init__(task_schema_resolver=lambda: "tasks")
        self.rows: list[dict] = []

    async def _exec_insert(self, conn, sql, params):  # type: ignore[override]
        self.rows.append({"sql": sql, "params": dict(params)})



@pytest.mark.asyncio
async def test_outbox_writer_inserts_task_row_on_caller_conn():
    """Happy path: writer issues INSERT INTO {task_schema}.tasks on the
    caller's connection.  The shape of the SQL + bind args is the load-
    bearing assertion — this is the contract that gives the atomicity
    guarantee.
    """
    ctx = IndexContext(
        catalog="cat-x", collection="col-y", correlation_id="cid-1",
        pg_conn=object(),
    )
    writer = _RecordingWriter()

    await writer.enqueue(
        indexer_id="items_elasticsearch_driver",
        ctx=ctx,
        ops=[_op()],
        last_error="ES timeout",
    )
    assert len(writer.rows) == 1
    sql = writer.rows[0]["sql"]
    params = writer.rows[0]["params"]

    # SQL must INSERT into the tasks table with task_type='index_propagation'.
    assert "INSERT INTO tasks.tasks" in sql
    # Named binds preserved (DQLQuery uses sqlalchemy text named binds).
    assert ":task_id" in sql
    # ``inputs`` is a JSON-serialized payload bound by name.
    import json as _json
    inputs = _json.loads(params["inputs"])
    assert inputs["indexer_id"] == "items_elasticsearch_driver"
    assert inputs["ops"][0]["entity_id"] == "item-1"
    assert inputs["op_type"] == "upsert"
    assert inputs["catalog"] == "cat-x"
    assert inputs["last_error"] == "ES timeout"


@pytest.mark.asyncio
async def test_outbox_dedup_key_is_stable_across_calls():
    """Same chunk identity → same dedup_key.  Different op_type → distinct
    key.  Different chunk membership → distinct key.
    """
    k_a = TaskTableOutboxWriter._dedup_key("ix", "upsert", "item", ["abc"])
    k_b = TaskTableOutboxWriter._dedup_key("ix", "delete", "item", ["abc"])
    k_c = TaskTableOutboxWriter._dedup_key("ix", "upsert", "item", ["abc"])
    k_d = TaskTableOutboxWriter._dedup_key("ix", "upsert", "item", ["abc", "def"])
    # Same chunk content, different ordering → same key (sorted).
    k_d2 = TaskTableOutboxWriter._dedup_key("ix", "upsert", "item", ["def", "abc"])
    assert k_a == k_c, "same chunk identity must coalesce"
    assert k_a != k_b, "upsert and delete must stay distinct"
    assert k_a != k_d, "different chunk membership must produce distinct keys"
    assert k_d == k_d2, "order-independent: sort entity_ids before hashing"


@pytest.mark.asyncio
async def test_outbox_chunks_large_batch_into_one_row_per_chunk():
    """A 1500-op batch with chunk_size=500 → 3 task rows, each carrying
    500 ops under inputs.ops.
    """
    ctx = IndexContext(
        catalog="cat-x", collection="col-y", correlation_id="cid-1",
        pg_conn=object(),  # truthy; bypassed by _RecordingWriter._exec_insert
    )
    writer = _RecordingWriter()
    ops = [_op(entity_id=f"i{i}") for i in range(1500)]

    await writer.enqueue(
        indexer_id="items_elasticsearch_driver",
        ctx=ctx, ops=ops, chunk_size=500,
    )
    assert len(writer.rows) == 3
    import json as _json
    sizes = [
        len(_json.loads(r["params"]["inputs"])["ops"]) for r in writer.rows
    ]
    assert sizes == [500, 500, 500]
    keys = {r["params"]["dedup_key"] for r in writer.rows}
    assert len(keys) == 3, "distinct chunks must produce distinct dedup_keys"


@pytest.mark.asyncio
async def test_outbox_one_op_call_writes_single_row_of_one_op():
    ctx = IndexContext(
        catalog="cat-x", collection="col-y", correlation_id="cid-1",
        pg_conn=object(),
    )
    writer = _RecordingWriter()

    await writer.enqueue(
        indexer_id="items_elasticsearch_driver",
        ctx=ctx, ops=[_op()],
    )
    assert len(writer.rows) == 1
    import json as _json
    inputs = _json.loads(writer.rows[0]["params"]["inputs"])
    assert len(inputs["ops"]) == 1
    assert inputs["ops"][0]["entity_id"] == "item-1"


@pytest.mark.asyncio
async def test_outbox_mixed_op_types_chunk_separately():
    """Mixing upsert + delete in one enqueue call splits per op_type so
    each chunk's dedup_key stays meaningful (upsert/delete don't share
    a coalescing identity).
    """
    ctx = IndexContext(
        catalog="cat-x", collection="col-y", correlation_id="cid-1",
        pg_conn=object(),
    )
    writer = _RecordingWriter()
    ops = [
        _op("upsert", entity_id="a"),
        _op("delete", entity_id="b"),
        _op("upsert", entity_id="c"),
    ]
    await writer.enqueue(
        indexer_id="items_elasticsearch_driver",
        ctx=ctx, ops=ops,
    )
    assert len(writer.rows) == 2
    import json as _json
    op_types = sorted(
        _json.loads(r["params"]["inputs"])["op_type"] for r in writer.rows
    )
    assert op_types == ["delete", "upsert"]


@pytest.mark.asyncio
async def test_outbox_empty_ops_is_noop():
    ctx = IndexContext(
        catalog="cat", collection="col", correlation_id="cid",
        pg_conn=object(),
    )
    writer = _RecordingWriter()
    await writer.enqueue(indexer_id="x", ctx=ctx, ops=[])
    assert writer.rows == []


# ---------------------------------------------------------------------------
# Singleton factory
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_index_dispatcher_returns_singleton():
    """Repeated calls return the same instance; reset clears the cache."""
    await reset_index_dispatcher()
    a = get_index_dispatcher()
    b = get_index_dispatcher()
    assert a is b
    await reset_index_dispatcher()
    c = get_index_dispatcher()
    assert c is not a


@pytest.mark.asyncio
async def test_default_dispatcher_describe_with_no_routing_returns_empty_indexers():
    """Without ConfigsProtocol in the process the default routing
    resolver yields an empty CollectionRoutingConfig — describe returns
    an empty indexer list rather than blowing up.
    """
    await reset_index_dispatcher()
    d = get_index_dispatcher()
    info = await d.describe(IndexContext(catalog="cat", collection="col"))
    assert info["indexers"] == [] or all(
        isinstance(x, dict) for x in info["indexers"]
    )


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
    ctx_with_conn = IndexContext(
        catalog="cat-x", collection="col-y",
        correlation_id="cid-1", pg_conn=object(),
    )
    writer = _RecordingWriter()

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
    await dispatcher.fan_out_bulk(ctx_with_conn, [_op()])

    # Indexer was attempted once (and raised).
    assert len(a.bulk_calls) == 1
    # Outbox row was written on the caller's connection.
    assert len(writer.rows) == 1
    assert "INSERT INTO tasks.tasks" in writer.rows[0]["sql"]


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


# ---------------------------------------------------------------------------
# #504 — index_dispatch_path structured log lines (mode + chunk_size)
# ---------------------------------------------------------------------------


def _extract_dispatch_path_records(caplog) -> List[dict]:
    """Parse the structured `index_dispatch_path mode=... ...` log lines."""
    rows: List[dict] = []
    for rec in caplog.records:
        msg = rec.getMessage()
        if not msg.startswith("index_dispatch_path "):
            continue
        fields: dict = {}
        for token in msg.split(" ")[1:]:
            if "=" not in token:
                continue
            k, v = token.split("=", 1)
            fields[k] = v
        rows.append(fields)
    return rows


@pytest.mark.asyncio
async def test_dispatch_path_logged_post_commit_inline(caplog):
    import logging as _logging
    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.WARN)],
        indexers={"a": a},
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2"), _op(entity_id="i3")]
    with caplog.at_level(_logging.INFO):
        await dispatcher.fan_out_bulk(_ctx(), ops)
    rows = _extract_dispatch_path_records(caplog)
    assert any(
        r.get("mode") == "post_commit_inline"
        and r.get("indexer") == "a"
        and r.get("chunk_size") == "3"
        for r in rows
    ), rows


@pytest.mark.asyncio
async def test_dispatch_path_not_logged_on_failure(caplog):
    import logging as _logging
    a = _StubIndexer("a", raise_on="upsert")
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.WARN)],
        indexers={"a": a},
    )
    with caplog.at_level(_logging.INFO):
        await dispatcher.fan_out_bulk(_ctx(), [_op(entity_id="i1")])
    rows = _extract_dispatch_path_records(caplog)
    assert not any(r.get("mode") == "post_commit_inline" for r in rows), rows


# ---------------------------------------------------------------------------
# #914 — silent no-op trap (dispatch-level): ops submitted with no routing entry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fan_out_warns_when_ops_submitted_but_routing_returns_no_entries(caplog):
    """If RoutingConfig.operations[WRITE] has no secondary-index entries,
    every write silently skips every indexer. Pin a WARN that names the scope
    so a misconfigured routing table is visible in logs instead of invisibly
    swallowing writes.
    """
    import logging as _logging
    dispatcher = _make_dispatcher(entries=[], indexers={})
    with caplog.at_level(_logging.WARNING):
        results = await dispatcher.fan_out_bulk(_ctx(), [_op(entity_id="i1")])
    assert results == {}
    msgs = [r.getMessage() for r in caplog.records if r.levelno >= _logging.WARNING]
    assert any(
        "routing returned NO secondary-index entries" in m
        and "cat-x" in m and "col-y" in m
        for m in msgs
    ), msgs


@pytest.mark.asyncio
async def test_fan_out_does_not_warn_when_no_ops_and_no_entries(caplog):
    """Empty-ops dispatch is a legitimate no-op; should not log the
    #914 WARN (which would be a false positive on routine probes).
    """
    import logging as _logging
    dispatcher = _make_dispatcher(entries=[], indexers={})
    with caplog.at_level(_logging.WARNING):
        results = await dispatcher.fan_out_bulk(_ctx(), [])
    assert results == {}
    msgs = [r.getMessage() for r in caplog.records if r.levelno >= _logging.WARNING]
    assert not any(
        "routing returned NO secondary-index entries" in m for m in msgs
    ), msgs


# ---------------------------------------------------------------------------
# #1861 — silent no-op converted to retryable outbox failure
# ---------------------------------------------------------------------------


class _NoopIndexer:
    """Stub indexer that always returns BulkResult(total=N, succeeded=0, failed=0)."""

    def __init__(self, indexer_id: str) -> None:
        self.indexer_id = indexer_id
        self.bulk_calls: List[Sequence[IndexOp]] = []
        self.ensure_calls: List[IndexContext] = []

    async def ensure_indexer(self, ctx: IndexContext) -> None:
        self.ensure_calls.append(ctx)

    async def index(self, ctx: IndexContext, op: IndexOp) -> None:  # pragma: no cover
        pass

    async def index_bulk(
        self, ctx: IndexContext, ops: Sequence[IndexOp],
    ) -> BulkResult:
        self.bulk_calls.append(ops)
        return BulkResult(total=len(ops), succeeded=0, failed=0)


def _make_dispatcher_with_outbox(
    entries: List[OperationDriverEntry],
    indexers: dict,
    outbox,
) -> IndexDispatcher:
    routing = _StubRouting(entries)

    async def routing_resolver(catalog, collection):
        return routing

    async def indexer_registry(indexer_id):
        return indexers.get(indexer_id)

    return IndexDispatcher(
        routing_resolver=routing_resolver,
        indexer_registry=indexer_registry,
        outbox=outbox,
    )


@pytest.mark.asyncio
async def test_silent_noop_upsert_batch_enqueues_and_returns_failed():
    """A stub indexer returning BulkResult(total=2, succeeded=0, failed=0)
    for 2 upsert ops must:
      (a) trigger an outbox enqueue call with those ops, and
      (b) return a result with failed == 2.
    """
    ctx_with_conn = IndexContext(
        catalog="cat-x", collection="col-y",
        correlation_id="cid-1", pg_conn=object(),
    )
    noop = _NoopIndexer("noop-es")
    writer = _RecordingWriter()

    dispatcher = _make_dispatcher_with_outbox(
        entries=[_entry("noop-es", on_failure=FailurePolicy.OUTBOX)],
        indexers={"noop-es": noop},
        outbox=writer,
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    results = await dispatcher.fan_out_bulk(ctx_with_conn, ops)

    # (a) outbox must have been written
    assert len(writer.rows) >= 1, "outbox enqueue must be called for noop upserts"
    import json as _json
    enqueued_ids = {
        item["entity_id"]
        for row in writer.rows
        for item in _json.loads(row["params"]["inputs"])["ops"]
    }
    assert "i1" in enqueued_ids
    assert "i2" in enqueued_ids

    # (b) failed count must equal the number of noop upsert ops
    assert results["noop-es"].failed == 2


@pytest.mark.asyncio
async def test_silent_noop_does_not_log_as_clean_success(caplog):
    """A converted no-op must NOT produce an index_dispatch_path
    post_commit_inline success log line.
    """
    import logging as _logging
    ctx_with_conn = IndexContext(
        catalog="cat-x", collection="col-y",
        correlation_id="cid-1", pg_conn=object(),
    )
    noop = _NoopIndexer("noop-es")
    writer = _RecordingWriter()

    dispatcher = _make_dispatcher_with_outbox(
        entries=[_entry("noop-es", on_failure=FailurePolicy.OUTBOX)],
        indexers={"noop-es": noop},
        outbox=writer,
    )
    with caplog.at_level(_logging.INFO):
        await dispatcher.fan_out_bulk(ctx_with_conn, [_op(entity_id="i1")])

    rows = _extract_dispatch_path_records(caplog)
    assert not any(r.get("mode") == "post_commit_inline" for r in rows), (
        "A converted no-op must not appear as a clean post_commit_inline success"
    )


@pytest.mark.asyncio
async def test_normal_success_does_not_enqueue():
    """A real success (succeeded=N) must NOT trigger outbox enqueue."""
    ctx_with_conn = IndexContext(
        catalog="cat-x", collection="col-y",
        correlation_id="cid-1", pg_conn=object(),
    )
    a = _StubIndexer("a")
    writer = _RecordingWriter()

    dispatcher = _make_dispatcher_with_outbox(
        entries=[_entry("a", on_failure=FailurePolicy.OUTBOX)],
        indexers={"a": a},
        outbox=writer,
    )
    results = await dispatcher.fan_out_bulk(ctx_with_conn, [_op(entity_id="i1")])

    assert writer.rows == [], "normal success must not enqueue anything"
    assert results["a"].succeeded == 1
    assert results["a"].failed == 0


@pytest.mark.asyncio
async def test_silent_noop_delete_only_does_not_enqueue():
    """A silent no-op batch composed only of delete ops must NOT be
    enqueued (delete pass-through is unaffected by this change).
    """
    ctx_with_conn = IndexContext(
        catalog="cat-x", collection="col-y",
        correlation_id="cid-1", pg_conn=object(),
    )
    noop = _NoopIndexer("noop-es")
    writer = _RecordingWriter()

    dispatcher = _make_dispatcher_with_outbox(
        entries=[_entry("noop-es", on_failure=FailurePolicy.OUTBOX)],
        indexers={"noop-es": noop},
        outbox=writer,
    )
    delete_op = _op(op_type="delete", entity_id="d1")
    results = await dispatcher.fan_out_bulk(ctx_with_conn, [delete_op])

    assert writer.rows == [], "delete-only no-op must not enqueue"
    # failed count stays 0 for deletes (no upserts to convert)
    assert results["noop-es"].failed == 0
