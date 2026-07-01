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
    INLINE_DISPATCH_CHUNK_SIZE, IndexDispatcher, IndexerFatal,
    TaskTableOutboxWriter, get_index_dispatcher, reset_index_dispatcher,
)
from dynastore.modules.storage.routing_config import (
    FailurePolicy, Operation, OperationDriverEntry, WriteMode,
)
from dynastore.tools.execution_context import in_task_run, task_run_scope


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
# #2064 — per-document rejections from a successful bulk are surfaced
# (per-item index_failure event + partial_failure_drop dispatch counter)
# instead of being silently absorbed into a count.
# ---------------------------------------------------------------------------


class _PartialFailIndexer:
    """``index_bulk`` returns a 200-bulk that rejected individual docs."""

    def __init__(self, indexer_id: str, failures: List[dict]) -> None:
        self.indexer_id = indexer_id
        self._failures = failures
        self.bulk_calls: List = []

    async def ensure_indexer(self, ctx: IndexContext) -> None:
        pass

    async def index_bulk(self, ctx, ops):
        self.bulk_calls.append(list(ops))
        n = len(ops)
        return BulkResult(
            total=n,
            succeeded=n - len(self._failures),
            failed=len(self._failures),
            failures=list(self._failures),
        )


def _capture_log_events(monkeypatch) -> List[dict]:
    import dynastore.modules.catalog.log_manager as log_manager

    events: List[dict] = []

    async def _capture(**kwargs):
        events.append(kwargs)
        return 1

    monkeypatch.setattr(log_manager, "log_event", _capture)
    return events


@pytest.mark.asyncio
async def test_partial_bulk_failures_emit_index_failure_events(monkeypatch, caplog):
    import logging

    events = _capture_log_events(monkeypatch)
    fail = {"id": "item-9", "reason": "400 invalid_shape_exception: duplicate coords"}
    idx = _PartialFailIndexer("a", [fail])
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.WARN)],
        indexers={"a": idx},
    )

    with caplog.at_level(logging.INFO, logger="dynastore.modules.storage.index_dispatcher"):
        results = await dispatcher.fan_out_bulk(
            _ctx(), [_op(entity_id="item-9"), _op(entity_id="item-ok")],
        )

    # One per-item index_failure_persistent event for the rejected doc.
    assert len(events) == 1
    ev = events[0]
    assert ev["event_type"] == "index_failure_persistent"
    assert ev["level"] == "ERROR"
    assert ev["details"]["item_id"] == "item-9"
    assert ev["details"]["source"] == "inline_dispatch_partial_bulk"
    assert ev["details"]["status"] == "dropped"
    assert "invalid_shape_exception" in ev["details"]["reason"]
    # The #504 dispatch-path counter records the drop.
    assert "mode=partial_failure_drop" in caplog.text
    # The failure is still reported back to the caller (207-style).
    assert results["a"].failed == 1


@pytest.mark.asyncio
async def test_clean_bulk_emits_no_index_failure_event(monkeypatch):
    events = _capture_log_events(monkeypatch)
    a = _StubIndexer("a")  # all-succeeded
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.WARN)],
        indexers={"a": a},
    )
    await dispatcher.fan_out_bulk(_ctx(), [_op()])
    assert events == []


@pytest.mark.asyncio
async def test_raised_bulk_does_not_emit_partial_failure_event(monkeypatch):
    """A bulk that RAISES is routed through the on_failure policy; the
    synthetic failures from the except branch are not per-doc rejections and
    must NOT also fire a partial-failure index_failure event (no double-signal)."""
    events = _capture_log_events(monkeypatch)
    a = _StubIndexer("a", raise_on="upsert")  # index_bulk raises
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.WARN)],
        indexers={"a": a},
    )
    results = await dispatcher.fan_out_bulk(_ctx(), [_op(op_type="upsert")])
    assert events == []
    assert results["a"].failed == 1


@pytest.mark.asyncio
async def test_partial_failure_surface_is_fail_open(monkeypatch):
    """If the observability emit raises, the committed write is unaffected."""
    import dynastore.modules.catalog.log_manager as log_manager

    async def _boom(**kwargs):
        raise RuntimeError("log service down")

    monkeypatch.setattr(log_manager, "log_event", _boom)

    idx = _PartialFailIndexer("a", [{"id": "item-9", "reason": "invalid_shape_exception"}])
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.WARN)],
        indexers={"a": idx},
    )
    # Must not raise despite the failing log_event.
    results = await dispatcher.fan_out_bulk(_ctx(), [_op(entity_id="item-9")])
    assert results["a"].failed == 1


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


# ---------------------------------------------------------------------------
# write_mode=ASYNC — dispatcher honors the routing-config contract
# (Refs #2438)
# ---------------------------------------------------------------------------


def _async_entry(driver_ref: str) -> OperationDriverEntry:
    """ASYNC secondary-index entry matching the items_elasticsearch_driver config."""
    return OperationDriverEntry(
        driver_ref=driver_ref,
        on_failure=FailurePolicy.OUTBOX,
        write_mode=WriteMode.ASYNC,
        secondary_index=True,
        source="auto",
    )


@pytest.mark.asyncio
async def test_async_write_mode_skips_inline_index_and_enqueues():
    """An ASYNC entry must NOT call the indexer inline; all ops go to the
    outbox so ES indexing runs off the write path.
    """
    assert not in_task_run(), "this test exercises the serving (non-task-run) path"
    a = _StubIndexer("a")
    writer = _RecordingWriter()
    ctx_with_conn = IndexContext(
        catalog="cat-x", collection="col-y",
        correlation_id="cid-1", pg_conn=object(),
    )
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_async_entry("a")],
        indexers={"a": a},
        outbox=writer,
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    await dispatcher.fan_out_bulk(ctx_with_conn, ops)

    # Inline indexer must NOT have been called.
    assert a.bulk_calls == [], (
        "write_mode=ASYNC must not call the indexer inline"
    )
    # Outbox must have received the ops.
    assert len(writer.rows) >= 1, "ASYNC entry must enqueue ops to outbox"
    import json as _json
    enqueued_ids = {
        item["entity_id"]
        for row in writer.rows
        for item in _json.loads(row["params"]["inputs"])["ops"]
    }
    assert "i1" in enqueued_ids
    assert "i2" in enqueued_ids


@pytest.mark.asyncio
async def test_async_not_in_task_run_still_enqueues_outbox():
    """Explicit companion to the test above: outside a task run, an ASYNC
    entry preserves the existing outbox behaviour and zero rows are
    dispatched inline.
    """
    assert not in_task_run()
    a = _StubIndexer("a")
    writer = _RecordingWriter()
    ctx_with_conn = IndexContext(
        catalog="cat-x", collection="col-y",
        correlation_id="cid-1", pg_conn=object(),
    )
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_async_entry("a")],
        indexers={"a": a},
        outbox=writer,
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    results = await dispatcher.fan_out_bulk(ctx_with_conn, ops)

    assert a.bulk_calls == [], "indexer must not be called inline outside a task run"
    assert len(writer.rows) >= 1, "outbox must be enqueued outside a task run"
    assert results["a"].succeeded == 2


@pytest.mark.asyncio
async def test_async_in_task_run_dispatches_inline_and_enqueues_zero_outbox():
    """Inside a task run, an ASYNC entry must be absorbed inline through
    the driver-agnostic bulk path instead of spawning an outbox row.
    """
    a = _StubIndexer("a")
    writer = _RecordingWriter()
    ctx_with_conn = IndexContext(
        catalog="cat-x", collection="col-y",
        correlation_id="cid-1", pg_conn=object(),
    )
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_async_entry("a")],
        indexers={"a": a},
        outbox=writer,
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(ctx_with_conn, ops)

    # Indexer received the ops inline.
    assert len(a.bulk_calls) == 1
    assert list(a.bulk_calls[0]) == ops
    # Zero outbox rows — the whole point of the fix.
    assert writer.rows == []
    assert results["a"].succeeded == 2
    assert results["a"].failed == 0


@pytest.mark.asyncio
async def test_inline_in_task_run_chunks_large_batch():
    """A batch larger than INLINE_DISPATCH_CHUNK_SIZE is split into
    sequential chunked driver calls; the aggregated result reflects the
    full batch.
    """
    a = _StubIndexer("a")
    writer = _RecordingWriter()
    ctx_with_conn = IndexContext(
        catalog="cat-x", collection="col-y",
        correlation_id="cid-1", pg_conn=object(),
    )
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_async_entry("a")],
        indexers={"a": a},
        outbox=writer,
    )
    n = INLINE_DISPATCH_CHUNK_SIZE * 2 + 37
    ops = [_op(entity_id=f"i{i}") for i in range(n)]
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(ctx_with_conn, ops)

    import math
    expected_chunks = math.ceil(n / INLINE_DISPATCH_CHUNK_SIZE)
    assert len(a.bulk_calls) == expected_chunks
    assert sum(len(c) for c in a.bulk_calls) == n
    assert writer.rows == []
    assert results["a"].total == n
    assert results["a"].succeeded == n


@pytest.mark.asyncio
async def test_inline_in_task_run_opens_one_short_tx_per_chunk():
    """With a tx_factory (the in-task-run inline path), the dispatcher opens
    a FRESH transaction per chunk — never one long-lived transaction across
    the whole sequential fan-out — and enters/exits each one, so a busy job
    never parks a pooled connection with an open transaction across the full
    ES dispatch.
    """
    opened: list = []

    class _FakeTx:
        def __init__(self) -> None:
            self.entered = False
            self.exited = False
            self.conn = object()

        async def __aenter__(self):
            self.entered = True
            return self.conn

        async def __aexit__(self, *exc):
            self.exited = True
            return False

    def _tx_factory():
        tx = _FakeTx()
        opened.append(tx)
        return tx

    a = _StubIndexer("a")
    writer = _RecordingWriter()
    ctx = IndexContext(
        catalog="cat-x", collection="col-y",
        correlation_id="cid-1", pg_conn=None,
    )
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_async_entry("a")],
        indexers={"a": a},
        outbox=writer,
    )
    n = INLINE_DISPATCH_CHUNK_SIZE * 2 + 5
    ops = [_op(entity_id=f"i{i}") for i in range(n)]
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(ctx, ops, tx_factory=_tx_factory)

    import math
    expected_chunks = math.ceil(n / INLINE_DISPATCH_CHUNK_SIZE)
    # One transaction per chunk, each entered AND exited (released between
    # chunks — not one TX spanning the whole batch).
    assert len(opened) == expected_chunks
    assert all(tx.entered and tx.exited for tx in opened)
    assert len(a.bulk_calls) == expected_chunks
    assert results["a"].succeeded == n
    assert writer.rows == []


@pytest.mark.asyncio
async def test_async_write_mode_result_signals_all_accepted():
    """The BulkResult for an ASYNC entry reports succeeded=N so
    health-check callers see 'all ops accepted' rather than a FAILED state.
    """
    a = _StubIndexer("a")
    writer = _RecordingWriter()
    ctx_with_conn = IndexContext(
        catalog="cat-x", collection="col-y",
        correlation_id="cid-1", pg_conn=object(),
    )
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_async_entry("a")],
        indexers={"a": a},
        outbox=writer,
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2"), _op(entity_id="i3")]
    results = await dispatcher.fan_out_bulk(ctx_with_conn, ops)

    assert "a" in results
    assert results["a"].succeeded == 3
    assert results["a"].failed == 0


@pytest.mark.asyncio
async def test_async_write_mode_logs_async_outbox_enqueued(caplog):
    """Dispatch-path log for an ASYNC entry must use mode=async_outbox_enqueued,
    not post_commit_inline.
    """
    import logging as _logging

    a = _StubIndexer("a")
    writer = _RecordingWriter()
    ctx_with_conn = IndexContext(
        catalog="cat-x", collection="col-y",
        correlation_id="cid-1", pg_conn=object(),
    )
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_async_entry("a")],
        indexers={"a": a},
        outbox=writer,
    )
    with caplog.at_level(_logging.INFO):
        await dispatcher.fan_out_bulk(ctx_with_conn, [_op(entity_id="i1")])

    rows = _extract_dispatch_path_records(caplog)
    assert any(r.get("mode") == "async_outbox_enqueued" for r in rows), rows
    assert not any(r.get("mode") == "post_commit_inline" for r in rows), (
        "ASYNC entry must not log post_commit_inline"
    )


@pytest.mark.asyncio
async def test_sync_write_mode_still_indexes_inline():
    """SYNC entries are unaffected by the ASYNC fix: they still call the
    indexer inline as before.
    """
    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(
        entries=[_entry("a", on_failure=FailurePolicy.WARN)],  # write_mode=SYNC
        indexers={"a": a},
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    results = await dispatcher.fan_out_bulk(_ctx(), ops)

    assert len(a.bulk_calls) == 1
    assert results["a"].succeeded == 2


@pytest.mark.asyncio
async def test_mixed_sync_and_async_entries_dispatch_correctly():
    """When one entry is SYNC and another is ASYNC, the SYNC one indexes
    inline and the ASYNC one enqueues without calling its indexer.
    """
    sync_indexer = _StubIndexer("sync-driver")
    async_indexer = _StubIndexer("async-driver")
    writer = _RecordingWriter()
    ctx_with_conn = IndexContext(
        catalog="cat-x", collection="col-y",
        correlation_id="cid-1", pg_conn=object(),
    )
    routing = _StubRouting([
        _entry("sync-driver", on_failure=FailurePolicy.WARN),  # SYNC
        _async_entry("async-driver"),                           # ASYNC
    ])

    async def routing_resolver(catalog, collection):
        return routing

    async def indexer_registry(indexer_id):
        return {"sync-driver": sync_indexer, "async-driver": async_indexer}.get(indexer_id)

    dispatcher = IndexDispatcher(
        routing_resolver=routing_resolver,
        indexer_registry=indexer_registry,
        outbox=writer,
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    results = await dispatcher.fan_out_bulk(ctx_with_conn, ops)

    # SYNC driver indexed inline.
    assert len(sync_indexer.bulk_calls) == 1
    assert results["sync-driver"].succeeded == 2

    # ASYNC driver NOT indexed inline; outbox got the ops.
    assert async_indexer.bulk_calls == [], "ASYNC driver must not be called inline"
    assert len(writer.rows) >= 1
    assert results["async-driver"].succeeded == 2


# ---------------------------------------------------------------------------
# write_mode=ASYNC — drop-path correctness (Refs #2438)
#
# _enqueue_or_warn has three silent-return paths that do NOT enqueue
# anything.  The ASYNC branch must convert each into failed=N rather than
# the false-success succeeded=N that _check_index_health reads to decide
# whether to mark an ingestion COMPLETED.
# ---------------------------------------------------------------------------


class _FailingWriter(TaskTableOutboxWriter):
    """Simulates a transient PG error during task-row insertion (drop path c)."""

    def __init__(self) -> None:
        super().__init__(task_schema_resolver=lambda: "tasks")

    async def _exec_insert(self, conn, sql, params):  # type: ignore[override]
        raise RuntimeError("simulated transient PG error")


@pytest.mark.asyncio
async def test_async_enqueue_drop_path_a_no_outbox_returns_failed():
    """Drop path (a): no OutboxWriter wired.
    The ASYNC branch must return BulkResult(succeeded=0, failed=N) so
    _check_index_health does NOT mark the ingestion COMPLETED.
    """
    a = _StubIndexer("a")
    # No outbox wired — _enqueue_or_warn returns 0.
    dispatcher = _make_dispatcher(
        entries=[_async_entry("a")],
        indexers={"a": a},
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    results = await dispatcher.fan_out_bulk(_ctx(), ops)

    assert a.bulk_calls == [], "ASYNC must never call the indexer inline"
    assert results["a"].succeeded == 0, (
        "No outbox wired: succeeded must be 0, not len(ops)"
    )
    assert results["a"].failed == 2


@pytest.mark.asyncio
async def test_async_enqueue_drop_path_b_pg_conn_none_returns_failed():
    """Drop path (b): ctx.pg_conn is None — TaskTableOutboxWriter.enqueue
    returns silently without enqueuing anything.  The ASYNC branch pre-checks
    and must return BulkResult(succeeded=0, failed=N).
    """
    a = _StubIndexer("a")
    writer = _RecordingWriter()
    # _ctx() has pg_conn=None — the known ASYNC silent-drop scenario.
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_async_entry("a")],
        indexers={"a": a},
        outbox=writer,
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2"), _op(entity_id="i3")]
    results = await dispatcher.fan_out_bulk(_ctx(), ops)

    assert a.bulk_calls == [], "ASYNC must never call the indexer inline"
    assert writer.rows == [], "No task rows should have been inserted"
    assert results["a"].succeeded == 0, (
        "pg_conn=None: succeeded must be 0, not len(ops)"
    )
    assert results["a"].failed == 3


@pytest.mark.asyncio
async def test_async_enqueue_drop_path_c_transient_pg_error_returns_failed():
    """Drop path (c): _exec_insert raises a transient PG error.
    The ASYNC branch must return BulkResult(succeeded=0, failed=N) rather
    than surfacing the exception to the ingestion task.
    """
    a = _StubIndexer("a")
    failing_writer = _FailingWriter()
    ctx_with_conn = IndexContext(
        catalog="cat-x", collection="col-y",
        correlation_id="cid-1", pg_conn=object(),
    )
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_async_entry("a")],
        indexers={"a": a},
        outbox=failing_writer,
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    results = await dispatcher.fan_out_bulk(ctx_with_conn, ops)

    assert a.bulk_calls == [], "ASYNC must never call the indexer inline"
    assert results["a"].succeeded == 0, (
        "Transient PG error: succeeded must be 0, not len(ops)"
    )
    assert results["a"].failed == 2
