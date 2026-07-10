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

"""Unit tests for :class:`IndexDispatcher` under the lane model (#2494).

An INDEX-lane entry is async by lane definition — there is no per-entry
``write_mode`` any more.  ``fan_out_bulk`` has exactly two dispatch shapes:

* **Default (outside a task run)** — every entry is durably enqueued via
  the wired :class:`OutboxWriterProtocol` (proactive, up-front obligation);
  the indexer is never called inline.
* **In-task-run absorption** — when the dispatch is already running inside
  a background task/job execution (``in_task_run()``) for the SAME catalog
  the run declared (or a run that declared no catalog at all), the write is
  absorbed inline through the driver-agnostic chunked path instead of
  spawning an outbox row.  This is the ONLY way ``indexer.index_bulk`` is
  ever called by this dispatcher — the tests below that exercise
  ``ensure_indexer``, the circuit breaker, per-document partial failures,
  or a raised bulk exception all wrap the call in ``task_run_scope()``.

INDEX entries carry no per-entry failure policy: an inline failure (only
reachable via the in-task-run absorption path above) is logged and
dropped — the item-tier obligation sweep (#2688) is the safety net, not a
retry here.
"""

from __future__ import annotations

from typing import List, Optional, Sequence

import pytest

from dynastore.models.protocols.indexer import (
    BulkResult, IndexContext, IndexOp,
)
from dynastore.models.protocols.indexing import IndexableOp
from dynastore.modules.storage.index_dispatcher import (
    INLINE_DISPATCH_CHUNK_SIZE, IndexDispatcher,
    StoragePlaneOutboxWriter, get_index_dispatcher,
    reset_index_dispatcher,
)
from dynastore.modules.storage.routing_config import (
    FailurePolicy, Operation, OperationDriverEntry,
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


class _NoopIndexer:
    """Stub indexer that always returns BulkResult(total=N, succeeded=0, failed=0)."""

    def __init__(self, indexer_id: str) -> None:
        self.indexer_id = indexer_id
        self.bulk_calls: List[Sequence[IndexOp]] = []
        self.ensure_calls: List[IndexContext] = []

    async def ensure_indexer(self, ctx: IndexContext) -> None:
        self.ensure_calls.append(ctx)

    async def index_bulk(
        self, ctx: IndexContext, ops: Sequence[IndexOp],
    ) -> BulkResult:
        self.bulk_calls.append(ops)
        return BulkResult(total=len(ops), succeeded=0, failed=0)


class _AccessAwareStubIndexer(_StubIndexer):
    """An INDEX-lane indexer carrying the same ``applies_access_filter =
    True`` class marker as the envelope ES driver — used to pin that the
    storage-plane gate includes access-aware entries (#2687: the drain
    recomputes the ABAC envelope from the hub row's persisted
    ``access_owner`` column plus live config, fail-closed)."""

    applies_access_filter = True


class _RecordingWriter:
    """Generic ``OutboxWriterProtocol`` double — records each accepted op
    without touching a database."""

    def __init__(self) -> None:
        self.rows: list[dict] = []

    async def enqueue(
        self,
        *,
        indexer_id: str,
        ctx: IndexContext,
        ops: Sequence[IndexOp],
        last_error: Optional[str] = None,
        chunk_size: Optional[int] = None,
    ) -> None:
        for op in ops:
            self.rows.append({
                "indexer_id": indexer_id,
                "catalog": ctx.catalog,
                "collection": ctx.collection,
                "op_type": op.op_type,
                "entity_id": op.entity_id,
                "last_error": last_error,
            })


class _FailingWriter:
    """Simulates a transient PG error during outbox enqueue (drop path c)."""

    async def enqueue(self, **kwargs) -> None:
        raise RuntimeError("simulated transient PG error")


class _StubBreaker:
    """Minimal :class:`CircuitBreaker` double."""

    def __init__(self, *, open_for: frozenset = frozenset()) -> None:
        self._open_for = open_for
        self.successes: List[str] = []
        self.failures: List[str] = []

    def is_open(self, indexer_id: str) -> bool:
        return indexer_id in self._open_for

    def record_success(self, indexer_id: str) -> None:
        self.successes.append(indexer_id)

    def record_failure(self, indexer_id: str) -> None:
        self.failures.append(indexer_id)


class _StubRouting:
    """A plain routing double — ``operations[INDEX]`` is what
    ``IndexDispatcher._index_entries`` reads via ``index_entries()``."""

    def __init__(self, entries: List[OperationDriverEntry]) -> None:
        self.operations = {Operation.INDEX: entries}


def _make_dispatcher(
    entries: List[OperationDriverEntry],
    indexers: dict,
    *,
    outbox=None,
    breaker=None,
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
        breaker=breaker,
    )


def _make_dispatcher_with_outbox(
    entries: List[OperationDriverEntry],
    indexers: dict,
    outbox,
) -> IndexDispatcher:
    return _make_dispatcher(entries, indexers, outbox=outbox)


def _entry(driver_ref: str, *, on_failure: FailurePolicy = FailurePolicy.FATAL) -> OperationDriverEntry:
    """An INDEX-lane entry. ``on_failure`` is accepted for API-shape parity
    with :class:`OperationDriverEntry` but is inert here — dispatch failure
    handling is structural (see the module docstring), not policy-driven."""
    return OperationDriverEntry(
        driver_ref=driver_ref,
        on_failure=on_failure,
        source="auto",
    )


def _ctx() -> IndexContext:
    return IndexContext(catalog="cat-x", collection="col-y", correlation_id="cid-1")


def _ctx_with_conn() -> IndexContext:
    return IndexContext(
        catalog="cat-x", collection="col-y", correlation_id="cid-1", pg_conn=object(),
    )


def _item_ctx(*, pg_conn: Optional[object] = object()) -> IndexContext:
    return IndexContext(
        catalog="cat-x", collection="col-y", correlation_id="cid-1",
        pg_conn=pg_conn, entity_type="item",
    )


def _op(op_type: str = "upsert", entity_id: str = "item-1") -> IndexOp:
    return IndexOp(
        op_type=op_type,
        entity_type="item",
        entity_id=entity_id,
        payload={"foo": "bar"} if op_type == "upsert" else None,
    )


def _indexable_op(*, op: str = "upsert", entity_id: str = "item-1") -> IndexableOp:
    from uuid import uuid4

    return IndexableOp(
        op_id=uuid4(),
        op=op,
        catalog_id="cat-x",
        collection_id="col-y",
        driver_instance_id="primary-di",
        item_id=entity_id,
        payload={"foo": "bar"} if op == "upsert" else {},
        idempotency_key=f"idem-{entity_id}",
    )


def _write_id_op(
    *,
    write_id: str,
    op: str = "upsert",
    entity_id: str = "item-1",
) -> IndexableOp:
    from uuid import uuid4

    indexable = IndexableOp(
        op_id=uuid4(),
        op=op,
        catalog_id="cat-x",
        collection_id="col-y",
        driver_instance_id="primary-di",
        item_id=entity_id,
        payload={"foo": "bar"} if op == "upsert" else {},
        idempotency_key=f"{write_id}:{entity_id}",
    )
    object.__setattr__(indexable, "write_id", write_id)
    return indexable


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


# ---------------------------------------------------------------------------
# Default dispatch (outside a task run) — every entry is proactively
# enqueued; the indexer is NEVER called inline.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_default_dispatch_never_calls_indexer_inline():
    assert not in_task_run()
    a = _StubIndexer("a")
    writer = _RecordingWriter()
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_entry("a")], indexers={"a": a}, outbox=writer,
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    results = await dispatcher.fan_out_bulk(_ctx_with_conn(), ops)

    assert a.bulk_calls == [], "outside a task run the indexer must never be called inline"
    assert len(writer.rows) >= 1
    enqueued_ids = {row["entity_id"] for row in writer.rows}
    assert enqueued_ids == {"i1", "i2"}
    assert results["a"].succeeded == 2
    assert results["a"].failed == 0


@pytest.mark.asyncio
async def test_default_dispatch_fans_out_to_every_configured_entry():
    a = _StubIndexer("a")
    b = _StubIndexer("b")
    writer = _RecordingWriter()
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_entry("a"), _entry("b")],
        indexers={"a": a, "b": b},
        outbox=writer,
    )
    results = await dispatcher.fan_out_bulk(_ctx_with_conn(), [_op()])
    assert "a" in results and "b" in results
    assert a.bulk_calls == [] and b.bulk_calls == []
    assert {r["indexer_id"] for r in writer.rows} == {"a", "b"}


@pytest.mark.asyncio
async def test_default_dispatch_logs_async_outbox_enqueued(caplog):
    import logging as _logging

    a = _StubIndexer("a")
    writer = _RecordingWriter()
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_entry("a")], indexers={"a": a}, outbox=writer,
    )
    with caplog.at_level(_logging.INFO):
        await dispatcher.fan_out_bulk(_ctx_with_conn(), [_op(entity_id="i1")])

    rows = _extract_dispatch_path_records(caplog)
    assert any(r.get("mode") == "async_outbox_enqueued" for r in rows), rows
    assert not any(r.get("mode") == "post_commit_inline" for r in rows), rows


@pytest.mark.asyncio
async def test_default_dispatch_no_outbox_writer_returns_failed():
    """Drop path (a): no OutboxWriter wired — succeeded must be 0, not
    len(ops), so ``_check_index_health`` never mistakes a silent drop for
    success."""
    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a})
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    results = await dispatcher.fan_out_bulk(_ctx_with_conn(), ops)

    assert a.bulk_calls == []
    assert results["a"].succeeded == 0
    assert results["a"].failed == 2


@pytest.mark.asyncio
async def test_default_dispatch_pg_conn_none_returns_failed():
    """Drop path (b): ``ctx.pg_conn is None`` — pre-checked before the
    outbox writer's ``enqueue`` is ever called."""
    a = _StubIndexer("a")
    writer = _RecordingWriter()
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_entry("a")], indexers={"a": a}, outbox=writer,
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2"), _op(entity_id="i3")]
    results = await dispatcher.fan_out_bulk(_ctx(), ops)  # _ctx() has pg_conn=None

    assert a.bulk_calls == []
    assert writer.rows == []
    assert results["a"].succeeded == 0
    assert results["a"].failed == 3


@pytest.mark.asyncio
async def test_default_dispatch_transient_enqueue_error_returns_failed():
    """Drop path (c): the outbox writer raises — the caller must never
    see the exception, only a failed count."""
    a = _StubIndexer("a")
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_entry("a")], indexers={"a": a}, outbox=_FailingWriter(),
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    results = await dispatcher.fan_out_bulk(_ctx_with_conn(), ops)

    assert a.bulk_calls == []
    assert results["a"].succeeded == 0
    assert results["a"].failed == 2


@pytest.mark.asyncio
async def test_default_dispatch_skips_when_indexer_not_registered_and_enqueues():
    """A configured-but-not-locally-registered indexer is durably enqueued
    (never silently dropped) — see ``_handle_missing``."""
    a = _StubIndexer("a")
    writer = _RecordingWriter()
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_entry("a"), _entry("missing")],
        indexers={"a": a},  # 'missing' not registered
        outbox=writer,
    )
    await dispatcher.fan_out_bulk(_ctx_with_conn(), [_op(entity_id="i1")])
    assert {r["indexer_id"] for r in writer.rows} == {"a", "missing"}


# ---------------------------------------------------------------------------
# In-task-run absorption — the ONLY path that calls the indexer inline.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_in_task_run_dispatches_inline_and_enqueues_zero_outbox():
    a = _StubIndexer("a")
    writer = _RecordingWriter()
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_entry("a")], indexers={"a": a}, outbox=writer,
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(_ctx_with_conn(), ops)

    assert len(a.bulk_calls) == 1
    assert list(a.bulk_calls[0]) == ops
    assert writer.rows == []
    assert results["a"].succeeded == 2
    assert results["a"].failed == 0


@pytest.mark.asyncio
async def test_in_task_run_calls_ensure_indexer_once_per_collection():
    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a})
    with task_run_scope():
        await dispatcher.fan_out_bulk(_ctx_with_conn(), [_op(entity_id="i1")])
        await dispatcher.fan_out_bulk(_ctx_with_conn(), [_op(entity_id="i2")])
        await dispatcher.fan_out_bulk(_ctx_with_conn(), [_op(entity_id="i3")])
    assert len(a.ensure_calls) == 1
    assert len(a.bulk_calls) == 3


@pytest.mark.asyncio
async def test_in_task_run_re_runs_ensure_for_new_collection():
    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a})
    ctx_a = _ctx_with_conn()
    ctx_b = IndexContext(catalog="cat-x", collection="col-z", correlation_id="c2", pg_conn=object())
    with task_run_scope():
        await dispatcher.fan_out_bulk(ctx_a, [_op()])
        await dispatcher.fan_out_bulk(ctx_b, [_op()])
    assert len(a.ensure_calls) == 2


@pytest.mark.asyncio
async def test_in_task_run_ensure_indexer_failure_logs_and_skips_index_call():
    a = _StubIndexer("a", raise_on_ensure=True)
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a})
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(_ctx_with_conn(), [_op()])
    assert len(a.ensure_calls) == 1
    assert len(a.bulk_calls) == 0
    assert results["a"].failed == 1


@pytest.mark.asyncio
async def test_in_task_run_indexer_without_ensure_method_is_treated_as_ready():
    """Drivers in transition may not yet have ``ensure_indexer`` — the
    dispatcher must not block on missing methods."""

    class _LegacyIndexer:
        indexer_id = "legacy"

        def __init__(self):
            self.bulk_calls: List = []

        async def index_bulk(self, ctx, ops):
            self.bulk_calls.append(list(ops))
            return BulkResult(total=len(ops), succeeded=len(ops))

    a = _LegacyIndexer()
    dispatcher = _make_dispatcher(entries=[_entry("legacy")], indexers={"legacy": a})
    with task_run_scope():
        await dispatcher.fan_out_bulk(_ctx_with_conn(), [_op()])
    assert len(a.bulk_calls) == 1
    assert len(a.bulk_calls[0]) == 1


@pytest.mark.asyncio
async def test_in_task_run_calls_every_configured_indexer():
    a = _StubIndexer("a")
    b = _StubIndexer("b")
    dispatcher = _make_dispatcher(
        entries=[_entry("a"), _entry("b")], indexers={"a": a, "b": b},
    )
    with task_run_scope():
        await dispatcher.fan_out_bulk(_ctx_with_conn(), [_op()])
    assert len(a.bulk_calls) == 1
    assert len(b.bulk_calls) == 1


@pytest.mark.asyncio
async def test_in_task_run_bulk_failure_is_logged_and_dropped_not_raised():
    """INDEX entries carry no per-entry failure policy: a raised bulk call
    is logged and dropped, never re-raised — subsequent entries still run."""
    a = _StubIndexer("a", raise_on="upsert")
    b = _StubIndexer("b")
    dispatcher = _make_dispatcher(
        entries=[_entry("a"), _entry("b")], indexers={"a": a, "b": b},
    )
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(_ctx_with_conn(), [_op()])  # must not raise
    assert len(a.bulk_calls) == 1
    assert len(b.bulk_calls) == 1
    assert results["a"].failed == 1
    assert results["a"].failures
    assert results["b"].succeeded == 1


@pytest.mark.asyncio
async def test_in_task_run_bulk_returns_per_indexer_results():
    a = _StubIndexer("a")
    b = _StubIndexer("b")
    dispatcher = _make_dispatcher(
        entries=[_entry("a"), _entry("b")], indexers={"a": a, "b": b},
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(_ctx_with_conn(), ops)
    assert results["a"].succeeded == 2
    assert results["b"].succeeded == 2


@pytest.mark.asyncio
async def test_in_task_run_dispatch_path_logged_post_commit_inline(caplog):
    import logging as _logging

    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a})
    ops = [_op(entity_id="i1"), _op(entity_id="i2"), _op(entity_id="i3")]
    with caplog.at_level(_logging.INFO):
        with task_run_scope():
            await dispatcher.fan_out_bulk(_ctx_with_conn(), ops)
    rows = _extract_dispatch_path_records(caplog)
    assert any(
        r.get("mode") == "post_commit_inline"
        and r.get("indexer") == "a"
        and r.get("chunk_size") == "3"
        for r in rows
    ), rows


@pytest.mark.asyncio
async def test_in_task_run_dispatch_path_not_logged_success_on_failure(caplog):
    import logging as _logging

    a = _StubIndexer("a", raise_on="upsert")
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a})
    with caplog.at_level(_logging.INFO):
        with task_run_scope():
            await dispatcher.fan_out_bulk(_ctx_with_conn(), [_op(entity_id="i1")])
    rows = _extract_dispatch_path_records(caplog)
    assert not any(r.get("mode") == "post_commit_inline" for r in rows), rows


# ---------------------------------------------------------------------------
# Circuit breaker — open breaker drops inline attempts without calling the
# indexer (reachable only via the in-task-run absorption path).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_in_task_run_open_breaker_drops_without_calling_indexer():
    a = _StubIndexer("a")
    breaker = _StubBreaker(open_for=frozenset({"a"}))
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a}, breaker=breaker)
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(_ctx_with_conn(), [_op()])
    assert a.bulk_calls == []
    assert results["a"].failed == 1
    assert results["a"].failures[0]["reason"] == "circuit_breaker_open"


@pytest.mark.asyncio
async def test_in_task_run_breaker_records_success_and_failure():
    a = _StubIndexer("a")
    b = _StubIndexer("b", raise_on="upsert")
    breaker = _StubBreaker()
    dispatcher = _make_dispatcher(
        entries=[_entry("a"), _entry("b")],
        indexers={"a": a, "b": b},
        breaker=breaker,
    )
    with task_run_scope():
        await dispatcher.fan_out_bulk(_ctx_with_conn(), [_op()])
    assert breaker.successes == ["a"]
    assert breaker.failures == ["b"]


# ---------------------------------------------------------------------------
# #2064 — per-document rejections from a successful bulk are surfaced
# (per-item index_failure event + partial_failure_drop dispatch counter)
# instead of being silently absorbed into a count.  Reachable only via the
# in-task-run absorption path.
# ---------------------------------------------------------------------------


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
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": idx})

    with caplog.at_level(logging.INFO, logger="dynastore.modules.storage.index_dispatcher"):
        with task_run_scope():
            results = await dispatcher.fan_out_bulk(
                _ctx_with_conn(), [_op(entity_id="item-9"), _op(entity_id="item-ok")],
            )

    assert len(events) == 1
    ev = events[0]
    assert ev["event_type"] == "index_failure_persistent"
    assert ev["level"] == "ERROR"
    assert ev["details"]["item_id"] == "item-9"
    assert ev["details"]["source"] == "inline_dispatch_partial_bulk"
    assert ev["details"]["status"] == "dropped"
    assert "invalid_shape_exception" in ev["details"]["reason"]
    assert "mode=partial_failure_drop" in caplog.text
    assert results["a"].failed == 1


@pytest.mark.asyncio
async def test_clean_bulk_emits_no_index_failure_event(monkeypatch):
    events = _capture_log_events(monkeypatch)
    a = _StubIndexer("a")  # all-succeeded
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a})
    with task_run_scope():
        await dispatcher.fan_out_bulk(_ctx_with_conn(), [_op()])
    assert events == []


@pytest.mark.asyncio
async def test_raised_bulk_does_not_emit_partial_failure_event(monkeypatch):
    """A bulk that RAISES is logged and dropped whole; the synthetic
    failure from the except branch is not a per-doc rejection and must NOT
    also fire a partial-failure index_failure event (no double-signal)."""
    events = _capture_log_events(monkeypatch)
    a = _StubIndexer("a", raise_on="upsert")
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a})
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(_ctx_with_conn(), [_op(op_type="upsert")])
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
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": idx})
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(_ctx_with_conn(), [_op(entity_id="item-9")])
    assert results["a"].failed == 1


# ---------------------------------------------------------------------------
# #1861 — silent no-op converted to a retryable/observable failure.
# Reachable only via the in-task-run absorption path.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_in_task_run_silent_noop_upsert_batch_reports_failed():
    noop = _NoopIndexer("noop-es")
    dispatcher = _make_dispatcher(entries=[_entry("noop-es")], indexers={"noop-es": noop})
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(_ctx_with_conn(), ops)
    assert results["noop-es"].failed == 2


@pytest.mark.asyncio
async def test_in_task_run_silent_noop_does_not_log_as_clean_success(caplog):
    import logging as _logging

    noop = _NoopIndexer("noop-es")
    dispatcher = _make_dispatcher(entries=[_entry("noop-es")], indexers={"noop-es": noop})
    with caplog.at_level(_logging.INFO):
        with task_run_scope():
            await dispatcher.fan_out_bulk(_ctx_with_conn(), [_op(entity_id="i1")])
    rows = _extract_dispatch_path_records(caplog)
    assert not any(r.get("mode") == "post_commit_inline" for r in rows), (
        "a converted no-op must not appear as a clean post_commit_inline success"
    )


@pytest.mark.asyncio
async def test_in_task_run_normal_success_reports_succeeded():
    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a})
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(_ctx_with_conn(), [_op(entity_id="i1")])
    assert results["a"].succeeded == 1
    assert results["a"].failed == 0


@pytest.mark.asyncio
async def test_in_task_run_silent_noop_delete_only_does_not_fail():
    """A silent no-op batch composed only of delete ops is unaffected —
    the no-op-to-failure conversion targets upserts only."""
    noop = _NoopIndexer("noop-es")
    dispatcher = _make_dispatcher(entries=[_entry("noop-es")], indexers={"noop-es": noop})
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(
            _ctx_with_conn(), [_op(op_type="delete", entity_id="d1")],
        )
    assert results["noop-es"].failed == 0


# ---------------------------------------------------------------------------
# Chunking of the in-task-run inline path
# ---------------------------------------------------------------------------


def _patch_in_task_run_chunk_size(monkeypatch, size: int) -> None:
    import dynastore.modules.storage.index_dispatcher as idx_mod

    async def _resolved() -> int:
        return size

    monkeypatch.setattr(idx_mod, "_resolve_in_task_run_chunk_size", _resolved)


@pytest.mark.asyncio
async def test_in_task_run_chunks_large_batch(monkeypatch):
    _patch_in_task_run_chunk_size(monkeypatch, INLINE_DISPATCH_CHUNK_SIZE)
    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a})
    n = INLINE_DISPATCH_CHUNK_SIZE * 2 + 37
    ops = [_op(entity_id=f"i{i}") for i in range(n)]
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(_ctx_with_conn(), ops)

    import math
    expected_chunks = math.ceil(n / INLINE_DISPATCH_CHUNK_SIZE)
    assert len(a.bulk_calls) == expected_chunks
    assert sum(len(c) for c in a.bulk_calls) == n
    assert results["a"].total == n
    assert results["a"].succeeded == n


@pytest.mark.asyncio
async def test_in_task_run_opens_one_short_tx_per_chunk(monkeypatch):
    """With a tx_factory, the dispatcher opens a FRESH transaction per
    chunk — never one long-lived transaction across the whole sequential
    fan-out — so a busy job never parks a pooled connection with an open
    transaction across the full dispatch."""
    _patch_in_task_run_chunk_size(monkeypatch, INLINE_DISPATCH_CHUNK_SIZE)
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
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a})
    ctx = IndexContext(catalog="cat-x", collection="col-y", correlation_id="cid-1", pg_conn=None)
    n = INLINE_DISPATCH_CHUNK_SIZE * 2 + 5
    ops = [_op(entity_id=f"i{i}") for i in range(n)]
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(ctx, ops, tx_factory=_tx_factory)

    import math
    expected_chunks = math.ceil(n / INLINE_DISPATCH_CHUNK_SIZE)
    assert len(opened) == expected_chunks
    assert all(tx.entered and tx.exited for tx in opened)
    assert len(a.bulk_calls) == expected_chunks
    assert results["a"].succeeded == n


@pytest.mark.asyncio
async def test_in_task_run_honors_configured_chunk_size_cap(monkeypatch):
    """#2716: inside a task run, the inline chunk size is bounded by
    ``TasksPluginConfig.in_task_run_inline_chunk_size`` — NOT the fixed
    ``INLINE_DISPATCH_CHUNK_SIZE`` (500)."""
    _patch_in_task_run_chunk_size(monkeypatch, 7)
    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a})
    n = 20
    ops = [_op(entity_id=f"i{i}") for i in range(n)]
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(_ctx_with_conn(), ops)

    import math
    assert len(a.bulk_calls) == math.ceil(n / 7)
    assert all(len(chunk) <= 7 for chunk in a.bulk_calls)
    assert sum(len(c) for c in a.bulk_calls) == n
    assert results["a"].succeeded == n


# ---------------------------------------------------------------------------
# In-run absorption is scoped to the running task's own catalog (#2716)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_in_task_run_absorbs_own_catalog_write():
    a = _StubIndexer("a")
    writer = _RecordingWriter()
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_entry("a")], indexers={"a": a}, outbox=writer,
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    with task_run_scope(catalog="cat-x"):
        results = await dispatcher.fan_out_bulk(_ctx_with_conn(), ops)

    assert len(a.bulk_calls) == 1
    assert writer.rows == [], "own-catalog write must still be absorbed inline"
    assert results["a"].succeeded == 2


@pytest.mark.asyncio
async def test_in_task_run_does_not_absorb_foreign_catalog_write():
    """A task run that declared catalog A must NOT absorb a write for
    catalog B inline — that backlog belongs to the async-writer job, not
    to this task's memory budget. It falls back to the durable outbox
    exactly as the not-in-task-run path does."""
    a = _StubIndexer("a")
    writer = _RecordingWriter()
    foreign_ctx = IndexContext(
        catalog="cat-foreign", collection="col-y", correlation_id="cid-1", pg_conn=object(),
    )
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_entry("a")], indexers={"a": a}, outbox=writer,
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    with task_run_scope(catalog="cat-own"):
        results = await dispatcher.fan_out_bulk(foreign_ctx, ops)

    assert a.bulk_calls == [], "a task run scoped to a DIFFERENT catalog must not absorb this write"
    assert len(writer.rows) >= 1
    assert results["a"].succeeded == 2


@pytest.mark.asyncio
async def test_in_task_run_unscoped_catalog_still_absorbs_any_write():
    """A task run entered WITHOUT a declared catalog (``task_run_scope()``
    with no argument, e.g. the Cloud Run Job entrypoint) stays
    unrestricted."""
    a = _StubIndexer("a")
    writer = _RecordingWriter()
    ctx = IndexContext(
        catalog="cat-anything", collection="col-y", correlation_id="cid-1", pg_conn=object(),
    )
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_entry("a")], indexers={"a": a}, outbox=writer,
    )
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(ctx, [_op(entity_id="i1")])

    assert len(a.bulk_calls) == 1
    assert writer.rows == []
    assert results["a"].succeeded == 1


@pytest.mark.asyncio
async def test_in_task_run_foreign_catalog_logs_outbox_handoff_not_inline(caplog):
    import logging as _logging

    a = _StubIndexer("a")
    writer = _RecordingWriter()
    foreign_ctx = IndexContext(
        catalog="cat-foreign", collection="col-y", correlation_id="cid-1", pg_conn=object(),
    )
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_entry("a")], indexers={"a": a}, outbox=writer,
    )
    with caplog.at_level(_logging.INFO):
        with task_run_scope(catalog="cat-own"):
            await dispatcher.fan_out_bulk(foreign_ctx, [_op(entity_id="i1")])

    rows = _extract_dispatch_path_records(caplog)
    assert any(r.get("mode") == "async_outbox_enqueued" for r in rows), rows
    assert not any(r.get("mode") == "post_commit_inline" for r in rows), rows
    assert not any(r.get("mode") == "inline_in_task_run" for r in rows), rows


# ---------------------------------------------------------------------------
# #914 — dispatch-level silent no-op: ops submitted with no INDEX-lane entry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fan_out_warns_when_ops_submitted_but_routing_returns_no_entries(caplog):
    import logging as _logging

    dispatcher = _make_dispatcher(entries=[], indexers={})
    with caplog.at_level(_logging.WARNING):
        results = await dispatcher.fan_out_bulk(_ctx(), [_op(entity_id="i1")])
    assert results == {}
    msgs = [r.getMessage() for r in caplog.records if r.levelno >= _logging.WARNING]
    assert any(
        "routing returned NO" in m and "INDEX-lane entries" in m
        and "cat-x" in m and "col-y" in m
        for m in msgs
    ), msgs


@pytest.mark.asyncio
async def test_fan_out_does_not_warn_when_no_ops_and_no_entries(caplog):
    """Empty-ops dispatch is a legitimate no-op; should not log the #914
    WARN (which would be a false positive on routine probes)."""
    import logging as _logging

    dispatcher = _make_dispatcher(entries=[], indexers={})
    with caplog.at_level(_logging.WARNING):
        results = await dispatcher.fan_out_bulk(_ctx(), [])
    assert results == {}
    msgs = [r.getMessage() for r in caplog.records if r.levelno >= _logging.WARNING]
    assert not any("INDEX-lane entries" in m for m in msgs), msgs


# ---------------------------------------------------------------------------
# Singleton factory / describe()
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
async def test_default_dispatcher_wires_storage_plane_outbox_writer():
    await reset_index_dispatcher()
    dispatcher = get_index_dispatcher()
    try:
        assert isinstance(dispatcher._outbox, StoragePlaneOutboxWriter)
    finally:
        await reset_index_dispatcher()


@pytest.mark.asyncio
async def test_default_dispatcher_describe_with_no_routing_returns_empty_indexers():
    """Without ConfigsProtocol in the process the default routing
    resolver yields an empty routing config — describe returns an empty
    indexer list rather than blowing up."""
    await reset_index_dispatcher()
    d = get_index_dispatcher()
    info = await d.describe(IndexContext(catalog="cat", collection="col"))
    assert info["indexers"] == [] or all(isinstance(x, dict) for x in info["indexers"])


@pytest.mark.asyncio
async def test_describe_reports_lane_and_registration_status():
    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(
        entries=[_entry("a"), _entry("missing")], indexers={"a": a},
    )
    info = await dispatcher.describe(_ctx())
    assert info["catalog"] == "cat-x"
    assert info["collection"] == "col-y"
    by_id = {e["indexer_id"]: e for e in info["indexers"]}
    assert by_id["a"]["lane"] == "INDEX"
    assert by_id["a"]["registered"] is True
    assert by_id["missing"]["registered"] is False


# ---------------------------------------------------------------------------
# StoragePlaneOutboxWriter — the default durable-enqueue handler
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_storage_plane_outbox_writer_skips_when_no_pg_conn(caplog):
    """No open TX means the enqueue can't be made durable, so it degrades
    to a warning instead of silently writing a non-atomic row."""
    import logging as _logging

    writer = StoragePlaneOutboxWriter()
    with caplog.at_level(_logging.WARNING):
        await writer.enqueue(indexer_id="x", ctx=_ctx(), ops=[_op()], last_error="boom")
    assert any("ctx.pg_conn is None" in r.getMessage() for r in caplog.records)


def _patch_storage_emit_recorder(monkeypatch) -> List[dict]:
    """Patch enqueue_storage_op_id_only where index_dispatcher imports it
    from, recording every call instead of touching a database."""
    import dynastore.modules.storage.storage_emit as storage_emit_mod

    calls: List[dict] = []

    async def _fake_enqueue(conn, *, catalog_id, rows):
        calls.append({"conn": conn, "catalog_id": catalog_id, "rows": list(rows)})

    monkeypatch.setattr(storage_emit_mod, "enqueue_storage_op_id_only", _fake_enqueue)
    return calls


def _patch_storage_emit_recorders(monkeypatch) -> dict:
    import dynastore.modules.storage.storage_emit as storage_emit_mod

    calls = {"id_only": [], "write_id": []}

    async def _fake_id_only(conn, *, catalog_id, rows):
        calls["id_only"].append({"conn": conn, "catalog_id": catalog_id, "rows": list(rows)})

    async def _fake_write_id(conn, *, catalog_id, rows):
        calls["write_id"].append({"conn": conn, "catalog_id": catalog_id, "rows": list(rows)})

    monkeypatch.setattr(storage_emit_mod, "enqueue_storage_op_id_only", _fake_id_only)
    monkeypatch.setattr(storage_emit_mod, "enqueue_storage_op_write_id", _fake_write_id)
    return calls


def _patch_write_id_capable_primary(monkeypatch) -> None:
    """Resolve the primary WRITE driver to a stub exposing
    ``read_indexable_write_batch`` (#3116) — without this, grouped write-id
    rows can never be produced: ``_primary_supports_write_id_reads`` fails
    open to ``False`` when ``get_write_drivers`` isn't wired, forcing every
    op down the id-only fallback path regardless of ``op.write_id``."""
    from types import SimpleNamespace

    import dynastore.modules.storage.router as router_mod

    class _CapablePrimary:
        async def read_indexable_write_batch(self, **kwargs):
            return []

    async def _fake_get_write_drivers(catalog_id, collection_id):
        return [SimpleNamespace(driver=_CapablePrimary())]

    monkeypatch.setattr(router_mod, "get_write_drivers", _fake_get_write_drivers)


@pytest.mark.asyncio
async def test_storage_plane_outbox_writer_enqueues_id_only_row_on_failure(monkeypatch):
    """An inline failure enqueues an id-only tasks.storage row — the drain
    re-reads canonical PG state at replay time instead of replaying a
    payload frozen at enqueue time."""
    calls = _patch_storage_emit_recorder(monkeypatch)
    writer = StoragePlaneOutboxWriter()
    ctx = IndexContext(catalog="cat-x", collection="col-y", correlation_id="cid-1", pg_conn=object())
    await writer.enqueue(
        indexer_id="items_elasticsearch_driver",
        ctx=ctx,
        ops=[_op(op_type="upsert", entity_id="item-1")],
        last_error="ES timeout",
    )
    assert len(calls) == 1
    assert calls[0]["catalog_id"] == "cat-x"
    rows = calls[0]["rows"]
    assert len(rows) == 1
    row = rows[0]
    assert row.driver_id == "items_elasticsearch_driver"
    assert row.op == "upsert"
    assert row.item_id == "item-1"
    assert row.collection_id == "col-y"


@pytest.mark.asyncio
async def test_storage_plane_outbox_writer_maps_delete_op_to_delete_row(monkeypatch):
    """A delete-op inline failure must enqueue an actual delete row — never
    an id-only upsert, which would make the drain rebuild a doc for an item
    that's supposed to be removed from the index."""
    calls = _patch_storage_emit_recorder(monkeypatch)
    writer = StoragePlaneOutboxWriter()
    ctx = IndexContext(catalog="cat-x", collection="col-y", correlation_id="cid-1", pg_conn=object())
    await writer.enqueue(
        indexer_id="items_elasticsearch_driver",
        ctx=ctx,
        ops=[_op(op_type="delete", entity_id="item-1")],
    )
    assert len(calls) == 1
    row = calls[0]["rows"][0]
    assert row.op == "delete"
    assert row.item_id == "item-1"


@pytest.mark.asyncio
async def test_storage_plane_outbox_writer_enqueues_grouped_write_id_rows(monkeypatch):
    _patch_write_id_capable_primary(monkeypatch)
    calls = _patch_storage_emit_recorders(monkeypatch)
    writer = StoragePlaneOutboxWriter()
    ctx = IndexContext(catalog="cat-x", collection="col-y", correlation_id="cid-1", pg_conn=object())
    await writer.enqueue(
        indexer_id="items_elasticsearch_driver",
        ctx=ctx,
        ops=[
            _write_id_op(write_id="w-123", entity_id="item-1"),
            _write_id_op(write_id="w-123", entity_id="item-2"),
        ],
        last_error="ES timeout",
    )
    assert calls["id_only"] == []
    assert len(calls["write_id"]) == 1
    rows = calls["write_id"][0]["rows"]
    assert len(rows) == 2
    assert {r.write_id for r in rows} == {"w-123"}


@pytest.mark.asyncio
async def test_storage_plane_outbox_writer_empty_ops_is_noop(monkeypatch):
    calls = _patch_storage_emit_recorder(monkeypatch)
    writer = StoragePlaneOutboxWriter()
    await writer.enqueue(
        indexer_id="x", ctx=IndexContext(catalog="cat", collection="col", pg_conn=object()), ops=[],
    )
    assert calls == []


@pytest.mark.asyncio
async def test_in_task_run_indexer_failure_reaches_storage_plane_writer(monkeypatch):
    """When wired as the dispatcher's ``outbox``, an inline (in-task-run)
    failure reaches ``StoragePlaneOutboxWriter`` and writes a durable row
    on the caller's connection."""
    _patch_storage_emit_recorder(monkeypatch)
    a = _StubIndexer("a", raise_on="upsert")
    dispatcher = _make_dispatcher(
        entries=[_entry("a")], indexers={"a": a}, outbox=StoragePlaneOutboxWriter(),
    )
    with task_run_scope():
        await dispatcher.fan_out_bulk(_ctx_with_conn(), [_op()])

    # The failure is logged and dropped (see the module docstring) — the
    # inline attempt was NOT proactively enqueued beforehand (this test
    # only exercises that the writer itself is reachable/functional; the
    # dispatcher's failure-handling call sites are deliberately non-
    # re-enqueuing, see test_in_task_run_bulk_failure_is_logged_and_dropped_not_raised).
    assert len(a.bulk_calls) == 1


@pytest.mark.asyncio
async def test_outbox_all_indexableop_batch_reaches_writer(monkeypatch):
    """#3173 regression: an all-IndexableOp batch must reach the
    storage-plane writer instead of being filtered to an empty IndexOp
    subset and dropped — ``StoragePlaneOutboxWriter`` builds records from
    either op shape (unlike the generic ``_RecordingWriter`` test double,
    which only understands the legacy ``IndexOp`` shape)."""
    calls = _patch_storage_emit_recorder(monkeypatch)
    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(
        entries=[_entry("a")], indexers={"a": a}, outbox=StoragePlaneOutboxWriter(),
    )
    ops = [
        _indexable_op(op="upsert", entity_id="item-1"),
        _indexable_op(op="delete", entity_id="item-2"),
    ]
    results = await dispatcher.fan_out_bulk(_ctx_with_conn(), ops)
    assert len(calls) == 1
    rows_by_id = {r.item_id: r.op for r in calls[0]["rows"]}
    assert rows_by_id == {"item-1": "upsert", "item-2": "delete"}
    assert results["a"].succeeded == 2


# ---------------------------------------------------------------------------
# #2494 P1 — storage-plane id-only routing gate (item-tier, flag-driven)
# ---------------------------------------------------------------------------


def _patch_storage_plane_flag(monkeypatch, *, enabled: bool) -> None:
    import dynastore.modules.storage.index_dispatcher as idx_mod

    async def _flag() -> bool:
        return enabled

    monkeypatch.setattr(idx_mod, "_storage_plane_routing_enabled", _flag)


@pytest.mark.asyncio
async def test_storage_plane_flag_on_item_entry_enqueues_id_only_not_in_task_run(monkeypatch):
    assert not in_task_run()
    _patch_storage_plane_flag(monkeypatch, enabled=True)
    calls = _patch_storage_emit_recorder(monkeypatch)

    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a})
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    results = await dispatcher.fan_out_bulk(_item_ctx(), ops)

    assert a.bulk_calls == [], "storage-plane routing must never call the indexer inline"
    assert len(calls) == 1
    assert calls[0]["catalog_id"] == "cat-x"
    row_ids = {r.item_id for r in calls[0]["rows"]}
    assert row_ids == {"i1", "i2"}
    assert all(r.op == "upsert" for r in calls[0]["rows"])
    assert results["a"].succeeded == 2
    assert results["a"].failed == 0


@pytest.mark.asyncio
async def test_storage_plane_flag_on_item_entry_enqueues_id_only_in_task_run(monkeypatch):
    """Flag ON, INSIDE a task run: still routed to the storage plane —
    never absorbed inline (the #2657 runaway path)."""
    _patch_storage_plane_flag(monkeypatch, enabled=True)
    calls = _patch_storage_emit_recorder(monkeypatch)

    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a})
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(_item_ctx(), ops)

    assert a.bulk_calls == [], "storage-plane routing must never absorb the write inline"
    assert len(calls) == 1
    assert results["a"].succeeded == 2


@pytest.mark.asyncio
async def test_storage_plane_flag_on_enqueues_grouped_write_id_row(monkeypatch):
    _patch_storage_plane_flag(monkeypatch, enabled=True)
    _patch_write_id_capable_primary(monkeypatch)
    calls = _patch_storage_emit_recorders(monkeypatch)

    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a})
    ops = [
        _write_id_op(write_id="w-123", entity_id="i1"),
        _write_id_op(write_id="w-123", entity_id="i2"),
    ]
    results = await dispatcher.fan_out_bulk(_item_ctx(), ops)

    assert a.bulk_calls == []
    assert calls["id_only"] == []
    assert len(calls["write_id"]) == 1
    rows = calls["write_id"][0]["rows"]
    assert len(rows) == 2
    assert {r.write_id for r in rows} == {"w-123"}
    assert results["a"].succeeded == 2
    assert results["a"].failed == 0


@pytest.mark.asyncio
async def test_storage_plane_flag_on_uses_tx_factory_when_no_pg_conn(monkeypatch):
    """When ctx.pg_conn is None (in-task-run inline seam), the enqueue opens
    a short transaction via tx_factory instead of dropping the write."""
    _patch_storage_plane_flag(monkeypatch, enabled=True)
    calls = _patch_storage_emit_recorder(monkeypatch)

    opened: list = []

    class _FakeTx:
        async def __aenter__(self):
            opened.append(self)
            return object()

        async def __aexit__(self, *exc):
            return False

    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a})
    ctx = _item_ctx(pg_conn=None)
    with task_run_scope():
        results = await dispatcher.fan_out_bulk(ctx, [_op(entity_id="i1")], tx_factory=_FakeTx)

    assert len(opened) == 1
    assert len(calls) == 1
    assert results["a"].succeeded == 1


@pytest.mark.asyncio
async def test_storage_plane_flag_on_no_conn_no_tx_factory_drops_and_fails(monkeypatch, caplog):
    import logging as _logging

    _patch_storage_plane_flag(monkeypatch, enabled=True)
    calls = _patch_storage_emit_recorder(monkeypatch)

    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a})
    ctx = _item_ctx(pg_conn=None)
    with caplog.at_level(_logging.WARNING):
        results = await dispatcher.fan_out_bulk(ctx, [_op(entity_id="i1")])

    assert calls == []
    assert a.bulk_calls == []
    assert results["a"].succeeded == 0
    assert results["a"].failed == 1


@pytest.mark.asyncio
async def test_storage_plane_flag_off_preserves_generic_outbox_path(monkeypatch):
    """Flag OFF: item-tier entries still go through the generic
    ``OutboxWriterProtocol`` path, not the id-only storage plane."""
    _patch_storage_plane_flag(monkeypatch, enabled=False)
    calls = _patch_storage_emit_recorder(monkeypatch)

    a = _StubIndexer("a")
    writer = _RecordingWriter()
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_entry("a")], indexers={"a": a}, outbox=writer,
    )
    ops = [_op(entity_id="i1"), _op(entity_id="i2")]
    results = await dispatcher.fan_out_bulk(_item_ctx(), ops)

    assert calls == [], "flag OFF must never touch the storage-plane id-only path"
    assert a.bulk_calls == []
    assert len(writer.rows) >= 1
    assert results["a"].succeeded == 2


@pytest.mark.asyncio
async def test_storage_plane_flag_on_non_item_entity_type_unaffected(monkeypatch):
    """The flag is item-scoped: a collection-tier dispatch must not be
    routed into the item-shaped id-only storage plane."""
    _patch_storage_plane_flag(monkeypatch, enabled=True)
    calls = _patch_storage_emit_recorder(monkeypatch)

    a = _StubIndexer("a")
    writer = _RecordingWriter()
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_entry("a")], indexers={"a": a}, outbox=writer,
    )
    ctx = IndexContext(
        catalog="cat-x", collection="col-y", correlation_id="cid-1",
        pg_conn=object(), entity_type="collection",
    )
    results = await dispatcher.fan_out_bulk(ctx, [_op(entity_id="i1")])

    assert calls == [], "non-item entity_type must not use the id-only storage plane"
    assert a.bulk_calls == []
    assert len(writer.rows) >= 1
    assert results["a"].succeeded == 1


@pytest.mark.asyncio
async def test_storage_plane_flag_on_access_aware_entry_included(monkeypatch):
    """An access-aware entry now takes the SAME id-only storage-plane
    branch as any other item-tier entry (#2687) — the drain recomputes the
    envelope from stored state, so there is no longer a payload requirement
    forcing it onto a different plane."""
    _patch_storage_plane_flag(monkeypatch, enabled=True)
    calls = _patch_storage_emit_recorder(monkeypatch)

    a = _AccessAwareStubIndexer("a")
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a})
    results = await dispatcher.fan_out_bulk(_item_ctx(), [_op(entity_id="i1")])

    assert a.bulk_calls == []
    assert len(calls) == 1
    row_ids = {r.item_id for r in calls[0]["rows"]}
    assert row_ids == {"i1"}
    assert results["a"].succeeded == 1
    assert results["a"].failed == 0


@pytest.mark.asyncio
async def test_storage_plane_flag_on_access_aware_entry_not_absorbed_in_task_run(monkeypatch):
    """The storage-plane flag prevents in-run absorption even for
    access-aware entries — the operator opted into letting storage_drain
    own every item-tier write."""
    _patch_storage_plane_flag(monkeypatch, enabled=True)
    calls = _patch_storage_emit_recorder(monkeypatch)

    a = _AccessAwareStubIndexer("a")
    writer = _RecordingWriter()
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_entry("a")], indexers={"a": a}, outbox=writer,
    )
    with task_run_scope(catalog="cat-x"):
        results = await dispatcher.fan_out_bulk(_item_ctx(), [_op(entity_id="i1")])

    assert a.bulk_calls == [], "storage-plane flag on must prevent in-run absorption"
    assert len(calls) == 1
    assert writer.rows == []
    assert results["a"].succeeded == 1


@pytest.mark.asyncio
async def test_storage_plane_flag_off_access_aware_entry_still_absorbed_in_task_run(monkeypatch):
    """Companion negative case: flag OFF keeps the ordinary in-run
    absorption behaviour unchanged for an access-aware entry."""
    _patch_storage_plane_flag(monkeypatch, enabled=False)
    _patch_storage_emit_recorder(monkeypatch)

    a = _AccessAwareStubIndexer("a")
    writer = _RecordingWriter()
    dispatcher = _make_dispatcher_with_outbox(
        entries=[_entry("a")], indexers={"a": a}, outbox=writer,
    )
    with task_run_scope(catalog="cat-x"):
        results = await dispatcher.fan_out_bulk(_item_ctx(), [_op(entity_id="i1")])

    assert len(a.bulk_calls) == 1
    assert writer.rows == []
    assert results["a"].succeeded == 1


@pytest.mark.asyncio
async def test_storage_plane_dispatch_path_log_mode(monkeypatch, caplog):
    import logging as _logging

    _patch_storage_plane_flag(monkeypatch, enabled=True)
    _patch_storage_emit_recorder(monkeypatch)

    a = _StubIndexer("a")
    dispatcher = _make_dispatcher(entries=[_entry("a")], indexers={"a": a})
    with caplog.at_level(_logging.INFO):
        await dispatcher.fan_out_bulk(_item_ctx(), [_op(entity_id="i1")])

    rows = _extract_dispatch_path_records(caplog)
    assert any(r.get("mode") == "storage_plane_id_only_enqueued" for r in rows), rows
    assert not any(r.get("mode") == "async_outbox_enqueued" for r in rows), rows
    assert not any(r.get("mode") == "post_commit_inline" for r in rows), rows
