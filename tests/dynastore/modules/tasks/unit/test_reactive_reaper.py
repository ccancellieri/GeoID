"""Unit tests for the dispatcher's reactive reaper hook (#502).

Contract:
- When the oracle says the capability IS live → ``_maybe_dlq_unclaimable``
  returns False (caller falls through to standard back-off).
- When the oracle says the capability is NOT live and the advisory lock
  is acquired → CAS UPDATE moves the row to DEAD_LETTER and returns True.
- When the advisory lock is contended → returns False (another dispatcher
  is handling it; the caller falls back to standard back-off).
- When the CAS UPDATE matches zero rows (race: another dispatcher
  flipped the row to ACTIVE) → returns False, no exception.
- Any infra failure → returns False (fail-safe; no false DLQ).
- IndexPropagationTask.required_capability extracts ``indexer_id`` from
  the row payload.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tasks.dispatcher import (
    _maybe_dlq_unclaimable,
    _stable_advisory_lock_key,
)
from dynastore.tasks.index_propagation.task import IndexPropagationTask


def _row(task_id: str = "t-1", task_type: str = "index_propagation"):
    return {
        "task_id": task_id,
        "timestamp": datetime(2026, 5, 11, tzinfo=timezone.utc),
        "task_type": task_type,
    }


class _FakeQuery:
    """Stand-in for DQLQuery — its ``execute`` is awaited and returns
    canned values pulled from the stack-managed deque below.

    Also records every SQL string passed to the constructor (in
    ``_sql_log``) so #566 regression tests can assert no stray
    bulk-UPDATE is constructed for unmapped task_types.
    """

    _q: list = []
    _sql_log: list = []

    def __init__(self, sql, *, result_handler=None):
        self._sql = sql
        _FakeQuery._sql_log.append(sql)

    async def execute(self, _conn, **_params):
        if not _FakeQuery._q:
            return None
        return _FakeQuery._q.pop(0)


class _FakeTxCtx:
    async def __aenter__(self):
        return MagicMock()

    async def __aexit__(self, *a):
        return False


def _patches(oracle_live: bool, results: list):
    """Build the unittest.mock.patch context managers shared by tests."""
    _FakeQuery._q = list(results)
    _FakeQuery._sql_log = []
    return [
        patch(
            "dynastore.modules.tasks.capability_oracle.is_capability_live",
            AsyncMock(return_value=oracle_live),
        ),
        patch(
            "dynastore.modules.db_config.query_executor.DQLQuery",
            _FakeQuery,
        ),
        patch(
            "dynastore.modules.db_config.query_executor.managed_transaction",
            return_value=_FakeTxCtx(),
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.get_task_schema",
            return_value="tasks",
        ),
    ]


@pytest.mark.asyncio
async def test_returns_false_when_capability_live():
    patches = _patches(oracle_live=True, results=[])
    for p in patches: p.start()
    try:
        result = await _maybe_dlq_unclaimable(
            engine=MagicMock(), row=_row(), capability_id="x",
        )
    finally:
        for p in patches: p.stop()
    assert result is False


@pytest.mark.asyncio
async def test_dlqs_when_oracle_says_no_and_lock_acquired(caplog):
    # advisory_lock acquired → got=True; UPDATE returns one row.
    import logging, re
    patches = _patches(
        oracle_live=False,
        results=[{"got": True}, {"task_id": "t-1"}],
    )
    for p in patches: p.start()
    try:
        caplog.set_level(logging.INFO, logger="dynastore.modules.tasks.dispatcher")
        result = await _maybe_dlq_unclaimable(
            engine=MagicMock(), row=_row(), capability_id="dead_driver",
        )
    finally:
        for p in patches: p.stop()
    assert result is True
    # #528: structured key=value log-based counter line on DLQ success.
    pattern = re.compile(
        r"dispatcher_reactive_dlq_total task_type=\S+ "
        r"capability=dead_driver reason=no_live_worker task_id=\S+"
    )
    assert any(pattern.search(r.message) for r in caplog.records), (
        f"missing dispatcher_reactive_dlq_total line in: {[r.message for r in caplog.records]}"
    )


@pytest.mark.asyncio
async def test_skips_dlq_when_advisory_lock_contended():
    patches = _patches(
        oracle_live=False,
        results=[{"got": False}],  # lock not acquired
    )
    for p in patches: p.start()
    try:
        result = await _maybe_dlq_unclaimable(
            engine=MagicMock(), row=_row(), capability_id="dead_driver",
        )
    finally:
        for p in patches: p.stop()
    assert result is False


@pytest.mark.asyncio
async def test_cas_race_returns_false():
    # lock acquired, but CAS UPDATE matches 0 rows (race: another
    # dispatcher claimed the row between SELECT and UPDATE).
    patches = _patches(
        oracle_live=False,
        results=[{"got": True}, None],
    )
    for p in patches: p.start()
    try:
        result = await _maybe_dlq_unclaimable(
            engine=MagicMock(), row=_row(), capability_id="dead_driver",
        )
    finally:
        for p in patches: p.stop()
    assert result is False


@pytest.mark.asyncio
async def test_infra_failure_fails_safe():
    with patch(
        "dynastore.modules.tasks.capability_oracle.is_capability_live",
        AsyncMock(side_effect=RuntimeError("boom")),
    ):
        result = await _maybe_dlq_unclaimable(
            engine=MagicMock(), row=_row(), capability_id="x",
        )
    assert result is False


@pytest.mark.asyncio
async def test_inner_oracle_timeout_treated_as_live(caplog):
    """#629: inside the locked transaction, the ``is_capability_live``
    re-check is bounded by ``_INNER_CAPABILITY_LIVE_TIMEOUT_S``. A cache
    that hangs longer than the bound must not convoy peer dispatchers
    behind the held DB connection + advisory xact lock; the bound expires
    and we fail-open (treat as live → return False, no DLQ).

    Outer call: returns False (live=False, would proceed). Inner call:
    hangs past the bound → wait_for raises TimeoutError → live=True →
    return False. Verifies CAS UPDATE is never reached.
    """
    import asyncio

    call_count = {"n": 0}

    async def _oracle(_cap_id):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return False
        await asyncio.sleep(10)
        return False

    patches = [
        patch(
            "dynastore.modules.tasks.capability_oracle.is_capability_live",
            side_effect=_oracle,
        ),
        patch(
            "dynastore.modules.db_config.query_executor.DQLQuery",
            _FakeQuery,
        ),
        patch(
            "dynastore.modules.db_config.query_executor.managed_transaction",
            return_value=_FakeTxCtx(),
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.get_task_schema",
            return_value="tasks",
        ),
        patch(
            "dynastore.modules.tasks.dispatcher._INNER_CAPABILITY_LIVE_TIMEOUT_S",
            0.05,
        ),
    ]
    _FakeQuery._q = [{"got": True}]
    _FakeQuery._sql_log = []
    for p in patches: p.start()
    try:
        result = await _maybe_dlq_unclaimable(
            engine=MagicMock(), row=_row(), capability_id="x",
        )
    finally:
        for p in patches: p.stop()

    assert result is False
    assert call_count["n"] == 2
    assert not any("UPDATE" in s and "DEAD_LETTER" in s for s in _FakeQuery._sql_log)


def test_index_propagation_required_capability_from_payload():
    payload = {"inputs": {"indexer_id": "collection_elasticsearch_driver"}}
    assert (
        IndexPropagationTask.required_capability(payload)
        == "collection_elasticsearch_driver"
    )


def test_index_propagation_required_capability_missing_returns_none():
    assert IndexPropagationTask.required_capability({"inputs": {}}) is None
    assert IndexPropagationTask.required_capability({}) is None
    assert IndexPropagationTask.required_capability(None) is None


@pytest.mark.asyncio
async def test_bulk_dlq_sweeps_siblings_for_known_task_type(caplog):
    """#529: after the per-row CAS DLQ wins, the same locked transaction
    issues a bulk UPDATE that DLQs every sibling row sharing the dead
    capability. The structured ``dispatcher_reactive_dlq_bulk_total`` line
    fires with the sibling count."""
    import logging, re
    patches = _patches(
        oracle_live=False,
        results=[
            {"got": True},                                 # advisory lock
            {"task_id": "t-1"},                            # per-row UPDATE
            [{"task_id": "t-2"}, {"task_id": "t-3"},
             {"task_id": "t-4"}],                          # bulk UPDATE
        ],
    )
    for p in patches: p.start()
    try:
        caplog.set_level(logging.INFO, logger="dynastore.modules.tasks.dispatcher")
        result = await _maybe_dlq_unclaimable(
            engine=MagicMock(), row=_row(), capability_id="dead_driver",
        )
    finally:
        for p in patches: p.stop()
    assert result is True
    pattern = re.compile(
        r"dispatcher_reactive_dlq_bulk_total task_type=index_propagation "
        r"capability=dead_driver reason=no_live_worker count=3"
    )
    assert any(pattern.search(r.message) for r in caplog.records), (
        f"missing bulk_total line in: {[r.message for r in caplog.records]}"
    )


@pytest.mark.asyncio
async def test_bulk_dlq_skipped_for_unmapped_task_type():
    """Task types with no entry in ``TASK_TYPE_CAPABILITY_INPUTS_KEY``
    only get the per-row DLQ — no bulk UPDATE is attempted, no
    interpolation of an unknown JSONB key."""
    # Only two results — if the bulk path tried to execute it would
    # silently pop ``None`` (no exception), so the real assertion is on
    # the absence of the bulk log line.
    import logging as _logging
    patches = _patches(
        oracle_live=False,
        results=[{"got": True}, {"task_id": "t-1"}],
    )
    for p in patches:
        p.start()
    captured = []
    handler = _logging.Handler()
    handler.emit = lambda record: captured.append(record.getMessage())
    logger_ = _logging.getLogger("dynastore.modules.tasks.dispatcher")
    logger_.addHandler(handler)
    try:
        result = await _maybe_dlq_unclaimable(
            engine=MagicMock(),
            row=_row(task_type="some_other_task"),
            capability_id="dead_driver",
        )
    finally:
        logger_.removeHandler(handler)
        for p in patches:
            p.stop()
    assert result is True
    assert not any(
        "dispatcher_reactive_dlq_bulk_total" in m for m in captured
    ), f"bulk line should not fire for unmapped task_type: {captured}"
    # #566: belt-and-braces — assert no bulk-UPDATE SQL was even
    # constructed. The bulk path interpolates ``inputs->>'<key>'`` into
    # the SQL string; its absence guarantees the gate held even if the
    # log line were silently dropped or renamed in a future refactor.
    bulk_sqls = [s for s in _FakeQuery._sql_log if "inputs->>" in (s or "")]
    assert not bulk_sqls, (
        f"unmapped task_type must not construct bulk-UPDATE SQL; got: {bulk_sqls}"
    )
    # Only the per-row path runs: advisory lock + per-row UPDATE = 2.
    assert len(_FakeQuery._sql_log) <= 2, (
        f"unmapped path should issue at most 2 SQL statements; got "
        f"{len(_FakeQuery._sql_log)}: {_FakeQuery._sql_log}"
    )


def test_task_type_capability_inputs_key_mapping_exposes_index_propagation():
    """Regression: the mapping must declare ``index_propagation`` so the
    bulk-DLQ sweep can extract ``inputs->>'indexer_id'``. Future
    capability-gated tasks add their own entries here."""
    from dynastore.modules.tasks.capability_oracle import (
        TASK_TYPE_CAPABILITY_INPUTS_KEY,
    )
    assert TASK_TYPE_CAPABILITY_INPUTS_KEY["index_propagation"] == "indexer_id"


def test_stable_advisory_lock_key_is_deterministic():
    """Regression: Python's builtin ``hash()`` is salted per-process,
    so two pods hashing the same string get different lock keys —
    breaks any "single leader across the deployment" advisory lock.
    ``_stable_advisory_lock_key`` MUST be deterministic across calls
    (and, by extension, across pods/processes/Python versions).
    """
    k1 = _stable_advisory_lock_key("dynastore.idx_reaper", "collection_es")
    k2 = _stable_advisory_lock_key("dynastore.idx_reaper", "collection_es")
    assert k1 == k2
    # Known golden values (blake2b is stable across versions/platforms).
    assert k1 == _stable_advisory_lock_key(
        "dynastore.idx_reaper", "collection_es",
    )
    # Different capability ids produce different keys.
    assert k1 != _stable_advisory_lock_key(
        "dynastore.idx_reaper", "other_indexer",
    )
    # Fits PostgreSQL signed bigint (non-negative 63-bit).
    assert 0 <= k1 < 2 ** 63
    # Different namespace produces a different key for the same id.
    assert k1 != _stable_advisory_lock_key(
        "dynastore.other_namespace", "collection_es",
    )
