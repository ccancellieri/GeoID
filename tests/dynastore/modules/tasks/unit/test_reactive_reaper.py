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
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tasks.dispatcher import (
    _maybe_dlq_unclaimable,
    _stable_advisory_lock_key,
    sweep_dead_capability_rows,
)


@pytest.fixture(autouse=True)
def _capability_gated_task_type():
    """No production ``TaskProtocol`` currently declares
    ``required_capability`` (``index_propagation`` was the last one,
    retired). Register a synthetic mapping for the duration of this
    suite so the bulk-DLQ sweep path — gated on
    ``TASK_TYPE_CAPABILITY_INPUTS_KEY`` — stays exercised end-to-end;
    a real capability-gated task lands its own entry here (#522).
    """
    from dynastore.modules.tasks.capability_oracle import (
        TASK_TYPE_CAPABILITY_INPUTS_KEY,
    )
    with patch.dict(TASK_TYPE_CAPABILITY_INPUTS_KEY, {"storage_drain": "indexer_id"}):
        yield


def _row(task_id: str = "t-1", task_type: str = "storage_drain"):
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
    import logging
    import re
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
    re-check is bounded by the value returned from
    ``_load_oracle_inner_timeout`` (sourced from
    ``CachePluginConfig.oracle_inner_timeout_seconds``, default 0.5 s). A
    cache that hangs longer than the bound must not convoy peer dispatchers
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
            "dynastore.modules.tasks.dispatcher._load_oracle_inner_timeout",
            new=AsyncMock(return_value=0.05),
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


@pytest.mark.asyncio
async def test_inner_oracle_timeout_uses_cache_config(caplog):
    """#639: _maybe_dlq_unclaimable reads oracle_inner_timeout_seconds from
    CachePluginConfig (via _load_oracle_inner_timeout) instead of a hardcoded
    constant. Patching the helper to 0.05 s must still trigger the fail-open path.
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
            "dynastore.modules.tasks.dispatcher._load_oracle_inner_timeout",
            new=AsyncMock(return_value=0.05),
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


@pytest.mark.asyncio
async def test_bulk_dlq_sweeps_siblings_for_known_task_type(caplog):
    """#529: after the per-row CAS DLQ wins, the same locked transaction
    issues a bulk UPDATE that DLQs every sibling row sharing the dead
    capability. The structured ``dispatcher_dlq_bulk_total`` line fires
    with ``source=reactive`` and the sibling count."""
    import logging
    import re
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
        r"dispatcher_dlq_bulk_total source=reactive "
        r"task_type=storage_drain "
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
        "dispatcher_dlq_bulk_total" in m for m in captured
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


# ---------------------------------------------------------------------------
# sweep_dead_capability_rows — standalone helper used by the proactive
# sweeper task (#524 PR B). Shares ``_emit_bulk_dlq`` with the reactive
# reaper above, so the SQL UPDATE shape is pinned by the existing
# ``test_bulk_dlq_*`` tests; the tests below pin only the standalone
# control flow (oracle gate, advisory lock, task_type gate, fail-safe).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sweep_returns_zero_when_task_type_missing():
    """No SQL is constructed when task_type is omitted."""
    _FakeQuery._sql_log = []
    with patch(
        "dynastore.modules.db_config.query_executor.DQLQuery",
        _FakeQuery,
    ):
        count = await sweep_dead_capability_rows(
            engine=MagicMock(), capability_id="x",
        )
    assert count == 0
    assert _FakeQuery._sql_log == []


@pytest.mark.asyncio
async def test_sweep_returns_zero_for_unmapped_task_type():
    """Unmapped task_type → no SQL, no lock attempt — same guarantee
    as ``test_bulk_dlq_skipped_for_unmapped_task_type`` for the reactive
    path. The proactive path must hold the same gate so it can't be used
    to interpolate an attacker-controlled ``inputs->>`` key."""
    _FakeQuery._sql_log = []
    with patch(
        "dynastore.modules.db_config.query_executor.DQLQuery",
        _FakeQuery,
    ):
        count = await sweep_dead_capability_rows(
            engine=MagicMock(),
            capability_id="dead_driver",
            task_type="some_other_task",
        )
    assert count == 0
    assert _FakeQuery._sql_log == []


@pytest.mark.asyncio
async def test_sweep_returns_zero_when_capability_live():
    patches = _patches(oracle_live=True, results=[])
    for p in patches: p.start()
    try:
        count = await sweep_dead_capability_rows(
            engine=MagicMock(),
            capability_id="alive_driver",
            task_type="storage_drain",
        )
    finally:
        for p in patches: p.stop()
    assert count == 0


@pytest.mark.asyncio
async def test_sweep_bulk_dlqs_when_capability_dead(caplog):
    """Dead capability + locked txn → bulk UPDATE fires and returns the
    sibling count. Mirrors ``test_bulk_dlq_sweeps_siblings_for_known_task_type``
    for the reactive path."""
    import logging
    import re
    patches = _patches(
        oracle_live=False,
        results=[
            {"got": True},                                  # advisory lock
            [{"task_id": "t-a"}, {"task_id": "t-b"}],       # bulk UPDATE
        ],
    )
    for p in patches: p.start()
    try:
        caplog.set_level(logging.INFO, logger="dynastore.modules.tasks.dispatcher")
        count = await sweep_dead_capability_rows(
            engine=MagicMock(),
            capability_id="dead_driver",
            task_type="storage_drain",
        )
    finally:
        for p in patches: p.stop()
    assert count == 2
    pattern = re.compile(
        r"dispatcher_dlq_bulk_total source=proactive "
        r"task_type=storage_drain "
        r"capability=dead_driver reason=no_live_worker count=2"
    )
    assert any(pattern.search(r.message) for r in caplog.records), (
        f"missing bulk_total line in: {[r.message for r in caplog.records]}"
    )


@pytest.mark.asyncio
async def test_sweep_skips_when_advisory_lock_contended():
    patches = _patches(
        oracle_live=False,
        results=[{"got": False}],
    )
    for p in patches: p.start()
    try:
        count = await sweep_dead_capability_rows(
            engine=MagicMock(),
            capability_id="dead_driver",
            task_type="storage_drain",
        )
    finally:
        for p in patches: p.stop()
    assert count == 0


@pytest.mark.asyncio
async def test_sweep_fails_safe_on_infra_error():
    with patch(
        "dynastore.modules.tasks.capability_oracle.is_capability_live",
        AsyncMock(side_effect=RuntimeError("boom")),
    ):
        count = await sweep_dead_capability_rows(
            engine=MagicMock(),
            capability_id="x",
            task_type="storage_drain",
        )
    assert count == 0


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
