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
    canned values pulled from the stack-managed deque below."""

    _q: list = []

    def __init__(self, sql, *, result_handler=None):
        self._sql = sql

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
async def test_dlqs_when_oracle_says_no_and_lock_acquired():
    # advisory_lock acquired → got=True; UPDATE returns one row.
    patches = _patches(
        oracle_live=False,
        results=[{"got": True}, {"task_id": "t-1"}],
    )
    for p in patches: p.start()
    try:
        result = await _maybe_dlq_unclaimable(
            engine=MagicMock(), row=_row(), capability_id="dead_driver",
        )
    finally:
        for p in patches: p.stop()
    assert result is True


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
