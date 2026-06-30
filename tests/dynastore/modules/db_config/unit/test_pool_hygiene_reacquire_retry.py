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

"""Unit tests for the bounded pool-hygiene re-acquire loop.

Covers ``_acquire_async_engine_connection`` under poison-storm conditions where
multiple consecutive pool slots have stale wires whose hygiene rollback raises
``AsyncpgInternalClientError``.

Scenarios tested:
- Storm: first two slots are poisoned, third is clean → clean conn returned,
  two poisoned slots invalidated, exactly three ``engine.connect()`` calls.
- Exhaustion: all budget slots are poisoned → re-raises, last slot invalidated.
- Happy path: first rollback succeeds → one connect call, no invalidate.
"""

from __future__ import annotations

from typing import List, Optional, Sequence

import pytest

import dynastore.modules.db_config.connection_health_config as chc
from dynastore.modules.db_config.connection_health_config import ConnectionRetryConfig


# ---------------------------------------------------------------------------
# Minimal fakes — mirroring _StateMachineFakeConn from the pool-hygiene suite
# ---------------------------------------------------------------------------


class _FakeTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _SlotConn:
    """AsyncConnection stub whose rollback() raises a pre-seeded exception list."""

    def __init__(
        self,
        *,
        rollback_errors: Sequence[Optional[BaseException]],
        log: List[str],
        tag: str,
    ):
        self._rollback_errors = list(rollback_errors)
        self._log = log
        self.tag = tag
        self.invalidated_count = 0
        self.closed = False
        self.rollback_count = 0
        self.sync_connection = type("_Sync", (), {})()

    async def rollback(self):
        self.rollback_count += 1
        self._log.append(f"rollback:{self.tag}")
        if self._rollback_errors:
            exc = self._rollback_errors.pop(0)
            if exc is not None:
                raise exc

    async def invalidate(self):
        self.invalidated_count += 1
        self._log.append(f"invalidate:{self.tag}")

    async def close(self):
        self.closed = True
        self._log.append(f"close:{self.tag}")

    def begin(self):
        self._log.append(f"begin:{self.tag}")
        return _FakeTx()

    def in_transaction(self) -> bool:
        return False


class _FakeEngine:
    def __init__(self, conns: List[_SlotConn]):
        self._conns = list(conns)
        self.connect_calls = 0

    async def connect(self) -> _SlotConn:
        self.connect_calls += 1
        return self._conns.pop(0)


# ---------------------------------------------------------------------------
# Shared patch helper
# ---------------------------------------------------------------------------


def _patch_qe(monkeypatch):
    from dynastore.modules.db_config import query_executor as qe

    monkeypatch.setattr(qe, "_get_wire_identity", lambda c: c, raising=True)
    monkeypatch.setattr(qe, "AsyncEngine", _FakeEngine, raising=True)
    monkeypatch.setattr(qe, "is_async_resource", lambda r: True, raising=True)

    async def _fast_sleep(_s):
        return None

    monkeypatch.setattr(qe.asyncio, "sleep", _fast_sleep, raising=True)
    return qe


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_poison_storm_two_bad_slots_then_clean(monkeypatch):
    """With budget=3, two consecutive poisoned slots are evicted and the third
    (clean) slot is returned. Exactly 3 connect() calls are made; both poisoned
    connections are invalidated and closed."""
    qe = _patch_qe(monkeypatch)

    # Override the live reader so we don't need a config service.
    async def _budget_3():
        return 3

    monkeypatch.setattr(
        qe, "_read_live_pool_hygiene_reacquire_attempts", _budget_3, raising=True
    )

    log: List[str] = []
    state15 = qe.AsyncpgInternalClientError(
        "cannot switch to state 15; another operation (2) is in progress"
    )
    poisoned_a = _SlotConn(rollback_errors=[state15], log=log, tag="p_a")
    poisoned_b = _SlotConn(
        rollback_errors=[
            qe.AsyncpgInternalClientError("cannot switch to state 15")
        ],
        log=log,
        tag="p_b",
    )
    clean = _SlotConn(rollback_errors=[None], log=log, tag="clean")
    engine = _FakeEngine([poisoned_a, poisoned_b, clean])

    result = await qe._acquire_async_engine_connection(engine)

    assert result is clean, log
    assert engine.connect_calls == 3, log

    # Both poisoned slots must have been invalidated and closed.
    assert poisoned_a.invalidated_count == 1, log
    assert poisoned_a.closed is True, log
    assert poisoned_b.invalidated_count == 1, log
    assert poisoned_b.closed is True, log

    # Clean slot: rollback called once, NOT invalidated.
    assert clean.rollback_count == 1, log
    assert clean.invalidated_count == 0, log


@pytest.mark.asyncio
async def test_poison_storm_exhaustion_raises_and_last_slot_invalidated(monkeypatch):
    """When every slot within the budget is poisoned, the function raises and
    the outer cleanup invalidates+closes the last acquired connection.

    The ``retry_on_transient_connect`` decorator also treats
    ``AsyncpgInternalClientError`` as a transient error and would normally
    retry the whole function. To isolate the inner-loop behaviour we pin the
    decorator to one total attempt via ``chc._retry_config``.
    """
    qe = _patch_qe(monkeypatch)

    # Pin the outer decorator to 1 total attempt so it does not retry the
    # whole function after the inner budget is exhausted.
    saved_retry_cfg = chc._retry_config
    chc._retry_config = ConnectionRetryConfig(max_retries=1, base_delay_seconds=0.0)

    # Budget of 2: at most 2 fresh slots tried after the initial poisoned one.
    async def _budget_2():
        return 2

    monkeypatch.setattr(
        qe, "_read_live_pool_hygiene_reacquire_attempts", _budget_2, raising=True
    )

    log: List[str] = []
    # Initial checkout (1) + budget (2) = 3 slots, all poisoned.
    slots = [
        _SlotConn(
            rollback_errors=[
                qe.AsyncpgInternalClientError("state 15")
            ],
            log=log,
            tag=f"p{i}",
        )
        for i in range(3)
    ]
    engine = _FakeEngine(slots)

    try:
        with pytest.raises(qe.AsyncpgInternalClientError):
            await qe._acquire_async_engine_connection(engine)
    finally:
        chc._retry_config = saved_retry_cfg

    # All three connects were attempted.
    assert engine.connect_calls == 3, log

    # The last slot was invalidated+closed by the outer BaseException handler.
    last = slots[2]
    assert last.invalidated_count == 1, log
    assert last.closed is True, log


@pytest.mark.asyncio
async def test_happy_path_no_invalidate(monkeypatch):
    """When the initial rollback succeeds, the budget reader is never called,
    exactly one connect() is made, and no invalidate occurs."""
    qe = _patch_qe(monkeypatch)

    budget_called = False

    async def _should_not_call():
        nonlocal budget_called
        budget_called = True
        return 3

    monkeypatch.setattr(
        qe, "_read_live_pool_hygiene_reacquire_attempts", _should_not_call, raising=True
    )

    log: List[str] = []
    clean = _SlotConn(rollback_errors=[None], log=log, tag="clean")
    engine = _FakeEngine([clean])

    result = await qe._acquire_async_engine_connection(engine)

    assert result is clean, log
    assert engine.connect_calls == 1, log
    assert clean.invalidated_count == 0, log
    assert clean.rollback_count == 1, log
    # Budget reader must NOT have been invoked on the happy path.
    assert not budget_called, "budget reader must not be called on the happy path"
