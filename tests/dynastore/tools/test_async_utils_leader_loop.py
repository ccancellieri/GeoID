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

"""run_leader_loop — leadership released on exception."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, Optional

import pytest

from dynastore.modules.db_config.exceptions import DatabaseConnectionError
from dynastore.tools.async_utils import run_leader_loop


class _LeadershipTracker:
    """Records enter/exit pairs around the leader-held context."""

    def __init__(
        self,
        *,
        is_leader_sequence: list[bool],
        lock_conn: Any = None,
    ) -> None:
        self._is_leader = list(is_leader_sequence)
        self.lock_conn = lock_conn
        self.acquired = 0
        self.released = 0
        self.held = False

    @asynccontextmanager
    async def acquire(self):
        is_leader = self._is_leader.pop(0) if self._is_leader else False
        self.acquired += 1
        if is_leader:
            self.held = True
        try:
            # run_leader_loop unpacks (is_leader, lock_conn)
            yield (is_leader, self.lock_conn if is_leader else None)
        finally:
            if is_leader:
                self.held = False
                self.released += 1


@pytest.mark.asyncio
async def test_resigns_on_exception_inside_on_leader():
    """Body raising must NOT keep leadership — the lock must be released
    before the outer loop retries."""
    tracker = _LeadershipTracker(is_leader_sequence=[True, True])
    held_during_exception = False

    async def _raising_body(lock_conn: Any) -> None:
        nonlocal held_during_exception
        held_during_exception = tracker.held
        raise RuntimeError("boom")

    stop_after = {"n": 0}

    def _is_shutdown():
        stop_after["n"] += 1
        return stop_after["n"] > 2  # let one full iteration run then stop

    await run_leader_loop(
        acquire_leadership=tracker.acquire,
        on_leader=_raising_body,
        name="test",
        cadence_seconds=0.0,
        is_shutdown=_is_shutdown,
    )

    assert held_during_exception is True
    assert tracker.held is False
    assert tracker.acquired >= 1
    assert tracker.released == tracker.acquired


@pytest.mark.asyncio
async def test_non_leader_sleeps_and_retries():
    tracker = _LeadershipTracker(is_leader_sequence=[False, False, True])
    body_calls = {"n": 0}

    async def _body(lock_conn: Any) -> None:
        body_calls["n"] += 1

    stop_after = {"n": 0}

    def _is_shutdown():
        stop_after["n"] += 1
        return stop_after["n"] > 3

    await run_leader_loop(
        acquire_leadership=tracker.acquire,
        on_leader=_body,
        name="test",
        cadence_seconds=0.0,
        is_shutdown=_is_shutdown,
    )

    assert body_calls["n"] == 1
    assert tracker.acquired == 3


@pytest.mark.asyncio
async def test_cancelled_error_propagates():
    tracker = _LeadershipTracker(is_leader_sequence=[True])

    async def _cancel_body(lock_conn: Any) -> None:
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await run_leader_loop(
            acquire_leadership=tracker.acquire,
            on_leader=_cancel_body,
            name="test",
            cadence_seconds=0.0,
        )

    assert tracker.held is False
    assert tracker.released == 1


@pytest.mark.asyncio
async def test_leader_paces_to_cadence_not_hot_loop():
    """A per-tick ``on_leader`` MUST be throttled to ``cadence_seconds`` on the
    success path: the leader releases the lock and sleeps before re-acquiring,
    rather than hot-looping. Regression — the success-path cadence sleep was
    dropped when the reapers migrated to one-tick ``on_leader`` callbacks, so a
    leader re-acquired and re-ticked thousands of times per second."""
    tracker = _LeadershipTracker(is_leader_sequence=[True] * 10_000)
    ticks = {"n": 0}

    async def _one_tick(lock_conn: Any) -> None:
        ticks["n"] += 1
        await asyncio.sleep(0)  # real yield, like DB I/O in a real tick

    task = asyncio.create_task(
        run_leader_loop(
            acquire_leadership=tracker.acquire,
            on_leader=_one_tick,
            name="pace-test",
            cadence_seconds=0.05,
        )
    )
    await asyncio.sleep(0.2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # ~4 ticks in 0.2s at a 0.05s cadence; a hot loop would be in the thousands.
    assert ticks["n"] <= 15, f"hot loop: {ticks['n']} ticks in 0.2s at 0.05s cadence"
    # Per-tick release preserved: the lock is released after every acquisition.
    assert tracker.released == tracker.acquired


@pytest.mark.asyncio
async def test_shutdown_event_interrupts_cadence_sleep():
    """With a ``shutdown_event`` the cadence sleep wakes immediately on shutdown
    instead of blocking for the full (long) cadence — so a leader pod drains
    promptly rather than being force-cancelled by the supervisor timeout."""
    tracker = _LeadershipTracker(is_leader_sequence=[True] * 100)
    shutdown = asyncio.Event()
    ticks = {"n": 0}

    async def _one_tick(lock_conn: Any) -> None:
        ticks["n"] += 1
        shutdown.set()  # request stop right after the first tick

    # Cadence is huge; without interruption the loop would block ~30s.
    await asyncio.wait_for(
        run_leader_loop(
            acquire_leadership=tracker.acquire,
            on_leader=_one_tick,
            name="interrupt-test",
            cadence_seconds=30.0,
            is_shutdown=shutdown.is_set,
            shutdown_event=shutdown,
        ),
        timeout=2.0,  # must finish well under the 30s cadence
    )
    assert ticks["n"] == 1


# ---------------------------------------------------------------------------
# pre_tick_probe tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_probe_runs_before_on_leader():
    """pre_tick_probe is called once per leader tick, before on_leader."""
    call_order: list[str] = []
    fake_conn = object()
    tracker = _LeadershipTracker(is_leader_sequence=[True], lock_conn=fake_conn)

    async def _probe(lock_conn: Any) -> None:
        assert lock_conn is fake_conn
        call_order.append("probe")

    async def _on_leader(lock_conn: Any) -> None:
        assert lock_conn is fake_conn
        call_order.append("on_leader")

    def _is_shutdown():
        # Stop after one full leader cycle
        return len(call_order) >= 2

    await run_leader_loop(
        acquire_leadership=tracker.acquire,
        on_leader=_on_leader,
        name="probe-order-test",
        cadence_seconds=0.0,
        is_shutdown=_is_shutdown,
        pre_tick_probe=_probe,
    )

    assert call_order == ["probe", "on_leader"]


@pytest.mark.asyncio
async def test_probe_failure_causes_resign_and_on_leader_not_called():
    """If the probe raises, on_leader must NOT be called and the lock is released."""
    fake_conn = object()
    tracker = _LeadershipTracker(is_leader_sequence=[True, True], lock_conn=fake_conn)
    on_leader_called = {"n": 0}

    async def _failing_probe(lock_conn: Any) -> None:
        raise DatabaseConnectionError("wire died")

    async def _on_leader(lock_conn: Any) -> None:
        on_leader_called["n"] += 1

    stop_after = {"n": 0}

    def _is_shutdown():
        stop_after["n"] += 1
        return stop_after["n"] > 2

    await run_leader_loop(
        acquire_leadership=tracker.acquire,
        on_leader=_on_leader,
        name="probe-fail-test",
        cadence_seconds=0.0,
        is_shutdown=_is_shutdown,
        pre_tick_probe=_failing_probe,
    )

    # on_leader must never be reached when probe fails
    assert on_leader_called["n"] == 0
    # Lock is released every time it was acquired
    assert tracker.released == tracker.acquired


@pytest.mark.asyncio
async def test_probe_skipped_when_not_leader():
    """pre_tick_probe is NOT called when this pod is not the leader."""
    probe_called = {"n": 0}
    tracker = _LeadershipTracker(is_leader_sequence=[False, False])

    async def _probe(lock_conn: Any) -> None:
        probe_called["n"] += 1

    async def _on_leader(lock_conn: Any) -> None:
        pass

    stop_after = {"n": 0}

    def _is_shutdown():
        stop_after["n"] += 1
        return stop_after["n"] > 2

    await run_leader_loop(
        acquire_leadership=tracker.acquire,
        on_leader=_on_leader,
        name="probe-non-leader-test",
        cadence_seconds=0.0,
        is_shutdown=_is_shutdown,
        pre_tick_probe=_probe,
    )

    assert probe_called["n"] == 0


@pytest.mark.asyncio
async def test_probe_cancelled_error_propagates_without_resign():
    """CancelledError from the probe must propagate — it is shutdown/drain,
    not a dead wire. The leader loop must NOT catch it as a resign signal."""
    fake_conn = object()
    tracker = _LeadershipTracker(is_leader_sequence=[True], lock_conn=fake_conn)

    async def _cancel_probe(lock_conn: Any) -> None:
        raise asyncio.CancelledError()

    async def _on_leader(lock_conn: Any) -> None:
        pass  # must not be reached

    with pytest.raises(asyncio.CancelledError):
        await run_leader_loop(
            acquire_leadership=tracker.acquire,
            on_leader=_on_leader,
            name="probe-cancel-test",
            cadence_seconds=0.0,
            pre_tick_probe=_cancel_probe,
        )

    # Leadership was acquired and released (the acquirer's finally block runs)
    assert tracker.released == 1
