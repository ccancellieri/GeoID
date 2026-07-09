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

"""Unit tests for ``dynastore.tools.render_admission.RenderAdmissionGate``
(geoid#3155).

No live DB, no network I/O — the gate is a pure asyncio primitive over a
mockable RSS/budget signal.
"""
from __future__ import annotations

import asyncio

import pytest

from dynastore.tools.render_admission import (
    RenderAdmissionGate,
    RenderAdmissionRejected,
    resolve_render_admission_cap,
)


def _no_pressure_gate(**kwargs) -> RenderAdmissionGate:
    """A gate whose pressure check always reports "not under pressure" —
    isolates the tests that only care about the concurrency cap / queueing
    from the pressure signal."""
    kwargs.setdefault("get_rss_bytes", lambda: None)
    kwargs.setdefault("get_budget_bytes", lambda: None)
    return RenderAdmissionGate(**kwargs)


# ---------------------------------------------------------------------------
# Cap enforced
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cap_enforced_within_one_loop():
    """No more than max_concurrent renders run at once."""
    gate = _no_pressure_gate(max_concurrent=2, queue_wait_seconds=5.0)
    peak = 0
    live = 0

    async def worker():
        nonlocal peak, live
        async with gate.admit():
            live += 1
            peak = max(peak, live)
            await asyncio.sleep(0.02)
            live -= 1

    await asyncio.gather(*(worker() for _ in range(6)))
    assert peak == 2


# ---------------------------------------------------------------------------
# Excess queues, then sheds on timeout
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_excess_render_queues_then_sheds_on_timeout():
    """A render that finds every slot taken waits up to queue_wait_seconds,
    then raises RenderAdmissionRejected(reason="queue_timeout") — it never
    blocks past that bound."""
    gate = _no_pressure_gate(max_concurrent=1, queue_wait_seconds=0.05)

    async with gate.admit():
        # Only slot is held for longer than the queue-wait bound.
        start = asyncio.get_running_loop().time()
        with pytest.raises(RenderAdmissionRejected) as exc_info:
            await gate.acquire()
        elapsed = asyncio.get_running_loop().time() - start

    assert exc_info.value.reason == "queue_timeout"
    # Shed at (approximately) the configured bound, not immediately and not
    # much later than it.
    assert elapsed < 1.0


@pytest.mark.asyncio
async def test_render_admitted_once_a_slot_frees_within_the_queue_wait():
    """A render that queues briefly and gets a slot before the timeout is
    admitted normally (sanity check: queueing is not always shedding)."""
    gate = _no_pressure_gate(max_concurrent=1, queue_wait_seconds=2.0)

    async def _hold_then_release():
        await gate.acquire()
        await asyncio.sleep(0.05)
        gate.release()

    holder = asyncio.ensure_future(_hold_then_release())
    await asyncio.sleep(0.01)  # let the holder acquire first

    # This queues briefly behind the holder, then succeeds well within the
    # 2s bound once the holder releases.
    await gate.acquire()
    gate.release()
    await holder


# ---------------------------------------------------------------------------
# Memory pressure forces a shed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pressure_signal_forces_shed_without_consuming_a_slot():
    """RSS at/above the pressure ratio sheds immediately — before even
    attempting the semaphore, so the cap is left untouched for the next
    (healthy) caller."""
    budget_bytes = 1000 * 1024 * 1024
    gate = RenderAdmissionGate(
        max_concurrent=1,
        queue_wait_seconds=5.0,
        pressure_ratio=0.90,
        get_rss_bytes=lambda: int(budget_bytes * 0.95),  # 95% >= 90% threshold
        get_budget_bytes=lambda: budget_bytes,
    )

    with pytest.raises(RenderAdmissionRejected) as exc_info:
        await gate.acquire()
    assert exc_info.value.reason == "memory_pressure"

    # The rejected attempt never touched the semaphore — a fresh acquire
    # (pressure aside) must still see a fully free cap. Swap the signal to
    # "no pressure" and confirm the single slot is available immediately.
    gate._get_rss_bytes = lambda: int(budget_bytes * 0.10)
    await asyncio.wait_for(gate.acquire(), timeout=0.5)
    gate.release()


@pytest.mark.asyncio
async def test_no_pressure_below_threshold_admits_normally():
    budget_bytes = 1000 * 1024 * 1024
    gate = RenderAdmissionGate(
        max_concurrent=1,
        queue_wait_seconds=5.0,
        pressure_ratio=0.90,
        get_rss_bytes=lambda: int(budget_bytes * 0.50),
        get_budget_bytes=lambda: budget_bytes,
    )
    await gate.acquire()
    gate.release()


# ---------------------------------------------------------------------------
# Released on completion AND on failure — no semaphore leak
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_slot_released_on_successful_completion():
    gate = _no_pressure_gate(max_concurrent=1, queue_wait_seconds=1.0)

    async with gate.admit():
        pass

    # If the slot leaked, this would time out and raise.
    await asyncio.wait_for(gate.acquire(), timeout=0.5)
    gate.release()


@pytest.mark.asyncio
async def test_slot_released_when_render_body_raises():
    gate = _no_pressure_gate(max_concurrent=1, queue_wait_seconds=1.0)

    with pytest.raises(RuntimeError):
        async with gate.admit():
            raise RuntimeError("render blew up")

    # The failed render must not have leaked its slot.
    await asyncio.wait_for(gate.acquire(), timeout=0.5)
    gate.release()


@pytest.mark.asyncio
async def test_repeated_admit_cycles_never_leak_a_slot():
    """Many sequential successful + failing renders through admit() must
    always leave the cap exactly as free as it started."""
    gate = _no_pressure_gate(max_concurrent=2, queue_wait_seconds=1.0)

    for i in range(10):
        if i % 3 == 0:
            with pytest.raises(RuntimeError):
                async with gate.admit():
                    raise RuntimeError("boom")
        else:
            async with gate.admit():
                pass

    # Full cap must still be available — acquire it twice concurrently,
    # bounded, to prove nothing is stuck held.
    await asyncio.wait_for(gate.acquire(), timeout=0.5)
    await asyncio.wait_for(gate.acquire(), timeout=0.5)
    gate.release()
    gate.release()


# ---------------------------------------------------------------------------
# Retry-After present
# ---------------------------------------------------------------------------


def test_rejection_carries_retry_after_seconds():
    exc = RenderAdmissionRejected("queue_timeout")
    assert exc.retry_after_seconds == 5
    assert exc.reason == "queue_timeout"

    custom = RenderAdmissionRejected("memory_pressure", retry_after_seconds=10)
    assert custom.retry_after_seconds == 10


# ---------------------------------------------------------------------------
# Cap derivation from the per-worker memory budget
# ---------------------------------------------------------------------------


def test_resolve_render_admission_cap_derives_from_budget(monkeypatch):
    import dynastore.tools.render_admission as render_admission_mod

    monkeypatch.setattr(
        render_admission_mod, "resolve_watchdog_budget_mb", lambda: 3814
    )
    # 3814 * 0.5 // 400 = 4
    assert resolve_render_admission_cap() == 4


def test_resolve_render_admission_cap_falls_back_when_no_budget(monkeypatch):
    import dynastore.tools.render_admission as render_admission_mod

    monkeypatch.setattr(render_admission_mod, "resolve_watchdog_budget_mb", lambda: None)
    assert resolve_render_admission_cap() == render_admission_mod._FALLBACK_CONCURRENT


def test_resolve_render_admission_cap_floors_at_minimum(monkeypatch):
    import dynastore.tools.render_admission as render_admission_mod

    # A tiny budget must still admit at least _MIN_CONCURRENT concurrent renders.
    monkeypatch.setattr(render_admission_mod, "resolve_watchdog_budget_mb", lambda: 50)
    assert resolve_render_admission_cap() == render_admission_mod._MIN_CONCURRENT
