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

"""Unit tests for ``dynastore.tools.async_utils.LoopLocalSemaphore``.

Like the ``LoopLocalLock`` tests, these deliberately use ``asyncio.run(...)``
(a fresh event loop each call) because the whole point of
``LoopLocalSemaphore`` is correctness across *distinct* loops — the scenario a
raw module-level / singleton ``asyncio.Semaphore()`` fails.
"""
from __future__ import annotations

import asyncio

from dynastore.tools.async_utils import LoopLocalSemaphore

# Constructed at module scope on purpose: must not raise (no running loop at
# import) and must be safely reusable from every test's fresh loop below.
_SHARED = LoopLocalSemaphore(2)


def test_construct_without_running_loop_is_safe() -> None:
    # No running loop, no first use -> construction alone must not raise.
    sem = LoopLocalSemaphore(3)
    assert sem._value == 3


def test_caps_concurrency_within_one_loop() -> None:
    async def _run() -> int:
        sem = LoopLocalSemaphore(2)
        peak = 0
        live = 0

        async def worker() -> None:
            nonlocal peak, live
            async with sem:
                live += 1
                peak = max(peak, live)
                await asyncio.sleep(0.01)
                live -= 1

        await asyncio.gather(*(worker() for _ in range(6)))
        return peak

    # value=2 → never more than 2 concurrent holders.
    assert asyncio.run(_run()) == 2


def test_reusable_across_separate_event_loops() -> None:
    async def _use() -> bool:
        async with _SHARED:
            return True

    # A raw module-level asyncio.Semaphore() would bind to the first loop and
    # could raise "bound to a different event loop" on the second asyncio.run().
    # LoopLocalSemaphore keeps a per-loop semaphore, so both fresh loops succeed.
    assert asyncio.run(_use()) is True
    assert asyncio.run(_use()) is True


def test_distinct_loops_get_distinct_semaphores() -> None:
    seen: list[int] = []

    async def _capture() -> None:
        async with _SHARED:
            seen.append(id(_SHARED._for_running_loop()))

    asyncio.run(_capture())
    asyncio.run(_capture())
    # Each fresh loop lazily created its own underlying semaphore object.
    assert len(seen) == 2 and seen[0] != seen[1]


def test_timeout_bounded_acquire_returns_true_when_a_slot_is_free() -> None:
    async def _run() -> bool:
        sem = LoopLocalSemaphore(1)
        acquired = await sem.acquire(timeout=1.0)
        sem.release()
        return acquired

    assert asyncio.run(_run()) is True


def test_timeout_bounded_acquire_returns_false_and_holds_no_slot_on_timeout() -> None:
    """A caller that times out must not hold a slot — a subsequent acquire
    on the same (now-uncontended) semaphore must succeed immediately."""
    async def _run() -> tuple[bool, bool]:
        sem = LoopLocalSemaphore(1)
        async with sem:
            # Cap is 1 and already held above -> this must time out quickly.
            timed_out_acquire = await sem.acquire(timeout=0.05)

        # The timed-out attempt held nothing, so once the first holder
        # releases (the `async with` above), the slot is free again.
        second_acquire = await sem.acquire(timeout=1.0)
        sem.release()
        return timed_out_acquire, second_acquire

    timed_out_acquire, second_acquire = asyncio.run(_run())
    assert timed_out_acquire is False
    assert second_acquire is True
