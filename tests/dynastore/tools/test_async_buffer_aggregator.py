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

"""Regression coverage for the #2749 architecture review of
``AsyncBufferAggregator`` (packages/core/src/dynastore/tools/async_utils.py):

1. A slow (not dead) flush callback must not block ``add()`` — the lock
   guarding the buffer must be released before the callback runs. Before
   the fix, ``_loop()`` held the lock for the full duration of the flush
   callback, so every producer's ``add()`` would stall behind it — exactly
   the "logging blocks the request path" failure class #2749 exists to
   eliminate, just moved into the aggregator instead of the DB layer.

2. A buffer at its configured cap drops the OLDEST entry to make room for
   a new one, rather than growing without bound while the backend is slow.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from dynastore.tools.async_utils import AsyncBufferAggregator

pytestmark = pytest.mark.asyncio


async def test_add_not_blocked_by_slow_flush_callback():
    """add() must return promptly even while a threshold-triggered flush's
    callback is still in flight."""
    callback_started = asyncio.Event()
    release_callback = asyncio.Event()

    async def slow_callback(items):
        callback_started.set()
        await release_callback.wait()

    agg = AsyncBufferAggregator(
        flush_callback=slow_callback, threshold=1, interval=0.05, name="test-slow"
    )
    await agg.start()
    try:
        # threshold=1 -> this add() sets the flush event; the background
        # loop picks it up and calls the (slow) callback.
        await agg.add("item-1")
        await asyncio.wait_for(callback_started.wait(), timeout=2.0)

        # The callback is now blocked mid-flight. If the buffer lock were
        # still held around it (the pre-fix defect), this would hang until
        # release_callback is set — and time out.
        await asyncio.wait_for(agg.add("item-2"), timeout=1.0)
    finally:
        release_callback.set()
        await agg.stop()


async def test_buffer_drops_oldest_entries_at_cap(caplog):
    """A bounded aggregator must drop the oldest buffered entries once at
    capacity, keeping the newest ones, and never grow past max_size."""

    async def noop_callback(items):
        pass

    agg = AsyncBufferAggregator(
        flush_callback=noop_callback,
        threshold=1000,  # never auto-flush during this test
        interval=999,
        name="test-cap",
        max_size=3,
    )

    with caplog.at_level(logging.WARNING, logger="dynastore.tools.async_utils"):
        for i in range(5):
            await agg.add(f"item-{i}")

    assert len(agg._buffer) == 3, "buffer must never exceed max_size"
    assert agg._buffer == ["item-2", "item-3", "item-4"], (
        "buffer must keep the NEWEST entries and drop the oldest, not the reverse"
    )
    assert any("dropped" in r.message for r in caplog.records), (
        "a full buffer must emit a warning when it starts dropping entries"
    )


async def test_unbounded_aggregator_keeps_every_item_by_default():
    """max_size=None (the default) preserves the historical unbounded
    behavior for every other caller of this generic utility."""

    async def noop_callback(items):
        pass

    agg = AsyncBufferAggregator(
        flush_callback=noop_callback, threshold=1000, interval=999, name="test-unbounded"
    )
    for i in range(10):
        await agg.add(f"item-{i}")

    assert len(agg._buffer) == 10
