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

"""``BatchedHeartbeat`` stall self-detection.

A blocking sync provisioner hook running on the same event loop as
``BatchedHeartbeat._beat_loop`` can starve the heartbeat wakeups, letting the
lease lapse and the reaper reclaim a task that is still being worked. This
does not fix that (see ``call_hook`` offload in ``dynastore.tasks._helpers``)
but adds a log-only self-check: if ``_beat_loop`` wakes up much later than
expected, it is the event loop that stalled, not the sleep — worth a WARNING
so the symptom is diagnosable straight from the logs.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tasks import dispatcher as dispatcher_mod


@pytest.mark.asyncio
async def test_beat_loop_warns_when_wake_is_delayed(caplog):
    """A wakeup far later than ``interval`` after the scheduled sleep must
    log a WARNING naming the suspected event-loop blocking."""
    hb = dispatcher_mod.BatchedHeartbeat(
        engine=MagicMock(), interval=timedelta(seconds=1)
    )

    # monotonic() is read once before the sleep (to compute expected_wake)
    # and once after. A 5s gap on a 1s interval simulates a stalled loop.
    monotonic_values = iter([0.0, 5.0])

    async def fake_flush():
        raise asyncio.CancelledError()

    with patch.object(
        dispatcher_mod.time, "monotonic", side_effect=lambda: next(monotonic_values)
    ), patch.object(
        dispatcher_mod.asyncio, "sleep", new=AsyncMock()
    ), patch.object(
        hb, "_flush", side_effect=fake_flush
    ):
        caplog.set_level(logging.WARNING, logger=dispatcher_mod.logger.name)
        with pytest.raises(asyncio.CancelledError):
            await hb._beat_loop()

    assert any(
        "loop stalled" in record.message for record in caplog.records
    ), f"expected a stall warning; got {[r.message for r in caplog.records]}"


@pytest.mark.asyncio
async def test_beat_loop_no_warning_on_normal_cadence(caplog):
    """A wakeup on schedule must not log a stall warning."""
    hb = dispatcher_mod.BatchedHeartbeat(
        engine=MagicMock(), interval=timedelta(seconds=1)
    )

    # monotonic() advances by exactly `interval` — no drift.
    monotonic_values = iter([0.0, 1.0])

    async def fake_flush():
        raise asyncio.CancelledError()

    with patch.object(
        dispatcher_mod.time, "monotonic", side_effect=lambda: next(monotonic_values)
    ), patch.object(
        dispatcher_mod.asyncio, "sleep", new=AsyncMock()
    ), patch.object(
        hb, "_flush", side_effect=fake_flush
    ):
        caplog.set_level(logging.WARNING, logger=dispatcher_mod.logger.name)
        with pytest.raises(asyncio.CancelledError):
            await hb._beat_loop()

    assert not any(
        "loop stalled" in record.message for record in caplog.records
    ), f"unexpected stall warning on normal cadence: {[r.message for r in caplog.records]}"
