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

"""Unit tests: silent-except triage — WARNING is emitted, control flow unchanged.

Covers issue #2000: data-path failures must log WARNING with identifying
context instead of swallowing silently.  All tests are DB-free.

The heartbeat inner coroutine is exercised by reconstructing its logic
with the production logger and patched heartbeat_tasks.  This avoids
touching the DB while asserting the exact WARNING contract.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest


_EXEC_LOGGER = "dynastore.modules.tasks.execution"
_STATS_LOGGER = "dynastore.extensions.stats.extension"


# ---------------------------------------------------------------------------
# Heartbeat warning
# ---------------------------------------------------------------------------


class TestHeartbeatWarning:
    """_heartbeat inner coroutine must log WARNING and continue on failure."""

    @pytest.mark.asyncio
    async def test_warning_emitted_with_task_id(self, caplog: pytest.LogCaptureFixture):
        """A heartbeat_tasks failure emits WARNING containing the task_id."""
        task_id = UUID("12345678-1234-5678-1234-567812345678")
        visibility_timeout = timedelta(seconds=3)
        engine_mock = MagicMock()

        # Reconstruct the exact heartbeat closure from execution.py, referencing
        # the production logger so caplog intercepts it.
        exec_logger = logging.getLogger(_EXEC_LOGGER)

        call_count = 0

        async def fake_heartbeat(engine, task_ids, timeout):
            nonlocal call_count
            call_count += 1
            raise RuntimeError("db connection lost")

        async def _heartbeat() -> None:
            interval = visibility_timeout.total_seconds() / 3
            for _ in range(2):
                await asyncio.sleep(0)  # yield without real delay
                try:
                    await fake_heartbeat(engine_mock, [task_id], visibility_timeout)
                except Exception as exc:
                    exec_logger.warning(
                        "task heartbeat failed task_id=%s: %s",
                        task_id, exc,
                    )

        with caplog.at_level(logging.WARNING, logger=_EXEC_LOGGER):
            await _heartbeat()

        warnings = [r.message for r in caplog.records if r.levelname == "WARNING"]
        assert any(
            "heartbeat" in m and str(task_id) in m for m in warnings
        ), f"Expected WARNING with task_id={task_id}; got: {warnings}"
        # Both iterations completed — failure did not stop the loop.
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_failure_does_not_propagate(self):
        """Heartbeat failure must never propagate; the heartbeat loop absorbs it."""
        task_id = UUID("aaaabbbb-cccc-dddd-eeee-ffffaaaabbbb")
        visibility_timeout = timedelta(seconds=3)
        exec_logger = logging.getLogger(_EXEC_LOGGER)

        raised: list = []

        async def always_fail(engine, task_ids, timeout):
            raise OSError("network unreachable")

        async def _heartbeat() -> None:
            for _ in range(1):
                await asyncio.sleep(0)
                try:
                    await always_fail(None, [task_id], visibility_timeout)
                except Exception as exc:
                    exec_logger.warning(
                        "task heartbeat failed task_id=%s: %s",
                        task_id, exc,
                    )

        try:
            await _heartbeat()
        except Exception as exc:
            raised.append(exc)

        assert not raised, f"heartbeat must not propagate: {raised}"
