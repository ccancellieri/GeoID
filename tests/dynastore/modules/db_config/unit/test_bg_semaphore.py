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

"""Unit tests for background_managed_transaction semaphore gating.

Covers:
- Saturated semaphore raises TimeoutError and logs a warning.
- At most max_background_db_concurrency tasks hold the inner transaction at once.
- Semaphore is rebuilt when the configured limit changes.
- Released slot is immediately reusable by the next caller.
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest

import dynastore.modules.db_config.connection_health_config as chc
from dynastore.modules.db_config import query_executor
from dynastore.modules.db_config.query_executor import (
    _get_bg_semaphore,
    background_managed_transaction,
)


@pytest.fixture(autouse=True)
def _reset_bg_globals():
    """Restore module-level bg-semaphore state after each test."""
    saved_sem = query_executor._bg_semaphore
    saved_limit = query_executor._bg_semaphore_limit
    saved_global = chc._max_background_db_concurrency
    saved_wait = query_executor._BG_SEMAPHORE_WAIT_S
    # Use a very short timeout so tests that expect saturation don't take 2 s.
    query_executor._BG_SEMAPHORE_WAIT_S = 0.05
    yield
    query_executor._bg_semaphore = saved_sem
    query_executor._bg_semaphore_limit = saved_limit
    chc._max_background_db_concurrency = saved_global
    query_executor._BG_SEMAPHORE_WAIT_S = saved_wait


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _noop_managed_transaction(_engine):
    """Fake inner managed_transaction: yields immediately without touching PG."""
    yield object()


# ---------------------------------------------------------------------------
# Saturation behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_saturated_bg_semaphore_raises_timeout_error():
    """When all bg slots are held, background_managed_transaction raises TimeoutError."""
    query_executor._bg_semaphore = None
    query_executor._bg_semaphore_limit = 0
    chc._max_background_db_concurrency = 1

    sem = await _get_bg_semaphore()
    await sem.acquire()  # manually hold the single slot

    with patch(
        "dynastore.modules.db_config.query_executor.managed_transaction",
        _noop_managed_transaction,
    ):
        with pytest.raises(asyncio.TimeoutError):
            async with background_managed_transaction(None):
                pass

    sem.release()


@pytest.mark.asyncio
async def test_saturated_bg_semaphore_logs_warning(caplog):
    """Semaphore saturation is reported as a WARNING log line."""
    query_executor._bg_semaphore = None
    query_executor._bg_semaphore_limit = 0
    chc._max_background_db_concurrency = 1

    sem = await _get_bg_semaphore()
    await sem.acquire()

    caplog.set_level(logging.WARNING)
    with patch(
        "dynastore.modules.db_config.query_executor.managed_transaction",
        _noop_managed_transaction,
    ):
        with pytest.raises(asyncio.TimeoutError):
            async with background_managed_transaction(None):
                pass

    sem.release()

    assert any(
        "background semaphore saturated" in r.message for r in caplog.records
    ), "expected a WARNING about bg semaphore saturation"


# ---------------------------------------------------------------------------
# Concurrency cap
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bg_concurrency_cap_respected():
    """At most max_background_db_concurrency tasks run the inner transaction at once."""
    cap = 2
    query_executor._bg_semaphore = None
    query_executor._bg_semaphore_limit = 0
    chc._max_background_db_concurrency = cap

    in_flight: list[int] = [0]
    peak: list[int] = [0]

    @asynccontextmanager
    async def _tracking_transaction(_engine):
        in_flight[0] += 1
        peak[0] = max(peak[0], in_flight[0])
        await asyncio.sleep(0.01)
        in_flight[0] -= 1
        yield object()

    async def _one_bg_call():
        with patch(
            "dynastore.modules.db_config.query_executor.managed_transaction",
            _tracking_transaction,
        ):
            async with background_managed_transaction(None):
                pass

    await asyncio.gather(*[asyncio.create_task(_one_bg_call()) for _ in range(6)])

    assert peak[0] <= cap, (
        f"Expected at most {cap} concurrent bg transactions; observed {peak[0]}"
    )


# ---------------------------------------------------------------------------
# Slot is released and reusable
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_slot_released_after_successful_call():
    """After a background_managed_transaction completes, the slot is released."""
    query_executor._bg_semaphore = None
    query_executor._bg_semaphore_limit = 0
    chc._max_background_db_concurrency = 1

    with patch(
        "dynastore.modules.db_config.query_executor.managed_transaction",
        _noop_managed_transaction,
    ):
        # First call — acquires + releases
        async with background_managed_transaction(None):
            pass

        # Second call — must succeed (slot was released)
        async with background_managed_transaction(None):
            pass


# ---------------------------------------------------------------------------
# Semaphore rebuild on limit change
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_semaphore_rebuilt_when_limit_changes():
    """A new semaphore is created when max_background_db_concurrency changes."""
    query_executor._bg_semaphore = None
    query_executor._bg_semaphore_limit = 0
    chc._max_background_db_concurrency = 2

    s1 = await _get_bg_semaphore()
    assert s1._value == 2

    chc._max_background_db_concurrency = 4
    s2 = await _get_bg_semaphore()
    assert s2._value == 4
    assert s2 is not s1, "semaphore must be rebuilt when the limit changes"
