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

"""Unit tests for the pool-pressure-aware retry semaphore.

Covers:
- Concurrent retries never exceed the configured cap.
- The happy path (no transient failure) never touches the semaphore.
- The saturation log is emitted when the semaphore is full.
- The semaphore is rebuilt when the configured limit changes (live resize).
- Module-global and semaphore state is restored after each test.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

import dynastore.modules.db_config.connection_health_config as chc
from dynastore.modules.db_config import query_executor
from dynastore.modules.db_config.query_executor import (
    _get_retry_semaphore,
    retry_on_transient_connect,
)


class _TransientErr(Exception):
    """A synthetic transient error recognised by the retry decorator."""


@pytest.fixture(autouse=True)
def _reset_semaphore_and_globals():
    """Restore module-level semaphore, limit tracker, and config globals after each test."""
    saved_sem = query_executor._retry_semaphore
    saved_sem_limit = query_executor._retry_semaphore_limit
    saved_limit = chc._max_concurrent_connection_retries
    saved_exceptions = query_executor._TRANSIENT_CONNECT_EXCEPTIONS
    # Patch the exception tuple so _TransientErr is retried by the decorator.
    query_executor._TRANSIENT_CONNECT_EXCEPTIONS = (_TransientErr,)
    yield
    query_executor._retry_semaphore = saved_sem
    query_executor._retry_semaphore_limit = saved_sem_limit
    chc._max_concurrent_connection_retries = saved_limit
    query_executor._TRANSIENT_CONNECT_EXCEPTIONS = saved_exceptions


class TestSemaphoreBuiltLazily:
    @pytest.mark.asyncio
    async def test_semaphore_uses_configured_limit(self):
        """_get_retry_semaphore() sizes itself from the module global fallback."""
        query_executor._retry_semaphore = None
        query_executor._retry_semaphore_limit = 0
        chc._max_concurrent_connection_retries = 5
        sem = await _get_retry_semaphore()
        assert sem._value == 5

    @pytest.mark.asyncio
    async def test_semaphore_is_reused_on_subsequent_calls(self):
        """Calling _get_retry_semaphore() twice with the same limit returns the same object."""
        query_executor._retry_semaphore = None
        query_executor._retry_semaphore_limit = 0
        chc._max_concurrent_connection_retries = 2
        s1 = await _get_retry_semaphore()
        s2 = await _get_retry_semaphore()
        assert s1 is s2

    def test_semaphore_not_created_for_happy_path(self):
        """A function that always succeeds never builds the semaphore."""
        query_executor._retry_semaphore = None
        query_executor._retry_semaphore_limit = 0

        @retry_on_transient_connect(max_retries=3, base_delay=0, max_delay=0, jitter=0)
        async def always_ok():
            return "ok"

        result = asyncio.run(always_ok())
        assert result == "ok"
        # Semaphore was never needed; module global remains None.
        assert query_executor._retry_semaphore is None


class TestSemaphoreResizeOnChange:
    """Semaphore is rebuilt when the configured limit changes (live resize)."""

    @pytest.mark.asyncio
    async def test_semaphore_rebuilt_when_limit_changes(self):
        """When the fallback limit changes the holder replaces the semaphore."""
        query_executor._retry_semaphore = None
        query_executor._retry_semaphore_limit = 0
        chc._max_concurrent_connection_retries = 3
        s1 = await _get_retry_semaphore()
        assert s1._value == 3

        # Operator changes the limit.
        chc._max_concurrent_connection_retries = 7
        s2 = await _get_retry_semaphore()
        assert s2._value == 7
        assert s2 is not s1, "a new semaphore must be built when the limit changes"

    @pytest.mark.asyncio
    async def test_semaphore_not_rebuilt_when_limit_unchanged(self):
        """No rebuild when the configured limit matches the current semaphore."""
        query_executor._retry_semaphore = None
        query_executor._retry_semaphore_limit = 0
        chc._max_concurrent_connection_retries = 4
        s1 = await _get_retry_semaphore()
        s2 = await _get_retry_semaphore()
        assert s1 is s2


class TestConcurrentRetryCap:
    """Concurrent retries never exceed the configured semaphore limit."""

    @pytest.mark.asyncio
    async def test_concurrent_retries_capped_at_limit(self):
        """With cap=2 and 10 concurrent callers each failing once, the maximum
        number of simultaneous in-flight retry calls never exceeds 2."""
        cap = 2
        query_executor._retry_semaphore = None
        query_executor._retry_semaphore_limit = 0
        chc._max_concurrent_connection_retries = cap

        in_flight: list[int] = [0]
        peak: list[int] = [0]

        async def _flaky_once(fail_first: list[bool]) -> str:
            if fail_first[0]:
                fail_first[0] = False
                raise _TransientErr("simulated transient")
            # Track concurrency during the retry call body.
            in_flight[0] += 1
            peak[0] = max(peak[0], in_flight[0])
            await asyncio.sleep(0)  # yield to let other tasks run
            in_flight[0] -= 1
            return "done"

        tasks = []
        for _ in range(10):
            fail_flag = [True]

            @retry_on_transient_connect(max_retries=5, base_delay=0, max_delay=0, jitter=0)
            async def _call(_ff=fail_flag) -> str:
                return await _flaky_once(_ff)

            tasks.append(asyncio.create_task(_call()))

        results = await asyncio.gather(*tasks)
        assert all(r == "done" for r in results)
        assert peak[0] <= cap, (
            f"Expected at most {cap} concurrent retries, observed peak={peak[0]}"
        )

    @pytest.mark.asyncio
    async def test_happy_path_concurrency_not_restricted(self):
        """Tasks that never fail are never gated — all run concurrently."""
        cap = 1  # restrictive cap; must not affect the happy path
        # Set both semaphore and limit tracker so the resize holder doesn't rebuild.
        query_executor._retry_semaphore = asyncio.Semaphore(cap)
        query_executor._retry_semaphore_limit = cap
        chc._max_concurrent_connection_retries = cap

        in_flight: list[int] = [0]
        peak: list[int] = [0]

        @retry_on_transient_connect(max_retries=3, base_delay=0, max_delay=0, jitter=0)
        async def _always_ok() -> str:
            in_flight[0] += 1
            peak[0] = max(peak[0], in_flight[0])
            await asyncio.sleep(0)
            in_flight[0] -= 1
            return "ok"

        tasks = [asyncio.create_task(_always_ok()) for _ in range(5)]
        results = await asyncio.gather(*tasks)
        assert all(r == "ok" for r in results)
        # Happy-path calls are not gated — peak can reach 5 (all concurrent).
        assert peak[0] > cap, (
            "Happy-path tasks should run concurrently (not serialised by the gate)"
        )


class TestSaturationLog:
    """A structured log line is emitted when the semaphore is saturated."""

    @pytest.mark.asyncio
    async def test_saturation_logged_when_semaphore_full(self, caplog):
        """When the semaphore is at capacity (value==0), an INFO line is logged
        so a GCP log-based metric can alert on retry-storm conditions."""
        # Set cap to 1 so the second concurrent retrier sees a saturated semaphore.
        cap = 1
        query_executor._retry_semaphore = None
        query_executor._retry_semaphore_limit = 0
        chc._max_concurrent_connection_retries = cap

        gate_event = asyncio.Event()   # retrier-1 signals when it holds the slot
        wait_event = asyncio.Event()   # test signals retrier-1 to finish

        async def _slow_retry(hold: bool) -> str:
            if hold:
                # Retrier-1: acquire the semaphore and pause so retrier-2 queues.
                gate_event.set()
                await wait_event.wait()
            return "done"

        fail_1 = [True]
        fail_2 = [True]

        @retry_on_transient_connect(max_retries=3, base_delay=0, max_delay=0, jitter=0)
        async def _retrier_1() -> str:
            if fail_1[0]:
                fail_1[0] = False
                raise _TransientErr("fail1")
            return await _slow_retry(hold=True)

        @retry_on_transient_connect(max_retries=3, base_delay=0, max_delay=0, jitter=0)
        async def _retrier_2() -> str:
            if fail_2[0]:
                fail_2[0] = False
                raise _TransientErr("fail2")
            return await _slow_retry(hold=False)

        caplog.set_level(logging.INFO, logger=query_executor.__name__)

        async def _run():
            t1 = asyncio.create_task(_retrier_1())
            # Wait until retrier-1 holds the semaphore slot before launching retrier-2.
            await gate_event.wait()
            t2 = asyncio.create_task(_retrier_2())
            # Small yield so retrier-2 can attempt to acquire the saturated semaphore.
            await asyncio.sleep(0)
            # Release retrier-1.
            wait_event.set()
            return await asyncio.gather(t1, t2)

        results = await _run()
        assert results == ["done", "done"]

        saturated_logs = [
            r for r in caplog.records
            if "retry_on_transient_connect_semaphore_saturated" in r.getMessage()
        ]
        assert saturated_logs, "expected a saturation INFO log when semaphore was full"
        assert all(r.levelno == logging.INFO for r in saturated_logs)


class TestHappyPathNeverTouchesSemaphore:
    """A function that always succeeds never acquires or creates the semaphore."""

    def test_no_semaphore_created_on_success(self):
        query_executor._retry_semaphore = None
        query_executor._retry_semaphore_limit = 0

        @retry_on_transient_connect(max_retries=5, base_delay=0, max_delay=0, jitter=0)
        async def ok() -> str:
            return "ok"

        result = asyncio.run(ok())
        assert result == "ok"
        assert query_executor._retry_semaphore is None, (
            "Semaphore must not be built on the happy path"
        )

    def test_existing_tests_unaffected(self):
        """Single-threaded retry test — semaphore has plenty of capacity, no blocking."""
        calls: list[int] = [0]

        @retry_on_transient_connect(max_retries=4, base_delay=0, max_delay=0, jitter=0)
        async def flaky() -> str:
            calls[0] += 1
            if calls[0] < 3:
                raise _TransientErr("simulated")
            return "recovered"

        result = asyncio.run(flaky())
        assert result == "recovered"
        assert calls[0] == 3
