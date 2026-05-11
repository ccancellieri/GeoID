"""Issue #486: assert two behavioural changes to ``query_executor``.

1. ``retry_on_transient_connect`` per-attempt retries log at DEBUG (was WARNING)
   so a wedged pool no longer floods Cloud Logging. Terminal exhaustion after
   ``max_retries`` keeps WARNING so real failures stay visible.
2. ``_acquire_async_engine_connection`` emits a structured ``db_pool_acquire``
   log line carrying ``wait_seconds``. INFO when slow (>= threshold), DEBUG
   otherwise. Feeds a GCP log-based metric without requiring a prometheus dep.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from unittest.mock import patch

import pytest

from dynastore.modules.db_config import query_executor
from dynastore.modules.db_config.query_executor import (
    _SLOW_POOL_ACQUIRE_THRESHOLD_S,
    _acquire_async_engine_connection,
    retry_on_transient_connect,
)


class _TransientOnce(Exception):
    """Subclass of one of the transient exception types so the decorator retries."""


# Force the decorator to recognise our exception by patching the tuple at runtime.
@pytest.fixture(autouse=True)
def _patch_transient_tuple():
    original = query_executor._TRANSIENT_CONNECT_EXCEPTIONS
    query_executor._TRANSIENT_CONNECT_EXCEPTIONS = (_TransientOnce,)
    try:
        yield
    finally:
        query_executor._TRANSIENT_CONNECT_EXCEPTIONS = original


class TestRetryLogDemote:
    def test_per_attempt_retry_logs_at_debug(self, caplog):
        calls = {"n": 0}

        @retry_on_transient_connect(
            max_retries=3, base_delay=0, max_delay=0, jitter=0
        )
        async def flaky() -> str:
            calls["n"] += 1
            if calls["n"] < 3:
                raise _TransientOnce("simulated transient")
            return "ok"

        caplog.set_level(logging.DEBUG, logger=query_executor.__name__)
        result = asyncio.run(flaky())

        assert result == "ok"
        retry_records = [
            r for r in caplog.records
            if "retry_on_transient_connect" in r.getMessage()
            and "exhausted" not in r.getMessage()
        ]
        assert retry_records, "expected at least one per-attempt retry log"
        for rec in retry_records:
            assert rec.levelno == logging.DEBUG, (
                f"per-attempt retry should be DEBUG, got "
                f"{logging.getLevelName(rec.levelno)}: {rec.getMessage()}"
            )

    def test_terminal_exhaustion_still_warns(self, caplog):
        @retry_on_transient_connect(
            max_retries=2, base_delay=0, max_delay=0, jitter=0
        )
        async def always_fails() -> None:
            raise _TransientOnce("doomed")

        caplog.set_level(logging.DEBUG, logger=query_executor.__name__)
        with pytest.raises(_TransientOnce):
            asyncio.run(always_fails())

        exhausted = [
            r for r in caplog.records
            if "exhausted" in r.getMessage()
        ]
        assert len(exhausted) == 1
        assert exhausted[0].levelno == logging.WARNING


class _FakeEngine:
    def __init__(self, delay: float = 0.0, raises: BaseException | None = None) -> None:
        self._delay = delay
        self._raises = raises

    async def connect(self) -> Any:
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._raises is not None:
            raise self._raises

        class _FakeConn:
            async def rollback(self_inner) -> None:
                return None

            async def close(self_inner) -> None:
                return None

            async def invalidate(self_inner) -> None:
                return None

        return _FakeConn()


class TestPoolWaitMetric:
    def test_fast_acquire_logs_debug(self, caplog):
        engine = _FakeEngine(delay=0)
        caplog.set_level(logging.DEBUG, logger=query_executor.__name__)
        asyncio.run(_acquire_async_engine_connection(engine))  # type: ignore[arg-type]

        acquires = [
            r for r in caplog.records
            if r.getMessage().startswith("db_pool_acquire ")
            and "slow" not in r.getMessage()
            and "failed" not in r.getMessage()
        ]
        assert acquires, "expected a db_pool_acquire DEBUG record"
        assert all(r.levelno == logging.DEBUG for r in acquires)

    def test_slow_acquire_logs_info(self, caplog):
        engine = _FakeEngine(delay=_SLOW_POOL_ACQUIRE_THRESHOLD_S + 0.05)
        caplog.set_level(logging.DEBUG, logger=query_executor.__name__)
        asyncio.run(_acquire_async_engine_connection(engine))  # type: ignore[arg-type]

        slow = [
            r for r in caplog.records
            if "db_pool_acquire slow" in r.getMessage()
        ]
        assert len(slow) == 1
        assert slow[0].levelno == logging.INFO
        assert "wait_seconds=" in slow[0].getMessage()

    def test_failed_acquire_logs_info_with_wait(self, caplog):
        # Use OSError — guaranteed in _TRANSIENT_CONNECT_EXCEPTIONS originally,
        # but we patched that tuple to (_TransientOnce,). Patch it back inline
        # so the decorator does NOT retry and the failure surfaces immediately.
        with patch.object(
            query_executor, "_TRANSIENT_CONNECT_EXCEPTIONS", (RuntimeError,)
        ):
            engine = _FakeEngine(raises=ValueError("boom"))
            caplog.set_level(logging.DEBUG, logger=query_executor.__name__)
            with pytest.raises(ValueError):
                asyncio.run(_acquire_async_engine_connection(engine))  # type: ignore[arg-type]

        failed = [
            r for r in caplog.records
            if "db_pool_acquire failed" in r.getMessage()
        ]
        assert len(failed) == 1
        assert failed[0].levelno == logging.INFO
        assert "wait_seconds=" in failed[0].getMessage()
