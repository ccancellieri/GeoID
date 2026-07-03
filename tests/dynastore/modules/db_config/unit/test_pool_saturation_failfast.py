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

"""Unit tests for the general DB pool-acquire fail-fast path (#1894).

Covers the general (non-tile) foreground path shared by every route that
acquires a connection through ``managed_transaction(engine)``: when the
pool's bounded acquire wait (``DBConfig.pool_acquire_timeout``) elapses,
``engine.connect()`` raises a bare ``sqlalchemy.exc.TimeoutError``.
``_acquire_async_engine_connection`` now wraps that in a typed
``PoolSaturationError`` (carrying a hot-reloadable Retry-After hint) so the
HTTP boundary (``extensions/tools/exception_handlers.py``) can map it to a
clean 503 instead of letting it fall through as an opaque 500.

Also confirms the bound is not amplified by ``retry_on_transient_connect``
(a saturated pool must fail fast once, not retry-and-compound the wait), and
that the happy path (pool not saturated) is unaffected by the new branch.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Optional

import pytest
from sqlalchemy.exc import TimeoutError as SAPoolTimeoutError

from dynastore.modules.db_config.exceptions import PoolSaturationError
from dynastore.modules.db_config.query_executor import (
    _acquire_async_engine_connection,
)


class _FakeConn:
    def __init__(self) -> None:
        self.rollback_calls = 0
        self.closed = False

    async def rollback(self) -> None:
        self.rollback_calls += 1

    async def close(self) -> None:
        self.closed = True

    async def invalidate(self) -> None:
        pass


class _FakePool:
    """Duck-typed pool exposing the four stats methods
    ``_format_pool_stats`` reads (#2898)."""

    def __init__(self, *, size=10, checkedin=6, checkedout=4, overflow=0) -> None:
        self._size = size
        self._checkedin = checkedin
        self._checkedout = checkedout
        self._overflow = overflow

    def size(self):
        return self._size

    def checkedin(self):
        return self._checkedin

    def checkedout(self):
        return self._checkedout

    def overflow(self):
        return self._overflow


class _FakeEngine:
    """Minimal duck-typed engine: only ``connect()`` is exercised by
    ``_acquire_async_engine_connection``."""

    def __init__(self, *, raise_timeout: bool, conn: Optional[_FakeConn] = None) -> None:
        self._raise_timeout = raise_timeout
        self._conn = conn or _FakeConn()
        self.connect_calls = 0
        self.pool = _FakePool()

    async def connect(self):
        self.connect_calls += 1
        if self._raise_timeout:
            raise SAPoolTimeoutError(
                "QueuePool limit of size 10 overflow 0 reached, "
                "connection timed out, timeout 30.00"
            )
        return self._conn


@pytest.mark.asyncio
async def test_saturated_pool_raises_pool_saturation_error_not_retried(monkeypatch):
    """A saturated pool fails fast with a typed ``PoolSaturationError`` and
    is NOT retried by ``retry_on_transient_connect`` -- the raw
    ``sqlalchemy.exc.TimeoutError`` is deliberately absent from
    ``_TRANSIENT_CONNECT_EXCEPTIONS`` so the bounded wait is never amplified
    into a multi-attempt wedge."""
    from dynastore.modules.db_config import query_executor as qe

    async def _fast_sleep(_seconds):
        return None

    monkeypatch.setattr(qe.asyncio, "sleep", _fast_sleep, raising=True)

    engine = _FakeEngine(raise_timeout=True)

    with pytest.raises(PoolSaturationError) as exc_info:
        await _acquire_async_engine_connection(engine)

    assert engine.connect_calls == 1, "the acquire timeout must not be retried"
    assert exc_info.value.retry_after == 5  # ConnectionHealthConfig default
    assert isinstance(exc_info.value.original_exception, SAPoolTimeoutError)


@pytest.mark.asyncio
async def test_normal_path_unaffected_by_saturation_handling():
    """When the pool is not saturated, ``connect()`` succeeds normally and no
    ``PoolSaturationError`` is raised -- the new except branch is a no-op on
    the happy path."""
    conn = _FakeConn()
    engine = _FakeEngine(raise_timeout=False, conn=conn)

    result = await _acquire_async_engine_connection(engine)

    assert result is conn
    assert engine.connect_calls == 1
    assert conn.rollback_calls == 1  # reset-on-checkout hygiene still runs


@pytest.mark.asyncio
async def test_saturated_pool_warning_includes_pool_stats(monkeypatch, caplog):
    """The pool-saturation WARNING (#2898) carries occupancy stats
    (``checkedout=``/``overflow=``) so an operator can see how saturated the
    pool was from this one log line, without correlating a second source."""
    from dynastore.modules.db_config import query_executor as qe

    async def _fast_sleep(_seconds):
        return None

    monkeypatch.setattr(qe.asyncio, "sleep", _fast_sleep, raising=True)

    engine = _FakeEngine(raise_timeout=True)

    with caplog.at_level(logging.WARNING, logger=qe.logger.name):
        with pytest.raises(PoolSaturationError):
            await _acquire_async_engine_connection(engine)

    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert any("checkedout=" in m and "overflow=" in m for m in warnings), warnings


@pytest.mark.asyncio
async def test_slow_successful_acquire_warns_with_pool_stats(monkeypatch, caplog):
    """A successful acquire whose wait crosses
    ``ConnectionHealthConfig.pool_acquire_warn_seconds`` must emit a WARNING
    carrying pool stats (#2898), not just the routine INFO line. The
    threshold itself is monkeypatched (rather than the global stdlib clock)
    so a small, real ``asyncio.sleep`` reliably crosses it without touching
    process-wide timing used by other fixtures/plugins."""
    from dynastore.modules.db_config import query_executor as qe

    monkeypatch.setattr(qe, "resolve_pool_acquire_warn_seconds", lambda: 0.01)

    conn = _FakeConn()
    engine = _FakeEngine(raise_timeout=False, conn=conn)

    async def _slow_connect():
        engine.connect_calls += 1
        await asyncio.sleep(0.05)
        return conn

    engine.connect = _slow_connect

    with caplog.at_level(logging.WARNING, logger=qe.logger.name):
        result = await _acquire_async_engine_connection(engine)

    assert result is conn
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert any("checkedout=" in m and "overflow=" in m for m in warnings), warnings


@pytest.mark.asyncio
async def test_slow_successful_acquire_warns_using_static_threshold(monkeypatch, caplog):
    """``pool_acquire_warn_seconds`` is read via the static
    ``resolve_pool_acquire_warn_seconds()`` fallback, not the live
    ``ConnectionHealthConfig`` (#2908). Monkeypatching the config service's
    ``get_config`` to a value that would cross the threshold must have NO
    effect on whether the WARNING fires -- only the static resolver's return
    value controls it, proving the acquire path never awaits the config
    service on this branch."""
    from unittest.mock import AsyncMock, MagicMock

    from dynastore.modules.db_config import query_executor as qe

    monkeypatch.setattr(qe, "resolve_pool_acquire_warn_seconds", lambda: 0.01)

    # A live config read would return an unrelated (much higher) threshold;
    # if the acquire path erroneously awaited it, this WARNING would not
    # fire for a 0.05 s delay.
    config_mock = MagicMock()
    config_mock.get_config = AsyncMock(side_effect=AssertionError(
        "acquire path must not call get_config on the success branch"
    ))
    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol",
        lambda *_a, **_kw: config_mock,
    )

    conn = _FakeConn()
    engine = _FakeEngine(raise_timeout=False, conn=conn)

    async def _slow_connect():
        engine.connect_calls += 1
        await asyncio.sleep(0.05)
        return conn

    engine.connect = _slow_connect

    with caplog.at_level(logging.WARNING, logger=qe.logger.name):
        result = await _acquire_async_engine_connection(engine)

    assert result is conn
    config_mock.get_config.assert_not_awaited()
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert any("checkedout=" in m and "overflow=" in m for m in warnings), warnings


def test_acquire_success_path_never_reads_live_config():
    """Regression guard for the #2908 boot deadlock.

    ``_acquire_async_engine_connection`` runs on EVERY successful pool
    acquire, including the first one at cold boot. If this function ever
    calls ``get_config`` on that path again, a cold-cache ``get_config``
    call queries the DB through this same acquire function -- the outer
    call is still holding the central ``@cached`` wrapper's per-key
    ``asyncio.Lock``, so the inner ``get_config`` call awaits that same
    key's lock and the process hangs forever right after the pool is
    established (verified live on dev: deploy runs 28682367394,
    28683333131, 28684993353). Assert the source has no ``get_config(``
    call so nobody reintroduces a live config read here.
    """
    from dynastore.modules.db_config import query_executor as qe

    source = inspect.getsource(qe._acquire_async_engine_connection)
    assert "get_config(" not in source, (
        "_acquire_async_engine_connection must not call get_config() -- "
        "doing so re-enters the acquire path and deadlocks on cold boot (#2908)"
    )


@pytest.mark.asyncio
async def test_fast_successful_acquire_does_not_warn(caplog):
    """A fast acquire (well below the default pool_acquire_warn_seconds)
    must not emit a WARNING -- the WARN threshold is deliberately above the
    existing slow_pool_acquire_threshold_seconds INFO threshold, not a
    replacement for it."""
    from dynastore.modules.db_config import query_executor as qe

    conn = _FakeConn()
    engine = _FakeEngine(raise_timeout=False, conn=conn)

    with caplog.at_level(logging.WARNING, logger=qe.logger.name):
        result = await _acquire_async_engine_connection(engine)

    assert result is conn
    assert not [r for r in caplog.records if r.levelname == "WARNING"]
