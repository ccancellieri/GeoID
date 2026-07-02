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


class _FakeEngine:
    """Minimal duck-typed engine: only ``connect()`` is exercised by
    ``_acquire_async_engine_connection``."""

    def __init__(self, *, raise_timeout: bool, conn: Optional[_FakeConn] = None) -> None:
        self._raise_timeout = raise_timeout
        self._conn = conn or _FakeConn()
        self.connect_calls = 0

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
