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

"""Unit tests for the shared fail-fast bounded pool-acquire path (#2933).

``acquire_engine_connection_bounded`` gives a request path a live-configurable
deadline shorter than the engine's own ``pool_timeout``. It deliberately uses
a bare ``asyncio.wait_for`` (no ``asyncio.shield``): under genuine pool
saturation -- the scenario this guard exists for -- a checkout is just
waiting on the pool's internal FIFO queue, and cancelling that wait is clean
(the queue removes its own waiter immediately, no leak, no zombie). An
earlier version shielded the whole checkout to close a narrower, rarer leak
(cancelling mid-handshake while a brand new physical connection is being
created) but that traded it for an unbounded zombie pileup under sustained
saturation -- see the integration tests for the concrete repeated-timeout
regression check. This module covers the mocked, fast-running side:
``managed_transaction``'s ``acquire_timeout`` dispatch, the clean-cancel
property itself, and source-level pins confirming the tiles and STAC
item-GET/search call sites actually use the shared helper.
"""
from __future__ import annotations

import asyncio
import pathlib
from unittest.mock import AsyncMock, patch

import pytest

from sqlalchemy.ext.asyncio import AsyncEngine

from dynastore.modules.db_config.exceptions import PoolSaturationError
from dynastore.modules.db_config.query_executor import (
    acquire_engine_connection_bounded,
    managed_transaction,
)


class _FakeTxnCm:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self) -> None:
        self.closed = False

    def begin(self):
        return _FakeTxnCm()

    async def close(self) -> None:
        self.closed = True


class _FakeEngine(AsyncEngine):
    """AsyncEngine subclass so ``managed_transaction``'s isinstance check
    succeeds; instantiated via ``__new__`` to skip the real ``__init__``
    (which needs a live ``sync_engine``) since the acquire call itself is
    mocked in every test below."""


@pytest.mark.asyncio
async def test_managed_transaction_uses_bounded_acquire_when_timeout_given():
    """``acquire_timeout`` routes the checkout through
    ``acquire_engine_connection_bounded`` instead of the plain acquire."""
    from dynastore.modules.db_config import query_executor as qe

    conn = _FakeConn()
    with patch.object(
        qe, "acquire_engine_connection_bounded", AsyncMock(return_value=conn)
    ) as bounded_mock, patch.object(
        qe, "_acquire_async_engine_connection", AsyncMock(return_value=conn)
    ) as plain_mock:
        engine = _FakeEngine.__new__(_FakeEngine)
        async with managed_transaction(engine, acquire_timeout=2.5) as got:
            assert got is conn

        bounded_mock.assert_awaited_once_with(engine, 2.5)
        plain_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_managed_transaction_uses_plain_acquire_when_timeout_omitted():
    """Every pre-existing caller (``acquire_timeout`` omitted) is unaffected --
    still the plain, engine-``pool_timeout``-bounded acquire."""
    from dynastore.modules.db_config import query_executor as qe

    conn = _FakeConn()
    with patch.object(
        qe, "acquire_engine_connection_bounded", AsyncMock(return_value=conn)
    ) as bounded_mock, patch.object(
        qe, "_acquire_async_engine_connection", AsyncMock(return_value=conn)
    ) as plain_mock:
        engine = _FakeEngine.__new__(_FakeEngine)
        async with managed_transaction(engine) as got:
            assert got is conn

        plain_mock.assert_awaited_once_with(engine)
        bounded_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_bounded_acquire_raises_pool_saturation_on_timeout(monkeypatch):
    """A checkout that does not finish inside ``timeout_s`` surfaces the same
    typed ``PoolSaturationError`` the engine-native path raises, so existing
    HTTP-boundary mapping (503 + Retry-After) needs no changes."""
    from dynastore.modules.db_config import query_executor as qe

    async def _never_finishes(_engine):
        await asyncio.sleep(10)
        raise AssertionError("must not actually complete in this test")

    monkeypatch.setattr(qe, "_acquire_async_engine_connection", _never_finishes)

    with pytest.raises(PoolSaturationError) as exc_info:
        await acquire_engine_connection_bounded(engine=object(), timeout_s=0.05)  # type: ignore[arg-type]

    assert exc_info.value.retry_after == 5  # ConnectionHealthConfig default


@pytest.mark.asyncio
async def test_bounded_acquire_cancels_the_underlying_checkout_cleanly(monkeypatch):
    """The defining property of the (corrected, #2933) design: a fired
    deadline DOES cancel the in-flight checkout -- and does so immediately,
    leaving nothing running in the background. This is what keeps a
    saturated-pool checkout's cancellation clean (no zombie left registered
    in the pool's wait queue); see the integration tests for the
    sustained-saturation pileup check this property is required for."""
    from dynastore.modules.db_config import query_executor as qe

    cancelled = asyncio.Event()

    async def _slow_checkout(_engine):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        raise AssertionError("must not actually complete in this test")

    monkeypatch.setattr(qe, "_acquire_async_engine_connection", _slow_checkout)

    with pytest.raises(PoolSaturationError):
        await acquire_engine_connection_bounded(engine=object(), timeout_s=0.05)  # type: ignore[arg-type]

    # The cancellation must have already happened by the time our bounded
    # wait returns -- not something we need to wait around for separately.
    assert cancelled.is_set(), (
        "the checkout must be cancelled immediately when the caller's "
        "bounded wait times out, not left running in the background"
    )


def _read_source(*rel_parts: str) -> str:
    here = pathlib.Path(__file__).resolve()
    repo_root = here.parents[5]
    return repo_root.joinpath(*rel_parts).read_text(encoding="utf-8")


def test_tiles_service_uses_the_shared_bounded_acquire_not_raw_wait_for():
    """Pin: the tile-serving path must go through the shared, hygienic
    ``acquire_engine_connection_bounded`` (pool-hygiene + PoolSaturationError
    mapping) rather than the bare ``asyncio.wait_for(engine.connect(), ...)``
    it used before #2933."""
    source = _read_source(
        "packages", "extensions", "tiles", "src", "dynastore",
        "extensions", "tiles", "tiles_service.py",
    )
    assert "acquire_engine_connection_bounded(" in source
    assert "asyncio.wait_for(\n                    get_async_engine(request).connect()" not in source


def test_stac_item_get_and_search_use_bounded_fail_fast_acquire():
    """Pin: STAC item GET-by-id and catalog-scoped item search (GET + POST)
    pass ``acquire_timeout`` to ``managed_transaction`` so a saturated pool
    fails fast with 503 instead of queuing for the full engine pool_timeout
    (#2933)."""
    source = _read_source(
        "packages", "extensions", "stac", "src", "dynastore",
        "extensions", "stac", "stac_service.py",
    )
    assert source.count("acquire_timeout=await _read_live_fg_acquire_timeout()") == 3
