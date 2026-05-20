"""Regression test for GeoID#1113 — async pool poisoned by mid-transaction
connection loss.

Verifies that when a live connection is killed during a transaction
(simulating a DB restart / pg_terminate_backend), the connection is
hard-invalidated and evicted from the pool rather than being returned dirty.
The next request must succeed without a service restart.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

import asyncpg.exceptions as asyncpg_exc

from dynastore.modules.db_config.query_executor import (
    _acquire_async_engine_connection,
    _is_transient_asyncpg_error,
    managed_transaction,
)
from sqlalchemy.exc import PendingRollbackError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dead_conn_exc(msg: str = "connection was closed in the middle of operation"):
    """Real asyncpg.exceptions.ConnectionDoesNotExistError instance."""
    return asyncpg_exc.ConnectionDoesNotExistError(msg)


def _make_internal_client_exc(msg: str = "cannot switch to state"):
    """Real asyncpg.exceptions.InternalClientError instance."""
    return asyncpg_exc.InternalClientError(msg)


def _make_async_conn():
    """Return an AsyncConnection spec mock used by managed_transaction."""
    conn = MagicMock(spec=AsyncConnection)
    conn.closed = False
    conn.invalidated = False
    conn.in_transaction = MagicMock(return_value=False)
    conn.invalidate = AsyncMock()
    conn.close = AsyncMock()
    conn.rollback = AsyncMock()
    conn.execute = AsyncMock()

    # begin() returns a txn context-manager
    txn = MagicMock()
    txn.__aenter__ = AsyncMock(return_value=txn)
    txn.__aexit__ = AsyncMock(return_value=False)
    conn.begin = MagicMock(return_value=txn)

    return conn


def _make_async_engine(connect_side_effect=None):
    """Return an AsyncEngine spec mock."""
    engine = MagicMock(spec=AsyncEngine)
    if connect_side_effect is not None:
        engine.connect = connect_side_effect
    return engine


# ---------------------------------------------------------------------------
# Unit: _is_transient_asyncpg_error
# ---------------------------------------------------------------------------


def test_is_transient_by_class_name():
    exc = _make_dead_conn_exc()
    assert _is_transient_asyncpg_error(exc)


def test_is_transient_internal_client_by_class_name():
    exc = _make_internal_client_exc()
    assert _is_transient_asyncpg_error(exc)


def test_is_transient_by_message_fragment():
    exc = Exception("connection was closed mid-operation")
    assert _is_transient_asyncpg_error(exc)


def test_not_transient_for_generic():
    exc = ValueError("some other error")
    assert not _is_transient_asyncpg_error(exc)


# ---------------------------------------------------------------------------
# Unit: _acquire_async_engine_connection pool-hygiene path
#
# When a pooled connection is dead at checkout, the hygiene rollback raises
# an asyncpg exception (not caught by the old SA-only except clause).
# After the fix, the function must invalidate, close, and return a fresh
# healthy connection.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_acquire_invalidates_dead_connection_on_checkout():
    """If rollback raises ConnectionDoesNotExistError, the slot is invalidated
    and a fresh healthy connection is returned."""
    dead_conn = _make_async_conn()
    dead_conn.rollback = AsyncMock(side_effect=_make_dead_conn_exc())
    healthy_conn = _make_async_conn()

    call_count = {"n": 0}

    async def _connect():
        call_count["n"] += 1
        return dead_conn if call_count["n"] == 1 else healthy_conn

    engine = _make_async_engine(connect_side_effect=_connect)

    result = await _acquire_async_engine_connection(engine)

    assert call_count["n"] == 2
    dead_conn.invalidate.assert_awaited_once()
    dead_conn.close.assert_awaited_once()
    assert result is healthy_conn


@pytest.mark.asyncio
async def test_acquire_invalidates_sa_pending_rollback_on_checkout():
    """PendingRollbackError on hygiene rollback also invalidates and retries."""
    dead_conn = _make_async_conn()
    dead_conn.rollback = AsyncMock(side_effect=PendingRollbackError("dirty"))
    healthy_conn = _make_async_conn()

    call_count = {"n": 0}

    async def _connect():
        call_count["n"] += 1
        return dead_conn if call_count["n"] == 1 else healthy_conn

    engine = _make_async_engine(connect_side_effect=_connect)

    result = await _acquire_async_engine_connection(engine)

    assert call_count["n"] == 2
    dead_conn.invalidate.assert_awaited_once()
    assert result is healthy_conn


@pytest.mark.asyncio
async def test_acquire_invalidates_internal_client_exc_on_checkout():
    """InternalClientError (cannot switch to state N) on hygiene rollback
    also invalidates and retries."""
    dead_conn = _make_async_conn()
    dead_conn.rollback = AsyncMock(side_effect=_make_internal_client_exc())
    healthy_conn = _make_async_conn()

    call_count = {"n": 0}

    async def _connect():
        call_count["n"] += 1
        return dead_conn if call_count["n"] == 1 else healthy_conn

    engine = _make_async_engine(connect_side_effect=_connect)

    result = await _acquire_async_engine_connection(engine)

    assert call_count["n"] == 2
    dead_conn.invalidate.assert_awaited_once()
    assert result is healthy_conn


# ---------------------------------------------------------------------------
# Unit: managed_transaction invalidates on dead-wire mid-transaction error
#
# When ConnectionDoesNotExistError fires inside the ``yield`` body, the
# connection must be invalidated before close() so the pool evicts the slot
# rather than handing it out again in an aborted-transaction state.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_managed_transaction_invalidates_on_dead_wire():
    """A dead-wire asyncpg error during a transaction triggers invalidation
    before close, evicting the slot from the pool."""
    dead_exc = _make_dead_conn_exc()
    conn = _make_async_conn()

    engine = _make_async_engine()
    engine.connect = AsyncMock(return_value=conn)

    with pytest.raises(asyncpg_exc.ConnectionDoesNotExistError):
        async with managed_transaction(engine) as _tx_conn:
            raise dead_exc

    conn.invalidate.assert_awaited()
    conn.close.assert_awaited()


@pytest.mark.asyncio
async def test_managed_transaction_invalidates_on_sa_wrapped_dead_wire():
    """SA wraps asyncpg errors as DBAPIError(.orig=<asyncpg exc>).
    managed_transaction must detect the dead wire via the .orig attribute."""
    from sqlalchemy.exc import DBAPIError

    dead_asyncpg = _make_dead_conn_exc()
    wrapped = DBAPIError("SA-wrapped dead wire", None, dead_asyncpg)

    conn = _make_async_conn()
    engine = _make_async_engine()
    engine.connect = AsyncMock(return_value=conn)

    with pytest.raises(Exception):
        async with managed_transaction(engine) as _tx_conn:
            raise wrapped

    conn.invalidate.assert_awaited()
    conn.close.assert_awaited()


@pytest.mark.asyncio
async def test_managed_transaction_does_not_invalidate_on_normal_error():
    """Non-transient errors (e.g. ValueError) must NOT trigger invalidation —
    the connection is healthy and should be recycled normally."""
    conn = _make_async_conn()
    engine = _make_async_engine()
    engine.connect = AsyncMock(return_value=conn)

    with pytest.raises(ValueError):
        async with managed_transaction(engine) as _tx_conn:
            raise ValueError("application logic error")

    conn.invalidate.assert_not_awaited()
    conn.close.assert_awaited()
