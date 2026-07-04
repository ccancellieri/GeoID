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

"""Unit tests for mid-flight read disconnect retry in BaseExecutor.

Verifies:
(a) Async DQL against a fake AsyncEngine whose FIRST acquired connection
    raises asyncpg.InterfaceError on .execute → second connection succeeds
    → result returned, exactly one retry, dead connection invalidated.
(b) Sync DQL equivalent.
(c) DDLExecutor (non-DQL, _retry_read_on_disconnect=False) under the same
    first-connection failure → does NOT retry, raises after the first failure.
(d) Retry exhaustion (async): both attempts fail → raises DatabaseConnectionError.
(e) Sync psycopg2 disconnect (OperationalError, connection_invalidated=True) is
    reclassified to DatabaseConnectionError and retried.
(f) Non-transient query error → no retry, no invalidate, raised as
    QueryExecutionError.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

# ---------------------------------------------------------------------------
# Module-level imports from the executor module under test
# ---------------------------------------------------------------------------

from dynastore.modules.db_config.query_executor import (
    DDLExecutor,
    DQLExecutor,
    DQLQuery,
    ResultHandler,
    TemplateQueryBuilder,
)
from dynastore.modules.db_config.connection_health_config import (
    resolve_read_disconnect_retry_attempts,
)
from dynastore.modules.db_config.exceptions import (
    DatabaseConnectionError,
    QueryExecutionError,
)

# asyncpg is a hard runtime dep of asyncpg-backed engines; available in CI.
from asyncpg.exceptions import InterfaceError as AsyncpgInterfaceError
from sqlalchemy.exc import OperationalError as SAOperationalError

# Real engine factories — creates the engine *object* (no socket opened)
# so isinstance(engine, AsyncEngine) / isinstance(engine, Engine) return True.
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.engine import Engine, create_engine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEAD_WIRE_MSG = "connection is closed"
_QUERY = "SELECT :sentinel"
_SENTINEL_VALUE = 42


def _make_good_async_conn(return_value: Any = _SENTINEL_VALUE) -> AsyncMock:
    """Return an async connection mock that succeeds on .execute."""
    mock_result = MagicMock()
    mock_result.scalar.return_value = return_value
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=mock_result)
    conn.in_transaction = MagicMock(return_value=False)
    conn.rollback = AsyncMock()
    conn.invalidate = AsyncMock()
    conn.close = AsyncMock()
    return conn


def _make_dead_async_conn() -> AsyncMock:
    """Return an async connection mock whose .execute raises asyncpg InterfaceError."""
    conn = AsyncMock()
    conn.execute = AsyncMock(side_effect=AsyncpgInterfaceError(_DEAD_WIRE_MSG))
    conn.in_transaction = MagicMock(return_value=False)
    conn.rollback = AsyncMock()
    conn.invalidate = AsyncMock()
    conn.close = AsyncMock()
    return conn


def _make_good_sync_conn(return_value: Any = _SENTINEL_VALUE) -> MagicMock:
    """Return a sync connection mock that succeeds on .execute."""
    mock_result = MagicMock()
    mock_result.scalar.return_value = return_value
    conn = MagicMock()
    conn.execute = MagicMock(return_value=mock_result)
    conn.in_transaction = MagicMock(return_value=False)
    conn.invalidate = MagicMock()
    conn.close = MagicMock()
    return conn


def _make_dead_sync_conn() -> MagicMock:
    """Return a sync connection mock whose .execute raises asyncpg InterfaceError.

    In production the sync path uses psycopg2, but the reclassification in
    _handle_db_exception covers the asyncpg path only.  To keep the test
    self-contained, we raise the same asyncpg error here — what matters is
    that the retry branch fires when DatabaseConnectionError is surfaced.
    """
    conn = MagicMock()
    conn.execute = MagicMock(side_effect=AsyncpgInterfaceError(_DEAD_WIRE_MSG))
    conn.in_transaction = MagicMock(return_value=False)
    conn.invalidate = MagicMock()
    conn.close = MagicMock()
    return conn


def _dql_query() -> DQLQuery:
    """Simple DQL query with a scalar result handler."""
    return DQLQuery(_QUERY, result_handler=ResultHandler.SCALAR)


# ---------------------------------------------------------------------------
# (a) Async DQL — retry on dead first connection, second succeeds
# ---------------------------------------------------------------------------


async def test_async_dql_retries_once_on_disconnect() -> None:
    """DQLExecutor retries exactly once when first async connection dies mid-flight."""
    engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/db")
    dead_conn = _make_dead_async_conn()
    good_conn = _make_good_async_conn(return_value=_SENTINEL_VALUE)

    acquire_calls: list[Any] = []

    async def _fake_acquire(eng: AsyncEngine) -> AsyncMock:
        acquire_calls.append(eng)
        return dead_conn if len(acquire_calls) == 1 else good_conn

    with patch(
        "dynastore.modules.db_config.query_executor._acquire_async_engine_connection",
        side_effect=_fake_acquire,
    ):
        result = await _dql_query().execute(engine, sentinel=0)

    # Result comes from the good second connection
    assert result == _SENTINEL_VALUE
    # Exactly one retry → two acquire calls
    assert len(acquire_calls) == 2
    # Dead connection was invalidated before closing
    dead_conn.invalidate.assert_called_once()
    dead_conn.close.assert_called_once()
    # Good connection was also closed (normal cleanup path)
    good_conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# (b) Sync DQL — retry on dead first connection, second succeeds
# ---------------------------------------------------------------------------


def test_sync_dql_retries_once_on_disconnect() -> None:
    """DQLExecutor (sync) retries exactly once when first connection dies mid-flight."""
    engine = create_engine("postgresql+psycopg2://user:pass@localhost/db")
    dead_conn = _make_dead_sync_conn()
    good_conn = _make_good_sync_conn(return_value=_SENTINEL_VALUE)

    connect_calls: list[Any] = []

    def _fake_connect() -> MagicMock:
        connect_calls.append(True)
        return dead_conn if len(connect_calls) == 1 else good_conn

    # Patch the engine's connect() method directly
    with patch.object(engine, "connect", side_effect=_fake_connect):
        result = DQLExecutor(
            TemplateQueryBuilder(_QUERY),
            result_handler=ResultHandler.SCALAR,
        )._execute_sync_workflow(engine, {"sentinel": 0})

    assert result == _SENTINEL_VALUE
    assert len(connect_calls) == 2
    dead_conn.invalidate.assert_called_once()
    dead_conn.close.assert_called_once()
    good_conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# (c) Write executor (retry disabled) — no retry on DatabaseConnectionError
#
# DDLExecutor's _execute_async wraps its body in managed_transaction(), which
# has complex savepoint/advisory-lock logic that doesn't work with plain mocks.
# To test the _retry_read_on_disconnect guard without fighting DDLExecutor
# internals, we take a DQLExecutor instance and override the flag to False —
# this is exactly how BaseExecutor (and DDLExecutor) behave: flag=False means
# the engine path does not retry on a mid-flight disconnect.
# ---------------------------------------------------------------------------


async def test_write_flag_false_does_not_retry() -> None:
    """_retry_read_on_disconnect=False → raises on first DatabaseConnectionError, no retry."""
    engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/db")
    dead_conn = _make_dead_async_conn()

    acquire_calls: list[Any] = []

    async def _fake_acquire(eng: AsyncEngine) -> AsyncMock:
        acquire_calls.append(eng)
        return dead_conn

    # Build a DQLExecutor with the retry guard disabled — mirrors BaseExecutor /
    # DDLExecutor behaviour for non-read executors without involving DDL internals.
    executor = DQLExecutor(
        TemplateQueryBuilder(_QUERY),
        result_handler=ResultHandler.SCALAR,
    )
    executor._retry_read_on_disconnect = False

    with patch(
        "dynastore.modules.db_config.query_executor._acquire_async_engine_connection",
        side_effect=_fake_acquire,
    ):
        with pytest.raises(DatabaseConnectionError):
            await executor(engine, sentinel=0)

    # Only one acquire call — no retry
    assert len(acquire_calls) == 1
    # Dead connection was still invalidated and closed (cleanup still runs)
    dead_conn.invalidate.assert_called_once()
    dead_conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# (d) Retry exhaustion (async) — both attempts fail → DatabaseConnectionError
# ---------------------------------------------------------------------------


async def test_async_dql_raises_after_all_attempts_exhausted() -> None:
    """When all configured attempts' connections die, DatabaseConnectionError is raised."""
    assert resolve_read_disconnect_retry_attempts() == 2, (
        "Test is written for 2 total attempts (the ConnectionHealthConfig "
        "default / module-global fallback); update if the default changes."
    )

    engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/db")
    # Both connections are dead
    dead_conn_1 = _make_dead_async_conn()
    dead_conn_2 = _make_dead_async_conn()

    acquire_calls: list[Any] = []

    async def _fake_acquire(eng: AsyncEngine) -> AsyncMock:
        acquire_calls.append(eng)
        return dead_conn_1 if len(acquire_calls) == 1 else dead_conn_2

    with patch(
        "dynastore.modules.db_config.query_executor._acquire_async_engine_connection",
        side_effect=_fake_acquire,
    ):
        with pytest.raises(DatabaseConnectionError):
            await _dql_query().execute(engine, sentinel=0)

    # Both attempts were made
    assert len(acquire_calls) == resolve_read_disconnect_retry_attempts()
    # Both dead connections were invalidated
    dead_conn_1.invalidate.assert_called_once()
    dead_conn_2.invalidate.assert_called_once()


# ---------------------------------------------------------------------------
# (e) Sync psycopg2 disconnect — real OperationalError(connection_invalidated)
#     is reclassified to DatabaseConnectionError and retried (reviewer Finding 1).
# ---------------------------------------------------------------------------


def test_sync_dql_retries_on_psycopg2_invalidated_connection() -> None:
    """A sync psycopg2 mid-flight disconnect (SQLAlchemy OperationalError with
    connection_invalidated=True) is reclassified to DatabaseConnectionError so
    the sync engine-path retry recovers it — proving the sync path is real, not
    just exercised via the asyncpg error class."""
    engine = create_engine("postgresql+psycopg2://user:pass@localhost/db")

    invalidated = SAOperationalError(
        "SELECT :sentinel",
        {"sentinel": 0},
        Exception("server closed the connection unexpectedly"),
    )
    invalidated.connection_invalidated = True

    dead_conn = MagicMock()
    dead_conn.execute = MagicMock(side_effect=invalidated)
    dead_conn.in_transaction = MagicMock(return_value=False)
    dead_conn.invalidate = MagicMock()
    dead_conn.close = MagicMock()
    good_conn = _make_good_sync_conn(return_value=_SENTINEL_VALUE)

    connect_calls: list[Any] = []

    def _fake_connect() -> MagicMock:
        connect_calls.append(True)
        return dead_conn if len(connect_calls) == 1 else good_conn

    with patch.object(engine, "connect", side_effect=_fake_connect):
        result = DQLExecutor(
            TemplateQueryBuilder(_QUERY),
            result_handler=ResultHandler.SCALAR,
        )._execute_sync_workflow(engine, {"sentinel": 0})

    assert result == _SENTINEL_VALUE
    assert len(connect_calls) == 2
    dead_conn.invalidate.assert_called_once()


# ---------------------------------------------------------------------------
# (f) Non-transient query error — NOT a disconnect → no retry, no invalidate,
#     re-raised as QueryExecutionError (reviewer Finding 3).
# ---------------------------------------------------------------------------


async def test_async_non_transient_error_does_not_retry() -> None:
    """A real query error (not a connection disconnect) surfaces as
    QueryExecutionError, is not retried, and the connection is closed WITHOUT
    being invalidated (it is still healthy)."""
    engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/db")

    conn = AsyncMock()
    conn.execute = AsyncMock(side_effect=ValueError("malformed query"))
    conn.in_transaction = MagicMock(return_value=False)
    conn.rollback = AsyncMock()
    conn.invalidate = AsyncMock()
    conn.close = AsyncMock()

    acquire_calls: list[Any] = []

    async def _fake_acquire(eng: AsyncEngine) -> AsyncMock:
        acquire_calls.append(eng)
        return conn

    with patch(
        "dynastore.modules.db_config.query_executor._acquire_async_engine_connection",
        side_effect=_fake_acquire,
    ):
        with pytest.raises(QueryExecutionError):
            await _dql_query().execute(engine, sentinel=0)

    # No retry — exactly one acquire
    assert len(acquire_calls) == 1
    # Healthy wire: closed but NOT invalidated (only dead wires are invalidated)
    conn.invalidate.assert_not_called()
    conn.close.assert_called_once()
