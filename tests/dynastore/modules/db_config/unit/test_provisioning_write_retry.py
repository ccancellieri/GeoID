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

"""Unit tests for :func:`provisioning_write_with_retry` and the supporting predicates.

Covers:
- (a) Async path: transient closed-connection error, retry succeeds on fresh connection.
- (b) Sync path: transient closed-connection error, retry succeeds on fresh connection.
- (c) Lock-not-available error, retry with backoff.
- (d) Non-transient error, no retry, propagates immediately.
- (e) Predicate tests for ``is_transient_db_error``, ``_is_lock_not_available_error``,
     and ``_is_sync_closed_connection_error``.

No real DB is required — transaction managers are monkeypatched.
"""

from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from typing import List

import pytest
from sqlalchemy.exc import OperationalError as SAOperationalError

from dynastore.modules.db_config.query_executor import (
    _is_lock_not_available_error,
    _is_sync_closed_connection_error,
    is_transient_db_error,
    provisioning_write_with_retry,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_invalidated_op_error(msg: str = "server closed the connection") -> SAOperationalError:
    err = SAOperationalError(statement="SELECT 1", params={}, orig=Exception(msg))
    err.connection_invalidated = True  # type: ignore[attr-defined]
    return err


def _make_lock_timeout_asyncpg_error() -> Exception:
    """Simulate asyncpg LockNotAvailableError by class name and pgcode."""
    exc = type(
        "LockNotAvailableError",
        (Exception,),
        {"pgcode": "55P03", "sqlstate": "55P03"},
    )("canceling statement due to lock timeout")
    return exc


def _make_lock_timeout_sync_error() -> SAOperationalError:
    """Simulate a psycopg2 lock-timeout OperationalError (pgcode 55P03 on .orig)."""
    orig = Exception("canceling statement due to lock timeout")
    orig.pgcode = "55P03"  # type: ignore[attr-defined]
    err = SAOperationalError(statement="SELECT 1", params={}, orig=orig)
    return err


def _make_interface_error() -> Exception:
    """Simulate asyncpg InterfaceError (connection closed mid-operation)."""
    return type("InterfaceError", (Exception,), {})("connection was closed")


class _FakeSyncConn:
    """Minimal stand-in for a SQLAlchemy sync Connection."""

    def __init__(self, tag: str, log: List[str]):
        self.tag = tag
        self._log = log

    def in_transaction(self) -> bool:
        return False


class _FakeSyncEngine:
    """Stand-in for a SQLAlchemy sync Engine. Yields _FakeSyncConn from begin()."""

    def __init__(self, conns: List[_FakeSyncConn]):
        self._conns = list(conns)
        self.begin_calls = 0

    @contextmanager
    def begin(self):
        self.begin_calls += 1
        conn = self._conns.pop(0)
        yield conn


class _FakeAsyncConn:
    """Minimal stand-in for a SQLAlchemy AsyncConnection."""

    def __init__(self, tag: str, log: List[str]):
        self.tag = tag
        self._log = log
        self.closed = False

    async def invalidate(self):
        self._log.append(f"invalidate:{self.tag}")

    async def close(self):
        self.closed = True
        self._log.append(f"close:{self.tag}")

    def begin(self):
        return _FakeAsyncTx()

    def in_transaction(self) -> bool:
        return False


class _FakeAsyncTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeAsyncEngine:
    """Stand-in for a SQLAlchemy AsyncEngine. Yields _FakeAsyncConn from connect()."""

    def __init__(self, conns: List[_FakeAsyncConn]):
        self._conns = list(conns)
        self.connect_calls = 0

    async def connect(self):
        self.connect_calls += 1
        return self._conns.pop(0)


# ---------------------------------------------------------------------------
# Predicate tests
# ---------------------------------------------------------------------------


class TestIsTransientDbError:
    def test_asyncpg_interface_error_class_name(self):
        exc = _make_interface_error()
        assert is_transient_db_error(exc) is True

    def test_asyncpg_connection_does_not_exist(self):
        asyncpg = pytest.importorskip("asyncpg")
        exc = asyncpg.exceptions.ConnectionDoesNotExistError("conn gone")
        assert is_transient_db_error(exc) is True

    def test_lock_not_available_asyncpg_class_name(self):
        exc = _make_lock_timeout_asyncpg_error()
        assert is_transient_db_error(exc) is True

    def test_lock_not_available_pgcode_55p03_on_orig(self):
        exc = _make_lock_timeout_sync_error()
        assert is_transient_db_error(exc) is True

    def test_sync_connection_invalidated(self):
        exc = _make_invalidated_op_error()
        assert is_transient_db_error(exc) is True

    def test_sync_server_closed_message(self):
        err = SAOperationalError(
            statement="SELECT 1",
            params={},
            orig=Exception("server closed the connection"),
        )
        assert is_transient_db_error(err) is True

    def test_generic_operational_error_not_transient(self):
        err = SAOperationalError(
            statement="SELECT 1",
            params={},
            orig=Exception("syntax error near SELECT"),
        )
        assert is_transient_db_error(err) is False

    def test_integrity_error_not_transient(self):
        from sqlalchemy.exc import IntegrityError

        err = IntegrityError(statement="INSERT", params={}, orig=Exception("unique violation"))
        assert is_transient_db_error(err) is False

    def test_runtime_error_not_transient(self):
        assert is_transient_db_error(RuntimeError("boom")) is False

    def test_none_returns_false(self):
        assert is_transient_db_error(None) is False


class TestIsLockNotAvailableError:
    def test_class_name_match(self):
        exc = type("LockNotAvailableError", (Exception,), {})("lock timeout")
        assert _is_lock_not_available_error(exc) is True

    def test_pgcode_on_direct_exc(self):
        exc = Exception("lock")
        exc.pgcode = "55P03"  # type: ignore[attr-defined]
        assert _is_lock_not_available_error(exc) is True

    def test_pgcode_on_orig_chain(self):
        orig = Exception("lock")
        orig.pgcode = "55P03"  # type: ignore[attr-defined]
        wrapper = SAOperationalError(statement="X", params={}, orig=orig)
        assert _is_lock_not_available_error(wrapper) is True

    def test_message_fragment_match(self):
        exc = RuntimeError("canceling statement due to lock timeout")
        assert _is_lock_not_available_error(exc) is True

    def test_unrelated_exception_not_matched(self):
        assert _is_lock_not_available_error(RuntimeError("connection refused")) is False

    def test_none_returns_false(self):
        assert _is_lock_not_available_error(None) is False


class TestIsSyncClosedConnectionError:
    def test_connection_invalidated_flag(self):
        exc = _make_invalidated_op_error()
        assert _is_sync_closed_connection_error(exc) is True

    def test_server_closed_message(self):
        err = SAOperationalError(
            statement="X", params={}, orig=Exception("server closed the connection unexpectedly")
        )
        assert _is_sync_closed_connection_error(err) is True

    def test_connection_already_closed_message(self):
        err = SAOperationalError(
            statement="X", params={}, orig=Exception("connection already closed")
        )
        assert _is_sync_closed_connection_error(err) is True

    def test_non_operational_error_not_matched(self):
        assert _is_sync_closed_connection_error(RuntimeError("server closed")) is False

    def test_generic_operational_error_not_matched(self):
        err = SAOperationalError(
            statement="X", params={}, orig=Exception("could not translate host name")
        )
        assert _is_sync_closed_connection_error(err) is False


# ---------------------------------------------------------------------------
# provisioning_write_with_retry tests
# ---------------------------------------------------------------------------


async def _fast_sleep(_delay: float) -> None:
    """No-op sleep for tests — avoids real delays from backoff."""
    return None


@pytest.mark.asyncio
async def test_async_path_retries_on_transient_and_succeeds(monkeypatch):
    """(a) Async path: first attempt raises InterfaceError (connection closed);
    second attempt with a fresh connection succeeds."""
    import dynastore.modules.db_config.query_executor as qe

    call_log: List[str] = []

    @asynccontextmanager
    async def _fake_managed_transaction(engine):
        call_log.append("enter")
        if len(call_log) == 1:
            yield object()  # first conn — fn will raise
        else:
            yield object()  # second conn — fn succeeds

    monkeypatch.setattr(qe, "managed_transaction", _fake_managed_transaction)
    monkeypatch.setattr(qe.asyncio, "sleep", _fast_sleep)

    fn_calls = 0

    async def fn(conn):
        nonlocal fn_calls
        fn_calls += 1
        if fn_calls == 1:
            raise _make_interface_error()
        return "ok"

    result = await provisioning_write_with_retry(object(), fn, attempts=3)
    assert result == "ok"
    assert fn_calls == 2


@pytest.mark.asyncio
async def test_sync_path_retries_on_transient_and_succeeds(monkeypatch):
    """(b) Sync path: first managed_transaction raises OperationalError(connection_invalidated);
    second attempt with a fresh connection succeeds."""
    import dynastore.modules.db_config.query_executor as qe

    call_count = 0

    @asynccontextmanager
    async def _fake_managed_transaction(engine):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            err = _make_invalidated_op_error()
            raise err
        yield object()

    monkeypatch.setattr(qe, "managed_transaction", _fake_managed_transaction)
    monkeypatch.setattr(qe.asyncio, "sleep", _fast_sleep)

    fn_calls = 0

    async def fn(conn):
        nonlocal fn_calls
        fn_calls += 1
        return "synced"

    result = await provisioning_write_with_retry(object(), fn, attempts=3)
    assert result == "synced"
    assert call_count == 2
    assert fn_calls == 1  # only called on the successful attempt


@pytest.mark.asyncio
async def test_lock_not_available_retried_with_backoff(monkeypatch):
    """(c) LockNotAvailableError triggers retry with a non-zero backoff delay."""
    import dynastore.modules.db_config.query_executor as qe

    call_count = 0
    sleep_delays: List[float] = []

    async def _recording_sleep(delay: float) -> None:
        sleep_delays.append(delay)

    @asynccontextmanager
    async def _fake_managed_transaction(engine):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise _make_lock_timeout_asyncpg_error()
        yield object()

    monkeypatch.setattr(qe, "managed_transaction", _fake_managed_transaction)
    monkeypatch.setattr(qe.asyncio, "sleep", _recording_sleep)

    fn_calls = 0

    async def fn(conn):
        nonlocal fn_calls
        fn_calls += 1
        return "locked_then_ok"

    result = await provisioning_write_with_retry(object(), fn, attempts=3, lock_backoff=2.0)
    assert result == "locked_then_ok"
    assert call_count == 2
    # For lock errors, delay = lock_backoff * (attempt + 1) = 2.0 * 1 = 2.0
    assert sleep_delays == [2.0]


@pytest.mark.asyncio
async def test_non_transient_error_is_not_retried(monkeypatch):
    """(d) A non-transient error (ValueError, IntegrityError) propagates immediately
    without retry — the wrapper must not mask real bugs."""
    import dynastore.modules.db_config.query_executor as qe

    call_count = 0

    @asynccontextmanager
    async def _fake_managed_transaction(engine):
        nonlocal call_count
        call_count += 1
        yield object()

    monkeypatch.setattr(qe, "managed_transaction", _fake_managed_transaction)
    monkeypatch.setattr(qe.asyncio, "sleep", _fast_sleep)

    async def fn(conn):
        raise ValueError("non-transient bug")

    with pytest.raises(ValueError, match="non-transient bug"):
        await provisioning_write_with_retry(object(), fn, attempts=3)

    assert call_count == 1  # exactly one attempt; no retry


@pytest.mark.asyncio
async def test_exhausted_transient_retries_raises(monkeypatch):
    """Exhausting all attempts on a transient error re-raises the last exception."""
    import dynastore.modules.db_config.query_executor as qe

    call_count = 0

    @asynccontextmanager
    async def _fake_managed_transaction(engine):
        nonlocal call_count
        call_count += 1
        raise _make_invalidated_op_error()
        yield  # unreachable; makes this an asynccontextmanager

    monkeypatch.setattr(qe, "managed_transaction", _fake_managed_transaction)
    monkeypatch.setattr(qe.asyncio, "sleep", _fast_sleep)

    async def fn(conn):
        return "ok"

    with pytest.raises(SAOperationalError):
        await provisioning_write_with_retry(object(), fn, attempts=3)

    assert call_count == 3  # all attempts exhausted
