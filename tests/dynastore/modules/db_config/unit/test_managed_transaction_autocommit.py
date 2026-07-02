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

"""Tests for AUTOCOMMIT connection handling in managed_transaction.

AUTOCOMMIT connections have no active PostgreSQL transaction.
managed_transaction must yield them as-is without calling begin() or
begin_nested() — calling begin() on a connection that already autobegan
raises a SQLAlchemy double-begin error ("This connection has already
initialized a SQLAlchemy Transaction() object via begin() or autobegin;
can't call begin() here unless rollback() or commit() is called first").

GcpLivenessReconciler passes ctx.lock_connection (an AUTOCOMMIT connection
under a leader-election backend that pins one) into select_lapsed_gcp_tasks
→ managed_transaction, where the old begin() call caused the recurring
double-begin error logged as managed_transaction_autocommit_detected. Refs
#2438 / #1894.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import List

import pytest
from sqlalchemy.exc import InvalidRequestError


class _FakeTx:
    """Fake transaction context manager."""

    def __init__(self, log: List[str], name: str = "tx"):
        self._log = log
        self._name = name
        self.committed = False
        self.rolled_back = False

    async def __aenter__(self):
        self._log.append(f"{self._name}_enter")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            self._log.append(f"{self._name}_exit_commit")
            self.committed = True
        else:
            self._log.append(f"{self._name}_exit_rollback")
            self.rolled_back = True
        return False

    async def commit(self):
        self._log.append(f"{self._name}_commit")
        self.committed = True

    async def rollback(self):
        self._log.append(f"{self._name}_rollback")
        self.rolled_back = True


class _FakeAutocommitConn:
    """Fake connection with AUTOCOMMIT isolation level.

    begin() raises the SQLAlchemy double-begin error that was the root cause
    of the recurring GcpLivenessReconciler log spam (#2438). After the fix,
    managed_transaction must never call begin() on an AUTOCOMMIT connection.
    """

    def __init__(self, log: List[str]):
        self._log = log
        self._in_transaction = False
        self._execution_options = {"isolation_level": "AUTOCOMMIT"}
        self.sync_connection = None
        self.connection = None
        self.closed = False
        self.invalidated = False

    def in_transaction(self) -> bool:
        # SQLAlchemy autobegin may set this True even though AUTOCOMMIT
        # has no real transaction
        return self._in_transaction

    def begin(self):
        # Simulate the real SQLAlchemy error when begin() is called on a
        # connection that already autobegan (the root cause of #2438).
        self._log.append("begin_called")
        raise InvalidRequestError(
            "This connection has already initialized a SQLAlchemy Transaction() "
            "object via begin() or autobegin; can't call begin() here unless "
            "rollback() or commit() is called first"
        )

    async def begin_nested(self):
        # This would fail with NoActiveSQLTransactionError in real asyncpg
        self._log.append("begin_nested")
        raise Exception("SAVEPOINT can only be used in transaction blocks")

    @property
    def is_active(self) -> bool:
        return True


class _FakeRegularConn:
    """Fake regular connection (non-AUTOCOMMIT)."""

    def __init__(self, log: List[str]):
        self._log = log
        self._in_transaction = False
        self._execution_options = {}
        self.sync_connection = None
        self.connection = None
        self.closed = False
        self.invalidated = False

    def in_transaction(self) -> bool:
        return self._in_transaction

    def begin(self):
        self._in_transaction = True
        self._log.append("begin")
        return _FakeTx(self._log, "begin")

    async def begin_nested(self):
        self._in_transaction = True
        self._log.append("begin_nested")
        return _FakeTx(self._log, "nested")

    @property
    def is_active(self) -> bool:
        return True


@pytest.mark.asyncio
async def test_managed_transaction_autocommit_yields_conn_without_begin(monkeypatch):
    """AUTOCOMMIT connection is yielded as-is; begin() is never called.

    This is the regression test for #2438: GcpLivenessReconciler passed an
    AUTOCOMMIT advisory-lock connection into managed_transaction, which then
    called begin() — raising InvalidRequestError because the connection already
    had an autobegun transaction.

    The mock's begin() raises that same error so we can confirm it is never
    invoked even when in_transaction() returns True (the autobegin state that
    triggered the bug path).
    """
    from dynastore.modules.db_config import query_executor as qe

    monkeypatch.setattr(qe, "AsyncConnection", _FakeAutocommitConn, raising=False)
    monkeypatch.setattr(qe, "_get_wire_identity", lambda c: c, raising=True)

    log: List[str] = []
    conn = _FakeAutocommitConn(log)
    # Simulate autobegin state — in_transaction() returns True but there is no
    # real PG transaction; this was the state that triggered the bug.
    conn._in_transaction = True

    # Must not raise — begin() on this conn would raise InvalidRequestError
    async with qe.managed_transaction(conn) as managed_conn:
        assert managed_conn is conn
        log.append("body")

    assert "body" in log
    assert "begin_called" not in log, "begin() must not be called on AUTOCOMMIT connection"
    assert "begin_nested" not in log


@pytest.mark.asyncio
async def test_managed_transaction_autocommit_body_error_propagates(monkeypatch):
    """Body errors propagate cleanly from an AUTOCOMMIT-yielded connection."""
    from dynastore.modules.db_config import query_executor as qe

    monkeypatch.setattr(qe, "AsyncConnection", _FakeAutocommitConn, raising=False)
    monkeypatch.setattr(qe, "_get_wire_identity", lambda c: c, raising=True)

    log: List[str] = []
    conn = _FakeAutocommitConn(log)
    conn._in_transaction = True

    with pytest.raises(ValueError, match="test error"):
        async with qe.managed_transaction(conn):
            log.append("body_before_error")
            raise ValueError("test error")

    assert "body_before_error" in log
    assert "begin_called" not in log


@pytest.mark.asyncio
async def test_managed_transaction_regular_connection_nested(monkeypatch):
    """Test that managed_transaction uses begin_nested() for regular connections in transaction."""
    from dynastore.modules.db_config import query_executor as qe

    # Monkeypatch to bypass isinstance check and wire_identity
    monkeypatch.setattr(
        qe,
        "AsyncConnection",
        _FakeRegularConn,
        raising=False
    )
    monkeypatch.setattr(qe, "_get_wire_identity", lambda c: c, raising=True)

    log: List[str] = []
    conn = _FakeRegularConn(log)

    # Simulate connection already in transaction
    conn._in_transaction = True

    async with qe.managed_transaction(conn) as managed_conn:
        assert managed_conn is conn
        log.append("body")

    # Should use begin_nested() for nested transaction
    assert "begin_nested" in log
    assert "nested_commit" in log  # savepoint.commit() is called
    assert "body" in log


@pytest.mark.asyncio
async def test_managed_transaction_regular_connection_new(monkeypatch):
    """Test that managed_transaction uses begin() for regular connections not in transaction."""
    from dynastore.modules.db_config import query_executor as qe

    # Monkeypatch to bypass isinstance check and wire_identity
    monkeypatch.setattr(
        qe,
        "AsyncConnection",
        _FakeRegularConn,
        raising=False
    )
    monkeypatch.setattr(qe, "_get_wire_identity", lambda c: c, raising=True)

    log: List[str] = []
    conn = _FakeRegularConn(log)

    # Connection not in transaction
    conn._in_transaction = False

    async with qe.managed_transaction(conn) as managed_conn:
        assert managed_conn is conn
        log.append("body")

    # Should use begin() for new transaction
    assert "begin" in log
    assert "begin_enter" in log
    assert "begin_exit_commit" in log  # __aexit__ calls commit
    assert "begin_nested" not in log
    assert "body" in log


def test_is_autocommit_connection_detection():
    """Test _is_autocommit_connection helper function."""
    from dynastore.modules.db_config.query_executor import _is_autocommit_connection

    # AUTOCOMMIT connection
    autocommit_conn = SimpleNamespace()
    autocommit_conn._execution_options = {"isolation_level": "AUTOCOMMIT"}
    assert _is_autocommit_connection(autocommit_conn) is True

    # Regular connection
    regular_conn = SimpleNamespace()
    regular_conn._execution_options = {}
    assert _is_autocommit_connection(regular_conn) is False

    # Connection without execution_options
    no_opts_conn = SimpleNamespace()
    no_opts_conn._execution_options = None
    assert _is_autocommit_connection(no_opts_conn) is False

    # Sync connection with AUTOCOMMIT (via sync_connection)
    sync_conn = SimpleNamespace()
    sync_conn._execution_options = None
    sync_conn.sync_connection = SimpleNamespace()
    sync_conn.sync_connection._execution_options = {"isolation_level": "AUTOCOMMIT"}
    assert _is_autocommit_connection(sync_conn) is True


# ---------------------------------------------------------------------------
# Sync path coverage
#
# The async AUTOCOMMIT branch above is well covered, but managed_transaction
# has a separate *sync* branch (psycopg2 Connection / Session) taken by Cloud
# Run Jobs. It mirrors the async AUTOCOMMIT detection but with synchronous
# begin()/begin_nested(). These tests exercise that branch directly.
# ---------------------------------------------------------------------------


class _FakeSyncTx:
    """Fake synchronous transaction context manager."""

    def __init__(self, log: List[str], name: str = "tx"):
        self._log = log
        self._name = name
        self.committed = False
        self.rolled_back = False

    def __enter__(self):
        self._log.append(f"{self._name}_enter")
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self._log.append(f"{self._name}_exit_commit")
            self.committed = True
        else:
            self._log.append(f"{self._name}_exit_rollback")
            self.rolled_back = True
        return False

    def commit(self):
        self._log.append(f"{self._name}_commit")
        self.committed = True

    def rollback(self):
        self._log.append(f"{self._name}_rollback")
        self.rolled_back = True


class _FakeSyncConnBase:
    """Common base so monkeypatched ``SAConnection`` matches both fakes."""

    def __init__(self, log: List[str]):
        self._log = log
        self._in_transaction = False
        self._execution_options: dict = {}
        self.sync_connection = None
        self.connection = None
        self.closed = False
        self.invalidated = False

    def in_transaction(self) -> bool:
        return self._in_transaction

    def begin(self):
        self._in_transaction = True
        self._log.append("begin")
        return _FakeSyncTx(self._log, "begin")

    @property
    def is_active(self) -> bool:
        return True


class _FakeSyncAutocommitConn(_FakeSyncConnBase):
    """Fake sync AUTOCOMMIT connection whose begin() raises the double-begin error."""

    def __init__(self, log: List[str]):
        super().__init__(log)
        self._execution_options = {"isolation_level": "AUTOCOMMIT"}

    def begin(self):
        # Raise the same error the real SQLAlchemy raises on double-begin
        self._log.append("begin_called")
        from sqlalchemy.exc import InvalidRequestError as _IRE
        raise _IRE(
            "This connection has already initialized a SQLAlchemy Transaction() "
            "object via begin() or autobegin; can't call begin() here unless "
            "rollback() or commit() is called first"
        )

    def begin_nested(self):
        # Would raise NoActiveSQLTransactionError on a real AUTOCOMMIT conn.
        self._log.append("begin_nested")
        raise Exception("SAVEPOINT can only be used in transaction blocks")


class _FakeSyncRegularConn(_FakeSyncConnBase):
    """Fake sync connection (non-AUTOCOMMIT)."""

    def begin_nested(self):
        self._in_transaction = True
        self._log.append("begin_nested")
        return _FakeSyncTx(self._log, "nested")


@pytest.mark.asyncio
async def test_managed_transaction_sync_autocommit_yields_without_begin(monkeypatch):
    """Sync AUTOCOMMIT connection is yielded as-is; begin() is never called.

    Mirrors the async regression test: the sync branch had the same bug.
    """
    from dynastore.modules.db_config import query_executor as qe

    monkeypatch.setattr(qe, "SAConnection", _FakeSyncConnBase, raising=False)
    monkeypatch.setattr(qe, "_get_wire_identity", lambda c: c, raising=True)

    log: List[str] = []
    conn = _FakeSyncAutocommitConn(log)
    conn._in_transaction = True  # autobegin state that triggered the bug

    # Must not raise even though begin() would raise InvalidRequestError
    async with qe.managed_transaction(conn) as managed_conn:
        assert managed_conn is conn
        log.append("body")

    assert "body" in log
    assert "begin_called" not in log, "begin() must not be called on AUTOCOMMIT connection"
    assert "begin_nested" not in log


@pytest.mark.asyncio
async def test_managed_transaction_sync_autocommit_body_error_propagates(monkeypatch):
    """Body errors propagate cleanly from a sync AUTOCOMMIT-yielded connection."""
    from dynastore.modules.db_config import query_executor as qe

    monkeypatch.setattr(qe, "SAConnection", _FakeSyncConnBase, raising=False)
    monkeypatch.setattr(qe, "_get_wire_identity", lambda c: c, raising=True)

    log: List[str] = []
    conn = _FakeSyncAutocommitConn(log)
    conn._in_transaction = True

    with pytest.raises(ValueError, match="test error"):
        async with qe.managed_transaction(conn):
            log.append("body_before_error")
            raise ValueError("test error")

    assert "body_before_error" in log
    assert "begin_called" not in log


@pytest.mark.asyncio
async def test_managed_transaction_sync_regular_connection_nested(monkeypatch):
    """Sync regular connection already in a transaction uses begin_nested()."""
    from dynastore.modules.db_config import query_executor as qe

    monkeypatch.setattr(qe, "SAConnection", _FakeSyncConnBase, raising=False)
    monkeypatch.setattr(qe, "_get_wire_identity", lambda c: c, raising=True)

    log: List[str] = []
    conn = _FakeSyncRegularConn(log)
    conn._in_transaction = True

    async with qe.managed_transaction(conn) as managed_conn:
        assert managed_conn is conn
        log.append("body")

    assert "begin_nested" in log
    assert "nested_commit" in log
    assert "body" in log
