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

AUTOCOMMIT connections (used by pg_advisory_leadership for advisory locks)
have no active PostgreSQL transaction. Attempting begin_nested() (SAVEPOINT)
on such connections raises NoActiveSQLTransactionError.

The managed_transaction function must detect AUTOCOMMIT mode and use begin()
instead of begin_nested().
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import List

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection


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
    """Fake connection with AUTOCOMMIT isolation level."""

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
        self._in_transaction = True
        self._log.append("begin")
        return _FakeTx(self._log, "begin")

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
async def test_managed_transaction_autocommit_connection(monkeypatch):
    """Test that managed_transaction uses begin() for AUTOCOMMIT connections."""
    from dynastore.modules.db_config import query_executor as qe

    # Monkeypatch to bypass isinstance check and wire_identity
    monkeypatch.setattr(
        qe,
        "AsyncConnection",
        _FakeAutocommitConn,
        raising=False
    )
    monkeypatch.setattr(qe, "_get_wire_identity", lambda c: c, raising=True)

    log: List[str] = []
    conn = _FakeAutocommitConn(log)

    # Simulate connection that appears in_transaction (autobegin)
    conn._in_transaction = True

    async with qe.managed_transaction(conn) as managed_conn:
        assert managed_conn is conn
        log.append("body")

    # Should use begin(), not begin_nested()
    assert "begin" in log
    assert "begin_nested" not in log
    assert "begin_enter" in log
    assert "begin_exit_commit" in log  # __aexit__ calls commit
    assert "body" in log


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


@pytest.mark.asyncio
async def test_managed_transaction_autocommit_with_error(monkeypatch):
    """Test that AUTOCOMMIT transactions roll back on error."""
    from dynastore.modules.db_config import query_executor as qe

    # Monkeypatch to bypass isinstance check and wire_identity
    monkeypatch.setattr(
        qe,
        "AsyncConnection",
        _FakeAutocommitConn,
        raising=False
    )
    monkeypatch.setattr(qe, "_get_wire_identity", lambda c: c, raising=True)

    log: List[str] = []
    conn = _FakeAutocommitConn(log)
    conn._in_transaction = True

    with pytest.raises(ValueError, match="test error"):
        async with qe.managed_transaction(conn):
            log.append("body_before_error")
            raise ValueError("test error")

    # Should roll back the transaction
    assert "begin" in log
    assert "begin_enter" in log
    assert "begin_exit_rollback" in log  # __aexit__ with exception calls rollback
    assert "body_before_error" in log


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
