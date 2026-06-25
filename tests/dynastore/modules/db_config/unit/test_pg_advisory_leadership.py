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

"""pg_advisory_leadership — the shared leader-election context manager.

The load-bearing contract: the generator yields exactly once on every path.
A second yield (e.g. from an ``except`` handler after a failed unlock) makes
``contextlib`` raise ``RuntimeError: generator didn't stop`` and the leader
loop resigns every cycle — the regression these tests pin down.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

import dynastore.modules.db_config.locking_tools as locking_tools
from dynastore.modules.db_config.locking_tools import (
    _get_stable_lock_id,
    pg_advisory_leadership,
)


class _FakeConn:
    async def execution_options(self, **_kw):
        return self


class _FakeConnCtx:
    def __init__(self, conn, *, connect_exc: Exception | None = None) -> None:
        self._conn = conn
        self._connect_exc = connect_exc
        self.exited = False

    async def __aenter__(self):
        if self._connect_exc is not None:
            raise self._connect_exc
        return self._conn

    async def __aexit__(self, *exc_info):
        self.exited = True
        return False


def _make_engine(conn_ctx) -> AsyncEngine:
    engine = MagicMock(spec=AsyncEngine)
    engine.connect.return_value = conn_ctx
    return engine


def _make_fake_dql(
    *,
    try_lock_result: bool = True,
    try_lock_exc: Exception | None = None,
    unlock_exc: Exception | None = None,
):
    """Build a DQLQuery stand-in routing on the SQL text; records calls."""
    calls: list[tuple[str, dict]] = []

    class _FakeDQLQuery:
        def __init__(self, sql, result_handler=None) -> None:
            self._sql = sql

        async def execute(self, conn, **params):
            calls.append((self._sql, params))
            if "pg_try_advisory_lock" in self._sql:
                if try_lock_exc is not None:
                    raise try_lock_exc
                return try_lock_result
            if "pg_advisory_unlock" in self._sql:
                if unlock_exc is not None:
                    raise unlock_exc
                return None
            raise AssertionError(f"unexpected SQL: {self._sql}")

    return _FakeDQLQuery, calls


@pytest.mark.asyncio
async def test_leader_acquires_unlocks_and_closes_connection(monkeypatch):
    fake_dql, calls = _make_fake_dql(try_lock_result=True)
    monkeypatch.setattr(locking_tools, "DQLQuery", fake_dql)
    conn_ctx = _FakeConnCtx(_FakeConn())

    async with pg_advisory_leadership(
        _make_engine(conn_ctx), 42, name="test"
    ) as (is_leader, lock_conn):
        assert is_leader is True
        assert lock_conn is not None

    sqls = [sql for sql, _ in calls]
    assert any("pg_try_advisory_lock" in s for s in sqls)
    assert any("pg_advisory_unlock" in s for s in sqls)
    assert conn_ctx.exited is True


@pytest.mark.asyncio
async def test_unlock_failure_does_not_double_yield(monkeypatch):
    """Regression: a failed unlock must NOT surface as a second yield
    (``RuntimeError: generator didn't stop``). The dedicated connection is
    still closed, which releases the session lock server-side."""
    fake_dql, _calls = _make_fake_dql(
        try_lock_result=True, unlock_exc=ConnectionError("server closed")
    )
    monkeypatch.setattr(locking_tools, "DQLQuery", fake_dql)
    conn_ctx = _FakeConnCtx(_FakeConn())

    async with pg_advisory_leadership(
        _make_engine(conn_ctx), 42, name="test"
    ) as (is_leader, lock_conn):
        assert is_leader is True
        assert lock_conn is not None
    # reaching here without RuntimeError IS the assertion
    assert conn_ctx.exited is True


@pytest.mark.asyncio
async def test_lock_busy_yields_false_without_unlock(monkeypatch):
    fake_dql, calls = _make_fake_dql(try_lock_result=False)
    monkeypatch.setattr(locking_tools, "DQLQuery", fake_dql)
    conn_ctx = _FakeConnCtx(_FakeConn())

    async with pg_advisory_leadership(
        _make_engine(conn_ctx), 42, name="test"
    ) as (is_leader, lock_conn):
        assert is_leader is False
        assert lock_conn is None

    assert not any("pg_advisory_unlock" in sql for sql, _ in calls)
    assert conn_ctx.exited is True


@pytest.mark.asyncio
async def test_acquire_query_failure_yields_false(monkeypatch):
    fake_dql, _calls = _make_fake_dql(try_lock_exc=ConnectionError("boom"))
    monkeypatch.setattr(locking_tools, "DQLQuery", fake_dql)
    conn_ctx = _FakeConnCtx(_FakeConn())

    async with pg_advisory_leadership(
        _make_engine(conn_ctx), 42, name="test"
    ) as (is_leader, lock_conn):
        assert is_leader is False
        assert lock_conn is None
    assert conn_ctx.exited is True


@pytest.mark.asyncio
async def test_connect_failure_yields_false(monkeypatch):
    fake_dql, _calls = _make_fake_dql()
    monkeypatch.setattr(locking_tools, "DQLQuery", fake_dql)
    conn_ctx = _FakeConnCtx(_FakeConn(), connect_exc=ConnectionError("down"))

    async with pg_advisory_leadership(
        _make_engine(conn_ctx), 42, name="test"
    ) as (is_leader, lock_conn):
        assert is_leader is False
        assert lock_conn is None


@pytest.mark.asyncio
@pytest.mark.parametrize("engine", [None, object()])
async def test_non_async_engine_yields_false(engine):
    async with pg_advisory_leadership(engine, 42, name="test") as (is_leader, lock_conn):
        assert is_leader is False
        assert lock_conn is None


@pytest.mark.asyncio
async def test_body_exception_propagates_but_releases(monkeypatch):
    """A failure during the leadership tenure must propagate (so the loop
    resigns loudly) while unlock + connection close still run."""
    fake_dql, calls = _make_fake_dql(try_lock_result=True)
    monkeypatch.setattr(locking_tools, "DQLQuery", fake_dql)
    conn_ctx = _FakeConnCtx(_FakeConn())

    with pytest.raises(RuntimeError, match="tick failed"):
        async with pg_advisory_leadership(
            _make_engine(conn_ctx), 42, name="test"
        ) as (is_leader, lock_conn):
            assert is_leader is True
            assert lock_conn is not None
            raise RuntimeError("tick failed")

    assert any("pg_advisory_unlock" in sql for sql, _ in calls)
    assert conn_ctx.exited is True


@pytest.mark.asyncio
async def test_key_folding_int_passthrough_str_hashed(monkeypatch):
    fake_dql, calls = _make_fake_dql(try_lock_result=True)
    monkeypatch.setattr(locking_tools, "DQLQuery", fake_dql)

    async with pg_advisory_leadership(
        _make_engine(_FakeConnCtx(_FakeConn())), 0x4D41, name="test"
    ) as (_is_leader, _lock_conn):
        pass
    async with pg_advisory_leadership(
        _make_engine(_FakeConnCtx(_FakeConn())), "events_consumer", name="test"
    ) as (_is_leader, _lock_conn):
        pass

    lock_ids = [
        params["id"] for sql, params in calls if "pg_try_advisory_lock" in sql
    ]
    assert lock_ids[0] == 0x4D41
    assert lock_ids[1] == _get_stable_lock_id("events_consumer")
