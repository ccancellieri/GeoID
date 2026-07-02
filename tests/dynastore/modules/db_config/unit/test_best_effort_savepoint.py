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

"""Tests for ``best_effort_savepoint`` (#2701 consolidation).

Five call sites (``events_emit``, ``storage_emit``, ``log_manager``,
``postgres_policy_storage``, ``lifecycle_manager``) hand-rolled the same
"wrap in a SAVEPOINT if the connection supports one, classify one expected
error, otherwise run unguarded" pattern. These tests pin the consolidated
helper's contract directly, independent of any of the five callers.

The helper drives ``begin_nested()`` manually (begin / commit / rollback)
rather than via ``async with``, because sync connections — the flavor every
job image uses — return a plain ``NestedTransaction`` that is not an async
context manager. The sync-fake tests below are the regression tests for
exactly that: the body must run, and failures must classify, on a
connection whose savepoint object has no ``__aenter__``/``__aexit__``.
"""

from __future__ import annotations

from typing import List

import pytest


class _FakeSyncSavepoint:
    """Sync ``NestedTransaction`` stand-in: plain ``rollback()``/``commit()``
    methods, NOT an async context manager — the job-image flavor."""

    def __init__(self, events: List[str], fail_commit: bool = False):
        self._events = events
        self._fail_commit = fail_commit

    def rollback(self) -> None:
        self._events.append("savepoint:rollback")

    def commit(self) -> None:
        if self._fail_commit:
            raise ValueError("RELEASE failed")
        self._events.append("savepoint:commit")


class _FakeSyncConn:
    """Fake sync connection: ``begin_nested()`` emits the SAVEPOINT eagerly
    and returns the transaction object directly."""

    def __init__(self, fail_commit: bool = False) -> None:
        self.events: List[str] = []
        self._fail_commit = fail_commit

    def begin_nested(self) -> _FakeSyncSavepoint:
        self.events.append("savepoint:begin")
        return _FakeSyncSavepoint(self.events, fail_commit=self._fail_commit)


class _FakeAsyncSavepoint:
    """Async ``AsyncTransaction`` stand-in: awaitable ``rollback()``/``commit()``."""

    def __init__(self, events: List[str]):
        self._events = events

    async def rollback(self) -> None:
        self._events.append("savepoint:rollback")

    async def commit(self) -> None:
        self._events.append("savepoint:commit")


class _FakeAsyncConn:
    """Fake async connection: ``begin_nested()`` returns a startable
    awaitable — awaiting it emits the SAVEPOINT, like SQLAlchemy's
    ``AsyncConnection``/``AsyncSession``."""

    def __init__(self) -> None:
        self.events: List[str] = []

    def begin_nested(self):
        async def _start() -> _FakeAsyncSavepoint:
            self.events.append("savepoint:begin")
            return _FakeAsyncSavepoint(self.events)

        return _start()


class _FakeConnNoSavepoint:
    """Fake connection with no ``begin_nested`` — the defensive fallback path."""

    def __init__(self) -> None:
        self.events: List[str] = []


class _FakeConnBrokenBeginNested:
    """``begin_nested()`` itself raises (aborted outer tx, driver quirk) —
    the body must still run, without SAVEPOINT isolation."""

    def __init__(self) -> None:
        self.events: List[str] = []

    def begin_nested(self):
        self.events.append("savepoint:begin-failed")
        raise RuntimeError("cannot open SAVEPOINT")


@pytest.mark.asyncio
async def test_happy_path_runs_inside_savepoint_and_commits():
    from dynastore.modules.db_config.query_executor import best_effort_savepoint

    conn = _FakeSyncConn()

    async with best_effort_savepoint(conn) as outcome:
        conn.events.append("body")

    assert outcome.error is None
    assert conn.events == [
        "savepoint:begin",
        "body",
        "savepoint:commit",
    ]


@pytest.mark.asyncio
async def test_default_tolerate_swallows_any_exception_and_rolls_back():
    from dynastore.modules.db_config.query_executor import best_effort_savepoint

    conn = _FakeSyncConn()

    async with best_effort_savepoint(conn) as outcome:
        conn.events.append("body")
        raise ValueError("boom")

    assert isinstance(outcome.error, ValueError)
    assert conn.events == [
        "savepoint:begin",
        "body",
        "savepoint:rollback",
    ]


@pytest.mark.asyncio
async def test_untolerated_exception_reraises_after_savepoint_rollback():
    from dynastore.modules.db_config.query_executor import best_effort_savepoint

    conn = _FakeSyncConn()

    with pytest.raises(RuntimeError, match="fatal"):
        async with best_effort_savepoint(conn, tolerate=lambda exc: False):
            conn.events.append("body")
            raise RuntimeError("fatal")

    # SAVEPOINT is still rolled back cleanly before the re-raise.
    assert conn.events == [
        "savepoint:begin",
        "body",
        "savepoint:rollback",
    ]


@pytest.mark.asyncio
async def test_tolerate_predicate_classifies_by_exception_type():
    """A ``tolerate`` predicate that only accepts one exception type re-raises
    everything else — the ``postgres_policy_storage`` duplicate-table shape."""
    from dynastore.modules.db_config.query_executor import best_effort_savepoint

    def _tolerate_value_error(exc: BaseException) -> bool:
        return isinstance(exc, ValueError)

    conn = _FakeSyncConn()

    async with best_effort_savepoint(conn, tolerate=_tolerate_value_error) as outcome:
        raise ValueError("tolerated")
    assert isinstance(outcome.error, ValueError)

    conn2 = _FakeSyncConn()
    with pytest.raises(RuntimeError, match="not tolerated"):
        async with best_effort_savepoint(conn2, tolerate=_tolerate_value_error):
            raise RuntimeError("not tolerated")


@pytest.mark.asyncio
async def test_async_flavor_happy_path_awaits_start_and_commit():
    """Async connections return a startable awaitable from ``begin_nested()``
    and awaitable ``rollback()``/``commit()`` — both must be awaited."""
    from dynastore.modules.db_config.query_executor import best_effort_savepoint

    conn = _FakeAsyncConn()

    async with best_effort_savepoint(conn) as outcome:
        conn.events.append("body")

    assert outcome.error is None
    assert conn.events == [
        "savepoint:begin",
        "body",
        "savepoint:commit",
    ]


@pytest.mark.asyncio
async def test_async_flavor_tolerated_failure_awaits_rollback():
    from dynastore.modules.db_config.query_executor import best_effort_savepoint

    conn = _FakeAsyncConn()

    async with best_effort_savepoint(conn) as outcome:
        conn.events.append("body")
        raise ValueError("boom")

    assert isinstance(outcome.error, ValueError)
    assert conn.events == [
        "savepoint:begin",
        "body",
        "savepoint:rollback",
    ]


@pytest.mark.asyncio
async def test_commit_failure_is_classified_by_tolerate():
    """A RELEASE-time failure is classified exactly like a body failure —
    the old ``async with`` exited through the same ``tolerate`` gate."""
    from dynastore.modules.db_config.query_executor import best_effort_savepoint

    conn = _FakeSyncConn(fail_commit=True)

    async with best_effort_savepoint(conn) as outcome:
        conn.events.append("body")

    assert isinstance(outcome.error, ValueError)

    conn2 = _FakeSyncConn(fail_commit=True)
    with pytest.raises(ValueError, match="RELEASE failed"):
        async with best_effort_savepoint(conn2, tolerate=lambda exc: False):
            conn2.events.append("body")


@pytest.mark.asyncio
async def test_no_begin_nested_runs_body_unguarded_but_still_classifies():
    """Connections without ``begin_nested`` (defensive fallback for
    engine-level resources) still run the body and still apply ``tolerate`` —
    the only thing lost is SAVEPOINT isolation, not the classification."""
    from dynastore.modules.db_config.query_executor import best_effort_savepoint

    conn = _FakeConnNoSavepoint()

    async with best_effort_savepoint(conn) as outcome:
        conn.events.append("body")
        raise ValueError("boom")

    assert isinstance(outcome.error, ValueError)
    assert conn.events == ["body"]  # no savepoint:begin/commit — none available


@pytest.mark.asyncio
async def test_no_begin_nested_untolerated_exception_reraises():
    from dynastore.modules.db_config.query_executor import best_effort_savepoint

    conn = _FakeConnNoSavepoint()

    with pytest.raises(RuntimeError, match="fatal"):
        async with best_effort_savepoint(conn, tolerate=lambda exc: False):
            raise RuntimeError("fatal")


@pytest.mark.asyncio
async def test_begin_nested_failure_still_runs_body():
    """Entering the SAVEPOINT failing must not raise out of the helper
    before the body runs (the 'generator didn't yield' shape) — the body
    executes unguarded and its outcome still classifies."""
    from dynastore.modules.db_config.query_executor import best_effort_savepoint

    conn = _FakeConnBrokenBeginNested()

    async with best_effort_savepoint(conn) as outcome:
        conn.events.append("body")
    assert outcome.error is None
    assert conn.events == ["savepoint:begin-failed", "body"]

    conn2 = _FakeConnBrokenBeginNested()
    async with best_effort_savepoint(conn2) as outcome2:
        raise ValueError("boom")
    assert isinstance(outcome2.error, ValueError)


@pytest.mark.asyncio
async def test_parent_transaction_still_usable_after_tolerated_failure():
    """The load-bearing invariant: after a tolerated inner failure, the
    caller's own connection is still usable for further statements — the
    SAVEPOINT rollback isolated the failure from the outer transaction."""
    from dynastore.modules.db_config.query_executor import best_effort_savepoint

    conn = _FakeSyncConn()

    async with best_effort_savepoint(conn) as outcome:
        raise ValueError("first attempt fails")
    assert outcome.error is not None

    # Same connection, a second best_effort_savepoint block succeeds cleanly —
    # proof the first failure didn't poison anything.
    async with best_effort_savepoint(conn) as outcome2:
        conn.events.append("second-attempt")
    assert outcome2.error is None
    assert conn.events == [
        "savepoint:begin",
        "savepoint:rollback",
        "savepoint:begin",
        "second-attempt",
        "savepoint:commit",
    ]


@pytest.mark.asyncio
async def test_real_sync_sqlite_savepoint_isolates_failure():
    """End-to-end on a real sync engine — the exact flavor that regressed:
    a sync ``NestedTransaction`` is not an async context manager, so the
    helper must drive it manually. The tolerated failure's writes roll back,
    the outer transaction stays healthy, and later writes commit."""
    from sqlalchemy import create_engine, event, text

    from dynastore.modules.db_config.query_executor import best_effort_savepoint

    engine = create_engine("sqlite://")

    # Standard pysqlite workaround: without it the driver never emits BEGIN
    # and SAVEPOINTs don't nest correctly (SQLAlchemy-documented recipe).
    @event.listens_for(engine, "connect")
    def _do_connect(dbapi_connection, connection_record):
        dbapi_connection.isolation_level = None

    @event.listens_for(engine, "begin")
    def _do_begin(conn):
        conn.exec_driver_sql("BEGIN")

    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text("CREATE TABLE t (id INTEGER)"))

            async with best_effort_savepoint(conn) as outcome:
                conn.execute(text("INSERT INTO t VALUES (1)"))
                raise ValueError("boom")
            assert isinstance(outcome.error, ValueError)

            # Outer transaction survived: further statements work…
            conn.execute(text("INSERT INTO t VALUES (2)"))

            # …and a second savepoint block on the same connection commits.
            async with best_effort_savepoint(conn) as outcome2:
                conn.execute(text("INSERT INTO t VALUES (3)"))
            assert outcome2.error is None

            rows = conn.execute(text("SELECT id FROM t ORDER BY id")).fetchall()
            assert [r[0] for r in rows] == [2, 3]
