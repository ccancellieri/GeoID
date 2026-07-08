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

"""Unit tests for the DDL framework primitives.

Covers the moving parts shipped during the v0.4.10–v0.4.17 series:

- ``split_ddl`` strips ``--`` line comments before splitting on ``;`` so a
  semicolon inside a comment doesn't produce a spurious statement.
- ``_infer_existence_check`` extracts ``(schema, table, trigger_name)`` from
  ``CREATE TRIGGER ... ON schema.table`` so per-relation triggers don't
  cross-match within a schema.
- ``check_trigger_exists`` accepts an optional ``table`` discriminator.
- ``DDLQuery.check_query`` callable accepts both ``()`` and ``(conn,)``.
- ``DDLBatch.execute`` short-circuits on a sentinel that returns ``True``
  and runs every step when it returns ``False``.
"""

from __future__ import annotations

import asyncio
import inspect
from typing import Any, List

import pytest


# ---------------------------------------------------------------------------
# split_ddl
# ---------------------------------------------------------------------------


def test_split_ddl_strips_line_comments_before_splitting():
    from dynastore.modules.db_config.query_executor import split_ddl

    out = split_ddl("-- comment; with semi\nCREATE TABLE a(id int);\nCREATE TABLE b(id int);")
    assert out == ["CREATE TABLE a(id int)", "CREATE TABLE b(id int)"]


def test_split_ddl_preserves_semicolons_inside_dollar_quoted_blocks():
    from dynastore.modules.db_config.query_executor import split_ddl

    sql = (
        "CREATE FUNCTION f() RETURNS void AS $$\n"
        "BEGIN\n"
        "  PERFORM 1; PERFORM 2;\n"
        "END;\n"
        "$$ LANGUAGE plpgsql;\n"
        "CREATE TABLE t(id int);"
    )
    out = split_ddl(sql)
    assert len(out) == 2
    assert "PERFORM 1; PERFORM 2;" in out[0]
    assert out[1].startswith("CREATE TABLE t")


def test_split_ddl_preserves_semicolons_inside_single_quoted_strings():
    from dynastore.modules.db_config.query_executor import split_ddl

    sql = "INSERT INTO t VALUES ('a;b;c'); CREATE TABLE u(id int);"
    out = split_ddl(sql)
    assert out == ["INSERT INTO t VALUES ('a;b;c')", "CREATE TABLE u(id int)"]


# ---------------------------------------------------------------------------
# _infer_existence_check — CREATE TRIGGER
# ---------------------------------------------------------------------------


def test_infer_trigger_check_extracts_schema_and_table_from_on_clause():
    from dynastore.modules.db_config.ddl_inference import _infer_existence_check

    check = _infer_existence_check(
        "CREATE TRIGGER my_trg AFTER INSERT ON myschema.mytable "
        "FOR EACH ROW EXECUTE FUNCTION myschema.notify();"
    )
    assert check is not None  # gate no longer requires a {placeholder}


def test_infer_trigger_check_supports_quoted_identifiers():
    from dynastore.modules.db_config.ddl_inference import _infer_existence_check

    check = _infer_existence_check(
        'CREATE TRIGGER my_trg AFTER INSERT ON "events"."events" '
        "EXECUTE FUNCTION events.notify();"
    )
    assert check is not None


def test_infer_create_table_partition_of_returns_none():
    """PARTITION OF must not get auto-inference (parent's check is enough)."""
    from dynastore.modules.db_config.ddl_inference import _infer_existence_check

    check = _infer_existence_check(
        "CREATE TABLE IF NOT EXISTS events.events_s0 "
        "PARTITION OF events.events FOR VALUES IN (0);"
    )
    assert check is None


def test_infer_drop_returns_none():
    from dynastore.modules.db_config.ddl_inference import _infer_existence_check

    assert _infer_existence_check("DROP TABLE foo;") is None
    assert _infer_existence_check("ALTER TABLE foo ADD COLUMN bar int;") is None


@pytest.mark.asyncio
async def test_infer_function_check_resolves_embedded_schema_placeholder(monkeypatch):
    from dynastore.modules.db_config.ddl_inference import (
        _infer_existence_check,
        clear_ddl_existence_cache,
    )

    clear_ddl_existence_cache()
    calls = []

    async def _fake_check_function_exists(conn, function_name, schema="platform"):
        calls.append((function_name, schema))
        return True

    monkeypatch.setattr(
        "dynastore.modules.db_config.locking_tools.check_function_exists",
        _fake_check_function_exists,
    )

    check = _infer_existence_check(
        'CREATE OR REPLACE FUNCTION "{schema}"."maintain_partitions_{schema}_tasks"() '
        "RETURNS void AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql;"
    )

    assert check is not None
    assert await check(object(), {}, {"schema": "tasks"}) is True
    assert calls == [("maintain_partitions_tasks_tasks", "tasks")]


# ---------------------------------------------------------------------------
# check_trigger_exists — table parameter
# ---------------------------------------------------------------------------


def test_check_trigger_exists_signature_has_table_kwarg():
    from dynastore.modules.db_config.locking_tools import check_trigger_exists

    sig = inspect.signature(check_trigger_exists)
    assert "table" in sig.parameters
    assert sig.parameters["table"].default is None  # backward-compatible


# ---------------------------------------------------------------------------
# DDLQuery.check_query callable contract
# ---------------------------------------------------------------------------


class _FakeConn:
    """Minimal stand-in. DDLQuery's _existence_check_impl only inspects the
    callable's signature and dispatches; it doesn't introspect the conn.
    """


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def test_ddlquery_accepts_zero_arg_check_query():
    from dynastore.modules.db_config.query_executor import DDLQuery

    calls: List[Any] = []

    def zero_arg():
        calls.append(("zero",))
        return True

    q = DDLQuery("CREATE TABLE IF NOT EXISTS t(id int);", check_query=zero_arg)
    fn = q._executor.existence_check  # the wrapped impl
    assert fn is not None
    assert _run(fn(_FakeConn(), {})) is True
    assert calls == [("zero",)]


def test_ddlquery_accepts_one_arg_check_query_with_conn():
    from dynastore.modules.db_config.query_executor import DDLQuery

    received: List[Any] = []

    def one_arg(conn):
        received.append(conn)
        return True

    q = DDLQuery("CREATE TABLE IF NOT EXISTS t(id int);", check_query=one_arg)
    sentinel_conn = _FakeConn()
    assert _run(q._executor.existence_check(sentinel_conn, {})) is True
    assert received == [sentinel_conn]


# ---------------------------------------------------------------------------
# DDLBatch — sentinel short-circuits or fans out
# ---------------------------------------------------------------------------


class _RecordingDDLQuery:
    """Stand-in that records execute() calls without touching a real DB."""

    def __init__(self, name: str, log: List[str], existence_check_returns: bool = False):
        self.name = name
        self.log = log
        self._executor = self
        self.existence_check = self._check
        self._raw_params: dict = {}

    def _check(self, *_a, **_kw):
        async def _ret():
            return self._existence_check_returns
        return _ret()

    async def _call_existence_check(self, _conn, _params):
        return self._existence_check_returns

    async def execute(self, _conn, **_kwargs):
        self.log.append(self.name)


def test_ddlbatch_short_circuits_when_sentinel_exists():
    from dynastore.modules.db_config.query_executor import DDLBatch

    log: List[str] = []
    sentinel = _RecordingDDLQuery("sentinel", log)
    sentinel._existence_check_returns = True

    s1 = _RecordingDDLQuery("s1", log)
    s2 = _RecordingDDLQuery("s2", log)

    batch = DDLBatch(sentinel=sentinel, steps=[s1, s2])
    _run(batch.execute(_FakeConn()))

    # Sentinel returned True → no step ran.
    assert log == []


def test_ddlbatch_runs_steps_when_sentinel_missing():
    from dynastore.modules.db_config.query_executor import DDLBatch

    log: List[str] = []
    sentinel = _RecordingDDLQuery("sentinel", log)
    sentinel._existence_check_returns = False

    s1 = _RecordingDDLQuery("s1", log)
    s2 = _RecordingDDLQuery("s2", log)

    batch = DDLBatch(sentinel=sentinel, steps=[s1, s2])
    _run(batch.execute(_FakeConn()))

    # Sentinel False → all steps executed in order.
    assert log == ["s1", "s2"]


# ---------------------------------------------------------------------------
# DDLQuery no longer accepts the dead lock_key kwarg (v0.4.16)
# ---------------------------------------------------------------------------


def test_ddlquery_rejects_lock_key_kwarg():
    from dynastore.modules.db_config.query_executor import DDLQuery

    with pytest.raises(TypeError):
        DDLQuery("CREATE TABLE IF NOT EXISTS t(id int);", lock_key="foo")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# DDLExecutor._call_existence_check_sync — async checks dispatched via
# run_in_event_loop so the sync path can use the same auto-inferred checks
# as the async path. Regression guard for the on_event_insert DuplicateObject
# crash where _execute_sync silently skipped async existence checks and
# proceeded to run a raw CREATE TRIGGER on a hot DB.
# ---------------------------------------------------------------------------


def test_call_existence_check_sync_dispatches_async_check():
    from dynastore.modules.db_config.query_executor import DDLExecutor, TemplateQueryBuilder

    invocations: List[Any] = []

    async def _async_check(conn, params, raw_params):
        invocations.append((conn, params, raw_params))
        return True

    _async_check._needs_raw_params = True  # type: ignore[attr-defined]

    executor = DDLExecutor(
        TemplateQueryBuilder("CREATE TABLE IF NOT EXISTS t(id int);"),
        existence_check=_async_check,
    )
    executor._raw_params = {"schema": "events"}

    fake_conn = _FakeConn()
    assert executor._call_existence_check_sync(fake_conn, {}) is True
    assert len(invocations) == 1
    assert invocations[0][0] is fake_conn
    assert invocations[0][2] == {"schema": "events"}


def test_call_existence_check_sync_returns_false_on_async_check_returning_false():
    from dynastore.modules.db_config.query_executor import DDLExecutor, TemplateQueryBuilder

    async def _async_check(conn, params):
        return False

    executor = DDLExecutor(
        TemplateQueryBuilder("CREATE TABLE IF NOT EXISTS t(id int);"),
        existence_check=_async_check,
    )
    executor._raw_params = {}

    assert executor._call_existence_check_sync(_FakeConn(), {}) is False


# ---------------------------------------------------------------------------
# retry_on_transient_connect — decorator-level coverage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_on_transient_connect_retries_then_succeeds(monkeypatch):
    """First call raises OperationalError, second returns. Decorator must
    retry once and surface the success value."""
    from dynastore.modules.db_config import query_executor as qe
    from sqlalchemy.exc import OperationalError

    async def _fast_sleep(_):
        return None

    monkeypatch.setattr(qe.asyncio, "sleep", _fast_sleep, raising=True)

    calls = {"n": 0}

    @qe.retry_on_transient_connect()
    async def _flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise OperationalError("x", {}, Exception("connection refused"))
        return "ok"

    assert await _flaky() == "ok"
    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_retry_on_transient_connect_does_not_retry_programming_error(monkeypatch):
    """ProgrammingError signals a real SQL bug — must propagate immediately."""
    from dynastore.modules.db_config import query_executor as qe
    from sqlalchemy.exc import ProgrammingError

    async def _fast_sleep(_):
        return None

    monkeypatch.setattr(qe.asyncio, "sleep", _fast_sleep, raising=True)

    calls = {"n": 0}

    @qe.retry_on_transient_connect()
    async def _broken():
        calls["n"] += 1
        raise ProgrammingError("CREATE TABLE bogus", {}, Exception("syntax error"))

    with pytest.raises(ProgrammingError):
        await _broken()
    assert calls["n"] == 1  # exactly one call, no retry


@pytest.mark.asyncio
async def test_retry_on_transient_connect_does_not_retry_integrity_error(monkeypatch):
    """IntegrityError is a real constraint violation — must propagate."""
    from dynastore.modules.db_config import query_executor as qe
    from sqlalchemy.exc import IntegrityError

    async def _fast_sleep(_):
        return None

    monkeypatch.setattr(qe.asyncio, "sleep", _fast_sleep, raising=True)

    calls = {"n": 0}

    @qe.retry_on_transient_connect()
    async def _violator():
        calls["n"] += 1
        raise IntegrityError("INSERT", {}, Exception("unique constraint"))

    with pytest.raises(IntegrityError):
        await _violator()
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_retry_on_transient_connect_exhausts_budget_and_raises(monkeypatch):
    """All 5 attempts fail → final exception propagates with original type
    and message preserved."""
    from dynastore.modules.db_config import query_executor as qe

    async def _fast_sleep(_):
        return None

    monkeypatch.setattr(qe.asyncio, "sleep", _fast_sleep, raising=True)

    calls = {"n": 0}

    @qe.retry_on_transient_connect(max_retries=5)
    async def _always_times_out():
        calls["n"] += 1
        raise asyncio.TimeoutError("connect timeout")

    with pytest.raises(asyncio.TimeoutError, match="connect timeout"):
        await _always_times_out()
    assert calls["n"] == 5


@pytest.mark.asyncio
async def test_retry_on_transient_connect_catches_oserror(monkeypatch):
    """OSError (e.g. ECONNREFUSED) must be in the retryable set so DB-still-warming
    doesn't crash the caller's lifespan."""
    from dynastore.modules.db_config import query_executor as qe

    async def _fast_sleep(_):
        return None

    monkeypatch.setattr(qe.asyncio, "sleep", _fast_sleep, raising=True)

    calls = {"n": 0}

    @qe.retry_on_transient_connect()
    async def _socket_blip():
        calls["n"] += 1
        if calls["n"] < 3:
            raise OSError(111, "Connection refused")
        return "recovered"

    assert await _socket_blip() == "recovered"
    assert calls["n"] == 3
