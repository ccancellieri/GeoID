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

"""Unit tests for the shared startup-DDL lock-timeout tolerance helper
(``locking_tools.run_startup_ddl_tolerating_lock_timeout``).

A lock-timeout on a foundational module's startup advisory lock (PG 55P03,
"canceling statement due to lock timeout") must not abort process startup:
the guarded DDL is idempotent, so it's safe to re-run unlocked instead of
crash-looping the worker. Any other exception must still propagate.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.modules.db_config import locking_tools
from dynastore.modules.db_config.exceptions import QueryExecutionError
from dynastore.modules.db_config.query_executor import DDLQuery


class LockNotAvailableError(Exception):
    """Stand-in for asyncpg's real exception class of the same name.

    ``is_lock_not_available_error`` matches on this exact class name, so
    reusing it here exercises the same detection path production hits.
    """

    pgcode = "55P03"


class QueryCanceledError(Exception):
    """Stand-in for asyncpg's real exception class of the same name.

    ``is_statement_timeout_error`` matches on this exact class name (57014,
    "canceling statement due to statement timeout") -- the error a boot herd
    of peers re-running the same startup DDL can hit under #3121.
    """

    pgcode = "57014"


class UndefinedTableError(Exception):
    """Stand-in for an unrelated PG error (42P01, undefined_table)."""

    pgcode = "42P01"


def _make_raising_acquire_startup_lock(exc: BaseException):
    """Build a fake ``acquire_startup_lock`` that raises *exc* before yielding
    -- matching real behaviour when the advisory-lock wait fails.
    """

    @asynccontextmanager
    async def _fake(conn: Any, lock_key: str, timeout: str | None = None):
        raise exc
        yield None  # pragma: no cover - unreachable; keeps this a generator

    return _fake


@pytest.mark.asyncio
async def test_lock_timeout_falls_back_to_unlocked_ddl_instead_of_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A lock-timeout must not propagate: the DDL runs once, unlocked."""
    monkeypatch.setattr(
        locking_tools,
        "acquire_startup_lock",
        _make_raising_acquire_startup_lock(
            LockNotAvailableError("canceling statement due to lock timeout")
        ),
    )

    fake_conn = object()

    @asynccontextmanager
    async def _fake_managed_transaction(engine: Any):
        yield fake_conn

    monkeypatch.setattr(locking_tools, "managed_transaction", _fake_managed_transaction)

    seen_conns = []

    async def _ddl_body(conn: Any) -> None:
        from dynastore.modules.db_config import query_executor

        assert query_executor.startup_ddl_fallback_active() is True
        seen_conns.append(conn)

    # Must complete without raising despite the simulated lock timeout.
    await locking_tools.run_startup_ddl_tolerating_lock_timeout(
        engine=object(),
        lock_key="some_module_storage_init",
        ddl_body=_ddl_body,
    )

    # The idempotent DDL still ran exactly once, on the unlocked fallback
    # connection.
    assert seen_conns == [fake_conn]


@pytest.mark.asyncio
async def test_lock_timeout_inside_unlocked_fallback_is_tolerated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second lock-timeout while replaying fallback DDL must not abort startup."""
    monkeypatch.setattr(
        locking_tools,
        "acquire_startup_lock",
        _make_raising_acquire_startup_lock(
            LockNotAvailableError("canceling statement due to lock timeout")
        ),
    )

    fake_conn = object()

    @asynccontextmanager
    async def _fake_managed_transaction(engine: Any):
        yield fake_conn

    monkeypatch.setattr(locking_tools, "managed_transaction", _fake_managed_transaction)

    seen_conns = []

    async def _ddl_body(conn: Any) -> None:
        seen_conns.append(conn)
        raise LockNotAvailableError("canceling statement due to lock timeout")

    await locking_tools.run_startup_ddl_tolerating_lock_timeout(
        engine=object(),
        lock_key="some_module_storage_init",
        ddl_body=_ddl_body,
    )

    assert seen_conns == [fake_conn]


@pytest.mark.asyncio
async def test_inner_ddl_advisory_wait_is_skipped_inside_startup_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fallback startup DDL may proceed when the per-query advisory lock is busy."""
    from dynastore.modules.db_config import query_executor

    outer_conn = AsyncMock(spec=AsyncConnection)
    tx_conn = AsyncMock(spec=AsyncConnection)
    executed_sql: list[str] = []

    async def _execute(statement, params=None):
        sql = str(statement)
        executed_sql.append(sql)
        result = MagicMock()
        result.scalar.return_value = (
            False if "pg_try_advisory_xact_lock" in sql else None
        )
        return result

    tx_conn.execute = AsyncMock(side_effect=_execute)

    @asynccontextmanager
    async def _fake_managed_transaction(_conn):
        yield tx_conn

    monkeypatch.setattr(
        query_executor, "managed_transaction", _fake_managed_transaction
    )

    with query_executor.startup_ddl_unlocked_fallback_scope():
        await DDLQuery("DO $$ BEGIN NULL; END $$;").execute(outer_conn)

    assert any("pg_try_advisory_xact_lock" in sql for sql in executed_sql)
    assert not any(
        sql.startswith("SELECT pg_advisory_xact_lock") for sql in executed_sql
    )


@pytest.mark.asyncio
async def test_non_lock_ddl_error_still_propagates_inside_startup_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fallback scope only skips the nested wait; DDL execution errors still raise."""
    from dynastore.modules.db_config import query_executor

    outer_conn = AsyncMock(spec=AsyncConnection)
    tx_conn = AsyncMock(spec=AsyncConnection)

    async def _execute(statement, params=None):
        sql = str(statement)
        result = MagicMock()
        if "pg_try_advisory_xact_lock" in sql:
            result.scalar.return_value = False
            return result
        if sql.startswith("SET LOCAL"):
            result.scalar.return_value = None
            return result
        raise RuntimeError("bad ddl")

    tx_conn.execute = AsyncMock(side_effect=_execute)

    @asynccontextmanager
    async def _fake_managed_transaction(_conn):
        yield tx_conn

    monkeypatch.setattr(
        query_executor, "managed_transaction", _fake_managed_transaction
    )

    with pytest.raises(QueryExecutionError):
        with query_executor.startup_ddl_unlocked_fallback_scope():
            await DDLQuery("DO $$ BEGIN RAISE EXCEPTION 'bad'; END $$;").execute(
                outer_conn
            )


@pytest.mark.asyncio
async def test_non_lock_timeout_error_still_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A genuine (non-lock-timeout) failure must still abort -- this helper
    only tolerates the specific, known-safe lock-contention race.
    """
    monkeypatch.setattr(
        locking_tools,
        "acquire_startup_lock",
        _make_raising_acquire_startup_lock(ConnectionError("db is unreachable")),
    )

    async def _ddl_body(conn: Any) -> None:
        raise AssertionError(
            "ddl_body must not run when the error isn't a lock timeout"
        )

    with pytest.raises(ConnectionError, match="db is unreachable"):
        await locking_tools.run_startup_ddl_tolerating_lock_timeout(
            engine=object(),
            lock_key="some_module_storage_init",
            ddl_body=_ddl_body,
        )


@pytest.mark.asyncio
async def test_happy_path_runs_ddl_under_the_lock_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the lock is acquired normally, the DDL runs once on the locked
    connection and ``managed_transaction`` (the unlocked fallback) is never
    touched.
    """
    locked_conn = object()

    @asynccontextmanager
    async def _fake_acquire(conn: Any, lock_key: str, timeout: str | None = None):
        yield locked_conn

    monkeypatch.setattr(locking_tools, "acquire_startup_lock", _fake_acquire)

    async def _unexpected_managed_transaction(engine: Any):
        raise AssertionError("unlocked fallback must not run on the happy path")

    monkeypatch.setattr(
        locking_tools, "managed_transaction", _unexpected_managed_transaction
    )

    seen_conns = []

    async def _ddl_body(conn: Any) -> None:
        seen_conns.append(conn)

    await locking_tools.run_startup_ddl_tolerating_lock_timeout(
        engine=object(),
        lock_key="some_module_storage_init",
        ddl_body=_ddl_body,
    )

    assert seen_conns == [locked_conn]



# ---------------------------------------------------------------------------
# Statement-timeout cancellation (57014) -- #3121
# ---------------------------------------------------------------------------
#
# A boot herd (catalog + async-writer re-running startup DDL simultaneously
# after a shared-cause restart) can push a DDL statement past
# ``_DDL_STATEMENT_TIMEOUT`` even once its advisory lock is held. Mirrors
# the 55P03 cases above.


@pytest.mark.asyncio
async def test_statement_timeout_falls_back_to_unlocked_ddl_instead_of_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A statement-timeout cancellation must not propagate: the DDL runs
    once, unlocked."""
    monkeypatch.setattr(
        locking_tools,
        "acquire_startup_lock",
        _make_raising_acquire_startup_lock(
            QueryCanceledError("canceling statement due to statement timeout")
        ),
    )

    fake_conn = object()

    @asynccontextmanager
    async def _fake_managed_transaction(engine: Any):
        yield fake_conn

    monkeypatch.setattr(locking_tools, "managed_transaction", _fake_managed_transaction)

    seen_conns = []

    async def _ddl_body(conn: Any) -> None:
        from dynastore.modules.db_config import query_executor

        assert query_executor.startup_ddl_fallback_active() is True
        seen_conns.append(conn)

    await locking_tools.run_startup_ddl_tolerating_lock_timeout(
        engine=object(),
        lock_key="some_module_storage_init",
        ddl_body=_ddl_body,
    )

    assert seen_conns == [fake_conn]


@pytest.mark.asyncio
async def test_statement_timeout_inside_unlocked_fallback_is_tolerated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second statement-timeout while replaying fallback DDL must not
    abort startup."""
    monkeypatch.setattr(
        locking_tools,
        "acquire_startup_lock",
        _make_raising_acquire_startup_lock(
            QueryCanceledError("canceling statement due to statement timeout")
        ),
    )

    fake_conn = object()

    @asynccontextmanager
    async def _fake_managed_transaction(engine: Any):
        yield fake_conn

    monkeypatch.setattr(locking_tools, "managed_transaction", _fake_managed_transaction)

    seen_conns = []

    async def _ddl_body(conn: Any) -> None:
        seen_conns.append(conn)
        raise QueryCanceledError("canceling statement due to statement timeout")

    await locking_tools.run_startup_ddl_tolerating_lock_timeout(
        engine=object(),
        lock_key="some_module_storage_init",
        ddl_body=_ddl_body,
    )

    assert seen_conns == [fake_conn]


@pytest.mark.asyncio
async def test_unrelated_pg_error_still_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A genuine, unrelated PG error (42P01, undefined_table) must still
    abort startup -- this helper only tolerates lock-timeout and
    statement-timeout, not arbitrary DDL failures."""
    monkeypatch.setattr(
        locking_tools,
        "acquire_startup_lock",
        _make_raising_acquire_startup_lock(
            UndefinedTableError("relation \"consys.systems\" does not exist")
        ),
    )

    async def _ddl_body(conn: Any) -> None:
        raise AssertionError(
            "ddl_body must not run when the error is neither a lock timeout "
            "nor a statement timeout"
        )

    with pytest.raises(UndefinedTableError):
        await locking_tools.run_startup_ddl_tolerating_lock_timeout(
            engine=object(),
            lock_key="some_module_storage_init",
            ddl_body=_ddl_body,
        )


# ---------------------------------------------------------------------------
# Structured log lines (#3120) — a confirmed recovery must be
# distinguishable from "still failing" via a stable, grep-able event name,
# not just free text.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recovered_replay_emits_detected_then_recovered_logs(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A lock-timeout followed by a successful unlocked replay must log both
    the initial detection and the confirmed recovery, distinctly."""
    monkeypatch.setattr(
        locking_tools,
        "acquire_startup_lock",
        _make_raising_acquire_startup_lock(
            LockNotAvailableError("canceling statement due to lock timeout")
        ),
    )

    fake_conn = object()

    @asynccontextmanager
    async def _fake_managed_transaction(engine: Any):
        yield fake_conn

    monkeypatch.setattr(locking_tools, "managed_transaction", _fake_managed_transaction)

    async def _ddl_body(conn: Any) -> None:
        pass

    with caplog.at_level("INFO", logger="dynastore.modules.db_config.locking_tools"):
        await locking_tools.run_startup_ddl_tolerating_lock_timeout(
            engine=object(),
            lock_key="some_module_storage_init",
            ddl_body=_ddl_body,
        )

    messages = [r.message for r in caplog.records]
    assert any("ddl_startup_peer_race_detected" in m and "reason=lock_timeout" in m for m in messages)
    assert any("ddl_startup_peer_race_recovered" in m and "reason=lock_timeout" in m for m in messages)
    assert not any("ddl_startup_peer_race_unresolved" in m for m in messages)


@pytest.mark.asyncio
async def test_unresolved_replay_emits_unresolved_log_not_recovered(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A statement-timeout that recurs on the unlocked replay must be logged
    as unresolved, never as a confirmed recovery."""
    monkeypatch.setattr(
        locking_tools,
        "acquire_startup_lock",
        _make_raising_acquire_startup_lock(
            QueryCanceledError("canceling statement due to statement timeout")
        ),
    )

    fake_conn = object()

    @asynccontextmanager
    async def _fake_managed_transaction(engine: Any):
        yield fake_conn

    monkeypatch.setattr(locking_tools, "managed_transaction", _fake_managed_transaction)

    async def _ddl_body(conn: Any) -> None:
        raise QueryCanceledError("canceling statement due to statement timeout")

    with caplog.at_level("INFO", logger="dynastore.modules.db_config.locking_tools"):
        await locking_tools.run_startup_ddl_tolerating_lock_timeout(
            engine=object(),
            lock_key="some_module_storage_init",
            ddl_body=_ddl_body,
        )

    messages = [r.message for r in caplog.records]
    assert any(
        "ddl_startup_peer_race_unresolved" in m and "retry_reason=statement_timeout" in m
        for m in messages
    )
    assert not any("ddl_startup_peer_race_recovered" in m for m in messages)
