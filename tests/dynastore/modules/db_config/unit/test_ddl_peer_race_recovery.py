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

"""Regression pin (un-fao/GeoID#821, deferred Option B): ``DDLExecutor``
recovers from peer-race UniqueViolation / DuplicateObject errors when the
post-wait existence re-check observes the peer's commit on the outer
connection.

Background: ``ensure_schema_exists`` already swallows the schema-specific
``pg_namespace_nspname_index`` UniqueViolation (PR #861, Option A). This
covers the general DDL case — any ``DDLQuery`` carrying an
``existence_check`` will recover from a peer winning the race past the
post-wait recheck.

Two layers tested:

1. ``_is_duplicate_object_error`` — pgcode detection across the
   ``.orig`` / ``__cause__`` / ``.pgcode`` / ``.sqlstate`` shapes that
   asyncpg / SQLAlchemy use.
2. ``DDLExecutor._try_peer_race_recovery_{async,sync}`` — when the
   inner DDL attempt raised a duplicate-object error AND the outer-conn
   re-check now reports the object exists, the helpers return True so
   the executor returns success instead of propagating.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.db_config.query_executor import (
    DDLExecutor,
    TemplateQueryBuilder,
    _is_duplicate_object_error,
)


# ---------------------------------------------------------------------------
# _is_duplicate_object_error — pgcode detection
# ---------------------------------------------------------------------------


class _PgErr(Exception):
    """Test-only stand-in for asyncpg.PostgresError carrying ``pgcode``."""

    def __init__(
        self,
        pgcode: str | None = None,
        sqlstate: str | None = None,
        message: str | None = None,
    ):
        super().__init__(message or f"pg error pgcode={pgcode} sqlstate={sqlstate}")
        if pgcode is not None:
            self.pgcode = pgcode
        if sqlstate is not None:
            self.sqlstate = sqlstate


@pytest.mark.parametrize("pgcode", ["42P06", "42P07", "42710", "23505"])
def test_is_duplicate_object_error_detects_pgcode_on_orig(pgcode: str) -> None:
    """SQLAlchemy DBAPIError wraps asyncpg errors under ``.orig``."""
    inner = _PgErr(pgcode=pgcode)
    outer = Exception("wrapped")
    setattr(outer, "orig", inner)
    assert _is_duplicate_object_error(outer)


@pytest.mark.parametrize("pgcode", ["42P06", "42P07", "42710", "23505"])
def test_is_duplicate_object_error_detects_sqlstate_attr(pgcode: str) -> None:
    """asyncpg surfaces error codes as ``.sqlstate`` when not wrapped."""
    exc = _PgErr(sqlstate=pgcode)
    assert _is_duplicate_object_error(exc)


def test_is_duplicate_object_error_walks_cause_chain() -> None:
    inner = _PgErr(pgcode="42P06")
    middle = Exception("middle")
    middle.__cause__ = inner
    outer = Exception("outer")
    outer.__cause__ = middle
    assert _is_duplicate_object_error(outer)


def test_is_duplicate_object_error_false_for_unrelated_pgcode() -> None:
    inner = _PgErr(pgcode="42501")  # permission denied
    outer = Exception("wrapped")
    setattr(outer, "orig", inner)
    assert not _is_duplicate_object_error(outer)


def test_is_duplicate_object_error_detects_tuple_concurrently_updated_race() -> None:
    """PostgreSQL may surface concurrent CREATE OR REPLACE FUNCTION as XX000."""
    inner = _PgErr(pgcode="XX000", message="tuple concurrently updated")
    outer = Exception("wrapped")
    setattr(outer, "orig", inner)
    assert _is_duplicate_object_error(outer)


def test_is_duplicate_object_error_false_for_unrelated_internal_error() -> None:
    inner = _PgErr(pgcode="XX000", message="cache lookup failed for function 123")
    outer = Exception("wrapped")
    setattr(outer, "orig", inner)
    assert not _is_duplicate_object_error(outer)


def test_is_duplicate_object_error_false_for_none_and_unrelated_exc() -> None:
    assert not _is_duplicate_object_error(None)
    assert not _is_duplicate_object_error(ValueError("nope"))


def test_is_duplicate_object_error_terminates_on_cycle() -> None:
    """Pathological self-referencing cause chain must not infinite-loop."""
    exc = Exception("self")
    exc.__cause__ = exc
    assert not _is_duplicate_object_error(exc)


# ---------------------------------------------------------------------------
# DDLExecutor._try_peer_race_recovery_{async,sync}
# ---------------------------------------------------------------------------


def _make_executor(existence_check: Any) -> DDLExecutor:
    builder = MagicMock(spec=TemplateQueryBuilder)
    return DDLExecutor(query_builder_strategy=builder, existence_check=existence_check)


def _dup_exc(pgcode: str = "42P06") -> Exception:
    inner = _PgErr(pgcode=pgcode)
    exc = Exception(f"duplicate object pgcode={pgcode}")
    setattr(exc, "orig", inner)
    return exc


@pytest.mark.asyncio
async def test_async_recovery_returns_true_when_peer_wins() -> None:
    """Duplicate-object error + outer-conn re-check reports object exists
    -> recovery succeeds (returns True)."""
    existence_mock = AsyncMock(return_value=True)
    existence_mock._needs_raw_params = False
    executor = _make_executor(existence_mock)

    conn = MagicMock()
    result = await executor._try_peer_race_recovery_async(conn, {}, _dup_exc("42P06"))

    assert result is True
    existence_mock.assert_awaited_once_with(conn, {})


@pytest.mark.asyncio
async def test_async_recovery_accepts_tuple_concurrently_updated_peer_race() -> None:
    existence_mock = AsyncMock(return_value=True)
    existence_mock._needs_raw_params = False
    executor = _make_executor(existence_mock)

    inner = _PgErr(pgcode="XX000", message="tuple concurrently updated")
    exc = Exception("wrapped")
    setattr(exc, "orig", inner)

    result = await executor._try_peer_race_recovery_async(MagicMock(), {}, exc)

    assert result is True


@pytest.mark.asyncio
async def test_async_recovery_returns_false_for_non_duplicate_pgcode() -> None:
    """Non-duplicate errors must NOT trigger recovery — they should
    surface so the underlying bug is visible."""
    existence_mock = AsyncMock(return_value=True)
    existence_mock._needs_raw_params = False
    executor = _make_executor(existence_mock)

    inner = _PgErr(pgcode="42501")  # permission denied
    perm_exc = Exception("permission denied")
    setattr(perm_exc, "orig", inner)

    result = await executor._try_peer_race_recovery_async(MagicMock(), {}, perm_exc)

    assert result is False
    existence_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_async_recovery_returns_false_when_object_still_absent() -> None:
    """Duplicate-object surfaced but outer-conn check still reports absent
    -> the error was NOT actually a peer-race (or peer rolled back). Do
    not silently swallow."""
    existence_mock = AsyncMock(return_value=False)
    existence_mock._needs_raw_params = False
    executor = _make_executor(existence_mock)

    result = await executor._try_peer_race_recovery_async(MagicMock(), {}, _dup_exc())

    assert result is False


@pytest.mark.asyncio
async def test_async_recovery_returns_false_when_no_existence_check() -> None:
    executor = _make_executor(existence_check=None)
    result = await executor._try_peer_race_recovery_async(MagicMock(), {}, _dup_exc())
    assert result is False


@pytest.mark.asyncio
async def test_async_recovery_returns_false_when_recheck_itself_raises() -> None:
    """If the recheck itself blows up, the original error must propagate
    — never silently swallow a duplicate-object whose post-condition we
    can't actually verify."""
    existence_mock = AsyncMock(side_effect=RuntimeError("recheck broken"))
    existence_mock._needs_raw_params = False
    executor = _make_executor(existence_mock)

    result = await executor._try_peer_race_recovery_async(MagicMock(), {}, _dup_exc())

    assert result is False


def test_sync_recovery_returns_true_when_peer_wins() -> None:
    existence_mock = MagicMock(return_value=True)
    existence_mock._needs_raw_params = False
    executor = _make_executor(existence_mock)

    conn = MagicMock()
    result = executor._try_peer_race_recovery_sync(conn, {}, _dup_exc("23505"))

    assert result is True
    existence_mock.assert_called_once_with(conn, {})


def test_sync_recovery_returns_false_for_non_duplicate_pgcode() -> None:
    existence_mock = MagicMock(return_value=True)
    existence_mock._needs_raw_params = False
    executor = _make_executor(existence_mock)

    inner = _PgErr(pgcode="42501")
    perm_exc = Exception("permission denied")
    setattr(perm_exc, "orig", inner)

    result = executor._try_peer_race_recovery_sync(MagicMock(), {}, perm_exc)

    assert result is False
    existence_mock.assert_not_called()


def test_sync_recovery_returns_false_when_object_still_absent() -> None:
    existence_mock = MagicMock(return_value=False)
    existence_mock._needs_raw_params = False
    executor = _make_executor(existence_mock)

    assert not executor._try_peer_race_recovery_sync(MagicMock(), {}, _dup_exc())


def test_sync_recovery_returns_false_when_no_existence_check() -> None:
    executor = _make_executor(existence_check=None)
    assert not executor._try_peer_race_recovery_sync(MagicMock(), {}, _dup_exc())


def test_sync_recovery_returns_false_when_recheck_raises() -> None:
    existence_mock = MagicMock(side_effect=RuntimeError("recheck broken"))
    existence_mock._needs_raw_params = False
    executor = _make_executor(existence_mock)

    assert not executor._try_peer_race_recovery_sync(MagicMock(), {}, _dup_exc())
