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

"""Unit + app-level tests for ``LockNotAvailableExceptionHandler`` (#3206).

During the geometry-stats backfill window on dev, DDL held a lock on the
gaul geometries table and every uncached tile render that queued behind it
died with ``asyncpg.exceptions.LockNotAvailableError`` (pgcode 55P03) once
its statement's ``lock_timeout`` elapsed. ``query_executor._handle_db_exception``
wraps that as a plain ``QueryExecutionError`` (55P03 has no entry in
``PGCODE_EXCEPTION_MAP``), which used to fall through to the generic
``DatabaseErrorHandler`` as an opaque 500 -- exactly the wrong signal for a
transient lock wait. Pool saturation and bare timeouts already return
503 + Retry-After; this pins that lock contention gets the same treatment.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from dynastore.extensions.tools.exception_handlers import (
    DatabaseErrorHandler,
    LockNotAvailableExceptionHandler,
    handle_exception,
    setup_exception_handlers,
)
from dynastore.modules.db_config.exceptions import QueryExecutionError


class _FakeLockNotAvailableError(Exception):
    """Stand-in for asyncpg's ``LockNotAvailableError``: carries pgcode 55P03
    and the class name the handler's type-name fallback matches on."""

    def __init__(self, message: str = "canceling statement due to lock timeout") -> None:
        super().__init__(message)
        self.pgcode = "55P03"


# Rename the class itself so ``type(original).__name__`` reads
# "LockNotAvailableError", matching the real asyncpg exception.
_FakeLockNotAvailableError.__name__ = "LockNotAvailableError"
_FakeLockNotAvailableError.__qualname__ = "LockNotAvailableError"


class _FakePGErrorPgcodeOnly:
    """A pgcode-55P03 error under an unrelated class name -- exercises the
    pgcode branch of ``can_handle`` independently of the type-name check."""

    def __init__(self) -> None:
        self.pgcode = "55P03"

    def __str__(self) -> str:
        return "canceling statement due to lock timeout"


def _lock_not_available_query_error() -> QueryExecutionError:
    return QueryExecutionError(
        "Database query failed.", original_exception=_FakeLockNotAvailableError()
    )


def _pgcode_only_query_error() -> QueryExecutionError:
    return QueryExecutionError(
        "Database query failed.", original_exception=_FakePGErrorPgcodeOnly()
    )


# ---------------------------------------------------------------------------
# Handler unit tests
# ---------------------------------------------------------------------------


class TestCanHandle:
    def test_matches_lock_not_available_error_by_class_name(self) -> None:
        h = LockNotAvailableExceptionHandler()
        assert h.can_handle(_lock_not_available_query_error()) is True

    def test_matches_by_pgcode_alone(self) -> None:
        h = LockNotAvailableExceptionHandler()
        assert h.can_handle(_pgcode_only_query_error()) is True

    def test_skips_unrelated_exceptions(self) -> None:
        h = LockNotAvailableExceptionHandler()
        assert h.can_handle(ValueError("nope")) is False
        assert h.can_handle(RuntimeError("nope")) is False

    def test_skips_other_pgcodes(self) -> None:
        from dynastore.modules.db_config.exceptions import TableNotFoundError

        class _FakeOtherPGError:
            pgcode = "42P01"

        h = LockNotAvailableExceptionHandler()
        exc = TableNotFoundError("Database error (42P01)", original_exception=_FakeOtherPGError())
        assert h.can_handle(exc) is False


class TestHandle:
    def test_returns_503_with_retry_after(self) -> None:
        h = LockNotAvailableExceptionHandler()
        result = h.handle(_lock_not_available_query_error())
        assert result is not None
        assert result.status_code == 503
        assert result.headers is not None
        assert result.headers["Retry-After"] == "5"


# ---------------------------------------------------------------------------
# Registry dispatch — must win over the generic DatabaseErrorHandler
# ---------------------------------------------------------------------------


class TestRegistryOrdering:
    def test_lock_not_available_wins_over_database_error_handler(self) -> None:
        """Pins the reported live bug: a 55P03-wrapped QueryExecutionError
        must dispatch to 503, not fall through to the generic 500 handler.
        This assertion fails without the fix (yields 500) and passes with it."""
        db_handler = DatabaseErrorHandler()
        assert db_handler.can_handle(_lock_not_available_query_error()) is True

        result = handle_exception(_lock_not_available_query_error())
        assert result.status_code == 503, (
            "LockNotAvailableExceptionHandler must be registered ahead of "
            "DatabaseErrorHandler so 55P03 maps to 503, not a generic 500"
        )
        assert result.headers["Retry-After"] == "5"

    def test_other_database_errors_still_reach_database_error_handler(self) -> None:
        class _FakeUnrelatedError:
            pgcode = None

        exc = QueryExecutionError("boom", original_exception=_FakeUnrelatedError())
        result = handle_exception(exc)
        assert result.status_code == 500


# ---------------------------------------------------------------------------
# App-level: the live-bug shape end to end through the real ASGI middleware
# ---------------------------------------------------------------------------


@pytest.fixture()
def app_with_global_handling() -> FastAPI:
    app = FastAPI()
    setup_exception_handlers(app)

    @app.get("/maps/catalogs/{catalog_id}/collections/{collection_id}/tiles/{z}/{x}/{y}")
    def _render_tile(catalog_id: str, collection_id: str, z: int, x: int, y: int):
        raise _lock_not_available_query_error()

    return app


def test_raw_500_becomes_retryable_503_through_full_app_stack(
    app_with_global_handling: FastAPI,
) -> None:
    """Reproduces the reported dev incident end to end: a tile render that
    hits a lock-timeout mid-backfill must surface as 503 + Retry-After, not
    the raw ``{"detail":"Internal Server Error: Database query failed. ..."}``
    500 body."""
    client = TestClient(app_with_global_handling, raise_server_exceptions=False)
    r = client.get("/maps/catalogs/gaulb/collections/gaul_level_1/tiles/5/10/12")

    assert r.status_code == 503, f"expected retryable 503, got {r.status_code}: {r.text}"
    assert r.headers["Retry-After"] == "5"


def test_unrelated_500_still_returns_500_through_full_app_stack() -> None:
    """Guard: this fix must not turn every 500 into a 503 — only the
    narrowly-scoped 55P03 / LockNotAvailableError case."""
    app = FastAPI()
    setup_exception_handlers(app)

    @app.get("/boom")
    def _boom():
        raise RuntimeError("totally unrelated failure")

    client = TestClient(app, raise_server_exceptions=False)
    r = client.get("/boom")
    assert r.status_code == 500
