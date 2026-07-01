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

"""Verify that ``DatabaseErrorHandler`` never leaks internal physical schema/table
names (e.g. ``c_<hash>``, ``items_<hash>``) into the HTTP 500 response body.

The raw PostgreSQL error message — which embeds relation names like
``c_abc123.items_xyz456`` — must only appear in the server-side log, not in
the ``detail`` field returned to API consumers.
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from dynastore.extensions.tools.exception_handlers import (
    DatabaseErrorHandler,
    _extract_pgcode,
    handle_exception,
)
from dynastore.modules.db_config.exceptions import (
    DatabaseError,
    QueryExecutionError,
    SchemaNotFoundError,
    TableNotFoundError,
)


# ---------------------------------------------------------------------------
# Fake raw PG error objects (no asyncpg dependency needed)
# ---------------------------------------------------------------------------


class _FakePGError:
    """Minimal stand-in for an asyncpg / psycopg2 error carrying pgcode and a
    raw message that includes internal physical relation names."""

    def __init__(self, pgcode: str, raw_message: str) -> None:
        self.pgcode = pgcode
        self._raw = raw_message

    def __str__(self) -> str:
        return self._raw


def _table_not_found(physical_name: str = "c_abc123def.items_456xyz") -> TableNotFoundError:
    raw = _FakePGError("42P01", f'relation "{physical_name}" does not exist')
    return TableNotFoundError("Table not found", original_exception=raw)


def _schema_not_found(physical_name: str = "c_abc123def") -> SchemaNotFoundError:
    raw = _FakePGError("3F000", f'schema "{physical_name}" does not exist')
    return SchemaNotFoundError("Schema not found", original_exception=raw)


def _query_exec_error(pgcode: str = "40001") -> QueryExecutionError:
    raw = _FakePGError(pgcode, "could not serialize access due to concurrent update")
    return QueryExecutionError("Serialization failure", original_exception=raw)


# ---------------------------------------------------------------------------
# _extract_pgcode helper
# ---------------------------------------------------------------------------


class TestExtractPgcode:
    def test_returns_pgcode_from_asyncpg_original(self) -> None:
        exc = _table_not_found()
        assert _extract_pgcode(exc) == "42P01"

    def test_returns_none_when_no_original_exception(self) -> None:
        exc = TableNotFoundError("no original")
        assert _extract_pgcode(exc) is None

    def test_returns_none_for_unrelated_exception(self) -> None:
        assert _extract_pgcode(ValueError("irrelevant")) is None


# ---------------------------------------------------------------------------
# DatabaseErrorHandler.can_handle
# ---------------------------------------------------------------------------


class TestCanHandle:
    def test_matches_database_error(self) -> None:
        h = DatabaseErrorHandler()
        assert h.can_handle(TableNotFoundError("x")) is True
        assert h.can_handle(SchemaNotFoundError("x")) is True
        assert h.can_handle(QueryExecutionError("x")) is True
        assert h.can_handle(DatabaseError("x")) is True

    def test_skips_unrelated_exceptions(self) -> None:
        h = DatabaseErrorHandler()
        assert h.can_handle(ValueError("nope")) is False
        assert h.can_handle(RuntimeError("nope")) is False
        assert h.can_handle(KeyError("nope")) is False


# ---------------------------------------------------------------------------
# Core security invariant: physical names must not appear in the HTTP detail
# ---------------------------------------------------------------------------


class TestNoPhysicalNameInHttpDetail:
    """The HTTP 500 detail must not contain any internal c_<hash> / items_<hash>
    fragments, regardless of what the raw PG message contains."""

    def test_table_not_found_excludes_physical_relation_name(self) -> None:
        exc = _table_not_found("c_deadbeef01.items_cafe9876")
        h = DatabaseErrorHandler()
        result = h.handle(exc, context={"operation": "upsert"})

        assert result is not None
        assert result.status_code == 500
        detail = str(result.detail)
        assert "c_deadbeef01" not in detail, (
            f"Physical schema name leaked into HTTP detail: {detail!r}"
        )
        assert "items_cafe9876" not in detail, (
            f"Physical table name leaked into HTTP detail: {detail!r}"
        )

    def test_schema_not_found_excludes_physical_schema_name(self) -> None:
        exc = _schema_not_found("c_0000ffff")
        h = DatabaseErrorHandler()
        result = h.handle(exc, context={"operation": "list"})

        assert result is not None
        detail = str(result.detail)
        assert "c_0000ffff" not in detail, (
            f"Physical schema name leaked into HTTP detail: {detail!r}"
        )

    def test_query_exec_error_excludes_raw_pg_message(self) -> None:
        exc = _query_exec_error("40001")
        h = DatabaseErrorHandler()
        result = h.handle(exc, context={"operation": "read"})

        assert result is not None
        detail = str(result.detail)
        # The raw asyncpg message must not appear verbatim
        assert "could not serialize access" not in detail, (
            f"Raw PG message leaked into HTTP detail: {detail!r}"
        )


# ---------------------------------------------------------------------------
# HTTP detail DOES include safe, useful fields
# ---------------------------------------------------------------------------


class TestSanitizedDetailContent:
    """The sanitized detail should still help operators cross-reference logs."""

    def test_contains_exception_type_name(self) -> None:
        exc = _table_not_found()
        result = DatabaseErrorHandler().handle(exc, {"operation": "create"})
        assert result is not None
        assert "TableNotFoundError" in str(result.detail)

    def test_contains_pgcode(self) -> None:
        exc = _table_not_found()
        result = DatabaseErrorHandler().handle(exc, {"operation": "create"})
        assert result is not None
        assert "42P01" in str(result.detail)

    def test_contains_operation(self) -> None:
        exc = _table_not_found()
        result = DatabaseErrorHandler().handle(exc, {"operation": "custom_op"})
        assert result is not None
        assert "custom_op" in str(result.detail)

    def test_contains_correlation_id_key(self) -> None:
        exc = _table_not_found()
        result = DatabaseErrorHandler().handle(exc, {"operation": "delete"})
        assert result is not None
        assert "correlation_id=" in str(result.detail)

    def test_returns_http_500(self) -> None:
        exc = _table_not_found()
        result = DatabaseErrorHandler().handle(exc)
        assert result is not None
        assert result.status_code == 500


# ---------------------------------------------------------------------------
# Full raw message is logged server-side (not suppressed)
# ---------------------------------------------------------------------------


class TestFullMessageLogged:
    """The raw PG error must reach the server log so operators can diagnose."""

    def test_logger_receives_full_str_exception(self, caplog: pytest.LogCaptureFixture) -> None:
        exc = _table_not_found("c_deadbeef01.items_cafe9876")
        with caplog.at_level(logging.ERROR):
            with patch(
                "dynastore.tools.correlation.get_correlation_id",
                return_value="test-cid-123",
            ):
                DatabaseErrorHandler().handle(exc, {"operation": "upsert"})

        combined_log = " ".join(caplog.messages)
        # Physical name must appear in logs...
        assert "c_deadbeef01" in combined_log or "items_cafe9876" in combined_log, (
            "Full PG error should be present in server-side log for operator diagnostics"
        )


# ---------------------------------------------------------------------------
# Registry dispatch path
# ---------------------------------------------------------------------------


class TestRegistryDispatch:
    def test_handle_exception_routes_table_not_found_to_404(self) -> None:
        """#2658: ``TableNotFoundExceptionHandler`` is registered ahead of
        the generic ``DatabaseErrorHandler`` and claims ``TableNotFoundError``
        first, mapping it to a clean 404 instead of an opaque 500 — a missing
        PG hub table means "nothing to render/query yet", not a server
        malfunction. See ``test_table_not_found_exception_handler.py`` for
        the dedicated coverage of that handler; this test only pins that the
        registry ordering routes here rather than to ``DatabaseErrorHandler``.
        """
        exc = _table_not_found("c_abc.items_xyz")
        result = handle_exception(exc)
        assert result.status_code == 404
        detail = str(result.detail)
        assert "c_abc" not in detail
        assert "items_xyz" not in detail

    def test_handle_exception_routes_other_database_error_to_500(self) -> None:
        """Non-``TableNotFoundError`` database errors still fall through to
        the generic ``DatabaseErrorHandler`` 500 mapping."""
        exc = _query_exec_error("40001")
        result = handle_exception(exc)
        assert result.status_code == 500
        detail = str(result.detail)
        assert "could not serialize access" not in detail
        assert "QueryExecutionError" in detail

    def test_unknown_exception_still_reraises(self) -> None:
        with pytest.raises(RuntimeError):
            handle_exception(RuntimeError("totally unrelated"))
