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

"""Unit + app-level tests for ``TableNotFoundExceptionHandler`` — #2658 item A.

Before this fix, a collection that exists in the catalog/STAC index (served
from Elasticsearch) but whose PostgreSQL hub table was never persisted (e.g.
an ingest job that OOM-crash-looped and rolled back before the table-create
transaction committed, #2657) made the maps/tiles render surfaces raise a
raw asyncpg ``UndefinedTableError`` (pgcode 42P01). That bubbled through the
generic ``DatabaseErrorHandler`` as an opaque HTTP 500 with a
``"Database error ... (TableNotFoundError, pgcode=42P01 ...)"`` body.

``TableNotFoundExceptionHandler`` is registered ahead of
``DatabaseErrorHandler`` and maps ``TableNotFoundError`` to a clean 404
instead — the missing table means "nothing to render yet", not a server
malfunction.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from dynastore.extensions.tools.exception_handlers import (
    TableNotFoundExceptionHandler,
    handle_exception,
    setup_exception_handlers,
)
from dynastore.modules.db_config.exceptions import TableNotFoundError


class _FakePGError:
    """Minimal stand-in for an asyncpg error carrying pgcode 42P01 and a raw
    message that embeds an internal physical relation name."""

    def __init__(self, physical_name: str = "c_abc123.gaul_level_1") -> None:
        self.pgcode = "42P01"
        self._raw = f'relation "{physical_name}" does not exist'

    def __str__(self) -> str:
        return self._raw


def _table_not_found(physical_name: str = "c_abc123.gaul_level_1") -> TableNotFoundError:
    return TableNotFoundError(
        "Database error (42P01)", original_exception=_FakePGError(physical_name)
    )


# ---------------------------------------------------------------------------
# Handler unit tests
# ---------------------------------------------------------------------------


class TestCanHandle:
    def test_matches_table_not_found_error(self) -> None:
        h = TableNotFoundExceptionHandler()
        assert h.can_handle(_table_not_found()) is True

    def test_skips_unrelated_exceptions(self) -> None:
        h = TableNotFoundExceptionHandler()
        assert h.can_handle(ValueError("nope")) is False
        assert h.can_handle(RuntimeError("nope")) is False

        from dynastore.modules.db_config.exceptions import SchemaNotFoundError

        assert h.can_handle(SchemaNotFoundError("x")) is False


class TestHandle:
    def test_returns_404(self) -> None:
        h = TableNotFoundExceptionHandler()
        result = h.handle(_table_not_found())
        assert result is not None
        assert result.status_code == 404

    def test_detail_is_actionable_not_a_raw_pg_message(self) -> None:
        h = TableNotFoundExceptionHandler()
        result = h.handle(_table_not_found())
        detail = str(result.detail)
        assert "has no PostgreSQL feature table" in detail
        assert "ingest has not populated a renderable store" in detail

    def test_detail_never_leaks_physical_table_name(self) -> None:
        h = TableNotFoundExceptionHandler()
        result = h.handle(_table_not_found("c_deadbeef01.gaul_level_1"))
        detail = str(result.detail)
        assert "c_deadbeef01" not in detail
        assert "42P01" not in detail

    def test_detail_never_leaks_pgcode(self) -> None:
        h = TableNotFoundExceptionHandler()
        result = h.handle(_table_not_found())
        assert "42P01" not in str(result.detail)

    def test_detail_includes_collection_id_from_context(self) -> None:
        h = TableNotFoundExceptionHandler()
        result = h.handle(
            _table_not_found(), context={"catalog_id": "gaulb", "collection_id": "gaul_level_1"}
        )
        assert result is not None
        assert "gaul_level_1" in str(result.detail)

    def test_detail_falls_back_to_generic_message_without_context(self) -> None:
        h = TableNotFoundExceptionHandler()
        result = h.handle(_table_not_found(), context=None)
        assert result is not None
        assert "no PostgreSQL feature table" in str(result.detail)

    def test_full_exception_logged_server_side(self, caplog: pytest.LogCaptureFixture) -> None:
        import logging

        exc = _table_not_found("c_deadbeef01.gaul_level_1")
        with caplog.at_level(logging.ERROR):
            TableNotFoundExceptionHandler().handle(
                exc, context={"catalog_id": "gaulb", "collection_id": "gaul_level_1"}
            )
        combined_log = " ".join(caplog.messages)
        assert "gaul_level_1" in combined_log


# ---------------------------------------------------------------------------
# Registry dispatch — must win over the generic DatabaseErrorHandler
# ---------------------------------------------------------------------------


class TestRegistryOrdering:
    def test_table_not_found_wins_over_database_error_handler(self) -> None:
        result = handle_exception(_table_not_found())
        assert result.status_code == 404, (
            "TableNotFoundExceptionHandler must be registered ahead of "
            "DatabaseErrorHandler so 42P01 maps to a clean 404, not a 500"
        )

    def test_other_database_errors_still_reach_database_error_handler(self) -> None:
        from dynastore.modules.db_config.exceptions import UniqueViolationError

        # UniqueViolationError is claimed by ConflictExceptionHandler (409),
        # registered even earlier — confirms this handler is narrowly scoped
        # to TableNotFoundError and doesn't shadow sibling DatabaseError types.
        exc = UniqueViolationError("dup", original_exception=_FakePGError())
        result = handle_exception(exc)
        assert result.status_code == 409

    def test_unrelated_database_error_still_500(self) -> None:
        from dynastore.modules.db_config.exceptions import QueryExecutionError

        exc = QueryExecutionError("boom", original_exception=_FakePGError())
        result = handle_exception(exc)
        assert result.status_code == 500


# ---------------------------------------------------------------------------
# App-level: the live-bug shape end to end through the real ASGI middleware
# ---------------------------------------------------------------------------


@pytest.fixture()
def app_with_global_handling() -> FastAPI:
    app = FastAPI()
    setup_exception_handlers(app)

    @app.get("/maps/catalogs/{catalog_id}/collections/{collection_id}/map")
    def _render_map(catalog_id: str, collection_id: str):
        raise _table_not_found(f"c_abc123.{collection_id}")

    return app


def test_raw_500_becomes_clean_404_through_full_app_stack(
    app_with_global_handling: FastAPI,
) -> None:
    """Reproduces the reported live bug end to end: a route that raises the
    same ``TableNotFoundError`` a real ``get_features_for_rendering`` call
    would raise for an un-ingested collection must surface as a clean 404,
    not the raw ``{"detail":"Database error ... (TableNotFoundError,
    pgcode=42P01 ...)"}`` 500 body.
    """
    client = TestClient(app_with_global_handling, raise_server_exceptions=False)
    r = client.get("/maps/catalogs/gaulb/collections/gaul_level_1/map")

    assert r.status_code == 404, f"expected clean 404, got {r.status_code}: {r.text}"
    body = r.json()
    assert isinstance(body["detail"], str)
    assert "42P01" not in body["detail"], f"raw pgcode leaked: {body}"
    assert "c_abc123" not in body["detail"], f"physical schema name leaked: {body}"
    assert "gaul_level_1" in body["detail"]
    assert "has no PostgreSQL feature table" in body["detail"]


def test_unrelated_500_still_returns_500_through_full_app_stack() -> None:
    """Guard: this fix must not turn every 500 into a 404 — only the
    narrowly-scoped 42P01 / TableNotFoundError case."""
    app = FastAPI()
    setup_exception_handlers(app)

    @app.get("/boom")
    def _boom():
        raise RuntimeError("totally unrelated failure")

    client = TestClient(app, raise_server_exceptions=False)
    r = client.get("/boom")
    assert r.status_code == 500
