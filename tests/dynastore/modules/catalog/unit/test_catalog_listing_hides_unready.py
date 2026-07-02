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

"""Catalog listing hides catalogs still on their first provisioning pass (#2676).

Mirrors the collection-side gate (#2194/#2308), but keyed on the monotonic
``first_ready_at`` marker rather than the live (re-enterable)
``provisioning_status`` — a catalog that reset to 'provisioning' for a
reprovision or deferred-storage backfill (``reset_checklist_for_reprovision``)
must stay listed throughout, which is exactly why the gate predicate below
must never reference ``provisioning_status``.

Pure SQL-inspection plus mocked-DB call-flow tests — no live PostgreSQL
required, consistent with ``test_collection_listing_hides_transient.py`` and
``test_list_catalogs_visibility.py``.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.catalog_service import (
    CatalogService,
    _get_catalog_query,
    _list_catalogs_query,
    _list_catalogs_query_include_unready,
)


def _make_service() -> CatalogService:
    return CatalogService(engine=None)


_RESOLVE_FN = "dynastore.models.protocols.visibility.resolve_catalog_listing_ids"
_MANAGED_TX = "dynastore.modules.catalog.catalog_service.managed_transaction"
_GET_ENGINE = "dynastore.modules.catalog.catalog_service.get_catalog_engine"


@asynccontextmanager
async def _null_tx(_engine):
    yield MagicMock()


# ---------------------------------------------------------------------------
# (a) Static SQL predicates
# ---------------------------------------------------------------------------


def test_unfiltered_list_query_excludes_unready():
    sql = _list_catalogs_query.template
    assert "deleted_at IS NULL" in sql
    assert "first_ready_at IS NOT NULL" in sql
    # The regression this issue exists for: the predicate must be the
    # monotonic marker, never the live (re-enterable) status column.
    assert "provisioning_status" not in sql


def test_unfiltered_list_query_include_unready_variant_has_no_predicate():
    sql = _list_catalogs_query_include_unready.template
    assert "deleted_at IS NULL" in sql
    assert "first_ready_at" not in sql


def test_get_catalog_by_id_stays_unfiltered():
    """A direct GET-by-id must never gate on first_ready_at (#2676) —
    clients poll a known id to watch provisioning progress."""
    sql = _get_catalog_query.template
    assert "first_ready_at" not in sql
    assert "provisioning_status" not in sql


# ---------------------------------------------------------------------------
# (b) list_catalogs() call-flow: default gate vs. include_unready bypass
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_catalogs_default_uses_gated_query(monkeypatch):
    monkeypatch.setattr(_RESOLVE_FN, AsyncMock(return_value=None))

    with (
        patch(_MANAGED_TX, side_effect=_null_tx),
        patch(_GET_ENGINE, return_value=MagicMock()),
        patch(
            "dynastore.modules.catalog.catalog_service._list_catalogs_query.execute",
            new=AsyncMock(return_value=[]),
        ) as gated_execute,
        patch(
            "dynastore.modules.catalog.catalog_service._list_catalogs_query_include_unready.execute",
            new=AsyncMock(return_value=[]),
        ) as bypass_execute,
    ):
        svc = _make_service()
        result = await svc.list_catalogs(limit=10)

    assert result == []
    gated_execute.assert_awaited_once()
    bypass_execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_list_catalogs_include_unready_uses_bypass_query(monkeypatch):
    monkeypatch.setattr(_RESOLVE_FN, AsyncMock(return_value=None))

    with (
        patch(_MANAGED_TX, side_effect=_null_tx),
        patch(_GET_ENGINE, return_value=MagicMock()),
        patch(
            "dynastore.modules.catalog.catalog_service._list_catalogs_query.execute",
            new=AsyncMock(return_value=[]),
        ) as gated_execute,
        patch(
            "dynastore.modules.catalog.catalog_service._list_catalogs_query_include_unready.execute",
            new=AsyncMock(return_value=[]),
        ) as bypass_execute,
    ):
        svc = _make_service()
        result = await svc.list_catalogs(limit=10, include_unready=True)

    assert result == []
    bypass_execute.assert_awaited_once()
    gated_execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_list_catalogs_ids_branch_carries_ready_predicate_by_default(monkeypatch):
    """The ``ids=`` branch builds its own SQL; the predicate must be present
    unless include_unready=True — this is the branch a reprovisioning
    catalog's own id would still pass through once first_ready_at is set."""
    monkeypatch.setattr(_RESOLVE_FN, AsyncMock(return_value=None))

    captured_sql: list[str] = []

    class _CapturingQuery:
        def __init__(self, sql, result_handler=None, **_kw):
            captured_sql.append(sql)

        async def execute(self, *_a, **_kw):
            return []

    with (
        patch(_MANAGED_TX, side_effect=_null_tx),
        patch(_GET_ENGINE, return_value=MagicMock()),
        patch(
            "dynastore.modules.catalog.catalog_service.DQLQuery",
            new=_CapturingQuery,
        ),
    ):
        svc = _make_service()
        await svc.list_catalogs(limit=10, ids={"cat_a", "cat_b"})

    assert captured_sql
    assert "first_ready_at IS NOT NULL" in captured_sql[0]


@pytest.mark.asyncio
async def test_list_catalogs_ids_branch_omits_predicate_with_include_unready(monkeypatch):
    monkeypatch.setattr(_RESOLVE_FN, AsyncMock(return_value=None))

    captured_sql: list[str] = []

    class _CapturingQuery:
        def __init__(self, sql, result_handler=None, **_kw):
            captured_sql.append(sql)

        async def execute(self, *_a, **_kw):
            return []

    with (
        patch(_MANAGED_TX, side_effect=_null_tx),
        patch(_GET_ENGINE, return_value=MagicMock()),
        patch(
            "dynastore.modules.catalog.catalog_service.DQLQuery",
            new=_CapturingQuery,
        ),
    ):
        svc = _make_service()
        await svc.list_catalogs(limit=10, ids={"cat_a"}, include_unready=True)

    assert captured_sql
    assert "first_ready_at" not in captured_sql[0]


@pytest.mark.asyncio
async def test_list_catalogs_search_branch_carries_aliased_ready_predicate(monkeypatch):
    """The ``q=`` (free-text search) branch joins ``catalog_core`` aliased
    as ``c`` — the predicate must use the ``c.`` prefix alongside the
    existing ``c.deleted_at IS NULL`` filter."""
    monkeypatch.setattr(_RESOLVE_FN, AsyncMock(return_value=None))

    captured_sql: list[str] = []

    class _CapturingQuery:
        def __init__(self, sql, result_handler=None, **_kw):
            captured_sql.append(sql)

        async def execute(self, *_a, **_kw):
            return []

    with (
        patch(_MANAGED_TX, side_effect=_null_tx),
        patch(_GET_ENGINE, return_value=MagicMock()),
        patch(
            "dynastore.modules.catalog.catalog_service.DQLQuery",
            new=_CapturingQuery,
        ),
    ):
        svc = _make_service()
        await svc.list_catalogs(limit=10, q="rain")

    assert captured_sql
    sql = captured_sql[0]
    assert "c.deleted_at IS NULL" in sql
    assert "c.first_ready_at IS NOT NULL" in sql
    assert "ILIKE" in sql


@pytest.mark.asyncio
async def test_list_catalogs_search_branch_include_unready_omits_predicate(monkeypatch):
    monkeypatch.setattr(_RESOLVE_FN, AsyncMock(return_value=None))

    captured_sql: list[str] = []

    class _CapturingQuery:
        def __init__(self, sql, result_handler=None, **_kw):
            captured_sql.append(sql)

        async def execute(self, *_a, **_kw):
            return []

    with (
        patch(_MANAGED_TX, side_effect=_null_tx),
        patch(_GET_ENGINE, return_value=MagicMock()),
        patch(
            "dynastore.modules.catalog.catalog_service.DQLQuery",
            new=_CapturingQuery,
        ),
    ):
        svc = _make_service()
        await svc.list_catalogs(limit=10, q="rain", include_unready=True)

    assert captured_sql
    assert "first_ready_at" not in captured_sql[0]


# ---------------------------------------------------------------------------
# (c) _unpack_catalog_row never leaks first_ready_at onto the public model
# ---------------------------------------------------------------------------


def test_unpack_catalog_row_drops_first_ready_at():
    svc = _make_service()
    row = {
        "id": "cat_x",
        "external_id": "cat_x",
        "provisioning_status": "ready",
        "first_ready_at": "2026-01-01T00:00:00Z",
        "deleted_at": None,
        "title": {"en": "t"},
    }
    model = svc._unpack_catalog_row(row)
    assert model is not None
    dumped = model.model_dump()
    assert "first_ready_at" not in dumped
