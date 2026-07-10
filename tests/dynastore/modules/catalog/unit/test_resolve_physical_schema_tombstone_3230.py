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

"""Regression coverage for #3230: pin the real tombstone predicate behind
``CatalogService.resolve_physical_schema``, without a live DB.

The route-level and resolver-level tests elsewhere for #3230
(``test_resolvers_collection_tombstone_3230.py``,
``test_features_collection_tombstoned_3230.py``,
``test_stac_collection_tombstoned_3230.py``) all mock ``get_collection``
raising ``ValueError("... not found.")`` directly — which is byte-identical
to a catalog that was never created. This file instead exercises the real
``resolve_physical_schema`` control flow (direct internal-id lookup, then
external_id fallback, both cache paths) against an in-memory fixture of
``catalog.catalogs`` rows that actually carry ``deleted_at`` — only the SQL
execution boundary (``DQLQuery.execute`` / ``managed_transaction``) is
faked, so the ``deleted_at IS NULL`` predicate in the real query text is
what drives the result, exactly as it would against a live PostgreSQL
connection.
"""

from __future__ import annotations

import datetime
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

import pytest

import dynastore.modules.catalog.catalog_service as catalog_service_mod
from dynastore.modules.catalog.catalog_service import (
    CatalogService,
    _catalog_external_id_cache,
    _physical_schema_cache,
)


def _fake_dql_query_class(rows: Dict[str, Dict[str, Any]]):
    """Build a ``DQLQuery`` stand-in bound to an in-memory row fixture.

    Evaluates the same ``deleted_at IS [NOT] NULL`` predicate a live PG
    connection would, against ``rows`` (keyed by the row's ``id``) — the
    real ``resolve_physical_schema`` SQL text decides what "matches",
    mirroring a live query without one.
    """

    class _FakeDQLQuery:
        def __init__(self, template: str, *, result_handler=None, post_processor=None) -> None:
            self._template = template.lower()

        async def execute(self, _conn: Any, **params: Any) -> Optional[str]:
            lookup_key = params.get("catalog_id") or params.get("external_id")
            row = rows.get(lookup_key)
            if row is None:
                return None
            is_deleted = row.get("deleted_at") is not None
            if "deleted_at is null" in self._template and is_deleted:
                return None
            if "deleted_at is not null" in self._template and not is_deleted:
                return None
            return row["id"]

    return _FakeDQLQuery


@asynccontextmanager
async def _fake_managed_transaction(_resource: Any):
    yield object()


def _patch_query_layer(monkeypatch: pytest.MonkeyPatch, rows: Dict[str, Dict[str, Any]]) -> None:
    monkeypatch.setattr(catalog_service_mod, "DQLQuery", _fake_dql_query_class(rows))
    monkeypatch.setattr(catalog_service_mod, "managed_transaction", _fake_managed_transaction)


@pytest.mark.asyncio
async def test_resolve_physical_schema_404s_a_tombstoned_catalog_row(monkeypatch):
    """A registry row that exists but carries ``deleted_at`` resolves exactly
    like a missing one: ``allow_missing=False`` raises the "not found"
    ``ValueError`` that ``resolve_collection_or_404`` maps to 404."""
    _physical_schema_cache.cache_clear()
    _catalog_external_id_cache.cache_clear()

    ts = datetime.datetime(2026, 7, 9, 22, 30, 0, tzinfo=datetime.timezone.utc)
    rows = {
        "cat-3230-shouldfix-tombstoned": {
            "id": "cat-3230-shouldfix-tombstoned",
            "deleted_at": ts,
        },
    }
    _patch_query_layer(monkeypatch, rows)

    svc = CatalogService()

    with pytest.raises(ValueError, match="not found"):
        await svc.resolve_physical_schema("cat-3230-shouldfix-tombstoned", allow_missing=False)


@pytest.mark.asyncio
async def test_resolve_physical_schema_allow_missing_returns_none_for_a_tombstoned_row(monkeypatch):
    """Sibling guard: ``allow_missing=True`` degrades to ``None`` instead of raising."""
    _physical_schema_cache.cache_clear()
    _catalog_external_id_cache.cache_clear()

    ts = datetime.datetime(2026, 7, 9, 22, 30, 0, tzinfo=datetime.timezone.utc)
    rows = {
        "cat-3230-shouldfix-tombstoned-2": {
            "id": "cat-3230-shouldfix-tombstoned-2",
            "deleted_at": ts,
        },
    }
    _patch_query_layer(monkeypatch, rows)

    svc = CatalogService()

    result = await svc.resolve_physical_schema(
        "cat-3230-shouldfix-tombstoned-2", allow_missing=True
    )
    assert result is None


@pytest.mark.asyncio
async def test_resolve_physical_schema_resolves_an_active_catalog_row(monkeypatch):
    """Sibling guard: the same row fixture with ``deleted_at=None`` resolves
    normally — proves the fake query layer differentiates on ``deleted_at``,
    it does not just blanket-return ``None``."""
    _physical_schema_cache.cache_clear()
    _catalog_external_id_cache.cache_clear()

    rows = {
        "cat-3230-shouldfix-active": {
            "id": "cat-3230-shouldfix-active",
            "deleted_at": None,
        },
    }
    _patch_query_layer(monkeypatch, rows)

    svc = CatalogService()

    result = await svc.resolve_physical_schema("cat-3230-shouldfix-active", allow_missing=False)
    assert result == "cat-3230-shouldfix-active"
