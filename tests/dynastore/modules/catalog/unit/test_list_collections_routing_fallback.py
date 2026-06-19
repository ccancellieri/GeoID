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

"""``CollectionService.list_collections`` — backend-agnostic listing.

Unfiltered listing enumerates collection ids from the thin PG ``collections``
registry (the authoritative existence ledger for every backend) and hydrates
each via the configured READ router.  This makes listing work for a pure-ES
(or DuckDB-only) catalog regardless of whether the ES SEARCH index has been
populated yet: existence comes from PG, metadata from wherever the preset
routes it.

A filtered listing (free-text ``q``) still goes through the SEARCH-capable
router (with a READ fallback) because the registry cannot evaluate ``q``.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import List
from unittest.mock import MagicMock

import pytest

from dynastore.modules.catalog import collection_router, collection_service
from dynastore.modules.storage.routing_config import Operation


def _model(cid: str) -> SimpleNamespace:
    """Stand-in for the hydrated Collection model (id + a STAC field)."""
    return SimpleNamespace(
        id=cid,
        extent={"spatial": {"bbox": [[-10.0, 35.0, 30.0, 60.0]]}},
    )


@asynccontextmanager
async def _fake_txn(_resource):
    yield MagicMock(name="conn")


def _patch_enumeration(monkeypatch, svc, ids: List[str]):
    """Wire the registry-enumeration seam: schema resolves, the ids query
    returns ``ids``, and each id hydrates to a model via the READ router."""
    async def _schema(_catalog_id, db_resource=None):
        return "cat_schema"

    monkeypatch.setattr(svc, "_resolve_physical_schema", _schema)
    monkeypatch.setattr(collection_service, "managed_transaction", _fake_txn)

    async def _execute(_conn, **_kw):
        return ids

    monkeypatch.setattr(
        collection_service,
        "_make_collection_list_ids_query",
        lambda _schema: SimpleNamespace(execute=_execute),
    )

    async def _hydrate(_catalog_id, collection_id, _conn, *, hints=frozenset()):
        return _model(collection_id) if collection_id in ids else None

    monkeypatch.setattr(svc, "_get_collection_model_logic", _hydrate)


@pytest.mark.asyncio
async def test_unfiltered_listing_enumerates_registry_and_hydrates(monkeypatch):
    """Unfiltered listing returns every registry collection, hydrated — and
    does NOT depend on the SEARCH router (which would be empty under ES lag)."""
    search_called: List[str] = []

    async def _fake_search(*_a, **_kw):
        search_called.append("search")
        return [], 0

    monkeypatch.setattr(
        collection_router, "search_collection_metadata", _fake_search,
    )

    svc = collection_service.CollectionService(engine=MagicMock())
    _patch_enumeration(monkeypatch, svc, ["coll-a", "coll-b"])

    out = await svc.list_collections("cat")

    assert [c.id for c in out] == ["coll-a", "coll-b"]
    assert out[0].extent is not None            # hydrated via READ router
    assert search_called == []                  # SEARCH router not used for unfiltered list


@pytest.mark.asyncio
async def test_unfiltered_listing_empty_registry_returns_empty(monkeypatch):
    """Genuinely empty catalog: registry yields no ids → []."""
    svc = collection_service.CollectionService(engine=MagicMock())
    _patch_enumeration(monkeypatch, svc, [])

    out = await svc.list_collections("cat")
    assert out == []


@pytest.mark.asyncio
async def test_unfiltered_listing_skips_ids_that_fail_to_hydrate(monkeypatch):
    """An id present in the registry but unhydratable (metadata absent) is
    skipped rather than returned as a blank shell."""
    svc = collection_service.CollectionService(engine=MagicMock())
    _patch_enumeration(monkeypatch, svc, ["coll-a", "coll-b"])

    async def _hydrate(_catalog_id, collection_id, _conn, *, hints=frozenset()):
        return _model(collection_id) if collection_id == "coll-a" else None

    monkeypatch.setattr(svc, "_get_collection_model_logic", _hydrate)

    out = await svc.list_collections("cat")
    assert [c.id for c in out] == ["coll-a"]


@pytest.mark.asyncio
async def test_filtered_listing_uses_search_router(monkeypatch):
    """A free-text ``q`` must route through the SEARCH driver (the registry
    cannot evaluate q), returning complete rows from the search slice."""
    ops: List[str] = []
    _COMPLETE_ROW = {
        "id": "coll-a",
        "title": {"en": "A complete collection"},
        "description": {"en": "carries STAC fields"},
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-10.0, 35.0, 30.0, 60.0]]},
            "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
        },
    }

    async def _fake_search(catalog_id, *, q=None, limit=100, offset=0,
                           db_resource=None, operation=Operation.SEARCH, **_kw):
        ops.append(operation)
        return [_COMPLETE_ROW], 1

    monkeypatch.setattr(
        collection_router, "search_collection_metadata", _fake_search,
    )

    svc = collection_service.CollectionService(engine=MagicMock())
    out = await svc.list_collections("cat", q="complete")

    assert [c.id for c in out] == ["coll-a"]
    assert out[0].extent is not None
    assert ops == [Operation.SEARCH]            # READ fallback not needed


@pytest.mark.asyncio
async def test_filtered_listing_falls_back_to_read_when_search_empty(monkeypatch):
    """``q`` set but the SEARCH slice is empty (ES index not populated) →
    re-run against the READ-routed driver."""
    ops: List[str] = []
    _COMPLETE_ROW = {
        "id": "coll-a",
        "title": {"en": "A complete collection"},
        "description": {"en": "carries STAC fields"},
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-10.0, 35.0, 30.0, 60.0]]},
            "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
        },
    }

    async def _fake_search(catalog_id, *, q=None, limit=100, offset=0,
                           db_resource=None, operation=Operation.SEARCH, **_kw):
        ops.append(operation)
        if operation == Operation.SEARCH:
            return [], 0
        return [_COMPLETE_ROW], 1

    monkeypatch.setattr(
        collection_router, "search_collection_metadata", _fake_search,
    )

    svc = collection_service.CollectionService(engine=MagicMock())
    out = await svc.list_collections("cat", q="complete")

    assert [c.id for c in out] == ["coll-a"]
    assert ops == [Operation.SEARCH, Operation.READ]
