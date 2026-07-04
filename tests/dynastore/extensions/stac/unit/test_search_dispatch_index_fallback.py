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

"""Regression test for #2894: STAC ``/search`` did not fall back to PG when
the ES items index is missing.

``_maybe_dispatch_to_es_search`` resolves the catalog's items SEARCH driver
and, when it is an ES items driver, streams ``read_entities`` /
``count_entities`` directly. Both of those calls degrade gracefully on a
missing index (``ignore_unavailable`` / a swallowed ``NotFoundError``) and
return an empty-but-successful result instead of raising — so the dispatch's
own ``try/except`` around them never fires, and a genuinely PG-resident item
was served as ``numberMatched: 0`` instead of falling through to the
PostgreSQL path (the reported symptom).

``maybe_dispatch_items_to_search_driver`` (OGC Features/Records ``/items``)
and ``item_query._try_driver_dispatch`` (by-id GET) already probe
``driver.index_available(catalog_id)`` before dispatching; this test pins the
same guard on the STAC-specific search dispatch.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pytest

from dynastore.extensions.stac.search import _maybe_dispatch_to_es_search
from dynastore.models.protocols.storage_driver import Capability


@dataclass
class _Resolved:
    driver: Any


class _FakeEsItemsDriver:
    """ES items driver reporting per-tenant index availability."""

    is_es_items_driver = True
    supports_cql_es = True
    capabilities = frozenset({Capability.READ})

    def __init__(self, index_available: bool):
        self._index_available = index_available
        self.read_calls: list = []
        self.count_calls: list = []

    async def index_available(self, catalog_id: str) -> bool:
        return self._index_available

    async def read_entities(
        self, catalog_id, collection_id, *,
        entity_ids=None, request=None, context=None,
        limit=100, offset=0, db_resource=None,
    ):
        self.read_calls.append(collection_id)
        return
        yield  # pragma: no cover — make this an async generator

    async def count_entities(
        self, catalog_id, collection_id, *, request=None, db_resource=None,
    ):
        self.count_calls.append(collection_id)
        return 0


@dataclass
class _SearchRequest:
    filter: Optional[Any] = None
    collections: Optional[List[str]] = None
    ids: Optional[List[str]] = None
    bbox: Optional[List[float]] = None
    intersects: Optional[Dict[str, Any]] = None
    datetime: Optional[str] = None
    limit: int = 10
    offset: int = 0


def _patch_resolver(monkeypatch, driver):
    import dynastore.modules.storage.router as _router

    async def _fake(cat, cid=None, **_kw):
        return _Resolved(driver=driver)

    monkeypatch.setattr(_router, "get_items_search_driver", _fake)


@pytest.mark.asyncio
async def test_search_dispatch_degrades_to_pg_when_index_absent(monkeypatch):
    drv = _FakeEsItemsDriver(index_available=False)
    _patch_resolver(monkeypatch, drv)

    out = await _maybe_dispatch_to_es_search(
        "cat-x", _SearchRequest(collections=["col-a"])
    )

    assert out is None                 # defers to the PG search_items path
    assert not drv.read_calls          # the index-less ES driver was never queried
    assert not drv.count_calls


@pytest.mark.asyncio
async def test_search_dispatch_uses_es_when_index_present(monkeypatch):
    drv = _FakeEsItemsDriver(index_available=True)
    _patch_resolver(monkeypatch, drv)

    out = await _maybe_dispatch_to_es_search(
        "cat-x", _SearchRequest(collections=["col-a"])
    )

    assert out is not None
    assert drv.read_calls
    assert drv.count_calls
