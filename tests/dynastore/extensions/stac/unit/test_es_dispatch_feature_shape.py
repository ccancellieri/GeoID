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

"""Unit tests pinning the routing-aware STAC ``/search`` dispatch contract.

``_maybe_dispatch_to_es_search`` resolves the items SEARCH driver via routing
(``router.get_items_search_driver`` — SEARCH→READ fallback, #989) and
dispatches the structural query to **that driver** via its streaming
``read_entities`` + ``count_entities`` contract. It no longer hardcodes the
public Elasticsearch class, so:

* a catalog routing SEARCH to the tenant-private ES driver is honoured;
* a catalog with no dedicated search driver (PG ``QUERY_FALLBACK_SOURCE``)
  falls through to the PostgreSQL path (dispatch returns ``None``);
* mixed drivers across collections fall through to PG.

The downstream serializer reads ``feature.properties["_catalog_id" |
"_collection_id"]`` on each hit, so the dispatch injects those markers on the
read-contract ``Feature`` instances ``read_entities`` yields — the shape the PG
path emits. Read-contract reconstruction itself is the driver's job and is
pinned at the driver layer (``test_elasticsearch_driver.py``), not here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pytest

from dynastore.extensions.stac.search import _maybe_dispatch_to_es_search
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.ogc import Feature


@dataclass
class _Resolved:
    """Stand-in for ``ResolvedDriver`` — the dispatcher reads ``.driver``."""

    driver: Any


class _FakeEsItemsDriver:
    """An ES items driver: ``is_es_items_driver`` marker + streaming
    ``read_entities`` (yields read-contract Features) + ``count_entities``.

    Stands in for either the public or the tenant-private ES driver — the
    dispatcher treats both identically because it dispatches through the
    streaming contract, not the concrete class.
    """

    is_es_items_driver = True
    supports_cql_es = True
    capabilities = frozenset({Capability.READ})

    def __init__(self, features: List[Feature], total: int):
        self._features = features
        self._total = total
        self.read_calls: list = []
        self.count_calls: list = []

    async def read_entities(
        self, catalog_id, collection_id, *,
        entity_ids=None, request=None, context=None,
        limit=100, offset=0, db_resource=None,
    ):
        self.read_calls.append(
            {"collection_id": collection_id, "request": request,
             "limit": limit, "offset": offset}
        )
        for f in self._features:
            yield f

    async def count_entities(
        self, catalog_id, collection_id, *, request=None, db_resource=None,
    ):
        self.count_calls.append({"collection_id": collection_id, "request": request})
        return self._total


class _FakePgFallbackDriver:
    """A read-primary driver with no dedicated search backend — the PG case."""

    capabilities = frozenset({Capability.READ, Capability.QUERY_FALLBACK_SOURCE})


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


def _feat(fid: str, collection: Optional[str] = None) -> Feature:
    d: Dict[str, Any] = {
        "type": "Feature",
        "id": fid,
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
    }
    if collection:
        d["collection"] = collection
    return Feature.model_validate(d)


def _patch_resolver(monkeypatch, driver_for):
    """Patch ``get_items_search_driver`` to return ``driver_for(cid)``."""
    import dynastore.modules.storage.router as _router

    async def _fake_get_items_search_driver(cat, cid=None, **_kw):
        drv = driver_for(cid)
        if drv is None:
            raise ValueError("no driver")
        return _Resolved(driver=drv)

    monkeypatch.setattr(
        _router, "get_items_search_driver", _fake_get_items_search_driver
    )


@pytest.mark.asyncio
async def test_dispatch_returns_feature_instances(monkeypatch):
    drv = _FakeEsItemsDriver(features=[_feat("item-1", "col-a")], total=1)
    _patch_resolver(monkeypatch, lambda cid: drv)

    out = await _maybe_dispatch_to_es_search("cat-x", _SearchRequest(collections=["col-a"]))

    assert out is not None
    features, total, aggs = out
    assert total == 1
    assert aggs is None
    assert len(features) == 1
    assert isinstance(features[0], Feature)


@pytest.mark.asyncio
async def test_dispatch_injects_cross_collection_markers(monkeypatch):
    drv = _FakeEsItemsDriver(features=[_feat("item-2", "col-b")], total=1)
    _patch_resolver(monkeypatch, lambda cid: drv)

    out = await _maybe_dispatch_to_es_search("cat-y", _SearchRequest(collections=["col-b"]))
    assert out is not None
    features, _total, _aggs = out
    assert features[0].properties["_catalog_id"] == "cat-y"
    assert features[0].properties["_collection_id"] == "col-b"


@pytest.mark.asyncio
async def test_dispatch_falls_back_to_request_collection_when_missing(monkeypatch):
    drv = _FakeEsItemsDriver(features=[_feat("legacy-item")], total=1)
    _patch_resolver(monkeypatch, lambda cid: drv)

    out = await _maybe_dispatch_to_es_search("cat-z", _SearchRequest(collections=["only-one"]))
    assert out is not None
    features, _, _ = out
    assert features[0].properties["_catalog_id"] == "cat-z"
    assert features[0].properties["_collection_id"] == "only-one"


@pytest.mark.asyncio
async def test_total_comes_from_count_not_stream_length(monkeypatch):
    # numberMatched is the count, decoupled from the page of Features streamed.
    drv = _FakeEsItemsDriver(features=[_feat("ok", "c1")], total=42)
    _patch_resolver(monkeypatch, lambda cid: drv)

    out = await _maybe_dispatch_to_es_search("cat-t", _SearchRequest(collections=["c1"]))
    assert out is not None
    features, total, _ = out
    assert total == 42
    assert len(features) == 1
    assert features[0].id == "ok"
    assert drv.count_calls, "count_entities must supply numberMatched"


@pytest.mark.asyncio
async def test_dispatch_threads_params_to_driver(monkeypatch):
    """STAC limit/offset + multi-collection set reach the driver request."""
    drv = _FakeEsItemsDriver(features=[], total=0)
    _patch_resolver(monkeypatch, lambda cid: drv)

    await _maybe_dispatch_to_es_search(
        "cat-x", _SearchRequest(collections=["c1", "c2"], limit=25, offset=50)
    )
    assert drv.read_calls
    rc = drv.read_calls[0]
    assert rc["limit"] == 25
    assert rc["offset"] == 50
    req = rc["request"]
    assert req.collections == ["c1", "c2"]
    assert req.offset == 50
    assert req.limit == 25


@pytest.mark.asyncio
async def test_dispatch_declines_for_pg_fallback_driver(monkeypatch):
    """A QUERY_FALLBACK_SOURCE (PG) driver → None so the PostgreSQL path runs."""
    _patch_resolver(monkeypatch, lambda cid: _FakePgFallbackDriver())
    out = await _maybe_dispatch_to_es_search("cat-x", _SearchRequest(collections=["c1"]))
    assert out is None


@pytest.mark.asyncio
async def test_dispatch_declines_for_mixed_drivers(monkeypatch):
    """Heterogeneous SEARCH drivers across collections → None (PG path)."""
    es = _FakeEsItemsDriver(features=[], total=0)
    pg = _FakePgFallbackDriver()
    mapping = {"c1": es, "c2": pg}
    _patch_resolver(monkeypatch, lambda cid: mapping[cid])
    out = await _maybe_dispatch_to_es_search(
        "cat-x", _SearchRequest(collections=["c1", "c2"])
    )
    assert out is None


@pytest.mark.asyncio
async def test_dispatch_declines_for_non_es_driver(monkeypatch):
    """A non-fallback driver without the is_es_items_driver marker → None."""

    class _NotEs:
        capabilities = frozenset({Capability.READ})

    _patch_resolver(monkeypatch, lambda cid: _NotEs())
    out = await _maybe_dispatch_to_es_search("cat-x", _SearchRequest(collections=["c1"]))
    assert out is None


@pytest.mark.asyncio
async def test_cql_filter_no_longer_forces_pg_when_driver_supports_cql(monkeypatch):
    """A translatable CQL filter on a CQL-ES-capable driver is served by ES.

    The translated clause is threaded onto the driver's ``QueryRequest.es_filter``
    (instead of unconditionally declining to the PostgreSQL path).
    """
    drv = _FakeEsItemsDriver(features=[_feat("hit", "c1")], total=1)
    _patch_resolver(monkeypatch, lambda cid: drv)

    # Isolate the dispatch wiring: stub the CQL→ES translation so the test does
    # not need a live ConfigsProtocol / QueryOptimizer. The point under test is
    # that a present-and-translatable filter no longer forces the PG fallback and
    # the clause reaches the driver request.
    import dynastore.extensions.stac.search as _search

    sentinel_clause = {"term": {"properties.cloud_cover": 10}}

    async def _fake_translate(cat_id, cids, filt, driver):
        return sentinel_clause

    monkeypatch.setattr(_search, "_translate_filter_to_es", _fake_translate)

    out = await _maybe_dispatch_to_es_search(
        "cat-x",
        _SearchRequest(
            collections=["c1"],
            filter=_search.AttributeFilter(field="cloud_cover", operator="eq", value=10),
        ),
    )

    assert out is not None, "CQL filter must NOT force the PG path anymore"
    assert drv.read_calls, "the ES driver must have served the search"
    req = drv.read_calls[0]["request"]
    assert req.es_filter == sentinel_clause


@pytest.mark.asyncio
async def test_cql_filter_declines_when_driver_lacks_cql_support(monkeypatch):
    """An ES driver without ``supports_cql_es`` still falls back to PG."""

    class _NoCqlEsDriver(_FakeEsItemsDriver):
        supports_cql_es = False

    drv = _NoCqlEsDriver(features=[], total=0)
    _patch_resolver(monkeypatch, lambda cid: drv)

    import dynastore.extensions.stac.search as _search

    out = await _maybe_dispatch_to_es_search(
        "cat-x",
        _SearchRequest(
            collections=["c1"],
            filter=_search.AttributeFilter(field="x", operator="eq", value=1),
        ),
    )
    assert out is None


@pytest.mark.asyncio
async def test_cql_filter_declines_when_untranslatable(monkeypatch):
    """A filter that cannot be translated (e.g. unknown property) → PG path."""
    drv = _FakeEsItemsDriver(features=[], total=0)
    _patch_resolver(monkeypatch, lambda cid: drv)

    import dynastore.extensions.stac.search as _search

    async def _fake_translate(cat_id, cids, filt, driver):
        return None  # untranslatable

    monkeypatch.setattr(_search, "_translate_filter_to_es", _fake_translate)

    out = await _maybe_dispatch_to_es_search(
        "cat-x",
        _SearchRequest(
            collections=["c1"],
            filter=_search.AttributeFilter(field="x", operator="eq", value=1),
        ),
    )
    assert out is None


@pytest.mark.asyncio
async def test_dispatch_declines_without_collections(monkeypatch):
    drv = _FakeEsItemsDriver(features=[], total=0)
    _patch_resolver(monkeypatch, lambda cid: drv)
    out = await _maybe_dispatch_to_es_search("cat-x", _SearchRequest(collections=None))
    assert out is None
