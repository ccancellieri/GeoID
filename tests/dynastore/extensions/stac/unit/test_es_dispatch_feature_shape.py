"""Unit tests pinning the routing-aware STAC ``/search`` dispatch contract.

``_maybe_dispatch_to_es_search`` resolves the items SEARCH driver via routing
(``router.get_items_search_driver`` — SEARCH→READ fallback, #989) and
dispatches the structural query to **that driver** via the
``ItemSearchProtocol`` capability. It no longer hardcodes the public
Elasticsearch class, so:

* a catalog routing SEARCH to the tenant-private ES driver is honoured;
* a catalog with no dedicated search driver (PG ``QUERY_FALLBACK_SOURCE``)
  falls through to the PostgreSQL path (dispatch returns ``None``);
* mixed drivers across collections fall through to PG.

The downstream serializer reads ``feature.properties["_catalog_id" |
"_collection_id"]`` on each hit, so the dispatch must return ``Feature``
pydantic instances with those markers injected — the shape the PG path emits.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pytest

from dynastore.extensions.stac.search import _maybe_dispatch_to_es_search
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.shared_models import Feature


@dataclass
class _StubItemSearchResult:
    features: List[Dict[str, Any]]
    total: int = 0


@dataclass
class _Resolved:
    """Stand-in for ``ResolvedDriver`` — the dispatcher reads ``.driver``."""

    driver: Any


class _FakeSearchDriver:
    """A search-capable items driver (satisfies ``ItemSearchProtocol``).

    Stands in for either the public or the tenant-private ES driver — the
    dispatcher treats both identically because it dispatches through the
    capability, not the concrete class.
    """

    capabilities = frozenset({Capability.READ})

    def __init__(self, features: List[Dict[str, Any]], total: int):
        self._result = _StubItemSearchResult(features=features, total=total)
        self.calls: list = []

    async def search_items_struct(self, **kwargs):
        self.calls.append(kwargs)
        return self._result


class _FakePgFallbackDriver:
    """A read-primary driver with no dedicated search backend — the PG case.

    Advertises ``QUERY_FALLBACK_SOURCE`` so the dispatcher declines and lets
    the PostgreSQL ``QueryOptimizer`` path serve the query.
    """

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
    hit = {
        "type": "Feature",
        "id": "item-1",
        "geometry": {"type": "Point", "coordinates": [10.0, 20.0]},
        "bbox": [10.0, 20.0, 10.0, 20.0],
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
        "collection": "col-a",
    }
    drv = _FakeSearchDriver(features=[hit], total=1)
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
    hit = {
        "type": "Feature",
        "id": "item-2",
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
        "collection": "col-b",
    }
    drv = _FakeSearchDriver(features=[hit], total=1)
    _patch_resolver(monkeypatch, lambda cid: drv)

    out = await _maybe_dispatch_to_es_search("cat-y", _SearchRequest(collections=["col-b"]))
    assert out is not None
    features, _total, _aggs = out
    assert features[0].properties["_catalog_id"] == "cat-y"
    assert features[0].properties["_collection_id"] == "col-b"


@pytest.mark.asyncio
async def test_dispatch_falls_back_to_request_collection_when_missing(monkeypatch):
    hit = {
        "type": "Feature",
        "id": "legacy-item",
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
    }
    drv = _FakeSearchDriver(features=[hit], total=1)
    _patch_resolver(monkeypatch, lambda cid: drv)

    out = await _maybe_dispatch_to_es_search("cat-z", _SearchRequest(collections=["only-one"]))
    assert out is not None
    features, _, _ = out
    assert features[0].properties["_catalog_id"] == "cat-z"
    assert features[0].properties["_collection_id"] == "only-one"


@pytest.mark.asyncio
async def test_dispatch_skips_malformed_hits(monkeypatch):
    bad = {"type": "Feature", "properties": {}}  # no id
    good = {
        "type": "Feature",
        "id": "ok",
        "geometry": None,
        "properties": {},
        "collection": "c1",
    }
    drv = _FakeSearchDriver(features=[bad, good], total=2)
    _patch_resolver(monkeypatch, lambda cid: drv)

    out = await _maybe_dispatch_to_es_search("cat-t", _SearchRequest(collections=["c1"]))
    assert out is not None
    features, total, _ = out
    assert total == 2  # numberMatched preserved; 1 hit dropped
    assert len(features) == 1
    assert features[0].id == "ok"


@pytest.mark.asyncio
async def test_dispatch_threads_offset_to_driver(monkeypatch):
    """STAC ``offset`` must reach the driver (paginates past page 1)."""
    drv = _FakeSearchDriver(features=[], total=0)
    _patch_resolver(monkeypatch, lambda cid: drv)

    await _maybe_dispatch_to_es_search(
        "cat-x", _SearchRequest(collections=["c1"], limit=25, offset=50)
    )
    assert drv.calls and drv.calls[0]["offset"] == 50
    assert drv.calls[0]["limit"] == 25


@pytest.mark.asyncio
async def test_dispatch_declines_for_pg_fallback_driver(monkeypatch):
    """A QUERY_FALLBACK_SOURCE (PG) driver → dispatch returns None so the
    PostgreSQL path serves the search."""
    _patch_resolver(monkeypatch, lambda cid: _FakePgFallbackDriver())
    out = await _maybe_dispatch_to_es_search("cat-x", _SearchRequest(collections=["c1"]))
    assert out is None


@pytest.mark.asyncio
async def test_dispatch_declines_for_mixed_drivers(monkeypatch):
    """Heterogeneous SEARCH drivers across collections → None (PG path)."""
    es = _FakeSearchDriver(features=[], total=0)
    pg = _FakePgFallbackDriver()
    mapping = {"c1": es, "c2": pg}
    _patch_resolver(monkeypatch, lambda cid: mapping[cid])
    out = await _maybe_dispatch_to_es_search(
        "cat-x", _SearchRequest(collections=["c1", "c2"])
    )
    assert out is None


@pytest.mark.asyncio
async def test_dispatch_declines_when_cql_filter_present(monkeypatch):
    drv = _FakeSearchDriver(features=[], total=0)
    _patch_resolver(monkeypatch, lambda cid: drv)
    out = await _maybe_dispatch_to_es_search(
        "cat-x", _SearchRequest(collections=["c1"], filter={"op": "=", "args": []})
    )
    assert out is None


@pytest.mark.asyncio
async def test_dispatch_declines_without_collections(monkeypatch):
    drv = _FakeSearchDriver(features=[], total=0)
    _patch_resolver(monkeypatch, lambda cid: drv)
    out = await _maybe_dispatch_to_es_search("cat-x", _SearchRequest(collections=None))
    assert out is None
