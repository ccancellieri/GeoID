"""Unit tests pinning the routing-aware OGC ``/items`` dispatch contract.

The OGC API - Features and OGC API - Records ``/items`` listing endpoints
resolve the items SEARCH driver via routing
(``router.get_items_search_driver`` — SEARCH→READ fallback, #989) and dispatch
the structural query to **that driver** via the ``ItemSearchProtocol``
capability — the same mechanism STAC ``/search`` uses after #1257, so they are
no longer hardcoded to one search backend. ``maybe_dispatch_items_to_search_driver``
is the shared helper both endpoints call before falling through to the existing
PostgreSQL ``stream_items`` path.

Decision matrix (mirrors ``stac/search.py::_maybe_dispatch_to_es_search``):

* a CQL2 / shorthand attribute filter is present → ``None`` (PG path);
* the resolved driver is a read-primary fallback (PostgreSQL,
  ``Capability.QUERY_FALLBACK_SOURCE``) → ``None`` (PG path);
* the resolved driver does not implement ``ItemSearchProtocol`` → ``None``;
* otherwise dispatch via ``search_items_struct`` and return a ``QueryResponse``
  carrying read-contract ``Feature`` instances + the total (``numberMatched``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import pytest

from dynastore.extensions.tools.query import (
    maybe_dispatch_items_to_search_driver,
)
from dynastore.models.ogc import Feature
from dynastore.models.protocols.storage_driver import Capability


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
    the PostgreSQL ``stream_items`` path serve the listing.
    """

    capabilities = frozenset({Capability.READ, Capability.QUERY_FALLBACK_SOURCE})


def _patch_resolver(monkeypatch, driver):
    """Patch ``get_items_search_driver`` to return ``driver``."""
    import dynastore.modules.storage.router as _router

    async def _fake_get_items_search_driver(cat, cid=None, **_kw):
        if driver is None:
            raise ValueError("no driver")
        return _Resolved(driver=driver)

    monkeypatch.setattr(
        _router, "get_items_search_driver", _fake_get_items_search_driver
    )


async def _dispatch(monkeypatch, driver, **overrides):
    _patch_resolver(monkeypatch, driver)
    kwargs: Dict[str, Any] = dict(
        catalog_id="cat-x",
        collection_id="col-a",
        bbox=None,
        intersects=None,
        datetime=None,
        ids=None,
        limit=10,
        offset=0,
        has_complex_filter=False,
    )
    kwargs.update(overrides)
    return await maybe_dispatch_items_to_search_driver(**kwargs)


@pytest.mark.asyncio
async def test_dispatch_returns_query_response_with_features(monkeypatch):
    hit = {
        "type": "Feature",
        "id": "item-1",
        "geometry": {"type": "Point", "coordinates": [10.0, 20.0]},
        "bbox": [10.0, 20.0, 10.0, 20.0],
        "properties": {"datetime": "2024-01-01T00:00:00Z"},
        "collection": "col-a",
    }
    drv = _FakeSearchDriver(features=[hit], total=1)
    resp = await _dispatch(monkeypatch, drv)

    assert resp is not None
    assert resp.total_count == 1
    assert resp.catalog_id == "cat-x"
    assert resp.collection_id == "col-a"
    features = [f async for f in resp]
    assert len(features) == 1
    assert isinstance(features[0], Feature)
    assert features[0].id == "item-1"


@pytest.mark.asyncio
async def test_dispatch_reconstructs_read_contract(monkeypatch):
    """Raw ES ``_source`` (extras nesting, ``_*`` fields, empty geom) is
    un-projected to the GeoJSON/STAC read contract before the wire."""
    raw = {
        "type": "Feature",
        "id": "item-2",
        "geometry": {},  # empty → normalised to null
        "properties": {"datetime": "2024-01-01T00:00:00Z", "extras": {"foo": "bar"}},
        "_external_id": "leaked-internal",
        "collection": "col-a",
    }
    drv = _FakeSearchDriver(features=[raw], total=1)
    resp = await _dispatch(monkeypatch, drv)
    assert resp is not None
    features = [f async for f in resp]
    feat = features[0]
    assert feat.geometry is None  # empty {} normalised
    assert feat.properties["foo"] == "bar"  # extras hoisted to flat properties
    # internal mirror field dropped from the wire
    assert "_external_id" not in feat.properties
    assert getattr(feat, "_external_id", None) is None


@pytest.mark.asyncio
async def test_dispatch_threads_bbox_datetime_ids_pagination(monkeypatch):
    drv = _FakeSearchDriver(features=[], total=0)
    await _dispatch(
        monkeypatch,
        drv,
        bbox=[1.0, 2.0, 3.0, 4.0],
        datetime="2024-01-01/2024-12-31",
        ids=["a", "b"],
        limit=25,
        offset=50,
    )
    assert drv.calls
    call = drv.calls[0]
    assert call["collections"] == ["col-a"]
    assert call["bbox"] == [1.0, 2.0, 3.0, 4.0]
    assert call["datetime"] == "2024-01-01/2024-12-31"
    assert call["ids"] == ["a", "b"]
    assert call["limit"] == 25
    assert call["offset"] == 50


@pytest.mark.asyncio
async def test_dispatch_passes_intersects(monkeypatch):
    drv = _FakeSearchDriver(features=[], total=0)
    geom = {"type": "Point", "coordinates": [1.0, 2.0]}
    await _dispatch(monkeypatch, drv, intersects=geom)
    assert drv.calls[0]["intersects"] == geom


@pytest.mark.asyncio
async def test_dispatch_skips_malformed_hits(monkeypatch):
    # A hit whose geometry is a malformed GeoJSON object fails Feature
    # validation and is skipped; the total (numberMatched) is preserved.
    bad = {
        "type": "Feature",
        "id": "bad",
        "geometry": {"type": "NotARealGeometry", "coordinates": "nonsense"},
        "properties": {},
        "collection": "col-a",
    }
    good = {
        "type": "Feature",
        "id": "ok",
        "geometry": None,
        "properties": {},
        "collection": "col-a",
    }
    drv = _FakeSearchDriver(features=[bad, good], total=2)
    resp = await _dispatch(monkeypatch, drv)
    assert resp is not None
    assert resp.total_count == 2  # numberMatched preserved; 1 hit dropped
    features = [f async for f in resp]
    assert len(features) == 1
    assert features[0].id == "ok"


@pytest.mark.asyncio
async def test_dispatch_declines_for_pg_fallback_driver(monkeypatch):
    """A QUERY_FALLBACK_SOURCE (PG) driver → None so the PG path serves it."""
    resp = await _dispatch(monkeypatch, _FakePgFallbackDriver())
    assert resp is None


@pytest.mark.asyncio
async def test_dispatch_declines_for_non_search_driver(monkeypatch):
    """A driver that does not implement ItemSearchProtocol → None (PG path)."""

    class _NoSearch:
        capabilities = frozenset({Capability.READ})

    resp = await _dispatch(monkeypatch, _NoSearch())
    assert resp is None


@pytest.mark.asyncio
async def test_dispatch_declines_when_complex_filter_present(monkeypatch):
    """CQL2 / shorthand attribute filters → None (PG path; no translator)."""
    drv = _FakeSearchDriver(features=[], total=0)
    resp = await _dispatch(monkeypatch, drv, has_complex_filter=True)
    assert resp is None


@pytest.mark.asyncio
async def test_dispatch_declines_when_resolution_fails(monkeypatch):
    """No registered driver (resolver raises) → None (PG path)."""
    resp = await _dispatch(monkeypatch, None)
    assert resp is None


@pytest.mark.asyncio
async def test_dispatch_declines_on_driver_error(monkeypatch):
    """A search_items_struct exception degrades to the PG path, never 500s."""

    class _Boom:
        capabilities = frozenset({Capability.READ})

        async def search_items_struct(self, **kwargs):
            raise RuntimeError("ES down")

    resp = await _dispatch(monkeypatch, _Boom())
    assert resp is None
