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

"""Regression tests for #2865: STAC ``/search`` hung 45s+ on a catalog with
~2,000 collections.

Root cause: an unscoped ``/search`` (no ``collections=`` in the request) is
auto-expanded by ``_expand_collections_for_search`` to every collection of
the catalog, and ``_maybe_dispatch_to_es_search`` used to resolve the items
SEARCH driver ONCE PER COLLECTION in that expanded set purely to verify they
all shared the same driver class before dispatching a single query — an
O(collections) sequence of awaited config/routing lookups executed before
any search query ran. On a cold cache after a large harvest this is an
O(collections) sequence of round trips; ~2,000 of them is exactly the
45s+ hang observed live.

The fix resolves the driver ONCE at catalog scope (``collection_id=None``)
for an auto-expanded (implicit) scope, and keeps the existing per-collection
verification only for an explicit, caller-bounded ``collections=`` filter.
These tests pin that O(1)-call contract for the auto-expanded path while
proving the explicit-scope path (and its mixed-driver PG fallback) is
unchanged.
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
    is_es_items_driver = True
    supports_cql_es = True
    capabilities = frozenset({Capability.READ})

    def __init__(self):
        self.read_calls: list = []
        self.count_calls: list = []

    async def read_entities(
        self, catalog_id, collection_id, *,
        entity_ids=None, request=None, context=None,
        limit=100, offset=0, db_resource=None,
    ):
        self.read_calls.append({"collection_id": collection_id})
        return
        yield  # pragma: no cover — make this an async generator

    async def count_entities(
        self, catalog_id, collection_id, *, request=None, db_resource=None,
    ):
        self.count_calls.append({"collection_id": collection_id})
        return 0


class _FakePgFallbackDriver:
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


def _patch_counting_resolver(monkeypatch, driver_for):
    """Patch ``get_items_search_driver`` and record every call's ``cid``."""
    import dynastore.modules.storage.router as _router

    calls: List[Optional[str]] = []

    async def _fake_get_items_search_driver(cat, cid=None, **_kw):
        calls.append(cid)
        drv = driver_for(cid)
        if drv is None:
            raise ValueError("no driver")
        return _Resolved(driver=drv)

    monkeypatch.setattr(
        _router, "get_items_search_driver", _fake_get_items_search_driver
    )
    return calls


@pytest.mark.asyncio
async def test_auto_expanded_scope_resolves_driver_once_for_many_collections(monkeypatch):
    """The reported #2865 shape: catalog-wide auto-expansion must not loop
    the driver resolution once per collection."""
    drv = _FakeEsItemsDriver()
    calls = _patch_counting_resolver(monkeypatch, lambda cid: drv)

    many_cids = [f"col-{i}" for i in range(500)]
    out = await _maybe_dispatch_to_es_search(
        "fao", _SearchRequest(collections=many_cids), had_explicit_scope=False,
    )

    assert out is not None
    # Exactly one catalog-level resolution — O(1), not O(collections).
    assert calls == [None]
    # The driver's own read_entities/count_entities is still invoked exactly
    # once (positional cid[0], per the multi-collection dispatch contract) —
    # the full collection set reaches it via ``request.collections``, unaffected.
    assert len(drv.read_calls) == 1
    assert drv.read_calls[0]["collection_id"] == many_cids[0]


@pytest.mark.asyncio
async def test_auto_expanded_scope_declines_for_pg_fallback_driver(monkeypatch):
    """Catalog-level resolution still defers to PG when the catalog has no
    dedicated search backend — unchanged decision, just resolved once."""
    calls = _patch_counting_resolver(monkeypatch, lambda cid: _FakePgFallbackDriver())

    many_cids = [f"col-{i}" for i in range(500)]
    out = await _maybe_dispatch_to_es_search(
        "cat-pg", _SearchRequest(collections=many_cids), had_explicit_scope=False,
    )

    assert out is None
    assert calls == [None]


@pytest.mark.asyncio
async def test_explicit_scope_still_verifies_every_collection(monkeypatch):
    """An explicit ``collections=`` filter is caller-bounded, so the existing
    per-collection homogeneity check is preserved unchanged."""
    drv = _FakeEsItemsDriver()
    cids = ["c1", "c2", "c3"]
    calls = _patch_counting_resolver(monkeypatch, lambda cid: drv)

    out = await _maybe_dispatch_to_es_search(
        "cat-x", _SearchRequest(collections=cids), had_explicit_scope=True,
    )

    assert out is not None
    assert calls == cids


@pytest.mark.asyncio
async def test_explicit_scope_still_falls_back_on_mixed_drivers(monkeypatch):
    """Heterogeneous explicit selection still declines to PG — unchanged."""
    es = _FakeEsItemsDriver()
    pg = _FakePgFallbackDriver()
    mapping = {"c1": es, "c2": pg}
    _patch_counting_resolver(monkeypatch, lambda cid: mapping[cid])

    out = await _maybe_dispatch_to_es_search(
        "cat-x", _SearchRequest(collections=["c1", "c2"]), had_explicit_scope=True,
    )
    assert out is None


@pytest.mark.asyncio
async def test_default_had_explicit_scope_preserves_prior_behavior(monkeypatch):
    """``had_explicit_scope`` defaults True — callers that never pass it (any
    other/future caller) keep the original per-collection verification."""
    drv = _FakeEsItemsDriver()
    cids = ["a", "b"]
    calls = _patch_counting_resolver(monkeypatch, lambda cid: drv)

    out = await _maybe_dispatch_to_es_search("cat-x", _SearchRequest(collections=cids))

    assert out is not None
    assert calls == cids
