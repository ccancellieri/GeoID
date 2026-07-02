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

"""Unit tests pinning collection expansion/resolution for STAC ``/search``.

A catalog-scoped ``/search`` with no ``collections`` parameter must still be
servable by the routing-resolved search driver. Before expansion, the
search-driver dispatch (``_maybe_dispatch_to_es_search``) declined unscoped
requests, and the PostgreSQL fallback then dropped every collection whose READ
driver carries no PG layer config — so a catalog whose items routing pins an
ES search driver answered ``numberMatched: 0`` to ``GET /search`` and to
``ids``-only lookups while the same query scoped with ``collections=`` matched.

``_expand_collections_for_search`` closes that hole: an unscoped request is
rewritten to explicitly scope ALL collections of the catalog (single
``list_collections`` round-trip, reused downstream), after which the dispatch
decision matrix applies unchanged. An empty catalog passes through untouched.

An *explicitly* scoped request goes through ``_resolve_scoped_collection_ids``
(#2786): ES canonical docs and the PG hydration path key ``collection_id`` on
the immutable INTERNAL id, so a caller-supplied EXTERNAL id must be resolved
before it reaches the search dispatch or it silently matches zero documents.
An id shaped like an internal id is rejected outright (never passed through —
that would leak the internal-id namespace onto the REST filter surface); an id
that fails to resolve as an external label is dropped the same way the PG
fallback already treats an unknown collection.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pytest

from dynastore.extensions.stac.search import (
    ItemSearchRequest,
    _expand_collections_for_search,
    _resolve_scoped_collection_ids,
)


@dataclass
class _Coll:
    id: str


class _FakeCollections:
    """``CollectionsProtocol`` stand-in — external_id -> internal_id lookup."""

    def __init__(self, external_to_internal: Dict[str, str]):
        self._map = external_to_internal
        self.calls: list = []

    async def resolve_collection_id(
        self, catalog_id: str, external_id: str, allow_missing: bool = False
    ) -> Optional[str]:
        self.calls.append({"catalog_id": catalog_id, "external_id": external_id})
        return self._map.get(external_id)


class _FakeCatalogs:
    """CatalogsProtocol stand-in — records ``list_collections`` calls."""

    def __init__(
        self,
        collection_ids: Optional[List[str]] = None,
        external_to_internal: Optional[Dict[str, str]] = None,
        internal_catalog_id: str = "cat-1",
    ):
        self._ids = collection_ids or []
        self.calls: list = []
        self.collections = _FakeCollections(external_to_internal or {})
        self._internal_catalog_id = internal_catalog_id

    async def list_collections(
        self, catalog_id: str, *args: Any, limit: int = 1000, ctx: Any = None, **kw: Any
    ) -> List[_Coll]:
        self.calls.append({"catalog_id": catalog_id, "limit": limit})
        return [_Coll(cid) for cid in self._ids]

    async def resolve_catalog_id(
        self, external_id: str, allow_missing: bool = False
    ) -> Optional[str]:
        return self._internal_catalog_id


def _request(collections: Optional[List[str]] = None) -> ItemSearchRequest:
    return ItemSearchRequest(catalog_id="cat-1", collections=collections, limit=5)


@pytest.mark.asyncio
async def test_unscoped_request_expands_to_all_catalog_collections():
    catalogs = _FakeCatalogs(["c-alpha", "c-beta"])
    req = _request(collections=None)

    out, coll_ext_id_map = await _expand_collections_for_search(
        catalogs, "cat-1", req, db_resource=None
    )

    assert out.collections == ["c-alpha", "c-beta"]
    assert catalogs.calls and catalogs.calls[0]["catalog_id"] == "cat-1"
    # The original request object is not mutated in place.
    assert req.collections is None


@pytest.mark.asyncio
async def test_empty_catalog_passes_through_unscoped():
    catalogs = _FakeCatalogs([])
    req = _request(collections=None)

    out, _ = await _expand_collections_for_search(catalogs, "cat-1", req, db_resource=None)

    assert out is req
    assert out.collections is None


@pytest.mark.asyncio
async def test_expansion_preserves_other_request_fields():
    catalogs = _FakeCatalogs(["c-alpha"])
    req = ItemSearchRequest(
        catalog_id="cat-1",
        collections=None,
        ids=["item-1"],
        datetime="2024-01-01T00:00:00Z/..",
        limit=7,
        offset=3,
    )

    out, _ = await _expand_collections_for_search(catalogs, "cat-1", req, db_resource=None)

    assert out.collections == ["c-alpha"]
    assert out.ids == ["item-1"]
    assert out.datetime == "2024-01-01T00:00:00Z/.."
    assert out.limit == 7 and out.offset == 3


# ---------------------------------------------------------------------------
# Explicit scope: external -> internal id resolution (#2786).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scoped_request_resolves_external_id_to_internal():
    catalogs = _FakeCatalogs(external_to_internal={"gaul_l1": "col_tv7476t25s3hf"})
    req = _request(collections=["gaul_l1"])

    out, coll_ext_id_map = await _expand_collections_for_search(
        catalogs, "cat-1", req, db_resource=None
    )

    assert out.collections == ["col_tv7476t25s3hf"]
    # No bulk enumeration on the explicit-scope path.
    assert catalogs.calls == []
    # Restoration map: internal -> external, for response-side hint injection.
    assert coll_ext_id_map == {"col_tv7476t25s3hf": "gaul_l1"}


@pytest.mark.asyncio
async def test_internal_shaped_id_never_passed_through():
    """An internal-id-shaped ``collections`` entry is rejected, not resolved.

    Passing it straight through would let it match its own internal-keyed
    documents directly (the leak this issue closes) — even though the fake
    resolver below would happily "resolve" it if asked.
    """
    catalogs = _FakeCatalogs(
        external_to_internal={"col_tv7476t25s3hf": "col_tv7476t25s3hf"}
    )
    req = _request(collections=["col_tv7476t25s3hf"])

    out, coll_ext_id_map = await _expand_collections_for_search(
        catalogs, "cat-1", req, db_resource=None
    )

    assert out.collections == []
    assert coll_ext_id_map == {}
    # The resolver must never even be consulted for an internal-shaped id.
    assert catalogs.collections.calls == []


@pytest.mark.asyncio
async def test_unknown_id_dropped_mixed_list_keeps_known():
    catalogs = _FakeCatalogs(external_to_internal={"gaul_l1": "col_tv7476t25s3hf"})
    req = _request(collections=["gaul_l1", "does-not-exist"])

    out, coll_ext_id_map = await _expand_collections_for_search(
        catalogs, "cat-1", req, db_resource=None
    )

    assert out.collections == ["col_tv7476t25s3hf"]
    assert coll_ext_id_map == {"col_tv7476t25s3hf": "gaul_l1"}


@pytest.mark.asyncio
async def test_resolve_scoped_collection_ids_all_unknown_returns_empty():
    catalogs = _FakeCatalogs(external_to_internal={})
    internal_ids, coll_ext_id_map = await _resolve_scoped_collection_ids(
        catalogs, "cat-1", ["nope", "also-nope"]
    )
    assert internal_ids == []
    assert coll_ext_id_map == {}
