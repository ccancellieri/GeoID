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

"""``search_items`` boundary tests for external->internal collection id
resolution (#2786).

ES canonical docs and the PG hydration path key ``collection_id`` on the
immutable INTERNAL id (#2653). The STAC ``collections=`` filter is public and
therefore external, so it must be resolved before it reaches either the ES
search-driver dispatch or the PostgreSQL fallback, or it silently matches zero
documents. These tests pin the ``search_items`` boundary behaviour without
standing up a live database or ES backend — the resolution logic itself is
pinned in ``test_search_collection_expansion.py``.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest

import dynastore.extensions.stac.search as search_mod
from dynastore.extensions.stac.search import ItemSearchRequest, search_items


class _FakeCollections:
    def __init__(self, external_to_internal: Dict[str, str]):
        self._map = external_to_internal

    async def resolve_collection_id(
        self, catalog_id: str, external_id: str, allow_missing: bool = False
    ) -> Optional[str]:
        return self._map.get(external_id)


class _FakeCatalogs:
    def __init__(self, external_to_internal: Optional[Dict[str, str]] = None):
        self.collections = _FakeCollections(external_to_internal or {})
        self.list_collections_calls: list = []

    async def resolve_catalog_id(
        self, external_id: str, allow_missing: bool = False
    ) -> Optional[str]:
        return "cat-int"

    async def list_collections(self, catalog_id: str, *a: Any, **kw: Any) -> List[Any]:
        self.list_collections_calls.append(catalog_id)
        return []


def _stac_config() -> SimpleNamespace:
    return SimpleNamespace(default_limit=10, max_limit=1000)


@pytest.mark.asyncio
async def test_search_items_all_unknown_collections_returns_empty_without_full_catalog_fallback(
    monkeypatch,
):
    """A request scoped to only unresolvable ids must not silently widen to
    every collection in the catalog — that would leak data the caller never
    asked for."""
    catalogs = _FakeCatalogs(external_to_internal={})
    monkeypatch.setattr(search_mod, "get_protocol", lambda _p: catalogs)

    async def _boom_dispatch(*a, **kw):
        raise AssertionError("ES dispatch must not run for an all-unknown scope")

    monkeypatch.setattr(search_mod, "_maybe_dispatch_to_es_search", _boom_dispatch)

    req = ItemSearchRequest(catalog_id="cat", collections=["nope", "also-nope"])
    result = await search_items(None, req, _stac_config())

    assert result == ([], 0, None)
    # No full-catalog ``list_collections`` round-trip was triggered as a
    # fallback for the failed explicit scope.
    assert catalogs.list_collections_calls == []


@pytest.mark.asyncio
async def test_search_items_passes_resolved_internal_ids_to_es_dispatch(monkeypatch):
    catalogs = _FakeCatalogs(external_to_internal={"gaul_l1": "col_int_1"})
    monkeypatch.setattr(search_mod, "get_protocol", lambda _p: catalogs)

    seen: dict = {}

    async def _spy_dispatch(cat_id, search_request, **kwargs):
        seen["cat_id"] = cat_id
        seen["collections"] = search_request.collections
        seen["coll_ext_id_map"] = kwargs.get("coll_ext_id_map")
        return ([], 0, None)

    monkeypatch.setattr(search_mod, "_maybe_dispatch_to_es_search", _spy_dispatch)

    req = ItemSearchRequest(catalog_id="cat", collections=["gaul_l1"])
    result = await search_items(None, req, _stac_config())

    assert result == ([], 0, None)
    assert seen["collections"] == ["col_int_1"]
    assert seen["coll_ext_id_map"] == {"col_int_1": "gaul_l1"}


@pytest.mark.asyncio
async def test_search_items_mixed_known_unknown_scopes_dispatch_to_known_only(monkeypatch):
    catalogs = _FakeCatalogs(external_to_internal={"gaul_l1": "col_int_1"})
    monkeypatch.setattr(search_mod, "get_protocol", lambda _p: catalogs)

    seen: dict = {}

    async def _spy_dispatch(cat_id, search_request, **kwargs):
        seen["collections"] = search_request.collections
        return ([], 0, None)

    monkeypatch.setattr(search_mod, "_maybe_dispatch_to_es_search", _spy_dispatch)

    req = ItemSearchRequest(catalog_id="cat", collections=["gaul_l1", "unknown-coll"])
    await search_items(None, req, _stac_config())

    assert seen["collections"] == ["col_int_1"]
