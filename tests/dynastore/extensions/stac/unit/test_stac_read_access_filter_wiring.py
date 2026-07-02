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

"""STAC read entry points thread the row-level ABAC envelope read filter (#1311).

Item 2 of #1311 wires the remaining STAC read paths to the same shared access
mechanism the OGC Features / Records ``/items`` endpoints and STAC ``/search``
already use (PR #1314). These tests pin the wiring at the handler boundary —
that the HTTP ``request`` (Features/Records-style dispatch) or the extracted
principals (STAC ``/search``-style) reach the shared helper — without standing
up a live database or ES backend. The compile/translate behaviour itself is
pinned by ``tests/.../tools/unit/test_items_search_driver_dispatch.py`` and
``tests/.../search/unit/test_search_access_filter.py``; here we only prove the
three STAC handlers actually reach those seams.

Paths covered:

* ``get_virtual_asset_items`` (virtual-asset view) — dispatches via
  ``maybe_dispatch_items_to_search_driver`` with the HTTP ``request`` and the
  ``asset_id`` structural filter, falling back to PG ``stream_items``.
* ``search_virtual_hierarchy_items`` — extracts ``(principals, principal)`` from
  request state and passes them into ``search_items``.
* ``get_stac_collection_items`` (collection ``/items``) — dispatches via
  ``maybe_dispatch_items_to_search_driver`` with the HTTP ``request`` and passes
  the result to ``create_item_collection``.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace

import dynastore.extensions.stac.stac_service as stac_service_mod
import dynastore.extensions.stac.stac_virtual as stac_virtual_mod
import dynastore.extensions.tools.query as query_mod
from dynastore.extensions.stac.stac_service import STACService


def _request(principal_id="user:alice", principal_role=("reader",), path="/items"):
    """A Starlette-ish request stub carrying IAM-middleware state + a real URL.

    A genuine ``starlette.datastructures.URL`` is used so ``get_url`` (paging
    link construction) works without a live ASGI scope.
    """
    from starlette.datastructures import URL

    return SimpleNamespace(
        state=SimpleNamespace(
            principal="P",
            principal_id=principal_id,
            principal_role=list(principal_role),
        ),
        query_params={},
        url=URL(f"http://t{path}"),
    )


@asynccontextmanager
async def _fake_txn(_engine):
    # Yield None so ``DriverContext(db_resource=None)`` validates (a real
    # SimpleNamespace fails the DbResource union); collaborators are patched.
    yield None


# ---------------------------------------------------------------------------
# Item 3: collection /items dispatches with the HTTP request.
# ---------------------------------------------------------------------------

async def test_get_stac_collection_items_dispatches_with_request(monkeypatch):
    svc = STACService.__new__(STACService)

    catalogs = SimpleNamespace(
        get_collection=_acoro({"id": "col-a"}),
    )
    monkeypatch.setattr(svc, "_get_catalogs_service", _acoro(catalogs))
    monkeypatch.setattr(
        svc, "_get_stac_config",
        _acoro(SimpleNamespace(default_limit=10, max_limit=1000)),
    )
    monkeypatch.setattr(stac_service_mod, "managed_transaction", _fake_txn)

    seen: dict = {}

    async def _spy_dispatch(**kwargs):
        seen["dispatch"] = kwargs
        return "QR"  # non-None sentinel → threaded into create_item_collection

    # The handler imports the helper from ``dynastore.extensions.tools.query``
    # at call time, so patch it at the source module.
    monkeypatch.setattr(
        query_mod, "maybe_dispatch_items_to_search_driver", _spy_dispatch,
    )

    async def _spy_collection(*args, **kwargs):
        seen["collection"] = kwargs
        return {"type": "FeatureCollection", "features": []}

    monkeypatch.setattr(
        stac_service_mod.stac_generator, "create_item_collection", _spy_collection,
    )

    req = _request()
    await svc.get_stac_collection_items(
        catalog_id="cat",
        collection_id="col_a",
        request=req,
        engine=object(),
        limit=10,
        offset=0,
        filter=None,
        language="en",
    )

    # The HTTP request is threaded into the shared dispatch helper (the helper
    # then compiles the access filter for an envelope driver — pinned elsewhere).
    assert seen["dispatch"]["request"] is req
    assert seen["dispatch"]["has_complex_filter"] is False
    # The dispatch result is forwarded to the collection builder, which prefers
    # it over the PG ``get_stac_items_paginated`` fallback.
    assert seen["collection"]["search_dispatch"] == "QR"


async def test_get_stac_collection_items_skips_dispatch_with_filter(monkeypatch):
    """A CQL/shorthand filter present → no SEARCH-driver dispatch (PG path),
    byte-for-byte unchanged for the existing filtered listing."""
    svc = STACService.__new__(STACService)

    catalogs = SimpleNamespace(get_collection=_acoro({"id": "col-a"}))
    monkeypatch.setattr(svc, "_get_catalogs_service", _acoro(catalogs))
    monkeypatch.setattr(
        svc, "_get_stac_config",
        _acoro(SimpleNamespace(default_limit=10, max_limit=1000)),
    )
    monkeypatch.setattr(stac_service_mod, "managed_transaction", _fake_txn)

    called = {"dispatch": False}

    async def _boom_dispatch(**kwargs):
        called["dispatch"] = True
        return None

    monkeypatch.setattr(
        query_mod, "maybe_dispatch_items_to_search_driver", _boom_dispatch,
    )

    seen: dict = {}

    async def _spy_collection(*args, **kwargs):
        seen["collection"] = kwargs
        return {}

    monkeypatch.setattr(
        stac_service_mod.stac_generator, "create_item_collection", _spy_collection,
    )

    await svc.get_stac_collection_items(
        catalog_id="cat",
        collection_id="col_a",
        request=_request(),
        engine=object(),
        limit=10,
        offset=0,
        filter="prop = 'x'",
        language="en",
    )

    assert called["dispatch"] is False
    # No dispatch → the PG path serves the listing (search_dispatch stays None).
    assert seen["collection"]["search_dispatch"] is None


# ---------------------------------------------------------------------------
# Item 1: virtual-asset view dispatches with the HTTP request + asset_id filter.
# ---------------------------------------------------------------------------

async def test_get_virtual_asset_items_dispatches_with_request(monkeypatch):
    svc = STACService.__new__(STACService)

    catalogs = SimpleNamespace(get_collection_config=_acoro(SimpleNamespace()))
    monkeypatch.setattr(svc, "_get_catalogs_service", _acoro(catalogs))
    monkeypatch.setattr(
        svc, "_get_stac_config",
        _acoro(SimpleNamespace(default_limit=10, max_limit=1000)),
    )
    monkeypatch.setattr(stac_virtual_mod, "managed_transaction", _fake_txn)

    # An ItemsProtocol whose stream_items would explode — proving the SEARCH
    # dispatch served the listing and the PG fallback was not reached.
    class _ItemsBoom:
        async def stream_items(self, *a, **k):
            raise AssertionError("PG stream_items fallback must not run")

    monkeypatch.setattr(stac_virtual_mod, "get_protocol", lambda _p: _ItemsBoom())

    seen: dict = {}

    async def _empty_aiter():
        return
        yield  # pragma: no cover — make it an async generator

    async def _spy_dispatch(**kwargs):
        seen["dispatch"] = kwargs
        return SimpleNamespace(items=_empty_aiter(), total_count=0)

    monkeypatch.setattr(
        query_mod, "maybe_dispatch_items_to_search_driver", _spy_dispatch,
    )

    req = _request(path="/c/col_a/assets/a1/items")

    await svc.get_virtual_asset_items(
        catalog_id="cat",
        collection_id="col_a",
        asset_id="a1",
        request=req,
        engine=object(),
        limit=10,
        offset=0,
        language="en",
    )

    # Request threaded; the asset_id equality threaded through ``filters``.
    assert seen["dispatch"]["request"] is req
    filters = seen["dispatch"]["filters"]
    assert len(filters) == 1
    assert filters[0].field == "asset_id"
    assert filters[0].value == "a1"


# ---------------------------------------------------------------------------
# Item 2: hierarchy search threads principals into search_items.
# ---------------------------------------------------------------------------

async def test_search_virtual_hierarchy_items_threads_principals(monkeypatch):
    svc = STACService.__new__(STACService)

    monkeypatch.setattr(stac_virtual_mod, "managed_transaction", _fake_txn)

    hierarchy = SimpleNamespace(enabled=True)
    stac_config = SimpleNamespace(hierarchy=hierarchy)
    configs = SimpleNamespace(get_config=_acoro(stac_config))
    monkeypatch.setattr(svc, "_get_configs_service", _acoro(configs))
    monkeypatch.setattr(
        svc, "_resolve_hierarchy_rule", lambda *a, **k: SimpleNamespace()
    )

    # Patch the hierarchy-queries + search modules the handler imports lazily.
    import dynastore.extensions.stac.stac_hierarchy_queries as hq
    import dynastore.extensions.stac.search as search_mod

    monkeypatch.setattr(
        hq, "build_hierarchy_where_clause", lambda *a, **k: "1=1"
    )

    seen: dict = {}

    async def _spy_search_items(conn, search_request, stac_config, **kwargs):
        seen["search"] = kwargs
        return [], 0, None

    monkeypatch.setattr(search_mod, "search_items", _spy_search_items)

    # search_items returns no rows, so the post-search mapping loop is a no-op;
    # the handler still asserts the items/catalogs protocols resolve, so return a
    # dummy for any protocol lookup.
    monkeypatch.setattr(
        stac_virtual_mod, "get_protocol", lambda _p: SimpleNamespace()
    )

    # Generator gather over an empty feature list is a no-op. offset/limit are
    # read for the paging links at the end of the handler.
    search_request = SimpleNamespace(
        catalog_id=None, collections=None, offset=0, limit=10,
    )

    await svc.search_virtual_hierarchy_items(
        hierarchy_id="h1",
        catalog_id="cat",
        collection_id="col_a",
        search_request=search_request,
        request=_request(),
        parent_value=None,
        engine=object(),
        language="en",
    )

    # Principals derived from request state are forwarded; the anonymous-role
    # token (here ``reader``) is no longer dropped.
    assert seen["search"]["principals"] == ["user:alice", "reader"]
    assert seen["search"]["principal"] == "P"


# ---------------------------------------------------------------------------
# Small async helper.
# ---------------------------------------------------------------------------

def _acoro(value):
    async def _coro(*args, **kwargs):
        return value
    return _coro
