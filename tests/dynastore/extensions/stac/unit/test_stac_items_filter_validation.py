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

"""``get_stac_collection_items`` ad-hoc ``?{property}={value}`` filter
validation (#2682).

Mirrors the OGC Features ``/items`` fix: an ad-hoc filter parameter with no
queryable mapping is rejected with HTTP 400 before any driver dispatch, using
the same SSOT (``resolve_queryable_property_names`` — the collection's field
defs merged with ``canonical_queryable_properties()``, exactly what
``/queryables`` advertises) that the Features endpoint uses.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException
from starlette.datastructures import URL

import dynastore.extensions.stac.stac_service as stac_service_mod
import dynastore.extensions.tools.query as query_mod
from dynastore.extensions.stac.stac_service import STACService


def _request(query_params: dict, path: str = "/stac/catalogs/cat/collections/col/items"):
    qs = "&".join(f"{k}={v}" for k, v in query_params.items())
    return SimpleNamespace(
        state=SimpleNamespace(
            principal="P", principal_id="user:alice", principal_role=["reader"],
        ),
        query_params=query_params,
        url=URL(f"http://t{path}" + (f"?{qs}" if qs else "")),
    )


@asynccontextmanager
async def _fake_txn(_engine):
    yield None


def _acoro(value: Any):
    async def _coro(*args: Any, **kwargs: Any):
        return value

    return _coro


async def test_unknown_filter_param_returns_400_naming_the_param(monkeypatch):
    """An ad-hoc filter with no queryable mapping 400s before the SEARCH-driver
    dispatch / PG fallback, so the count and the listing are never asked to
    describe two different selections."""
    svc = STACService.__new__(STACService)
    catalogs = SimpleNamespace(get_collection=_acoro({"id": "col-a"}))
    monkeypatch.setattr(svc, "_get_catalogs_service", _acoro(catalogs))
    monkeypatch.setattr(stac_service_mod, "managed_transaction", _fake_txn)

    async def _valid_names(catalog_id, collection_id):
        return {"title", "country"}

    monkeypatch.setattr(query_mod, "resolve_queryable_property_names", _valid_names)

    async def _no_dispatch(**kwargs: Any):
        raise AssertionError("dispatch must not be called for an unknown filter")

    monkeypatch.setattr(
        query_mod, "maybe_dispatch_items_to_search_driver", _no_dispatch
    )

    with pytest.raises(HTTPException) as excinfo:
        await svc.get_stac_collection_items(
            catalog_id="cat",
            collection_id="col_a",
            request=_request({"GAUL1_CODE": "3296"}),
            engine=object(),
            limit=10,
            offset=0,
            filter=None,
            language="en",
            request_hints=frozenset(),
        )
    assert excinfo.value.status_code == 400
    assert "GAUL1_CODE" in str(excinfo.value.detail)


async def test_mapped_filter_param_reaches_create_item_collection(monkeypatch):
    """A filter name present in the resolved queryable surface is accepted
    and threaded into the shared ``cql_filter`` string passed downstream."""
    svc = STACService.__new__(STACService)
    catalogs = SimpleNamespace(get_collection=_acoro({"id": "col-a"}))
    monkeypatch.setattr(svc, "_get_catalogs_service", _acoro(catalogs))
    monkeypatch.setattr(
        svc, "_get_stac_config",
        _acoro(SimpleNamespace(default_limit=10, max_limit=1000)),
    )
    monkeypatch.setattr(stac_service_mod, "managed_transaction", _fake_txn)

    async def _valid_names(catalog_id, collection_id):
        return {"title", "country"}

    monkeypatch.setattr(query_mod, "resolve_queryable_property_names", _valid_names)

    seen: dict = {}

    async def _spy_collection(*args: Any, **kwargs: Any) -> dict:
        seen["cql_filter"] = kwargs.get("cql_filter")
        return {"type": "FeatureCollection", "features": []}

    monkeypatch.setattr(
        stac_service_mod.stac_generator, "create_item_collection", _spy_collection
    )

    await svc.get_stac_collection_items(
        catalog_id="cat",
        collection_id="col_a",
        request=_request({"country": "IT"}),
        engine=object(),
        limit=10,
        offset=0,
        filter=None,
        language="en",
        request_hints=frozenset(),
    )

    assert seen["cql_filter"] is not None
    assert "country" in seen["cql_filter"]


async def test_canonical_queryable_name_accepted_as_filter(monkeypatch):
    """A canonical system/stats lane (absent from the collection's own field
    defs but present in ``canonical_queryable_properties()``) is accepted as
    an ad-hoc filter, mirroring the OGC Features endpoint (refs #2230/#2235)."""
    svc = STACService.__new__(STACService)
    catalogs = SimpleNamespace(get_collection=_acoro({"id": "col-a"}))
    monkeypatch.setattr(svc, "_get_catalogs_service", _acoro(catalogs))
    monkeypatch.setattr(
        svc, "_get_stac_config",
        _acoro(SimpleNamespace(default_limit=10, max_limit=1000)),
    )
    monkeypatch.setattr(stac_service_mod, "managed_transaction", _fake_txn)

    async def _valid_names(catalog_id, collection_id):
        from dynastore.modules.elasticsearch.mappings import (
            canonical_queryable_properties,
        )

        return {"title"} | canonical_queryable_properties().keys()

    monkeypatch.setattr(query_mod, "resolve_queryable_property_names", _valid_names)

    seen: dict = {}

    async def _spy_collection(*args: Any, **kwargs: Any) -> dict:
        seen["cql_filter"] = kwargs.get("cql_filter")
        return {"type": "FeatureCollection", "features": []}

    monkeypatch.setattr(
        stac_service_mod.stac_generator, "create_item_collection", _spy_collection
    )

    # No exception → "area" (canonical) is an accepted filter.
    await svc.get_stac_collection_items(
        catalog_id="cat",
        collection_id="col_a",
        request=_request({"area": "42"}),
        engine=object(),
        limit=10,
        offset=0,
        filter=None,
        language="en",
        request_hints=frozenset(),
    )
    assert seen["cql_filter"] is not None
    assert "area" in seen["cql_filter"]
