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

"""Plain STAC collections listing must fall back to the PG-backed search
query when the routed (possibly ES) page comes back empty while PG already
has rows for the catalog.

Under job load, bulk collection creation writes to PG synchronously but the
ES secondary index write can lag or fail (job-side connection-pool
exhaustion); the plain-listing path hydrates each collection through the
routed READ driver, which can return nothing while PG — the system of
record — already has data. This must not silently render an empty listing.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Dict
from unittest.mock import AsyncMock

import pytest
from fastapi import Request

import dynastore.extensions.stac.stac_service as stac_service
from dynastore.extensions.stac.stac_service import (
    STACService,
    _pg_collections_to_stac_dicts,
)
from dynastore.models.shared_models import Collection


class _FakeCatalogsService:
    async def get_catalog(self, catalog_id, lang=None, hints=None):
        return {"id": catalog_id}


def _make_request() -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/stac/catalogs/cat-1/collections",
        "headers": [],
        "query_string": b"",
    }
    return Request(scope)


@pytest.mark.asyncio
async def test_plain_listing_falls_back_to_pg_when_es_page_empty(monkeypatch):
    """ES-routed page empty + PG count 34 -> listing returns the PG page,
    with matched=34 and returned=len(page)."""
    svc = STACService()
    svc._get_catalogs_service = AsyncMock(return_value=_FakeCatalogsService())
    svc._get_stac_config = AsyncMock(
        return_value=type("Cfg", (), {"default_limit": 10, "max_limit": 1000})()
    )

    async def _empty_catalog(*_a, **_kw) -> Dict[str, Any]:
        return {"collections": [], "links": []}

    monkeypatch.setattr(
        stac_service.stac_generator, "create_collections_catalog", _empty_catalog,
    )

    @asynccontextmanager
    async def _fake_managed_transaction(_engine):
        yield None

    monkeypatch.setattr(stac_service, "managed_transaction", _fake_managed_transaction)

    pg_page = [object(), object(), object()]  # opaque; conversion is stubbed below

    async def _fake_search_collections(_conn, search_req, **_kw):
        assert search_req.catalog_id == "cat-1"
        assert search_req.bbox is None and search_req.q is None
        # Real search_collections() resolves/clamps limit in place (see
        # resolve_page_limit); replicate that so the context assertion below
        # matches production behaviour.
        search_req.limit = 10
        return pg_page, 34

    monkeypatch.setattr(stac_service, "search_collections", _fake_search_collections)

    stac_dicts = [{"id": f"col-{i}", "type": "Collection"} for i in range(len(pg_page))]
    monkeypatch.setattr(
        stac_service, "_pg_collections_to_stac_dicts",
        lambda collections_list, language: stac_dicts,
    )

    response = await svc.list_stac_collections(
        catalog_id="cat-1",
        request=_make_request(),
        engine="fake-engine",
        language="en",
        request_hints=frozenset(),
        bbox=None,
        datetime=None,
        q=None,
        limit=None,
        offset=0,
        sortby=None,
    )

    import json
    body = json.loads(response.body)

    assert body["collections"] == stac_dicts
    assert body["context"] == {
        "limit": 10,
        "offset": 0,
        "matched": 34,
        "returned": 3,
    }


@pytest.mark.asyncio
async def test_plain_listing_no_fallback_when_page_non_empty(monkeypatch):
    """When the routed page already has collections, the PG fallback query
    must not run at all (surgical: fallback is opt-in on empty page only)."""
    svc = STACService()
    svc._get_catalogs_service = AsyncMock(return_value=_FakeCatalogsService())

    async def _populated_catalog(*_a, **_kw) -> Dict[str, Any]:
        return {"collections": [{"id": "col-a", "type": "Collection"}], "links": []}

    monkeypatch.setattr(
        stac_service.stac_generator, "create_collections_catalog", _populated_catalog,
    )

    fallback_called = False

    async def _fake_search_collections(*_a, **_kw):
        nonlocal fallback_called
        fallback_called = True
        return [], 0

    monkeypatch.setattr(stac_service, "search_collections", _fake_search_collections)

    response = await svc.list_stac_collections(
        catalog_id="cat-1",
        request=_make_request(),
        engine="fake-engine",
        language="en",
        request_hints=frozenset(),
        bbox=None,
        datetime=None,
        q=None,
        limit=None,
        offset=0,
        sortby=None,
    )

    import json
    body = json.loads(response.body)

    assert fallback_called is False
    assert body["collections"] == [{"id": "col-a", "type": "Collection"}]
    assert "context" not in body


def test_pg_collections_to_stac_dicts_keeps_null_extent_rows():
    """Harvested collections persist ``extent = NULL`` in PG (harvest does
    not aggregate/persist collection extents). The converter must render a
    STAC-valid default extent for those rows instead of dropping them —
    otherwise a page the caller already counted in ``matched`` comes back
    with ``returned=0``, breaking the pagination contract."""
    no_extent_coll = Collection(
        id="harvested-1",
        description="harvested, no aggregated extent",
        license="proprietary",
        extent=None,
    )
    real_extent_coll = Collection(
        id="authored-1",
        description="has a real extent",
        license="proprietary",
        extent={
            "spatial": {"bbox": [[10.0, 20.0, 30.0, 40.0]]},
            "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
        },
    )

    stac_dicts = _pg_collections_to_stac_dicts(
        [no_extent_coll, real_extent_coll], language="en",
    )

    assert [d["id"] for d in stac_dicts] == ["harvested-1", "authored-1"]

    no_extent_dict = stac_dicts[0]
    assert no_extent_dict["extent"]["spatial"]["bbox"] == [[-180.0, -90.0, 180.0, 90.0]]
    assert no_extent_dict["extent"]["temporal"]["interval"] == [[None, None]]

    real_extent_dict = stac_dicts[1]
    assert real_extent_dict["extent"]["spatial"]["bbox"] == [[10.0, 20.0, 30.0, 40.0]]
