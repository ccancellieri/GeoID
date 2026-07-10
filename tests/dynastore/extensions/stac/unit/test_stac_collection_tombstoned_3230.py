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

"""Regression coverage for #3230: STAC collection-level routes must not
resurrect a tombstoned (soft-deleted) catalog's collection.

Companion to #3166/PR #3228 (catalog-level fix) and #3164 (STAC direct
catalog GET). These two collection-level routes already 404'd a tombstoned
catalog, but only as a side effect of
``resolve_physical_schema(allow_missing=False)`` raising a ``ValueError``
that a generic message-sniffing exception handler happened to downgrade to
404 — nothing pinned that contract at the route level. These tests pin
``GET /stac/catalogs/{cat}/collections/{col}`` and
``GET /stac/catalogs/{cat}/collections/{col}/items`` directly against the
shared resolver (``_resolve_collection_or_404``), independent of the
message text a lower layer happens to raise.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException, Request

import dynastore.extensions.stac.stac_service as stac_service


def _make_request(path: str) -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "headers": [],
        "query_string": b"",
        "server": ("testserver", 80),
    }
    return Request(scope)


class _FakeCatalogsService:
    """Stands in for the ``CatalogsProtocol`` reached by ``_resolve_collection_or_404``.

    A tombstoned catalog reaches ``get_collection`` via
    ``resolve_physical_schema(allow_missing=False)``, which raises
    ``ValueError`` for a soft-deleted (or missing) catalog — mirrored here
    directly rather than re-deriving the whole DB chain.
    """

    def __init__(self, *, collection: Any = None, tombstoned: bool = False) -> None:
        if tombstoned:
            async def _raise(*_a: Any, **_kw: Any) -> Any:
                raise ValueError("Catalog 'deleted-cat' not found.")

            self.get_collection = AsyncMock(side_effect=_raise)
        else:
            self.get_collection = AsyncMock(return_value=collection)


def _svc(monkeypatch, catalogs_svc: _FakeCatalogsService):
    from dynastore.extensions.stac.stac_service import STACService

    svc = STACService.__new__(STACService)

    async def _get_catalogs_service():
        return catalogs_svc

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs_service, raising=False)

    @asynccontextmanager
    async def _fake_managed_transaction(_engine):
        yield None

    monkeypatch.setattr(stac_service, "managed_transaction", _fake_managed_transaction)
    return svc


@pytest.mark.asyncio
async def test_get_stac_collection_404s_a_tombstoned_catalog(monkeypatch):
    catalogs_svc = _FakeCatalogsService(tombstoned=True)
    svc = _svc(monkeypatch, catalogs_svc)

    with pytest.raises(HTTPException) as exc_info:
        await svc.get_stac_collection(
            catalog_id="deleted-cat",
            collection_id="col-a",
            request=_make_request("/stac/catalogs/deleted-cat/collections/col-a"),
            language="en",
            request_hints=frozenset(),
        )

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_get_stac_collection_items_404s_a_tombstoned_catalog(monkeypatch):
    """The read-item-listing sibling: must 404 before any driver dispatch."""
    catalogs_svc = _FakeCatalogsService(tombstoned=True)
    svc = _svc(monkeypatch, catalogs_svc)

    with pytest.raises(HTTPException) as exc_info:
        await svc.get_stac_collection_items(
            catalog_id="deleted-cat",
            collection_id="col-a",
            request=_make_request("/stac/catalogs/deleted-cat/collections/col-a/items"),
            engine=object(),
            limit=10,
            offset=0,
            filter=None,
            language="en",
            request_hints=frozenset(),
        )

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_get_stac_collection_serves_an_active_catalog(monkeypatch):
    active = {"id": "col-a"}
    catalogs_svc = _FakeCatalogsService(collection=active)
    svc = _svc(monkeypatch, catalogs_svc)

    import pystac

    fake_collection = pystac.Collection(
        id="col-a",
        description="Live",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
            temporal=pystac.TemporalExtent([[None, None]]),
        ),
    )

    async def _fake_create_collection(*_a: Any, **_kw: Any):
        return fake_collection

    monkeypatch.setattr(
        stac_service.stac_generator, "create_collection", _fake_create_collection
    )

    response = await svc.get_stac_collection(
        catalog_id="live-cat",
        collection_id="col-a",
        request=_make_request("/stac/catalogs/live-cat/collections/col-a"),
        language="en",
        request_hints=frozenset(),
    )

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_get_stac_collection_items_serves_an_active_catalog(monkeypatch):
    active = {"id": "col-a"}
    catalogs_svc = _FakeCatalogsService(collection=active)
    svc = _svc(monkeypatch, catalogs_svc)
    monkeypatch.setattr(
        svc, "_get_stac_config",
        AsyncMock(return_value=type("Cfg", (), {"default_limit": 10, "max_limit": 1000})()),
        raising=False,
    )

    async def _fake_create_item_collection(*_a: Any, **_kw: Any) -> dict:
        return {"type": "FeatureCollection", "features": []}

    monkeypatch.setattr(
        stac_service.stac_generator, "create_item_collection", _fake_create_item_collection
    )

    response = await svc.get_stac_collection_items(
        catalog_id="live-cat",
        collection_id="col-a",
        request=_make_request("/stac/catalogs/live-cat/collections/col-a/items"),
        engine=object(),
        limit=10,
        offset=0,
        filter=None,
        language="en",
        request_hints=frozenset(),
    )

    assert response.status_code == 200
