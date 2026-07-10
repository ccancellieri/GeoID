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

"""Regression coverage for #3230: OGC Features collection-level routes must
not resurrect a tombstoned (soft-deleted) catalog's collection.

Unlike the catalog-level fix in #3166/PR #3228, these routes were already
404-ing a tombstoned catalog — but only as a side effect of
``resolve_physical_schema(allow_missing=False)`` raising a ``ValueError``
that a generic message-sniffing exception handler happened to downgrade to
404. Nothing pinned that contract at the route level, so a future change to
the resolver's error semantics could silently reopen the leak. These tests
pin ``GET /features/catalogs/{cat}/collections/{col}`` and
``GET /features/catalogs/{cat}/collections/{col}/items`` directly against
the shared resolver (``_resolve_collection_or_404``), independent of the
message text a lower layer happens to raise.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from starlette.requests import Request as StarletteRequest

from dynastore.models.shared_models import Collection


def _make_request(path: str) -> StarletteRequest:
    scope = {
        "type": "http",
        "method": "GET",
        "scheme": "http",
        "server": ("testserver", 80),
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": [(b"host", b"testserver")],
        "root_path": "",
    }
    return StarletteRequest(scope)


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
    from dynastore.extensions.features.features_service import OGCFeaturesService

    svc = OGCFeaturesService.__new__(OGCFeaturesService)

    async def _get_catalogs_service():
        return catalogs_svc

    async def _get_configs_service():
        # get_items() resolves this before the collection guard runs but
        # never touches it once the guard raises — a bare stub is enough.
        return AsyncMock()

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs_service, raising=False)
    monkeypatch.setattr(svc, "_get_configs_service", _get_configs_service, raising=False)
    return svc


@pytest.mark.asyncio
async def test_get_collection_404s_a_tombstoned_catalog(monkeypatch):
    catalogs_svc = _FakeCatalogsService(tombstoned=True)
    from dynastore.extensions.features.features_service import OGCFeaturesService

    svc = OGCFeaturesService.__new__(OGCFeaturesService)

    async def _get_catalogs_service():
        return catalogs_svc

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs_service, raising=False)

    with pytest.raises(HTTPException) as exc_info:
        await svc.get_collection(
            catalog_id="deleted-cat",
            collection_id="col-a",
            request=_make_request("/features/catalogs/deleted-cat/collections/col-a"),
            language="en",
            request_hints=frozenset(),
        )

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_get_items_404s_a_tombstoned_catalog(monkeypatch):
    """The read-item-listing sibling: must 404 before any driver dispatch."""
    catalogs_svc = _FakeCatalogsService(tombstoned=True)
    svc = _svc(monkeypatch, catalogs_svc)

    with pytest.raises(HTTPException) as exc_info:
        await svc.get_items(
            request=_make_request("/features/catalogs/deleted-cat/collections/col-a/items"),
            catalog_id="deleted-cat",
            collection_id="col-a",
            conn=object(),
        )

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_get_collection_serves_an_active_catalog(monkeypatch):
    active = Collection.model_validate({"id": "col-a", "title": "Live"})
    catalogs_svc = _FakeCatalogsService(collection=active)
    from dynastore.extensions.features.features_service import OGCFeaturesService

    svc = OGCFeaturesService.__new__(OGCFeaturesService)

    async def _get_catalogs_service():
        return catalogs_svc

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs_service, raising=False)

    response = await svc.get_collection(
        catalog_id="live-cat",
        collection_id="col-a",
        request=_make_request("/features/catalogs/live-cat/collections/col-a"),
        language="en",
        request_hints=frozenset(),
    )

    assert response.status_code == 200
