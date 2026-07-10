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

"""Regression coverage for #3166: ``GET /stac/catalogs/{id}/collections``
must not list a tombstoned (soft-deleted) catalog's collections.

This is a second STAC leak beyond the direct catalog GET #3164 already
fixed: ``list_stac_collections`` resolved its parent catalog with an inline
``get_catalog`` + falsy check that never inspected ``deleted_at``, so a
tombstoned catalog's collections stayed listable at a stable URL
indefinitely. It now routes through the same ``_resolve_catalog_or_404``
used everywhere else.
"""

from __future__ import annotations

import datetime
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException, Request

import dynastore.extensions.stac.stac_service as stac_service
from dynastore.extensions.stac.stac_service import STACService
from dynastore.models.shared_models import Catalog


def _make_request() -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/stac/catalogs/deleted-cat/collections",
        "headers": [],
        "query_string": b"",
    }
    return Request(scope)


class _FakeCatalogsService:
    def __init__(self, model) -> None:
        self.get_catalog = AsyncMock(return_value=model)


def _mk_service(monkeypatch, model) -> STACService:
    svc = STACService()
    svc._get_catalogs_service = AsyncMock(return_value=_FakeCatalogsService(model))
    svc._get_stac_config = AsyncMock(
        return_value=type("Cfg", (), {"default_limit": 10, "max_limit": 1000})()
    )

    @asynccontextmanager
    async def _fake_managed_transaction(_engine):
        yield None

    monkeypatch.setattr(stac_service, "managed_transaction", _fake_managed_transaction)

    async def _fake_search_collections(_conn, search_req, **_kwargs):
        # The real search_collections() resolves/clamps search_req.limit
        # in place before returning; build_pagination_links relies on that.
        search_req.limit = search_req.limit or 10
        return [], 0

    monkeypatch.setattr(stac_service, "search_collections", _fake_search_collections)
    return svc


@pytest.mark.asyncio
async def test_list_stac_collections_404s_a_tombstoned_catalog(monkeypatch):
    ts = datetime.datetime(2026, 7, 9, 22, 30, 0, tzinfo=datetime.timezone.utc)
    tombstoned = Catalog.model_validate({"id": "deleted-cat", "deleted_at": ts})
    svc = _mk_service(monkeypatch, tombstoned)

    with pytest.raises(HTTPException) as exc_info:
        await svc.list_stac_collections(
            catalog_id="deleted-cat",
            request=_make_request(),
            engine=None,
            language="en",
            request_hints=frozenset(),
            bbox=None,
            datetime=None,
            q=None,
            limit=None,
            offset=0,
            sortby=None,
        )

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_list_stac_collections_serves_an_active_catalog(monkeypatch):
    active = Catalog.model_validate({"id": "live-cat", "title": "Live"})
    svc = _mk_service(monkeypatch, active)

    response = await svc.list_stac_collections(
        catalog_id="live-cat",
        request=_make_request(),
        engine=None,
        language="en",
        request_hints=frozenset(),
        bbox=None,
        datetime=None,
        q=None,
        limit=None,
        offset=0,
        sortby=None,
    )

    assert response.status_code == 200
