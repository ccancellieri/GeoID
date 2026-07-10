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

"""Regression coverage for #3166: ``GET /features/catalogs/{id}`` must not
resurrect a tombstoned (soft-deleted) catalog.

Unlike the STAC direct GET (#3159/#3164), the OGC Features catalog route had
no downstream ``deleted_at`` guard at all — it renders whatever
``_resolve_catalog_or_404`` (via ``get_catalog``) returns. That resolver now
carries the same fail-closed default the rest of the read surfaces share.
"""

from __future__ import annotations

import datetime
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from starlette.requests import Request as StarletteRequest

from dynastore.models.shared_models import Catalog


def _make_request(path: str = "/features/catalogs/deleted-cat") -> StarletteRequest:
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
    def __init__(self, model) -> None:
        self.get_catalog = AsyncMock(return_value=model)
        self.get_catalog_model = AsyncMock(return_value=model)


@pytest.mark.asyncio
async def test_get_catalog_404s_a_tombstoned_catalog(monkeypatch):
    """The fingerprint from #3166: OGC Features' catalog GET had no
    tombstone guard at all — it would render deleted-state metadata as a
    live catalog document indefinitely."""
    from dynastore.extensions.features.features_service import OGCFeaturesService

    svc = OGCFeaturesService.__new__(OGCFeaturesService)

    ts = datetime.datetime(2026, 7, 9, 22, 30, 0, tzinfo=datetime.timezone.utc)
    tombstoned = Catalog.model_validate({"id": "deleted-cat", "deleted_at": ts})
    catalogs_svc = _FakeCatalogsService(tombstoned)

    async def _get_catalogs_service():
        return catalogs_svc

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs_service, raising=False)

    with pytest.raises(HTTPException) as exc_info:
        await svc.get_catalog(
            catalog_id="deleted-cat", request=_make_request(), language="en",
        )

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_get_catalog_renders_an_active_catalog(monkeypatch):
    """Sibling guard: an active catalog still renders normally."""
    from dynastore.extensions.features.features_service import OGCFeaturesService

    svc = OGCFeaturesService.__new__(OGCFeaturesService)

    active = Catalog.model_validate({"id": "live-cat", "title": "Live"})
    catalogs_svc = _FakeCatalogsService(active)

    async def _get_catalogs_service():
        return catalogs_svc

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs_service, raising=False)

    response = await svc.get_catalog(
        catalog_id="live-cat", request=_make_request("/features/catalogs/live-cat"), language="en",
    )

    assert response.status_code == 200
