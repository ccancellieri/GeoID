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

"""Regression coverage for #3166: ``/wfs/catalogs/{id}`` must not resolve a
tombstoned (soft-deleted) catalog as live.

``handle_scoped_wfs_request`` guards the whole scoped-request dispatch with
``self._resolve_catalog_or_404`` up front. That shared resolver now carries
the same fail-closed tombstone default the STAC direct GET got in #3164.
"""

from __future__ import annotations

import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette.requests import Request as StarletteRequest

from dynastore.models.shared_models import Catalog


def _fake_conn() -> MagicMock:
    return MagicMock(spec=AsyncConnection, name="async_conn")


def _make_request(path: str = "/wfs/catalogs/deleted-cat") -> StarletteRequest:
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
async def test_handle_scoped_wfs_request_404s_a_tombstoned_catalog(monkeypatch):
    """A tombstoned catalog must 404 on the WFS scoped entry point before
    request dispatch ever runs."""
    from dynastore.extensions.wfs.wfs_service import WFSService

    svc = WFSService.__new__(WFSService)

    ts = datetime.datetime(2026, 7, 9, 22, 30, 0, tzinfo=datetime.timezone.utc)
    tombstoned = Catalog.model_validate({"id": "deleted-cat", "deleted_at": ts})
    catalogs_svc = _FakeCatalogsService(tombstoned)

    async def _get_catalogs_service():
        return catalogs_svc

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs_service, raising=False)
    dispatch_mock = AsyncMock()
    monkeypatch.setattr(svc, "_dispatch_request", dispatch_mock, raising=False)

    with pytest.raises(HTTPException) as exc_info:
        await svc.handle_scoped_wfs_request(
            request=_make_request(),
            catalog_id="deleted-cat",
            conn=_fake_conn(),
            language="en",
        )

    assert exc_info.value.status_code == 404
    dispatch_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_handle_scoped_wfs_request_dispatches_an_active_catalog(monkeypatch):
    """Sibling guard: an active catalog still reaches request dispatch."""
    from dynastore.extensions.wfs.wfs_service import WFSService

    svc = WFSService.__new__(WFSService)

    active = Catalog.model_validate({"id": "live-cat", "title": "Live"})
    catalogs_svc = _FakeCatalogsService(active)

    async def _get_catalogs_service():
        return catalogs_svc

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs_service, raising=False)
    dispatch_mock = AsyncMock(return_value="dispatched")
    monkeypatch.setattr(svc, "_dispatch_request", dispatch_mock, raising=False)

    result = await svc.handle_scoped_wfs_request(
        request=_make_request("/wfs/catalogs/live-cat"),
        catalog_id="live-cat",
        conn=_fake_conn(),
        language="en",
    )

    assert result == "dispatched"
    dispatch_mock.assert_awaited_once()
