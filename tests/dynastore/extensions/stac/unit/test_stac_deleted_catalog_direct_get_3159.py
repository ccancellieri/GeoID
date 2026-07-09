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

"""Regression coverage for #3159: direct STAC catalog GET must not resurrect
a tombstoned (soft- or hard-deleted) catalog.

``get_catalog_model``'s tombstone fallback deliberately returns a
200+deleted-state model (see ``test_softdelete_semantics.py``) so internal/
admin consumers can observe a reclaimable soft-delete without a 404. The
public STAC catalog document generator must not treat that as a live
catalog: a model with ``deleted_at`` set must render 404 — the same
predicate the catalog listing (``list_catalogs``) and the task-scoped
catalog route (``resolve_physical_schema``) already apply.
"""

from __future__ import annotations

import datetime
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from starlette.requests import Request as StarletteRequest

from dynastore.models.shared_models import Catalog


def _make_request(path: str = "/stac/catalogs/provision_check_a") -> StarletteRequest:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "query_string": b"",
        "headers": [],
        "server": ("localhost", 80),
    }
    return StarletteRequest(scope)


class _FakeCatalogsService:
    """Stands in for ``CatalogsProtocol`` on both call sites involved in a
    direct catalog GET: the route's ``_resolve_catalog_or_404`` (via
    ``get_catalog``) and the generator's own lookup (``get_catalog_model``)."""

    def __init__(self, model: Catalog):
        self.get_catalog_model = AsyncMock(return_value=model)
        self.get_catalog = AsyncMock(return_value=model)


@pytest.mark.asyncio
async def test_create_catalog_404s_a_tombstoned_catalog(monkeypatch):
    """The fingerprint from #3159: a deleted catalog kept rendering a full
    STAC document indefinitely because ``create_catalog`` only checked for
    ``None``, never for a tombstoned (non-None, ``deleted_at``-set) model."""
    import dynastore.extensions.stac.stac_generator as gen

    ts = datetime.datetime(2026, 7, 9, 22, 30, 0, tzinfo=datetime.timezone.utc)
    tombstoned = Catalog.model_validate(
        {"id": "provision_check_a", "deleted_at": ts}
    )
    catalogs_svc = _FakeCatalogsService(tombstoned)
    monkeypatch.setattr(gen, "get_protocol", lambda _proto: catalogs_svc)

    with pytest.raises(HTTPException) as exc_info:
        await gen.create_catalog(
            _make_request(), catalog_id="provision_check_a", lang="en",
        )

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_create_catalog_renders_an_active_catalog(monkeypatch):
    """Sibling guard: an active catalog (``deleted_at=None``) still renders
    normally — the fix must not turn every catalog GET into a 404."""
    import dynastore.extensions.stac.stac_generator as gen

    active = Catalog.model_validate(
        {"id": "provision_check_a", "title": "Provision Check A"}
    )
    catalogs_svc = _FakeCatalogsService(active)
    monkeypatch.setattr(gen, "get_protocol", lambda _proto: catalogs_svc)

    result = await gen.create_catalog(
        _make_request(), catalog_id="provision_check_a", lang="en",
    )

    assert result["id"] == "provision_check_a"


@pytest.mark.asyncio
async def test_get_stac_catalog_route_404s_a_deleted_catalog(monkeypatch):
    """End-to-end: ``GET /stac/catalogs/{id}`` for a tombstoned catalog
    returns 404 — matching the catalog listing and the task-scoped catalog
    route — instead of resurrecting the deleted catalog's metadata."""
    from dynastore.extensions.stac.stac_service import STACService
    import dynastore.extensions.stac.stac_generator as gen

    ts = datetime.datetime(2026, 7, 9, 22, 30, 0, tzinfo=datetime.timezone.utc)
    tombstoned = Catalog.model_validate(
        {"id": "provision_check_a", "deleted_at": ts}
    )
    catalogs_svc = _FakeCatalogsService(tombstoned)

    svc = STACService.__new__(STACService)

    async def _get_catalogs_service() -> Any:
        return catalogs_svc

    monkeypatch.setattr(svc, "_get_catalogs_service", _get_catalogs_service)
    monkeypatch.setattr(gen, "get_protocol", lambda _proto: catalogs_svc)

    with pytest.raises(HTTPException) as exc_info:
        await svc.get_stac_catalog(
            catalog_id="provision_check_a",
            request=_make_request(),
            language="en",
            request_hints=frozenset(),
        )

    assert exc_info.value.status_code == 404
