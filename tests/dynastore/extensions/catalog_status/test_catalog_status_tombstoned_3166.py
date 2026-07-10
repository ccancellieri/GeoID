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

"""Regression coverage for #3166: ``GET /catalog/catalogs/{id}`` (catalog
provisioning status) must not resolve a tombstoned (soft-deleted) catalog.

``get_catalog_status`` is a catalog-member read surface (gated by
``catalog_membership_required``, not sysadmin-only) that resolves the
catalog via the shared ``resolve_catalog_or_404(..., use_model=True)``. It
inherits the resolver's fail-closed tombstone default the same way the
STAC/Features/WFS surfaces do.
"""

from __future__ import annotations

import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

_SVC_MODULE = "dynastore.extensions.catalog_status.catalog_status_service"
_RESOLVE_CATALOG = f"{_SVC_MODULE}.resolve_catalog_listing_ids"
_GET_PROTOCOL = f"{_SVC_MODULE}.get_protocol"


def _get_catalog_status_handler():
    from dynastore.extensions.catalog_status.catalog_status_service import CatalogStatusService
    for route in CatalogStatusService.router.routes:
        if getattr(route, "name", "") == "get_catalog_status":
            return route.endpoint
    raise AssertionError("get_catalog_status route not found")


@pytest.mark.asyncio
async def test_get_catalog_status_404s_a_tombstoned_catalog():
    ts = datetime.datetime(2026, 7, 9, 22, 30, 0, tzinfo=datetime.timezone.utc)
    tombstoned = SimpleNamespace(
        id="c_deleted", provisioning_status="ready", deleted_at=ts,
    )

    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog_model = AsyncMock(return_value=tombstoned)

    def _proto(proto):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols import DatabaseProtocol
        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is DatabaseProtocol:
            return None
        return None

    handler = _get_catalog_status_handler()

    with (
        patch(_RESOLVE_CATALOG, AsyncMock(return_value=None)),
        patch(_GET_PROTOCOL, side_effect=_proto),
    ):
        with pytest.raises(HTTPException) as exc_info:
            await handler("deleted-cat")

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_get_catalog_status_serves_an_active_catalog():
    """Sibling guard: an active catalog's status is unaffected."""
    fake_cat = SimpleNamespace(id="c_live", provisioning_status="ready")

    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog_model = AsyncMock(return_value=fake_cat)
    catalogs_mock.resolve_physical_schema = AsyncMock(return_value="c_live")
    catalogs_mock.get_provisioning_checklist = AsyncMock(return_value={})

    def _proto(proto):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols import DatabaseProtocol
        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is DatabaseProtocol:
            return None
        return None

    handler = _get_catalog_status_handler()

    with (
        patch(_RESOLVE_CATALOG, AsyncMock(return_value=None)),
        patch(_GET_PROTOCOL, side_effect=_proto),
    ):
        result = await handler("live-cat")

    assert result.provisioning_status == "ready"
