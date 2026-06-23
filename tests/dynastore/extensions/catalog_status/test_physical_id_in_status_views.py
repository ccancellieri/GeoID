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

"""Tests that CatalogStatusView and CollectionStatusView carry physical_id.

Pure-unit, no live DB.  Covers:
- DTO field presence (static shape check).
- physical_id == physical_schema for CatalogStatusView.
- CollectionStatusView exposes both physical_id (parent catalog) and
  collection_physical_id (the collection's own table name).
- get_catalog_status handler populates physical_id.
- get_collection_status handler populates physical_id and
  collection_physical_id.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_SVC_MODULE = "dynastore.extensions.catalog_status.catalog_status_service"
_RESOLVE_CATALOG = f"{_SVC_MODULE}.resolve_catalog_listing_ids"
_RESOLVE_COLLECTION = f"{_SVC_MODULE}.resolve_collection_listing_ids"
_GET_PROTOCOL = f"{_SVC_MODULE}.get_protocol"

_SCHEMA = "s_abc12345"
_TABLE = "t_xyz98765"


def _fake_catalog(catalog_id: str = "cat1", status: str = "ready") -> SimpleNamespace:
    return SimpleNamespace(id=catalog_id, provisioning_status=status)


def _fake_collection(collection_id: str = "col1") -> SimpleNamespace:
    return SimpleNamespace(id=collection_id)


# ---------------------------------------------------------------------------
# 1. DTO shape — CatalogStatusView
# ---------------------------------------------------------------------------


def test_catalog_status_view_has_physical_id_field():
    """CatalogStatusView must declare a physical_id Optional[str] field."""
    from dynastore.extensions.catalog_status.catalog_status_models import CatalogStatusView
    import inspect

    fields = CatalogStatusView.model_fields
    assert "physical_id" in fields, (
        f"physical_id missing from CatalogStatusView; got: {list(fields)}"
    )


def test_catalog_status_view_physical_id_defaults_none():
    from dynastore.extensions.catalog_status.catalog_status_models import CatalogStatusView

    view = CatalogStatusView(catalog_id="cat1", provisioning_status="ready")
    assert view.physical_id is None


def test_catalog_status_view_physical_id_equals_physical_schema():
    """physical_id and physical_schema must carry the same value."""
    from dynastore.extensions.catalog_status.catalog_status_models import CatalogStatusView

    view = CatalogStatusView(
        catalog_id="cat1",
        provisioning_status="ready",
        physical_schema=_SCHEMA,
        physical_id=_SCHEMA,
    )
    assert view.physical_schema == _SCHEMA
    assert view.physical_id == _SCHEMA
    assert view.physical_id == view.physical_schema


# ---------------------------------------------------------------------------
# 2. DTO shape — CollectionStatusView
# ---------------------------------------------------------------------------


def test_collection_status_view_has_physical_id_field():
    """CollectionStatusView must declare physical_id (parent catalog schema)."""
    from dynastore.extensions.catalog_status.catalog_status_models import CollectionStatusView

    fields = CollectionStatusView.model_fields
    assert "physical_id" in fields, (
        f"physical_id missing from CollectionStatusView; got: {list(fields)}"
    )


def test_collection_status_view_has_collection_physical_id_field():
    """CollectionStatusView must declare collection_physical_id (the table name)."""
    from dynastore.extensions.catalog_status.catalog_status_models import CollectionStatusView

    fields = CollectionStatusView.model_fields
    assert "collection_physical_id" in fields, (
        f"collection_physical_id missing from CollectionStatusView; got: {list(fields)}"
    )


def test_collection_status_view_defaults_none():
    from dynastore.extensions.catalog_status.catalog_status_models import CollectionStatusView

    view = CollectionStatusView(
        catalog_id="cat1",
        collection_id="col1",
        catalog_provisioning_status="ready",
    )
    assert view.physical_id is None
    assert view.collection_physical_id is None


def test_collection_status_view_physical_id_carries_catalog_schema():
    from dynastore.extensions.catalog_status.catalog_status_models import CollectionStatusView

    view = CollectionStatusView(
        catalog_id="cat1",
        collection_id="col1",
        catalog_provisioning_status="ready",
        physical_schema=_SCHEMA,
        physical_id=_SCHEMA,
        collection_physical_id=_TABLE,
    )
    assert view.physical_id == _SCHEMA
    assert view.collection_physical_id == _TABLE


# ---------------------------------------------------------------------------
# 3. Handler populates physical_id — get_catalog_status
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_catalog_status_populates_physical_id():
    """get_catalog_status handler must set physical_id == physical_schema."""
    fake_cat = _fake_catalog()

    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog_model = AsyncMock(return_value=fake_cat)
    catalogs_mock.resolve_physical_schema = AsyncMock(return_value=_SCHEMA)

    def _proto(proto):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        from dynastore.models.protocols import DatabaseProtocol
        if proto is CatalogsProtocol:
            return catalogs_mock
        if proto is DatabaseProtocol:
            return None
        return None

    from dynastore.extensions.catalog_status.catalog_status_service import CatalogStatusService

    handler = None
    for route in CatalogStatusService.router.routes:
        if getattr(route, "name", "") == "get_catalog_status":
            handler = route.endpoint
            break
    assert handler is not None, "get_catalog_status route not found"

    with (
        patch(_RESOLVE_CATALOG, AsyncMock(return_value=None)),
        patch(_GET_PROTOCOL, side_effect=_proto),
    ):
        result = await handler("cat1")

    assert result.physical_id == _SCHEMA
    assert result.physical_schema == _SCHEMA
    assert result.physical_id == result.physical_schema


# ---------------------------------------------------------------------------
# 4. Handler populates physical_id + collection_physical_id — get_collection_status
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_collection_status_populates_physical_id_and_collection_physical_id():
    """get_collection_status handler must set both physical fields."""
    fake_cat = _fake_catalog()
    fake_col = _fake_collection()

    async def _resolve_physical_id(cat, col=None, **kw):
        return _TABLE if col is not None else _SCHEMA

    catalogs_mock = MagicMock()
    catalogs_mock.get_catalog_model = AsyncMock(return_value=fake_cat)
    catalogs_mock.get_collection = AsyncMock(return_value=fake_col)
    catalogs_mock.resolve_physical_schema = AsyncMock(return_value=_SCHEMA)
    catalogs_mock.resolve_physical_id = _resolve_physical_id

    def _proto(proto):
        from dynastore.models.protocols.catalogs import CatalogsProtocol
        if proto is CatalogsProtocol:
            return catalogs_mock
        return None

    from dynastore.extensions.catalog_status.catalog_status_service import CatalogStatusService

    handler = None
    for route in CatalogStatusService.router.routes:
        if getattr(route, "name", "") == "get_collection_status":
            handler = route.endpoint
            break
    assert handler is not None, "get_collection_status route not found"

    with (
        patch(_RESOLVE_CATALOG, AsyncMock(return_value=None)),
        patch(_RESOLVE_COLLECTION, AsyncMock(return_value=None)),
        patch(_GET_PROTOCOL, side_effect=_proto),
    ):
        result = await handler("cat1", "col1")

    # Catalog-level physical_id == physical_schema
    assert result.physical_id == _SCHEMA
    assert result.physical_schema == _SCHEMA
    # Collection-level physical_id
    assert result.collection_physical_id == _TABLE
