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

"""Unit tests for CatalogsProtocol.resolve_physical_id and the CatalogService impl.

Pure-unit — no live DB.  Uses AsyncMock stubs.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch, MagicMock

import pytest


_CAT_ID = "test_catalog"
_COL_ID = "test_collection"
_SCHEMA = "s_abc12345"
_TABLE = "t_xyz98765"

_SVC_MODULE = "dynastore.modules.catalog.catalog_service"


# ---------------------------------------------------------------------------
# 1. Protocol declares resolve_physical_id
# ---------------------------------------------------------------------------


def test_catalogs_protocol_declares_resolve_physical_id():
    """CatalogsProtocol must expose resolve_physical_id."""
    from dynastore.models.protocols.catalogs import CatalogsProtocol
    import inspect

    members = dict(inspect.getmembers(CatalogsProtocol))
    assert "resolve_physical_id" in members, (
        "CatalogsProtocol missing resolve_physical_id method"
    )


# ---------------------------------------------------------------------------
# 2. CatalogService.resolve_physical_id — catalog-only path uses cache
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_physical_id_catalog_only_returns_schema():
    """resolve_physical_id(cat) returns the value from the physical schema cache."""
    from dynastore.modules.catalog.catalog_service import CatalogService

    svc = CatalogService.__new__(CatalogService)

    with patch(
        f"{_SVC_MODULE}._physical_schema_cache",
        new=AsyncMock(return_value=_SCHEMA),
    ) as mock_cache:
        result = await svc.resolve_physical_id(_CAT_ID)

    assert result == _SCHEMA
    mock_cache.assert_awaited_once_with(svc, _CAT_ID)


@pytest.mark.asyncio
async def test_resolve_physical_id_catalog_only_allow_missing_returns_none():
    """allow_missing=True: missing catalog returns None instead of raising."""
    from dynastore.modules.catalog.catalog_service import CatalogService

    svc = CatalogService.__new__(CatalogService)

    with patch(
        f"{_SVC_MODULE}._physical_schema_cache",
        new=AsyncMock(return_value=None),
    ):
        result = await svc.resolve_physical_id(_CAT_ID, allow_missing=True)

    assert result is None


@pytest.mark.asyncio
async def test_resolve_physical_id_catalog_only_raises_when_missing_strict():
    """allow_missing=False (default): missing catalog raises ValueError."""
    from dynastore.modules.catalog.catalog_service import CatalogService

    svc = CatalogService.__new__(CatalogService)

    with patch(
        f"{_SVC_MODULE}._physical_schema_cache",
        new=AsyncMock(return_value=None),
    ):
        with pytest.raises(ValueError, match=_CAT_ID):
            await svc.resolve_physical_id(_CAT_ID, allow_missing=False)


# ---------------------------------------------------------------------------
# 3. CatalogService.resolve_physical_id — collection path delegates to table
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_physical_id_collection_returns_registry_column():
    """resolve_physical_id(cat, col) returns the authoritative
    ``{schema}.collections.physical_id`` registry value — no JSONB fallback."""
    from dynastore.modules.catalog.catalog_service import CatalogService

    svc = CatalogService.__new__(CatalogService)

    with patch.object(
        CatalogService,
        "_resolve_collection_physical_id_db",
        new=AsyncMock(return_value=_TABLE),
    ) as mock_db:
        result = await svc.resolve_physical_id(_CAT_ID, _COL_ID)

    assert result == _TABLE
    mock_db.assert_awaited_once_with(_CAT_ID, _COL_ID, db_resource=None)


@pytest.mark.asyncio
async def test_resolve_physical_id_collection_raises_when_missing():
    """Missing collection raises by default, mirroring the catalog path."""
    from dynastore.modules.catalog.catalog_service import CatalogService

    svc = CatalogService.__new__(CatalogService)

    with patch.object(
        CatalogService,
        "_resolve_collection_physical_id_db",
        new=AsyncMock(return_value=None),
    ):
        with pytest.raises(ValueError):
            await svc.resolve_physical_id(_CAT_ID, _COL_ID)


@pytest.mark.asyncio
async def test_resolve_physical_id_collection_allow_missing_returns_none():
    """allow_missing=True yields None for an absent collection."""
    from dynastore.modules.catalog.catalog_service import CatalogService

    svc = CatalogService.__new__(CatalogService)

    with patch.object(
        CatalogService,
        "_resolve_collection_physical_id_db",
        new=AsyncMock(return_value=None),
    ):
        result = await svc.resolve_physical_id(_CAT_ID, _COL_ID, allow_missing=True)

    assert result is None


# ---------------------------------------------------------------------------
# 4. resolve_physical_schema shim delegates to resolve_physical_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_physical_schema_shim_delegates_to_physical_id():
    """resolve_physical_schema must delegate to resolve_physical_id."""
    from dynastore.modules.catalog.catalog_service import CatalogService

    svc = CatalogService.__new__(CatalogService)

    with patch.object(
        CatalogService,
        "resolve_physical_id",
        new=AsyncMock(return_value=_SCHEMA),
    ) as mock_pid:
        result = await svc.resolve_physical_schema(_CAT_ID)

    assert result == _SCHEMA
    mock_pid.assert_awaited_once_with(_CAT_ID, ctx=None, allow_missing=False)


@pytest.mark.asyncio
async def test_resolve_physical_schema_shim_passes_ctx_and_allow_missing():
    """resolve_physical_schema forwards ctx and allow_missing to resolve_physical_id."""
    from dynastore.modules.catalog.catalog_service import CatalogService

    svc = CatalogService.__new__(CatalogService)
    fake_ctx = MagicMock()

    with patch.object(
        CatalogService,
        "resolve_physical_id",
        new=AsyncMock(return_value=None),
    ) as mock_pid:
        result = await svc.resolve_physical_schema(
            _CAT_ID, ctx=fake_ctx, allow_missing=True
        )

    assert result is None
    mock_pid.assert_awaited_once_with(_CAT_ID, ctx=fake_ctx, allow_missing=True)
