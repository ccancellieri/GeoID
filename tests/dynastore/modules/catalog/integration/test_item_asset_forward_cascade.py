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

"""
Integration tests for the item→asset forward soft-cascade.

Verifies that soft-deleting an item via ``CatalogsProtocol.delete_item``
triggers ``ItemForwardCascadeSubscriber``, which soft-deletes every virtual
asset that carries a ``CoreAssetReferenceType.ITEM`` back-reference for that
item.

Test 1 — event-driven path (via ``ITEM_DELETION`` async listener).
Test 2 — direct handler call (deterministic, no event timing dependency).
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from dynastore.models.protocols import CatalogsProtocol
from dynastore.models.shared_models import CoreAssetReferenceType
from dynastore.modules.catalog.asset_service import VirtualAssetCreate
from dynastore.modules.catalog.asset_sync import ItemForwardCascadeSubscriber
from dynastore.modules.concurrency import await_all_background_tasks
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.tools.discovery import get_protocol


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ITEM_FEATURE = {
    "id": "tile_x",
    "type": "Feature",
    "geometry": {"type": "Point", "coordinates": [12.5, 41.9]},
    "properties": {"name": "Cascade Test Item"},
}


async def _asset_status_from_db(engine, schema: str, asset_id: str) -> str | None:
    """Read the raw ``status`` column directly from the assets table."""
    async with managed_transaction(engine) as conn:
        result = await conn.execute(
            text(f'SELECT status FROM "{schema}".assets WHERE asset_id = :aid'),
            {"aid": asset_id},
        )
        row = result.fetchone()
    return row[0] if row else None


async def _setup(catalogs: CatalogsProtocol, catalog_id: str, catalog_obj, collection_id: str, collection_obj):
    """Create catalog + collection, upsert item, create virtual asset + back-ref."""
    await catalogs.delete_catalog(catalog_id, force=True)
    await catalogs.create_catalog(catalog_obj)
    await catalogs.create_collection(catalog_id, collection_obj)

    feature = dict(_ITEM_FEATURE)
    await catalogs.upsert(catalog_id, collection_id, feature)

    va = VirtualAssetCreate(
        asset_id="tile_x.cog",
        href="https://example.com/tile_x.cog",
        metadata={},
    )
    await catalogs.assets.create_asset(
        catalog_id=catalog_id,
        asset=va,
        collection_id=collection_id,
    )
    await catalogs.assets.add_asset_reference(
        asset_id="tile_x.cog",
        catalog_id=catalog_id,
        ref_type=CoreAssetReferenceType.ITEM,
        ref_id="tile_x",
        cascade_delete=True,
    )


# ---------------------------------------------------------------------------
# Test 1: event-driven soft-cascade
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.enable_extensions("assets")
async def test_item_delete_soft_cascades_virtual_assets(
    app_lifespan,
    catalog_id: str,
    catalog_obj,
    collection_id: str,
    collection_obj,
):
    """Deleting an item via CatalogsProtocol soft-deletes its virtual assets.

    The cascade is delivered asynchronously via the ``ITEM_DELETION`` event
    listener.  ``await_all_background_tasks()`` flushes pending tasks before
    asserting.  The virtual asset row must still exist (soft, not hard delete).
    """
    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None, "CatalogsProtocol not registered"

    try:
        await _setup(catalogs, catalog_id, catalog_obj, collection_id, collection_obj)

        # Confirm back-ref is stored
        asset_ids = await catalogs.assets.list_assets_for_reference(
            catalog_id, CoreAssetReferenceType.ITEM, "tile_x"
        )
        assert "tile_x.cog" in asset_ids, (
            f"Expected back-ref to be present; got: {asset_ids}"
        )

        # Soft-delete the item — emits ITEM_DELETION → schedules forward cascade
        await catalogs.delete_item(catalog_id, collection_id, "tile_x")

        # Flush async listeners
        await await_all_background_tasks()

        # Resolve physical schema for direct DB assertion
        phys_schema = await catalogs.resolve_physical_schema(catalog_id)
        assert phys_schema, "Could not resolve physical schema"

        status = await _asset_status_from_db(app_lifespan.engine, phys_schema, "tile_x.cog")
        assert status is not None, "Virtual asset row was hard-deleted; expected soft-delete only"
        assert status == "deleted", (
            f"Expected virtual asset status='deleted' after cascade; got {status!r}"
        )

    finally:
        await catalogs.delete_catalog(catalog_id, force=True)


# ---------------------------------------------------------------------------
# Test 2: direct handler call (deterministic)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.enable_extensions("assets")
async def test_forward_cascade_handler_direct(
    app_lifespan,
    catalog_id: str,
    catalog_obj,
    collection_id: str,
    collection_obj,
):
    """Calling ItemForwardCascadeSubscriber.on_item_delete directly soft-deletes
    the virtual assets without relying on event delivery timing.

    This test is the authoritative assertion for the cascade logic: it calls
    the handler in-process, bypassing the async event bus.
    """
    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None, "CatalogsProtocol not registered"

    try:
        await _setup(catalogs, catalog_id, catalog_obj, collection_id, collection_obj)

        # Confirm back-ref is present
        asset_ids = await catalogs.assets.list_assets_for_reference(
            catalog_id, CoreAssetReferenceType.ITEM, "tile_x"
        )
        assert "tile_x.cog" in asset_ids

        # Call handler directly — no event bus, fully synchronous
        await ItemForwardCascadeSubscriber.on_item_delete(
            catalog_id=catalog_id,
            collection_id=collection_id,
            item_id="tile_x",
        )

        # Resolve physical schema for direct DB assertion
        phys_schema = await catalogs.resolve_physical_schema(catalog_id)
        assert phys_schema, "Could not resolve physical schema"

        status = await _asset_status_from_db(app_lifespan.engine, phys_schema, "tile_x.cog")
        assert status is not None, "Virtual asset row was hard-deleted; expected soft-delete only"
        assert status == "deleted", (
            f"Expected virtual asset status='deleted' after direct handler call; got {status!r}"
        )

    finally:
        await catalogs.delete_catalog(catalog_id, force=True)
