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

"""Authoritative liveness gate for collection writes (#1995).

ensure_alive / is_alive must reflect committed registry state, bypassing
secondary indexes and cached model reads.  Tests exercise the three
CollectionLifecycle states: ACTIVE, TOMBSTONED (soft-delete), and MISSING
(hard-delete or never existed).
"""

from __future__ import annotations

import pytest

from dynastore.models.protocols import CatalogsProtocol
from dynastore.models.protocols.entity_store import (
    CollectionLifecycle,
    EntityStoreCapability,
)
from dynastore.models.protocols.entity_store import CollectionStore
from dynastore.modules.catalog.collection_service import (
    CollectionNotAliveError,
    CollectionService,
)
from dynastore.tools.discovery import get_protocol, get_protocols


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _lifecycle_driver() -> "CollectionStore":
    """Return the first CollectionStore driver that declares LIFECYCLE."""
    for drv in get_protocols(CollectionStore):
        if EntityStoreCapability.LIFECYCLE in getattr(drv, "capabilities", frozenset()):
            return drv
    raise RuntimeError("No LIFECYCLE-capable CollectionStore driver registered")


# ---------------------------------------------------------------------------
# Service-level tests (ensure_alive / is_alive)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_alive_passes_for_live_collection(
    app_lifespan,
    catalog_id,
    collection_id,
):
    """ensure_alive must not raise for a live (ACTIVE) collection."""
    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None

    await catalogs.delete_catalog(catalog_id, force=True)
    await catalogs.create_catalog({"id": catalog_id, "title": "Alive Gate Test"})
    await catalogs.create_collection(
        catalog_id,
        {
            "id": collection_id,
            "description": "Alive gate test collection",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
        },
    )

    try:
        svc = CollectionService(engine=app_lifespan.engine)
        # Must not raise
        await svc.ensure_alive(catalog_id, collection_id)
    finally:
        await catalogs.delete_catalog(catalog_id, force=True)


@pytest.mark.asyncio
async def test_ensure_alive_missing_raises_missing(
    app_lifespan,
    catalog_id,
    collection_id,
):
    """ensure_alive on a never-created collection must raise with reason='missing'."""
    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None

    # Catalog must exist so the schema is resolvable, but the collection must not.
    await catalogs.delete_catalog(catalog_id, force=True)
    await catalogs.create_catalog({"id": catalog_id, "title": "Alive Gate Test"})

    try:
        svc = CollectionService(engine=app_lifespan.engine)
        with pytest.raises(CollectionNotAliveError) as exc_info:
            await svc.ensure_alive(catalog_id, collection_id)
        assert exc_info.value.reason == "missing"
        assert exc_info.value.catalog_id == catalog_id
        assert exc_info.value.collection_id == collection_id
    finally:
        await catalogs.delete_catalog(catalog_id, force=True)


@pytest.mark.asyncio
async def test_ensure_alive_hard_deleted_raises_missing(
    app_lifespan,
    catalog_id,
    collection_id,
):
    """After force=True delete the registry row is gone; ensure_alive → 'missing'."""
    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None

    await catalogs.delete_catalog(catalog_id, force=True)
    await catalogs.create_catalog({"id": catalog_id, "title": "Alive Gate Test"})
    await catalogs.create_collection(
        catalog_id,
        {
            "id": collection_id,
            "description": "Alive gate test collection",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
        },
    )

    try:
        # Write an item to trigger lazy provisioning
        await catalogs.upsert(
            catalog_id,
            collection_id,
            {
                "id": "alive-gate-item-1",
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [12.5, 41.9]},
                "properties": {"name": "Alive Gate Item"},
            },
        )

        # Hard delete — removes registry row synchronously
        await catalogs.delete_collection(catalog_id, collection_id, force=True)

        svc = CollectionService(engine=app_lifespan.engine)
        with pytest.raises(CollectionNotAliveError) as exc_info:
            await svc.ensure_alive(catalog_id, collection_id)
        assert exc_info.value.reason == "missing"
    finally:
        await catalogs.delete_catalog(catalog_id, force=True)


@pytest.mark.asyncio
async def test_ensure_alive_soft_deleted_raises_tombstoned(
    app_lifespan,
    catalog_id,
    collection_id,
):
    """Soft delete sets deleted_at; ensure_alive must raise with reason='tombstoned'."""
    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None

    await catalogs.delete_catalog(catalog_id, force=True)
    await catalogs.create_catalog({"id": catalog_id, "title": "Alive Gate Test"})
    await catalogs.create_collection(
        catalog_id,
        {
            "id": collection_id,
            "description": "Alive gate test collection",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
        },
    )

    try:
        # Soft delete — tombstones the registry row (deleted_at set, row retained)
        await catalogs.delete_collection(catalog_id, collection_id, force=False)

        svc = CollectionService(engine=app_lifespan.engine)
        with pytest.raises(CollectionNotAliveError) as exc_info:
            await svc.ensure_alive(catalog_id, collection_id)
        assert exc_info.value.reason == "tombstoned"
    finally:
        await catalogs.delete_catalog(catalog_id, force=True)


# ---------------------------------------------------------------------------
# Write-path gate test (#1995): no resurrection through lazy activation
# ---------------------------------------------------------------------------


async def _snapshot_tables(engine, schema: str) -> set:
    """Set of table names currently present in the given schema."""
    from sqlalchemy import text

    from dynastore.modules.db_config.query_executor import managed_transaction

    async with managed_transaction(engine) as conn:
        res = await conn.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = :s"
            ),
            {"s": schema},
        )
        return {row[0] for row in res}


async def _count_collection_config_rows(engine, schema: str, collection_id: str) -> int:
    """Rows pinned at collection scope (e.g. ItemsRoutingConfig) for the collection."""
    from sqlalchemy import text

    from dynastore.modules.db_config.query_executor import managed_transaction

    async with managed_transaction(engine) as conn:
        res = await conn.execute(
            text(
                f'SELECT COUNT(*) FROM "{schema}".collection_configs '
                "WHERE collection_id = :c"
            ),
            {"c": collection_id},
        )
        return int(res.scalar_one())


@pytest.mark.asyncio
async def test_upsert_after_hard_delete_is_rejected_and_does_not_reprovision(
    app_lifespan,
    catalog_id,
    collection_id,
):
    """A write to a hard-deleted collection must fail with
    CollectionNotAliveError and must NOT re-provision storage: no new
    physical tables, no fresh collection-scope routing pin.
    """
    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None
    engine = app_lifespan.engine

    item = {
        "id": "alive-gate-race-item",
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [12.5, 41.9]},
        "properties": {"name": "Race Item"},
    }

    await catalogs.delete_catalog(catalog_id, force=True)
    await catalogs.create_catalog({"id": catalog_id, "title": "Alive Gate Race"})
    await catalogs.create_collection(
        catalog_id,
        {
            "id": collection_id,
            "description": "Race test collection",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
        },
    )

    try:
        # First write provisions storage (lazy activation).
        await catalogs.upsert(catalog_id, collection_id, item)

        # Hard delete: registry row gone, storage torn down.
        await catalogs.delete_collection(catalog_id, collection_id, force=True)

        schema = await catalogs.resolve_physical_schema(catalog_id)
        assert schema is not None
        tables_before = await _snapshot_tables(engine, schema)
        pins_before = await _count_collection_config_rows(
            engine, schema, collection_id
        )

        # The race: a late writer retries its upsert after the delete.
        with pytest.raises(CollectionNotAliveError) as exc_info:
            await catalogs.upsert(catalog_id, collection_id, item)
        assert exc_info.value.reason == "missing"

        # The rejected write must not have re-provisioned anything.
        tables_after = await _snapshot_tables(engine, schema)
        assert tables_after == tables_before, (
            f"Rejected write re-created tables: {tables_after - tables_before}"
        )
        pins_after = await _count_collection_config_rows(
            engine, schema, collection_id
        )
        assert pins_after == pins_before == 0, (
            "Rejected write must not pin collection-scope config"
        )
    finally:
        await catalogs.delete_catalog(catalog_id, force=True)


# ---------------------------------------------------------------------------
# Driver-level test (get_lifecycle states via PG CollectionStore)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pg_driver_get_lifecycle_states(
    app_lifespan,
    catalog_id,
    collection_id,
):
    """PG CollectionStore driver must return correct lifecycle for all three states."""
    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None

    driver = _lifecycle_driver()

    await catalogs.delete_catalog(catalog_id, force=True)
    await catalogs.create_catalog({"id": catalog_id, "title": "Alive Gate Test"})

    try:
        # MISSING — collection does not exist yet
        lc = await driver.get_lifecycle(catalog_id, collection_id)
        assert lc == CollectionLifecycle.MISSING, f"Expected MISSING, got {lc}"

        # Create → ACTIVE
        await catalogs.create_collection(
            catalog_id,
            {
                "id": collection_id,
                "description": "Lifecycle driver test",
                "extent": {
                    "spatial": {"bbox": [[-180, -90, 180, 90]]},
                    "temporal": {"interval": [[None, None]]},
                },
            },
        )
        lc = await driver.get_lifecycle(catalog_id, collection_id)
        assert lc == CollectionLifecycle.ACTIVE, f"Expected ACTIVE, got {lc}"

        # Soft delete → TOMBSTONED
        await catalogs.delete_collection(catalog_id, collection_id, force=False)
        lc = await driver.get_lifecycle(catalog_id, collection_id)
        assert lc == CollectionLifecycle.TOMBSTONED, f"Expected TOMBSTONED, got {lc}"

        # Hard delete → MISSING again
        await catalogs.delete_collection(catalog_id, collection_id, force=True)
        lc = await driver.get_lifecycle(catalog_id, collection_id)
        assert lc == CollectionLifecycle.MISSING, f"Expected MISSING after hard delete, got {lc}"

    finally:
        await catalogs.delete_catalog(catalog_id, force=True)
