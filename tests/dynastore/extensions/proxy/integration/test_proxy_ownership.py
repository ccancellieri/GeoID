
import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.catalog.lifecycle_manager import lifecycle_registry
from dynastore.models.driver_context import DriverContext

@pytest.mark.enable_modules("db_config", "db", "catalog", "stac", "httpx", "proxy", "stats", "collection_postgresql", "catalog_postgresql")
@pytest.mark.enable_extensions("proxy")
@pytest.mark.asyncio
async def test_proxy_ownership_and_cleanup(app_lifespan, catalog_obj, collection_obj):
    """
    Verifies the proxy URL ownership tracking and automatic cleanup.

    The proxy tenant slice is a single, non-partitioned
    ``collection_proxy_urls`` table per catalog schema, keyed by ``short_key``
    with a ``collection_id`` column. Deleting a collection cascades to a
    ``DELETE ... WHERE collection_id = ...`` rather than dropping a partition.
    """
    app = app_lifespan.app
    engine = app_lifespan.engine
    catalog_id = catalog_obj.id
    collection_id = collection_obj.id

    # 1. Create Catalog
    catalogs = get_protocol(CatalogsProtocol)
    async with managed_transaction(engine) as conn:
        phys_schema_before = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True)
        if phys_schema_before:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{phys_schema_before}" CASCADE'))
            await conn.commit()

        await catalogs.create_catalog(catalog_obj, ctx=DriverContext(db_resource=conn))
        await lifecycle_registry.wait_for_all_tasks()

    # 2. Verify the flat ownership table exists in the tenant schema
    phys_schema = await catalogs.resolve_physical_schema(catalog_id)
    assert phys_schema is not None

    async with managed_transaction(engine) as conn:
        res = await conn.execute(text(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = :schema AND table_name = 'collection_proxy_urls'"
        ), {"schema": phys_schema})
        assert res.scalar() == 1, f"Ownership table 'collection_proxy_urls' not found in tenant schema '{phys_schema}'"

    # 3. Create Collection
    async with managed_transaction(engine) as conn:
        await catalogs.create_collection(catalog_id, collection_obj, ctx=DriverContext(db_resource=conn))
        await lifecycle_registry.wait_for_all_tasks()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:

        # 4. Create collection-scoped Proxy URL
        long_url = "https://example.com/collection-data"
        payload = {
            "long_url": long_url,
            "comment": "Collection URL"
        }

        create_resp = await ac.post(
            f"/proxy/catalogs/{catalog_id}/collections/{collection_id}/urls",
            json=payload
        )
        assert create_resp.status_code == 201, f"Create failed: {create_resp.text}"
        data = create_resp.json()
        short_key = data["short_key"]

        # 5. Verify the row exists in the flat ownership table, scoped to the collection
        async with managed_transaction(engine) as conn:
            q = text(
                f'SELECT count(*) FROM "{phys_schema}".collection_proxy_urls '
                "WHERE short_key = :key AND collection_id = :coll"
            )
            res = await conn.execute(q, {"key": short_key, "coll": collection_id})
            assert res.scalar() == 1, "Short URL not found in collection_proxy_urls"

        # 6. Delete Collection (programmatically, not via REST API)
        async with managed_transaction(engine) as conn:
            await catalogs.delete_collection(catalog_id, collection_id, force=True, ctx=DriverContext(db_resource=conn))

        # Wait for background tasks (cleanup hook)
        await lifecycle_registry.wait_for_all_tasks()

        # 7. Verify automatic cleanup: the cascade owner deletes the
        #    collection's rows by collection_id (no partition to drop).
        async with managed_transaction(engine) as conn:
            q_parent = text(
                f'SELECT count(*) FROM "{phys_schema}".collection_proxy_urls '
                "WHERE short_key = :key"
            )
            res_parent = await conn.execute(q_parent, {"key": short_key})
            assert res_parent.scalar() == 0, f"Proxy URL {short_key} still present in parent table"
