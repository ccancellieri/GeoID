import pytest

from dynastore.models.driver_context import DriverContext
import dynastore.modules.catalog.catalog_module as catalog_manager
from dynastore.modules.stac.stac_config import (
    StacPluginConfig,
    AggregationConfig,
)
from dynastore.extensions.stac.stac_models import STACItem


# --- Setup/Cleanup Fixtures (STAC Extension Level) ---


@pytest.fixture
async def setup_catalog(sysadmin_in_process_client, catalog_data, catalog_id):
    """Fixture to ensure a catalog exists and is cleaned up using the STAC API."""
    # Cleanup any existing catalog (redundant with random IDs)
    # await sysadmin_in_process_client.delete(f"/stac/catalogs/{catalog_id}")

    # Create catalog via STAC
    r = await sysadmin_in_process_client.post("/stac/catalogs", json=catalog_data)
    # If it already exists (409), that's "fine" for setup, but we prefer a clean slate.
    # If we got 409, it means the delete failed or didn't happen.
    if r.status_code == 409:
        # Try to get it to ensure it's the right one? Or just proceed.
        pass
    else:
        assert r.status_code == 201, f"Failed to create setup catalog: {r.text}"

    yield catalog_id

    # Cleanup handled by session fixture
    # await in_process_client.delete(f"/stac/catalogs/{catalog_id}")


@pytest.fixture
async def setup_collection(
    sysadmin_in_process_client, setup_catalog, collection_data, collection_id
):
    """Fixture to ensure a collection exists and is cleaned up using the STAC API."""
    catalog_id = setup_catalog

    # Cleanup any existing collection (redundant with random IDs)
    # await sysadmin_in_process_client.delete(
    #     f"/stac/catalogs/{catalog_id}/collections/{collection_id}"
    # )

    # Create collection via STAC
    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections", json=collection_data
    )
    if r.status_code == 409:
        pass
    else:
        assert r.status_code == 201, f"Failed to create setup collection: {r.text}"

    yield collection_id

    # Cleanup handled by session fixture
    # await in_process_client.delete(
    #     f"/stac/catalogs/{catalog_id}/collections/{collection_id}"
    # )


@pytest.fixture
async def setup_aggregation_data(
    in_process_client, setup_catalog, setup_collection, test_data_loader, app_lifespan
):
    """Fixture to set up data for STAC aggregation tests."""
    catalog_id = setup_catalog
    collection_id = setup_collection

    import dynastore.tools.identifiers as ident

    print(f"DEBUG: identifiers module file: {ident.__file__}")

    # Load test data from file

    test_items = test_data_loader("aggregation_items.json")

    # Enable Aggregations in STAC config
    from dynastore.tools.discovery import get_protocol
    from dynastore.models.protocols import ConfigsProtocol
    from dynastore.modules.db_config.tools import managed_transaction

    stac_config = StacPluginConfig(
        aggregations=AggregationConfig(
            allow_custom=True, max_aggregations_per_request=10
        )
    )

    configs = get_protocol(ConfigsProtocol)

    async with managed_transaction(app_lifespan.engine) as conn:
        await configs.set_config(
            StacPluginConfig,
            stac_config,
            catalog_id=catalog_id,
            collection_id=collection_id,
            ctx=DriverContext(db_resource=conn),
        )

    # Ingest Items
    from dynastore.models.protocols import ItemsProtocol

    items_svc = get_protocol(ItemsProtocol)

    # Verify table is empty
    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler
    from dynastore.models.protocols import CatalogsProtocol

    catalogs = get_protocol(CatalogsProtocol)

    async with managed_transaction(app_lifespan.engine) as conn:
        # Lazy-activation: a freshly-created collection has no physical_table
        # until the first POST /items (or explicit activate). Activating
        # eagerly here so resolve_physical_table returns a real table name
        # and the diagnostic count query has something to point at.
        await catalogs.activate_collection(
            catalog_id, collection_id, ctx=DriverContext(db_resource=conn)
        )
        schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=conn))
        table = await catalogs.resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )
        count = await DQLQuery(
            f'SELECT count(*) FROM "{schema}"."{table}"',
            result_handler=ResultHandler.SCALAR,
        ).execute(conn)
        print(f"DEBUG: Initial table count for {schema}.{table}: {count}")

    for i, item_data in enumerate(test_items):
        print(f"DEBUG: Ingesting item {i}: {item_data['id']}")
        stac_item = STACItem(
            type="Feature", stac_version="1.0.0", **item_data, collection=collection_id
        )

        # Call upsert directly to debug
        async with managed_transaction(app_lifespan.engine) as conn:
            await items_svc.upsert(
                catalog_id, collection_id, stac_item, ctx=DriverContext(db_resource=conn)
            )

        # res = await in_process_client.post(
        #     f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items",
        #     json=stac_item.model_dump(by_alias=True),
        # )
        # if res.status_code != 201:
        #     print(f"Failed to ingest item: {res.text}")
        # assert res.status_code == 201

    yield catalog_id, collection_id
