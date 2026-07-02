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

import pytest

from dynastore.models.driver_context import DriverContext
import dynastore.modules.catalog.catalog_module as catalog_manager
from dynastore.modules.stac.stac_config import (
    StacPluginConfig,
    AggregationConfig,
)
from dynastore.extensions.stac.stac_models import STACItem

from tests.dynastore.extensions.conftest import build_setup_fixtures


# --- Setup/Cleanup Fixtures (STAC Extension Level) ---
#
# No delete-before-create (random ids make it redundant), 201-or-409 accepted,
# no teardown delete (cleanup is handled by the session-level DB reset).

setup_catalog, setup_collection = build_setup_fixtures("/stac", assert_mode="loose")


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
