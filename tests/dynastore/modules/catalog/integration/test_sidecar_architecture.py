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
from sqlalchemy import text
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
    GeometriesSidecarConfig,
    TargetDimension,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    FeatureAttributeSidecarConfig,
    AttributeStorageMode,
    AttributeSchemaEntry,
    PostgresType,
    AttributeIndexType,
)
from dynastore.models.protocols import CatalogsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import (
    managed_transaction,
    DQLQuery,
    ResultHandler,
)


@pytest.mark.asyncio
async def test_columnar_attributes_sidecar(app_lifespan, catalog_id, collection_id):
    """
    Real-world test verifying the flexibility of the Attributes Sidecar
    in COLUMNAR mode (strict schema, high performance).
    """
    catalogs = get_protocol(CatalogsProtocol)

    # 1. Setup Catalog
    await catalogs.ensure_catalog_exists(catalog_id)

    # 2. Define Collection with COLUMNAR Attributes
    # This forces the creation of physical columns instead of a JSONB blob
    attr_sidecar = FeatureAttributeSidecarConfig(
        storage_mode=AttributeStorageMode.COLUMNAR,
        attribute_schema=[
            AttributeSchemaEntry(
                name="population",
                type=PostgresType.INTEGER,
                index=AttributeIndexType.BTREE,
                nullable=True,
            ),
            AttributeSchemaEntry(
                name="category",
                type=PostgresType.TEXT,
                index=AttributeIndexType.HASH,  # Hash index for exact matches
                nullable=False,
            ),
        ],
    )

    # 1. Setup Config
    geo_config = GeometriesSidecarConfig(target_srid=3857)
    col_config = ItemsPostgresqlDriverConfig(
        sidecars=[geo_config, attr_sidecar]
    )

    await catalogs.create_collection(
        catalog_id,
        {
            "id": collection_id,
            "title": {"en": "Columnar Cities"},
            "layer_config": col_config.model_dump(),
        },
        lang="*",
    )

    # 3. Insert Data
    item_id = "city-1"
    item_data = {
        "id": item_id,
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [12.5, 41.9]},
        "properties": {
            "population": 2800000,
            "category": "capital",
            "ignored_field": "this should be dropped in columnar mode",
        },
    }

    res = await catalogs.upsert(catalog_id, collection_id, item_data)
    assert res is not None
    # res is Feature object
    # We query by external_id (id) instead of geoid because geoid is internal
    item_id = res.id

    # 4. Verify Physical Storage
    phys_schema = await catalogs.resolve_physical_schema(catalog_id)
    hub_table = await catalogs.resolve_physical_table(catalog_id, collection_id)
    attr_table = f"{hub_table}_attributes"

    async with managed_transaction(app_lifespan.engine) as conn:
        # Inspect columns in the attributes sidecar table
        # We expect: geoid, validity, external_id, asset_id, population, category
        # We expect NOT: attributes (JSONB)

        # Check if 'population' column exists
        col_check = await conn.execute(
            text(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = '{phys_schema}' AND table_name = '{attr_table}'
        """)
        )
        cols = {r[0]: r[1] for r in col_check.fetchall()}

        assert "population" in cols, "Column 'population' missing in sidecar"
        assert "category" in cols, "Column 'category' missing in sidecar"
        assert "attributes" not in cols, (
            "JSONB 'attributes' column should not exist in COLUMNAR mode"
        )

        # Verify Data
        row = await DQLQuery(
            f'SELECT population, category FROM "{phys_schema}"."{attr_table}" WHERE external_id = :ext_id',
            result_handler=ResultHandler.ONE_DICT,
        ).execute(conn, ext_id=item_id)

        assert row["population"] == 2800000
        assert row["category"] == "capital"

    # 5. Verify Query Optimization (Functional)
    # Search filtering by the columnar attribute
    search_res = await catalogs.search(
        catalog_id, collection_id, filter_cql="category = 'capital'"
    )
    assert len(search_res["features"]) == 1
    assert search_res["features"][0].properties["population"] == 2800000

    # Search filtering by non-matching attribute
    search_res_empty = await catalogs.search(
        catalog_id, collection_id, filter_cql="category = 'village'"
    )
    assert len(search_res_empty["features"]) == 0


@pytest.mark.asyncio
async def test_partial_retrieval_optimization(app_lifespan, catalog_id, collection_id):
    """
    Verifies that the Query Optimizer allows fetching ONLY attribute data
    without loading the geometry, demonstrating the 'Pay-for-what-you-use' pattern.
    """
    catalogs = get_protocol(CatalogsProtocol)

    # 1. Setup Collection
    await catalogs.ensure_catalog_exists(catalog_id)

    # Standard JSONB config
    col_config = ItemsPostgresqlDriverConfig(
        sidecars=[
            GeometriesSidecarConfig(),
            FeatureAttributeSidecarConfig(storage_mode=AttributeStorageMode.JSONB),
        ]
    )

    await catalogs.create_collection(
        catalog_id,
        {
            "id": collection_id,
            "title": {"en": "Optimization Test"},
            "layer_config": col_config.model_dump(),
        },
        lang="*",
    )

    # 2. Insert Data
    item_data = {
        "id": "opt-1",
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "properties": {"name": "Test Item", "rank": 1},
    }
    await catalogs.upsert(catalog_id, collection_id, item_data)

    # 3. Perform Optimized Search (Attributes Only)
    # Asking for "id" and "name" (properties.name)
    # This should trigger a join on Attributes Sidecar but NOT Geometry Sidecar

    # We can't easily spy on the SQL without mocking, but we can verify the OUTPUT structure
    # and ensures it doesn't crash or return geometry when not asked.

    res = await catalogs.search(
        catalog_id,
        collection_id,
        properties=["name"],  # Select specific properties
        include_geometry=False,  # Explicitly exclude geometry
    )

    feature = res["features"][0]

    # Verify Geometry is missing (as requested)
    assert feature.geometry is None

    # Verify Properties are present
    assert feature.properties["name"] == "Test Item"

    # 4. Perform Optimized Search (Geometry Only)
    # This should join Geometry Sidecar but might skip Attributes if we don't ask for props
    # (Though currently we often default to fetching props, let's see if we can exclude them)

    res_geom = await catalogs.search(
        catalog_id,
        collection_id,
        properties=[],  # No properties requested
        include_geometry=True,
    )

    feature_geom = res_geom["features"][0]
    assert feature_geom.geometry is not None
    # We might still get minimal props (id) but payload should be light
