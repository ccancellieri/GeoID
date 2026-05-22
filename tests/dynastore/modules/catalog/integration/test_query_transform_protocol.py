#    Copyright 2025 FAO
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

"""
Integration test for QueryTransformProtocol.

Tests that MVT and DWH query transformations are applied correctly
when generating queries through ItemService.
"""

import pytest
from dynastore.models.query_builder import QueryRequest, FieldSelection
from dynastore.models.protocols import ItemsProtocol, QueryTransformProtocol
from dynastore.tools.discovery import get_protocol, get_protocols
from dynastore.models.driver_context import DriverContext



# Enable extensions required for these tests
pytestmark = [
    pytest.mark.enable_extensions("tiles", "dwh", "features"),
    # 'stac' MODULE (not extension) registers CollectionStacPostgresqlDriver
    # and provisions the collection_stac sidecar at lifespan DDL time.
    # The collection-write code path always touches that sidecar, so omitting
    # it here causes setup_collection to 500 on the first POST /collections.
    pytest.mark.enable_modules(
        "db_config", "db", "catalog", "stats", "stac", "tiles",
        "collection_postgresql", "catalog_postgresql",
    ),
]


async def _activate(catalogs, catalog_id: str, collection_id: str, conn) -> None:
    """Lazy-activation pin: get_features_query needs a resolved physical_table,
    which only exists after activation. Production gets it on the first
    POST /items; tests don't insert, so we trigger it explicitly."""
    await catalogs.activate_collection(
        catalog_id, collection_id, ctx=DriverContext(db_resource=conn)
    )


@pytest.mark.asyncio
async def test_mvt_query_transformation(catalog_id, collection_id, db_engine, app_lifespan, setup_collection):
    """
    Verify that MVTQueryTransform is registered and applies correctly.
    """
    # Get ItemsProtocol
    items_svc = get_protocol(ItemsProtocol)
    assert items_svc is not None

    # Verify MVT transformer is registered
    transformers = get_protocols(QueryTransformProtocol)
    mvt_transformer = next((t for t in transformers if t.transform_id == "mvt"), None)
    assert mvt_transformer is not None, "MVTQueryTransform should be registered"

    # Get collection config
    from dynastore.models.protocols import CatalogsProtocol
    catalogs = get_protocol(CatalogsProtocol)

    async with db_engine.begin() as conn:
        await _activate(catalogs, catalog_id, collection_id, conn)
        col_config = await catalogs.get_collection_config(
            catalog_id, collection_id, ctx=DriverContext(db_resource=conn)
        )
        
        # Build MVT query parameters
        params = {
            "geom_format": "MVT",
            "target_srid": 3857,
            "srid": 4326,
            "tile_wkb": b"\x00" * 100,  # Fake WKB
            "extent": 4096,
            "buffer": 256,
        }
        
        # Generate query
        sql, bind_params = await items_svc.get_features_query(
            conn, catalog_id, collection_id, col_config, params
        )
        
        # Verify MVT transformation was applied
        assert "ST_AsMVTGeom" in sql, "MVT transformation should add ST_AsMVTGeom"
        assert "ST_Intersects" in sql, "MVT transformation should add spatial filter"
        assert "target_srid" in bind_params
        assert "tile_wkb" in bind_params
        # extent/buffer are emitted as literals inside ST_AsMVTGeom (matching the
        # wrapping ST_AsMVT extent), not as bind params.
        assert "4096" in sql
        assert "extent" not in bind_params
        assert "buffer" not in bind_params


@pytest.mark.asyncio
async def test_dwh_join_transformation(catalog_id, collection_id, db_engine, app_lifespan, setup_collection):
    """
    Verify that DWHJoinQueryTransform is registered and applies correctly.
    """
    # Get ItemsProtocol
    items_svc = get_protocol(ItemsProtocol)
    assert items_svc is not None
    
    # Unwrap CatalogService if needed to access private methods
    if hasattr(items_svc, "_item_service"):
        items_svc = items_svc._item_service
    
    # Verify DWH transformer is registered
    transformers = get_protocols(QueryTransformProtocol)
    dwh_transformer = next((t for t in transformers if t.transform_id == "dwh_join"), None)
    assert dwh_transformer is not None, "DWHJoinQueryTransform should be registered"
    
    # Get collection config
    from dynastore.models.protocols import CatalogsProtocol
    catalogs = get_protocol(CatalogsProtocol)
    
    async with db_engine.begin() as conn:
        await _activate(catalogs, catalog_id, collection_id, conn)
        col_config = await catalogs.get_collection_config(
            catalog_id, collection_id, ctx=DriverContext(db_resource=conn)
        )

        # Build QueryRequest with DWH join context
        request = QueryRequest(
            select=[
                FieldSelection(field="geoid", alias="id"),
                FieldSelection(field="geom"),
                FieldSelection(field="attributes"),
            ],
            raw_params={"dwh_join_column": "admin_code"}
        )
        
        # Build context for transformation
        context = {
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "col_config": col_config,
            "dwh_join_column": "admin_code",
        }
        
        # Apply transformations. Pass db_resource so storage resolution
        # sees the activation we just did inside this same transaction.
        sql, params = await items_svc._apply_query_transformations(
            request, context, catalog_id, collection_id, col_config,
            db_resource=conn,
        )
        
        # Verify DWH transformation was applied
        # The join column should be in the SELECT
        assert "admin_code" in sql, "DWH transformation should ensure join column is selected"


@pytest.mark.asyncio
async def test_transformation_priority_order(app_lifespan):
    """
    Verify that transformations are applied in priority order.
    """
    
    # Get all transformers
    transformers = get_protocols(QueryTransformProtocol)
    
    # Verify we have multiple transformers
    assert len(transformers) >= 2, "Should have at least MVT and DWH transformers"
    
    # Verify they have different priorities
    priorities = [getattr(t, "priority", 100) for t in transformers]
    transformer_ids = [t.transform_id for t in transformers]
    
    # MVT should have priority 100, DWH should have priority 50
    mvt_priority = next((p for t, p in zip(transformers, priorities) if t.transform_id == "mvt"), None)
    dwh_priority = next((p for t, p in zip(transformers, priorities) if t.transform_id == "dwh_join"), None)
    
    assert mvt_priority == 100, "MVT transformer should have priority 100"
    assert dwh_priority == 50, "DWH transformer should have priority 50"
    assert dwh_priority < mvt_priority, "DWH should run before MVT (lower priority = earlier)"


@pytest.mark.asyncio
async def test_no_transformation_when_not_applicable(catalog_id, collection_id, db_engine, app_lifespan, setup_collection):
    """
    Verify that transformations don't apply when context doesn't match.
    """
    items_svc = get_protocol(ItemsProtocol)
    from dynastore.models.protocols import CatalogsProtocol
    catalogs = get_protocol(CatalogsProtocol)
    
    async with db_engine.begin() as conn:
        await _activate(catalogs, catalog_id, collection_id, conn)
        col_config = await catalogs.get_collection_config(
            catalog_id, collection_id, ctx=DriverContext(db_resource=conn)
        )

        # Build standard GeoJSON query (no MVT, no DWH)
        params = {
            "geom_format": "WKB",  # Not MVT
            # No dwh_join_column
        }
        
        sql, bind_params = await items_svc.get_features_query(
            conn, catalog_id, collection_id, col_config, params
        )
        
        # Verify transformations were NOT applied
        assert "ST_AsMVTGeom" not in sql, "MVT transformation should not apply for WKB format"
        assert "tile_wkb" not in bind_params, "MVT params should not be present"
