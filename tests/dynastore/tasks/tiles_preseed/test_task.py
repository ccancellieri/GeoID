
import pytest
import asyncio
from tests.dynastore.test_utils import generate_test_id
from dynastore.tasks.tiles_preseed.task import TilePreseedTask
from dynastore.tasks.tiles_preseed.models import TilePreseedRequest
from dynastore.modules.processes.models import ExecuteRequest
from dynastore.modules.tasks.models import TaskPayload, TaskStatusEnum, TaskCreate
from dynastore.modules.tasks import tasks_module
from dynastore.modules.tiles.tiles_config import TilesPreseedConfig
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler
from dynastore.models.driver_context import DriverContext

@pytest.mark.asyncio
@pytest.mark.enable_modules("db_config", "db", "catalog", "stac", "tiles", "tasks", "crs", "collection_postgresql", "catalog_postgresql")
@pytest.mark.enable_extensions("tiles", "assets", "features")
# @pytest.mark.skip(reason="WIP")
async def test_tile_preseed_task_run_integration(app_lifespan, in_process_client):
    """
    Integration test for TilePreseedTask.
    Verifies that tiles are correctly generated and stored in Postgres ('pg' provider).
    """
    catalog_id = f"cat_preseed_{generate_test_id(12)}"
    collection_id = f"coll_preseed_{generate_test_id(12)}"
    
    # 1. Setup Catalog and Collection via API
    resp = await in_process_client.post(
        "/features/catalogs", 
        json={"id": catalog_id, "title": "Preseed Test Catalog"}
    )
    assert resp.status_code == 201, resp.text
    
    resp = await in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", 
        json={
            "id": collection_id,
            "title": "Preseed Test Collection",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]}
            }
        }
    )
    assert resp.status_code == 201, resp.text
    
    # 2. Ingest a Feature to generate tiles from
    bulk_payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]]},
                "properties": {"name": "Test Area"}
            }
        ]
    }
    resp = await in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}/items", 
        json=bulk_payload
    )
    
    # 3. Configure Tiles Plugin for the catalog (to set zoom limits)
    from dynastore.modules.tiles.tiles_config import TilesConfig
    config_service = get_protocol(ConfigsProtocol)
    tiles_plugin_conf = TilesConfig(
        min_zoom=0,
        max_zoom=2, # Limit zoom for speed
        supported_tms_ids=["WebMercatorQuad"]
    )
    await config_service.set_config(TilesConfig, tiles_plugin_conf, catalog_id=catalog_id)

    # 4. Configure Preseed for 'pg' storage
    preseed_conf = TilesPreseedConfig(
        preseed_enabled=True,
        target_tms_ids=["WebMercatorQuad"],
        formats=["mvt"],
        storage_priority=["pg"],
        collections_to_preseed=[collection_id]
    )
    await config_service.set_config(TilesPreseedConfig, preseed_conf, catalog_id=catalog_id)
    
    # 4. Instantiate and Run Task
    task = TilePreseedTask(app_lifespan)
    request = TilePreseedRequest(
        catalog_id=catalog_id, 
        collection_id=collection_id,
        tms_ids=["WebMercatorQuad"],
        formats=["mvt"]
    )
    
    # Resolve physical schema to create task in the right place
    # This also ensures the tasks table is created via the task_module.create_task logic
    catalogs = get_protocol(CatalogsProtocol)
    schema = await catalogs.resolve_physical_schema(catalog_id, ctx=DriverContext(db_resource=app_lifespan.engine))
    
    # Properly create task record to ensure table existence
    db_task = await tasks_module.create_task(
        app_lifespan.engine,
        TaskCreate(
            task_type="tiles_preseed",
            caller_id="test_runner",
            inputs=request.model_dump(),
            collection_id=collection_id
        ),
        schema=schema
    )
    
    # Wrap in ExecuteRequest to match the OGC Process dispatch shape the task
    # expects (task.py:62-70 reads payload.inputs.inputs). Same pattern as
    # DwhJoinExportTask and ExportFeaturesTask payloads.
    payload = TaskPayload(
        task_id=db_task.task_id,
        inputs=ExecuteRequest(inputs=request.model_dump()),
        caller_id="test_admin"
    )
    
    await task.run(payload)
    
    # 6. Verify Database Storage
    # TilesPGPreseedStorage saves to the physical schema
    async with app_lifespan.engine.connect() as conn:
        query = f'SELECT COUNT(*) FROM "{schema}".preseeded_tiles WHERE collection_id = :col'
        count = await DQLQuery(query, result_handler=ResultHandler.SCALAR_ONE).execute(conn, col=collection_id)
        assert count >= 1
        
        # Verify specific tile (0/0/0) exists for WebMercatorQuad
        query_tile = f'SELECT data FROM "{schema}".preseeded_tiles WHERE collection_id = :col AND z=0 AND x=0 AND y=0'
        tile_data = await DQLQuery(query_tile, result_handler=ResultHandler.SCALAR_ONE_OR_NONE).execute(conn, col=collection_id)
        assert tile_data is not None
        assert len(tile_data) > 0
