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
import os
from sqlalchemy import text

pytest.importorskip("geopandas")  # optional dep — skip when SCOPE excludes it

from dynastore.tools.identifiers import generate_task_id, generate_id_hex
from dynastore.tasks.ingestion.ingestion_task import IngestionTask
from dynastore.modules.tasks.models import TaskPayload
from dynastore.modules.processes.models import ExecuteRequest
from dynastore.models.protocols import CatalogsProtocol, ConfigsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.models.driver_context import DriverContext


@pytest.fixture(autouse=True)
def setup_ingestion_env(monkeypatch):
    """Ensure proxy module is enabled for ingestion tests."""
    monkeypatch.setenv("SCOPE", "db_config,db,catalog,gcp,stats,tasks")
    monkeypatch.setenv("DYNASTORE_TESTING", "true")


@pytest.mark.asyncio
async def test_tiles_module_not_loaded(task_app_state):
    """
    Verifies that TilesModule is NOT instantiated when not in SCOPE.
    """
    from typing import Protocol
    from dynastore.tools.discovery import get_protocol
    
    # Check that a non-existent protocol provider is NOT initialized
    class NonExistentProtocol(Protocol): pass
    
    with pytest.raises(Exception):
        get_protocol(NonExistentProtocol)


@pytest.mark.asyncio
async def test_geojson_ingestion(task_app_state, test_data_loader, data_id):
    """
    Integration test for GeoJSON ingestion.
    """
    catalog_id = f"cat_geo_{data_id}"

    collection_id = "test_points_geojson"

    # Get absolute path for geojson
    geojson_path = os.path.join(os.path.dirname(__file__), "data", "points.geojson")

    # 1. Setup Catalog and Collection with Schema
    from dynastore.modules.catalog.models import (
        Catalog,
        Collection,
        Extent,
        SpatialExtent,
        TemporalExtent,
    )

    catalogs: CatalogsProtocol = get_protocol(CatalogsProtocol)
    configs: ConfigsProtocol = get_protocol(ConfigsProtocol)
    async with managed_transaction(task_app_state.engine) as conn:
        # Cleanup
        phys_schema_before = await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
        )
        if phys_schema_before:
            await conn.execute(
                text(f'DROP SCHEMA IF EXISTS "{phys_schema_before}" CASCADE')
            )

    # Let the API calls commit their own transactions so background tasks can see them
    await catalogs.create_catalog(
        Catalog(id=catalog_id, title=catalog_id)
    )

    col_def = Collection(
        id=collection_id,
        title=collection_id,
        description="Test collection",
        extent=Extent(
            spatial=SpatialExtent(bbox=[[-180.0, -90.0, 180.0, 90.0]]),
            temporal=TemporalExtent(interval=[[None, None]]),
        ),
        attribute_schema={
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "value": {"type": "integer"},
            },
        },
    )
    await catalogs.create_collection(catalog_id, col_def)

    # FORCE DISABLE PARTITIONING & DROP TABLE to ensure ingestion works without H3 complications
    # which are not auto-handled by simple ingestion yet.
    from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
    pg_plugin_id = ItemsPostgresqlDriverConfig
    config = await configs.get_config(pg_plugin_id, catalog_id=catalog_id, collection_id=collection_id)
    if config.partitioning:
        config.partitioning.enabled = False
    await configs.set_config(
        pg_plugin_id,
        config,
        catalog_id=catalog_id,
        collection_id=collection_id,
    )

    async with managed_transaction(task_app_state.engine) as conn:
        phys_schema = await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=conn)
        )
        phys_table = await catalogs.resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )
        if phys_schema and phys_table:
            await conn.execute(
                text(f'DROP TABLE IF EXISTS "{phys_schema}"."{phys_table}" CASCADE')
            )

    # 2. Prepare Task
    task = IngestionTask(task_app_state)

    payload = TaskPayload(
        task_id=generate_task_id(),
        caller_id="test_user",
        inputs=ExecuteRequest(
            inputs={
                "catalog_id": catalog_id,
                "collection_id": collection_id,
                "ingestion_request": {
                    "asset": {"uri": geojson_path},
                    "source_srid": 4326,
                    "column_mapping": {
                        "external_id": "id",
                        "attributes_source_type": "all",
                    },
                },
            }
        ),
    )

    # 3. Execution
    await task.run(payload)

    # 4. Verification
    async with task_app_state.engine.connect() as conn:
        # Re-resolve physical names to navigate the cellular structure
        catalogs = get_protocol(CatalogsProtocol)
        phys_schema = await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=conn)
        )
        phys_table = await catalogs.resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )

        result = await conn.execute(
            text(f'SELECT count(*) FROM "{phys_schema}"."{phys_table}"')
        )
        count = result.scalar()
        assert count == 2

        # Verify columns (joining attributes sidecar)
        result = await conn.execute(
            text(
                f'SELECT s.attributes->>\'name\', (s.attributes->>\'value\')::int FROM "{phys_schema}"."{phys_table}" h JOIN "{phys_schema}"."{phys_table}_attributes" s ON h.geoid = s.geoid ORDER BY s.external_id'
            )
        )
        rows = result.fetchall()
        assert rows[0][0] == "Rome"
        assert rows[0][1] == 100
        assert rows[1][0] == "Milan"
        assert rows[1][1] == 200


@pytest.mark.asyncio
async def test_geojson_ingestion_rerun_converges_without_configured_external_id(
    task_app_state, test_data_loader, data_id,
):
    """Regression coverage for GeoID #2709.

    Re-running the SAME ingestion job — with no ``column_mapping.external_id``
    configured at all — must CONVERGE (2 features stay 2) rather than
    duplicate every feature on the second run. Deliberately omits
    ``external_id`` so identity falls through to the reader-surfaced natural
    id / OGR-FID tier rather than the explicit-field tier already covered by
    ``test_geojson_ingestion``.
    """
    catalog_id = f"cat_geo_rerun_{data_id}"
    collection_id = "test_points_geojson_rerun"

    geojson_path = os.path.join(os.path.dirname(__file__), "data", "points.geojson")

    from dynastore.modules.catalog.models import (
        Catalog,
        Collection,
        Extent,
        SpatialExtent,
        TemporalExtent,
    )

    catalogs: CatalogsProtocol = get_protocol(CatalogsProtocol)
    configs: ConfigsProtocol = get_protocol(ConfigsProtocol)
    async with managed_transaction(task_app_state.engine) as conn:
        phys_schema_before = await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
        )
        if phys_schema_before:
            await conn.execute(
                text(f'DROP SCHEMA IF EXISTS "{phys_schema_before}" CASCADE')
            )

    await catalogs.create_catalog(Catalog(id=catalog_id, title=catalog_id))

    col_def = Collection(
        id=collection_id,
        title=collection_id,
        description="Test collection",
        extent=Extent(
            spatial=SpatialExtent(bbox=[[-180.0, -90.0, 180.0, 90.0]]),
            temporal=TemporalExtent(interval=[[None, None]]),
        ),
        attribute_schema={
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "value": {"type": "integer"},
            },
        },
    )
    await catalogs.create_collection(catalog_id, col_def)

    from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
    pg_plugin_id = ItemsPostgresqlDriverConfig
    config = await configs.get_config(pg_plugin_id, catalog_id=catalog_id, collection_id=collection_id)
    if config.partitioning:
        config.partitioning.enabled = False
    await configs.set_config(
        pg_plugin_id, config, catalog_id=catalog_id, collection_id=collection_id,
    )

    async with managed_transaction(task_app_state.engine) as conn:
        phys_schema = await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=conn)
        )
        phys_table = await catalogs.resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )
        if phys_schema and phys_table:
            await conn.execute(
                text(f'DROP TABLE IF EXISTS "{phys_schema}"."{phys_table}" CASCADE')
            )

    task = IngestionTask(task_app_state)

    def _payload() -> TaskPayload:
        return TaskPayload(
            task_id=generate_task_id(),
            caller_id="test_user",
            inputs=ExecuteRequest(
                inputs={
                    "catalog_id": catalog_id,
                    "collection_id": collection_id,
                    "ingestion_request": {
                        "asset": {"uri": geojson_path},
                        "source_srid": 4326,
                        # No column_mapping.external_id — exercises the
                        # deterministic-identity fallback tiers (#2709)
                        # instead of an explicitly configured field.
                        "column_mapping": {"attributes_source_type": "all"},
                    },
                }
            ),
        )

    async def _count() -> int:
        async with task_app_state.engine.connect() as conn:
            phys_schema = await catalogs.resolve_physical_schema(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )
            phys_table = await catalogs.resolve_physical_table(
                catalog_id, collection_id, db_resource=conn
            )
            result = await conn.execute(
                text(f'SELECT count(*) FROM "{phys_schema}"."{phys_table}"')
            )
            return result.scalar()

    # First run: inserts both features.
    await task.run(_payload())
    assert await _count() == 2

    # Second run of the SAME source: must converge (upsert), not duplicate.
    await task.run(_payload())
    assert await _count() == 2


@pytest.mark.asyncio
async def test_csv_ingestion(task_app_state, test_data_loader, data_id):
    """
    Integration test for CSV ingestion with explicit column mapping.
    """
    catalog_id = f"cat_csv_{data_id}"
    collection_id = "test_points_csv"

    # Get absolute path for csv
    csv_path = os.path.join(os.path.dirname(__file__), "data", "points.csv")

    # 1. Setup Catalog and Collection with Schema
    from dynastore.modules.catalog.models import (
        Catalog,
        Collection,
        Extent,
        SpatialExtent,
        TemporalExtent,
    )
    from dynastore.modules.catalog.models import (
        Catalog,
        Collection,
        Extent,
        SpatialExtent,
        TemporalExtent,
    )

    catalogs = get_protocol(CatalogsProtocol)
    async with managed_transaction(task_app_state.engine) as conn:
        # Cleanup
        phys_schema_before = await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
        )
        if phys_schema_before:
            await conn.execute(
                text(f'DROP SCHEMA IF EXISTS "{phys_schema_before}" CASCADE')
            )

        await catalogs.create_catalog(
            Catalog(id=catalog_id, title=catalog_id), ctx=DriverContext(db_resource=conn)
        )

        col_def = Collection(
            id=collection_id,
            title=collection_id,
            description="Test collection",
            extent=Extent(
                spatial=SpatialExtent(bbox=[[-180.0, -90.0, 180.0, 90.0]]),
                temporal=TemporalExtent(interval=[[None, None]]),
            ),
            attribute_schema={
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "value": {"type": "integer"},
                },
            },
        )
        await catalogs.create_collection(catalog_id, col_def, ctx=DriverContext(db_resource=conn))

        # FORCE DISABLE PARTITIONING & DROP TABLE
        # The legacy "driver:postgresql" string key was retired in the
        # April-2026 configs-API reshape; identity is now the config class.
        from dynastore.modules.storage.driver_config import (
            ItemsPostgresqlDriverConfig as _PgDriverConfig,
        )
        configs = get_protocol(ConfigsProtocol)
        config = await configs.get_config(
            _PgDriverConfig,
            catalog_id=catalog_id,
            collection_id=collection_id,
            ctx=DriverContext(db_resource=conn),
        )
        if config.partitioning:
            config.partitioning.enabled = False
        await configs.set_config(
            _PgDriverConfig,
            config,
            catalog_id=catalog_id,
            collection_id=collection_id,
            ctx=DriverContext(db_resource=conn),
        )

        phys_schema = await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=conn)
        )
        phys_table = await catalogs.resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )
        if phys_schema and phys_table:
            await conn.execute(
                text(f'DROP TABLE IF EXISTS "{phys_schema}"."{phys_table}" CASCADE')
            )

    # 2. Prepare Task
    task = IngestionTask(task_app_state)

    payload = TaskPayload(
        task_id=generate_task_id(),
        caller_id="test_user",
        inputs=ExecuteRequest(
            inputs={
                "catalog_id": catalog_id,
                "collection_id": collection_id,
                "ingestion_request": {
                    "asset": {"uri": csv_path},
                    "source_srid": 4326,
                    "column_mapping": {
                        "external_id": "id",
                        "csv_lat_column": "lat",
                        "csv_lon_column": "lon",
                        "attributes_source_type": "all",
                    },
                },
            }
        ),
    )

    # 3. Execution
    await task.run(payload)

    # 4. Verification
    # 4. Verification
    async with task_app_state.engine.connect() as conn:
        catalogs = get_protocol(CatalogsProtocol)
        phys_schema = await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=conn)
        )
        phys_table = await catalogs.resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )

        # Check count
        result = await conn.execute(
            text(f'SELECT count(*) FROM "{phys_schema}"."{phys_table}"')
        )
        assert result.scalar() == 2

        # Check data (joining attributes sidecar)
        result = await conn.execute(
            text(
                f'SELECT s.attributes->>\'name\', (s.attributes->>\'value\')::int FROM "{phys_schema}"."{phys_table}" h JOIN "{phys_schema}"."{phys_table}_attributes" s ON h.geoid = s.geoid ORDER BY s.external_id'
            )
        )
        rows = result.fetchall()
        assert rows[0][0] == "Rome"
        assert rows[1][0] == "Milan"


@pytest.mark.asyncio
async def test_csv_pointz_ingestion(task_app_state, test_data_loader, data_id):
    """
    Integration test for CSV ingestion with PointZ support.
    """
    catalog_id = f"cat_csvz_{generate_id_hex()}"
    collection_id = f"test_points_csvz_{generate_id_hex()}"

    csv_path = os.path.join(os.path.dirname(__file__), "data", "points_z.csv")

    # 1. Setup Catalog and Collection
    from dynastore.modules.catalog.models import (
        Catalog,
        Collection,
        Extent,
        SpatialExtent,
        TemporalExtent,
    )
    from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
        GeometriesSidecarConfig,
        TargetDimension,
    )
    from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
        FeatureAttributeSidecarConfig,
    )

    catalogs = get_protocol(CatalogsProtocol)
    await catalogs.create_catalog(
        Catalog(id=catalog_id, title=catalog_id)
    )

    # Configure sidecars explicitly with 3D support
    col_def = Collection(
        id=collection_id,
        title=collection_id,
        extent=Extent(
            spatial=SpatialExtent(bbox=[[-180.0, -90.0, 180.0, 90.0]]),
            temporal=TemporalExtent(interval=[[None, None]]),
        ),
        sidecars=[
            GeometriesSidecarConfig(target_dimension=TargetDimension.FORCE_3D),
            FeatureAttributeSidecarConfig(),
        ],
        attribute_schema={
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "value": {"type": "integer"},
            },
        },
    )
    await catalogs.create_collection(catalog_id, col_def)

    configs = get_protocol(ConfigsProtocol)
    from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
    pg_plugin_id = ItemsPostgresqlDriverConfig
    config = await configs.get_config(pg_plugin_id, catalog_id=catalog_id, collection_id=collection_id)
    if config.partitioning:
        config.partitioning.enabled = False
    await configs.set_config(
        pg_plugin_id,
        config,
        catalog_id=catalog_id,
        collection_id=collection_id,
    )

    # 2. Prepare Task
    task = IngestionTask(task_app_state)
    payload = TaskPayload(
        task_id=generate_task_id(),
        caller_id="test_user",
        inputs=ExecuteRequest(
            inputs={
                "catalog_id": catalog_id,
                "collection_id": collection_id,
                "ingestion_request": {
                    "asset": {"uri": csv_path},
                    "source_srid": 4326,
                    "column_mapping": {
                        "external_id": "id",
                        "csv_lat_column": "lat",
                        "csv_lon_column": "lon",
                        "csv_elevation_column": "elev",
                        "attributes_source_type": "all",
                    },
                },
            }
        ),
    )

    # 3. Execution
    await task.run(payload)

    # 4. Verification
    async with task_app_state.engine.connect() as conn:
        phys_schema = await catalogs.resolve_physical_schema(
            catalog_id, ctx=DriverContext(db_resource=conn)
        )
        phys_table = await catalogs.resolve_physical_table(
            catalog_id, collection_id, db_resource=conn
        )

        # Check Z dimension in PostGIS (ST_Z returns NULL for 2D, but here we expect the value)
        result = await conn.execute(
            text(
                f'SELECT ST_AsText(geom) FROM "{phys_schema}"."{phys_table}_geometries" ORDER BY geoid'
            )
        )
        wkt_rows = result.fetchall()
        assert "50.5" in wkt_rows[0][0]
        assert "120" in wkt_rows[1][0]

        # Verify it has 3 dimensions
        result = await conn.execute(
            text(
                f'SELECT ST_CoordDim(geom) FROM "{phys_schema}"."{phys_table}_geometries" LIMIT 1'
            )
        )
        assert result.scalar() == 3
