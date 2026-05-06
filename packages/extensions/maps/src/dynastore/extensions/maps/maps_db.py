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
# 
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

# dynastore/extensions/maps/maps_db.py

import re
from typing import Dict, Any, Optional, List
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection
import asyncio

from dynastore.modules.db_config import shared_queries
from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler
import dynastore.modules.catalog.catalog_module as catalog_manager

async def get_features_for_rendering(
    conn: AsyncConnection, 
    schema: str, 
    collections: List[str],
    bbox: List[float], 
    crs: str,
    width: int, 
    height: int,
    bbox_srid: int = 4326, # Defaults to OGC:CRS84 per OGC API Maps Req 18
    datetime_str: Optional[str] = None,
    subset_params: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Fetches geometries and attributes for rendering, with full filter capabilities.
    
    Optimizations:
    1. Decouples Input BBOX CRS (bbox_srid) from Output Map CRS.
    2. Calculates dynamic simplification tolerance based on request width/height.
    3. Performs simplification in PostGIS to reduce I/O and memory usage.
    """
    # Fetch column names and layer config for the first collection to build the query.
    from dynastore.modules.storage.router import get_driver
    from dynastore.modules.storage.routing_config import Operation
    _driver = await get_driver(Operation.READ, schema, collections[0])
    table_columns, layer_config = await asyncio.gather(
        shared_queries.get_table_column_names(conn, schema, collections[0]),
        _driver.get_driver_config(schema, collections[0]),
    )
    
    where_clause, bind_params = shared_queries.build_filter_clause(table_columns, datetime_str, subset_params)
    
    # Identify source SRID from the PG geometries sidecar, if any.
    # Sidecars are PG-driver-internal — driver_sidecars() returns []
    # for non-PG resolved layer configs and we fall back to 4326.
    from dynastore.modules.storage.drivers.pg_sidecars import driver_sidecars
    from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
        GeometriesSidecarConfig,
    )
    source_srid = next(
        (
            sc.target_srid
            for sc in driver_sidecars(layer_config)
            if isinstance(sc, GeometriesSidecarConfig)
        ),
        4326,
    )

    # --- Handle Coordinate System Limits & Input CRS ---
    xmin, ymin, xmax, ymax = bbox

    # Fix for PostGIS error when transforming global 4326 bboxes to 3857 (Web Mercator).
    # If the INPUT bbox is 4326 and the STORAGE is 3857, we must clamp before transform.
    # (Note: This logic handles the specific case where we filter against 3857 storage)
    if bbox_srid == 4326 and source_srid == 3857:
        MAX_LAT = 85.05112878
        ymin = max(ymin, -MAX_LAT)
        ymax = min(ymax, MAX_LAT)

    # 1. Create the BBOX envelope in its native Input CRS (bbox_srid)
    bbox_envelope_sql = f"ST_MakeEnvelope(:xmin, :ymin, :xmax, :ymax, {bbox_srid})"
    
    # 2. Transform that envelope to the Source CRS to use with the Spatial Index
    source_envelope_sql = f"ST_Transform({bbox_envelope_sql}, {source_srid})"
    spatial_filter = f"ST_Intersects(geom, {source_envelope_sql})"
    
    # 3. Calculate Simplification Tolerance (Generalization)
    # Tolerance = (Width of BBOX in Source Units) / (Image Width in Pixels)
    # We use the transformed envelope width to determine scale in source units.
    # We add a CASE to prevent division by zero or a zero tolerance, which causes a PostGIS error.
    resolution_sql = f"GREATEST( (ST_XMax({source_envelope_sql}) - ST_XMin({source_envelope_sql})) / GREATEST(:img_width, 1), 1e-9 )"

    union_queries = []   
    for collection in collections:
        # We simplify the geometry in PostGIS before sending it to Python.
        # This significantly boosts performance for large datasets.
        union_queries.append(f"""
            SELECT 
                '{collection}' as layer, 
                ST_AsBinary(
                    ST_SimplifyPreserveTopology(geom, {resolution_sql})
                ) as geom, 
                geoid, 
                attributes
            FROM "{schema}"."{collection}" 
            WHERE {spatial_filter} AND ({where_clause})
        """)

    full_query = ' UNION ALL '.join(union_queries)
    final_params = {
        'xmin': xmin, 'ymin': ymin, 'xmax': xmax, 'ymax': ymax,
        'img_width': width,
        **bind_params
    }
    
    return await DQLQuery(full_query, result_handler=ResultHandler.ALL_DICTS).execute(conn, **final_params)