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

# dynastore/extensions/maps/maps_db.py

from typing import Dict, Any, Optional, List
from sqlalchemy.ext.asyncio import AsyncConnection
import asyncio

from dynastore.modules.db_config import shared_queries
from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler

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

    Multi-collection (UNION) requests must be **schema-homogeneous**: every
    collection in `collections` must expose the same column set and resolve to
    the same source SRID. The single `where_clause` and `source_srid` derived
    from `collections[0]` are applied to every UNION arm; diverging collections
    would silently produce wrong tiles (a column referenced in `subset_params`
    that exists only in collection[0] would explode on the next arm, and a
    spatial filter against the wrong storage CRS would return empty results).
    The heterogeneity check below raises ``ValueError`` (mapped to HTTP 400 by
    the caller) so the failure mode is explicit instead of silent. Refs #737.
    """
    from dynastore.modules.storage.router import get_driver
    from dynastore.modules.storage.routing_config import Operation
    from dynastore.modules.storage.drivers.pg_sidecars import driver_sidecars
    from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
        GeometriesSidecarConfig,
    )

    def _resolve_source_srid(layer_cfg: Any) -> int:
        # Sidecars are PG-driver-internal — driver_sidecars() returns []
        # for non-PG resolved layer configs and we fall back to 4326.
        return next(
            (
                sc.target_srid
                for sc in driver_sidecars(layer_cfg)
                if isinstance(sc, GeometriesSidecarConfig)
            ),
            4326,
        )

    async def _resolve_collection_meta(collection: str) -> tuple[str, List[str], int]:
        drv = await get_driver(Operation.READ, schema, collection)
        # ``collection_id`` is not guaranteed to be the physical table name —
        # resolve it the same way every other physical read/write path does
        # (see ``CatalogsProtocol.resolve_physical_table`` / GeoID #2325).
        # Querying/rendering against the raw collection id verbatim
        # false-negatives (or renders an empty/wrong table) whenever the
        # physical table diverges from the collection id.
        physical_table = collection
        if hasattr(drv, "resolve_physical_table"):
            resolved = await drv.resolve_physical_table(  # type: ignore[attr-defined]
                schema, collection, db_resource=conn
            )
            physical_table = resolved or collection
        cols, cfg = await asyncio.gather(
            shared_queries.get_table_column_names(conn, schema, physical_table),
            drv.get_driver_config(schema, collection),
        )
        return physical_table, cols, _resolve_source_srid(cfg)

    # Single-collection (the hot path) keeps its previous one-pass shape; the
    # multi-collection path resolves metadata for every arm in parallel and
    # asserts homogeneity before building the UNION.
    if len(collections) == 1:
        physical_table, table_columns, source_srid = await _resolve_collection_meta(collections[0])
        physical_tables = [physical_table]
    else:
        metas = await asyncio.gather(*(_resolve_collection_meta(c) for c in collections))
        physical_tables = [m[0] for m in metas]
        table_columns, source_srid = metas[0][1], metas[0][2]
        base_cols = set(table_columns)
        for collection, (_, cols, srid) in zip(collections[1:], metas[1:]):
            if set(cols) != base_cols:
                raise ValueError(
                    f"Heterogeneous multi-collection map request: column sets differ "
                    f"between '{collections[0]}' and '{collection}'. UNION rendering "
                    f"requires schema-homogeneous collections."
                )
            if srid != source_srid:
                raise ValueError(
                    f"Heterogeneous multi-collection map request: source SRID differs "
                    f"between '{collections[0]}' ({source_srid}) and '{collection}' ({srid}). "
                    f"UNION rendering requires a single storage CRS."
                )

    where_clause, bind_params = shared_queries.build_filter_clause(table_columns, datetime_str, subset_params)

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
    for collection, physical_table in zip(collections, physical_tables):
        # We simplify the geometry in PostGIS before sending it to Python.
        # This significantly boosts performance for large datasets.
        # ``layer`` stays the (external-facing) collection id; the FROM
        # clause uses the resolved physical table, which may diverge from it.
        union_queries.append(f"""
            SELECT
                '{collection}' as layer,
                ST_AsBinary(
                    ST_SimplifyPreserveTopology(geom, {resolution_sql})
                ) as geom,
                geoid,
                attributes
            FROM "{schema}"."{physical_table}"
            WHERE {spatial_filter} AND ({where_clause})
        """)

    full_query = ' UNION ALL '.join(union_queries)
    final_params = {
        'xmin': xmin, 'ymin': ymin, 'xmax': xmax, 'ymax': ymax,
        'img_width': width,
        **bind_params
    }
    
    return await DQLQuery(full_query, result_handler=ResultHandler.ALL_DICTS).execute(conn, **final_params)