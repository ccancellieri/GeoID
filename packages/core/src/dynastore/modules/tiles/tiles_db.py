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

# dynastore/modules/tiles/tiles_db.py

import logging
from typing import Dict, Any, Optional, List, Tuple, Union
from sqlalchemy import text
from shapely.geometry import box
from shapely import wkb

import morecantile

from dynastore.modules.db_config.query_executor import (
    DQLExecutor,
    ResultHandler,
    DQLQuery,
)
from dynastore.modules.db_config.shared_queries import (
    build_filter_clause,
    get_table_column_names,
)
from dynastore.modules.catalog.validation import get_valid_properties
from dynastore.tools.geospatial import SimplificationAlgorithm
from .tiles_models import TileMatrixSet
from .tiles_config import TilesConfig
from dynastore.modules.db_config.exceptions import InternalValidationError

logger = logging.getLogger(__name__)

# Query to check if a specific SRID exists in PostGIS
check_srid_query = text(
    "SELECT EXISTS (SELECT 1 FROM spatial_ref_sys WHERE srid = :srid)"
)


def _calculate_tile_envelope_wkb(
    tms_def: Union[TileMatrixSet, "morecantile.TileMatrixSet"],
    matrix_id: str,
    x: int,
    y: int,
) -> bytes:
    """
    Calculates the exact bounding box for the tile using Shapely and returns WKB.
    This reduces boilerplate SQL math and leverages the geometry stack.
    """
    matrix = next((m for m in tms_def.tileMatrices if m.id == matrix_id), None)
    if not matrix:
        raise ValueError(f"Matrix {matrix_id} not found")

    origin = matrix.pointOfOrigin
    tile_width = matrix.tileWidth
    cell_size = matrix.cellSize

    tile_span_x = tile_width * cell_size
    tile_span_y = matrix.tileHeight * cell_size

    # Default OGC TopLeft
    min_x = origin[0] + (x * tile_span_x)
    max_x = min_x + tile_span_x

    # y axis points down
    max_y = origin[1] - (y * tile_span_y)
    min_y = max_y - tile_span_y

    # Create shapely box
    bbox = box(min_x, min_y, max_x, max_y)
    # Return WKB hex or binary? Sqlalchemy text() handles params better as binary/string.
    # We will pass this as a bind parameter to ST_GeomFromWKB
    return wkb.dumps(bbox)


async def _build_collection_subquery(
    conn,
    catalog_id: str,
    collection_id: str,
    col_config: Any,
    source_srid: int,
    target_srid: int,
    simplification_by_zoom: Dict[int, float],
    z: str,
    x: int,
    y: int,
    index_i: int,
    datetime_str: Optional[str] = None,
    cql_filter: Optional[str] = None,
    subset_params: Optional[Dict[str, Any]] = None,
    simplification: Optional[float] = None,
    simplification_algorithm: SimplificationAlgorithm = SimplificationAlgorithm.TOPOLOGY_PRESERVING,
    extent: int = 4096,
    buffer: int = 256,
    tile_wkb: Optional[bytes] = None,
) -> Tuple[Optional[str], Dict[str, Any]]:
    """
    Builds the subquery for a single collection using ItemService.
    """
    from dynastore.models.protocols import ItemsProtocol
    from dynastore.tools.discovery import get_protocol

    items_svc = get_protocol(ItemsProtocol)
    if not items_svc:
        return None, {}

    # 1. Resolve Effective Simplification
    eff_simplification = simplification
    if eff_simplification is None and simplification_by_zoom:
        try:
            z_int = int(z)
            for zoom_level, tol in sorted(simplification_by_zoom.items(), reverse=True):
                if z_int >= zoom_level:
                    eff_simplification = tol
                    break
        except ValueError:
            pass

    # 2. Build Parameters for ItemService
    params = {
        "srid": source_srid,
        "target_srid": target_srid,
        "geom_format": "MVT",
        "extent": extent,
        "buffer": buffer,
        "simplification": eff_simplification,
        "simplification_algorithm": simplification_algorithm.value
        if hasattr(simplification_algorithm, "value")
        else str(simplification_algorithm),
        "datetime": datetime_str,
        "cql_filter": cql_filter,
        "tile_wkb": tile_wkb,
    }

    if subset_params:
        params.update(subset_params)

    # 3. Get Query from ItemService
    # We pass tile_wkb via params so GeometrySidecar can use it as bind param

    try:
        sql, bind_params = await items_svc.get_features_query(
            conn,
            catalog_id=catalog_id,
            collection_id=collection_id,
            col_config=col_config,
            params=params,
            param_suffix=f"_{index_i}",
        )
    except ValueError as exc:
        # Storage resolution failed mid-pipeline (e.g. driver config has no
        # physical_table, or catalog row's physical_schema is null).  The
        # tile-resolution-params cache may have served a non-empty meta
        # because driver.location() previously synthesized fallbacks; the
        # deeper resolver disagrees.  Treat as "no features for this
        # collection in this tile" so the caller can still emit a valid
        # (possibly-empty) MVT for the remaining collections.
        logger.warning(
            "Skipping collection %s/%s in tile: %s",
            catalog_id, collection_id, exc,
        )
        return None, {}

    # Return raw SQL (ItemService query now uses :tile_wkb bind param instead of join)
    return sql.rstrip(";"), bind_params


async def get_features_as_mvt_filtered(
    conn,
    resolved_collections: List[Dict[str, Any]],
    tms_def: Union[TileMatrixSet, "morecantile.TileMatrixSet"],
    target_srid: int,
    z: str,
    x: int,
    y: int,
    datetime_str: Optional[str] = None,
    cql_filter: Optional[str] = None,
    subset_params: Optional[Dict[str, Any]] = None,
    simplification: Optional[float] = None,
    simplification_algorithm: SimplificationAlgorithm = SimplificationAlgorithm.TOPOLOGY_PRESERVING,
    extent: int = 4096,
    buffer: int = 256,
):
    """
    Generates MVT using a list of pre-resolved collection metadata.
    Extreme speed: focuses purely on parallel SQL construction and execution.
    """
    # 1. PostGIS check: Ensure target SRID exists
    srid_exists = await DQLQuery(
        check_srid_query, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
    ).execute(conn, srid=target_srid)
    if not srid_exists:
        logger.error(f"SRID {target_srid} missing in PostGIS spatial_ref_sys.")
        return None

    # 2. Calculate Tile Envelope in Python
    try:
        tile_wkb = _calculate_tile_envelope_wkb(tms_def, z, x, y)
    except ValueError:
        return None

    all_bind_params = {"tile_wkb": tile_wkb, "target_srid": target_srid}
    union_queries = []

    # 3. Build Subqueries for each collection
    for i, meta in enumerate(resolved_collections):
        # meta contains: catalog_id, collection_id, col_config, source_srid, simplification_by_zoom
        subq, params = await _build_collection_subquery(
            conn,
            catalog_id=meta["catalog_id"],
            collection_id=meta["collection_id"],
            col_config=meta["col_config"],
            source_srid=meta["source_srid"],
            target_srid=target_srid,
            simplification_by_zoom=meta.get("simplification_by_zoom", {}),
            z=z,
            x=x,
            y=y,
            index_i=i,
            datetime_str=datetime_str,
            cql_filter=cql_filter,
            subset_params=subset_params,
            simplification=simplification,
            simplification_algorithm=simplification_algorithm,
            extent=extent,
            buffer=buffer,
            tile_wkb=tile_wkb,
        )
        if subq:
            union_queries.append(subq)
            all_bind_params.update(params)

    if not union_queries:
        return None

    # 4. Final SQL Execution
    full_query = f"""
        WITH 
        mvtgeom AS ({" UNION ALL ".join(union_queries)})
        SELECT ST_AsMVT(mvtgeom.*, 'default', {extent}, 'geom') 
        FROM mvtgeom;
    """

    logger.info(
        f"Executing MVT query. Bind params types: { {k: type(v) for k, v in all_bind_params.items()} }"
    )
    logger.info(f"target_srid value: {all_bind_params.get('target_srid')}")

    return await DQLQuery(
        full_query, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
    ).execute(conn, **all_bind_params)
