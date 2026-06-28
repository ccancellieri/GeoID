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

# dynastore/modules/tiles/tiles_db.py

import logging
from typing import Dict, Any, List, Mapping, Optional, Tuple, Union
from sqlalchemy import text
from shapely.geometry import box
from shapely import wkb

import morecantile

from dynastore.modules.db_config.query_executor import (
    ResultHandler,
    DQLQuery,
)
from dynastore.tools.geospatial import SimplificationAlgorithm
from .tiles_models import TileMatrixSet

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
    from dynastore.models.protocols import ConfigsProtocol, ItemsProtocol
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

    # Resolve the collection's read-shape contract once. ST_AsMVT emits every
    # selected column as a tile property, so honouring feature_type at SELECT
    # time is the only way to prevent leaks (raw geometry WKB, undeclared
    # JSONB keys, geoid).
    #
    # We pass the raw ``ItemsSchema.fields`` dict down to the SSOT helper
    # ``project_select_for_feature_type`` — it filters out geometry-typed and
    # ``expose=False`` entries internally (read-side mirror of the write SSOTs
    # ``schema_field_materializes_as_column`` / ``bridge_schema_to_attribute_sidecar``,
    # which keep geometry out of the attributes sidecar). The driver's MVT
    # query then materialises the per-row ``ST_AsMVTGeom(...) AS geom`` and
    # the wrapping ``ST_AsMVT`` aggregates only those columns.
    feature_type = None
    schema_fields: Optional[Mapping[str, Any]] = None
    try:
        from dynastore.modules.storage.driver_config import ItemsSchema
        from dynastore.modules.storage.read_policy import ItemsReadPolicy

        configs = get_protocol(ConfigsProtocol)
        if configs is not None:
            policy = await configs.get_config(
                ItemsReadPolicy,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
            feature_type = getattr(policy, "feature_type", None)
            schema = await configs.get_config(
                ItemsSchema,
                catalog_id=catalog_id,
                collection_id=collection_id,
            )
            schema_fields = getattr(schema, "fields", None) or {}
    except Exception as exc:  # noqa: BLE001 — read assembly must not break on config miss
        logger.debug(
            "tile read_policy resolution skipped for %s/%s: %s",
            catalog_id, collection_id, exc,
        )

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
        "feature_type": feature_type,
        "schema_fields": schema_fields,
    }

    if subset_params:
        params.update(subset_params)

    # 3. Get Query from ItemService
    # We pass tile_wkb via params so GeometrySidecar can use it as bind param

    # Privileged system read: tile rendering is a server-side operation with no
    # end-user principal; allow all rows from the envelope JOIN.
    from dynastore.models.protocols.access_filter import AccessFilter
    try:
        sql, bind_params = await items_svc.get_features_query(
            conn,
            catalog_id=catalog_id,
            collection_id=collection_id,
            col_config=col_config,
            params=params,
            param_suffix=f"_{index_i}",
            access_filter=AccessFilter.allow_everything(),
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

    # 4. Resolve zoom-aware feature density filter.
    #
    # Read min_feature_pixel_area_by_zoom from the first resolved collection (it comes
    # from TilesConfig which is catalog-scoped; using the first entry is correct for
    # single-collection tiles, and a reasonable fallback for multi-collection tiles
    # that share a catalog).  The lookup mirrors the simplification bracket logic:
    # find the highest zoom key ≤ the current zoom level.
    #
    # Safety: points and lines (projected area = 0) always pass because the predicate is
    #   NOT (ST_Area(geom) > 0 AND ST_Area(geom) < :min_pixel_area)
    # which is True when area = 0 (first conjunct is False).
    # NULL geoms (ST_AsMVTGeom returns NULL for out-of-tile features) are also filtered
    # because ST_Area(NULL) IS NULL, making the NOT(…) expression evaluate to NULL,
    # which SQL treats as FALSE in a WHERE clause — consistent with the existing
    # spatial-intersects pre-filter in the per-collection subqueries.
    area_where = ""
    min_pixel_area: Optional[float] = None
    if resolved_collections:
        density_map: Dict[int, float] = (
            resolved_collections[0].get("min_feature_pixel_area_by_zoom") or {}
        )
        if density_map:
            try:
                z_int = int(z)
                for zoom_key, area_threshold in sorted(
                    density_map.items(), reverse=True
                ):
                    if z_int >= zoom_key:
                        min_pixel_area = area_threshold
                        break
            except ValueError:
                pass

    if min_pixel_area and min_pixel_area > 0:
        # Exclude sub-pixel polygon features; points/lines (area=0) always pass.
        area_where = (
            " WHERE NOT (ST_Area(mvtgeom.geom) > 0"
            " AND ST_Area(mvtgeom.geom) < :min_pixel_area)"
        )
        all_bind_params["min_pixel_area"] = min_pixel_area
        logger.debug(
            "density filter active: z=%s min_pixel_area=%.4f", z, min_pixel_area
        )

    # 5. Final SQL Execution
    full_query = f"""
        WITH
        mvtgeom AS ({" UNION ALL ".join(union_queries)})
        SELECT ST_AsMVT(mvtgeom.*, 'default', {extent}, 'geom')
        FROM mvtgeom{area_where};
    """

    logger.info(
        f"Executing MVT query. Bind params types: { {k: type(v) for k, v in all_bind_params.items()} }"
    )
    logger.info(f"target_srid value: {all_bind_params.get('target_srid')}")

    return await DQLQuery(
        full_query, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
    ).execute(conn, **all_bind_params)
