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

import pyproj as _pyproj_scope_gate  # noqa: F401  # SCOPE gate: extension_dwh requires pyproj (via tms_definitions)
_ = _pyproj_scope_gate  # silence pyright "unused" — load-bearing for SCOPE filtering

from dynastore.tools.discovery import get_protocol
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional as _Optional
from dynastore.tools.cache import cached
from dynastore.tools.enrichment import enrich_features
from dynastore.modules.tools.item_stream import resolve_join_value, stream_normalized_items  # noqa: E402

from fastapi import (
    FastAPI,
    HTTPException,
    status,
    Depends,
    Response,
    Request,
    APIRouter,
)

from dynastore.extensions.dwh.models import (
    DWHJoinRequest,
    DWHJoinRequestBase,
    DWHTiledJoinRequest,
)
from dynastore.extensions.tools.formatters import format_response
from dynastore.models.protocols import (
    BigQueryProtocol,
    CatalogsProtocol,
    ItemsProtocol,
)
from dynastore.models.driver_context import DriverContext
from dynastore.modules.storage.hints import Hint, EXACT_READ_HINTS  # noqa: E402  # after pyproj SCOPE-gate stmt
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
)
from dynastore.tools.db import validate_sql_identifier
from dynastore.extensions.tools.db import get_async_connection
from sqlalchemy.ext.asyncio import AsyncConnection
from shapely.geometry import box
from shapely import wkb
import hashlib
import json
import dynastore.modules.tiles.tiles_module as tms_manager
from dynastore.modules.tiles.tiles_module import TileStorageProtocol
from dynastore.modules.tiles.tms_definitions import BUILTIN_TILE_MATRIX_SETS

# --- App Setup & Configuration ---
logger = logging.getLogger(__name__)


@cached(maxsize=128, namespace="dwh")
async def execute_bigquery_async(
    query: str, join_column: str, project_id: str
) -> Dict[Any, Dict[str, Any]]:
    """Execute a BigQuery query via BigQueryProtocol and index results by join column.

    Cached to optimize repeated lookups for the same query.
    """
    if not project_id:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="GOOGLE_CLOUD_PROJECT is not configured.",
        )

    bq = get_protocol(BigQueryProtocol)
    if not bq:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="BigQuery service not available (GCP module not loaded).",
        )

    try:
        logger.info("Executing BQ Query (cache miss): %s...", query[:200])
        records = await bq.execute_query(query, project_id)

        if not records:
            return {}

        if join_column not in records[0]:
            raise HTTPException(
                status_code=400,
                detail=f"Join column '{join_column}' not found in BigQuery results.",
            )

        return {row[join_column]: row for row in records}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("BigQuery query failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"BigQuery query error: {str(e)}") from e


from dynastore.models.query_builder import QueryRequest, FieldSelection
from dynastore.modules.tools.field_categories import resolve_category_field_names  # noqa: E402


from dynastore.extensions.protocols import ExtensionProtocol
class DwhService(ExtensionProtocol):
    priority: int = 100

    def __init__(self):
        self.router = APIRouter(prefix="/dwh", tags=["Data Warehouse API"])
        self._register_routes()

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        from dynastore.tools.discovery import register_plugin
        from .link_contrib import DwhLinkContributor
        from .query_transform import DWHJoinQueryTransform
        register_plugin(DWHJoinQueryTransform())
        register_plugin(DwhLinkContributor())
        yield

    async def _dwh_join_impl(
        self,
        request: Request,
        req: DWHJoinRequestBase,
        catalog_id: str,
        conn: AsyncConnection,
    ):
        """
        Shared implementation for DWH join logic.
        """
        try:
            validate_sql_identifier(catalog_id)
            validate_sql_identifier(req.collection)
            validate_sql_identifier(req.join_column)
        except (ValueError, TypeError) as e:
            raise HTTPException(
                status_code=400, detail=f"Invalid identifier in request: {e}"
            ) from e

        # 1. Resolve Catalog Provider
        catalogs_provider = get_protocol(CatalogsProtocol)
        if not catalogs_provider:
            raise HTTPException(
                status_code=500, detail="CatalogsProtocol not available."
            )

        # 2. Fetch BQ Data (Cached)
        join_values = await execute_bigquery_async(
            query=req.dwh_query,
            join_column=req.dwh_join_column,
            project_id=req.dwh_project_id,
        )
        if not join_values:
            logger.info(
                "DWH query returned no join values. Returning empty feature set."
            )
            # Return empty response immediately
            return format_response(
                request=request,
                features=[],  # Empty list
                output_format=req.output_format,
                collection_id=req.collection,
                target_srid=req.destination_crs,
                encoding=req.output_encoding,
            )

        # 3. Stream Features from ItemsService
        items_svc = get_protocol(ItemsProtocol)
        if not items_svc:
            raise HTTPException(status_code=500, detail="ItemsProtocol not available.")

        # Build QueryRequest
        selects = []
        if req.with_geometry:
            if req.destination_crs:
                selects.append(
                    FieldSelection(
                        field="geom",
                        transformation="ST_Transform",
                        transform_args={"srid": req.destination_crs},
                    )
                )
            else:
                selects.append(FieldSelection(field="geom"))

        # Resolve field names from three storage-aware categories, then project
        # each as a plain FieldSelection. join_column is guaranteed to be included
        # in its declared section (join_source) so the PG SELECT fetches it and
        # resolve_join_value can find it there. See #1827.
        join_source = getattr(req, "join_source", "properties")
        _jc_props = req.join_column if join_source == "properties" else None
        _jc_stats = req.join_column if join_source == "stats" else None
        _jc_system_list: _Optional[list] = (
            [req.join_column] if join_source == "system" else None
        )
        # Merge the join column into the explicit system list when join_source="system"
        # and the caller already passed a system list, to avoid cross-category errors.
        _req_system = getattr(req, "system", None)
        if join_source == "system" and _req_system is not None and req.join_column not in _req_system:
            _req_system = list(_req_system) + [req.join_column]
        elif join_source == "system":
            _req_system = _jc_system_list
        else:
            _req_system = getattr(req, "system", None)
        try:
            field_names = await resolve_category_field_names(
                catalog_id,
                req.collection,
                properties=req.properties,
                stats=getattr(req, "stats", None),
                system=_req_system,
                join_column=_jc_props if join_source == "properties" else None,
                db_resource=conn,
            )
            # When join_source is stats, ensure the join column is selected.
            if join_source == "stats" and req.join_column not in field_names:
                # Append via stats category (stats join_column is unusual but user-requested).
                field_names.append(req.join_column)
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e)) from e

        for name in field_names:
            selects.append(FieldSelection(field=name))

        # Reject user-supplied raw SQL to prevent SQL injection.
        # Use the structured `filters` mechanism (FilterCondition) instead.
        if req.where:
            raise HTTPException(
                status_code=400,
                detail=(
                    "The 'where' field is disabled for security reasons. "
                    "Use structured FilterCondition filters instead."
                ),
            )

        query_req = QueryRequest(
            select=selects, limit=req.limit, offset=req.offset
        )

        # PG row-level ABAC: the DWH join is a user-facing HTTP route, so the
        # caller's per-row read scope MUST be enforced — joining external DWH
        # data does not exempt the source collection from its access_envelope.
        # Compile and inject the principal's read filter when the collection
        # carries the sidecar; non-ABAC collections leave access_filter unset
        # (the optimiser does not force-include the sidecar for them).
        from dynastore.modules.storage.access_scope import (
            collection_uses_pg_access_envelope,
            compile_read_access_filter,
            principals_from_request_state,
        )

        if await collection_uses_pg_access_envelope(catalog_id, req.collection):
            principals, principal = principals_from_request_state(request)
            query_req.access_filter = await compile_read_access_filter(
                catalog_id=catalog_id,
                collections=[req.collection],
                principals=principals,
                principal=principal,
            )

        # Stream via ItemService. Force the full-precision PG read path with
        # Hint.JOIN: the public default routing puts Elasticsearch first for
        # READ, but ES only carries simplified geometry and cannot run the
        # ST_Transform projection this join builds into the request — only the
        # PG driver advertises Hint.JOIN, so resolution selects it (and, being
        # the read-primary fallback, hands the read to the inline PG SQL path).
        # The access_filter set above rides along on query_req into the
        # optimiser, so the forced PG path stays per-row ABAC-enforced.
        # stream_normalized_items lifts PG model_extra columns into
        # feature.properties so enrich_features can join on them uniformly.
        normalized_stream = stream_normalized_items(
            items_svc, catalog_id, req.collection, query_req,
            ctx=DriverContext(db_resource=conn),
            hints=frozenset({Hint.JOIN}),
        )

        # Enrich streamed features with DWH data (O(1) dict lookup per feature).
        # join_source routes the key lookup to the declared section (#1827).
        enriched_stream = enrich_features(
            normalized_stream,
            join_values,
            join_column=req.join_column,
            join_source=getattr(req, "join_source", "properties"),
        )

        return format_response(
            request=request,
            features=enriched_stream,
            output_format=req.output_format,
            collection_id=req.collection,
            target_srid=req.destination_crs,
            encoding=req.output_encoding,
        )

    def _register_routes(self):
        self.router.add_api_route(
            "/join", self.dwh_join, methods=["POST"], response_class=Response
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/join",
            self.dwh_catalog_join,
            methods=["POST"],
            response_class=Response,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/tiles/{z}/{x}/{y}/join.{format}",
            self.dwh_tiled_join,
            methods=["POST"],
            response_class=Response,
        )

    async def dwh_join(
        self,
        request: Request,
        req: DWHJoinRequest,
        conn: AsyncConnection = Depends(get_async_connection),
    ):
        """
        Legacy endpoint: Retrieves features and streams the joined response.
        """
        return await self._dwh_join_impl(request, req, req.catalog, conn)

    async def dwh_catalog_join(
        self,
        request: Request,
        catalog_id: str,
        base_req: DWHJoinRequestBase,
        conn: AsyncConnection = Depends(get_async_connection),
    ):
        """
        New endpoint: Retrieves features from a specific catalog and streams the joined response.
        """
        return await self._dwh_join_impl(request, base_req, catalog_id, conn)

    async def dwh_tiled_join(
        self,
        request: Request,
        catalog_id: str,
        z: int,
        x: int,
        y: int,
        format: str,
        req: DWHTiledJoinRequest,
        conn: AsyncConnection = Depends(get_async_connection),
    ):
        """
        Tiled DWH join endpoint: Returns MVT tiles with joined DWH data.
        Cache is stored separately from regular tiles using a DWH-specific cache ID.
        """
        try:
            validate_sql_identifier(catalog_id)
            validate_sql_identifier(req.collection)
            validate_sql_identifier(req.join_column)
        except (ValueError, TypeError) as e:
            raise HTTPException(
                status_code=400, detail=f"Invalid identifier in request: {e}"
            ) from e

        if format not in ["mvt", "pbf"]:
            raise HTTPException(
                status_code=400, detail=f"Format '{format}' not supported."
            )

        # 1. Check cache if enabled (stored separately from regular tiles)
        provider = None
        cache_id = None
        if req.tiles.enable_cache:
            query_hash = hashlib.sha256(
                f"{req.dwh_query}:{req.dwh_join_column}".encode()
            ).hexdigest()[:12]
            cache_id = f"{req.collection}@dwh-{query_hash}"

            provider = get_protocol(TileStorageProtocol)

            if provider:
                try:
                    tile = await provider.get_tile(
                        catalog_id, cache_id, req.tiles.tileMatrixSetId, z, x, y, format
                    )
                    if tile:
                        logger.info(
                            f"DWH tile cache HIT: {catalog_id}/{cache_id}/{z}/{x}/{y}"
                        )
                        return Response(
                            content=tile,
                            media_type="application/vnd.mapbox-vector-tile",
                        )
                except Exception as e:
                    logger.warning(f"DWH tile cache lookup failed: {e}")

        # 2. Fetch BQ Data (Cached)
        join_values = await execute_bigquery_async(
            query=req.dwh_query,
            join_column=req.dwh_join_column,
            project_id=req.dwh_project_id,
        )
        if not join_values:
            logger.info("DWH query returned no join values. Returning empty tile.")
            return Response(status_code=204)

        # 3. Validate TMS and get tile envelope
        tms_def = await tms_manager.get_custom_tms(
            catalog_id=catalog_id, tms_id=req.tiles.tileMatrixSetId
        )
        if not tms_def:
            tms_def = BUILTIN_TILE_MATRIX_SETS.get(req.tiles.tileMatrixSetId)
            if not tms_def:
                raise HTTPException(
                    status_code=404,
                    detail=f"TMS '{req.tiles.tileMatrixSetId}' not supported.",
                )

        matrix = next((m for m in tms_def.tileMatrices if m.id == str(z)), None)
        if not matrix:
            raise HTTPException(
                status_code=400,
                detail=f"Zoom {z} not defined in TMS {req.tiles.tileMatrixSetId}.",
            )

        if not (0 <= x < matrix.matrixWidth and 0 <= y < matrix.matrixHeight):
            raise HTTPException(
                status_code=400,
                detail=f"Tile out of bounds for TMS {req.tiles.tileMatrixSetId} @ {z}.",
            )

        # 4. Calculate tile envelope
        origin = matrix.pointOfOrigin
        tile_width = matrix.tileWidth
        cell_size = matrix.cellSize
        tile_span_x = tile_width * cell_size
        tile_span_y = matrix.tileHeight * cell_size

        min_x = origin[0] + (x * tile_span_x)
        max_x = min_x + tile_span_x
        max_y = origin[1] - (y * tile_span_y)
        min_y = max_y - tile_span_y

        bbox = box(min_x, min_y, max_x, max_y)
        tile_wkb = wkb.dumps(bbox)

        # 5. Resolve SRID
        try:
            target_srid = await tms_manager.resolve_srid(
                conn=conn, crs_str=tms_def.crs, catalog_id=catalog_id
            )
            if not target_srid:
                raise ValueError("Failed to resolve SRID.")
        except Exception as e:
            logger.error(f"SRID resolution failed: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Could not process CRS for TMS '{req.tiles.tileMatrixSetId}'.",
            ) from e

        # 6. Get source SRID for collection
        from dynastore.modules.tiles import tiles_module

        meta = await tiles_module.get_tile_resolution_params(catalog_id, req.collection)
        if not meta:
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{req.collection}' not configured for tiles.",
            )

        source_srid = meta["source_srid"]
        col_config = meta.get("col_config")
        phys_schema = meta.get("phys_schema")
        phys_table = meta.get("phys_table")

        # 7. Stream Features (No Geometry)
        items_svc = get_protocol(ItemsProtocol)
        if not items_svc:
            raise HTTPException(status_code=500, detail="ItemsProtocol not available.")

        # Spatial Filter
        spatial_filter = "ST_Intersects(geom, ST_Transform(ST_SetSRID(ST_GeomFromWKB(:tile_wkb), CAST(:target_srid AS INTEGER)), CAST(:srid AS INTEGER)))"

        query_req = QueryRequest(
            select=[
                FieldSelection(field="geoid", alias="id"),
                FieldSelection(field=req.join_column),
                FieldSelection(field="attributes"),
            ],
            raw_where=spatial_filter,
            raw_params={
                "tile_wkb": tile_wkb,
                "target_srid": target_srid,
                "srid": source_srid,
                "join_keys": list(join_values.keys()),
            },
        )
        # Also filter by join keys to reduce data transfer if DWH result is small subset?
        # If DWH result is smaller than tile features, filtering by join keys is good.
        # If DWH result is larger, filtering by tile is better.
        # We do both: tile filter AND join keys filter (if feasible).
        # query_req.raw_where += f' AND "{req.join_column}" = ANY(:join_keys)'
        # Using FieldSelection for join_column alias might be needed if it's different.

        # PG row-level ABAC: the tiled DWH join is a user-facing HTTP route and
        # the emitted MVT carries per-feature attributes, so an unfiltered read
        # would leak ABAC-protected row attributes into the vector tile. Compile
        # and inject the principal's read filter when the collection carries the
        # access_envelope sidecar; non-ABAC collections leave access_filter unset.
        from dynastore.modules.storage.access_scope import (
            collection_uses_pg_access_envelope,
            compile_read_access_filter,
            principals_from_request_state,
        )

        if await collection_uses_pg_access_envelope(catalog_id, req.collection):
            principals, principal = principals_from_request_state(request)
            query_req.access_filter = await compile_read_access_filter(
                catalog_id=catalog_id,
                collections=[req.collection],
                principals=principals,
                principal=principal,
            )

        # stream_normalized_items lifts PG model_extra columns into
        # feature.properties so the join key is visible uniformly.
        # EXACT_READ_HINTS: the tiled join emits MVT attributes from these
        # features and the downstream geometry query (geom_sql) fetches raw
        # WKB from PG — both require full-precision rows from the exact driver.
        normalized_stream = stream_normalized_items(
            items_svc, catalog_id, req.collection, query_req,
            ctx=DriverContext(db_resource=conn),
            hints=EXACT_READ_HINTS,
        )

        # 8. Join with DWH data and materialize for MVT query.
        # LEFT-join semantics (inner_join=False): features whose join key is
        # present but have no BigQuery match are kept (their props are passed
        # through unchanged). Features whose join key is absent or None are
        # skipped below with the explicit `continue` guard.
        # join_source routes the key lookup to the declared section (#1827).
        tiled_join_source = getattr(req, "join_source", "properties")
        enriched = enrich_features(
            normalized_stream, join_values,
            join_column=req.join_column, inner_join=False,
            join_source=tiled_join_source,
        )
        ids = []
        attributes_array = []

        async for feature in enriched:
            # Use resolve_join_value so the skip check respects join_source (#1827).
            if resolve_join_value(feature, req.join_column, tiled_join_source) is None:
                continue

            ids.append(feature.id)
            attributes_array.append(json.dumps(feature.properties or {}))

        if not ids:
            return Response(status_code=204)

        # Use QueryOptimizer to handle geometry resolution (sidecars, transforms)
        from dynastore.modules.catalog.query_optimizer import QueryOptimizer

        optimizer = QueryOptimizer(col_config)  # type: ignore[arg-type]
        geom_req = QueryRequest(
            select=[
                FieldSelection(
                    field="geom",
                    transformation="ST_Transform",
                    transform_args={"srid": int(target_srid)},
                )
            ],
            raw_where="h.geoid = ANY(CAST(:ids_filter AS UUID[]))",
            raw_params={"ids_filter": ids},
        )
        geom_sql, geom_params = optimizer.build_optimized_query(
            geom_req, schema=phys_schema, table=phys_table  # type: ignore[arg-type]
        )

        # Merge params
        mvt_params = {
            "tile_wkb": tile_wkb,
            "target_srid": target_srid,
            "attributes_array": attributes_array,
            "extent": req.tiles.extent,
            "buffer": req.tiles.buffer,
            "ids_input": ids,  # used for unnest
            **geom_params,
        }

        # Use unnest(arr1, arr2) for cleaner query and direct join
        mvt_query = f"""
            WITH 
            bounds AS (SELECT ST_SetSRID(ST_GeomFromWKB(:tile_wkb), :target_srid) AS geom),
            features_db AS ( {geom_sql} ),
            features_val AS (
                SELECT 
                    unnest(CAST(:ids_input AS uuid[])) AS id,
                    unnest(CAST(:attributes_array AS jsonb[])) AS attributes
            ),
            features_final AS (
                SELECT 
                    v.id,
                    v.attributes,
                    ST_AsMVTGeom(
                        d.geom, -- Already transformed by QueryOptimizer
                        bounds.geom,
                        :extent,
                        :buffer,
                        true
                    ) AS geom
                FROM features_val v
                JOIN features_db d ON d.geoid = v.id
                CROSS JOIN bounds
                WHERE d.geom IS NOT NULL
            )
            SELECT ST_AsMVT(features_final.*, 'default', :extent, 'geom', 'id')
            FROM features_final
        """

        mvt_content = await DQLQuery(
            mvt_query, result_handler=ResultHandler.SCALAR_ONE_OR_NONE
        ).execute(conn, **mvt_params)

        if not mvt_content:
            return Response(status_code=204)

        # 10. Cache the result if enabled
        if req.tiles.enable_cache and provider and cache_id:
            try:
                await provider.save_tile(
                    catalog_id,
                    cache_id,
                    req.tiles.tileMatrixSetId,
                    z,
                    x,
                    y,
                    mvt_content,
                    format,
                )
                logger.info(f"DWH tile cached: {catalog_id}/{cache_id}/{z}/{x}/{y}")
            except Exception as e:
                logger.warning(f"Failed to cache DWH tile: {e}")

        return Response(
            content=mvt_content, media_type="application/vnd.mapbox-vector-tile"
        )
