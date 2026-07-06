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

"""OGC API - Moving Features Part 1 extension for DynaStore.

Provides CRUD for moving features and their temporal geometry sequences,
scoped to (catalog_id, collection_id) pairs.

Conforms to OGC API - Moving Features Part 1 (approved Feb 2026).
"""

import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, FrozenSet, List, Optional

from fastapi import APIRouter, Body, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from dynastore.extensions import protocols
from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.web.decorators import expose_web_page
from dynastore.extensions.tools.db import get_async_connection, get_async_engine
from dynastore.extensions.tools.query import parse_hints_param
from dynastore.models.protocols import MovingFeaturesProtocol
from dynastore.modules.catalog import catalog_module
from dynastore.modules.moving_features import db as mf_db
from dynastore.modules.moving_features.db import delete_temporal_geometries_by_mf
from dynastore.modules.moving_features.models import (
    MovingFeature,
    MovingFeatureCreate,
    MovingFeatureList,
    MovingFeatureUpdate,
    TemporalGeometry,
    TemporalGeometryCreate,
    TemporalGeometryUpdate,
)
from dynastore.tools.db import validate_sql_identifier

logger = logging.getLogger(__name__)


OGC_API_MOVING_FEATURES_URIS = [
    "http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/mf-collection",
    "http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/tgsequence",
]


class MovingFeaturesService(protocols.ExtensionProtocol, OGCServiceMixin, MovingFeaturesProtocol):
    """OGC API - Moving Features Part 1 extension.

    Priority 100 — alongside STAC and Features. Uses Pattern B (instance
    router) so ``self`` is available in handlers and ``OGCServiceMixin``
    helpers work correctly.
    """

    priority: int = 100
    router: APIRouter

    conformance_uris = OGC_API_MOVING_FEATURES_URIS
    prefix = "/movingfeatures"
    protocol_title = "DynaStore OGC API - Moving Features"
    protocol_description = "Temporal tracking of moving objects via OGC API - Moving Features"

    # StaticPageMixin (folded into OGCServiceMixin) class attributes
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    static_prefix = "movingfeatures"

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self.router = APIRouter(prefix="/movingfeatures", tags=["OGC API - Moving Features"])
        self._register_routes()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        logger.info("MovingFeaturesService: started.")
        yield
        logger.info("MovingFeaturesService: stopped.")

    # ------------------------------------------------------------------
    # Route registration (Pattern B)
    # ------------------------------------------------------------------

    def _register_routes(self) -> None:
        col = "/catalogs/{catalog_id}/collections/{collection_id}"

        self.register_ogc_standard_routes()
        route_table: list[tuple[str, str, list[str], dict[str, Any]]] = [
            (
                "/catalogs",
                "list_catalogs",
                ["GET"],
                {"summary": "List catalogs available to the Moving Features service"},
            ),
            (
                "/catalogs/{catalog_id}/collections",
                "list_collections",
                ["GET"],
                {"summary": "List moving-feature collections in a catalog"},
            ),
            (
                col,
                "get_collection",
                ["GET"],
                {"summary": "Get moving-feature collection metadata"},
            ),
            (
                col + "/items",
                "list_moving_features",
                ["GET"],
                {
                    "response_model": MovingFeatureList,
                    "summary": "List moving features in a collection",
                },
            ),
            (
                col + "/items",
                "create_moving_feature",
                ["POST"],
                {
                    "response_model": MovingFeature,
                    "status_code": status.HTTP_201_CREATED,
                    "summary": "Create a moving feature",
                },
            ),
            (
                col + "/items/{mf_id}",
                "get_moving_feature",
                ["GET"],
                {"response_model": MovingFeature, "summary": "Get a moving feature"},
            ),
            (
                col + "/items/{mf_id}",
                "delete_moving_feature",
                ["DELETE"],
                {
                    "status_code": status.HTTP_204_NO_CONTENT,
                    "summary": "Delete a moving feature and its temporal data",
                },
            ),
            (
                col + "/items/{mf_id}",
                "update_moving_feature",
                ["PUT"],
                {
                    "response_model": MovingFeature,
                    "summary": "Update a moving feature's properties",
                },
            ),
            (
                col + "/items/{mf_id}/tgsequence",
                "list_tg_sequence",
                ["GET"],
                {
                    "response_model": List[TemporalGeometry],
                    "summary": "Get temporal geometry sequences for a moving feature",
                },
            ),
            (
                col + "/items/{mf_id}/tgsequence",
                "add_tg_sequence",
                ["POST"],
                {
                    "response_model": TemporalGeometry,
                    "status_code": status.HTTP_201_CREATED,
                    "summary": "Add a temporal geometry sequence to a moving feature",
                },
            ),
            (
                col + "/items/{mf_id}/tgsequence/{tg_id}",
                "update_tg_sequence",
                ["PATCH"],
                {
                    "response_model": TemporalGeometry,
                    "summary": "Update a temporal geometry sequence",
                },
            ),
        ]
        for path, handler_name, methods, kwargs in route_table:
            self.router.add_api_route(path, getattr(self, handler_name), methods=methods, **kwargs)

    # Standard OGC endpoints (landing/conformance delegated to OGCServiceMixin
    # via register_ogc_standard_routes; see _register_routes).

    async def list_catalogs(
        self,
        limit: Optional[int] = Query(
            None,
            ge=1,
            description=(
                "Maximum number of catalogs to return. Omitted falls back to "
                "the configured default; a value above the configured "
                "maximum is clamped, not rejected (fc-limit-response-1)."
            ),
        ),
        offset: int = Query(0, ge=0),
    ) -> JSONResponse:
        from dynastore.extensions.moving_features.config import MovingFeaturesPluginConfig
        from dynastore.extensions.tools.pagination import resolve_page_limit

        mf_config = await self._get_plugin_config(MovingFeaturesPluginConfig)
        limit = resolve_page_limit(
            limit, default_limit=mf_config.default_limit, max_limit=mf_config.max_limit,
        )

        catalogs_svc = await self._get_catalogs_service()
        catalogs = await catalogs_svc.list_catalogs(limit=limit, offset=offset)
        return JSONResponse(
            content={
                "catalogs": [
                    {"id": c.id, "title": getattr(c, "title", None)}
                    for c in (catalogs or [])
                ]
            }
        )

    async def _resolve_internal_catalog_id(self, external_catalog_id: str) -> str:
        """Resolve the public external catalog id to the immutable internal id.

        All DB operations and partition keys use the internal id so that a
        catalog rename (external id change) never orphans existing rows.
        Raises 404 when the catalog does not exist.
        """
        catalogs_svc = await self._get_catalogs_service()
        internal_id = await catalogs_svc.resolve_catalog_id(external_catalog_id)
        if not internal_id:
            raise HTTPException(
                status_code=404,
                detail=f"Catalog '{external_catalog_id}' not found.",
            )
        return internal_id

    # ------------------------------------------------------------------
    # Collection endpoints (delegate to DynaStore catalog)
    # ------------------------------------------------------------------

    async def list_collections(
        self,
        catalog_id: str,
        limit: Optional[int] = Query(
            None,
            ge=1,
            description=(
                "Maximum number of collections to return. Omitted falls back "
                "to the configured default; a value above the configured "
                "maximum is clamped, not rejected (fc-limit-response-1)."
            ),
        ),
        offset: int = Query(0, ge=0),
    ) -> JSONResponse:
        validate_sql_identifier(catalog_id)

        from dynastore.extensions.moving_features.config import MovingFeaturesPluginConfig
        from dynastore.extensions.tools.pagination import resolve_page_limit

        mf_config = await self._get_plugin_config(MovingFeaturesPluginConfig, catalog_id)
        limit = resolve_page_limit(
            limit, default_limit=mf_config.default_limit, max_limit=mf_config.max_limit,
        )

        catalogs_svc = await self._get_catalogs_service()
        collections = await catalogs_svc.list_collections(
            catalog_id, limit=limit, offset=offset
        )
        return JSONResponse(
            content={
                "collections": [
                    {"id": c.id, "title": getattr(c, "title", None)}
                    for c in (collections or [])
                ]
            }
        )

    async def get_collection(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> JSONResponse:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        collection = await catalog_module.get_collection(catalog_id, collection_id)
        if not collection:
            raise HTTPException(status_code=404, detail="Collection not found.")
        col_dict = collection if isinstance(collection, dict) else collection.model_dump()
        return JSONResponse(content=col_dict)

    # ------------------------------------------------------------------
    # Moving feature CRUD
    # ------------------------------------------------------------------

    async def list_moving_features(
        self,
        catalog_id: str,
        collection_id: str,
        conn: AsyncConnection = Depends(get_async_connection),
        limit: Optional[int] = Query(
            None,
            ge=1,
            description=(
                "Maximum number of moving features to return. Omitted falls "
                "back to the configured default; a value above the "
                "configured maximum is clamped, not rejected "
                "(fc-limit-response-1)."
            ),
        ),
        offset: int = Query(0, ge=0),
        bbox: Optional[str] = Query(
            None,
            description="Bounding box filter. Comma-separated: minx,miny,maxx,maxy (WGS84).",
        ),
        intersects: Optional[str] = Query(
            None,
            description="Geometry filter (WKT format, WGS84). Filters trajectories intersecting this geometry.",
        ),
        request_hints: FrozenSet = Depends(parse_hints_param),
    ) -> MovingFeatureList:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        if not await catalog_module.get_collection(catalog_id, collection_id):
            raise HTTPException(status_code=404, detail="Collection not found.")
        internal_id = await self._resolve_internal_catalog_id(catalog_id)

        from dynastore.extensions.moving_features.config import MovingFeaturesPluginConfig
        from dynastore.extensions.tools.pagination import resolve_page_limit

        mf_config = await self._get_plugin_config(
            MovingFeaturesPluginConfig, catalog_id, collection_id,
        )
        limit = resolve_page_limit(
            limit, default_limit=mf_config.default_limit, max_limit=mf_config.max_limit,
        )

        if bbox and intersects:
            raise HTTPException(
                status_code=400,
                detail="Only one of 'bbox' or 'intersects' parameters can be specified.",
            )

        if bbox:
            from dynastore.tools.geospatial import parse_bbox_string, BboxDimensionality
            try:
                bbox_coords = parse_bbox_string(
                    bbox,
                    dimensionality=BboxDimensionality.STRICT_2D,
                    allow_none=False,
                )
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e)) from e

            results, total = await mf_db.list_moving_features_by_bbox(
                conn,
                internal_id,
                collection_id,
                min_lon=bbox_coords[0],
                min_lat=bbox_coords[1],
                max_lon=bbox_coords[2],
                max_lat=bbox_coords[3],
                limit=limit,
                offset=offset,
            )
            features = [f.model_copy(update={"catalog_id": catalog_id}) for f in results]
            return MovingFeatureList(
                features=features, numberMatched=total, numberReturned=len(features)
            )

        if intersects:
            try:
                import shapely.wkt as wkt
                geom = wkt.loads(intersects)
                if not geom.is_valid:
                    raise HTTPException(
                        status_code=400,
                        detail="Invalid geometry: geometry is not valid.",
                    )
            except Exception as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid geometry WKT: {str(e)}",
                ) from e

            results, total = await mf_db.list_moving_features_by_geometry(
                conn,
                internal_id,
                collection_id,
                geometry_wkt=intersects,
                limit=limit,
                offset=offset,
            )
            features = [f.model_copy(update={"catalog_id": catalog_id}) for f in results]
            return MovingFeatureList(
                features=features, numberMatched=total, numberReturned=len(features)
            )

        results, total = await mf_db.list_moving_features(conn, internal_id, collection_id, limit, offset)
        features = [f.model_copy(update={"catalog_id": catalog_id}) for f in results]
        return MovingFeatureList(
            features=features, numberMatched=total, numberReturned=len(features)
        )

    async def create_moving_feature(
        self,
        catalog_id: str,
        collection_id: str,
        mf: MovingFeatureCreate = Body(...),
        conn: AsyncConnection = Depends(get_async_connection),
        engine=Depends(get_async_engine),
    ) -> MovingFeature:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        await self._require_catalog_ready(catalog_id)
        if not await catalog_module.get_collection(catalog_id, collection_id):
            raise HTTPException(status_code=404, detail="Collection not found.")
        internal_id = await self._resolve_internal_catalog_id(catalog_id)

        from dynastore.modules.db_config.partition_tools import (
            ensure_partitions_off_request_connection,
        )

        try:
            # Provisioned on a dedicated connection (not `conn`, the
            # request-scoped one) so the ACCESS EXCLUSIVE lock these
            # CREATE TABLE ... PARTITION OF statements take on the shared
            # moving_features.* parent tables is released as soon as this
            # call returns, instead of being held for the rest of the
            # request's transaction (see #2749, #2831).
            await ensure_partitions_off_request_connection(
                engine,
                partitions=[
                    dict(table_name="moving_features", schema="moving_features", strategy="LIST", partition_value=internal_id),
                    dict(table_name="temporal_geometries", schema="moving_features", strategy="LIST", partition_value=internal_id),
                ],
            )
        except Exception as exc:
            logger.error(
                "Failed to ensure partition for catalog '%s': %s", catalog_id, exc, exc_info=True
            )
            raise HTTPException(
                status_code=500,
                detail=f"Could not prepare database for catalog '{catalog_id}'.",
            ) from exc

        created = await mf_db.create_moving_feature(conn, internal_id, collection_id, mf)
        if not created:
            raise HTTPException(status_code=500, detail="Failed to create moving feature.")
        return created.model_copy(update={"catalog_id": catalog_id})

    async def get_moving_feature(
        self,
        catalog_id: str,
        collection_id: str,
        mf_id: uuid.UUID,
        conn: AsyncConnection = Depends(get_async_connection),
        # Accepted for uniform protocol consistency; moving-features reads go through
        # a dedicated SQL path that does not yet implement the hints routing layer.
        request_hints: FrozenSet = Depends(parse_hints_param),
    ) -> MovingFeature:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        internal_id = await self._resolve_internal_catalog_id(catalog_id)
        feature = await mf_db.get_moving_feature(conn, internal_id, mf_id)
        if not feature:
            raise HTTPException(status_code=404, detail="Moving feature not found.")
        if feature.collection_id != collection_id:
            raise HTTPException(status_code=404, detail="Moving feature not found.")
        return feature.model_copy(update={"catalog_id": catalog_id})

    async def delete_moving_feature(
        self,
        catalog_id: str,
        collection_id: str,
        mf_id: uuid.UUID,
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> None:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        await self._require_catalog_ready(catalog_id)
        internal_id = await self._resolve_internal_catalog_id(catalog_id)
        feature = await mf_db.get_moving_feature(conn, internal_id, mf_id)
        if not feature or feature.collection_id != collection_id:
            raise HTTPException(status_code=404, detail="Moving feature not found.")
        await delete_temporal_geometries_by_mf(conn, internal_id, mf_id)
        await mf_db.delete_moving_feature(conn, internal_id, mf_id)

    async def update_moving_feature(
        self,
        catalog_id: str,
        collection_id: str,
        mf_id: uuid.UUID,
        mf_update: MovingFeatureUpdate = Body(...),
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> MovingFeature:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        await self._require_catalog_ready(catalog_id)
        internal_id = await self._resolve_internal_catalog_id(catalog_id)
        feature = await mf_db.get_moving_feature(conn, internal_id, mf_id)
        if not feature or feature.collection_id != collection_id:
            raise HTTPException(status_code=404, detail="Moving feature not found.")

        updated = await mf_db.update_moving_feature(conn, internal_id, mf_id, mf_update)
        if not updated:
            raise HTTPException(status_code=500, detail="Failed to update moving feature.")
        return updated.model_copy(update={"catalog_id": catalog_id})

    # ------------------------------------------------------------------
    # Temporal geometry sequence
    # ------------------------------------------------------------------

    async def list_tg_sequence(
        self,
        catalog_id: str,
        collection_id: str,
        mf_id: uuid.UUID,
        conn: AsyncConnection = Depends(get_async_connection),
        dt_start: Optional[datetime] = Query(None, description="Temporal filter start (ISO 8601)."),
        dt_end: Optional[datetime] = Query(None, description="Temporal filter end (ISO 8601)."),
        # Accepted for uniform protocol consistency; temporal-geometry reads go through
        # a dedicated SQL path that does not yet implement the hints routing layer.
        request_hints: FrozenSet = Depends(parse_hints_param),
    ) -> List[TemporalGeometry]:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        internal_id = await self._resolve_internal_catalog_id(catalog_id)
        feature = await mf_db.get_moving_feature(conn, internal_id, mf_id)
        if not feature or feature.collection_id != collection_id:
            raise HTTPException(status_code=404, detail="Moving feature not found.")
        results = await mf_db.list_temporal_geometries(conn, internal_id, mf_id, dt_start, dt_end)
        return [tg.model_copy(update={"catalog_id": catalog_id}) for tg in results]

    async def add_tg_sequence(
        self,
        catalog_id: str,
        collection_id: str,
        mf_id: uuid.UUID,
        tg: TemporalGeometryCreate = Body(...),
        conn: AsyncConnection = Depends(get_async_connection),
        engine=Depends(get_async_engine),
    ) -> TemporalGeometry:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        await self._require_catalog_ready(catalog_id)
        if len(tg.datetimes) != len(tg.coordinates):
            raise HTTPException(
                status_code=400,
                detail=f"datetimes length ({len(tg.datetimes)}) must match coordinates length ({len(tg.coordinates)}).",
            )
        internal_id = await self._resolve_internal_catalog_id(catalog_id)
        feature = await mf_db.get_moving_feature(conn, internal_id, mf_id)
        if not feature or feature.collection_id != collection_id:
            raise HTTPException(status_code=404, detail="Moving feature not found.")

        from dynastore.modules.db_config.partition_tools import (
            ensure_partitions_off_request_connection,
        )

        try:
            # Provisioned on a dedicated connection (not `conn`, the
            # request-scoped one) so the ACCESS EXCLUSIVE lock this
            # CREATE TABLE ... PARTITION OF statement takes on the shared
            # moving_features.temporal_geometries parent table is released
            # as soon as this call returns, instead of being held for the
            # rest of the request's transaction (see #2749, #2831).
            await ensure_partitions_off_request_connection(
                engine,
                partitions=[
                    dict(table_name="temporal_geometries", schema="moving_features", strategy="LIST", partition_value=internal_id),
                ],
            )
        except Exception as exc:
            logger.error(
                "Failed to ensure partition for catalog '%s': %s", catalog_id, exc, exc_info=True
            )
            raise HTTPException(
                status_code=500,
                detail=f"Could not prepare database for catalog '{catalog_id}'.",
            ) from exc

        created = await mf_db.create_temporal_geometry(conn, internal_id, mf_id, tg)
        if not created:
            raise HTTPException(status_code=500, detail="Failed to create temporal geometry sequence.")
        return created.model_copy(update={"catalog_id": catalog_id})

    async def update_tg_sequence(
        self,
        catalog_id: str,
        collection_id: str,
        mf_id: uuid.UUID,
        tg_id: uuid.UUID,
        tg_update: TemporalGeometryUpdate = Body(...),
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> TemporalGeometry:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        await self._require_catalog_ready(catalog_id)
        internal_id = await self._resolve_internal_catalog_id(catalog_id)

        # Verify the moving feature exists and belongs to the collection
        feature = await mf_db.get_moving_feature(conn, internal_id, mf_id)
        if not feature or feature.collection_id != collection_id:
            raise HTTPException(status_code=404, detail="Moving feature not found.")

        # Verify the temporal geometry exists and belongs to the moving feature
        tg = await mf_db.get_temporal_geometry(conn, internal_id, tg_id)
        if not tg or tg.mf_id != mf_id:
            raise HTTPException(status_code=404, detail="Temporal geometry sequence not found.")

        # Validate datetimes and coordinates length match if both provided
        if tg_update.datetimes is not None and tg_update.coordinates is not None:
            if len(tg_update.datetimes) != len(tg_update.coordinates):
                raise HTTPException(
                    status_code=400,
                    detail=f"datetimes length ({len(tg_update.datetimes)}) must match coordinates length ({len(tg_update.coordinates)}).",
                )

        updated = await mf_db.update_temporal_geometry(conn, internal_id, tg_id, tg_update)
        if not updated:
            raise HTTPException(status_code=500, detail="Failed to update temporal geometry sequence.")
        return updated.model_copy(update={"catalog_id": catalog_id})

    # get_web_pages / get_static_assets / get_notebooks / provide_static_files /
    # _serve_page_template are provided by OGCServiceMixin (static_dir /
    # static_prefix above opt this service into the default wiring).

    @expose_web_page(
        page_id="movingfeatures_browser",
        title="Moving Features Browser",
        icon="fa-route",
        description="Browse moving features and their trajectories.",
    )
    async def provide_movingfeatures_browser(self, request: Request):
        return await self._serve_page_template("movingfeatures_browser.html")
