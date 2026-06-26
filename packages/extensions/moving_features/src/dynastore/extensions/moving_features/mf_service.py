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
from typing import FrozenSet, List, Optional

from fastapi import APIRouter, Body, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from dynastore.extensions import protocols
from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.tools.fast_api import AppJSONResponse as _AppJSONResponse
from dynastore.extensions.tools.language_utils import get_language
from dynastore.extensions.web.decorators import expose_web_page, expose_static
from dynastore.extensions.tools.db import get_async_connection
from dynastore.extensions.tools.query import parse_hints_param
from dynastore.models.protocols import MovingFeaturesProtocol
from dynastore.modules.catalog import catalog_module
from dynastore.modules.moving_features import db as mf_db
from dynastore.modules.moving_features.db import delete_temporal_geometries_by_mf
from dynastore.modules.moving_features.models import (
    MovingFeature,
    MovingFeatureCreate,
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
        self.router.add_api_route("/", self.get_landing_page, methods=["GET"])
        self.router.add_api_route("/conformance", self.get_conformance, methods=["GET"])
        self.router.add_api_route(
            "/catalogs",
            self.list_catalogs,
            methods=["GET"],
            summary="List catalogs available to the Moving Features service",
        )

        col = "/catalogs/{catalog_id}/collections/{collection_id}"

        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections",
            self.list_collections,
            methods=["GET"],
            summary="List moving-feature collections in a catalog",
        )
        self.router.add_api_route(
            col,
            self.get_collection,
            methods=["GET"],
            summary="Get moving-feature collection metadata",
        )
        self.router.add_api_route(
            col + "/items",
            self.list_moving_features,
            methods=["GET"],
            response_model=List[MovingFeature],
            summary="List moving features in a collection",
        )
        self.router.add_api_route(
            col + "/items",
            self.create_moving_feature,
            methods=["POST"],
            response_model=MovingFeature,
            status_code=status.HTTP_201_CREATED,
            summary="Create a moving feature",
        )
        self.router.add_api_route(
            col + "/items/{mf_id}",
            self.get_moving_feature,
            methods=["GET"],
            response_model=MovingFeature,
            summary="Get a moving feature",
        )
        self.router.add_api_route(
            col + "/items/{mf_id}",
            self.delete_moving_feature,
            methods=["DELETE"],
            status_code=status.HTTP_204_NO_CONTENT,
            summary="Delete a moving feature and its temporal data",
        )
        self.router.add_api_route(
            col + "/items/{mf_id}",
            self.update_moving_feature,
            methods=["PUT"],
            response_model=MovingFeature,
            summary="Update a moving feature's properties",
        )
        self.router.add_api_route(
            col + "/items/{mf_id}/tgsequence",
            self.list_tg_sequence,
            methods=["GET"],
            response_model=List[TemporalGeometry],
            summary="Get temporal geometry sequences for a moving feature",
        )
        self.router.add_api_route(
            col + "/items/{mf_id}/tgsequence",
            self.add_tg_sequence,
            methods=["POST"],
            response_model=TemporalGeometry,
            status_code=status.HTTP_201_CREATED,
            summary="Add a temporal geometry sequence to a moving feature",
        )
        self.router.add_api_route(
            col + "/items/{mf_id}/tgsequence/{tg_id}",
            self.update_tg_sequence,
            methods=["PATCH"],
            response_model=TemporalGeometry,
            summary="Update a temporal geometry sequence",
        )

    # ------------------------------------------------------------------
    # Standard OGC endpoints
    # ------------------------------------------------------------------

    async def get_landing_page(
        self, request: Request, language: str = Depends(get_language)
    ) -> _AppJSONResponse:
        return await self.ogc_landing_page_handler(request, language=language)

    async def get_conformance(self, request: Request):
        return await self.ogc_conformance_handler(request)

    async def list_catalogs(
        self,
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ) -> JSONResponse:
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

    # ------------------------------------------------------------------
    # Collection endpoints (delegate to DynaStore catalog)
    # ------------------------------------------------------------------

    async def list_collections(
        self,
        catalog_id: str,
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ) -> JSONResponse:
        validate_sql_identifier(catalog_id)
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
        limit: int = Query(100, ge=1, le=1000),
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
    ) -> List[MovingFeature]:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        if not await catalog_module.get_collection(catalog_id, collection_id):
            raise HTTPException(status_code=404, detail="Collection not found.")
        
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
            
            return await mf_db.list_moving_features_by_bbox(
                conn,
                catalog_id,
                collection_id,
                min_lon=bbox_coords[0],
                min_lat=bbox_coords[1],
                max_lon=bbox_coords[2],
                max_lat=bbox_coords[3],
                limit=limit,
                offset=offset,
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
            
            return await mf_db.list_moving_features_by_geometry(
                conn,
                catalog_id,
                collection_id,
                geometry_wkt=intersects,
                limit=limit,
                offset=offset,
            )
        
        return await mf_db.list_moving_features(conn, catalog_id, collection_id, limit, offset)

    async def create_moving_feature(
        self,
        catalog_id: str,
        collection_id: str,
        mf: MovingFeatureCreate = Body(...),
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> MovingFeature:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        await self._require_catalog_ready(catalog_id)
        if not await catalog_module.get_collection(catalog_id, collection_id):
            raise HTTPException(status_code=404, detail="Collection not found.")

        from dynastore.modules.db_config.partition_tools import ensure_partition_exists

        try:
            await ensure_partition_exists(
                conn,
                table_name="moving_features",
                schema="moving_features",
                strategy="LIST",
                partition_value=catalog_id,
            )
            await ensure_partition_exists(
                conn,
                table_name="temporal_geometries",
                schema="moving_features",
                strategy="LIST",
                partition_value=catalog_id,
            )
        except Exception as exc:
            logger.error(
                "Failed to ensure partition for catalog '%s': %s", catalog_id, exc, exc_info=True
            )
            raise HTTPException(
                status_code=500,
                detail=f"Could not prepare database for catalog '{catalog_id}'.",
            ) from exc

        created = await mf_db.create_moving_feature(conn, catalog_id, collection_id, mf)
        if not created:
            raise HTTPException(status_code=500, detail="Failed to create moving feature.")
        return created

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
        feature = await mf_db.get_moving_feature(conn, catalog_id, mf_id)
        if not feature:
            raise HTTPException(status_code=404, detail="Moving feature not found.")
        if feature.collection_id != collection_id:
            raise HTTPException(status_code=404, detail="Moving feature not found.")
        return feature

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
        feature = await mf_db.get_moving_feature(conn, catalog_id, mf_id)
        if not feature or feature.collection_id != collection_id:
            raise HTTPException(status_code=404, detail="Moving feature not found.")
        await delete_temporal_geometries_by_mf(conn, catalog_id, mf_id)
        await mf_db.delete_moving_feature(conn, catalog_id, mf_id)

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
        feature = await mf_db.get_moving_feature(conn, catalog_id, mf_id)
        if not feature or feature.collection_id != collection_id:
            raise HTTPException(status_code=404, detail="Moving feature not found.")
        
        updated = await mf_db.update_moving_feature(conn, catalog_id, mf_id, mf_update)
        if not updated:
            raise HTTPException(status_code=500, detail="Failed to update moving feature.")
        return updated

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
        feature = await mf_db.get_moving_feature(conn, catalog_id, mf_id)
        if not feature or feature.collection_id != collection_id:
            raise HTTPException(status_code=404, detail="Moving feature not found.")
        return await mf_db.list_temporal_geometries(conn, catalog_id, mf_id, dt_start, dt_end)

    async def add_tg_sequence(
        self,
        catalog_id: str,
        collection_id: str,
        mf_id: uuid.UUID,
        tg: TemporalGeometryCreate = Body(...),
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> TemporalGeometry:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        await self._require_catalog_ready(catalog_id)
        if len(tg.datetimes) != len(tg.coordinates):
            raise HTTPException(
                status_code=400,
                detail=f"datetimes length ({len(tg.datetimes)}) must match coordinates length ({len(tg.coordinates)}).",
            )
        feature = await mf_db.get_moving_feature(conn, catalog_id, mf_id)
        if not feature or feature.collection_id != collection_id:
            raise HTTPException(status_code=404, detail="Moving feature not found.")

        from dynastore.modules.db_config.partition_tools import ensure_partition_exists

        try:
            await ensure_partition_exists(
                conn,
                table_name="temporal_geometries",
                schema="moving_features",
                strategy="LIST",
                partition_value=catalog_id,
            )
        except Exception as exc:
            logger.error(
                "Failed to ensure partition for catalog '%s': %s", catalog_id, exc, exc_info=True
            )
            raise HTTPException(
                status_code=500,
                detail=f"Could not prepare database for catalog '{catalog_id}'.",
            ) from exc

        created = await mf_db.create_temporal_geometry(conn, catalog_id, mf_id, tg)
        if not created:
            raise HTTPException(status_code=500, detail="Failed to create temporal geometry sequence.")
        return created

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
        
        # Verify the moving feature exists and belongs to the collection
        feature = await mf_db.get_moving_feature(conn, catalog_id, mf_id)
        if not feature or feature.collection_id != collection_id:
            raise HTTPException(status_code=404, detail="Moving feature not found.")
        
        # Verify the temporal geometry exists and belongs to the moving feature
        tg = await mf_db.get_temporal_geometry(conn, catalog_id, tg_id)
        if not tg or tg.mf_id != mf_id:
            raise HTTPException(status_code=404, detail="Temporal geometry sequence not found.")
        
        # Validate datetimes and coordinates length match if both provided
        if tg_update.datetimes is not None and tg_update.coordinates is not None:
            if len(tg_update.datetimes) != len(tg_update.coordinates):
                raise HTTPException(
                    status_code=400,
                    detail=f"datetimes length ({len(tg_update.datetimes)}) must match coordinates length ({len(tg_update.coordinates)}).",
                )
        
        updated = await mf_db.update_temporal_geometry(conn, catalog_id, tg_id, tg_update)
        if not updated:
            raise HTTPException(status_code=500, detail="Failed to update temporal geometry sequence.")
        return updated

    # ------------------------------------------------------------------
    # Web page contribution (WebPageContributor / StaticAssetProvider)
    # ------------------------------------------------------------------

    def get_web_pages(self):
        from dynastore.extensions.tools.web_collect import collect_web_pages
        return collect_web_pages(self)

    def get_static_assets(self):
        from dynastore.extensions.tools.web_collect import collect_static_assets
        return collect_static_assets(self)

    def get_notebooks(self):
        try:
            from .notebooks import build_contributions
        except Exception:
            return []
        return build_contributions()

    @expose_static("movingfeatures")
    def provide_static_files(self) -> list[str]:
        """Exposes the internal static directory for the MovingFeatures browser."""
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        files = []
        for root, _, filenames in os.walk(static_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    @expose_web_page(
        page_id="movingfeatures_browser",
        title="Moving Features Browser",
        icon="fa-route",
        description="Browse moving features and their trajectories.",
    )
    async def provide_movingfeatures_browser(self, request: Request):
        return await self._serve_page_template("movingfeatures_browser.html")

    async def _serve_page_template(self, filename: str):
        from dynastore._version import VERSION
        file_path = os.path.join(os.path.dirname(__file__), "static", filename)
        if not os.path.exists(file_path):
            return Response(content=f"Template {filename} not found", status_code=404)
        with open(file_path, "r", encoding="utf-8") as f:
            return Response(content=f.read().replace("{{VERSION}}", VERSION), media_type="text/html")
