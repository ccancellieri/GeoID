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

"""OGC API - Moving Features Part 1 extension for DynaStore.

Provides CRUD for moving features and their temporal geometry sequences,
scoped to (catalog_id, collection_id) pairs.

Conforms to OGC API - Moving Features Part 1 (approved Feb 2026).
"""

import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Body, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from dynastore.extensions import protocols
from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.tools.db import get_async_connection
from dynastore.models.protocols import MovingFeaturesProtocol
from dynastore.modules.catalog import catalog_module
from dynastore.modules.moving_features import db as mf_db
from dynastore.modules.moving_features.db import delete_temporal_geometries_by_mf
from dynastore.modules.moving_features.models import (
    MovingFeature,
    MovingFeatureCreate,
    TemporalGeometry,
    TemporalGeometryCreate,
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

    # ------------------------------------------------------------------
    # Standard OGC endpoints
    # ------------------------------------------------------------------

    async def get_landing_page(self, request: Request):
        return await self.ogc_landing_page_handler(request)

    async def get_conformance(self, request: Request):
        return await self.ogc_conformance_handler(request)

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
    ) -> List[MovingFeature]:
        validate_sql_identifier(catalog_id)
        validate_sql_identifier(collection_id)
        if not await catalog_module.get_collection(catalog_id, collection_id):
            raise HTTPException(status_code=404, detail="Collection not found.")
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
            )

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
            )

        created = await mf_db.create_temporal_geometry(conn, catalog_id, mf_id, tg)
        if not created:
            raise HTTPException(status_code=500, detail="Failed to create temporal geometry sequence.")
        return created
