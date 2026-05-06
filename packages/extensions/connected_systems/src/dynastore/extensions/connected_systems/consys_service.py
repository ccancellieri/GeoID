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

"""OGC API - Connected Systems Part 1 extension for DynaStore.

Implements a pragmatic subset of the Connected Systems specification:
systems (sensors / stations), deployments, datastreams, and observations.
Scoped per catalog; all write endpoints respect the catalog-readiness guard.
"""

import logging
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import APIRouter, Body, Depends, FastAPI, HTTPException, Query, Request
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection
from starlette import status

from dynastore.extensions import protocols
from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.tools.db import get_async_connection
from dynastore.models.protocols import ConnectedSystemsProtocol
from dynastore.modules.connected_systems import db as consys_db
from dynastore.modules.connected_systems.models import (
    DataStream,
    DataStreamCreate,
    Deployment,
    Observation,
    ObservationCreate,
    System,
    SystemCreate,
    SystemUpdate,
)
from dynastore.tools.db import validate_sql_identifier

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Conformance URIs
# ---------------------------------------------------------------------------

OGC_CONNECTED_SYSTEMS_URIS = [
    "http://www.opengis.net/spec/ogcapi-connectedsystems-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-connectedsystems-1/1.0/conf/system-features",
    "http://www.opengis.net/spec/ogcapi-connectedsystems-1/1.0/conf/datastreams",
    "http://www.opengis.net/spec/ogcapi-connectedsystems-1/1.0/conf/observations",
]


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class ConnectedSystemsService(
    protocols.ExtensionProtocol, OGCServiceMixin, ConnectedSystemsProtocol
):
    """OGC API - Connected Systems Part 1 extension.

    All resources are scoped to a catalog (``?catalog_id=`` query parameter
    for list operations; ``catalog_id`` in the request body for creates).
    """

    priority: int = 100
    router: APIRouter

    conformance_uris = OGC_CONNECTED_SYSTEMS_URIS
    prefix = "/consys"
    protocol_title = "DynaStore OGC API - Connected Systems"
    protocol_description = (
        "Sensor, IoT, weather station and field device integration via OGC API - Connected Systems"
    )

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self.router = APIRouter(prefix="/consys", tags=["OGC API - Connected Systems"])
        self._register_routes()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        logger.info("ConnectedSystemsService: started.")
        yield
        logger.info("ConnectedSystemsService: stopped.")

    # ------------------------------------------------------------------
    # Route registration (Pattern B — instance router)
    # ------------------------------------------------------------------

    def _register_routes(self) -> None:
        self.router.add_api_route("/", self.get_landing_page, methods=["GET"])
        self.router.add_api_route("/conformance", self.get_conformance, methods=["GET"])

        # Systems
        self.router.add_api_route(
            "/systems",
            self.list_systems,
            methods=["GET"],
            response_model=List[System],
            summary="List connected systems (sensors, stations) for a catalog",
        )
        self.router.add_api_route(
            "/systems",
            self.create_system,
            methods=["POST"],
            response_model=System,
            status_code=status.HTTP_201_CREATED,
            summary="Register a new connected system",
        )
        self.router.add_api_route(
            "/systems/{system_id}",
            self.get_system,
            methods=["GET"],
            response_model=System,
            summary="Get a connected system by ID",
        )
        self.router.add_api_route(
            "/systems/{system_id}",
            self.update_system,
            methods=["PUT"],
            response_model=System,
            summary="Update a connected system",
        )
        self.router.add_api_route(
            "/systems/{system_id}",
            self.delete_system,
            methods=["DELETE"],
            status_code=status.HTTP_204_NO_CONTENT,
            summary="Delete a connected system",
        )
        self.router.add_api_route(
            "/systems/{system_id}/deployments",
            self.list_system_deployments,
            methods=["GET"],
            response_model=List[Deployment],
            summary="List deployments for a system",
        )
        self.router.add_api_route(
            "/systems/{system_id}/datastreams",
            self.list_system_datastreams,
            methods=["GET"],
            response_model=List[DataStream],
            summary="List datastreams for a system",
        )

        # DataStreams
        self.router.add_api_route(
            "/datastreams",
            self.list_datastreams,
            methods=["GET"],
            response_model=List[DataStream],
            summary="List all datastreams for a catalog",
        )
        self.router.add_api_route(
            "/datastreams",
            self.create_datastream,
            methods=["POST"],
            response_model=DataStream,
            status_code=status.HTTP_201_CREATED,
            summary="Create a datastream",
        )
        self.router.add_api_route(
            "/datastreams/{datastream_id}",
            self.get_datastream,
            methods=["GET"],
            response_model=DataStream,
            summary="Get a datastream by ID",
        )
        self.router.add_api_route(
            "/datastreams/{datastream_id}/observations",
            self.list_observations,
            methods=["GET"],
            response_model=List[Observation],
            summary="List observations for a datastream",
        )
        self.router.add_api_route(
            "/datastreams/{datastream_id}/observations",
            self.create_observation,
            methods=["POST"],
            response_model=Observation,
            status_code=status.HTTP_201_CREATED,
            summary="Insert an observation into a datastream",
        )

    # ------------------------------------------------------------------
    # Standard OGC endpoints
    # ------------------------------------------------------------------

    async def get_landing_page(self, request: Request):
        return await self.ogc_landing_page_handler(request)

    async def get_conformance(self, request: Request):
        return await self.ogc_conformance_handler(request)

    # ------------------------------------------------------------------
    # Systems
    # ------------------------------------------------------------------

    async def list_systems(
        self,
        catalog_id: str = Query(..., description="Catalog identifier"),
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> List[System]:
        validate_sql_identifier(catalog_id)
        await self._require_catalog_ready(catalog_id)
        return await consys_db.list_systems(conn, catalog_id, limit=limit, offset=offset)

    async def create_system(
        self,
        catalog_id: str = Query(..., description="Catalog identifier"),
        system: SystemCreate = Body(...),
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> System:
        validate_sql_identifier(catalog_id)
        await self._require_catalog_ready(catalog_id)

        from dynastore.modules.db_config.partition_tools import ensure_partition_exists

        try:
            await ensure_partition_exists(
                conn,
                table_name="systems",
                schema="consys",
                strategy="LIST",
                partition_value=catalog_id,
            )
            await ensure_partition_exists(
                conn,
                table_name="deployments",
                schema="consys",
                strategy="LIST",
                partition_value=catalog_id,
            )
            await ensure_partition_exists(
                conn,
                table_name="datastreams",
                schema="consys",
                strategy="LIST",
                partition_value=catalog_id,
            )
            await ensure_partition_exists(
                conn,
                table_name="observations",
                schema="consys",
                strategy="LIST",
                partition_value=catalog_id,
            )
        except Exception as exc:
            logger.error(
                "Failed to ensure partitions for catalog '%s': %s",
                catalog_id, exc, exc_info=True,
            )
            raise HTTPException(
                status_code=500,
                detail=f"Could not prepare database for catalog '{catalog_id}'.",
            )

        try:
            result = await consys_db.create_system(conn, catalog_id, system)
        except IntegrityError as exc:
            if exc.orig and getattr(exc.orig, "sqlstate", None) == "23505":
                raise HTTPException(
                    status_code=409,
                    detail=f"System '{system.system_id}' already exists in catalog '{catalog_id}'.",
                )
            raise
        if not result:
            raise HTTPException(status_code=500, detail="Failed to create system.")
        return result

    async def get_system(
        self,
        system_id: str,
        catalog_id: str = Query(..., description="Catalog identifier"),
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> System:
        validate_sql_identifier(catalog_id)
        result = await consys_db.get_system(conn, catalog_id, system_id)
        if not result:
            raise HTTPException(status_code=404, detail="System not found.")
        return result

    async def update_system(
        self,
        system_id: str,
        catalog_id: str = Query(..., description="Catalog identifier"),
        system_update: SystemUpdate = Body(...),
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> System:
        validate_sql_identifier(catalog_id)
        await self._require_catalog_ready(catalog_id)

        result = await consys_db.update_system(conn, catalog_id, system_id, system_update)
        if not result:
            raise HTTPException(status_code=404, detail="System not found.")
        return result

    async def delete_system(
        self,
        system_id: str,
        catalog_id: str = Query(..., description="Catalog identifier"),
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> None:
        validate_sql_identifier(catalog_id)
        await self._require_catalog_ready(catalog_id)

        existing = await consys_db.get_system(conn, catalog_id, system_id)
        if not existing:
            raise HTTPException(status_code=404, detail="System not found.")

        deleted = await consys_db.delete_system(conn, catalog_id, existing.id)
        if not deleted:
            raise HTTPException(status_code=404, detail="System found but could not be deleted.")

    async def list_system_deployments(
        self,
        system_id: str,
        catalog_id: str = Query(..., description="Catalog identifier"),
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> List[Deployment]:
        validate_sql_identifier(catalog_id)
        existing = await consys_db.get_system(conn, catalog_id, system_id)
        if not existing:
            raise HTTPException(status_code=404, detail="System not found.")
        return await consys_db.list_deployments_for_system(
            conn, catalog_id, system_id, limit=limit, offset=offset
        )

    async def list_system_datastreams(
        self,
        system_id: str,
        catalog_id: str = Query(..., description="Catalog identifier"),
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> List[DataStream]:
        validate_sql_identifier(catalog_id)
        existing = await consys_db.get_system(conn, catalog_id, system_id)
        if not existing:
            raise HTTPException(status_code=404, detail="System not found.")
        return await consys_db.list_datastreams_for_system(
            conn, catalog_id, system_id, limit=limit, offset=offset
        )

    # ------------------------------------------------------------------
    # DataStreams
    # ------------------------------------------------------------------

    async def list_datastreams(
        self,
        catalog_id: str = Query(..., description="Catalog identifier"),
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> List[DataStream]:
        validate_sql_identifier(catalog_id)
        return await consys_db.list_datastreams(conn, catalog_id, limit=limit, offset=offset)

    async def create_datastream(
        self,
        catalog_id: str = Query(..., description="Catalog identifier"),
        datastream: DataStreamCreate = Body(...),
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> DataStream:
        validate_sql_identifier(catalog_id)
        await self._require_catalog_ready(catalog_id)

        system = await consys_db.get_system(conn, catalog_id, str(datastream.system_id))
        if not system:
            raise HTTPException(
                status_code=404,
                detail=f"System '{datastream.system_id}' not found in catalog '{catalog_id}'.",
            )

        try:
            result = await consys_db.create_datastream(conn, catalog_id, datastream)
        except IntegrityError as exc:
            if exc.orig and getattr(exc.orig, "sqlstate", None) == "23505":
                raise HTTPException(
                    status_code=409,
                    detail=f"DataStream '{datastream.datastream_id}' already exists.",
                )
            raise
        if not result:
            raise HTTPException(status_code=500, detail="Failed to create datastream.")
        return result

    async def get_datastream(
        self,
        datastream_id: str,
        catalog_id: str = Query(..., description="Catalog identifier"),
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> DataStream:
        validate_sql_identifier(catalog_id)
        result = await consys_db.get_datastream(conn, catalog_id, datastream_id)
        if not result:
            raise HTTPException(status_code=404, detail="DataStream not found.")
        return result

    # ------------------------------------------------------------------
    # Observations
    # ------------------------------------------------------------------

    async def list_observations(
        self,
        datastream_id: str,
        catalog_id: str = Query(..., description="Catalog identifier"),
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> List[Observation]:
        validate_sql_identifier(catalog_id)
        ds = await consys_db.get_datastream(conn, catalog_id, datastream_id)
        if not ds:
            raise HTTPException(status_code=404, detail="DataStream not found.")
        return await consys_db.list_observations(
            conn, catalog_id, datastream_id, limit=limit, offset=offset
        )

    async def create_observation(
        self,
        datastream_id: str,
        catalog_id: str = Query(..., description="Catalog identifier"),
        observation: ObservationCreate = Body(...),
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> Observation:
        validate_sql_identifier(catalog_id)
        await self._require_catalog_ready(catalog_id)

        ds = await consys_db.get_datastream(conn, catalog_id, datastream_id)
        if not ds:
            raise HTTPException(status_code=404, detail="DataStream not found.")

        result = await consys_db.create_observation(
            conn, catalog_id, ds.id, observation
        )
        if not result:
            raise HTTPException(status_code=500, detail="Failed to create observation.")
        return result
