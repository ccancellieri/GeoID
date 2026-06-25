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

import logging
from typing import List, Optional
import pyproj as _pyproj_scope_gate  # noqa: F401  # SCOPE gate: extension_crs requires module_crs (pyproj)
_ = _pyproj_scope_gate  # silence pyright "unused" — load-bearing for SCOPE filtering
from fastapi import APIRouter, Depends, HTTPException, Query, Response, status, Request, FastAPI
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.db import get_async_connection

from dynastore.modules.crs.models import CRS, CRSCreate, CRSDefinition, CRSLink, GlobalCRSList

from dynastore.models.protocols import CatalogsProtocol
from dynastore.models.protocols.crs import CRSProtocol
from dynastore.models.driver_context import DriverContext
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


def _resolve_global_crs(crs_uri: str) -> CRSDefinition:
    """Resolve a global authority CRS identifier to a canonical WKT2 definition.

    Accepts the standard identifier forms PROJ understands offline — EPSG codes
    (``EPSG:4326`` / ``4326``), URNs (``urn:ogc:def:crs:EPSG::4326``) and OGC
    register URLs (``http://www.opengis.net/def/crs/EPSG/0/4326``). Resolution is
    read-only and pyproj-backed; it touches no tenant data, so it is safe to
    expose at platform scope. Tenant-registered custom CRS are NOT resolved here —
    they live under ``/crs/catalogs/{catalog_id}/...`` and stay catalog-isolated.
    """
    try:
        from pyproj import CRS as PyprojCRS
        from pyproj.exceptions import CRSError
    except ImportError as e:
        raise HTTPException(
            status_code=503, detail="pyproj is not available for CRS resolution."
        ) from e

    try:
        crs_obj = PyprojCRS.from_user_input(crs_uri)
    except (CRSError, ValueError, TypeError) as e:
        raise HTTPException(
            status_code=404, detail=f"Unknown authority CRS '{crs_uri}'."
        ) from e

    wkt = crs_obj.to_wkt(version="WKT2_2019")
    return CRSDefinition(
        definition_type=CRSDefinition.CRSDefinitionType.WKT2, definition=wkt
    )


def _list_global_crs(
    authority: Optional[str], limit: int, offset: int
) -> tuple[list[str], int]:
    """List global authority CRS as OGC register URIs, with the page total.

    Backed by the PROJ database via pyproj; read-only and tenant-free, so it is
    safe at platform scope. Deprecated CRS are excluded. Returns the page of URIs
    and the total match count (for OGC ``numberMatched`` and the ``next`` link).
    """
    try:
        from pyproj.database import query_crs_info
    except ImportError as e:
        raise HTTPException(
            status_code=503, detail="pyproj is not available for CRS listing."
        ) from e

    try:
        infos = query_crs_info(auth_name=authority or None, allow_deprecated=False)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"CRS query failed: {e}") from e

    total = len(infos)
    page = infos[offset : offset + limit]
    uris = [
        f"http://www.opengis.net/def/crs/{info.auth_name}/0/{info.code}"
        for info in page
    ]
    return uris, total


class CRSExtension(ExtensionProtocol):
    priority: int = 100
    """
    An extension for managing and registering Coordinate Reference Systems (CRS).
    It provides RESTful endpoints for creating, retrieving, searching, and deleting CRS definitions.
    Includes Content-Negotiation for OGC compliance.
    """
    def __init__(self, app: FastAPI):
        self.app = app
        self.router = APIRouter(prefix="/crs", tags=["CRS Definitions"])
        self._setup_routes()

    def _setup_routes(self):
        # Routes follow the platform convention: /crs/catalogs/{catalog_id}/...
        self.router.add_api_route(
            "/catalogs/{catalog_id}",
            self.create_crs_endpoint,
            methods=["POST"],
            response_model=CRS,
            status_code=status.HTTP_201_CREATED,
            summary="Register a New CRS (OGC aligned path)",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/{crs_uri:path}",
            self.update_crs_endpoint,
            methods=["PUT"],
            response_model=CRS,
            summary="Update a CRS Definition (OGC aligned path)",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}",
            self.list_crs_endpoint,
            methods=["GET"],
            response_model=List[CRS],
            summary="List All CRS Definitions (OGC aligned path)",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/search",
            self.search_crs_endpoint,
            methods=["GET"],
            response_model=List[CRS],
            summary="Search CRS Definitions (OGC aligned path)",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/by-name/{crs_name}",
            self.get_crs_by_name_endpoint,
            methods=["GET"],
            response_model=CRS,
            summary="Get CRS by Name (OGC aligned path)",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/{crs_uri:path}",
            self.get_crs_by_uri_endpoint,
            methods=["GET"],
            summary="Get CRS by URI (OGC aligned path)",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/{crs_uri:path}",
            self.delete_crs_endpoint,
            methods=["DELETE"],
            status_code=status.HTTP_204_NO_CONTENT,
            summary="Delete a CRS (OGC aligned path)",
        )
        # Platform scope: paginated catalogue of resolvable global authority CRS.
        self.router.add_api_route(
            "/",
            self.list_global_crs_endpoint,
            methods=["GET"],
            response_model=GlobalCRSList,
            summary="List global authority CRS (EPSG/OGC) — platform scope, paginated",
        )
        # Platform scope: read-only resolution of global authority CRS (EPSG/OGC).
        # Registered LAST — the greedy {crs_uri:path} converter must not shadow the
        # catalog-scoped routes above (or the list route), which are matched first
        # by registration order.
        self.router.add_api_route(
            "/{crs_uri:path}",
            self.resolve_global_crs_endpoint,
            methods=["GET"],
            response_model=CRSDefinition,
            summary="Resolve a global authority CRS (EPSG/OGC) — platform scope, read-only",
        )

    @property
    def catalogs(self) -> CatalogsProtocol:
        svc = get_protocol(CatalogsProtocol)
        if svc is None:
            raise HTTPException(status_code=503, detail="Catalogs service not registered")
        return svc

    @property
    def crs(self) -> CRSProtocol:
        svc = get_protocol(CRSProtocol)
        if svc is None:
            raise HTTPException(status_code=503, detail="CRS service not registered")
        return svc


    async def create_crs_endpoint(
        self,
        catalog_id: str,
        crs_data: CRSCreate,
        conn: AsyncConnection = Depends(get_async_connection)
    ):
        """
        Registers a new CRS definition. 
        Validates WKT structure against OGC 18-010r11 via the internal model validators.
        """
        if not await self.catalogs.get_catalog(catalog_id, ctx=DriverContext(db_resource=conn)):
            raise HTTPException(status_code=404, detail=f"Catalog '{catalog_id}' not found.")

        try:
            new_crs = await self.crs.create_crs(conn, catalog_id, crs_data)
            return new_crs
        except Exception as e:
            logger.error(f"Failed to create CRS '{crs_data.crs_uri}' for catalog '{catalog_id}': {e}")
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e)) from e


    async def update_crs_endpoint(
        self,
        catalog_id: str,
        crs_uri: str,
        crs_data: CRSCreate,
        conn: AsyncConnection = Depends(get_async_connection)
    ):
        """Updates an existing CRS definition."""
        if crs_uri != crs_data.crs_uri:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="CRS URI in path does not match URI in payload.")

        if not await self.catalogs.get_catalog(catalog_id, ctx=DriverContext(db_resource=conn)):
            raise HTTPException(status_code=404, detail=f"Catalog '{catalog_id}' not found.")
            
        updated_crs = await self.crs.update_crs(conn, catalog_id, crs_uri, crs_data)
        if not updated_crs:
            raise HTTPException(status_code=404, detail=f"CRS with URI '{crs_uri}' not found in catalog '{catalog_id}'.")
        return updated_crs


    async def list_crs_endpoint(
        self,
        catalog_id: str,
        conn: AsyncConnection = Depends(get_async_connection),
        limit: int = Query(20, ge=1, le=1000),
        offset: int = Query(0, ge=0)
    ):
        if not await self.catalogs.get_catalog(catalog_id, ctx=DriverContext(db_resource=conn)):
            raise HTTPException(status_code=404, detail=f"Catalog '{catalog_id}' not found.")
        
        return await self.crs.list_crs(conn, catalog_id, limit, offset)


    async def search_crs_endpoint(
        self,
        catalog_id: str,
        q: str = Query(..., description="Search term."),
        conn: AsyncConnection = Depends(get_async_connection),
        limit: int = Query(20, ge=1, le=1000),
        offset: int = Query(0, ge=0)
    ):
        if not await self.catalogs.get_catalog(catalog_id, ctx=DriverContext(db_resource=conn)):
            raise HTTPException(status_code=404, detail=f"Catalog '{catalog_id}' not found.")
        
        return await self.crs.search_crs(conn, catalog_id, q, limit, offset)


    async def get_crs_by_name_endpoint(
        self,
        catalog_id: str,
        crs_name: str,
        conn: AsyncConnection = Depends(get_async_connection)
    ):
        crs = await self.crs.get_crs_by_name(conn, catalog_id, crs_name)
        if not crs:
            raise HTTPException(status_code=404, detail=f"CRS with name '{crs_name}' not found.")
        return crs


    async def get_crs_by_uri_endpoint(
        self,
        request: Request,
        catalog_id: str,
        crs_uri: str,
        conn: AsyncConnection = Depends(get_async_connection)
    ):
        """
        Retrieves a single CRS definition.
        
        **Content Negotiation**:
        - If `Accept: application/json` (default): Returns the full CRS metadata object.
        - If `Accept: text/plain`: Returns the raw WKT/PROJ string definition.
        
        This allows this endpoint to serve as the authoritative resolution URL for OGC API Features.
        """
        crs = await self.crs.get_crs_by_uri(conn, catalog_id, crs_uri)
        if not crs:
            raise HTTPException(status_code=404, detail=f"CRS with URI '{crs_uri}' not found.")
        
        # Basic Content Negotiation
        accept_header = request.headers.get("Accept", "application/json")
        
        if "text/plain" in accept_header:
            return Response(content=crs.definition.definition, media_type="text/plain")
        
        # Default to returning the full JSON model
        return crs


    async def list_global_crs_endpoint(
        self,
        request: Request,
        limit: int = Query(20, ge=1, le=1000, description="Page size."),
        offset: int = Query(0, ge=0, description="Number of CRS to skip."),
        authority: str = Query(
            "EPSG",
            description="CRS authority to enumerate (e.g. 'EPSG'). Empty enumerates all.",
        ),
    ) -> GlobalCRSList:
        """List global authority CRS at platform scope (read-only, paginated).

        Each entry is an OGC register URI that can be fed back to the resolver
        (``/crs/{crs_uri}``) or used as an OGC API - Features ``crs`` value. Only
        globally-defined authority CRS are listed; tenant-registered custom CRS
        are served per-catalog under ``/crs/catalogs/{catalog_id}/...``.
        """
        uris, total = _list_global_crs(authority, limit, offset)

        base = str(request.url).split("?", 1)[0]
        links = [CRSLink(href=str(request.url), rel="self", type="application/json")]
        if offset + limit < total:
            links.append(
                CRSLink(
                    href=f"{base}?limit={limit}&offset={offset + limit}&authority={authority}",
                    rel="next",
                    type="application/json",
                )
            )
        return GlobalCRSList(
            crs=uris,
            numberMatched=total,
            numberReturned=len(uris),
            links=links,
        )


    async def resolve_global_crs_endpoint(
        self,
        request: Request,
        crs_uri: str,
    ):
        """Resolve a global authority CRS (EPSG/OGC) at platform scope (read-only).

        **Content Negotiation** (mirrors the catalog-scoped resolver):
        - ``Accept: text/plain`` → the canonical WKT2 string.
        - otherwise → the full ``CRSDefinition`` JSON (name/area auto-extracted).

        Only globally-defined authority CRS are resolved here; tenant-registered
        custom CRS are served per-catalog under ``/crs/catalogs/{catalog_id}/...``.
        """
        definition = _resolve_global_crs(crs_uri)

        accept_header = request.headers.get("Accept", "application/json")
        if "text/plain" in accept_header:
            return Response(content=definition.definition, media_type="text/plain")
        return definition


    async def delete_crs_endpoint(
        self,
        catalog_id: str,
        crs_uri: str,
        conn: AsyncConnection = Depends(get_async_connection)
    ):
        if not await self.catalogs.get_catalog(catalog_id, ctx=DriverContext(db_resource=conn)):
            raise HTTPException(status_code=404, detail=f"Catalog '{catalog_id}' not found.")

        success = await self.crs.delete_crs(conn, catalog_id, crs_uri)
        if not success:
            raise HTTPException(status_code=404, detail=f"CRS with URI '{crs_uri}' not found.")
        
        return Response(status_code=status.HTTP_204_NO_CONTENT)