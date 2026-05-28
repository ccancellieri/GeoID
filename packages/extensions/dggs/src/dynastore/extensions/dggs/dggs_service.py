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

"""OGC API - Discrete Global Grid Systems (DGGS) Part 1 extension for DynaStore.

Implements the following endpoints:

    GET /dggs/                                                  landing page
    GET /dggs/conformance                                       conformance
    GET /dggs/dggs-list                                         list DGGRS
    GET /dggs/dggs-list/{dggsId}                               DGGRS metadata
    GET /dggs/collections                                       DGGS-enabled collections
    GET /dggs/catalogs/{catalog_id}/collections/{collection_id}/dggs
        Aggregate collection features into DGGS zones
    GET /dggs/catalogs/{catalog_id}/collections/{collection_id}/dggs/{zoneId}
        Features aggregated within a specific DGGS zone
"""

import logging
from contextlib import asynccontextmanager
from typing import List, Optional, Set

# Dependency-free helper (no h3/s2sphere): safe to import before the SCOPE gate.
from dynastore.modules.dggs.bbox import parse_bbox

import h3 as _h3_scope_gate  # noqa: F401  # SCOPE gate: extension_dggs requires h3
import s2sphere as _s2sphere_scope_gate  # noqa: F401  # SCOPE gate: extension_dggs requires s2sphere
_ = (_h3_scope_gate, _s2sphere_scope_gate)  # silence pyright "unused" — load-bearing for SCOPE filtering

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request, status

from dynastore.extensions.dggs.config import DGGSConfig
from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.url import get_root_url
from dynastore.models.shared_models import Link
from dynastore.modules.dggs import h3_indexer, s2_indexer
from dynastore.modules.dggs.aggregator import aggregate_features
from dynastore.modules.dggs.h3_indexer import (
    H3_MAX_RESOLUTION,
    H3_MIN_RESOLUTION,
)
from dynastore.modules.dggs.models import (
    DGGRSInfo,
    DGGRSList,
    DGGSFeatureCollection,
)
from dynastore.modules.dggs.zone_query import (
    build_global_query,
    build_query_for_bbox,
    build_query_for_zone,
    build_query_for_zone_indexed,
)
from dynastore.modules.dggs.s2_indexer import S2_MAX_LEVEL, S2_MIN_LEVEL

logger = logging.getLogger(__name__)

# S2 levels go 0-30; H3 resolutions go 0-15.
# The zone-level query parameter uses the widest range; each DGGRS validates internally.
_ZONE_LEVEL_MAX = max(H3_MAX_RESOLUTION, S2_MAX_LEVEL)
_ZONE_LEVEL_MIN = min(H3_MIN_RESOLUTION, S2_MIN_LEVEL)


def _parse_parameter_names(parameter_name: Optional[str]) -> Optional[Set[str]]:
    """Parse a comma-separated parameter-name query string into a set, or None."""
    if not parameter_name:
        return None
    return {p.strip() for p in parameter_name.split(",") if p.strip()}


OGC_API_DGGS_URIS = [
    "https://www.opengis.net/spec/ogcapi-dggs-1/1.0/conf/core",
    "https://www.opengis.net/spec/ogcapi-dggs-1/1.0/conf/data-retrieval",
    "https://www.opengis.net/spec/ogcapi-dggs-1/1.0/conf/zone-query",
]

_H3_DGGRS = DGGRSInfo(
    id="H3",
    title="Uber H3 Hierarchical Hexagonal Grid",
    description=(
        "The H3 Discrete Global Grid Reference System uses a hierarchical hexagonal "
        "tessellation of the sphere at 16 resolutions (0–15). It is widely used for "
        "planetary-scale geospatial aggregations (agriculture, climate, food security)."
    ),
    uri="https://h3geo.org/",
    maxRefinementLevel=H3_MAX_RESOLUTION,
    defaultRefinementLevel=5,
    links=[],
)

_S2_DGGRS = DGGRSInfo(
    id="S2",
    title="Google S2 Geometry Hierarchical Grid",
    description=(
        "The S2 Discrete Global Grid Reference System uses a hierarchical quadrilateral "
        "tessellation of the sphere at 31 levels (0–30), based on a space-filling curve "
        "on the faces of a projected cube. It provides exact spatial indexing with "
        "compact integer cell IDs."
    ),
    uri="https://s2geometry.io/",
    maxRefinementLevel=S2_MAX_LEVEL,
    defaultRefinementLevel=10,
    links=[],
)

_SUPPORTED_DGGRS: dict = {"H3": _H3_DGGRS, "S2": _S2_DGGRS}


class DGGSService(ExtensionProtocol, OGCServiceMixin):
    """OGC API - DGGS Part 1 extension.

    Priority 170 — after Coverages (160), before Dimensions (200).
    """

    priority: int = 170
    router: APIRouter

    conformance_uris = OGC_API_DGGS_URIS
    prefix = "/dggs"
    protocol_title = "DynaStore OGC API - DGGS"
    protocol_description = (
        "Access to collection data aggregated into Discrete Global Grid Systems (DGGS)"
    )

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self.router = APIRouter(prefix="/dggs", tags=["OGC API - DGGS"])
        self._register_routes()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        yield

    # ------------------------------------------------------------------
    # Route registration
    # ------------------------------------------------------------------

    def _register_routes(self) -> None:
        self.router.add_api_route(
            "/",
            self.get_landing_page,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/conformance",
            self.get_conformance,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/dggs-list",
            self.get_dggrs_list,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/dggs-list/{dggsId}",
            self.get_dggrs,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/collections",
            self.get_collections,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/dggs",
            self.get_dggs_data,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/dggs/{zoneId}",
            self.get_dggs_zone,
            methods=["GET"],
        )

    # ------------------------------------------------------------------
    # Landing page & conformance
    # ------------------------------------------------------------------

    async def get_landing_page(self, request: Request):
        return await self.ogc_landing_page_handler(request)

    async def get_conformance(self, request: Request):
        return await self.ogc_conformance_handler(request)

    # ------------------------------------------------------------------
    # DGGRS discovery endpoints
    # ------------------------------------------------------------------

    async def get_dggrs_list(self, request: Request) -> DGGRSList:
        """List all supported DGGRS."""
        root_url = get_root_url(request)
        dggrs = []
        for dggrs_id, info in _SUPPORTED_DGGRS.items():
            enriched = info.model_copy(
                update={
                    "links": [
                        Link(
                            href=f"{root_url}/dggs/dggs-list/{dggrs_id}",
                            rel="self",
                            type="application/json",
                            title=f"{dggrs_id} DGGRS definition",  # type: ignore[arg-type]
                        )
                    ]
                }
            )
            dggrs.append(enriched)
        return DGGRSList(
            dggrs=dggrs,
            links=[
                Link(
                    href=f"{root_url}/dggs/dggs-list",
                    rel="self",
                    type="application/json",
                    title="List of supported DGGRS",  # type: ignore[arg-type]
                )
            ],
        )

    async def get_dggrs(self, dggsId: str, request: Request) -> DGGRSInfo:
        """Return metadata for a specific DGGRS."""
        info = _SUPPORTED_DGGRS.get(dggsId.upper())
        if info is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"DGGRS '{dggsId}' not supported. Supported: {list(_SUPPORTED_DGGRS)}",
            )
        return info

    # ------------------------------------------------------------------
    # Collections listing
    # ------------------------------------------------------------------

    async def get_collections(self, request: Request) -> dict:
        """List all collections accessible via DGGS endpoints."""
        catalogs_svc = await self._get_catalogs_service()
        try:
            catalogs = await catalogs_svc.list_catalogs()
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to list catalogs: {exc}",
            )

        root_url = get_root_url(request)
        result = []
        for catalog in catalogs or []:
            # list_catalogs may return Catalog models or dicts depending on
            # the protocol implementation — handle both.
            catalog_id = (
                getattr(catalog, "id", None)
                or (catalog.get("id", "") if isinstance(catalog, dict) else "")
            )
            try:
                collections = await catalogs_svc.list_collections(catalog_id)
            except Exception:
                continue
            for col in collections or []:
                col_id = (
                    getattr(col, "id", None)
                    or (col.get("id", "") if isinstance(col, dict) else "")
                )
                result.append(
                    {
                        "id": col_id,
                        "catalog": catalog_id,
                        "links": [
                            {
                                "href": (
                                    f"{root_url}/dggs/catalogs/{catalog_id}"
                                    f"/collections/{col_id}/dggs"
                                ),
                                "rel": "dggs",
                                "type": "application/geo+json",
                                "title": f"DGGS data for {col_id}",
                            }
                        ],
                    }
                )

        return {"collections": result, "numberMatched": len(result)}

    # ------------------------------------------------------------------
    # DGGS data retrieval endpoints
    # ------------------------------------------------------------------

    async def get_dggs_data(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        zone_level: Optional[int] = Query(
            None,
            alias="zone-level",
            ge=_ZONE_LEVEL_MIN,
            le=_ZONE_LEVEL_MAX,
            description="Resolution/level (H3: 0-15, S2: 0-30). Defaults to DGGSConfig.default_resolution.",
        ),
        bbox: Optional[str] = Query(
            None,
            description="Bounding box filter: xmin,ymin,xmax,ymax (WGS-84)",
        ),
        datetime: Optional[str] = Query(
            None,
            description="Temporal filter (RFC 3339 date or date-time, or open interval)",
        ),
        parameter_name: Optional[str] = Query(
            None,
            alias="parameter-name",
            description="Comma-separated property names to include in aggregation",
        ),
        dggs_id: str = Query(
            "H3",
            alias="dggs-id",
            description="DGGRS identifier: 'H3' or 'S2'",
        ),
    ) -> DGGSFeatureCollection:
        """Retrieve collection features aggregated into DGGS zones.

        Queries PostGIS for features in the requested bbox/datetime, then
        aggregates them on-the-fly into cells at the requested resolution.
        """
        dggs_id = dggs_id.upper()
        if dggs_id not in _SUPPORTED_DGGRS:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported DGGRS '{dggs_id}'. Supported: {list(_SUPPORTED_DGGRS)}",
            )

        config = await self._get_plugin_config(DGGSConfig, catalog_id, collection_id)
        resolution = zone_level if zone_level is not None else config.default_resolution
        resolution = min(resolution, config.max_resolution)

        param_names = _parse_parameter_names(parameter_name)

        bbox_tuple = None
        if bbox:
            try:
                bbox_tuple = parse_bbox(bbox)
            except ValueError as exc:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
                )

        if bbox_tuple:
            query = build_query_for_bbox(
                *bbox_tuple,
                datetime_str=datetime,
                limit=config.max_features_per_request,
            )
        else:
            query = build_global_query(
                datetime_str=datetime,
                limit=config.max_features_per_request,
            )

        features = await self._fetch_features(catalog_id, collection_id, query)
        return aggregate_features(features, resolution, param_names, dggs_id)

    async def get_dggs_zone(
        self,
        catalog_id: str,
        collection_id: str,
        zoneId: str,
        request: Request,
        datetime: Optional[str] = Query(None),
        parameter_name: Optional[str] = Query(
            None,
            alias="parameter-name",
        ),
        dggs_id: str = Query(
            "H3",
            alias="dggs-id",
            description="DGGRS identifier: 'H3' or 'S2'",
        ),
    ) -> DGGSFeatureCollection:
        """Retrieve data aggregated within a specific DGGS zone.

        The zoneId must be a valid cell token for the requested DGGRS.
        Features intersecting the zone's bounding box are fetched from PostGIS
        and aggregated into the single requested cell.
        """
        dggs_id = dggs_id.upper()
        if dggs_id not in _SUPPORTED_DGGRS:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported DGGRS '{dggs_id}'. Supported: {list(_SUPPORTED_DGGRS)}",
            )

        # Validate zone ID and extract resolution using the correct indexer.
        if dggs_id == "H3":
            if not h3_indexer.is_valid_cell(zoneId):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid H3 zone ID: {zoneId!r}",
                )
            resolution = h3_indexer.get_resolution(zoneId)
            sidecar_field = f"h3_res{resolution}"
            cell_int = h3_indexer.cell_str_to_int(zoneId)
        else:  # S2
            if not s2_indexer.is_valid_cell(zoneId):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid S2 zone ID: {zoneId!r}",
                )
            resolution = s2_indexer.get_level(zoneId)
            sidecar_field = f"s2_res{resolution}"
            cell_int = s2_indexer.cell_str_to_int(zoneId)

        config = await self._get_plugin_config(DGGSConfig, catalog_id, collection_id)

        if await self._has_sidecar_field(catalog_id, collection_id, sidecar_field):
            # Preferred: exact B-tree EQ on pre-computed sidecar column.
            query = build_query_for_zone_indexed(
                sidecar_field=sidecar_field,
                cell_int=cell_int,
                datetime_str=datetime,
                limit=config.max_features_per_request,
            )
        elif dggs_id == "H3":
            # Fallback for H3: GIST bbox scan derived from H3 polygon.
            query = build_query_for_zone(
                zoneId,
                datetime_str=datetime,
                limit=config.max_features_per_request,
            )
        else:
            # Fallback for S2: derive bbox from S2 cell's rect bound.
            # Using get_rect_bound() instead of vertex min/max correctly handles
            # cells that cross the antimeridian or contain a pole.
            xmin, ymin, xmax, ymax = s2_indexer.rect_bound_for_cell(zoneId)
            query = build_query_for_bbox(
                xmin=xmin,
                ymin=ymin,
                xmax=xmax,
                ymax=ymax,
                datetime_str=datetime,
                limit=config.max_features_per_request,
            )

        features = await self._fetch_features(catalog_id, collection_id, query)

        param_names = _parse_parameter_names(parameter_name)
        all_zones = aggregate_features(features, resolution, param_names, dggs_id=dggs_id)

        # Keep only the requested zone
        matching = [f for f in all_zones.features if f.id == zoneId]
        return all_zones.model_copy(
            update={
                "features": matching,
                "numberMatched": len(matching),
                "numberReturned": len(matching),
            }
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _has_sidecar_field(
        self,
        catalog_id: str,
        collection_id: str,
        sidecar_field: str,
    ) -> bool:
        """Return True if the collection's geometry sidecar has *sidecar_field* pre-computed.

        When True, :func:`build_query_for_zone_indexed` can be used instead of the
        bbox fallback, giving exact B-tree indexed lookups.
        """
        try:
            from dynastore.models.protocols import ItemsProtocol
            from dynastore.tools.discovery import get_protocol

            items_svc = get_protocol(ItemsProtocol)
            if not items_svc:
                return False
            fields = await items_svc.get_collection_fields(catalog_id, collection_id)
            return sidecar_field in (fields or {})
        except Exception:
            return False

    async def _fetch_features(
        self,
        catalog_id: str,
        collection_id: str,
        query,
    ) -> list:
        catalogs_svc = await self._get_catalogs_service()
        try:
            return await catalogs_svc.search_items(catalog_id, collection_id, query)
        except Exception as exc:
            logger.exception("DGGS query failed for collection '%s/%s'", catalog_id, collection_id)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to query collection '{collection_id}': {exc}",
            ) from exc
