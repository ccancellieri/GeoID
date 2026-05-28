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

"""OGC API - Environmental Data Retrieval (EDR) extension for DynaStore.

Reuses the coverages GDAL VSI pipeline (open_raster_vsi, resolve_window,
read_window_iter) for spatial extraction. EDR adds temporal context and
the position/area/cube query vocabulary on top.
"""

import logging
from contextlib import asynccontextmanager
from typing import List, Optional, cast

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from dynastore.extensions.ogc_base import OGCServiceMixin, ogc_asset_href
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.url import get_root_url
from dynastore.models.protocols import CatalogsProtocol

from . import edr_models as em

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# OGC API - EDR conformance URIs (OGC 19-086r5)
# ---------------------------------------------------------------------------

OGC_API_EDR_URIS: List[str] = [
    "http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/core",
    "http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/collections",
    "http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/json",
    "http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/edr-geojson",
    "http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/queries",
]

_COVJSON_MEDIA_TYPE = "application/prs.coverage+json"
_GEOJSON_MEDIA_TYPE = "application/geo+json"


# ---------------------------------------------------------------------------
# Module-level helpers (pure functions, no self)
# ---------------------------------------------------------------------------


def _resolve_edr_format(f: Optional[str]) -> str:
    if f is None:
        return "covjson"
    v = f.lower()
    if v in ("covjson", "coveragejson"):
        return "covjson"
    if v == "geojson":
        return "geojson"
    raise HTTPException(status_code=415, detail=f"Unsupported EDR format: {f!r}")


def _resolve_band_names(item: dict, requested_params: Optional[List[str]]) -> List[str]:
    """Return filtered band names from STAC raster:bands metadata."""
    from dynastore.modules.edr.parameter_metadata import _select_bands, filter_parameters

    bands = _select_bands(item)
    if requested_params:
        bands = filter_parameters(bands, requested_params)
    if not bands:
        return ["value"]
    return [b.get("name", f"band_{i + 1}") for i, b in enumerate(bands)]


def _item_datetime(item: dict) -> Optional[str]:
    props = item.get("properties") or {}
    return props.get("datetime") or props.get("start_datetime")


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class EDRService(ExtensionProtocol, OGCServiceMixin):
    """OGC API - Environmental Data Retrieval (EDR) extension.

    Priority 165 — after CoveragesService (160), before Dimensions (200).
    """

    priority: int = 165
    router: APIRouter

    # OGCServiceMixin class attributes
    conformance_uris = OGC_API_EDR_URIS
    prefix = "/edr"
    protocol_title = "DynaStore OGC API - EDR"
    protocol_description = "Environmental Data Retrieval via OGC API - EDR"

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self.router = APIRouter(prefix="/edr", tags=["OGC API - EDR"])
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
            response_model=em.EDRLandingPage,
        )
        self.router.add_api_route(
            "/conformance",
            self.get_conformance,
            methods=["GET"],
            response_model=em.Conformance,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections",
            self.list_collections,
            methods=["GET"],
            response_model=em.EDRCollections,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self.get_collection,
            methods=["GET"],
            response_model=em.EDRCollection,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/position",
            self.query_position,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/area",
            self.query_area,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/cube",
            self.query_cube,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/locations",
            self.list_locations,
            methods=["GET"],
            response_model=em.EDRLocations,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/locations/{location_id}",
            self.get_location,
            methods=["GET"],
        )

    # ------------------------------------------------------------------
    # Landing page & conformance (delegated to OGCServiceMixin)
    # ------------------------------------------------------------------

    async def get_landing_page(self, request: Request) -> em.EDRLandingPage:
        # The shared OGC handler returns the base ``LandingPage``; re-validate
        # through ``EDRLandingPage`` so the EDR-specific defaults for title /
        # description apply when the underlying landing page omits them.
        base = await self.ogc_landing_page_handler(request)
        return em.EDRLandingPage(**base.model_dump())

    async def get_conformance(self, request: Request) -> em.Conformance:
        return await self.ogc_conformance_handler(request)

    # ------------------------------------------------------------------
    # Collections
    # ------------------------------------------------------------------

    async def list_collections(
        self,
        catalog_id: str,
        request: Request,
    ) -> em.EDRCollections:
        catalogs = await self._get_catalogs_service()
        try:
            collections = await catalogs.list_collections(catalog_id)
        except Exception as exc:
            raise HTTPException(
                status_code=404,
                detail=f"Catalog {catalog_id!r} not found.",
            ) from exc

        base_url = get_root_url(request).rstrip("/")
        from dynastore.modules.edr.collection_metadata import build_edr_collection

        items: List[em.EDRCollection] = []
        for col in collections or []:
            try:
                col_dict = build_edr_collection(catalog_id, col, base_url=base_url)
                items.append(em.EDRCollection(**col_dict))
            except Exception as exc:
                logger.warning(
                    "EDR: skipping collection %s: %s",
                    getattr(col, "id", "?"),
                    exc,
                )

        from dynastore.models.shared_models import Link

        return em.EDRCollections(
            collections=items,
            links=[
                Link(
                    href=f"{base_url}/edr/catalogs/{catalog_id}/collections",
                    rel="self",
                    type="application/json",
                    title="EDR collections",  # type: ignore[arg-type]
                ),
            ],
        )

    async def get_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
    ) -> em.EDRCollection:
        catalogs = await self._get_catalogs_service()
        try:
            collection = await catalogs.get_collection(catalog_id, collection_id)
        except Exception:
            collection = None
        if collection is None:
            raise HTTPException(
                status_code=404,
                detail=f"Collection {collection_id!r} not found in catalog {catalog_id!r}.",
            )

        await self._require_catalog_ready(catalog_id)
        base_url = get_root_url(request).rstrip("/")
        from dynastore.modules.edr.collection_metadata import build_edr_collection

        col_dict = build_edr_collection(catalog_id, collection, base_url=base_url)

        item = await self._get_first_item(catalog_id, collection_id)
        if item:
            from dynastore.modules.edr.parameter_metadata import build_parameters

            col_dict["parameter_names"] = build_parameters(item)

        return em.EDRCollection(**col_dict)

    # ------------------------------------------------------------------
    # EDR query endpoints
    # ------------------------------------------------------------------

    async def query_position(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        coords: str = Query(..., description="WKT POINT geometry, e.g. POINT(lon lat)"),
        z: Optional[str] = Query(None, description="Vertical level or range"),
        datetime: Optional[str] = Query(None, description="ISO 8601 instant or interval"),
        parameter_name: Optional[str] = Query(None, alias="parameter-name"),
        crs: Optional[str] = Query(None, description="Output CRS (default CRS84)"),
        f: Optional[str] = Query("CoverageJSON", description="Output format"),
    ):
        """Extract values at a geographic point (position query)."""
        from dynastore.modules.edr.query_handlers.position import (
            extract_point_values,
            parse_wkt_point,
        )
        from dynastore.modules.edr.parameter_metadata import build_parameters

        try:
            lon, lat = parse_wkt_point(coords)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        fmt = _resolve_edr_format(f)
        await self._require_catalog_ready(catalog_id)
        item = await self._get_item_for_datetime(catalog_id, collection_id, datetime)
        if item is None:
            raise HTTPException(
                status_code=404,
                detail="No items found for the given query parameters.",
            )

        href = ogc_asset_href(item, error_detail="No asset href on EDR item.")
        requested_params = (
            [p.strip() for p in parameter_name.split(",")]
            if parameter_name
            else None
        )
        band_names = _resolve_band_names(item, requested_params)
        values = extract_point_values(href, lon, lat)
        dt = _item_datetime(item)
        parameters = build_parameters(item)
        if requested_params:
            parameters = {k: v for k, v in parameters.items() if k in requested_params}

        if fmt == "covjson":
            from dynastore.modules.edr.output.coveragejson import write_position_coveragejson

            gen = write_position_coveragejson(lon, lat, dt, parameters, values, band_names)
            return StreamingResponse(gen, media_type=_COVJSON_MEDIA_TYPE)
        else:
            from dynastore.modules.edr.output.geojson import write_position_geojson

            gen = write_position_geojson(lon, lat, dt, band_names, values)
            return StreamingResponse(gen, media_type=_GEOJSON_MEDIA_TYPE)

    async def query_area(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        coords: str = Query(..., description="WKT POLYGON geometry"),
        z: Optional[str] = Query(None),
        datetime: Optional[str] = Query(None),
        parameter_name: Optional[str] = Query(None, alias="parameter-name"),
        crs: Optional[str] = Query(None),
        f: Optional[str] = Query("CoverageJSON"),
    ):
        """Extract a coverage subset within a polygon (area query)."""
        from dynastore.modules.edr.query_handlers.area import (
            extract_area_values,
            parse_wkt_polygon_bbox,
        )
        from dynastore.modules.edr.parameter_metadata import build_parameters

        try:
            bbox = parse_wkt_polygon_bbox(coords)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        fmt = _resolve_edr_format(f)
        if fmt != "covjson":
            raise HTTPException(
                status_code=415,
                detail="Area queries only support CoverageJSON output.",
            )
        await self._require_catalog_ready(catalog_id)
        item = await self._get_item_for_datetime(catalog_id, collection_id, datetime)
        if item is None:
            raise HTTPException(
                status_code=404,
                detail="No items found for the given query parameters.",
            )

        href = ogc_asset_href(item, error_detail="No asset href on EDR item.")
        requested_params = (
            [p.strip() for p in parameter_name.split(",")]
            if parameter_name
            else None
        )
        band_names = _resolve_band_names(item, requested_params)
        _, _, band_arrays = extract_area_values(href, bbox)

        parameters = build_parameters(item)
        if requested_params:
            parameters = {k: v for k, v in parameters.items() if k in requested_params}

        from dynastore.modules.edr.output.coveragejson import write_area_coveragejson

        gen = write_area_coveragejson(bbox, parameters, band_names, band_arrays)
        return StreamingResponse(gen, media_type=_COVJSON_MEDIA_TYPE)

    async def query_cube(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        bbox: str = Query(
            ..., description="Bounding box: minlon,minlat,maxlon,maxlat"
        ),
        z: Optional[str] = Query(None),
        datetime: Optional[str] = Query(None),
        parameter_name: Optional[str] = Query(None, alias="parameter-name"),
        crs: Optional[str] = Query(None),
        f: Optional[str] = Query("CoverageJSON"),
    ):
        """Extract a bounding-box coverage subset (cube query)."""
        from dynastore.modules.edr.query_handlers.area import extract_area_values
        from dynastore.modules.edr.query_handlers.cube import parse_cube_bbox
        from dynastore.modules.edr.parameter_metadata import build_parameters

        try:
            parsed_bbox = parse_cube_bbox(bbox)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        fmt = _resolve_edr_format(f)
        if fmt != "covjson":
            raise HTTPException(
                status_code=415,
                detail="Cube queries only support CoverageJSON output.",
            )
        await self._require_catalog_ready(catalog_id)
        item = await self._get_item_for_datetime(catalog_id, collection_id, datetime)
        if item is None:
            raise HTTPException(
                status_code=404,
                detail="No items found for the given query parameters.",
            )

        href = ogc_asset_href(item, error_detail="No asset href on EDR item.")
        requested_params = (
            [p.strip() for p in parameter_name.split(",")]
            if parameter_name
            else None
        )
        band_names = _resolve_band_names(item, requested_params)
        _, _, band_arrays = extract_area_values(href, parsed_bbox)

        parameters = build_parameters(item)
        if requested_params:
            parameters = {k: v for k, v in parameters.items() if k in requested_params}

        from dynastore.modules.edr.output.coveragejson import write_area_coveragejson

        gen = write_area_coveragejson(parsed_bbox, parameters, band_names, band_arrays)
        return StreamingResponse(gen, media_type=_COVJSON_MEDIA_TYPE)

    # ------------------------------------------------------------------
    # Locations
    # ------------------------------------------------------------------

    async def list_locations(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
    ) -> em.EDRLocations:
        """Return an empty FeatureCollection — named locations require explicit metadata."""
        return em.EDRLocations(features=[])

    async def get_location(
        self,
        catalog_id: str,
        collection_id: str,
        location_id: str,
        request: Request,
    ):
        raise HTTPException(
            status_code=404,
            detail=f"Location {location_id!r} not found.",
        )

    # ------------------------------------------------------------------
    # Internal item access helpers
    # ------------------------------------------------------------------

    async def _get_item_for_datetime(
        self,
        catalog_id: str,
        collection_id: str,
        datetime_param: Optional[str],
    ) -> Optional[dict]:
        from dynastore.models.query_builder import FilterCondition, QueryRequest
        from dynastore.modules.edr.temporal import parse_datetime_param

        filters: List[FilterCondition] = []
        if datetime_param:
            start, end = parse_datetime_param(datetime_param)
            if start and end and start == end:
                filters.append(
                    FilterCondition(field="datetime", operator="eq", value=start)
                )
            else:
                if start:
                    filters.append(
                        FilterCondition(field="datetime", operator="gte", value=start)
                    )
                if end:
                    filters.append(
                        FilterCondition(field="datetime", operator="lte", value=end)
                    )

        catalogs = cast(CatalogsProtocol, await self._get_catalogs_service())
        try:
            features = await catalogs.search_items(
                catalog_id,
                collection_id,
                QueryRequest(limit=1, filters=filters),
            )
        except Exception:
            return None
        if not features:
            return None
        first = features[0]
        if hasattr(first, "model_dump"):
            return first.model_dump(by_alias=True, exclude_none=True)
        return dict(first)
