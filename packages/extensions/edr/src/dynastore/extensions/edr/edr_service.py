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

"""OGC API - Environmental Data Retrieval (EDR) extension for DynaStore.

Reuses the coverages GDAL VSI pipeline (open_raster_vsi, resolve_window,
read_window_iter) for spatial extraction. EDR adds temporal context and
the position/area/cube query vocabulary on top.
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, FrozenSet, List, Optional, cast

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse

from dynastore.extensions.ogc_base import OGCServiceMixin, ogc_asset_href
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.fast_api import AppJSONResponse as JSONResponse
from dynastore.extensions.tools.language_utils import get_language
from dynastore.extensions.tools.query import parse_hints_param
from dynastore.extensions.tools.response_i18n import localize_model
from dynastore.extensions.tools.url import get_root_url
from dynastore.extensions.web.decorators import expose_static, expose_web_page
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


def _get_edr_locations(coll: Any) -> List[Dict[str, Any]]:
    """Extract EDR locations from collection extras.

    Locations are stored under ``edr:locations`` key in collection extras.
    Each location is a GeoJSON Feature with:
    - ``id``: unique identifier (e.g., station code, GeoHash)
    - ``geometry``: POINT geometry
    - ``properties``: optional metadata (name, description, etc.)

    Returns an empty list if no locations are defined.
    """
    raw_extra = getattr(coll, "model_extra", None) or {}
    extras = raw_extra.get("extras") or {}
    if not extras and isinstance(raw_extra, dict):
        extras = {k: v for k, v in raw_extra.items() if ":" in k}
    if not extras:
        extra_metadata = getattr(coll, "extra_metadata", None) or {}
        dump = getattr(extra_metadata, "model_dump", None)
        if callable(dump):
            extra_metadata = dump()
        if isinstance(extra_metadata, dict):
            extras = extra_metadata

    locations = extras.get("edr:locations") or []
    if not isinstance(locations, list):
        return []
    return [loc for loc in locations if isinstance(loc, dict) and loc.get("id")]


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
            "/catalogs",
            self.list_catalogs,
            methods=["GET"],
            summary="List catalogs available to the EDR service",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections",
            self.list_collections,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self.get_collection,
            methods=["GET"],
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

    async def get_landing_page(
        self, request: Request, language: str = Depends(get_language)
    ) -> JSONResponse:
        # Build an EDRLandingPage (which applies EDR-specific title/description
        # defaults) and return it localized to the requested language.
        from dynastore.models.shared_models import Link as _Link

        root_url = get_root_url(request)
        landing = em.EDRLandingPage(
            links=[
                _Link(
                    href=f"{root_url}{self.prefix}/",
                    rel="self",
                    type="application/json",
                    title="This document",  # type: ignore[arg-type]
                ),
                _Link(
                    href=f"{root_url}{self.prefix}/conformance",
                    rel="conformance",
                    type="application/json",
                    title="Conformance classes",  # type: ignore[arg-type]
                ),
                _Link(
                    href=f"{root_url}/api",
                    rel="service-doc",
                    type="application/json",
                    title="API documentation",  # type: ignore[arg-type]
                ),
            ],
        )
        return JSONResponse(content=localize_model(landing, language))

    async def get_conformance(self, request: Request) -> em.Conformance:
        return await self.ogc_conformance_handler(request)

    # ------------------------------------------------------------------
    # Collections
    # ------------------------------------------------------------------

    async def list_catalogs(
        self,
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ):
        """List catalogs available to the EDR service (web-browser nav)."""
        catalogs_svc = await self._get_catalogs_service()
        catalogs = await catalogs_svc.list_catalogs(limit=limit, offset=offset)
        return {
            "catalogs": [
                {"id": getattr(c, "external_id", None) or c.id, "title": getattr(c, "title", None)}
                for c in (catalogs or [])
            ]
        }

    async def list_collections(
        self,
        catalog_id: str,
        request: Request,
        language: str = Depends(get_language),
    ) -> JSONResponse:
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
                col_dict = build_edr_collection(
                    catalog_id, col, base_url=base_url, language=language
                )
                items.append(em.EDRCollection(**col_dict))
            except Exception as exc:
                logger.warning(
                    "EDR: skipping collection %s: %s",
                    getattr(col, "id", "?"),
                    exc,
                )

        from dynastore.models.shared_models import Link

        result = em.EDRCollections(
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
        return JSONResponse(content=localize_model(result, language))

    async def get_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        language: str = Depends(get_language),
    ) -> JSONResponse:
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

        col_dict = build_edr_collection(
            catalog_id, collection, base_url=base_url, language=language
        )

        item = await self._get_first_item(catalog_id, collection_id)
        if item:
            from dynastore.modules.edr.parameter_metadata import build_parameters

            col_dict["parameter_names"] = build_parameters(item)

        result = em.EDRCollection(**col_dict)
        return JSONResponse(content=localize_model(result, language))

    # ------------------------------------------------------------------
    # EDR query endpoints
    # ------------------------------------------------------------------

    async def query_position(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        coords: str = Query(..., description="WKT POINT geometry, e.g. POINT(lon lat)"),
        z: Optional[str] = Query(None, description="Vertical level or range (e.g. '850' or '850/1000')"),
        datetime: Optional[str] = Query(None, description="ISO 8601 instant or interval"),
        parameter_name: Optional[str] = Query(None, alias="parameter-name"),
        crs: Optional[str] = Query(None, description="Output CRS (default CRS84)"),
        f: Optional[str] = Query("CoverageJSON", description="Output format"),
    ):
        """Extract values at a geographic point (position query)."""
        from dynastore.modules.edr.query_handlers.position import (
            extract_point_values,
            get_raster_crs,
            parse_wkt_point,
        )
        from dynastore.modules.edr.parameter_metadata import (
            _select_bands,
            build_parameters,
            filter_parameters,
        )
        from dynastore.modules.edr.vertical import parse_z_param, select_bands_by_z
        from dynastore.modules.edr.crs import (
            parse_crs_param,
            validate_crs,
            transform_point,
            DEFAULT_OUTPUT_CRS,
        )

        try:
            lon, lat = parse_wkt_point(coords)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        try:
            output_crs = parse_crs_param(crs)
            if output_crs:
                validate_crs(output_crs)
            else:
                output_crs = DEFAULT_OUTPUT_CRS
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        try:
            z_low, z_high = parse_z_param(z)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        fmt = _resolve_edr_format(f)
        await self._require_catalog_ready(catalog_id)
        await self._require_collection_visible(catalog_id, collection_id)
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

        bands = _select_bands(item)
        if requested_params:
            bands = filter_parameters(bands, requested_params)
        band_names = [b.get("name", f"band_{i + 1}") for i, b in enumerate(bands)] if bands else ["value"]

        z_bands = select_bands_by_z(bands, z_low, z_high) if bands else None

        raster_crs = get_raster_crs(href)

        if raster_crs and output_crs != DEFAULT_OUTPUT_CRS:
            try:
                out_lon, out_lat = transform_point(lon, lat, raster_crs, output_crs)
            except Exception as exc:
                logger.warning("CRS transformation failed, using native coordinates: %s", exc)
                out_lon, out_lat = lon, lat
        else:
            out_lon, out_lat = lon, lat

        values = extract_point_values(href, lon, lat, z_bands)

        z_level = z_low if z_low == z_high else None

        dt = _item_datetime(item)
        parameters = build_parameters(item)
        if requested_params:
            parameters = {k: v for k, v in parameters.items() if k in requested_params}

        if fmt == "covjson":
            from dynastore.modules.edr.output.coveragejson import write_position_coveragejson

            gen = write_position_coveragejson(
                out_lon, out_lat, dt, parameters, values, band_names, output_crs, z_level
            )
            return StreamingResponse(gen, media_type=_COVJSON_MEDIA_TYPE)
        else:
            from dynastore.modules.edr.output.geojson import write_position_geojson

            gen = write_position_geojson(out_lon, out_lat, dt, band_names, values)
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
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        """Extract a coverage subset within a polygon (area query).

        ``?hints=`` is accepted uniformly (e.g. ``hints=geometry_exact``) but
        reserved for forward-compatible routing — EDR area data is read from a
        raster asset directly and does not flow through a hints-capable vector
        read seam, so the value has no effect on this route today.
        """
        from dynastore.modules.edr.query_handlers.area import (
            extract_area_values,
            parse_wkt_polygon_bbox,
        )
        from dynastore.modules.edr.parameter_metadata import (
            _select_bands,
            build_parameters,
            filter_parameters,
        )
        from dynastore.modules.edr.vertical import parse_z_param, select_bands_by_z
        from dynastore.modules.edr.crs import (
            parse_crs_param,
            validate_crs,
            DEFAULT_OUTPUT_CRS,
        )

        try:
            bbox = parse_wkt_polygon_bbox(coords)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        try:
            output_crs = parse_crs_param(crs)
            if output_crs:
                validate_crs(output_crs)
            else:
                output_crs = DEFAULT_OUTPUT_CRS
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        try:
            z_low, z_high = parse_z_param(z)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        fmt = _resolve_edr_format(f)
        if fmt != "covjson":
            raise HTTPException(
                status_code=415,
                detail="Area queries only support CoverageJSON output.",
            )
        await self._require_catalog_ready(catalog_id)
        await self._require_collection_visible(catalog_id, collection_id)
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

        bands = _select_bands(item)
        if requested_params:
            bands = filter_parameters(bands, requested_params)
        band_names = [b.get("name", f"band_{i + 1}") for i, b in enumerate(bands)] if bands else ["value"]

        z_bands = select_bands_by_z(bands, z_low, z_high) if bands else None

        _, _, band_arrays = extract_area_values(href, bbox, z_bands)

        parameters = build_parameters(item)
        if requested_params:
            parameters = {k: v for k, v in parameters.items() if k in requested_params}

        from dynastore.modules.edr.output.coveragejson import write_area_coveragejson

        gen = write_area_coveragejson(bbox, parameters, band_names, band_arrays, output_crs)
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
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        """Extract a bounding-box coverage subset (cube query).

        ``?hints=`` is accepted uniformly (e.g. ``hints=geometry_exact``) but
        reserved for forward-compatible routing — EDR cube data is read from a
        raster asset directly and does not flow through a hints-capable vector
        read seam, so the value has no effect on this route today.
        """
        from dynastore.modules.edr.query_handlers.area import extract_area_values
        from dynastore.tools.geospatial import parse_bbox_string, BboxDimensionality
        from dynastore.modules.edr.parameter_metadata import (
            _select_bands,
            build_parameters,
            filter_parameters,
        )
        from dynastore.modules.edr.vertical import parse_z_param, select_bands_by_z
        from dynastore.modules.edr.crs import (
            parse_crs_param,
            validate_crs,
            DEFAULT_OUTPUT_CRS,
        )

        try:
            parsed_bbox = parse_bbox_string(
                bbox,
                dimensionality=BboxDimensionality.ALLOW_EXTRA_DIMS,
                allow_none=False,
                validate_geometry=True,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        try:
            output_crs = parse_crs_param(crs)
            if output_crs:
                validate_crs(output_crs)
            else:
                output_crs = DEFAULT_OUTPUT_CRS
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        try:
            z_low, z_high = parse_z_param(z)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        fmt = _resolve_edr_format(f)
        if fmt != "covjson":
            raise HTTPException(
                status_code=415,
                detail="Cube queries only support CoverageJSON output.",
            )
        await self._require_catalog_ready(catalog_id)
        await self._require_collection_visible(catalog_id, collection_id)
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

        bands = _select_bands(item)
        if requested_params:
            bands = filter_parameters(bands, requested_params)
        band_names = [b.get("name", f"band_{i + 1}") for i, b in enumerate(bands)] if bands else ["value"]

        z_bands = select_bands_by_z(bands, z_low, z_high) if bands else None

        _, _, band_arrays = extract_area_values(href, parsed_bbox, z_bands)

        parameters = build_parameters(item)
        if requested_params:
            parameters = {k: v for k, v in parameters.items() if k in requested_params}

        from dynastore.modules.edr.output.coveragejson import write_area_coveragejson

        gen = write_area_coveragejson(parsed_bbox, parameters, band_names, band_arrays, output_crs)
        return StreamingResponse(gen, media_type=_COVJSON_MEDIA_TYPE)

    # ------------------------------------------------------------------
    # Locations
    # ------------------------------------------------------------------

    async def list_locations(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        request_hints: FrozenSet = Depends(parse_hints_param),
    ) -> em.EDRLocations:
        """Return named locations from collection extras.

        Locations are stored in collection extras under the ``edr:locations``
        key as a list of GeoJSON Feature objects. Each feature represents a
        named position (e.g., weather station, monitoring site) with an ``id``,
        ``geometry`` (POINT), and optional ``properties``.

        ``?hints=`` is accepted uniformly for API consistency; this route
        performs no data reads so the value has no effect today.
        """
        catalogs_svc = await self._get_catalogs_service()
        coll = await catalogs_svc.get_collection(catalog_id, collection_id)
        if coll is None:
            raise HTTPException(
                status_code=404,
                detail=f"Collection {collection_id!r} not found.",
            )

        locations = _get_edr_locations(coll)
        return em.EDRLocations(features=locations)

    async def get_location(
        self,
        catalog_id: str,
        collection_id: str,
        location_id: str,
        request: Request,
    ):
        """Return a specific named location."""
        catalogs_svc = await self._get_catalogs_service()
        coll = await catalogs_svc.get_collection(catalog_id, collection_id)
        if coll is None:
            raise HTTPException(
                status_code=404,
                detail=f"Collection {collection_id!r} not found.",
            )

        locations = _get_edr_locations(coll)
        for loc in locations:
            if loc.get("id") == location_id:
                return JSONResponse(content=loc)

        raise HTTPException(
            status_code=404,
            detail=f"Location {location_id!r} not found.",
        )

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

    @expose_static("edr")
    def provide_static_files(self) -> list:
        """Exposes the internal static directory for the EDR browser."""
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        files = []
        for root, _, filenames in os.walk(static_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    @expose_web_page(
        page_id="edr_browser",
        title="EDR Browser",
        icon="fa-magnifying-glass-location",
        description="Query environmental data by position and time.",
    )
    async def provide_edr_browser(self, request: Request):
        return await self._serve_page_template("edr_browser.html")

    async def _serve_page_template(self, filename: str):
        from dynastore._version import VERSION
        file_path = os.path.join(os.path.dirname(__file__), "static", filename)
        if not os.path.exists(file_path):
            return Response(content=f"Template {filename} not found", status_code=404)
        with open(file_path, "r", encoding="utf-8") as f:
            return Response(content=f.read().replace("{{VERSION}}", VERSION), media_type="text/html")

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
