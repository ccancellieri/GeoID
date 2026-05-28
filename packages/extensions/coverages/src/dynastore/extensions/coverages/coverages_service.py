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

"""OGC API - Coverages extension for DynaStore.

Demonstrates the OGCServiceMixin architecture: a new OGC protocol
extension requires only a service class with routes, conformance URIs,
and protocol-specific response models. Zero core changes needed.
"""

import logging
from contextlib import asynccontextmanager
from typing import Optional

import rasterio as _rasterio_scope_gate  # noqa: F401  # SCOPE gate: extension_coverages requires rasterio
_ = _rasterio_scope_gate  # silence pyright "unused" — load-bearing for SCOPE filtering

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from dynastore.extensions.coverages.config import CoveragesConfig
from dynastore.extensions.coverages.links import build_coverage_links
from dynastore.extensions.ogc_base import OGCServiceMixin, ogc_asset_href
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.url import get_root_url
from dynastore.modules.coverages.domainset import build_domainset
from dynastore.modules.coverages.rangetype import build_rangetype
from dynastore.modules.coverages.subset import parse_subset
from dynastore.modules.coverages.writers import MEDIA_TYPE_FOR

from . import coverages_models as cm


def _stream_coverage_geotiff(href: str, subset):
    """Stream a GeoTIFF response by reading the source raster at ``href``.

    Imports rasterio lazily so the helper remains importable without it.
    """
    from dynastore.modules.coverages.reader import read_window_iter
    from dynastore.modules.coverages.window import RasterGeoRef, resolve_window
    from dynastore.modules.coverages.writers.geotiff import write_geotiff
    from dynastore.modules.gdal.service import open_raster_vsi
    from rasterio.transform import from_origin

    req = parse_subset(subset)
    ds = open_raster_vsi(href)
    try:
        t = ds.transform
        ref = RasterGeoRef(
            width=ds.width, height=ds.height,
            origin_x=t.c, origin_y=t.f,
            pixel_x=t.a, pixel_y=t.e,
            crs=str(ds.crs),
            axis_order=("Lon", "Lat"),
        )
        box = resolve_window(req, ref)
        out_transform = from_origin(
            ref.origin_x + ref.pixel_x * box.col_off,
            ref.origin_y + ref.pixel_y * box.row_off,
            ref.pixel_x, -ref.pixel_y,
        )
        tiles = ((0, 0, block) for block in read_window_iter(ds, box))
        yield from write_geotiff(
            width=box.width, height=box.height,
            transform=out_transform, crs=ds.crs,
            dtype=str(ds.dtypes[0]), band_count=1,
            tiles=tiles,
        )
    finally:
        ds.close()


def _stream_coverage_netcdf(href: str, subset, *, band_names: list):
    """Stream a NetCDF-4 response by reading the source raster at ``href``."""
    from dynastore.modules.coverages.reader import read_window_iter
    from dynastore.modules.coverages.window import RasterGeoRef, resolve_window
    from dynastore.modules.coverages.writers.netcdf import write_netcdf
    from dynastore.modules.gdal.service import open_raster_vsi

    req = parse_subset(subset)
    ds = open_raster_vsi(href)
    try:
        t = ds.transform
        ref = RasterGeoRef(
            width=ds.width, height=ds.height,
            origin_x=t.c, origin_y=t.f,
            pixel_x=t.a, pixel_y=t.e,
            crs=str(ds.crs),
            axis_order=("Lon", "Lat"),
        )
        box = resolve_window(req, ref)
        west = ref.origin_x + ref.pixel_x * box.col_off
        east = ref.origin_x + ref.pixel_x * (box.col_off + box.width)
        north = ref.origin_y + ref.pixel_y * box.row_off
        south = ref.origin_y + ref.pixel_y * (box.row_off + box.height)
        bbox = [west, min(south, north), east, max(south, north)]
        tiles = ((0, 0, block) for block in read_window_iter(ds, box))
        yield from write_netcdf(
            width=box.width, height=box.height,
            bbox=bbox, crs=str(ds.crs),
            band_names=band_names, tiles=tiles,
        )
    finally:
        ds.close()


def _stream_coverage_zarr(href: str, subset, *, band_names: list, chunk_size: int = 256):
    """Stream a ZIP-wrapped Zarr response by reading the source raster at ``href``."""
    from dynastore.modules.coverages.reader import read_window_iter
    from dynastore.modules.coverages.window import RasterGeoRef, resolve_window
    from dynastore.modules.coverages.writers.zarr import write_zarr
    from dynastore.modules.gdal.service import open_raster_vsi

    req = parse_subset(subset)
    ds = open_raster_vsi(href)
    try:
        t = ds.transform
        ref = RasterGeoRef(
            width=ds.width, height=ds.height,
            origin_x=t.c, origin_y=t.f,
            pixel_x=t.a, pixel_y=t.e,
            crs=str(ds.crs),
            axis_order=("Lon", "Lat"),
        )
        box = resolve_window(req, ref)
        west = ref.origin_x + ref.pixel_x * box.col_off
        east = ref.origin_x + ref.pixel_x * (box.col_off + box.width)
        north = ref.origin_y + ref.pixel_y * box.row_off
        south = ref.origin_y + ref.pixel_y * (box.row_off + box.height)
        bbox = [west, min(south, north), east, max(south, north)]
        tiles = ((0, 0, block) for block in read_window_iter(ds, box))
        yield from write_zarr(
            width=box.width, height=box.height,
            bbox=bbox, crs=str(ds.crs),
            band_names=band_names, tiles=tiles,
            chunk_size=chunk_size,
        )
    finally:
        ds.close()


def _resolve_format(f) -> str:
    if f is None:
        return "geotiff"
    v = f.lower()
    if v not in MEDIA_TYPE_FOR:
        raise HTTPException(status_code=415, detail=f"Unsupported coverage format: {f!r}")
    return v


def _extract_domainset(item: Optional[dict]) -> dict:
    ds = build_domainset(item) if item is not None else None
    if ds is None:
        raise HTTPException(status_code=404, detail="No coverage item found.")
    return ds


def _extract_rangetype(item: Optional[dict]) -> dict:
    rt = build_rangetype(item) if item is not None else None
    if rt is None:
        raise HTTPException(status_code=404, detail="No coverage item found.")
    return rt


def _resolve_default_style_for_coverage(*, config, item):
    """Pass 2 precedence: CoveragesConfig.default_style_id > STAC item-assets default > None."""
    if getattr(config, "default_style_id", None):
        return config.default_style_id
    if item is None:
        return None
    default = (item.get("assets") or {}).get("default_style") or {}
    return default.get("id")


def _build_metadata_response(
    *,
    item: dict,
    base_url: str,
    catalog_id: str,
    collection_id: str,
    default_style_id: Optional[str],
) -> dict:
    """Assemble the /coverage/metadata payload for a given STAC-ish item dict."""
    return {
        "title": item.get("id"),
        "extent": {"spatial": {"bbox": [item.get("bbox", [])]}},
        "domainset": build_domainset(item),
        "rangetype": build_rangetype(item),
        "links": build_coverage_links(
            base_url=base_url,
            catalog_id=catalog_id,
            collection_id=collection_id,
            default_style_id=default_style_id,
        ),
    }

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OGC API - Coverages conformance URIs (OGC 19-087r6)
# ---------------------------------------------------------------------------

OGC_API_COVERAGES_URIS = [
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/geodata-coverage",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/json",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/html",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/coverage-subset",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/coverage-bbox",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/coverage-datetime",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/geotiff",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/netcdf",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/coveragejson",
]


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class CoveragesService(ExtensionProtocol, OGCServiceMixin):
    """OGC API - Coverages extension.

    Priority 160 — after Records (150), before Dimensions (200).
    """

    priority: int = 160
    router: APIRouter

    # OGCServiceMixin class attributes
    conformance_uris = OGC_API_COVERAGES_URIS
    prefix = "/coverages"
    protocol_title = "DynaStore OGC API - Coverages"
    protocol_description = "Access to coverage data via OGC API - Coverages"

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self.router = APIRouter(prefix="/coverages", tags=["OGC API - Coverages"])
        self._register_routes()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        logger.info("CoveragesService: policies registered.")
        yield

    def get_notebooks(self):
        try:
            from .notebooks import build_contributions
        except Exception:
            return []
        return build_contributions()

    # ------------------------------------------------------------------
    # Route registration
    # ------------------------------------------------------------------

    def _register_routes(self) -> None:
        self.router.add_api_route(
            "/",
            self.get_landing_page,
            methods=["GET"],
            response_model=cm.CoveragesLandingPage,
        )
        self.router.add_api_route(
            "/conformance",
            self.get_conformance,
            methods=["GET"],
            response_model=cm.Conformance,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/coverage",
            self.get_coverage,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/coverage/metadata",
            self.get_coverage_metadata,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/coverage/domainset",
            self.get_coverage_domainset,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/coverage/rangetype",
            self.get_coverage_rangetype,
            methods=["GET"],
        )

    # ------------------------------------------------------------------
    # Landing page & conformance (delegated to OGCServiceMixin)
    # ------------------------------------------------------------------

    async def get_landing_page(self, request: Request):
        return await self.ogc_landing_page_handler(request)

    async def get_conformance(self, request: Request) -> cm.Conformance:
        return await self.ogc_conformance_handler(request)

    # ------------------------------------------------------------------
    # Coverage endpoints (stubs — to be implemented per data model)
    # ------------------------------------------------------------------

    async def get_coverage(
        self,
        catalog_id: str,
        collection_id: str,
        subset: Optional[str] = Query(None),
        f: Optional[str] = Query("geotiff"),
    ):
        """Stream a coverage by content-negotiated format with optional subset."""
        fmt = _resolve_format(f)
        item = await self._get_first_item(catalog_id, collection_id)
        if item is None:
            raise HTTPException(status_code=404, detail="No coverage item found.")
        href = ogc_asset_href(item, error_detail="No asset href on coverage item.")
        if fmt == "geotiff":
            gen = _stream_coverage_geotiff(href, subset=subset)
        elif fmt == "covjson":
            from dynastore.modules.coverages.writers.coveragejson import (
                write_coveragejson,
            )
            ds = build_domainset(item) or {}
            rt = build_rangetype(item) or {"type": "DataRecord", "field": []}
            gen = write_coveragejson(ds, rt, iter([]))
        elif fmt == "netcdf":
            rt = build_rangetype(item) or {"type": "DataRecord", "field": []}
            band_names = [f["name"] for f in rt.get("field", [])] or ["band"]
            gen = _stream_coverage_netcdf(href, subset=subset, band_names=band_names)
        elif fmt == "zarr":
            rt = build_rangetype(item) or {"type": "DataRecord", "field": []}
            band_names = [f["name"] for f in rt.get("field", [])] or ["band"]
            gen = _stream_coverage_zarr(href, subset=subset, band_names=band_names)
        else:  # pragma: no cover - guarded by _resolve_format above
            raise HTTPException(status_code=415, detail=f"Unsupported format: {fmt!r}")
        return StreamingResponse(gen, media_type=MEDIA_TYPE_FOR[fmt])

    async def get_coverage_domainset(
        self, catalog_id: str, collection_id: str,
    ) -> dict:
        """Return the OGC Coverages DomainSet derived from the first item."""
        item = await self._get_first_item(catalog_id, collection_id)
        return _extract_domainset(item)

    async def get_coverage_rangetype(
        self, catalog_id: str, collection_id: str,
    ) -> dict:
        """Return the OGC Coverages RangeType derived from the first item."""
        item = await self._get_first_item(catalog_id, collection_id)
        return _extract_rangetype(item)

    async def get_coverage_metadata(
        self, catalog_id: str, collection_id: str, request: Request,
    ):
        item = await self._get_first_item(catalog_id, collection_id)
        if item is None:
            raise HTTPException(status_code=404, detail="No coverage item found.")
        cfg = await self._get_plugin_config(CoveragesConfig, catalog_id, collection_id)
        return _build_metadata_response(
            item=item,
            base_url=get_root_url(request).rstrip("/"),
            catalog_id=catalog_id,
            collection_id=collection_id,
            default_style_id=_resolve_default_style_for_coverage(config=cfg, item=item),
        )

    async def _build_domain(self, catalog_id: str, collection_id: str) -> dict:
        """Build a CoverageJSON domain dict from collection extent."""
        catalogs = await self._get_catalogs_service()
        collection = await catalogs.get_collection(catalog_id, collection_id)
        domain = {"type": "Domain", "domainType": "Grid", "axes": {}}

        if collection and collection.extent:
            extent_dict = collection.extent.model_dump(exclude_none=True)
            spatial = extent_dict.get("spatial") or {}
            if spatial.get("bbox"):
                bbox = spatial["bbox"]
                first_bbox = bbox[0] if isinstance(bbox[0], list) else bbox
                domain["axes"]["x"] = {"values": [first_bbox[0], first_bbox[2]]}
                domain["axes"]["y"] = {"values": [first_bbox[1], first_bbox[3]]}

        return domain
