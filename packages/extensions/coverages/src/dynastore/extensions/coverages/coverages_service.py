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

"""OGC API - Coverages extension for DynaStore.

Demonstrates the OGCServiceMixin architecture: a new OGC protocol
extension requires only a service class with routes, conformance URIs,
and protocol-specific response models. Zero core changes needed.
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Any, FrozenSet, Optional, Tuple

import rasterio as _rasterio_scope_gate  # noqa: F401  # SCOPE gate: extension_coverages requires rasterio
_ = _rasterio_scope_gate  # silence pyright "unused" — load-bearing for SCOPE filtering

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from dynastore.extensions.web.decorators import expose_web_page

from dynastore.extensions.coverages.config import CoveragesConfig
from dynastore.extensions.coverages.links import build_coverage_links
from dynastore.extensions.ogc_base import OGCServiceMixin, ogc_asset_href
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.language_utils import get_language
from dynastore.extensions.tools.query import parse_hints_param  # noqa: E402
from dynastore.extensions.tools.response_i18n import localize_response_dict  # noqa: E402
from dynastore.extensions.tools.url import get_root_url
from dynastore.modules.coverages.domainset import build_domainset
from dynastore.modules.coverages.rangetype import build_rangetype
from dynastore.modules.coverages.subset import AxisRange, SubsetRequest, parse_subset
from dynastore.modules.coverages.writers import MEDIA_TYPE_FOR
from dynastore.tools.geospatial import BboxDimensionality, parse_bbox_string

from . import coverages_models as cm


def _bbox_to_subset(bbox: str) -> SubsetRequest:
    """Parse OGC bbox query param and convert to a SubsetRequest.

    OGC API - Coverages /req/subsetting-spatial (bbox is folded into spatial
    subsetting in the current draft): the ``bbox`` query parameter follows the
    OGC API Common Part 2 convention:
      ``minlon,minlat,maxlon,maxlat``
    which maps directly to Lon(minlon:maxlon),Lat(minlat:maxlat) subset axes.
    """
    from fastapi import HTTPException
    try:
        parsed_bbox = parse_bbox_string(
            bbox,
            dimensionality=BboxDimensionality.STRICT_2D,
            allow_none=False,
            validate_geometry=False,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    assert parsed_bbox is not None  # allow_none=False guarantees this
    minlon, minlat, maxlon, maxlat = parsed_bbox
    if minlon > maxlon or minlat > maxlat:
        raise HTTPException(
            status_code=400,
            detail="bbox: min values must be <= max values.",
        )
    return SubsetRequest(axes=[
        AxisRange("Lon", minlon, maxlon),
        AxisRange("Lat", minlat, maxlat),
    ])


def _merge_subset_and_bbox(
    subset: Optional[str], bbox: Optional[str]
) -> Optional[str]:
    """Merge ``subset`` and ``bbox`` parameters into a single ``subset`` string.

    When both are given, bbox is converted to subset axes and appended; axes
    appearing in both are rejected with 400 (ambiguous constraint, per
    OGC 19-087 §7.8).
    """
    from fastapi import HTTPException
    if bbox is None:
        return subset

    bbox_req = _bbox_to_subset(bbox)
    if subset is None:
        # Serialise the bbox SubsetRequest back to the wire format so that
        # the downstream parse_subset call in each writer handles it uniformly.
        parts = [
            f"{ar.axis}({ar.low}:{ar.high})" for ar in bbox_req.axes
        ]
        return ",".join(parts)

    # Both present: parse the explicit subset and check for axis collisions.
    explicit_req = parse_subset(subset)
    explicit_axes = {ar.axis.lower() for ar in explicit_req.axes}
    bbox_axes = {ar.axis.lower() for ar in bbox_req.axes}
    overlap = explicit_axes & bbox_axes
    if overlap:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Axis conflict: {sorted(overlap)} appear in both "
                "the 'subset' and 'bbox' parameters. Use one or the other."
            ),
        )
    combined = explicit_req.axes + bbox_req.axes
    parts = [f"{ar.axis}({ar.low}:{ar.high})" for ar in combined]
    return ",".join(parts)


def _resolve_scale(
    *,
    box_width: int,
    box_height: int,
    scale_factor: Optional[float],
    scale_size: Optional[str],
) -> Tuple[int, int]:
    """Resolve (out_width, out_height) from scale-factor or scale-size.

    OGC 19-087 §7.11 /req/scale-factor: ``scale-factor`` is a positive real
    number; the output grid size is floor(native * scale-factor) in each axis.
    OGC 19-087 §7.12 /req/scale-size: ``scale-size`` is a comma-separated
    ``AxisLabel(n)`` list; the named axes are resampled to the given pixel count.
    Only Lon/Lat (X/Y) axes are supported; Time is not.

    Returns ``(out_width, out_height)`` for use with rasterio ``out_shape``.
    Raises 400 on conflicting or invalid params.
    """
    from fastapi import HTTPException
    import math

    if scale_factor is not None and scale_size is not None:
        raise HTTPException(
            status_code=400,
            detail="Use either scale-factor or scale-size, not both.",
        )

    if scale_factor is not None:
        if scale_factor <= 0:
            raise HTTPException(
                status_code=400,
                detail="scale-factor must be a positive number.",
            )
        out_w = max(1, int(math.floor(box_width * scale_factor)))
        out_h = max(1, int(math.floor(box_height * scale_factor)))
        return out_w, out_h

    if scale_size is not None:
        out_w = box_width
        out_h = box_height
        # Format: AxisLabel1(n1),AxisLabel2(n2)
        import re
        for token in scale_size.split(","):
            m = re.match(r"^([A-Za-z][A-Za-z0-9_]*)\((\d+)\)$", token.strip())
            if not m:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"scale-size token {token!r} is not in 'AxisLabel(n)' form."
                    ),
                )
            label, n = m.group(1).lower(), int(m.group(2))
            if n < 1:
                raise HTTPException(
                    status_code=400, detail=f"scale-size: {m.group(1)} size must be >= 1."
                )
            if label in {"lon", "longitude", "x", "e", "east"}:
                out_w = n
            elif label in {"lat", "latitude", "y", "n", "north"}:
                out_h = n
            else:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"scale-size: unsupported axis '{m.group(1)}'. "
                        "Only Lon/Lat (X/Y) axes are supported."
                    ),
                )
        return out_w, out_h

    return box_width, box_height


def _stream_coverage_geotiff(
    href: str,
    subset,
    *,
    media_type: str = "",
    scale_factor: Optional[float] = None,
    scale_size: Optional[str] = None,
):
    """Stream a GeoTIFF response by reading the source raster at ``href``.

    ``media_type`` is the source asset's declared STAC media type — it
    selects the reader via the format->reader registry
    (:func:`dynastore.modules.coverages.reader.open_coverage`), not the
    output encoding. ``scale_factor`` (OGC 19-087 §7.11) and ``scale_size``
    (§7.12) control output resolution. Imports rasterio lazily so the
    helper remains importable without it.
    """
    from dynastore.modules.coverages.reader import open_coverage, read_scaled
    from dynastore.modules.coverages.window import resolve_window
    from dynastore.modules.coverages.writers.geotiff import write_geotiff
    from rasterio.transform import from_bounds

    req = parse_subset(subset)
    with open_coverage(href, media_type) as (ds, ref):
        box = resolve_window(req, ref)
        out_w, out_h = _resolve_scale(
            box_width=box.width, box_height=box.height,
            scale_factor=scale_factor, scale_size=scale_size,
        )
        west = ref.origin_x + ref.pixel_x * box.col_off
        east = ref.origin_x + ref.pixel_x * (box.col_off + box.width)
        north = ref.origin_y + ref.pixel_y * box.row_off
        south = ref.origin_y + ref.pixel_y * (box.row_off + box.height)
        out_transform = from_bounds(
            min(west, east), min(south, north),
            max(west, east), max(south, north),
            out_w, out_h,
        )
        arr = read_scaled(ds, box, band=1, out_shape=(out_h, out_w))
        tiles = ((0, 0, arr),)
        yield from write_geotiff(
            width=out_w, height=out_h,
            transform=out_transform, crs=ds.crs,
            dtype=str(ds.dtypes[0]), band_count=1,
            tiles=tiles,
        )


def _stream_coverage_netcdf(href: str, subset, *, band_names: list, media_type: str = ""):
    """Stream a NetCDF-4 response by reading the source raster at ``href``."""
    from dynastore.modules.coverages.reader import open_coverage, read_window_iter
    from dynastore.modules.coverages.window import resolve_window
    from dynastore.modules.coverages.writers.netcdf import write_netcdf

    req = parse_subset(subset)
    with open_coverage(href, media_type) as (ds, ref):
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


def _stream_coverage_zarr(
    href: str, subset, *, band_names: list, media_type: str = "", chunk_size: int = 256
):
    """Stream a ZIP-wrapped Zarr response by reading the source raster at ``href``."""
    from dynastore.modules.coverages.reader import open_coverage, read_window_iter
    from dynastore.modules.coverages.window import resolve_window
    from dynastore.modules.coverages.writers.zarr import write_zarr

    req = parse_subset(subset)
    with open_coverage(href, media_type) as (ds, ref):
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


def _read_coverage_values(href: str, subset, rangetype: dict, *, media_type: str = ""):
    """Yield one 2-D array per band for a CoverageJSON response.

    Reads the raster at ``href`` restricted to ``subset``, one band per
    field declared in ``rangetype``.  Each yielded item is a list-of-lists
    ``[[row0_col0, row0_col1, ...], [row1_col0, ...], ...]`` as expected by
    :func:`write_coveragejson`.
    """
    from dynastore.modules.coverages.reader import open_coverage, read_scaled
    from dynastore.modules.coverages.window import resolve_window

    req = parse_subset(subset)
    with open_coverage(href, media_type) as (ds, ref):
        box = resolve_window(req, ref)
        n_bands = ds.count
        field_count = len(rangetype.get("field", []))
        bands_to_read = range(1, min(n_bands, field_count) + 1) if field_count else range(1, n_bands + 1)
        for band_idx in bands_to_read:
            arr = read_scaled(ds, box, band=band_idx)
            yield arr.tolist()


def _resolve_format(f) -> str:
    if f is None:
        return "geotiff"
    v = f.lower()
    if v not in MEDIA_TYPE_FOR:
        raise HTTPException(status_code=415, detail=f"Unsupported coverage format: {f!r}")
    return v


def _resolve_coverage_asset(item: dict) -> Tuple[str, str]:
    """Return ``(href, media_type)`` for the item's coverage source asset.

    Reuses :func:`ogc_asset_href` for the href — same ``data``/``coverage``
    key preference and 404 behaviour — then looks up the declared STAC media
    type (``type``) of that same asset so the caller can pick a reader from
    the format->reader registry (:func:`dynastore.modules.coverages.reader.reader_for`).
    """
    href = ogc_asset_href(item, error_detail="No asset href on coverage item.")
    for asset in (item.get("assets") or {}).values():
        if asset.get("href") == href:
            return href, asset.get("type") or ""
    return href, ""


def _require_reader(media_type: str) -> None:
    """Validate a reader is registered for ``media_type``, else raise 415.

    Runs eagerly — before the streaming generator is built — so an
    unsupported source asset format fails fast with a clean response
    instead of surfacing mid-stream once headers are already sent.
    """
    from dynastore.modules.coverages.reader import UnsupportedReaderMediaType, reader_for
    try:
        reader_for(media_type)
    except UnsupportedReaderMediaType as exc:
        raise HTTPException(status_code=415, detail=str(exc)) from exc


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
    language: str = "en",
) -> dict:
    """Assemble the /coverage/metadata payload for a given STAC-ish item dict.

    ``language`` is used to resolve any LocalizedText values that may appear in
    the response dict (title, description) to a single language string.
    """
    data: dict = {
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
    return localize_response_dict(data, language)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OGC API - Coverages conformance URIs (OGC 19-087r6)
# ---------------------------------------------------------------------------

# OGC API - Coverages Part 1 is a DRAFT (OGC 19-087). Conformance URIs track
# the current draft's axis-typed taxonomy (subsetting-spatial / scaling-spatial);
# re-verify these slugs on each 19-087 revision. Only spatial classes are claimed
# because no temporal subsetting/scaling is implemented; general (arbitrary-axis)
# is not claimed either.
OGC_API_COVERAGES_URIS = [
    # /req/core — landing page, conformance, /coverage endpoint
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/core",
    # /req/geodata-coverage — collection-tied coverage resource
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/geodata-coverage",
    # /req/json — JSON-encoded coverage metadata responses
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/json",
    # /req/html — HTML landing page and navigation
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/html",
    # /req/subsetting-spatial — ?subset=Axis(low:high) and ?bbox= spatial trimming
    # (bbox is folded into spatial subsetting in the current draft taxonomy)
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/subsetting-spatial",
    # /req/scaling-spatial — ?scale-factor / ?scale-size spatial resampling
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/scaling-spatial",
    # /req/geotiff — GeoTIFF binary output
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/geotiff",
    # /req/netcdf — NetCDF-4 binary output
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/netcdf",
    # /req/coveragejson — CoverageJSON output with populated range
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
    landing_response_model = cm.CoveragesLandingPage

    # StaticPageMixin (folded into OGCServiceMixin) class attributes
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    static_prefix = "coverages"

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

    # get_web_pages / get_static_assets / get_notebooks / provide_static_files /
    # _serve_page_template are provided by OGCServiceMixin (static_dir /
    # static_prefix below opt this service into the default wiring).

    @expose_web_page(
        page_id="coverages_browser",
        title="Coverages Browser",
        icon="fa-layer-group",
        description="Inspect coverage axes and range types.",
    )
    async def provide_coverages_browser(self, request: Request):
        return await self._serve_page_template("coverages_browser.html")

    # ------------------------------------------------------------------
    # Route registration
    # ------------------------------------------------------------------

    def _register_routes(self) -> None:
        self.register_ogc_standard_routes()
        route_table: list[tuple[str, str, list[str], dict[str, Any]]] = [
            # Catalog / collection listing (drives the web browser's navigation)
            (
                "/catalogs",
                "list_catalogs",
                ["GET"],
                {"summary": "List catalogs available to the Coverages service"},
            ),
            (
                "/catalogs/{catalog_id}/collections",
                "list_collections",
                ["GET"],
                {"summary": "List collections in a catalog"},
            ),
            (
                "/catalogs/{catalog_id}/collections/{collection_id}/coverage",
                "get_coverage",
                ["GET"],
                {},
            ),
            (
                "/catalogs/{catalog_id}/collections/{collection_id}/coverage/metadata",
                "get_coverage_metadata",
                ["GET"],
                {},
            ),
            (
                "/catalogs/{catalog_id}/collections/{collection_id}/coverage/domainset",
                "get_coverage_domainset",
                ["GET"],
                {},
            ),
            (
                "/catalogs/{catalog_id}/collections/{collection_id}/coverage/rangetype",
                "get_coverage_rangetype",
                ["GET"],
                {},
            ),
        ]
        for path, handler_name, methods, kwargs in route_table:
            self.router.add_api_route(path, getattr(self, handler_name), methods=methods, **kwargs)

    # ------------------------------------------------------------------
    # Catalog / collection listing (web-browser navigation)
    # ------------------------------------------------------------------

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
        language: str = Depends(get_language),
    ):
        """List catalogs available to the Coverages service."""
        from dynastore.extensions.tools.pagination import resolve_page_limit

        coverages_config = await self._get_plugin_config(CoveragesConfig)
        limit = resolve_page_limit(
            limit,
            default_limit=coverages_config.default_limit,
            max_limit=coverages_config.max_limit,
        )

        return await self._ogc_list_catalogs(limit=limit, offset=offset, language=language)

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
        language: str = Depends(get_language),
    ):
        """List collections in a catalog (web-browser navigation)."""
        from dynastore.extensions.tools.pagination import resolve_page_limit

        coverages_config = await self._get_plugin_config(CoveragesConfig, catalog_id)
        limit = resolve_page_limit(
            limit,
            default_limit=coverages_config.default_limit,
            max_limit=coverages_config.max_limit,
        )

        return await self._ogc_list_collections(
            catalog_id, limit=limit, offset=offset, language=language
        )

    # ------------------------------------------------------------------
    # Coverage endpoints (stubs — to be implemented per data model)
    # ------------------------------------------------------------------

    async def get_coverage(
        self,
        catalog_id: str,
        collection_id: str,
        subset: Optional[str] = Query(None),
        bbox: Optional[str] = Query(
            None,
            description=(
                "Bounding-box filter in CRS84: minlon,minlat,maxlon,maxlat. "
                "OGC 19-087 /req/subsetting-spatial. "
                "Cannot repeat an axis already named in the 'subset' parameter."
            ),
        ),
        scale_factor: Optional[float] = Query(
            None,
            alias="scale-factor",
            gt=0,
            description=(
                "Uniform downsampling ratio applied to the output grid. "
                "A value of 0.5 halves both width and height. "
                "OGC 19-087 §7.11 /req/scale-factor."
            ),
        ),
        scale_size: Optional[str] = Query(
            None,
            alias="scale-size",
            description=(
                "Per-axis output pixel count in 'AxisLabel(n)' form, "
                "e.g. 'Lon(256),Lat(128)'. "
                "OGC 19-087 §7.12 /req/scale-size."
            ),
        ),
        f: Optional[str] = Query("geotiff"),
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        """Stream a coverage by content-negotiated format with optional subset.

        Supported subsetting: ``subset`` (OGC 19-087 §7.7) and ``bbox``
        (§7.8). Supported scaling: ``scale-factor`` (§7.11) and ``scale-size``
        (§7.12).  ``scale-factor`` / ``scale-size`` are implemented for GeoTIFF
        and CoverageJSON; NetCDF-4 and Zarr return 501 when scaling is
        requested (the writer pipeline does not support out_shape resampling).

        ``?hints=`` is accepted uniformly (e.g. ``hints=geometry_exact``) but
        reserved for forward-compatible routing — coverage data is read from a
        raster asset directly and does not flow through a hints-capable vector
        read seam, so the value has no effect on this route today.
        """
        await self._require_collection_visible(catalog_id, collection_id)
        fmt = _resolve_format(f)
        effective_subset = _merge_subset_and_bbox(subset, bbox)
        item = await self._get_first_item(catalog_id, collection_id)
        if item is None:
            raise HTTPException(status_code=404, detail="No coverage item found.")
        href, media_type = _resolve_coverage_asset(item)
        _require_reader(media_type)
        if fmt == "geotiff":
            gen = _stream_coverage_geotiff(
                href,
                subset=effective_subset,
                media_type=media_type,
                scale_factor=scale_factor,
                scale_size=scale_size,
            )
        elif fmt == "covjson":
            if scale_factor is not None or scale_size is not None:
                raise HTTPException(
                    status_code=501,
                    detail=(
                        "scale-factor and scale-size are not yet supported for "
                        "CoverageJSON output."
                    ),
                )
            from dynastore.modules.coverages.writers.coveragejson import (
                write_coveragejson,
            )
            ds_meta = build_domainset(item) or {}
            rt = build_rangetype(item) or {"type": "DataRecord", "field": []}
            values_iter = _read_coverage_values(href, effective_subset, rt, media_type=media_type)
            gen = write_coveragejson(ds_meta, rt, values_iter)
        elif fmt == "netcdf":
            if scale_factor is not None or scale_size is not None:
                raise HTTPException(
                    status_code=501,
                    detail=(
                        "scale-factor and scale-size are not yet supported for "
                        "NetCDF output."
                    ),
                )
            rt = build_rangetype(item) or {"type": "DataRecord", "field": []}
            band_names = [fld["name"] for fld in rt.get("field", [])] or ["band"]
            gen = _stream_coverage_netcdf(
                href, subset=effective_subset, band_names=band_names, media_type=media_type
            )
        elif fmt == "zarr":
            if scale_factor is not None or scale_size is not None:
                raise HTTPException(
                    status_code=501,
                    detail=(
                        "scale-factor and scale-size are not yet supported for "
                        "Zarr output."
                    ),
                )
            rt = build_rangetype(item) or {"type": "DataRecord", "field": []}
            band_names = [fld["name"] for fld in rt.get("field", [])] or ["band"]
            gen = _stream_coverage_zarr(
                href, subset=effective_subset, band_names=band_names, media_type=media_type
            )
        else:  # pragma: no cover - guarded by _resolve_format above
            raise HTTPException(status_code=415, detail=f"Unsupported format: {fmt!r}")
        return StreamingResponse(gen, media_type=MEDIA_TYPE_FOR[fmt])

    async def get_coverage_domainset(
        self, catalog_id: str, collection_id: str,
    ) -> dict:
        """Return the OGC Coverages DomainSet derived from the first item."""
        await self._require_collection_visible(catalog_id, collection_id)
        item = await self._get_first_item(catalog_id, collection_id)
        return _extract_domainset(item)

    async def get_coverage_rangetype(
        self, catalog_id: str, collection_id: str,
    ) -> dict:
        """Return the OGC Coverages RangeType derived from the first item."""
        await self._require_collection_visible(catalog_id, collection_id)
        item = await self._get_first_item(catalog_id, collection_id)
        return _extract_rangetype(item)

    async def get_coverage_metadata(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        language: str = Depends(get_language),
    ):
        await self._require_collection_visible(catalog_id, collection_id)
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
            language=language,
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
