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

"""OGC API - 3D GeoVolumes service.

Delivers HTTP endpoints for:
  - GET /…/3dtiles/tileset.json    — BSP-partitioned tileset index
  - GET /…/3dtiles/tiles/{id}.b3dm — B3DM tile (Cesium 3D Tiles 1.0)
  - GET /…/3dtiles/tiles/{id}.glb  — glTF 2.0 tile (3D Tiles 1.1)
  - GET /…/3dtiles/metadata        — service metadata + links
  - GET /volumes/catalogs/{cat_id}/collections          — GeoVolumes Core listing
  - GET /volumes/catalogs/{cat_id}/collections/{col_id} — single 3D container
  - GET /volumes/catalogs/{cat_id}/collections/{col_id}/cityjsonseq — CityJSONSeq stream

Tile content pipeline:
  1. The BSP tree (tileset dict with ``_feature_ids`` per leaf) is built
     once per (catalog_id, collection_id) and cached in ``_TILESET_CACHE``
     for ``VolumesConfig.on_demand_cache_ttl_s`` seconds.
  2. A tile request calls ``_resolve_tile`` which does ONE BSP lookup via
     ``find_leaf`` (O(depth) path decode), ONE config fetch, and ONE
     geometry DB round-trip.
  3. ``GeometryFetcherProtocol`` retrieves WKB geometries + height attrs.
  4. ``mesh_builder`` converts them; ``writers/glb`` packs GLB bytes;
     ``writers/b3dm`` wraps in B3DM when requested.

Draft spec — URIs verified against the OGC 3D GeoVolumes 1.0 working
draft at Phase 5 spec authorship.
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse

from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.volumes.config import VolumesConfig
from dynastore.extensions.volumes.volumes_models import (
    ContentExtent,
    ContentLink,
    ThreeDContainer,
    ThreeDContainerList,
    _bbox_intersects,
    _parse_bbox,
)
from dynastore.extensions.web.decorators import expose_static, expose_web_page
from dynastore.models.protocols.bounds_source import (
    BoundsSourceProtocol,
    EmptyBoundsSource,
)
from dynastore.extensions.volumes.platform_bounds_source import (
    register_sidecar_bounds_source,
)
from dynastore.models.protocols.geometry_fetcher import GeometryFetcherProtocol
from dynastore.modules.volumes.mesh_builder import (
    build_mesh_from_geometries,
    empty_mesh,
)
from dynastore.modules.volumes.geo import ecef_to_geodetic
from dynastore.modules.volumes.tileset_builder import build_tileset, find_leaf
from dynastore.modules.volumes.writers.b3dm import pack_b3dm
from dynastore.modules.volumes.writers.glb import pack_glb
from dynastore.modules.volumes.writers.tileset_json import write_tileset_json
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.tools.cache import cached
from dynastore.extensions.tools.url import get_url
from dynastore.extensions.tools.language_utils import get_language
from dynastore.extensions.tools.response_i18n import resolve_localized

logger = logging.getLogger(__name__)


OGC_API_VOLUMES_URIS = [
    "http://www.opengis.net/spec/ogcapi-3d-geovolumes-1/0.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-3d-geovolumes-1/0.0/conf/3dtiles",
    "http://www.opengis.net/spec/ogcapi-3d-geovolumes-1/0.0/conf/tileset",
    "http://www.opengis.net/spec/ogcapi-3d-geovolumes-1/0.0/conf/spatialquery",
]


class VolumesService(ExtensionProtocol, OGCServiceMixin):
    """OGC API - 3D GeoVolumes extension."""

    priority: int = 170

    conformance_uris = OGC_API_VOLUMES_URIS
    prefix = "/volumes"
    protocol_title = "DynaStore OGC API - 3D GeoVolumes"
    protocol_description = "Access to 3D tile data via OGC API - 3D GeoVolumes"

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self.router = APIRouter(prefix=self.prefix, tags=["OGC API - 3D GeoVolumes"])
        self._register_routes()

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        # Wire the runtime tiler: register the sidecar-backed bounds source and
        # geometry fetcher so tileset.json carries real content (and tiles
        # render real geometry) for CityJSON collections. Registration is
        # lazy — the DB is only touched per request — and the read paths
        # degrade to an empty tileset for collections without a geometries
        # sidecar (e.g. external 3D Tiles references), so this is safe to do
        # unconditionally. A registration failure must never block startup.
        try:
            register_sidecar_bounds_source()
        except Exception as exc:  # pragma: no cover - defensive startup guard
            logger.warning("volumes: could not register sidecar bounds source: %s", exc)
        yield

    async def get_conformance(self, request: Request):
        return await self.ogc_conformance_handler(request)

    async def get_landing_page(self, request: Request):
        return await self.ogc_landing_page_handler(request)

    def _register_routes(self) -> None:
        self.router.add_api_route(
            "/",
            self.get_landing_page,
            methods=["GET"],
            summary="OGC API - 3D GeoVolumes landing page",
        )
        self.router.add_api_route(
            "/conformance",
            self.get_conformance,
            methods=["GET"],
            summary="OGC API - 3D GeoVolumes conformance",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/3dtiles/tileset.json",
            self.get_tileset_json, methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/3dtiles/tiles/{tile_id}.b3dm",
            self.get_tile_b3dm, methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/3dtiles/tiles/{tile_id}.glb",
            self.get_tile_glb, methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/3dtiles/metadata",
            self.get_volumes_metadata, methods=["GET"],
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections",
            self.list_3d_collections, methods=["GET"],
            summary="List 3D container collections",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self.get_3d_collection, methods=["GET"],
            summary="Get a single 3D container",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/cityjsonseq",
            self.stream_cityjsonseq, methods=["GET"],
            summary="Stream CityJSONSeq for a 3D container collection",
        )

    # ------------------------------------------------------------------
    # Tileset index
    # ------------------------------------------------------------------

    async def get_tileset_json(
        self, catalog_id: str, collection_id: str, request: Request,
    ) -> StreamingResponse:
        cfg = await self._get_volumes_config(catalog_id, collection_id)
        tileset = await self._get_or_build_tileset(catalog_id, collection_id, cfg, request)
        return StreamingResponse(
            write_tileset_json(tileset),
            media_type="application/json",
        )

    # ------------------------------------------------------------------
    # Tile content
    # ------------------------------------------------------------------

    async def get_tile_b3dm(
        self, catalog_id: str, collection_id: str, tile_id: str, request: Request,
    ) -> Response:
        cfg = await self._get_volumes_config(catalog_id, collection_id)
        feature_ids, glb_bytes = await self._resolve_tile(
            catalog_id, collection_id, tile_id, request, cfg,
        )
        return Response(
            content=pack_b3dm(glb_bytes, feature_ids=feature_ids),
            media_type="application/octet-stream",
        )

    async def get_tile_glb(
        self, catalog_id: str, collection_id: str, tile_id: str, request: Request,
    ) -> Response:
        cfg = await self._get_volumes_config(catalog_id, collection_id)
        _, glb_bytes = await self._resolve_tile(
            catalog_id, collection_id, tile_id, request, cfg,
        )
        return Response(content=glb_bytes, media_type="model/gltf-binary")

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    async def get_volumes_metadata(
        self, catalog_id: str, collection_id: str, request: Request,
    ):
        cfg = await self._get_volumes_config(catalog_id, collection_id)
        # get_url honors FORCE_HTTPS: behind the review/dev load balancer the
        # inner request scheme is http, which would emit mixed-content links on
        # the https page. get_url also strips query params + trailing slash.
        base = get_url(request).rsplit("/", 1)[0]
        return {
            "title": f"3D GeoVolumes for {catalog_id}/{collection_id}",
            "description": "OGC API - 3D GeoVolumes (Cesium 3D Tiles encoding)",
            "config": {
                "max_features_per_tile": cfg.max_features_per_tile,
                "max_tree_depth": cfg.max_tree_depth,
                "root_geometric_error": cfg.root_geometric_error,
                "default_extrusion_height": cfg.default_extrusion_height,
                "supported_formats": cfg.supported_formats,
            },
            "links": [
                {"rel": "self", "type": "application/json", "href": f"{base}/metadata"},
                {"rel": "data", "type": "application/json", "href": f"{base}/tileset.json"},
            ],
        }

    # ------------------------------------------------------------------
    # GeoVolumes Core + SpatialQuery container API
    # ------------------------------------------------------------------

    async def list_3d_collections(
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
        bbox: Optional[str] = Query(None),
        language: str = Depends(get_language),
    ) -> Dict[str, Any]:
        """List collections flagged as 3D containers.

        A collection is 3D iff its extras carry ``cityjson:version`` or
        the explicit marker ``geovolumes:enabled``. The optional ``bbox``
        parameter (4 or 6 comma-separated floats) filters by spatial extent.
        """
        from dynastore.extensions.tools.pagination import resolve_page_limit

        cfg = await self._get_volumes_config(catalog_id)
        limit = resolve_page_limit(
            limit, default_limit=cfg.default_limit, max_limit=cfg.max_limit,
        )

        parsed_bbox: Optional[Tuple] = None
        if bbox is not None:
            try:
                parsed_bbox = _parse_bbox(bbox)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

        catalogs_svc = await self._get_catalogs_service()
        all_collections = await catalogs_svc.list_collections(
            catalog_id, limit=limit, offset=offset
        )

        containers: List[ThreeDContainer] = []
        for coll in (all_collections or []):
            if not _is_3d_collection(coll):
                continue
            container = _build_3d_container(coll, catalog_id, language)
            if parsed_bbox is not None:
                if not _bbox_intersects(container.contentExtent.bbox, parsed_bbox):
                    continue
            containers.append(container)

        return ThreeDContainerList(
            collections=containers,
            links=[],
        ).model_dump(exclude_none=True)

    async def get_3d_collection(
        self,
        catalog_id: str,
        collection_id: str,
        language: str = Depends(get_language),
    ) -> Dict[str, Any]:
        """Return a ThreeDContainer for the given collection.

        Returns 404 if the collection does not exist or is not 3D.
        """
        coll = await self._resolve_collection_or_404(
            catalog_id, collection_id, detail="Collection not found.",
        )
        if not _is_3d_collection(coll):
            raise HTTPException(
                status_code=404,
                detail="Collection is not a 3D GeoVolumes container.",
            )
        container = _build_3d_container(coll, catalog_id, language)
        return container.model_dump(exclude_none=True)

    async def stream_cityjsonseq(
        self,
        catalog_id: str,
        collection_id: str,
        limit: Optional[int] = Query(
            None,
            ge=1,
            description=(
                "Maximum number of items to stream. Omitted falls back to "
                "the configured default; a value above the configured "
                "maximum is clamped, not rejected (fc-limit-response-1)."
            ),
        ),
    ) -> StreamingResponse:
        """Stream a CityJSONSeq response for all items in a 3D collection.

        Line 1 is a CityJSONSeq header reconstructed from collection extras.
        Subsequent lines are individual CityJSONFeature objects (NDJSON).
        Media type: ``application/city+json``.
        """
        coll = await self._resolve_collection_or_404(
            catalog_id, collection_id, detail="Collection not found.",
        )
        if not _is_3d_collection(coll):
            raise HTTPException(
                status_code=404,
                detail="Collection is not a 3D GeoVolumes container.",
            )

        from dynastore.extensions.tools.pagination import resolve_page_limit

        cfg = await self._get_volumes_config(catalog_id, collection_id)
        limit = resolve_page_limit(
            limit,
            default_limit=cfg.stream_default_limit,
            max_limit=cfg.stream_max_limit,
        )

        extras = _get_extras(coll)
        header = _build_cityjsonseq_header(extras)

        from dynastore.models.query_builder import QueryRequest

        # Stream instead of materializing: each item carries the full
        # CityJSONFeature payload, so buffering the whole collection can
        # cost hundreds of MB at the default limit.
        catalogs_svc = await self._get_catalogs_service()
        query_response = await catalogs_svc.stream_items(
            catalog_id, collection_id, QueryRequest(limit=limit), ctx=None
        )

        async def _generate():
            yield json.dumps(header) + "\n"
            async for feat in query_response.items:
                cityjson = _extract_cityjson(feat)
                if cityjson is not None:
                    yield json.dumps(cityjson) + "\n"

        return StreamingResponse(
            _generate(),
            media_type="application/city+json",
        )

    # ------------------------------------------------------------------
    # Web page contributions (globe browser)
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

    @expose_static("volumes")
    def provide_static_files(self) -> list:
        """Exposes the static directory for the GeoVolumes globe browser."""
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        files = []
        if os.path.isdir(static_dir):
            for root, _, filenames in os.walk(static_dir):
                for filename in filenames:
                    files.append(os.path.join(root, filename))
        return files

    @expose_web_page(
        page_id="volumes_browser",
        title={"en": "3D GeoVolumes", "fr": "GéoVolumes 3D", "es": "GeoVolúmenes 3D"},
        icon="fa-cube",
        description={
            "en": "Browse 3D building volumes and CityJSON data on a globe.",
            "fr": "Explorer des volumes de bâtiments 3D et des données CityJSON sur un globe.",
            "es": "Explorar volúmenes de edificios 3D y datos CityJSON en un globo.",
        },
    )
    async def provide_volumes_browser(self, request: Request):
        return await self._serve_page_template("volumes_browser.html")

    async def _serve_page_template(self, filename: str):
        from dynastore._version import VERSION

        file_path = os.path.join(os.path.dirname(__file__), "static", filename)
        if not os.path.exists(file_path):
            return Response(content=f"Template {filename} not found", status_code=404)
        with open(file_path, "r", encoding="utf-8") as f:
            return Response(
                content=f.read().replace("{{VERSION}}", VERSION),
                media_type="text/html",
            )

    # ------------------------------------------------------------------
    # Internals — tileset pipeline
    # ------------------------------------------------------------------

    async def _get_volumes_config(
        self, catalog_id: str, collection_id: Optional[str] = None,
    ) -> VolumesConfig:
        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return VolumesConfig()
        return await configs.get_config(VolumesConfig, catalog_id, collection_id)

    async def _get_or_build_tileset(
        self,
        catalog_id: str,
        collection_id: str,
        cfg: VolumesConfig,
        request: Request,
    ) -> Dict[str, Any]:
        base = get_url(request).rsplit("/", 1)[0]
        primary_fmt = cfg.supported_formats[0] if cfg.supported_formats else "b3dm"
        template = f"{base}/tiles/{{tile_id}}.{primary_fmt}"
        return await self._build_tileset_cached(catalog_id, collection_id, template, cfg)

    @staticmethod
    @cached(
        maxsize=256,
        ttl=3600,
        namespace="volumes_tileset",
        distributed=True,
    )
    async def _build_tileset_cached(
        catalog_id: str,
        collection_id: str,
        template: str,
        cfg: VolumesConfig,
    ) -> Dict[str, Any]:
        bounds_source: BoundsSourceProtocol = (
            get_protocol(BoundsSourceProtocol) or EmptyBoundsSource()
        )
        try:
            bounds = list(await bounds_source.get_bounds(catalog_id, collection_id))
        except Exception as exc:
            logger.warning(
                "volumes: bounds lookup failed for %s/%s (%s); serving empty tileset",
                catalog_id, collection_id, exc,
            )
            bounds = []

        return build_tileset(bounds, cfg, content_uri_template=template)

    async def _resolve_tile(
        self,
        catalog_id: str,
        collection_id: str,
        tile_id: str,
        request: Request,
        cfg: VolumesConfig,
    ) -> Tuple[List[str], bytes]:
        """Resolve a tile_id to (feature_ids, glb_bytes) in one pass.

        Single config fetch + single BSP lookup + single geometry DB call.
        """
        tileset = await self._get_or_build_tileset(catalog_id, collection_id, cfg, request)
        leaf = find_leaf(tileset["root"], tile_id)
        if leaf is None:
            raise HTTPException(status_code=404, detail=f"Tile {tile_id!r} not found")
        feature_ids: List[str] = leaf.get("_feature_ids", [])
        # The tile mesh must be built in the SAME local ENU frame as the
        # tileset boxes. Recover the dataset origin from the root transform's
        # translation column (= ECEF of the origin) so the two stay aligned.
        origin = _origin_from_tileset(tileset)
        glb_bytes = await self._geometry_to_glb(
            catalog_id, collection_id, feature_ids, cfg, origin,
        )
        return feature_ids, glb_bytes

    async def _geometry_to_glb(
        self,
        catalog_id: str,
        collection_id: str,
        feature_ids: List[str],
        cfg: VolumesConfig,
        origin: Tuple[float, float, float],
    ) -> bytes:
        if not feature_ids:
            return pack_glb(empty_mesh())

        fetcher: Optional[GeometryFetcherProtocol] = get_protocol(GeometryFetcherProtocol)
        if fetcher is None:
            logger.warning(
                "No GeometryFetcherProtocol registered; returning empty tile for %s/%s",
                catalog_id, collection_id,
            )
            return pack_glb(empty_mesh())

        try:
            geometries = await fetcher.get_geometries(catalog_id, collection_id, feature_ids)
        except Exception as exc:
            logger.warning(
                "volumes: geometry fetch failed for %s/%s (%s); returning empty tile",
                catalog_id, collection_id, exc,
            )
            return pack_glb(empty_mesh())
        color_ramp = None
        if cfg.color_by_height and cfg.height_color_ramp:
            from dynastore.modules.volumes.color_ramp import parse_ramp
            color_ramp = parse_ramp(cfg.height_color_ramp)

        # Procedural facade texture (our own Apache-2.0 content) — windows/floors
        # on the otherwise geometry-only buildings. Baked once per process and
        # cached; UVs are emitted only when it is active so untextured tiles stay
        # lean. Composes with the height tint (texture × COLOR_0).
        texture_png = None
        if cfg.facade_texture:
            from dynastore.modules.volumes.facade_texture import build_default_facade_png
            texture_png = build_default_facade_png()

        mesh = build_mesh_from_geometries(
            geometries,
            origin=origin,
            default_extrusion_height=cfg.default_extrusion_height,
            color_ramp=color_ramp,
            with_uv=texture_png is not None,
        )
        return pack_glb(mesh, texture_png=texture_png)


# ---------------------------------------------------------------------------
# Collection classification and decoration helpers
# ---------------------------------------------------------------------------


def _origin_from_tileset(tileset: Dict[str, Any]) -> Tuple[float, float, float]:
    """Recover the ENU origin (lon, lat, height) from the root transform.

    ``build_tileset`` stamps ``root.transform`` with the ENU→ECEF matrix whose
    translation column (indices 12..14) is the ECEF of the origin. An empty
    tileset (no bounds) has no transform → origin (0, 0, 0), which is harmless
    because there is no geometry to place.
    """
    transform = tileset.get("root", {}).get("transform")
    if not transform or len(transform) < 15:
        return (0.0, 0.0, 0.0)
    return ecef_to_geodetic(transform[12], transform[13], transform[14])


def _get_extras(coll: Any) -> Dict[str, Any]:
    """Extract the extras dict from a Collection, tolerating varied shapes.

    Search-routed collections carry extras in ``model_extra`` (nested under
    an ``extras`` key or flat namespaced keys).  Read-routed collections
    only expose the PG-persisted ``extra_metadata`` column, so fall back to
    it — the ingest writes the CityJSON provenance to both surfaces.
    """
    raw_extra = getattr(coll, "model_extra", None) or {}
    extras = raw_extra.get("extras") or {}
    if not extras and isinstance(raw_extra, dict):
        # Flat extras stored directly on model_extra
        extras = {k: v for k, v in raw_extra.items() if ":" in k}
    if not extras:
        extra_metadata = getattr(coll, "extra_metadata", None) or {}
        dump = getattr(extra_metadata, "model_dump", None)
        if callable(dump):
            extra_metadata = dump()
        if isinstance(extra_metadata, dict):
            # The BaseMetadata validator always stores extra_metadata
            # language-keyed ({"en": {...}}); unwrap before scanning.
            if extra_metadata and not any(":" in k for k in extra_metadata):
                localized = extra_metadata.get("en")
                if not isinstance(localized, dict):
                    localized = next(
                        (v for v in extra_metadata.values() if isinstance(v, dict)),
                        None,
                    )
                if isinstance(localized, dict):
                    extra_metadata = localized
            nested = extra_metadata.get("extras")
            if isinstance(nested, dict) and nested:
                extras = nested
            else:
                extras = {k: v for k, v in extra_metadata.items() if ":" in k}
    return extras


def _plain_text(value: Any, language: str = "en") -> Any:
    """Coerce a LocalizedText (or plain string) to the requested language for the wire.

    Delegates to ``resolve_localized``:
    - ``language == '*'``: returns *value* unchanged (full multi-language passthrough).
    - Plain ``str``: returned as-is.
    - ``LocalizedDTO`` / language-keyed dict: collapsed to the best matching translation.
    - Anything else: returned as-is.
    """
    return resolve_localized(value, language)


def _is_3d_collection(coll: Any) -> bool:
    """Return True iff the collection carries 3D GeoVolumes provenance."""
    extras = _get_extras(coll)
    return bool(extras.get("cityjson:version") or extras.get("geovolumes:enabled"))


def _normalize_bbox(raw: Any) -> Optional[Tuple[float, float, float, float, float, float]]:
    """Coerce a 4-element 2D or 6-element CRS84h bbox to (minx, miny, zmin, maxx, maxy, zmax).

    Returns None when the value is not a usable bbox (wrong shape, non-numeric,
    or all-zero horizontal axes).
    """
    if not isinstance(raw, (list, tuple)):
        return None
    try:
        vals = [float(v) for v in raw]
    except (TypeError, ValueError):
        return None
    if len(vals) >= 6:
        minx, miny, zmin, maxx, maxy, zmax = vals[:6]
    elif len(vals) >= 4:
        minx, miny, maxx, maxy = vals[:4]
        zmin = zmax = 0.0
    else:
        return None
    if not any((minx, miny, maxx, maxy)):
        return None
    return (minx, miny, zmin, maxx, maxy, zmax)


def _collection_bbox_3d(coll: Any, extras: Dict[str, Any]) -> List[float]:
    """Build a 6-element 3D bbox from collection extent + z-range extras."""
    bbox: Optional[Tuple[float, float, float, float, float, float]] = None
    extent = getattr(coll, "extent", None)
    if extent is not None:
        spatial = getattr(extent, "spatial", None)
        if spatial is not None:
            raw = getattr(spatial, "bbox", None) or []
            if raw:
                first = raw[0] if isinstance(raw[0], (list, tuple)) else raw
                bbox = _normalize_bbox(first)

    # The collection ``extent`` column only persists where the optional
    # STAC collection sidecar is materialized; ingest therefore also
    # stamps the dataset bbox into extras (geovolumes:bbox), which the
    # always-present core driver persists.  Prefer a real extent, fall
    # back to the stamped bbox.  The stamped value is 4-element for
    # ingested CityJSON collections (z carried by geovolumes:zrange) and
    # 6-element CRS84h for external-reference containers.
    if bbox is None:
        bbox = _normalize_bbox(extras.get("geovolumes:bbox"))

    minx, miny, zmin, maxx, maxy, zmax = bbox or (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    if zmin == 0.0 and zmax == 0.0:
        zrange = extras.get("geovolumes:zrange") or {}
        zmin = float(zrange.get("zmin", 0.0))
        zmax = float(zrange.get("zmax", 0.0))
    return [minx, miny, zmin, maxx, maxy, zmax]


def _build_cityjsonseq_link(catalog_id: str, collection_id: str) -> ContentLink:
    """Build the alternate CityJSONSeq content link for the given collection."""
    return ContentLink(
        rel="alternate",
        href=f"/volumes/catalogs/{catalog_id}/collections/{collection_id}/cityjsonseq",
        type="application/city+json",
        title="CityJSONSeq stream",
    )


def _build_3d_container(coll: Any, catalog_id: str, language: str = "en") -> ThreeDContainer:
    """Build a ThreeDContainer wire model from a Collection.

    When extras carry ``geovolumes:tileset_url`` (a non-empty string), that
    absolute URL is emitted as the 3D Tiles content href and the CityJSONSeq
    alternate link is omitted (external containers have no CityJSON payload).
    Otherwise the native runtime tileset href is used and the CityJSONSeq
    link is included (CityJSON collections only).

    *language* controls how ``LocalizedText`` title values are resolved on the
    wire.  Pass ``'*'`` to return the full multi-language object; any other
    value collapses to that language with ``'en'`` as fallback.
    """
    extras = _get_extras(coll)
    bbox_3d = _collection_bbox_3d(coll, extras)

    external_url: Optional[str] = extras.get("geovolumes:tileset_url") or None
    if external_url and isinstance(external_url, str) and external_url.strip():
        tiles_href = external_url.strip()
        tiles_title = "3D Tiles tileset (external)"
    else:
        external_url = None
        tiles_href = (
            f"/volumes/catalogs/{catalog_id}/collections/{coll.id}/3dtiles/tileset.json"
        )
        tiles_title = "3D Tiles tileset"

    content: List[ContentLink] = [
        ContentLink(
            rel="http://www.opengis.net/def/rel/ogc/1.0/3dtiles",
            href=tiles_href,
            type="application/json",
            title=tiles_title,
        ),
    ]

    # Include the CityJSONSeq alternate link only for native CityJSON collections.
    if not external_url and extras.get("cityjson:version"):
        content.append(_build_cityjsonseq_link(catalog_id, coll.id))

    attribution: Optional[str] = extras.get("geovolumes:attribution") or None
    if attribution and isinstance(attribution, str):
        attribution = attribution.strip() or None

    # ThreeDContainer.title is Optional[str]; _plain_text with lang="*" returns
    # the raw LocalizedText which Pydantic cannot accept there.  Collapse to
    # "en" in that case so the model validates — the multi-language dict cannot
    # be represented in this model's wire shape.
    resolved_title = _plain_text(getattr(coll, "title", None), language if language != "*" else "en")
    return ThreeDContainer(
        id=coll.id,
        title=resolved_title if isinstance(resolved_title, str) or resolved_title is None else str(resolved_title),
        collectionType="3dcontainer",
        contentExtent=ContentExtent(bbox=bbox_3d),
        content=content,
        links=None,
        children=None,
        attribution=attribution if attribution else None,
    )


def _build_cityjsonseq_header(extras: Dict[str, Any]) -> Dict[str, Any]:
    """Reconstruct a CityJSONSeq header dict from collection extras."""
    transform = extras.get("cityjson:transform") or {
        "scale": [1.0, 1.0, 1.0],
        "translate": [0.0, 0.0, 0.0],
    }
    header: Dict[str, Any] = {
        "type": "CityJSONSeq",
        "version": extras.get("cityjson:version", "2.0"),
        "transform": transform,
    }
    ref_sys = extras.get("cityjson:referenceSystem")
    if ref_sys:
        header["metadata"] = {"referenceSystem": ref_sys}
    return header


def _extract_cityjson(feat: Any) -> Optional[Dict[str, Any]]:
    """Extract the stored CityJSONFeature dict from an item, or None.

    Ingest persists the payload under ``properties.cityjson`` (the only
    surface every item driver round-trips); older shapes carried it in
    ``extras``/top-level, kept here as fallbacks.
    """
    properties = getattr(feat, "properties", None)
    if not isinstance(properties, dict) and isinstance(feat, dict):
        properties = feat.get("properties")
    if isinstance(properties, dict):
        cityjson = properties.get("cityjson")
        if cityjson is not None:
            return cityjson
    # Try model_extra path (extras container)
    model_extra = getattr(feat, "model_extra", None) or {}
    extras = model_extra.get("extras") or {}
    cityjson = extras.get("cityjson")
    if cityjson is not None:
        return cityjson
    # Flat model_extra path
    cityjson = model_extra.get("cityjson")
    if cityjson is not None:
        return cityjson
    # Plain dict from model_dump
    if isinstance(feat, dict):
        return feat.get("extras", {}).get("cityjson") or feat.get("cityjson")
    return None
