"""OGC API - 3D GeoVolumes service.

Delivers HTTP endpoints for:
  - GET /…/3dtiles/tileset.json    — BSP-partitioned tileset index
  - GET /…/3dtiles/tiles/{id}.b3dm — B3DM tile (Cesium 3D Tiles 1.0)
  - GET /…/3dtiles/tiles/{id}.glb  — glTF 2.0 tile (3D Tiles 1.1)
  - GET /…/3dtiles/metadata        — service metadata + links

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

import logging
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.responses import Response, StreamingResponse

from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.ogc_policies import (
    ogc_anonymous_role_binding,
    ogc_public_access_policy,
)
from dynastore.extensions.volumes.config import VolumesConfig
from dynastore.models.protocols.bounds_source import (
    BoundsSourceProtocol,
    EmptyBoundsSource,
)
from dynastore.models.protocols.geometry_fetcher import GeometryFetcherProtocol
from dynastore.modules.volumes.mesh_builder import (
    build_mesh_from_geometries,
    empty_mesh,
)
from dynastore.modules.volumes.tileset_builder import build_tileset, find_leaf
from dynastore.modules.volumes.writers.b3dm import pack_b3dm
from dynastore.modules.volumes.writers.glb import pack_glb
from dynastore.modules.volumes.writers.tileset_json import write_tileset_json
from dynastore.tools.discovery import get_protocol

logger = logging.getLogger(__name__)


OGC_API_VOLUMES_URIS = [
    "http://www.opengis.net/spec/ogcapi-3d-geovolumes-1/0.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-3d-geovolumes-1/0.0/conf/3dtiles",
    "http://www.opengis.net/spec/ogcapi-3d-geovolumes-1/0.0/conf/tileset",
]

# Module-level BSP-tree cache: (catalog_id, collection_id) → (expires_at, tileset_dict)
_TILESET_CACHE: Dict[Tuple[str, str], Tuple[float, Dict[str, Any]]] = {}
_TILESET_CACHE_MAX = 256  # entries; evicts soonest-expiring when full


def _cache_get(catalog_id: str, collection_id: str) -> Optional[Dict[str, Any]]:
    key = (catalog_id, collection_id)
    entry = _TILESET_CACHE.get(key)
    if entry is None:
        return None
    if time.monotonic() >= entry[0]:
        _TILESET_CACHE.pop(key, None)  # evict expired entry
        return None
    return entry[1]


def _cache_set(
    catalog_id: str,
    collection_id: str,
    tileset: Dict[str, Any],
    ttl_s: int,
) -> None:
    if len(_TILESET_CACHE) >= _TILESET_CACHE_MAX:
        # Evict the entry with the soonest expiry to bound memory usage.
        oldest_key = min(_TILESET_CACHE, key=lambda k: _TILESET_CACHE[k][0])
        _TILESET_CACHE.pop(oldest_key, None)
    _TILESET_CACHE[(catalog_id, collection_id)] = (
        time.monotonic() + ttl_s,
        tileset,
    )


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
        logger.info("VolumesService: policies registered.")
        yield

    def get_policies(self):
        return [ogc_public_access_policy("volumes")]

    def get_role_bindings(self):
        return [ogc_anonymous_role_binding("volumes")]

    def _register_routes(self) -> None:
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
        base = str(request.url).rstrip("/").rsplit("/", 1)[0]
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
    # Internals
    # ------------------------------------------------------------------

    async def _get_volumes_config(
        self, catalog_id: str, collection_id: Optional[str] = None,
    ) -> VolumesConfig:
        # TODO(Phase 5d): replace with ConfigsProtocol resolver once confirmed.
        return VolumesConfig()

    async def _get_or_build_tileset(
        self,
        catalog_id: str,
        collection_id: str,
        cfg: VolumesConfig,
        request: Request,
    ) -> Dict[str, Any]:
        cached = _cache_get(catalog_id, collection_id)
        if cached is not None:
            return cached

        bounds_source: BoundsSourceProtocol = (
            get_protocol(BoundsSourceProtocol) or EmptyBoundsSource()
        )
        bounds = list(await bounds_source.get_bounds(catalog_id, collection_id))

        base = str(request.url).rstrip("/").rsplit("/", 1)[0]
        primary_fmt = cfg.supported_formats[0] if cfg.supported_formats else "b3dm"
        template = f"{base}/tiles/{{tile_id}}.{primary_fmt}"

        tileset = build_tileset(bounds, cfg, content_uri_template=template)
        _cache_set(catalog_id, collection_id, tileset, cfg.on_demand_cache_ttl_s)
        return tileset

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
        glb_bytes = await self._geometry_to_glb(catalog_id, collection_id, feature_ids, cfg)
        return feature_ids, glb_bytes

    async def _geometry_to_glb(
        self,
        catalog_id: str,
        collection_id: str,
        feature_ids: List[str],
        cfg: VolumesConfig,
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

        geometries = await fetcher.get_geometries(catalog_id, collection_id, feature_ids)
        mesh = build_mesh_from_geometries(
            geometries,
            default_extrusion_height=cfg.default_extrusion_height,
        )
        return pack_glb(mesh)
