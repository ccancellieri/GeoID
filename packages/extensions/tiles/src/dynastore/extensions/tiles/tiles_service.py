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

# dynastore/extensions/tiles/tiles_service.py

import logging
import json
import asyncio
import hashlib
import re
import time
from typing import ClassVar, FrozenSet, Literal, Optional, Dict, List, Tuple
from contextlib import asynccontextmanager
from fastapi import (
    FastAPI,
    APIRouter,
    Depends,
    HTTPException,
    Response,
    Query,
    Request,
    Path,
    BackgroundTasks,
)
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.extensions import protocols
from dynastore.extensions.ogc_base import OGCServiceMixin
from dynastore.extensions.tools.fast_api import AppJSONResponse as JSONResponse
from dynastore.extensions.tools.language_utils import get_language
from dynastore.extensions.tools.ogc_common_models import Conformance, LandingPage
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.models.protocols.web import WebModuleProtocol, StaticFilesProtocol
from dynastore.extensions.web.decorators import expose_static
from dynastore.extensions.tools.db import get_async_connection
from dynastore.extensions.tools.query import parse_hints_param
import dynastore.modules.tiles.tiles_module as tms_manager
from dynastore.tools.geospatial import SimplificationAlgorithm
from dynastore.extensions.web.decorators import expose_web_page
import os

from dynastore.modules.tiles import tiles_db
from dynastore.modules.tiles.tiles_module import TileStorageProtocol, TileArchiveStorageProtocol
from dynastore.tools.cache import cached
from dynastore.modules.tiles.tiles_config import (
    TilesConfig,
)
from dynastore.modules.tiles.tiles_models import (
    TileMatrixSetList,
    TileMatrixSet,
    TileMatrixSetRef,
    Link,
    TileMatrixSetCreate,
    TileSetItem,
    TileSetList,
)
from dynastore.modules.tiles.tms_definitions import BUILTIN_TILE_MATRIX_SETS

logger = logging.getLogger(__name__)

# Raster render imports — guarded so the tiles extension can load in
# environments without rio-tiler (graceful degradation: map-tile routes
# return 422 when rio-tiler is absent rather than failing import).
_RENDER_COG_TILE = None
_RENDER_COG_TERRAIN_RGB = None
_RENDER_COG_HILLSHADE = None
_PARSE_SLD_COLORMAP = None
_EXTRACT_SLD_BODY = None
_BUILD_RENDER_CACHE_KEY = None
_BUILD_RENDER_PARAMS_HASH = None
_RenderCachingConfig = None
try:
    from dynastore.modules.renders.engine import (  # noqa: E402
        render_cog_tile as _rct,
        render_cog_terrain_rgb as _rctr,
        render_cog_hillshade as _rch,
    )
    from dynastore.modules.renders.colormap import (  # noqa: E402
        parse_sld_colormap as _psc,
        extract_sld_body as _esb,
    )
    from dynastore.modules.renders.config import (  # noqa: E402
        build_render_cache_key as _brck,
        build_render_params_hash as _brph,
        RenderCachingConfig as _RCC,
    )
    _RENDER_COG_TILE = _rct
    _RENDER_COG_TERRAIN_RGB = _rctr
    _RENDER_COG_HILLSHADE = _rch
    _PARSE_SLD_COLORMAP = _psc
    _EXTRACT_SLD_BODY = _esb
    _BUILD_RENDER_CACHE_KEY = _brck
    _BUILD_RENDER_PARAMS_HASH = _brph
    _RenderCachingConfig = _RCC
except ImportError:
    pass

# Allowed characters in style_id path segment (reject path-traversal attempts).
_STYLE_ID_RE = re.compile(r'^[A-Za-z0-9._-]+$')

_FORMAT_MEDIA_TYPE: dict[str, str] = {
    "png": "image/png",
    "webp": "image/webp",
}

OGC_API_TILES_URIS = [
    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tileset",
    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tilesets-list",
    "http://www.opengis.net/spec/tms/2.0/conf/tilematrixset",
    "http://www.opengis.net/spec/tms/2.0/conf/json-tilematrixset",
    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/mvt",
    # Map-tile (dataType=map) conformance classes — this extension owns /map/tiles/...
    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/geodata-tilesets",
    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/collections-selection",
    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/png",
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/tilesets",
]


class TilesService(protocols.ExtensionProtocol, StaticFilesProtocol, OGCServiceMixin):
    priority: int = 100
    """
    Provides OGC API - Tiles functionality.

    Supports both vector (MVT) tiles backed by PostGIS and raster map tiles
    (dataType=map) rendered from COG assets via rio-tiler with SLD colormaps,
    terrain-RGB, and hillshade. Map-tile routes live under the collection
    resource: /catalogs/{cat}/collections/{coll}/map/tiles/{tms}/{z}/{x}/{y}.
    """

    conformance_uris: ClassVar[List[str]] = OGC_API_TILES_URIS
    prefix = "/tiles"
    protocol_title = "DynaStore OGC API - Tiles"
    protocol_description = (
        "Vector tile generation (MVT) backed by PostGIS and raster map tiles "
        "(dataType=map) rendered from COG assets via rio-tiler"
    )
    router: APIRouter

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

    def configure_app(self, app: FastAPI):
        """Early configuration for the Tiles extension."""
        pass

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self.router = APIRouter(tags=["OGC API - Tiles"], prefix="/tiles")
        self._register_routes()
        logger.info("Tiles Service: Initializing.")

    def contribute(self, ref):
        """AssetContributor: emit a vector-tiles XYZ template link for items."""
        from dynastore.models.protocols.asset_contrib import AssetLink
        if ref.item_id is None:
            return
        href = (
            f"{ref.base_url}{self.router.prefix}/{ref.catalog_id}"
            f"/tiles/{{z}}/{{x}}/{{y}}.mvt?collections={ref.collection_id}"
        )
        yield AssetLink(
            key="vector_tiles",
            href=href,
            title="Vector Tiles (MVT)",
            media_type="application/vnd.mapbox-vector-tile",
            roles=("tiles",),
        )

    def _register_routes(self):
        # OGC API Common: landing + conformance (delegated to OGCServiceMixin)
        self.router.add_api_route(
            "/", self.get_landing_page, methods=["GET"],
            response_model=LandingPage, summary="OGC API - Tiles landing page", name="get_tiles_landing_page",
        )
        self.router.add_api_route(
            "/conformance", self.get_conformance, methods=["GET"],
            response_model=Conformance, summary="OGC API - Tiles conformance", name="get_tiles_conformance",
        )
        # Tile Matrix Sets (server-level, untouched)
        self.router.add_api_route(
            "/tileMatrixSets", self.get_tile_matrix_sets, methods=["GET"],
            response_model=TileMatrixSetList, summary="Retrieve available Tile Matrix Sets"
        )
        self.router.add_api_route(
            "/tileMatrixSets/{tileMatrixSetId}", self.get_tile_matrix_set, methods=["GET"],
            response_model=TileMatrixSet, summary="Retrieve a Tile Matrix Set definition"
        )

        # Tile Content — deprecated flat/catalog-dataset paths
        self.router.add_api_route(
            "/catalogs/{dataset}/tiles/{z}/{x}/{y}.mvt", self.get_vector_tile_catalog_default, methods=["GET"],
            deprecated=True,
            summary=(
                "Catalog-centric MVT endpoint (deprecated). "
                "Use /tiles/catalogs/{catalog_id}/collections/{collection_id}/tiles/{z}/{x}/{y}.mvt instead."
            ),
        )
        self.router.add_api_route(
            "/catalogs/{dataset}/tiles/{tileMatrixSetId}/{z}/{x}/{y}.{format}", self.get_vector_tile_catalog, methods=["GET"],
            deprecated=True,
            summary=(
                "Catalog-centric MVT with TMS (deprecated). "
                "Use /tiles/catalogs/{catalog_id}/collections/{collection_id}/tiles/{tms}/{z}/{x}/{y}.{format} instead."
            ),
        )
        self.router.add_api_route(
            "/{dataset}/tiles/{z}/{x}/{y}.mvt", self.get_vector_tile_default, methods=["GET"],
            deprecated=True,
            summary=(
                "Legacy MVT endpoint (deprecated). "
                "Use /tiles/catalogs/{catalog_id}/collections/{collection_id}/tiles/{z}/{x}/{y}.mvt instead."
            ),
        )
        self.router.add_api_route(
            "/{dataset}/tiles/{tileMatrixSetId}/{z}/{x}/{y}.{format}", self.get_vector_tile, methods=["GET"],
            deprecated=True,
            summary=(
                "Get filtered MVT (deprecated). "
                "Use /tiles/catalogs/{catalog_id}/collections/{collection_id}/tiles/{tms}/{z}/{x}/{y}.{format} instead."
            ),
        )

        # Cache Management — deprecated flat path
        self.router.add_api_route(
            "/{dataset}/tiles/cache", self.invalidate_tile_cache, methods=["DELETE"],
            status_code=200,
            deprecated=True,
            summary=(
                "Invalidate tile cache (deprecated). "
                "Use DELETE /tiles/catalogs/{catalog_id}/collections/{collection_id}/tiles/cache instead."
            ),
        )

        # Tile Matrix Sets (per-collection deprecated path)
        self.router.add_api_route(
            "/{dataset}/tileMatrixSets", self.create_tile_matrix_set, methods=["POST"],
            deprecated=True,
            response_model=TileMatrixSet, status_code=201,
            summary=(
                "Create a custom Tile Matrix Set (deprecated). "
                "Use POST /tiles/catalogs/{catalog_id}/collections/{collection_id}/tileMatrixSets instead."
            ),
        )

        # --- Aligned endpoints: /tiles/catalogs/{catalog_id}/collections/{collection_id}/... ---

        # Aligned vector tile endpoints
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/tiles/{z}/{x}/{y}.mvt",
            self.get_vector_tile_aligned_default,
            methods=["GET"],
            summary="Get vector tile (MVT) for a collection (OGC aligned path, default WebMercatorQuad TMS)",
            name="get_vector_tile_aligned_default",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/tiles/{tileMatrixSetId}/{z}/{x}/{y}.{format}",
            self.get_vector_tile_aligned,
            methods=["GET"],
            summary="Get vector tile for a collection with explicit TMS (OGC aligned path)",
            name="get_vector_tile_aligned",
        )

        # Aligned tileset list
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/tiles",
            self.get_collection_tilesets,
            methods=["GET"],
            response_model=TileSetList,
            summary="List available tilesets for a collection (OGC API Tiles §7.1)",
            name="get_collection_tilesets",
        )
        # Tileset-metadata: full TileSet document for a specific TMS (vector tiles)
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/tiles/{tileMatrixSetId}",
            self.get_collection_tileset,
            methods=["GET"],
            response_model=TileSetItem,
            summary=(
                "Tileset metadata for vector tiles of a collection with a given TMS "
                "(OGC API Tiles §7.2, dataType='vector')"
            ),
            name="get_collection_tileset",
        )
        # Tileset-metadata: full TileSet document for map tiles (dataType=map)
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/map/tiles/{tms_id}",
            self.get_collection_map_tileset,
            methods=["GET"],
            response_model=TileSetItem,
            summary=(
                "Tileset metadata for raster map tiles of a collection with a given TMS "
                "(OGC API Maps §7.2, dataType='map')"
            ),
            name="get_collection_map_tileset",
        )

        # Aligned cache invalidation
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/tiles/cache",
            self.invalidate_collection_tile_cache,
            methods=["DELETE"],
            status_code=200,
            summary="Invalidate tile cache for a specific collection (OGC aligned path)",
            name="invalidate_collection_tile_cache",
        )

        # Map tiles (dataType=map) — default style resolved via binding
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}"
            "/map/tiles/{tms_id}/{z}/{x}/{y}.{format}",
            self.get_map_tile,
            methods=["GET"],
            summary=(
                "Render a styled raster map tile (dataType=map) from a COG asset using "
                "the collection's default style. catalog_id and collection_id are public "
                "(external) IDs."
            ),
            name="get_map_tile",
        )
        # Map tiles with explicit style
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}"
            "/styles/{style_id}/map/tiles/{tms_id}/{z}/{x}/{y}.{format}",
            self.get_map_tile_styled,
            methods=["GET"],
            summary=(
                "Render a styled raster map tile (dataType=map) with an explicit style. "
                "Use style_id='terrain-rgb' for Terrain-RGB encoding. "
                "Add ?relief=hillshade for hillshade rendering. "
                "catalog_id and collection_id are public (external) IDs."
            ),
            name="get_map_tile_styled",
        )

    @expose_static("tiles")
    def provide_static_files(self) -> list[str]:
        """Exposes the internal static directory for the tiles viewer."""
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        files = []
        for root, _, filenames in os.walk(static_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    def get_static_prefix(self) -> str:
        """Returns the static prefix for tiles."""
        return "tiles"

    async def is_file_provided(self, path: str) -> bool:
        """Checks if a static file is provided."""
        static_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), "static"))
        full_path = os.path.realpath(os.path.join(static_dir, path.lstrip("/")))
        if not full_path.startswith(static_dir + os.sep) and full_path != static_dir:
            return False
        return os.path.isfile(full_path)

    async def list_static_files(self, query: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[str]:
        """Lists static files for tiles with pagination and search."""
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        files = []
        for root, _, filenames in os.walk(static_dir):
            for filename in filenames:
                full_path = os.path.join(root, filename)
                rel_path = os.path.relpath(full_path, static_dir)
                if not query or query.lower() in rel_path.lower():
                    files.append(full_path)
        return sorted(files)[offset : offset + limit]

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """Manages the Tiles Service configuration."""
        from dynastore.tools.discovery import register_plugin, unregister_plugin
        from .stac_contributor import TilesStacContributor

        contributor = TilesStacContributor()
        register_plugin(contributor)
        logger.info("Tiles Service startup.")
        try:
            yield
        finally:
            unregister_plugin(contributor)
            logger.info("Tiles Service shutdown.")

    @expose_web_page(
        page_id="map_viewer",
        title="Map Viewer",
        icon="fa-map",
        priority=10,
        description="Visualize tiled datasets.",
    )
    async def provide_map_viewer(self, request: Request):
        file_path = os.path.join(os.path.dirname(__file__), "static", "map.html")
        if not os.path.exists(file_path):
            return Response(content="Template map.html not found", status_code=404)
        with open(file_path, "r", encoding="utf-8") as f:
            return Response(content=f.read(), media_type="text/html")

    @expose_web_page(
        page_id="terrain_viewer",
        title="Terrain Viewer",
        icon="fa-mountain",
        description="3D terrain with hillshade and colormap overlay from a DEM COG.",
        priority=90,
    )
    async def provide_terrain_viewer(self, request: Request) -> Response:
        from dynastore._version import VERSION
        file_path = os.path.join(os.path.dirname(__file__), "static", "terrain_viewer.html")
        if not os.path.exists(file_path):
            return Response(content="Template terrain_viewer.html not found", status_code=404)
        with open(file_path, "r", encoding="utf-8") as fh:
            return Response(
                content=fh.read().replace("{{VERSION}}", VERSION),
                media_type="text/html",
            )

    # --- OGC API Common (delegated to OGCServiceMixin) ---

    async def get_landing_page(
        self, request: Request, language: str = Depends(get_language)
    ) -> JSONResponse:
        return await self.ogc_landing_page_handler(request, language=language)

    async def get_conformance(self, request: Request) -> Conformance:
        return await self.ogc_conformance_handler(request)

    # --- Tile Matrix Sets Endpoints ---

    async def create_tile_matrix_set(self, dataset: str, tms_data: TileMatrixSetCreate):
        """Creates a new custom TileMatrixSet scoped to a specific dataset (catalog)."""
        # The tms_data is already a TileMatrixSetCreate model, which has 'id' and 'definition' fields.
        # The module function expects this exact model.
        stored_tms = await tms_manager.create_custom_tms(
            catalog_id=dataset, tms_data=tms_data
        )
        return stored_tms.definition

    async def get_tile_matrix_sets(
        self,
        request: Request,
        dataset: Optional[str] = Query(
            None, description="Filter TMS list by dataset (catalog)."
        ),
    ):
        """List all supported Tile Matrix Sets."""
        tms_refs = []
        for tms_id, tms_def in BUILTIN_TILE_MATRIX_SETS.items():
            tms_refs.append(
                TileMatrixSetRef(
                    id=tms_id,
                    title=tms_def.title,
                    links=[
                        Link(
                            href=str(
                                request.url_for(
                                    "get_tile_matrix_set", tileMatrixSetId=tms_id
                                )
                            ),
                            rel="self",
                            type="application/json",
                            title=tms_def.title,
                            hreflang="en",
                        )
                    ],
                )
            )

        # Append custom TMS from DB if a dataset is specified
        if dataset:
            custom_tms_list = await tms_manager.list_custom_tms(catalog_id=dataset)
            for tms in custom_tms_list:
                # Avoid duplicating if a custom TMS overrides a built-in one
                if not any(ref.id == tms.id for ref in tms_refs):
                    tms_refs.append(
                        TileMatrixSetRef(
                            id=tms.id,
                            title=tms.title,
                            links=[
                                Link(
                                    href=str(
                                        request.url_for(
                                            "get_tile_matrix_set",
                                            tileMatrixSetId=tms.id,
                                        ).include_query_params(dataset=dataset)
                                    ),
                                    rel="self",
                                    type="application/json",
                                    title=tms.title,
                                    hreflang="en",
                                )
                            ],
                        )
                    )
        return TileMatrixSetList(tileMatrixSets=tms_refs)

    async def get_tile_matrix_set(
        self,
        tileMatrixSetId: str = Path(
            ..., description="The Identifier of the Tile Matrix Set"
        ),
        dataset: Optional[str] = Query(
            None, description="Dataset (catalog) for custom TMS lookup."
        ),
    ):
        """Return the full definition of a specific Tile Matrix Set."""
        tms = None
        if dataset:
            tms = await tms_manager.get_custom_tms(
                catalog_id=dataset, tms_id=tileMatrixSetId
            )
        if not tms:
            tms_model = BUILTIN_TILE_MATRIX_SETS.get(tileMatrixSetId)
            if not tms_model:
                raise HTTPException(
                    status_code=404,
                    detail=f"TileMatrixSet '{tileMatrixSetId}' not found.",
                )
            return tms_model
        return tms

    # --- Tile Content Endpoints ---

    async def get_vector_tile_catalog_default(
        self,
        dataset: str,
        z: int,
        x: int,
        y: int,
        request: Request,
        background_tasks: BackgroundTasks,
        conn: AsyncConnection = Depends(get_async_connection),
        collections: str = Query(..., description="Comma-separated collection IDs."),
        datetime: Optional[str] = Query(None),
        filter: Optional[str] = Query(None, description="CQL2 Filter expression."),
        filter_lang: Optional[str] = Query("cql2-text"),
        subset: Optional[str] = Query(None),
        simplification: Optional[float] = Query(None),
        simplification_by_zoom: Optional[str] = Query(None),
        simplification_algorithm: SimplificationAlgorithm = Query(
            SimplificationAlgorithm.TOPOLOGY_PRESERVING
        ),
        disable_cache: bool = Query(
            False, description="Disable cache for this request."
        ),
        refresh_cache: bool = Query(
            False, description="Refresh cache by invalidating before fetching."
        ),
        # Accepted for uniform protocol consistency; MVT is generated by PostGIS
        # and does not pass through the hints routing layer.
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        """Catalog-centric endpoint defaulting to WebMercatorQuad."""
        return await self.get_vector_tile(
            request=request,
            dataset=dataset,
            tileMatrixSetId="WebMercatorQuad",
            z=z,
            x=x,
            y=y,
            format="mvt",
            background_tasks=background_tasks,
            conn=conn,
            collections=collections,
            datetime=datetime,
            filter=filter,
            filter_lang=filter_lang,
            subset=subset,
            simplification=simplification,
            simplification_by_zoom=simplification_by_zoom,
            simplification_algorithm=simplification_algorithm,
            disable_cache=disable_cache,
            refresh_cache=refresh_cache,
        )

    async def get_vector_tile_catalog(
        self,
        request: Request,
        dataset: str,
        tileMatrixSetId: str,
        z: int,
        x: int,
        y: int,
        format: str,
        background_tasks: BackgroundTasks,
        conn: AsyncConnection = Depends(get_async_connection),
        collections: str = Query(
            ..., description="Comma-separated list of collection IDs to include."
        ),
        datetime: Optional[str] = Query(None, description="Temporal filter."),
        filter: Optional[str] = Query(None, description="CQL2 Filter expression."),
        filter_lang: Optional[str] = Query("cql2-text"),
        subset: Optional[str] = Query(None),
        simplification: Optional[float] = Query(None),
        simplification_by_zoom: Optional[str] = Query(None),
        simplification_algorithm: SimplificationAlgorithm = Query(
            SimplificationAlgorithm.TOPOLOGY_PRESERVING
        ),
        disable_cache: bool = Query(
            False, description="Disable cache for this request."
        ),
        refresh_cache: bool = Query(
            False, description="Refresh cache by invalidating before fetching."
        ),
        # Accepted for uniform protocol consistency; MVT is generated by PostGIS
        # and does not pass through the hints routing layer.
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        """Catalog-centric endpoint with full TMS support."""
        return await self.get_vector_tile(
            request=request,
            dataset=dataset,
            tileMatrixSetId=tileMatrixSetId,
            z=z,
            x=x,
            y=y,
            format=format,
            background_tasks=background_tasks,
            conn=conn,
            collections=collections,
            datetime=datetime,
            filter=filter,
            filter_lang=filter_lang,
            subset=subset,
            simplification=simplification,
            simplification_by_zoom=simplification_by_zoom,
            simplification_algorithm=simplification_algorithm,
            disable_cache=disable_cache,
            refresh_cache=refresh_cache,
        )

    async def get_vector_tile_default(
        self,
        dataset: str,
        z: int,
        x: int,
        y: int,
        request: Request,
        background_tasks: BackgroundTasks,
        conn: AsyncConnection = Depends(get_async_connection),
        collections: str = Query(..., description="Comma-separated collection IDs."),
        datetime: Optional[str] = Query(None),
        filter: Optional[str] = Query(None, description="CQL2 Filter expression."),
        filter_lang: Optional[str] = Query("cql2-text"),
        subset: Optional[str] = Query(None),
        simplification: Optional[float] = Query(None),
        simplification_by_zoom: Optional[str] = Query(None),
        simplification_algorithm: SimplificationAlgorithm = Query(
            SimplificationAlgorithm.TOPOLOGY_PRESERVING
        ),
        disable_cache: bool = Query(
            False, description="Disable cache for this request."
        ),
        refresh_cache: bool = Query(
            False, description="Refresh cache by invalidating before fetching."
        ),
        # Accepted for uniform protocol consistency; MVT is generated by PostGIS
        # and does not pass through the hints routing layer.
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        """Defaults to WebMercatorQuad."""
        return await self.get_vector_tile(
            request=request,
            dataset=dataset,
            tileMatrixSetId="WebMercatorQuad",
            z=z,
            x=x,
            y=y,
            format="mvt",
            background_tasks=background_tasks,
            conn=conn,
            collections=collections,
            datetime=datetime,
            filter=filter,
            filter_lang=filter_lang,
            subset=subset,
            simplification=simplification,
            simplification_by_zoom=simplification_by_zoom,
            simplification_algorithm=simplification_algorithm,
            disable_cache=disable_cache,
            refresh_cache=refresh_cache,
        )

    async def get_vector_tile(
        self,
        request: Request,
        dataset: str,
        tileMatrixSetId: str,
        z: int,
        x: int,
        y: int,
        format: str,
        background_tasks: BackgroundTasks,
        conn: AsyncConnection = Depends(get_async_connection),
        collections: str = Query(
            ..., description="Comma-separated list of collection IDs to include."
        ),
        datetime: Optional[str] = Query(None, description="Temporal filter."),
        filter: Optional[str] = Query(None, description="CQL2 Filter expression."),
        filter_lang: Optional[str] = Query("cql2-text"),
        subset: Optional[str] = Query(None),
        simplification: Optional[float] = Query(None),
        simplification_by_zoom: Optional[str] = Query(None),
        simplification_algorithm: SimplificationAlgorithm = Query(
            SimplificationAlgorithm.TOPOLOGY_PRESERVING
        ),
        disable_cache: bool = Query(
            False, description="Disable cache for this request."
        ),
        refresh_cache: bool = Query(
            False, description="Refresh cache by invalidating before fetching."
        ),
        # Accepted for uniform protocol consistency; MVT is generated by PostGIS
        # and does not pass through the hints routing layer.
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        """
        Generates an MVT tile dynamically reprojected to the requested TileMatrixSet.
        Checks for pre-seeded tiles first.
        """
        start_time = time.perf_counter()

        try:
            # 1. Validation & Configuration
            if format not in ["mvt", "pbf"]:
                raise HTTPException(
                    status_code=400, detail=f"Format '{format}' not supported."
                )

            config_manager = get_protocol(ConfigsProtocol)
            if not config_manager:
                raise HTTPException(
                    status_code=500, detail="Configuration service unavailable."
                )
            requested_cols_list = [c.strip() for c in collections.split(",")]

            # Enforce collection-visibility before any catalog/tile resolution.
            # Each collection in the comma-separated list is checked
            # independently; a hidden collection is indistinguishable from a
            # missing one (404), matching CatalogService.get_collection.
            for _coll_id in requested_cols_list:
                await self._require_collection_visible(dataset, _coll_id)

            tiles_config = await self._resolve_request_config(
                config_manager, dataset
            )

            # 2. Storage & Cache Resolution
            cache_enabled = await self._is_cache_enabled(
                config_manager, dataset, requested_cols_list, tiles_config
            )

            # 3. Cache Key & Pre-seed Check
            params_hash = self._generate_params_hash(
                collections,
                datetime,
                filter,
                filter_lang,
                subset,
                simplification,
                simplification_by_zoom,
                simplification_algorithm,
            )

            # Handle cache control flags
            effective_cache_enabled = cache_enabled and not disable_cache

            if effective_cache_enabled:
                cache_id = (
                    collections
                    if len(requested_cols_list) > 1
                    else requested_cols_list[0]
                )
                effective_cache_id = (
                    f"{cache_id}@{params_hash}" if params_hash else cache_id
                )

                provider = get_protocol(TileStorageProtocol)
                if provider:
                    # If refresh_cache is True, invalidate the tile first
                    if refresh_cache:
                        try:
                            await provider.delete_tile(  # type: ignore[attr-defined]
                                dataset,
                                effective_cache_id,
                                tileMatrixSetId,
                                z,
                                x,
                                y,
                                format,
                            )
                            logger.info(
                                f"Tile cache REFRESHED: {dataset}/{collections}/{z}/{x}/{y}"
                            )
                        except Exception as e:
                            logger.warning(f"Failed to refresh tile cache: {e}")
                    else:
                        # Attempt redirect or proxy
                        res = await self._try_cached_tile(
                            provider,
                            dataset,
                            effective_cache_id,
                            tileMatrixSetId,
                            z,
                            x,
                            y,
                            format,
                            start_time,
                        )
                        if res:
                            return res

            logger.info(
                "tile_cache event=miss catalog=%s collection=%s z=%s x=%s y=%s "
                "cache_enabled=%s",
                dataset, collections, z, x, y, effective_cache_enabled,
            )

            # 4. TMS & Coordinate Validation
            tms_def = await self._validate_tms_and_matrix(
                dataset, tileMatrixSetId, z, x, y
            )

            # 5. SRID Resolution
            try:
                target_srid = await tms_manager.resolve_srid(
                    conn=conn, crs_str=tms_def.crs, catalog_id=dataset
                )
                if not target_srid:
                    raise ValueError("Failed to resolve SRID.")
            except Exception as e:
                logger.error(f"SRID resolution failed: {e}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Could not process CRS for TMS '{tileMatrixSetId}'.",
                ) from e

            # Resolve metadata for each collection (Cached in TilesModule)
            from dynastore.modules.tiles import tiles_module

            resolved_collections = []
            for coll_id in requested_cols_list:
                # get_tile_resolution_params is cached and validates existence
                meta = await tiles_module.get_tile_resolution_params(dataset, coll_id)
                if meta:
                    resolved_collections.append(meta)

            if not resolved_collections:
                logger.warning(
                    "No valid collections found for %s/%s "
                    "(requested=%d) — see tile metadata warnings above for "
                    "the underlying driver/routing failure",
                    dataset, collections, len(requested_cols_list),
                )
                return self._finalize_response(request, b"")

            # L2 cache miss — try PMTiles archive fallback before hitting PostGIS
            if effective_cache_enabled and not disable_cache:
                archive_storage = get_protocol(TileArchiveStorageProtocol)
                if archive_storage:
                    single_col = (
                        requested_cols_list[0] if len(requested_cols_list) == 1 else None
                    )
                    if single_col and await archive_storage.archive_exists(
                        dataset, single_col, tileMatrixSetId
                    ):
                        tile_bytes = await archive_storage.get_tile_from_archive(
                            dataset, single_col, tileMatrixSetId, z, x, y
                        )
                        if tile_bytes is not None:
                            duration_ms = (time.perf_counter() - start_time) * 1000
                            logger.info(
                                "tile_cache event=hit source=pmtiles_archive catalog=%s "
                                "collection=%s z=%s x=%s y=%s duration_ms=%.2f bytes=%d",
                                dataset, single_col, z, x, y, duration_ms, len(tile_bytes),
                            )
                            from fastapi.responses import Response as FResponse
                            return FResponse(
                                content=tile_bytes,
                                media_type="application/vnd.mapbox-vector-tile",
                                headers={
                                    "X-Tile-Cache": "hit",
                                    "X-Tile-Source": "pmtiles_archive",
                                },
                            )

            # Retrieve MVT content — PostGIS generation
            mvt_content = await self._generate_mvt(
                conn,
                resolved_collections,
                tms_def,
                target_srid,
                str(z),
                x,
                y,
                datetime,
                filter,
                subset,
                simplification,
                simplification_algorithm,
            )

            # 9. Background Caching
            effective_cache_enabled = cache_enabled and not disable_cache
            if mvt_content and effective_cache_enabled:
                cache_id = (
                    collections
                    if len(requested_cols_list) > 1
                    else requested_cols_list[0]
                )
                effective_cache_id = (
                    f"{cache_id}@{params_hash}" if params_hash else cache_id
                )
                provider = get_protocol(TileStorageProtocol)
                if provider:
                    background_tasks.add_task(
                        provider.save_tile,
                        dataset,
                        effective_cache_id,
                        tileMatrixSetId,
                        z,
                        x,
                        y,
                        mvt_content,
                        format,
                    )

            if not mvt_content:
                duration_ms = (time.perf_counter() - start_time) * 1000
                logger.info(
                    "tile_cache event=miss source=postgis catalog=%s collection=%s "
                    "z=%s x=%s y=%s duration_ms=%.2f bytes=0",
                    dataset, collections, z, x, y, duration_ms,
                )
                return Response(
                    status_code=204,
                    headers={"X-Tile-Cache": "miss", "X-Tile-Source": "postgis"},
                )

            duration_ms = (time.perf_counter() - start_time) * 1000
            logger.info(
                "tile_cache event=miss source=postgis catalog=%s collection=%s "
                "z=%s x=%s y=%s duration_ms=%.2f bytes=%d",
                dataset, collections, z, x, y, duration_ms, len(mvt_content),
            )

            response = self._finalize_response(request, mvt_content)
            response.headers["X-Tile-Cache"] = "miss"
            response.headers["X-Tile-Source"] = "postgis"
            return response

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"CRITICAL ERROR in get_vector_tile: {e}", exc_info=True)
            raise HTTPException(
                status_code=500, detail=f"Internal Server Error: {str(e)}"
            ) from e

    # --- Helper Private Methods ---

    @cached(
        maxsize=512,
        ttl=60,
        jitter=5,
        namespace="mvt_l1",
        ignore=["conn"],
        condition=lambda r: r is not None,
    )
    async def _generate_mvt(
        self,
        conn: AsyncConnection,
        resolved_collections: list,
        tms_def,
        target_srid: int,
        z: str,
        x: int,
        y: int,
        datetime_str: Optional[str],
        cql_filter: Optional[str],
        subset_params: Optional[str],
        simplification: Optional[float],
        simplification_algorithm,
    ) -> Optional[bytes]:
        """PostGIS MVT generation — L1 in-process cache above the storage provider L2."""
        try:
            return await tiles_db.get_features_as_mvt_filtered(
                conn=conn,
                resolved_collections=resolved_collections,
                tms_def=tms_def,
                target_srid=target_srid,
                z=z,
                x=x,
                y=y,
                datetime_str=datetime_str,
                cql_filter=cql_filter,
                subset_params=subset_params,  # type: ignore[arg-type]
                simplification=simplification,
                simplification_algorithm=simplification_algorithm,
            )
        except ValueError as exc:
            # Belt-and-suspenders: tiles_db._build_collection_subquery already
            # catches ValueError per-collection, but any storage-resolution
            # ValueError that escapes here would otherwise become an opaque
            # 500.  Returning None becomes a 204 upstream and — because the
            # @cached condition rejects None — does not poison the L1 cache.
            logger.warning("MVT generation skipped (storage unresolved): %s", exc)
            return None

    @staticmethod
    async def _resolve_request_config(
        config_manager, dataset: str
    ) -> TilesConfig:
        config = await config_manager.get_config(TilesConfig, dataset)
        if isinstance(config, TilesConfig) and not config.enabled:
            raise HTTPException(
                status_code=404, detail="Tiles are disabled for this catalog."
            )
        return config

    @staticmethod
    async def _is_cache_enabled(
        config_manager,
        dataset: str,
        collections: List[str],
        catalog_config: TilesConfig,
    ) -> bool:
        catalog_cache = getattr(catalog_config, "cache_on_demand", True)
        if not catalog_cache:
            return False
        if len(collections) == 1:
            coll_config = await config_manager.get_config(
                TilesConfig, dataset, collections[0]
            )
            return getattr(coll_config, "cache_on_demand", catalog_cache)
        return catalog_cache

    @staticmethod
    def _generate_params_hash(*args) -> Optional[str]:
        # Canonical tiles have no extra params
        if not any(args[1:]):
            return None
        params_str = "|".join(str(a) for a in args)
        return hashlib.sha256(params_str.encode()).hexdigest()[:16]

    @staticmethod
    async def _try_cached_tile(
        provider, dataset, cache_id, tms_id, z, x, y, format, start_time
    ):
        try:
            url = await provider.get_tile_url(
                dataset, cache_id, tms_id, z, x, y, format
            )
            if url:
                duration_ms = (time.perf_counter() - start_time) * 1000
                logger.info(
                    "tile_cache event=hit source=bucket_redirect catalog=%s collection=%s "
                    "z=%s x=%s y=%s duration_ms=%.2f",
                    dataset, cache_id, z, x, y, duration_ms,
                )
                return RedirectResponse(
                    url=url,
                    status_code=307,
                    headers={
                        "X-Tile-Cache": "hit",
                        "X-Tile-Source": "bucket_redirect",
                    },
                )

            tile = await provider.get_tile(dataset, cache_id, tms_id, z, x, y, format)
            if tile:
                duration_ms = (time.perf_counter() - start_time) * 1000
                logger.info(
                    "tile_cache event=hit source=bucket_proxy catalog=%s collection=%s "
                    "z=%s x=%s y=%s duration_ms=%.2f bytes=%d",
                    dataset, cache_id, z, x, y, duration_ms, len(tile),
                )
                return Response(
                    content=tile,
                    media_type="application/vnd.mapbox-vector-tile",
                    headers={
                        "X-Tile-Cache": "hit",
                        "X-Tile-Source": "bucket_proxy",
                    },
                )
        except Exception as e:
            logger.warning(f"Cache lookup failed: {e}")
        return None

    @staticmethod
    async def _validate_tms_and_matrix(dataset, tms_id, z, x, y):
        tms_def = await tms_manager.get_custom_tms(catalog_id=dataset, tms_id=tms_id)
        if not tms_def:
            tms_def = BUILTIN_TILE_MATRIX_SETS.get(tms_id)
            if not tms_def:
                raise HTTPException(
                    status_code=404, detail=f"TMS '{tms_id}' not supported."
                )

        matrix = next((m for m in tms_def.tileMatrices if m.id == str(z)), None)
        if not matrix:
            raise HTTPException(
                status_code=400, detail=f"Zoom {z} not defined in TMS {tms_id}."
            )

        if not (0 <= x < matrix.matrixWidth and 0 <= y < matrix.matrixHeight):
            raise HTTPException(
                status_code=400, detail=f"Tile out of bounds for TMS {tms_id} @ {z}."
            )
        return tms_def

    @staticmethod
    def _parse_simplification_rules(
        rules_str: Optional[str],
    ) -> Optional[Dict[int, float]]:
        if not rules_str:
            return None
        try:
            return {int(k): float(v) for k, v in json.loads(rules_str).items()}
        except Exception as e:
            raise HTTPException(
                status_code=400, detail=f"Invalid simplification_by_zoom format: {e}"
            ) from e

    @staticmethod
    def _finalize_response(request: Request, content: bytes) -> Response:
        web_service = get_protocol(WebModuleProtocol)
        if web_service:
            etag = web_service.generate_etag([content])
            if etag == request.headers.get("if-none-match"):
                return Response(status_code=304)
            response = Response(
                content=content, media_type="application/vnd.mapbox-vector-tile"
            )
            response.headers.update(web_service.get_cache_headers())
            response.headers["ETag"] = etag
            return response
        return Response(
            content=content, media_type="application/vnd.mapbox-vector-tile"
        )

    # ------------------------------------------------------------------
    # Map-tile helpers (raster COG → styled PNG/WebP via rio-tiler)
    # ------------------------------------------------------------------

    @staticmethod
    def _require_raster_engine() -> None:
        """Raise 422 when rio-tiler is not installed in this environment."""
        if _RENDER_COG_TILE is None:
            raise HTTPException(
                status_code=422,
                detail=(
                    "Raster map-tile rendering is unavailable in this deployment. "
                    "Install the tiles extension with raster extras "
                    "(rio-tiler, rasterio, lxml)."
                ),
            )

    @staticmethod
    def _validate_style_id(style_id: str) -> None:
        """Raise 400 when style_id contains characters unsafe for cache keys."""
        if not _STYLE_ID_RE.match(style_id):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid style_id {style_id!r}: "
                    "only [A-Za-z0-9._-] are allowed."
                ),
            )

    @staticmethod
    def _parse_multiband_params(
        bands_str: Optional[str],
        expression: Optional[str],
        rescale_str: Optional[str],
    ) -> Tuple[Optional[List[int]], Optional[str], Optional[List[Tuple[float, float]]]]:
        """Parse and validate optional multiband query params."""
        bands: Optional[List[int]] = None
        rescale: Optional[List[Tuple[float, float]]] = None

        if bands_str:
            try:
                bands = [int(b.strip()) for b in bands_str.split(",") if b.strip()]
            except ValueError as exc:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid 'bands' parameter {bands_str!r}: expected comma-separated integers.",
                ) from exc
            if not bands:
                raise HTTPException(
                    status_code=400,
                    detail="'bands' parameter must contain at least one band index.",
                )

        if rescale_str:
            try:
                rescale = []
                for pair in rescale_str.split(";"):
                    pair = pair.strip()
                    if not pair:
                        continue
                    lo_s, hi_s = pair.split(",", 1)
                    rescale.append((float(lo_s.strip()), float(hi_s.strip())))
            except (ValueError, TypeError) as exc:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Invalid 'rescale' parameter {rescale_str!r}: "
                        "expected semicolon-separated 'min,max' pairs."
                    ),
                ) from exc

        return bands, expression, rescale

    @staticmethod
    async def _load_render_caching_config():  # type: ignore[return]
        """Fetch live RenderCachingConfig; fall back to defaults if unavailable."""
        if _RenderCachingConfig is None:
            return None
        from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
        mgr = get_protocol(PlatformConfigsProtocol)
        if mgr is None:
            return _RenderCachingConfig()
        try:
            cfg = await mgr.get_config(_RenderCachingConfig)
        except Exception as exc:
            logger.debug("RenderCachingConfig: get_config failed (%s); using defaults", exc)
            return _RenderCachingConfig()
        return cfg if isinstance(cfg, _RenderCachingConfig) else _RenderCachingConfig()

    @staticmethod
    async def _try_render_cache(
        provider: TileStorageProtocol,
        catalog_id: str,
        cache_key: str,
        tms_id: str,
        z: int,
        x: int,
        y: int,
        fmt: str,
        start: float,
        cfg,
    ) -> Optional[Response]:
        """Return a 307 redirect or proxy response on a cache hit, else None."""
        try:
            url = await provider.get_tile_url(
                catalog_id, cache_key, tms_id, z, x, y, fmt
            )
            if url:
                duration_ms = (time.perf_counter() - start) * 1000
                logger.info(
                    "map_tile: cache=hit source=bucket_redirect catalog=%s "
                    "cache_key=%s z=%s x=%s y=%s duration_ms=%.2f",
                    catalog_id, cache_key, z, x, y, duration_ms,
                )
                return RedirectResponse(
                    url=url,
                    status_code=307,
                    headers={
                        "X-Render-Cache": "hit",
                        "X-Render-Source": "bucket_redirect",
                        "Cache-Control": f"public, max-age={cfg.ttl_seconds}",
                    },
                )

            tile = await provider.get_tile(
                catalog_id, cache_key, tms_id, z, x, y, fmt
            )
            if tile:
                duration_ms = (time.perf_counter() - start) * 1000
                logger.info(
                    "map_tile: cache=hit source=bucket_proxy catalog=%s "
                    "cache_key=%s z=%s x=%s y=%s duration_ms=%.2f bytes=%d",
                    catalog_id, cache_key, z, x, y, duration_ms, len(tile),
                )
                media_type = _FORMAT_MEDIA_TYPE.get(fmt, "application/octet-stream")
                return Response(
                    content=tile,
                    media_type=media_type,
                    headers={
                        "X-Render-Cache": "hit",
                        "X-Render-Source": "bucket_proxy",
                        "Cache-Control": f"public, max-age={cfg.ttl_seconds}",
                    },
                )
        except Exception as exc:
            logger.warning("map_tile: cache lookup failed: %s", exc)
        return None

    async def _resolve_catalog_and_collection(
        self,
        catalog_id: str,
        collection_id: str,
    ) -> Tuple[str, str]:
        """Resolve external → internal catalog and collection IDs.

        Raises HTTPException(404) when either ID is not found.
        Splits ValueError (not found) from AttributeError (test stub) per
        the reviewer fix — ValueError must not be swallowed.
        """
        catalogs_svc = await self._get_catalogs_service()
        try:
            internal_catalog_id = await catalogs_svc.resolve_catalog_id(
                catalog_id, allow_missing=False
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        if not internal_catalog_id:
            raise HTTPException(
                status_code=404, detail=f"Catalog '{catalog_id}' not found."
            )

        try:
            internal_collection_id = await catalogs_svc.collections.resolve_collection_id(
                internal_catalog_id, collection_id, allow_missing=False
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except AttributeError:
            # Test-stub path: stub may not implement resolve_collection_id.
            internal_collection_id = collection_id

        if not internal_collection_id:
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{collection_id}' not found.",
            )
        return internal_catalog_id, internal_collection_id

    # ------------------------------------------------------------------
    # Map-tile route handlers
    # ------------------------------------------------------------------

    async def get_map_tile(
        self,
        request: Request,
        background_tasks: BackgroundTasks,
        catalog_id: str = Path(..., description="Public catalog ID (external_id)."),
        collection_id: str = Path(..., description="Public collection ID (external_id)."),
        tms_id: str = Path(..., description="Tile Matrix Set ID."),
        z: int = Path(..., ge=0, le=30, description="Zoom level."),
        x: int = Path(..., ge=0, description="Tile column."),
        y: int = Path(..., ge=0, description="Tile row."),
        format: str = Path(..., description="Output image format: 'png' or 'webp'."),
        bands: Optional[str] = Query(
            None,
            description="Comma-separated 1-based band indices for multiband rendering.",
        ),
        expression: Optional[str] = Query(
            None,
            description="Band-math expression evaluated by rio-tiler.",
        ),
        rescale: Optional[str] = Query(
            None,
            description="Per-band rescale ranges as semicolon-separated 'min,max' pairs.",
        ),
    ) -> Response:
        """Render a COG map tile using the collection's default style (resolved via binding).

        Flow:
        1. Validate format; ensure rio-tiler is available.
        2. Resolve external catalog/collection IDs to internal IDs.
        3. Check bucket cache.
        4. Resolve the default style via binding, fetch SLD, parse colormap.
        5. Resolve the first COG asset href.
        6. Validate TMS/matrix (upgrades renders' WebMercatorQuad-only frozenset).
        7. Render via run_in_thread(render_cog_tile); write to cache in background.
        """
        from dynastore.modules.concurrency import run_in_thread

        self._require_raster_engine()

        fmt_lower = format.lower()
        if fmt_lower not in _FORMAT_MEDIA_TYPE:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported format '{format}'. Use 'png' or 'webp'.",
            )
        output_format: Literal["PNG", "WEBP"] = "PNG" if fmt_lower == "png" else "WEBP"

        bands_parsed, expression_parsed, rescale_parsed = self._parse_multiband_params(
            bands, expression, rescale
        )

        start = time.perf_counter()

        internal_catalog_id, internal_collection_id = await self._resolve_catalog_and_collection(
            catalog_id, collection_id
        )
        await self._require_collection_visible(internal_catalog_id, internal_collection_id)

        # Validate TMS before cache check (avoids a spurious cache lookup on bad TMS)
        await self._validate_tms_and_matrix(internal_catalog_id, tms_id, z, x, y)

        cfg = await self._load_render_caching_config()
        params_hash = _BUILD_RENDER_PARAMS_HASH(  # type: ignore[misc]
            bands=bands_parsed,
            expression=expression_parsed,
            rescale=rescale_parsed,
        ) if _BUILD_RENDER_PARAMS_HASH else None

        # Resolve default style via binding — needs the first item's properties.
        item = await self._get_first_item(internal_catalog_id, internal_collection_id)
        if not item:
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{collection_id}' has no items.",
            )

        style_id = "default"
        try:
            from dynastore.modules.styles.binding_resolver import resolve_binding_style_id
            item_props = item.get("properties") or item
            resolved = await resolve_binding_style_id(
                internal_catalog_id, internal_collection_id, item_props
            )
            if resolved:
                style_id = resolved
        except Exception as exc:
            logger.debug(
                "map_tile: binding resolver failed for %s/%s: %s — using 'default'",
                internal_catalog_id, internal_collection_id, exc,
            )

        cache_key = _BUILD_RENDER_CACHE_KEY(  # type: ignore[misc]
            cfg.key_prefix if cfg else "",  # cfg is non-None when _BUILD_RENDER_CACHE_KEY is set
            internal_collection_id,
            style_id,
            tms_id,
            z,
            x,
            y,
            fmt_lower,
            params_hash=params_hash,
        ) if _BUILD_RENDER_CACHE_KEY else f"map/{internal_collection_id}/{style_id}/{tms_id}/{z}/{x}/{y}.{fmt_lower}"

        provider = get_protocol(TileStorageProtocol)
        if provider and cfg and cfg.cache_enabled:
            res = await self._try_render_cache(
                provider, internal_catalog_id, cache_key, tms_id, z, x, y, fmt_lower, start, cfg
            )
            if res is not None:
                return res

        # Resolve style colormap
        colormap = None
        from dynastore.models.protocols import StylesProtocol as _StylesProtocol
        styles_svc = get_protocol(_StylesProtocol)
        if styles_svc and style_id != "default":
            style_obj = await styles_svc.get_style(
                internal_catalog_id, internal_collection_id, style_id
            )
            if style_obj and _EXTRACT_SLD_BODY:
                sld_body = _EXTRACT_SLD_BODY(style_obj)
                if sld_body and _PARSE_SLD_COLORMAP:
                    try:
                        colormap = _PARSE_SLD_COLORMAP(sld_body) or None
                    except ValueError as exc:
                        raise HTTPException(
                            status_code=422,
                            detail=f"SLD colormap parse failed: {exc}",
                        ) from exc

        from dynastore.extensions.ogc_base import ogc_asset_href
        cog_href = ogc_asset_href(
            item,
            error_detail=(
                f"No COG asset href found for collection '{collection_id}'. "
                "Ensure at least one item carries a 'data' or 'coverage' asset."
            ),
        )

        logger.info(
            "map_tile: cache=miss catalog=%s collection=%s style=%s tms=%s z=%s x=%s y=%s fmt=%s",
            internal_catalog_id, internal_collection_id, style_id, tms_id, z, x, y, fmt_lower,
        )
        assert _RENDER_COG_TILE is not None  # guaranteed by _require_raster_engine()
        try:
            tile_bytes = await run_in_thread(
                _RENDER_COG_TILE,
                cog_href,
                z,
                x,
                y,
                colormap=colormap,
                output_format=output_format,
                bands=bands_parsed,
                expression=expression_parsed,
                rescale=rescale_parsed,
            )
        except Exception as exc:
            # Import errors or missing rio_tiler — check specific types
            exc_type = type(exc).__name__
            if exc_type == "InvalidExpression":
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid band expression: {exc}",
                ) from exc
            if exc_type == "TileOutsideBounds":
                return Response(status_code=204)
            logger.error(
                "map_tile: rio-tiler failed for %s/%s z=%s x=%s y=%s: %s",
                internal_catalog_id, internal_collection_id, z, x, y, exc,
                exc_info=True,
            )
            raise HTTPException(
                status_code=500,
                detail=f"Raster render failed: {exc}",
            ) from exc

        if provider and cfg and cfg.cache_enabled and tile_bytes:
            background_tasks.add_task(
                provider.save_tile,
                internal_catalog_id,
                cache_key,
                tms_id,
                z,
                x,
                y,
                tile_bytes,
                fmt_lower,
            )

        media_type = _FORMAT_MEDIA_TYPE[fmt_lower]
        return Response(
            content=tile_bytes,
            media_type=media_type,
            headers={
                "X-Render-Cache": "miss",
                "X-Render-Source": "rio-tiler",
                "Cache-Control": f"public, max-age={cfg.ttl_seconds if cfg else 3600}",
            },
        )

    async def get_map_tile_styled(
        self,
        request: Request,
        background_tasks: BackgroundTasks,
        catalog_id: str = Path(..., description="Public catalog ID (external_id)."),
        collection_id: str = Path(..., description="Public collection ID (external_id)."),
        style_id: str = Path(
            ...,
            description=(
                "Style ID. Use 'terrain-rgb' for Terrain-RGB elevation encoding. "
                "Any other ID resolves via StylesProtocol for SLD colormap."
            ),
        ),
        tms_id: str = Path(..., description="Tile Matrix Set ID."),
        z: int = Path(..., ge=0, le=30, description="Zoom level."),
        x: int = Path(..., ge=0, description="Tile column."),
        y: int = Path(..., ge=0, description="Tile row."),
        format: str = Path(..., description="Output image format: 'png' or 'webp'."),
        bands: Optional[str] = Query(
            None,
            description="Comma-separated 1-based band indices for multiband rendering.",
        ),
        expression: Optional[str] = Query(
            None,
            description="Band-math expression evaluated by rio-tiler.",
        ),
        rescale: Optional[str] = Query(
            None,
            description="Per-band rescale ranges as semicolon-separated 'min,max' pairs.",
        ),
        band: int = Query(default=1, ge=1, description="Elevation band index (1-based); used for terrain-rgb and hillshade."),
        relief: Optional[str] = Query(
            None,
            description="Relief mode. Use 'hillshade' to render shaded-relief + colormap.",
        ),
        azimuth: float = Query(default=315.0, ge=0.0, lt=360.0, description="Hillshade sun azimuth in degrees (0=North, clockwise)."),
        altitude: float = Query(default=45.0, ge=0.0, le=90.0, description="Hillshade sun altitude above horizon in degrees."),
    ) -> Response:
        """Render a styled COG map tile with explicit style, terrain-RGB, or hillshade.

        - ``style_id='terrain-rgb'``: Mapbox Terrain-RGB elevation encoding.
        - ``?relief=hillshade``: shaded-relief + hypsometric colormap.
        - All other style IDs: fetch SLD from StylesProtocol, parse colormap.

        Flow:
        1. Validate style_id characters (reject path-traversal).
        2. Validate format; ensure rio-tiler is available.
        3. Resolve external IDs → internal IDs.
        4. Check bucket cache.
        5. Branch on terrain-rgb / hillshade / styled.
        6. Render via run_in_thread; write to cache in background.
        """
        from dynastore.modules.concurrency import run_in_thread

        # Security: reject style_id values that could contaminate the cache key.
        self._validate_style_id(style_id)
        self._require_raster_engine()

        fmt_lower = format.lower()
        if fmt_lower not in _FORMAT_MEDIA_TYPE:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported format '{format}'. Use 'png' or 'webp'.",
            )

        is_terrain_rgb = style_id == "terrain-rgb"
        is_hillshade = (relief or "").lower() == "hillshade"

        # terrain-rgb always PNG; hillshade always PNG
        if is_terrain_rgb or is_hillshade:
            fmt_lower = "png"
        output_format: Literal["PNG", "WEBP"] = "PNG" if fmt_lower == "png" else "WEBP"

        bands_parsed: Optional[List[int]] = None
        expression_parsed: Optional[str] = None
        rescale_parsed: Optional[List[Tuple[float, float]]] = None
        if not is_terrain_rgb and not is_hillshade:
            bands_parsed, expression_parsed, rescale_parsed = self._parse_multiband_params(
                bands, expression, rescale
            )

        start = time.perf_counter()

        internal_catalog_id, internal_collection_id = await self._resolve_catalog_and_collection(
            catalog_id, collection_id
        )
        await self._require_collection_visible(internal_catalog_id, internal_collection_id)

        # Validate TMS before cache check
        await self._validate_tms_and_matrix(internal_catalog_id, tms_id, z, x, y)

        cfg = await self._load_render_caching_config()

        # Build cache key
        if is_terrain_rgb:
            cache_style_segment = "terrain-rgb"
        elif is_hillshade:
            az_int = int(round(azimuth))
            alt_int = int(round(altitude))
            cache_style_segment = f"hillshade-{style_id}-az{az_int}-alt{alt_int}"
        else:
            params_hash = _BUILD_RENDER_PARAMS_HASH(  # type: ignore[misc]
                bands=bands_parsed,
                expression=expression_parsed,
                rescale=rescale_parsed,
            ) if _BUILD_RENDER_PARAMS_HASH else None
            cache_style_segment = f"{style_id}@{params_hash}" if params_hash else style_id

        cache_key = _BUILD_RENDER_CACHE_KEY(  # type: ignore[misc]
            cfg.key_prefix if cfg else "",  # cfg is non-None when _BUILD_RENDER_CACHE_KEY is set
            internal_collection_id,
            cache_style_segment,
            tms_id,
            z,
            x,
            y,
            fmt_lower,
        ) if _BUILD_RENDER_CACHE_KEY else (
            f"map/{internal_collection_id}/{cache_style_segment}/{tms_id}/{z}/{x}/{y}.{fmt_lower}"
        )

        provider = get_protocol(TileStorageProtocol)
        if provider and cfg and cfg.cache_enabled:
            res = await self._try_render_cache(
                provider, internal_catalog_id, cache_key, tms_id, z, x, y, fmt_lower, start, cfg
            )
            if res is not None:
                return res

        # Resolve first COG asset href
        item = await self._get_first_item(internal_catalog_id, internal_collection_id)
        if not item:
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{collection_id}' has no items.",
            )

        from dynastore.extensions.ogc_base import ogc_asset_href
        cog_href = ogc_asset_href(
            item,
            error_detail=f"No COG asset href found for collection '{collection_id}'.",
        )

        # ------------------------------------------------------------------
        # Terrain-RGB branch
        # ------------------------------------------------------------------
        if is_terrain_rgb:
            logger.info(
                "map_tile: terrain-rgb cache=miss catalog=%s collection=%s tms=%s z=%s x=%s y=%s",
                internal_catalog_id, internal_collection_id, tms_id, z, x, y,
            )
            assert _RENDER_COG_TERRAIN_RGB is not None  # guaranteed by _require_raster_engine()
            try:
                tile_bytes = await run_in_thread(
                    _RENDER_COG_TERRAIN_RGB,
                    cog_href,
                    z,
                    x,
                    y,
                    band=band,
                )
            except Exception as exc:
                exc_type = type(exc).__name__
                if exc_type == "TileOutsideBounds":
                    return Response(status_code=204)
                logger.error(
                    "map_tile: terrain-rgb failed for %s/%s z=%s x=%s y=%s: %s",
                    internal_catalog_id, internal_collection_id, z, x, y, exc,
                    exc_info=True,
                )
                raise HTTPException(
                    status_code=500, detail=f"Terrain-RGB render failed: {exc}"
                ) from exc

            if provider and cfg and cfg.cache_enabled and tile_bytes:
                background_tasks.add_task(
                    provider.save_tile,
                    internal_catalog_id,
                    cache_key,
                    tms_id,
                    z,
                    x,
                    y,
                    tile_bytes,
                    "png",
                )
            return Response(
                content=tile_bytes,
                media_type="image/png",
                headers={
                    "X-Render-Cache": "miss",
                    "X-Render-Source": "rio-tiler-terrain-rgb",
                    "Cache-Control": f"public, max-age={cfg.ttl_seconds if cfg else 3600}",
                },
            )

        # ------------------------------------------------------------------
        # Resolve SLD colormap (shared by styled and hillshade paths)
        # ------------------------------------------------------------------
        colormap = None
        from dynastore.models.protocols import StylesProtocol as _StylesProtocol
        styles_svc = get_protocol(_StylesProtocol)
        if styles_svc:
            style_obj = await styles_svc.get_style(
                internal_catalog_id, internal_collection_id, style_id
            )
            if not style_obj and not is_hillshade:
                raise HTTPException(
                    status_code=404,
                    detail=f"Style '{style_id}' not found for collection '{collection_id}'.",
                )
            if style_obj and _EXTRACT_SLD_BODY:
                sld_body = _EXTRACT_SLD_BODY(style_obj)
                if sld_body and _PARSE_SLD_COLORMAP:
                    try:
                        colormap = _PARSE_SLD_COLORMAP(sld_body) or None
                    except ValueError as exc:
                        if not is_hillshade:
                            raise HTTPException(
                                status_code=422,
                                detail=f"SLD colormap parse failed: {exc}",
                            ) from exc
                        logger.warning(
                            "map_tile: hillshade SLD parse failed for style=%s: %s — "
                            "falling back to greyscale",
                            style_id, exc,
                        )
        elif not is_hillshade:
            raise HTTPException(
                status_code=500, detail="Styles service not available."
            )

        # ------------------------------------------------------------------
        # Hillshade branch
        # ------------------------------------------------------------------
        if is_hillshade:
            logger.info(
                "map_tile: hillshade cache=miss catalog=%s collection=%s style=%s "
                "azimuth=%.1f altitude=%.1f tms=%s z=%s x=%s y=%s",
                internal_catalog_id, internal_collection_id, style_id,
                azimuth, altitude, tms_id, z, x, y,
            )
            assert _RENDER_COG_HILLSHADE is not None  # guaranteed by _require_raster_engine()
            try:
                tile_bytes = await run_in_thread(
                    _RENDER_COG_HILLSHADE,
                    cog_href,
                    z,
                    x,
                    y,
                    band=band,
                    azimuth=azimuth,
                    altitude=altitude,
                    colormap=colormap,
                )
            except Exception as exc:
                exc_type = type(exc).__name__
                if exc_type == "InvalidExpression":
                    raise HTTPException(
                        status_code=422,
                        detail=f"Invalid band expression: {exc}",
                    ) from exc
                if exc_type == "TileOutsideBounds":
                    return Response(status_code=204)
                logger.error(
                    "map_tile: hillshade failed for %s/%s z=%s x=%s y=%s: %s",
                    internal_catalog_id, internal_collection_id, z, x, y, exc,
                    exc_info=True,
                )
                raise HTTPException(
                    status_code=500, detail=f"Hillshade render failed: {exc}"
                ) from exc

            if provider and cfg and cfg.cache_enabled and tile_bytes:
                background_tasks.add_task(
                    provider.save_tile,
                    internal_catalog_id,
                    cache_key,
                    tms_id,
                    z,
                    x,
                    y,
                    tile_bytes,
                    "png",
                )
            return Response(
                content=tile_bytes,
                media_type="image/png",
                headers={
                    "X-Render-Cache": "miss",
                    "X-Render-Source": "rio-tiler-hillshade",
                    "Cache-Control": f"public, max-age={cfg.ttl_seconds if cfg else 3600}",
                },
            )

        # ------------------------------------------------------------------
        # Styled raster tile branch
        # ------------------------------------------------------------------
        logger.info(
            "map_tile: cache=miss catalog=%s collection=%s style=%s tms=%s z=%s x=%s y=%s fmt=%s",
            internal_catalog_id, internal_collection_id, style_id, tms_id, z, x, y, fmt_lower,
        )
        assert _RENDER_COG_TILE is not None  # guaranteed by _require_raster_engine()
        try:
            tile_bytes = await run_in_thread(
                _RENDER_COG_TILE,
                cog_href,
                z,
                x,
                y,
                colormap=colormap,
                output_format=output_format,
                bands=bands_parsed,
                expression=expression_parsed,
                rescale=rescale_parsed,
            )
        except Exception as exc:
            exc_type = type(exc).__name__
            if exc_type == "InvalidExpression":
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid band expression: {exc}",
                ) from exc
            if exc_type == "TileOutsideBounds":
                return Response(status_code=204)
            logger.error(
                "map_tile: rio-tiler failed for %s/%s z=%s x=%s y=%s: %s",
                internal_catalog_id, internal_collection_id, z, x, y, exc,
                exc_info=True,
            )
            raise HTTPException(
                status_code=500,
                detail=f"Raster render failed: {exc}",
            ) from exc

        if provider and cfg and cfg.cache_enabled and tile_bytes:
            background_tasks.add_task(
                provider.save_tile,
                internal_catalog_id,
                cache_key,
                tms_id,
                z,
                x,
                y,
                tile_bytes,
                fmt_lower,
            )

        media_type = _FORMAT_MEDIA_TYPE[fmt_lower]
        return Response(
            content=tile_bytes,
            media_type=media_type,
            headers={
                "X-Render-Cache": "miss",
                "X-Render-Source": "rio-tiler",
                "Cache-Control": f"public, max-age={cfg.ttl_seconds if cfg else 3600}",
            },
        )

    async def _invalidate_tile_cache_impl(self, catalog_id: str, collection_id: Optional[str]) -> dict:
        """Shared cache invalidation logic for deprecated and aligned endpoints."""
        delete_tasks = []
        invalidated_targets = []

        if collection_id:
            invalidated_targets = [f"{catalog_id}:{collection_id}"]
            logger.info("Invalidating tile cache for collection: %s", invalidated_targets)
            delete_tasks.append(
                tms_manager.invalidate_collection_tiles(
                    catalog_id=catalog_id, collection_id=collection_id
                )
            )
        else:
            invalidated_targets = [f"{catalog_id} (full catalog)"]
            logger.info("Invalidating tile cache for entire catalog: %s", catalog_id)
            delete_tasks.append(
                tms_manager.invalidate_catalog_tiles(catalog_id=catalog_id)
            )

        await asyncio.gather(*delete_tasks, return_exceptions=True)
        return {
            "message": f"Successfully triggered tile cache invalidation for catalog '{catalog_id}'.",
            "invalidated_targets": invalidated_targets,
        }

    async def invalidate_tile_cache(
        self,
        dataset: str,
        collections: Optional[str] = Query(
            None,
            description="Comma-separated list of collection IDs to invalidate. If not provided, the cache for the entire catalog is cleared.",
        ),
    ):
        """
        Deletes cached tiles from the underlying storage for a given catalog or a subset of its collections.
        This is useful for forcing a refresh of tiles after data updates.
        """
        try:
            delete_tasks = []
            invalidated_targets = []

            if collections:
                collection_list = collections.split(",")
                invalidated_targets = [
                    f"{dataset}:{coll_id}" for coll_id in collection_list
                ]
                logger.info(
                    f"Invalidating tile cache for specific collections: {invalidated_targets}"
                )
                for coll_id in collection_list:
                    delete_tasks.append(
                        tms_manager.invalidate_collection_tiles(
                            catalog_id=dataset, collection_id=coll_id
                        )
                    )
            else:
                invalidated_targets = [f"{dataset} (full catalog)"]
                logger.info(f"Invalidating tile cache for entire catalog: {dataset}")
                delete_tasks.append(
                    tms_manager.invalidate_catalog_tiles(catalog_id=dataset)
                )

            await asyncio.gather(*delete_tasks, return_exceptions=True)

            return {
                "message": f"Successfully triggered tile cache invalidation for catalog '{dataset}'.",
                "invalidated_targets": invalidated_targets,
            }
        except Exception as e:
            logger.error(
                f"Failed to invalidate tile cache for '{dataset}': {e}", exc_info=True
            )
            raise HTTPException(
                status_code=500,
                detail=f"An error occurred during cache invalidation: {e}",
            ) from e

    # ------------------------------------------------------------------
    # Aligned endpoints: /tiles/catalogs/{catalog_id}/collections/{collection_id}/...
    # ------------------------------------------------------------------

    async def get_vector_tile_aligned_default(
        self,
        catalog_id: str,
        collection_id: str,
        z: int,
        x: int,
        y: int,
        request: Request,
        background_tasks: BackgroundTasks,
        conn: AsyncConnection = Depends(get_async_connection),
        datetime: Optional[str] = Query(None),
        filter: Optional[str] = Query(None, description="CQL2 Filter expression."),
        filter_lang: Optional[str] = Query("cql2-text"),
        subset: Optional[str] = Query(None),
        simplification: Optional[float] = Query(None),
        simplification_by_zoom: Optional[str] = Query(None),
        simplification_algorithm: SimplificationAlgorithm = Query(
            SimplificationAlgorithm.TOPOLOGY_PRESERVING
        ),
        disable_cache: bool = Query(False, description="Disable cache for this request."),
        refresh_cache: bool = Query(False, description="Refresh cache by invalidating before fetching."),
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        """Get a vector tile (MVT) using the default WebMercatorQuad TMS.

        catalog_id and collection_id are external (public) IDs resolved to
        internal IDs before dispatch.
        """
        internal_catalog_id, internal_collection_id = await self._resolve_catalog_and_collection(
            catalog_id, collection_id
        )
        return await self.get_vector_tile(
            request=request,
            dataset=internal_catalog_id,
            tileMatrixSetId="WebMercatorQuad",
            z=z,
            x=x,
            y=y,
            format="mvt",
            background_tasks=background_tasks,
            conn=conn,
            collections=internal_collection_id,
            datetime=datetime,
            filter=filter,
            filter_lang=filter_lang,
            subset=subset,
            simplification=simplification,
            simplification_by_zoom=simplification_by_zoom,
            simplification_algorithm=simplification_algorithm,
            disable_cache=disable_cache,
            refresh_cache=refresh_cache,
        )

    async def get_vector_tile_aligned(
        self,
        catalog_id: str,
        collection_id: str,
        tileMatrixSetId: str,
        z: int,
        x: int,
        y: int,
        format: str,
        request: Request,
        background_tasks: BackgroundTasks,
        conn: AsyncConnection = Depends(get_async_connection),
        datetime: Optional[str] = Query(None),
        filter: Optional[str] = Query(None, description="CQL2 Filter expression."),
        filter_lang: Optional[str] = Query("cql2-text"),
        subset: Optional[str] = Query(None),
        simplification: Optional[float] = Query(None),
        simplification_by_zoom: Optional[str] = Query(None),
        simplification_algorithm: SimplificationAlgorithm = Query(
            SimplificationAlgorithm.TOPOLOGY_PRESERVING
        ),
        disable_cache: bool = Query(False, description="Disable cache for this request."),
        refresh_cache: bool = Query(False, description="Refresh cache by invalidating before fetching."),
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        """Get a vector tile for a collection with an explicit TMS (OGC aligned path).

        catalog_id and collection_id are external (public) IDs resolved to
        internal IDs before dispatch.
        """
        internal_catalog_id, internal_collection_id = await self._resolve_catalog_and_collection(
            catalog_id, collection_id
        )
        return await self.get_vector_tile(
            request=request,
            dataset=internal_catalog_id,
            tileMatrixSetId=tileMatrixSetId,
            z=z,
            x=x,
            y=y,
            format=format,
            background_tasks=background_tasks,
            conn=conn,
            collections=internal_collection_id,
            datetime=datetime,
            filter=filter,
            filter_lang=filter_lang,
            subset=subset,
            simplification=simplification,
            simplification_by_zoom=simplification_by_zoom,
            simplification_algorithm=simplification_algorithm,
            disable_cache=disable_cache,
            refresh_cache=refresh_cache,
        )

    def _build_tileset_item(
        self,
        tms_id: str,
        title: Optional[str],
        data_type: Literal["vector", "map", "coverage"],
        self_href: str,
        tms_scheme_href: str,
    ) -> TileSetItem:
        """Construct a TileSetItem with the required self and tiling-scheme links."""
        return TileSetItem(
            id=tms_id,
            dataType=data_type,
            title=title,
            links=[
                Link(
                    href=self_href,
                    rel="self",
                    type="application/json",
                    title=title,
                    hreflang="en",
                ),
                Link(
                    href=tms_scheme_href,
                    rel="http://www.opengis.net/def/rel/ogc/1.0/tiling-scheme",
                    type="application/json",
                    title=title,
                    hreflang="en",
                ),
            ],
        )

    async def get_collection_tilesets(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
    ) -> TileSetList:
        """List available tilesets for a collection (OGC API Tiles §7.1).

        Returns an OGC API Tiles tilesets list where each entry carries
        ``dataType`` (``'vector'`` for MVT tilesets, ``'map'`` for raster
        map-tile tilesets) and the required links:

        - ``rel='self'`` → tileset-metadata resource for this TMS.
        - ``rel='http://www.opengis.net/def/rel/ogc/1.0/tiling-scheme'`` →
          the TileMatrixSet definition document.

        Resolves external IDs and enforces collection visibility.
        """
        internal_catalog_id, internal_collection_id = await self._resolve_catalog_and_collection(
            catalog_id, collection_id
        )
        await self._require_collection_visible(internal_catalog_id, internal_collection_id)

        # Collect all applicable TMS ids (built-in + per-catalog custom)
        all_tms: List[Tuple[str, Optional[str]]] = [
            (tms_id, tms_def.title)
            for tms_id, tms_def in BUILTIN_TILE_MATRIX_SETS.items()
        ]
        seen_ids = {tms_id for tms_id, _ in all_tms}
        custom_tms_list = await tms_manager.list_custom_tms(catalog_id=internal_catalog_id)
        for tms in custom_tms_list:
            if tms.id not in seen_ids:
                all_tms.append((tms.id, tms.title))
                seen_ids.add(tms.id)

        tilesets: List[TileSetItem] = []
        for tms_id, title in all_tms:
            tms_scheme_href = str(
                request.url_for("get_tile_matrix_set", tileMatrixSetId=tms_id)
            )
            # Vector tileset (MVT) — dataType='vector'
            tilesets.append(
                self._build_tileset_item(
                    tms_id=tms_id,
                    title=title,
                    data_type="vector",
                    self_href=str(
                        request.url_for(
                            "get_collection_tileset",
                            catalog_id=catalog_id,
                            collection_id=collection_id,
                            tileMatrixSetId=tms_id,
                        )
                    ),
                    tms_scheme_href=tms_scheme_href,
                )
            )
            # Map tileset (raster/COG render) — dataType='map'
            tilesets.append(
                self._build_tileset_item(
                    tms_id=tms_id,
                    title=title,
                    data_type="map",
                    self_href=str(
                        request.url_for(
                            "get_collection_map_tileset",
                            catalog_id=catalog_id,
                            collection_id=collection_id,
                            tms_id=tms_id,
                        )
                    ),
                    tms_scheme_href=tms_scheme_href,
                )
            )

        return TileSetList(tilesets=tilesets)

    async def get_collection_tileset(
        self,
        catalog_id: str,
        collection_id: str,
        tileMatrixSetId: str,
        request: Request,
    ) -> TileSetItem:
        """Tileset metadata for vector tiles of a collection (OGC API Tiles §7.2).

        Returns a TileSet document with ``dataType='vector'`` for the requested
        TileMatrixSet.  Resolves external IDs and enforces collection visibility.
        """
        internal_catalog_id, internal_collection_id = await self._resolve_catalog_and_collection(
            catalog_id, collection_id
        )
        await self._require_collection_visible(internal_catalog_id, internal_collection_id)

        # Resolve title from built-in TMS or custom TMS
        tms_def = BUILTIN_TILE_MATRIX_SETS.get(tileMatrixSetId)
        title: Optional[str] = tms_def.title if tms_def else None
        if title is None:
            custom = await tms_manager.get_custom_tms(
                catalog_id=internal_catalog_id, tms_id=tileMatrixSetId
            )
            if custom is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"TileMatrixSet '{tileMatrixSetId}' not found.",
                )
            title = custom.title

        return self._build_tileset_item(
            tms_id=tileMatrixSetId,
            title=title,
            data_type="vector",
            self_href=str(
                request.url_for(
                    "get_collection_tileset",
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    tileMatrixSetId=tileMatrixSetId,
                )
            ),
            tms_scheme_href=str(
                request.url_for("get_tile_matrix_set", tileMatrixSetId=tileMatrixSetId)
            ),
        )

    async def get_collection_map_tileset(
        self,
        catalog_id: str,
        collection_id: str,
        tms_id: str,
        request: Request,
    ) -> TileSetItem:
        """Tileset metadata for raster map tiles of a collection (OGC API Maps §7.2).

        Returns a TileSet document with ``dataType='map'`` for the requested
        TileMatrixSet.  Resolves external IDs and enforces collection visibility.
        """
        internal_catalog_id, internal_collection_id = await self._resolve_catalog_and_collection(
            catalog_id, collection_id
        )
        await self._require_collection_visible(internal_catalog_id, internal_collection_id)

        tms_def = BUILTIN_TILE_MATRIX_SETS.get(tms_id)
        title: Optional[str] = tms_def.title if tms_def else None
        if title is None:
            custom = await tms_manager.get_custom_tms(
                catalog_id=internal_catalog_id, tms_id=tms_id
            )
            if custom is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"TileMatrixSet '{tms_id}' not found.",
                )
            title = custom.title

        return self._build_tileset_item(
            tms_id=tms_id,
            title=title,
            data_type="map",
            self_href=str(
                request.url_for(
                    "get_collection_map_tileset",
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    tms_id=tms_id,
                )
            ),
            tms_scheme_href=str(
                request.url_for("get_tile_matrix_set", tileMatrixSetId=tms_id)
            ),
        )

    async def invalidate_collection_tile_cache(
        self,
        catalog_id: str,
        collection_id: str,
    ):
        """Invalidate the tile cache for a specific collection (OGC aligned path).

        Resolves external catalog_id and collection_id to internal IDs before
        dispatching to the shared invalidation logic.
        """
        try:
            internal_catalog_id, internal_collection_id = await self._resolve_catalog_and_collection(
                catalog_id, collection_id
            )
            return await self._invalidate_tile_cache_impl(internal_catalog_id, internal_collection_id)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(
                "Failed to invalidate tile cache for '%s/%s': %s", catalog_id, collection_id, e,
                exc_info=True,
            )
            raise HTTPException(
                status_code=500,
                detail=f"An error occurred during cache invalidation: {e}",
            ) from e
