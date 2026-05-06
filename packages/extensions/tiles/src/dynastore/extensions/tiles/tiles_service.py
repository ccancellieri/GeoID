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

# dynastore/extensions/tiles/tiles_service.py

import logging
import json
import asyncio
import hashlib
import time
from typing import Optional, Dict, List, Any
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
from fastapi.responses import HTMLResponse, RedirectResponse
from dynastore.extensions.tools.fast_api import AppJSONResponse as JSONResponse
from sqlalchemy.ext.asyncio import AsyncConnection
from pyproj import CRS

from dynastore.extensions import protocols, get_extension_instance
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols.configs import ConfigsProtocol
from dynastore.models.protocols.web import WebModuleProtocol, StaticFilesProtocol
from dynastore.extensions.web.decorators import expose_static
from dynastore.extensions.tools.db import get_async_connection
from .policies import register_tiles_policies
from dynastore.modules.db_config import shared_queries
import dynastore.modules.catalog.catalog_module as catalog_manager
import dynastore.modules.tiles.tiles_module as tms_manager
from dynastore.tools.ogc_common import parse_subset_parameter
from dynastore.tools.geospatial import SimplificationAlgorithm
from dynastore.extensions.web import expose_web_page
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
)
from dynastore.modules.tiles.tms_definitions import BUILTIN_TILE_MATRIX_SETS

logger = logging.getLogger(__name__)

OGC_API_TILES_URIS = [
    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tileset",
    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tilesets-list",
    "http://www.opengis.net/spec/tms/2.0/conf/tilematrixset",
    "http://www.opengis.net/spec/tms/2.0/conf/json-tilematrixset",
    "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/mvt",
]


class TilesService(protocols.ExtensionProtocol, StaticFilesProtocol):
    priority: int = 100
    """
    Provides OGC API - Tiles functionality.
    Uses PyProj for CRS handling and PostGIS for dynamic MVT generation.
    """

    conformance_uris: List[str] = OGC_API_TILES_URIS
    router: APIRouter

    def get_web_pages(self):
        from dynastore.extensions.tools.web_collect import collect_web_pages
        return collect_web_pages(self)

    def get_static_assets(self):
        from dynastore.extensions.tools.web_collect import collect_static_assets
        return collect_static_assets(self)

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
        # Tile Matrix Sets
        self.router.add_api_route(
            "/{dataset}/tileMatrixSets", self.create_tile_matrix_set, methods=["POST"],
            response_model=TileMatrixSet, status_code=201, summary="Create a custom Tile Matrix Set"
        )
        self.router.add_api_route(
            "/tileMatrixSets", self.get_tile_matrix_sets, methods=["GET"],
            response_model=TileMatrixSetList, summary="Retrieve available Tile Matrix Sets"
        )
        self.router.add_api_route(
            "/tileMatrixSets/{tileMatrixSetId}", self.get_tile_matrix_set, methods=["GET"],
            response_model=TileMatrixSet, summary="Retrieve a Tile Matrix Set definition"
        )

        # Tile Content
        self.router.add_api_route(
            "/catalogs/{dataset}/tiles/{z}/{x}/{y}.mvt", self.get_vector_tile_catalog_default, methods=["GET"],
            summary="Catalog-centric MVT Endpoint"
        )
        self.router.add_api_route(
            "/catalogs/{dataset}/tiles/{tileMatrixSetId}/{z}/{x}/{y}.{format}", self.get_vector_tile_catalog, methods=["GET"],
            summary="Catalog-centric MVT with TMS"
        )
        self.router.add_api_route(
            "/{dataset}/tiles/{z}/{x}/{y}.mvt", self.get_vector_tile_default, methods=["GET"],
            summary="Legacy MVT Endpoint"
        )
        self.router.add_api_route(
            "/{dataset}/tiles/{tileMatrixSetId}/{z}/{x}/{y}.{format}", self.get_vector_tile, methods=["GET"],
            summary="Get filtered MVT"
        )

        # Cache Management
        self.router.add_api_route(
            "/{dataset}/tiles/cache", self.invalidate_tile_cache, methods=["DELETE"],
            status_code=200, summary="Invalidate tile cache for a catalog or specific collections"
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
        register_tiles_policies()
        logger.info("Tiles Service startup: policies registered.")
        yield
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
            return Response(content=f"Template map.html not found", status_code=404)
        with open(file_path, "r", encoding="utf-8") as f:
            return Response(content=f.read(), media_type="text/html")

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
            tiles_config = await self._resolve_request_config(
                config_manager, dataset
            )

            requested_cols_list = [c.strip() for c in collections.split(",")]

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
                f"Tile cache MISS: {dataset}/{collections}/{z}/{x}/{y} (cache_enabled={effective_cache_enabled})"
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
                )

            # Resolve metadata for each collection (Cached in TilesModule)
            from dynastore.modules.tiles import tiles_module

            resolved_collections = []
            for coll_id in requested_cols_list:
                # get_tile_resolution_params is cached and validates existence
                meta = await tiles_module.get_tile_resolution_params(dataset, coll_id)
                if meta:
                    resolved_collections.append(meta)

            if not resolved_collections:
                logger.info(f"No valid collections found for {dataset}/{collections}")
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
                            from fastapi.responses import Response as FResponse
                            return FResponse(
                                content=tile_bytes,
                                media_type="application/vnd.mapbox-vector-tile",
                                headers={"X-Tile-Source": "pmtiles-archive"},
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
                logger.info(
                    f"Tile generated (empty): {dataset}/{collections}/{z}/{x}/{y} in {(time.perf_counter() - start_time) * 1000:.2f}ms"
                )
                return Response(status_code=204)

            logger.info(
                f"Tile generated: {dataset}/{collections}/{z}/{x}/{y} in {(time.perf_counter() - start_time) * 1000:.2f}ms. Size: {len(mvt_content)}"
            )

            return self._finalize_response(request, mvt_content)

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"CRITICAL ERROR in get_vector_tile: {e}", exc_info=True)
            raise HTTPException(
                status_code=500, detail=f"Internal Server Error: {str(e)}"
            )

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
                return RedirectResponse(url=url, status_code=307)

            tile = await provider.get_tile(dataset, cache_id, tms_id, z, x, y, format)
            if tile:
                logger.info(
                    f"Tile cache HIT (Proxy): {dataset}/{cache_id}/{z}/{x}/{y} in {(time.perf_counter() - start_time) * 1000:.2f}ms"
                )
                return Response(
                    content=tile, media_type="application/vnd.mapbox-vector-tile"
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
        except Exception:
            raise HTTPException(
                status_code=400, detail="Invalid simplification_by_zoom format."
            )

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
            # Instead of interacting with a specific module (GCP), we use the TileStorageSPI.
            # We call the public API functions from the tiles_module.

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
                # If no collections are specified, invalidate the entire catalog's tile storage.
                invalidated_targets = [f"{dataset} (full catalog)"]
                logger.info(f"Invalidating tile cache for entire catalog: {dataset}")
                delete_tasks.append(
                    tms_manager.invalidate_catalog_tiles(catalog_id=dataset)
                )

            # Execute all cleanup tasks concurrently.
            # The results from the event handlers are primarily for logging and not aggregated here.
            await asyncio.gather(*delete_tasks, return_exceptions=True)

            # The number of deleted items is not easily aggregated here since the event handlers
            # log the details per provider. The main goal is to confirm the action was triggered.
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
            )
