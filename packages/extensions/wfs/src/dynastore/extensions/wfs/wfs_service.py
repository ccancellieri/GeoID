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

# dynastore/extensions/wfs/wfs_service.py
import asyncio

from dynastore.tools.discovery import get_protocol
from dynastore.models.driver_context import DriverContext
from ...tools.features import Feature, FeatureCollection
import logging
import re
from typing import List, Optional, cast, Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response, Query, FastAPI
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncConnection
from contextlib import asynccontextmanager

from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.db_config.exceptions import TableNotFoundError, SchemaNotFoundError

from dynastore.extensions.tools.db import get_async_connection, get_async_engine
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols import (
    CatalogsProtocol,
    StorageProtocol,
    ConfigsProtocol,
    ItemsProtocol,
)
from . import wfs_generator, wfs_db
from .wfs_models import WFSException
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.exception_handlers import (
    ExceptionHandler,
    register_extension_handler,
)
from .wfs_config import WFSPluginConfig

# --- Refactoring Step ---
# Import the centralized formatting tools.
from dynastore.extensions.tools.formatters import OutputFormatEnum, format_response
from dynastore.extensions.tools.fast_api import _parse_srid_from_srs_name
import pyproj as _pyproj_scope_gate  # noqa: F401  # SCOPE gate: extension_wfs requires pyproj
_ = _pyproj_scope_gate  # silence pyright "unused" — load-bearing for SCOPE filtering
from dynastore.extensions.tools.url import get_root_url
from dynastore.extensions.tools.language_utils import get_language

from dynastore.modules.catalog.models import Catalog, Collection
from dynastore.models.shared_models import Link
from dynastore.models.localization import LocalizedText
from dynastore.extensions.tools.query import parse_ogc_query_request, stream_ogc_features

logger = logging.getLogger(__name__)


class WFSGlobalExceptionHandler(ExceptionHandler):
    """
    Handles WFS-specific exceptions and returns a standard OGC ExceptionReport.
    Integrated with the global exception registry.
    """

    def can_handle(self, exception: Exception) -> bool:
        return isinstance(exception, WFSException)

    def handle(
        self, exception: Exception, context: Optional[dict] = None
    ) -> Optional[Response]:
        """
        Convert WFSException to OGC XML Response.
        """
        # Ensure it is a WFSException (mypy)
        if not isinstance(exception, WFSException):
            return None

        logger.warning(
            f"WFS operation failed due to client error: {exception.message} (Code: {exception.code}, Locator: {exception.locator})"
        )

        # Generates standard ExceptionReport XML
        xml_content = wfs_generator.create_exception_report(
            exception.code, exception.locator, exception.message
        )

        # WFS errors are typically returned with a 200 OK or 400 status code
        return Response(
            content=xml_content, media_type="application/xml", status_code=400
        )


# Map WFS outputFormat to our internal enum
format_map = {
    "application/json": OutputFormatEnum.GEOJSON,
    "text/csv": OutputFormatEnum.CSV,
    "application/vnd.geopackage+sqlite3": OutputFormatEnum.GEOPACKAGE,
    "application/x-sqlite3": OutputFormatEnum.GEOPACKAGE,
    "application/octet-stream": OutputFormatEnum.PARQUET,
    "application/zip": OutputFormatEnum.SHAPEFILE,
    "application/gml+xml": OutputFormatEnum.GML,
    "application/gml+xml; version=3.2": OutputFormatEnum.GML,
}
class WFSService(ExtensionProtocol):
    priority: int = 100
    """
    Provides a comprehensive OGC Web Feature Service (WFS) 2.0 interface. This
    extension is crucial for interoperability with traditional desktop GIS clients
    like QGIS and ArcGIS. It supports core operations including GetCapabilities,
    GetFeature, and DescribeFeatureType through both a root-level endpoint and
    catalog-specific endpoints for more RESTful interactions.
    """

    router: Optional[APIRouter] = APIRouter(prefix="/wfs", tags=["OGC WFS 2.0"])

    async def _get_catalogs_service(self) -> CatalogsProtocol:
        """Helper to get the catalogs service protocol."""
        if self._catalogs_protocol is None:
            svc = get_protocol(CatalogsProtocol)
            if not svc:
                raise HTTPException(
                    status_code=500, detail="Catalogs service not available."
                )
            self._catalogs_protocol = svc
        return self._catalogs_protocol

    async def _get_storage_service(self) -> Optional[StorageProtocol]:
        """Helper to get the storage service protocol."""
        if self._storage_protocol is None:
            self._storage_protocol = get_protocol(StorageProtocol)
        return self._storage_protocol

    async def _get_configs_service(self) -> ConfigsProtocol:
        """Helper to get the configs service protocol."""
        if self._configs_protocol is None:
            svc = get_protocol(ConfigsProtocol)
            if not svc:
                raise HTTPException(
                    status_code=500, detail="Configs service not available."
                )
            self._configs_protocol = svc
        return self._configs_protocol

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """Lifecycle hook."""
        logger.info("WFSService initialized.")
        yield

    def configure_app(self, app: FastAPI):
        """
        Registers the WFS-specific exception handler with the main FastAPI application.
        This is the correct lifecycle hook for adding app-level configurations.
        """
        # Register WFS handler with global registry.
        # We prepend to ensure it takes precedence over generic handlers
        # (in case WFSException inherits from generic types like ValueError).
        # Note: This technically places it before the Logging handler if both are prepended,
        # but the logging handler is registered during app startup (setup_exception_handlers).
        # Extensions initialize later.
        # If we prepend now, WFS comes before Logging.
        # To fix this, we'll append, but we must verify WFSException inheritance.
        # Assuming for now we append to play nice with Logging handler which is at index 0.
        # Wait, if WFSException inherits ValueError, we MUST prepend or be before ValidationHandler.
        # ValidationHandler is last in builtin list.
        # So appending puts WFS AFTER ValidationHandler.
        # This is RISKY if inheritance exists.

        # Safe bet: Prepend, and add DB logging inside WFS handler or accept that WFS uses its own logging for now.
        # The WFS handler *does* log to logger.warning.
        # Let's prepend to be safe for WFS XML requirement.
        register_extension_handler(WFSGlobalExceptionHandler(), prepend=True)

    @staticmethod
    def _get_case_insensitive_params(request: Request) -> dict:
        """Returns a dictionary of query parameters with case-insensitive keys."""
        return {k.lower(): v for k, v in request.query_params.items()}

    @staticmethod
    def _get_root_wfs_url(request: Request) -> str:
        """
        Determines the root WFS service URL (e.g., http://.../wfs) using the
        shared URL generation utility.
        """
        # This helper function correctly reconstructs the root URL
        # even if the request came to /wfs/catalog1
        return f"{get_root_url(request)}/wfs"

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self._catalogs_protocol: Optional[CatalogsProtocol] = None
        self._storage_protocol: Optional[StorageProtocol] = None
        self._configs_protocol: Optional[ConfigsProtocol] = None
        """Initializes the service and registers its dual API route structure."""
        self._register_routes()

    def _register_routes(self):
        """
        Registers two distinct entry points to handle WFS requests:
        1. A root endpoint (`/wfs`) for service-wide discovery (GetCapabilities).
        2. A scoped endpoint (`/wfs/{catalog_id}`) for operations within a specific catalog.

        This dual structure provides flexibility for different client behaviors.
        """
        if self.router is None:
            return
        # The root endpoint is disabled to enforce catalog-scoped requests.
        self.router.add_api_route("", self.handle_root_wfs_request, methods=["GET"])
        self.router.add_api_route(
            "/{catalog_id}", self.handle_scoped_wfs_request, methods=["GET"]
        )

    async def _dispatch_request(
        self,
        request: Request,
        conn: AsyncConnection,
        params: dict,
        catalog_id_from_path: Optional[str] = None,
        language: str = "en",
    ):
        """
        Internal dispatcher that routes a WFS request to the correct handler.
        It validates common parameters and handles unsupported operations.
        """
        service = params.get("service", "").upper()
        request_type = params.get("request", "").lower()

        if service != "WFS":
            xml = wfs_generator.create_exception_report(
                "InvalidParameterValue",
                "service",
                "The 'service' parameter must be 'WFS'.",
            )
            return Response(content=xml, media_type="application/xml", status_code=400)

        root_wfs_url = self._get_root_wfs_url(request)

        if request_type == "getcapabilities":
            return await self.get_wfs_capabilities(
                request, conn, root_wfs_url, catalog_id_from_path, language=language
            )
        elif request_type == "getfeature":
            return await self.get_feature(
                request, conn, params, root_wfs_url, catalog_id_from_path
            )
        elif request_type == "describefeaturetype":
            return await self.describe_feature_type(
                request, conn, params, root_wfs_url, catalog_id_from_path
            )
        else:
            logger.warning(
                f"Unsupported WFS request type received: '{params.get('request')}'"
            )
            xml = wfs_generator.create_exception_report(
                "OperationNotSupported",
                "request",
                f"The operation '{params.get('request')}' is not supported.",
            )
            return Response(content=xml, media_type="application/xml", status_code=501)

    async def handle_root_wfs_request(
        self,
        request: Request,
        conn: AsyncConnection = Depends(get_async_connection),
        language: str = Depends(get_language),
    ):
        """Handles requests to the root `/wfs` endpoint."""
        return await self._dispatch_request(
            request, conn, self._get_case_insensitive_params(request), language=language
        )

    async def handle_scoped_wfs_request(
        self,
        request: Request,
        catalog_id: str,
        conn: AsyncConnection = Depends(get_async_connection),
        language: str = Depends(get_language),
    ):
        """Handles requests scoped to a specific catalog, e.g., `/wfs/my_catalog`."""
        safe_catalog_id = catalog_id.replace(":", "_").replace("-", "_")
        catalogs_svc = await self._get_catalogs_service()
        if not await catalogs_svc.get_catalog(safe_catalog_id, ctx=DriverContext(db_resource=conn)):
            raise HTTPException(
                status_code=404, detail=f"Catalog '{safe_catalog_id}' not found."
            )
        return await self._dispatch_request(
            request,
            conn,
            self._get_case_insensitive_params(request),
            catalog_id_from_path=safe_catalog_id,
            language=language,
        )

    async def get_wfs_capabilities(
        self,
        request: Request,
        conn: AsyncConnection,
        root_wfs_url: str,
        catalog_id: Optional[str] = None,
        language: str = "en",
    ):
        """
        Generates the WFS GetCapabilities XML response. If a `catalog_id` is
        provided (from a scoped request), it lists only the collections within
        that catalog. Otherwise, it generates a service-wide capabilities
        document listing all catalogs and their collections.
        """
        service_metadata = None
        catalogs_with_collections = {}

        catalogs_svc = await self._get_catalogs_service()
        configs_svc = await self._get_configs_service()

        # Phase 1.6: collection_type was hoisted out of ItemsPostgresqlDriverConfig
        # into its own CollectionInfo PluginConfig. Fetch from configs_svc instead
        # of reading driver config (which no longer exposes the attribute).
        from dynastore.modules.catalog.catalog_config import (
            CollectionInfo, CollectionKind,
        )

        async def _is_vector(cat_id: str, coll_id: str) -> bool:
            ct = await configs_svc.get_config(
                CollectionInfo, catalog_id=cat_id, collection_id=coll_id,
            ) if configs_svc else None
            # Default per CollectionInfo.kind = VECTOR — missing config means
            # vector, matching the pre-Phase-1.6 "no driver config → include" path.
            return ct is None or ct.kind == CollectionKind.VECTOR

        if catalog_id:
            # Scoped request: only fetch collections for the specified catalog.
            # Sequential awaits — both calls share `conn` (the request's
            # asyncpg Connection); concurrent SELECTs on the same wire
            # deadlock asyncpg's single-stream protocol (regression observed
            # in PRs #28, #32, #43).
            catalog_metadata = await catalogs_svc.get_catalog(
                catalog_id, ctx=DriverContext(db_resource=conn)
            )
            all_collections_summary = await catalogs_svc.list_collections(
                catalog_id, limit=1000, ctx=DriverContext(db_resource=conn)
            )
            if catalog_metadata:
                # Use the catalog's metadata for the service identification block.
                service_metadata, _ = catalog_metadata.localize(language)

            vector_collections = []
            for c_summary in all_collections_summary:
                if await _is_vector(catalog_id, c_summary.id):
                    full_collection_details = await catalogs_svc.get_collection(
                        catalog_id, c_summary.id, ctx=DriverContext(db_resource=conn)
                    )
                    if full_collection_details:
                        localized, _ = full_collection_details.localize(language)
                        vector_collections.append(localized)
            if vector_collections:
                catalogs_with_collections[catalog_id] = vector_collections
        else:
            # Root request: fetch all catalogs and their respective collections.
            catalogs = await catalogs_svc.list_catalogs(limit=1000, ctx=DriverContext(db_resource=conn))
            for catalog in catalogs:
                all_collections_summary = await catalogs_svc.list_collections(
                    catalog.id, limit=1000, ctx=DriverContext(db_resource=conn)
                )
                vector_collections = []
                for c_summary in all_collections_summary:
                    if await _is_vector(catalog.id, c_summary.id):
                        full_collection_details = await catalogs_svc.get_collection(
                            catalog.id, c_summary.id, ctx=DriverContext(db_resource=conn)
                        )
                        if full_collection_details:
                            localized, _ = full_collection_details.localize(language)
                            vector_collections.append(localized)
                if vector_collections:
                    catalogs_with_collections[catalog.id] = vector_collections

        # The generator already handles localization based on the localized dictionaries passed.
        xml_content = wfs_generator.create_capabilities_response(
            root_wfs_url,
            catalog_id,
            catalogs_with_collections,
            service_metadata,
            language,
        )

        return Response(content=xml_content, media_type="application/xml")

    async def describe_feature_type(
        self,
        request: Request,
        conn: AsyncConnection,
        params: dict,
        root_wfs_url: str,
        catalog_id_from_path: Optional[str] = None,
    ):
        """Handles WFS DescribeFeatureType requests, providing an XSD schema for a feature type."""
        typename = params.get("typename") or params.get(
            "typenames"
        )  # Support both casings
        if not typename:
            xml = wfs_generator.create_exception_report(
                "MissingParameterValue",
                "TYPENAME",
                "The 'TYPENAME' parameter is required.",
            )
            return Response(content=xml, media_type="application/xml", status_code=400)

        try:
            schema_prefix, collection_id = typename.split(":")

        except ValueError:
            xml = wfs_generator.create_exception_report(
                "InvalidParameterValue",
                "TYPENAME",
                "TYPENAME must be in 'catalog:collection' format.",
            )
            return Response(content=xml, media_type="application/xml", status_code=400)

        if catalog_id_from_path and catalog_id_from_path != schema_prefix:
            xml = wfs_generator.create_exception_report(
                "InvalidParameterValue",
                "TYPENAME",
                f"The feature type '{typename}' does not belong to the catalog '{catalog_id_from_path}'.",
            )
            return Response(content=xml, media_type="application/xml", status_code=400)

        # Introspect using logical IDs; ItemService handles sidecar aggregation.
        async with managed_transaction(conn) as db_conn:
            feature_schema = await wfs_db.introspect_feature_type_schema(
                cast(AsyncConnection, db_conn),
                catalog_id=schema_prefix,
                collection_id=collection_id,
            )

        if not feature_schema:
            xml = wfs_generator.create_exception_report(
                "InvalidParameterValue",
                "TYPENAME",
                f"Feature type '{typename}' not found.",
            )
            return Response(content=xml, media_type="application/xml", status_code=404)

        # This resolves the ".../catalog1/catalog1" bug
        target_namespace_url = f"{root_wfs_url}/{schema_prefix}"
        xml_content = wfs_generator.create_describe_feature_type_response(
            schema_prefix, collection_id, target_namespace_url, feature_schema
        )

        return Response(content=xml_content, media_type="application/xsd")

    async def get_feature(
        self,
        request: Request,
        conn: AsyncConnection,
        params: dict,
        root_wfs_url: str,
        catalog_id_from_path: Optional[str] = None,
    ):
        """Handles WFS GetFeature requests, returning features as a GML FeatureCollection."""
        typename = params.get("typename") or params.get(
            "typenames"
        )  # Support both casings
        bbox_str = params.get("bbox")
        count = int(params.get("count", 1000))
        output_format_str = params.get(
            "outputformat", "application/gml+xml; version=3.2"
        )
        time_str = params.get("time")
        property_name_str = params.get("propertyname")
        feature_id_str = params.get("featureid")
        cql_filter = params.get("cql_filter")
        sort_by_str = params.get("sortby")
        start_index = int(params.get("startindex", 0))

        # Resolve output format early to support empty collection responses
        normalized_format = output_format_str.lower()
        # Handle "application/json" explicitly if not in map
        if normalized_format == "application/json":
            format_enum = OutputFormatEnum.GEOJSON
        else:
            format_enum = format_map.get(normalized_format, OutputFormatEnum.GML)

        result_type = params.get("resulttype", "results").lower()

        if not typename:
            xml = wfs_generator.create_exception_report(
                "MissingParameterValue",
                "TYPENAME",
                "The 'TYPENAME' parameter is required.",
            )
            return Response(content=xml, media_type="application/xml", status_code=400)

        # --- Caching Support ---
        catalogs_svc = await self._get_catalogs_service()
        configs_svc = await self._get_configs_service()
        storage_svc = await self._get_storage_service()

        plugin_config = cast(WFSPluginConfig, await configs_svc.get_config(
            WFSPluginConfig, catalog_id=catalog_id_from_path, ctx=DriverContext(db_resource=conn
        )))

        cache_key = None
        if plugin_config.cache_on_demand and storage_svc:
            import hashlib

            # Generate a stable cache key based on sorted parameters
            cache_params = {
                k: v
                for k, v in params.items()
                if k not in ("_", "access_token", "token")
            }
            param_str = "&".join(f"{k}={v}" for k, v in sorted(cache_params.items()))
            cache_key_hash = hashlib.md5(param_str.encode()).hexdigest()

            catalog_id = catalog_id_from_path or "global"
            bucket_name = await storage_svc.get_storage_identifier(catalog_id)
            if bucket_name:
                cache_key = f"gs://{bucket_name}/wfs_cache/{cache_key_hash}"

                if await storage_svc.file_exists(cache_key):
                    logger.info(f"WFS Cache HIT: {cache_key}")
                    import tempfile

                    with tempfile.NamedTemporaryFile() as tmp:
                        await storage_svc.download_file(cache_key, tmp.name)
                        tmp.seek(0)
                        cached_data = tmp.read()

                    return Response(content=cached_data, media_type=normalized_format)

        try:
            schema_prefix, collection_id = typename.split(":")

        except ValueError:
            xml = wfs_generator.create_exception_report(
                "InvalidParameterValue",
                "TYPENAME",
                "TYPENAME must be in 'catalog:collection' format.",
            )
            return Response(content=xml, media_type="application/xml", status_code=400)

        if catalog_id_from_path and catalog_id_from_path != schema_prefix:
            xml = wfs_generator.create_exception_report(
                "InvalidParameterValue",
                "TYPENAME",
                f"The feature type '{typename}' does not belong to the catalog '{catalog_id_from_path}'.",
            )
            return Response(content=xml, media_type="application/xml", status_code=400)

        catalogs_svc = await self._get_catalogs_service()

        # --- Parameter Mapping (WFS KVPs to Unified OGC Params) ---
        bbox_parts = bbox_str.split(",") if bbox_str else []
        bbox_val = ",".join(bbox_parts[:4]) if len(bbox_parts) >= 4 else None
        
        # Resolve BBOX SRID from WFS-specific URN or srsname
        bbox_crs_srid = None
        bbox_crs_str = bbox_parts[4] if len(bbox_parts) >= 5 else params.get("srsname")
        if bbox_crs_str:
            match = re.search(r"EPSG::(\d+)", bbox_crs_str)
            if match:
                bbox_crs_srid = int(match.group(1))

        request_obj = parse_ogc_query_request(
            bbox=bbox_val,
            datetime_param=time_str,
            sortby=sort_by_str,
            filter=cql_filter,
            item_ids=feature_id_str,
            limit=count,
            offset=start_index,
            bbox_crs_srid=bbox_crs_srid,
            include_total_count=True,
        )

        target_namespace_url = f"{root_wfs_url}/{schema_prefix}"

        # Execute search via ItemsProtocol (Streaming support)
        items_svc = cast(ItemsProtocol, catalogs_svc)
        try:
            query_response = await items_svc.stream_items(
                catalog_id=schema_prefix,
                collection_id=collection_id,
                request=request_obj,
                # Decouple from request connection to allow background streaming
                # without premature closure errors.
                ctx=None,
            )
        except ValueError as e:
            xml = wfs_generator.create_exception_report("InvalidParameterValue", None, str(e))
            return Response(content=xml, media_type="application/xml", status_code=400)
        except (TableNotFoundError, SchemaNotFoundError):
            # Physical hub table/schema has not been materialized yet (lazy creation
            # on first write). Return an empty FeatureCollection in the requested
            # format rather than a 500.
            from dynastore.models.query_builder import QueryResponse
            async def _empty():
                if False:
                    yield  # pragma: no cover
            query_response = QueryResponse(
                items=_empty(),
                total_count=0,
                catalog_id=schema_prefix,
                collection_id=collection_id,
                collection_config=None,
            )

        total_count = query_response.total_count or 0

        if result_type == "hits":
            xml_content = wfs_generator.create_hits_response(
                total_count, schema_prefix, collection_id, target_namespace_url
            )
            return Response(
                content=xml_content, media_type="application/gml+xml; version=3.2"
            )

        property_names = property_name_str.split(",") if property_name_str else None

        # --- Unified Response Construction ---
        if format_enum == OutputFormatEnum.GML:
            # GML still requires materialization for the current generator
            features_list = [f async for f in query_response.items]
            number_returned = len(features_list)

            # Pagination links for GML Response
            base_url = str(request.url).split("?")[0]
            original_params = dict(request.query_params)
            
            previous_url = None
            if start_index > 0:
                prev_params = original_params.copy()
                prev_params["startIndex"] = str(max(0, start_index - count))
                previous_url = f"{base_url}?{'&'.join([f'{k}={v}' for k, v in prev_params.items()])}"

            next_url = None
            if (start_index + number_returned) < total_count:
                next_params = original_params.copy()
                next_params["startIndex"] = str(start_index + count)
                next_url = f"{base_url}?{'&'.join([f'{k}={v}' for k, v in next_params.items()])}"

            xml_content = wfs_generator.create_feature_collection_response(
                features_list,
                schema_prefix,
                collection_id,
                target_namespace_url,
                number_matched=total_count,
                previous_url=previous_url,
                next_url=next_url,
                number_returned=number_returned,
            )
            return Response(content=xml_content, media_type="application/gml+xml; version=3.2")

        # Pagination links for OGC formats
        base_url = str(request.url).split("?")[0]
        original_params = dict(request.query_params)
        
        links = []
        if start_index > 0:
            prev_params = original_params.copy()
            prev_params["startIndex"] = str(max(0, start_index - count))
            links.append(Link(
                rel="prev",
                href=f"{base_url}?{'&'.join([f'{k}={v}' for k, v in prev_params.items()])}",
                type=normalized_format,
                title=LocalizedText(en="Previous page"),
            ))

        if (start_index + count) < total_count:
            next_params = original_params.copy()
            next_params["startIndex"] = str(start_index + count)
            links.append(Link(
                rel="next",
                href=f"{base_url}?{'&'.join([f'{k}={v}' for k, v in next_params.items()])}",
                type=normalized_format,
                title=LocalizedText(en="Next page"),
            ))

        return stream_ogc_features(
            request=request,
            query_response=query_response,
            output_format=format_enum,
            catalog_id=schema_prefix,
            collection_id=collection_id,
            target_srid=4326,
            links=links
        )
