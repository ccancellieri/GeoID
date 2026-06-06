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

from dynastore.models.driver_context import DriverContext
import logging
import re
from typing import Optional, cast

from fastapi import APIRouter, Depends, HTTPException, Request, Response, FastAPI
from sqlalchemy.ext.asyncio import AsyncConnection
from contextlib import asynccontextmanager

from dynastore.modules.db_config.query_executor import managed_transaction
from dynastore.modules.db_config.exceptions import TableNotFoundError, SchemaNotFoundError

from dynastore.extensions.tools.db import get_async_connection
from dynastore.models.protocols import ItemsProtocol
from . import wfs_generator, wfs_db
from .wfs_models import WFSException
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.tools.exception_handlers import (
    ExceptionHandler,
    register_extension_handler,
)
from .wfs_config import WFSPluginConfig
from dynastore.extensions.tools.ondemand_cache import ondemand_cache_lookup
from dynastore.extensions.ogc_base import OGCServiceMixin

# --- Refactoring Step ---
# Import the centralized formatting tools.
from dynastore.extensions.tools.formatters import OutputFormatEnum
import pyproj as _pyproj_scope_gate  # noqa: F401  # SCOPE gate: extension_wfs requires pyproj
_ = _pyproj_scope_gate  # silence pyright "unused" — load-bearing for SCOPE filtering
from dynastore.extensions.tools.url import get_root_url
from dynastore.extensions.tools.language_utils import get_language

from dynastore.models.shared_models import Link
from dynastore.models.localization import LocalizedText
from dynastore.extensions.tools.query import parse_ogc_query_request, stream_ogc_features
from dynastore.modules.storage.hints import EXACT_READ_HINTS

logger = logging.getLogger(__name__)


def wfs_sortby_to_ogc(sort_by_str: Optional[str]) -> Optional[str]:
    """Translate a WFS ``SORTBY`` value into the OGC API - Features ``sortby`` form.

    WFS 2.0 expresses sort order as ``property [ASC|DESC]`` (or the ``A``/``D``
    abbreviations), whitespace-separated, comma-delimited for multiple keys —
    e.g. ``geoid ASC,date DESC``. The shared :func:`parse_ogc_query_request`
    parser instead expects the OGC API - Features syntax where direction is a
    ``+``/``-`` prefix on the property name (``geoid``/``-date``). Forwarding the
    raw WFS value made the parser read ``"geoid ASC"`` as a single field name and
    reject it as unknown. This normalises each clause so both spellings work.

    Returns ``None`` when there is nothing to sort by, so the caller can pass it
    straight through unchanged.
    """
    if not sort_by_str:
        return None
    clauses = []
    for raw in sort_by_str.split(","):
        token = raw.strip()
        if not token:
            continue
        parts = token.split()
        field = parts[0].lstrip("+-")
        if not field:
            continue
        direction = parts[1].upper() if len(parts) > 1 else ""
        # Honour an OGC-style ``-`` prefix too, so a value that is already in
        # OGC form survives the round-trip untouched.
        descending = direction in ("DESC", "D") or parts[0].startswith("-")
        clauses.append(f"-{field}" if descending else field)
    return ",".join(clauses) if clauses else None


# Names a WFS client uses to refer to the geometry property in ``PROPERTYNAME``.
# Geometry is governed separately from attribute projection (it becomes
# ``Feature.geometry``, not a ``properties`` member), so these are stripped from
# the projection list and instead drive the ``skip_geometry`` decision.
_WFS_GEOMETRY_PROPERTY_ALIASES = frozenset({"geom", "geometry", "the_geom"})


def wfs_property_names(property_name_str: Optional[str]) -> Optional[list]:
    """Resolve a WFS ``PROPERTYNAME`` value into a concrete projection list.

    WFS clients may request a namespace wildcard such as ``attributes.*`` (or a
    bare ``*``) to mean "return every attribute". The shared projection layer
    only understands concrete queryable names and rejects ``attributes.*`` as an
    unknown field. When any wildcard token is present we therefore decline to
    narrow the projection at all (return ``None`` -> all properties), preserving
    the historical "return everything" behaviour. A purely concrete list is
    passed through so explicit field selection still works.

    Geometry property aliases are dropped from the returned list — geometry is
    not a ``properties`` member and is governed by :func:`wfs_skip_geometry`.
    """
    if not property_name_str:
        return None
    names = [p.strip() for p in property_name_str.split(",") if p.strip()]
    if not names:
        return None
    has_wildcard = any(n == "*" or n.endswith(".*") for n in names)
    if has_wildcard:
        # ``attributes.*`` / ``*`` -> no narrowing; return the full feature.
        return None
    concrete = [n for n in names if n.lower() not in _WFS_GEOMETRY_PROPERTY_ALIASES]
    # If the only token was the geometry property, there is nothing left to
    # narrow -> return all attribute properties (geometry handled separately).
    return concrete or None


def wfs_skip_geometry(property_name_str: Optional[str]) -> bool:
    """Decide whether a WFS ``PROPERTYNAME`` selection excludes the geometry.

    WFS ``PROPERTYNAME`` enumerates exactly the properties to return, so the
    geometry is emitted only when it is explicitly named (or when no projection
    is requested at all). ``attributes.*`` selects every *attribute* — geometry
    is not an attribute — so it omits the geometry. A bare ``*`` means the whole
    feature and keeps the geometry.

    Returns ``True`` when the geometry should be omitted (``skip_geometry``).
    """
    if not property_name_str:
        return False
    names = [p.strip().lower() for p in property_name_str.split(",") if p.strip()]
    if not names:
        return False
    if "*" in names:
        # Whole-feature wildcard keeps the geometry.
        return False
    if any(n in _WFS_GEOMETRY_PROPERTY_ALIASES for n in names):
        return False
    # A projection was requested and it did not name the geometry -> omit it.
    return True


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
class WFSService(ExtensionProtocol, OGCServiceMixin):
    priority: int = 100
    """
    Provides a comprehensive OGC Web Feature Service (WFS) 2.0 interface. This
    extension is crucial for interoperability with traditional desktop GIS clients
    like QGIS and ArcGIS. It supports core operations including GetCapabilities,
    GetFeature, and DescribeFeatureType through both a root-level endpoint and
    catalog-specific endpoints for more RESTful interactions.
    """

    router: Optional[APIRouter] = APIRouter(prefix="/wfs", tags=["OGC WFS 2.0"])

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
        # Prepend so the WFS handler runs before the generic handlers and can
        # emit the OGC ExceptionReport XML that WFS clients require. The handler
        # logs client errors to logger.warning itself.
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
        # Catalogs/configs/storage accessors are provided by OGCServiceMixin.
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

        # Phase 1.6: collection_type was hoisted out of ItemsPostgresqlDriverConfig
        # into its own CollectionInfo PluginConfig. Kind classification is the
        # shared OGCServiceMixin helper, which reads that SSOT and fails open to
        # VECTOR on a missing config — matching the pre-Phase-1.6 "no driver
        # config → include" path.
        from dynastore.modules.catalog.catalog_config import CollectionKind

        async def _is_vector(cat_id: str, coll_id: str) -> bool:
            return await self._collection_kind(cat_id, coll_id) == CollectionKind.VECTOR

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

        if plugin_config.cache_on_demand:
            cached = await ondemand_cache_lookup(
                storage_svc,
                cache_prefix="wfs_cache",
                catalog_id=catalog_id_from_path or "global",
                params=params,
                media_type=normalized_format,
            )
            if cached is not None:
                return cached

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

        # WFS ``propertyName`` -> the shared driver-level projection
        # (``select_fields``), exactly as features/records/STAC narrow their
        # attribute output. A namespace wildcard (``attributes.*`` / ``*``)
        # means "all attributes" -> no narrowing (see ``wfs_property_names``).
        property_names = wfs_property_names(property_name_str)

        # Geometry is a property under WFS ``PROPERTYNAME`` semantics: it is
        # returned only when explicitly named (or when no projection is given).
        # ``propertyName=geoid,attributes.*`` selects attributes, not geometry,
        # so omit the geometry in that case.
        wfs_skip_geom = wfs_skip_geometry(property_name_str)

        # WFS ``SORTBY`` uses ``field ASC|DESC``; translate to the OGC
        # ``[+-]field`` form the shared parser expects.
        ogc_sortby = wfs_sortby_to_ogc(sort_by_str)

        request_obj = parse_ogc_query_request(
            bbox=bbox_val,
            datetime_param=time_str,
            sortby=ogc_sortby,
            filter=cql_filter,
            item_ids=feature_id_str,
            limit=count,
            offset=start_index,
            bbox_crs_srid=bbox_crs_srid,
            include_total_count=True,
            select_fields=property_names,
            skip_geometry=wfs_skip_geom,
        )

        target_namespace_url = f"{root_wfs_url}/{schema_prefix}"

        # Execute search via ItemsProtocol (Streaming support)
        items_svc = cast(ItemsProtocol, catalogs_svc)

        # PG row-level ABAC: compile and inject access_filter when the collection
        # carries an access_envelope sidecar (user-facing read).
        from dynastore.modules.storage.access_scope import (
            collection_uses_pg_access_envelope,
            compile_read_access_filter,
            principals_from_request_state,
        )

        if await collection_uses_pg_access_envelope(schema_prefix, collection_id):
            principals, principal = principals_from_request_state(request)
            request_obj.access_filter = await compile_read_access_filter(
                catalog_id=schema_prefix,
                collections=[collection_id],
                principals=principals,
                principal=principal,
            )

        try:
            query_response = await items_svc.stream_items(
                catalog_id=schema_prefix,
                collection_id=collection_id,
                request=request_obj,
                # Decouple from request connection to allow background streaming
                # without premature closure errors.
                ctx=None,
                # WFS GetFeature must return exact, full-precision geometry.
                # EXACT_READ_HINTS routes past any simplified-geometry ES driver
                # to whichever driver declares Hint.GEOMETRY_EXACT.
                hints=EXACT_READ_HINTS,
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
