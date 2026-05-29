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

# dynastore/extensions/stac/stac_service.py

import logging
import asyncio
from contextlib import asynccontextmanager
from typing import Optional, cast, Dict, Any, Tuple, List, Union

import pystac
from fastapi import FastAPI, APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import Response, HTMLResponse
from dynastore.extensions.tools.fast_api import AppJSONResponse as JSONResponse
from dynastore.models.driver_context import DriverContext
from sqlalchemy import text
from dynastore.models.protocols import ConfigsProtocol
from dynastore.models.protocols.authentication import AuthenticatorProtocol
from dynastore.models.protocols.authorization import IamRolesConfig


import dynastore.modules.db_config.shared_queries as shared_queries
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.web.decorators import expose_static
from dynastore.extensions.tools.db import get_async_engine
from dynastore.extensions.tools.exception_handlers import handle_or_raise
from dynastore.modules.db_config.query_executor import (
    managed_transaction,
    ResultHandler,
)
from dynastore.extensions.stac.search import (
    CollectionSearchRequest,
    ItemSearchRequest,
    search_collections,
    search_items,
)

from dynastore.modules.stac.stac_config import StacPluginConfig

from dynastore.tools.db import validate_sql_identifier
from .stac_models import STACCatalogRequest, stac_localize
from .stac_validator import validate_stac_item, validate_stac_collection
from dynastore.models.shared_models import Feature
from . import stac_generator, stac_db, asset_factory
from .stac_models import (
    STACCollectionUpdate,
    STACItem,
    STACItemCollection,
    STACItemOrItemCollection,
    STACCollectionRequest,
    STACCatalogUpdate,
    STACItemResponse,
    STACItemCollectionResponse,
)
from .stac_aggregation_models import AggregationRequest
from dynastore.extensions.ogc_base import OGCServiceMixin, OGCTransactionMixin
from datetime import datetime, timezone
from dynastore.extensions.tools.url import get_url, get_parent_url, get_root_url
from dynastore.tools.discovery import get_protocol, get_protocols
from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry
from dynastore.models.localization import normalize_i18n_for_replace

logger = logging.getLogger(__name__)
from dynastore.modules.db_config.exceptions import TableNotFoundError
from dynastore.extensions.tools.language_utils import get_language
from dynastore.extensions.tools.localization_utils import detect_use_lang
from dynastore.extensions.web.decorators import expose_web_page
from dynastore.models.protocols.web import StaticFilesProtocol
import os
from .stac_virtual import StacVirtualMixin

def _assert_stac_capable_collection_stack() -> None:
    """Verify the catalog-tier can persist a STAC envelope; warn on the
    collection tier.

    The catalog tier is a **hard requirement** — the ``stac_version``,
    ``stac_extensions``, ``conforms_to``, ``links``, and ``assets`` of
    the catalog itself must land somewhere, or the STAC catalog is
    meaningless.  Fail with HTTPException(422) if no registered
    ``CatalogStore`` satisfies ``StacCatalogEntityStoreCapability``.

    The collection tier is a **soft requirement**: a STAC catalog can
    legitimately exist with zero STAC collections at create time
    (collections added later, possibly on a different driver config).
    When the collection tier has no STAC-capable driver at catalog
    creation, log a WARNING rather than refusing — operators can
    register a collection-tier STAC driver before adding STAC
    collections, or accept that STAC collections cannot be persisted
    under this catalog's configured backing.

    Default PG config satisfies both checks via the per-tier
    composition wrappers ``CatalogPostgresqlDriver`` +
    ``CollectionPostgresqlDriver`` once the stac extra is installed —
    each wrapper composes its tier's STAC slice via
    ``*CollectionPgSidecarRegistry`` try-import (PR 1e steps 3b+3c).
    """
    from dynastore.extensions.stac.protocols import (
        StacCatalogEntityStoreCapability,
        StacCollectionEntityStoreCapability,
    )
    from dynastore.models.protocols.entity_store import (
        CatalogStore,
        CollectionStore,
    )

    def _has_stac(proto_cls: type, capability_cls: type) -> bool:
        """True iff a registered driver satisfies the capability AND
        returns a non-empty column tuple.

        The empty-tuple guard catches composition drivers (e.g.
        ``CollectionPostgresqlDriver`` after PR 1e step 3b) that always
        expose the ``stac_metadata_columns`` marker but return ``()`` in
        deployments without the stac extra installed — purely-structural
        ``isinstance`` would falsely pass in that case and silence this
        warning even when the STAC slice would genuinely be dropped on
        write.
        """
        for d in get_protocols(proto_cls):
            if not isinstance(d, capability_cls):
                continue
            try:
                cols = d.stac_metadata_columns()
            except Exception:
                continue
            if cols:
                return True
        return False

    if not _has_stac(CatalogStore, StacCatalogEntityStoreCapability):
        raise HTTPException(
            status_code=422,
            detail=(
                "STAC catalog creation requires a registered "
                "CatalogStore driver implementing "
                "StacCatalogEntityStoreCapability and exposing non-empty "
                "stac_metadata_columns().  The default PG deployment "
                "satisfies this via CatalogPostgresqlDriver once the "
                "stac extra is installed (the wrapper composes the "
                "STAC slice via the unified SidecarRegistry's "
                "catalog-tier try-import).  Install ``dynastore[module_stac]`` or "
                "check the routing config."
            ),
        )
    if not _has_stac(CollectionStore, StacCollectionEntityStoreCapability):
        logger.warning(
            "STAC catalog creation proceeding without a STAC-capable "
            "CollectionStore driver.  Adding a STAC collection "
            "under this catalog will drop the STAC slice on write.  "
            "Default PG deployment satisfies this via "
            "CollectionPostgresqlDriver once the stac extra is "
            "installed; install ``dynastore[module_stac]`` or register "
            "an equivalent implementing StacCollectionEntityStoreCapability "
            "before creating STAC collections."
        )


STAC_API_URIS = [
    "https://api.stacspec.org/v1.0.0/core",
    "https://api.stacspec.org/v1.0.0/item-search",
    "https://api.stacspec.org/v1.0.0/collections",
    "https://api.stacspec.org/v1.0.0-rc.2/transactions",
    "https://api.stacspec.org/v1.0.0/item-search/definition",
    "https://api.stacspec.org/v1.0.0/item-search#filter",
    "https://api.stacspec.org/v1.0.0/item-search#sort",
    "https://api.stacspec.org/v1.0.0/item-search#fields",
    "https://api.stacspec.org/v1.0.0/item-search#context",
    "https://api.stacspec.org/v1.0.0/item-search#aggregation",
    "https://api.stacspec.org/v1.0.0/item-search#query",
    "https://api.stacspec.org/v1.0.0/item-search#filter:cql-json",
    "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/filter",
    "https://api.stacspec.org/v1.0.0/children",
]
class STACService(ExtensionProtocol, StaticFilesProtocol, StacVirtualMixin, OGCServiceMixin, OGCTransactionMixin):
    priority: int = 100
    router: APIRouter = APIRouter(
        prefix="/stac", tags=["OGC API - STAC - Spatio Temporal Asset Catalog"]
    )

    # OGCServiceMixin class attributes
    conformance_uris = STAC_API_URIS
    prefix = "/stac"
    protocol_title = "DynaStore OGC API - STAC"
    protocol_description = "SpatioTemporal Asset Catalog API for discovery and access"

    def get_web_pages(self):
        from dynastore.extensions.tools.web_collect import collect_web_pages
        return collect_web_pages(self)

    def get_static_assets(self):
        from dynastore.extensions.tools.web_collect import collect_static_assets
        return collect_static_assets(self)

    def configure_app(self, app: FastAPI):
        """Early configuration for the STAC extension."""
        # Sidecar registration is canonical at module-import time inside
        # ``stac_items_sidecar.py``; no re-registration needed here.
        # Web pages / static assets are discovered by WebModule via the
        # WebPageContributor / StaticAssetProvider capability protocols —
        # implemented on this class via collect_web_pages/collect_static_assets.

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        # Policies declared via PolicyContributor; IAM forwards centrally.
        yield

    # NotebookContributorProtocol — opt-in surface picked up by
    # NotebooksModule via discovery. Returning an empty list when
    # NotebookContribution can't be imported keeps the extension
    # usable in SCOPEs that don't load the notebooks module.
    def get_notebooks(self):
        try:
            from .notebooks import build_contributions
        except Exception:
            return []
        return build_contributions()

    def get_static_prefix(self) -> str:
        """Returns the static prefix for STAC."""
        return "stac"

    async def is_file_provided(self, path: str) -> bool:
        """Checks if a static file is provided."""
        static_dir = os.path.realpath(os.path.join(os.path.dirname(__file__), "static"))
        full_path = os.path.realpath(os.path.join(static_dir, path.lstrip("/")))
        if not full_path.startswith(static_dir + os.sep) and full_path != static_dir:
            return False
        return os.path.isfile(full_path)

    async def list_static_files(self, query: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[str]:
        """Lists static files for STAC with pagination and search."""
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        files = []
        for root, _, filenames in os.walk(static_dir):
            for filename in filenames:
                full_path = os.path.join(root, filename)
                rel_path = os.path.relpath(full_path, static_dir)
                if not query or query.lower() in rel_path.lower():
                    files.append(full_path)
        return sorted(files)[offset : offset + limit]

    def __init__(self, app: Optional[FastAPI] = None):
        self.app = app
        self._register_routes()
        self._register_exception_handlers()

    def _register_exception_handlers(self):
        from dynastore.extensions.tools.exception_handlers import register_handler
        from dynastore.extensions.stac.stac_validator import (
            STACValidationExceptionHandler,
        )

        # prepend so STAC-specific 422 wins over the generic ValidationExceptionHandler
        register_handler(STACValidationExceptionHandler(), prepend=True)

    def _register_routes(self):
        """Registers routes using a declarative route table."""
        _J = JSONResponse  # shorthand

        # (path, handler_name, methods, kwargs)
        route_table = [
            # Catalog Discovery
            ("/", "get_stac_root_catalog", ["GET"], {}),
            ("/conformance", "ogc_conformance_handler", ["GET"], {}),
            ("/catalogs", "list_stac_catalogs", ["GET"], {"response_class": _J}),
            ("/catalogs/{catalog_id}", "get_stac_catalog", ["GET"], {"response_class": _J}),
            ("/catalogs/{catalog_id}/collections", "list_stac_collections", ["GET"], {"response_class": _J}),
            ("/catalogs/{catalog_id}/collections/{collection_id}", "get_stac_collection", ["GET"], {"response_class": _J}),
            # Write Operations
            ("/catalogs", "create_stac_catalog", ["POST"], {"status_code": status.HTTP_201_CREATED}),
            ("/catalogs/{catalog_id}/collections", "create_stac_collection", ["POST"], {"status_code": status.HTTP_201_CREATED}),
            ("/catalogs/{catalog_id}", "replace_stac_catalog", ["PUT"], {"status_code": status.HTTP_200_OK}),
            ("/catalogs/{catalog_id}", "update_stac_catalog", ["PATCH"], {"status_code": status.HTTP_200_OK}),
            ("/catalogs/{catalog_id}", "delete_stac_catalog", ["DELETE"], {}),
            ("/catalogs/{catalog_id}/collections/{collection_id}", "replace_stac_collection", ["PUT"], {"status_code": status.HTTP_200_OK}),
            ("/catalogs/{catalog_id}/collections/{collection_id}", "update_stac_collection", ["PATCH"], {"status_code": status.HTTP_200_OK}),
            ("/catalogs/{catalog_id}/collections/{collection_id}", "delete_stac_collection", ["DELETE"], {}),
            # Item Endpoints
            ("/catalogs/{catalog_id}/collections/{collection_id}/items", "get_stac_collection_items", ["GET"], {"response_class": _J}),
            ("/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}", "get_stac_item", ["GET"], {"response_class": _J}),
            ("/catalogs/{catalog_id}/collections/{collection_id}/items", "add_stac_item", ["POST"], {"status_code": status.HTTP_201_CREATED}),
            ("/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}", "update_stac_item", ["PUT"], {"status_code": status.HTTP_200_OK}),
            ("/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}", "delete_stac_item", ["DELETE"], {}),
            # Search Endpoints
            ("/catalogs/{catalog_id}/search", "search_items_post", ["POST"], {"response_class": _J}),
            ("/collections-search", "search_stac_collections_post", ["POST"], {"response_class": _J}),
            ("/search", "search_items_post", ["POST"], {"response_class": _J, "deprecated": True}),
            ("/collections/search", "search_stac_collections_post", ["POST"], {"response_class": _J, "deprecated": True}),
            # Virtual STAC Endpoints
            ("/virtual/assets/catalogs/{catalog_id}/collections/{collection_id}", "get_virtual_asset_list", ["GET"], {"response_class": _J}),
            ("/virtual/assets/{asset_code}/catalogs/{catalog_id}/collections/{collection_id}", "get_virtual_asset_collection", ["GET"], {"response_class": _J}),
            ("/virtual/assets/{asset_id}/catalogs/{catalog_id}/collections", "get_virtual_asset_collections", ["GET"], {"response_class": _J}),
            ("/virtual/assets/{asset_id}/catalogs/{catalog_id}/collections/{collection_id}/items", "get_virtual_asset_items", ["GET"], {"response_class": _J}),
            ("/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_code}/source", "resolve_asset_source", ["GET"], {}),
            ("/virtual/hierarchy/{hierarchy_id}/catalogs/{catalog_id}/collections/{collection_id}", "get_virtual_hierarchy_collection", ["GET"], {"response_class": _J}),
            ("/virtual/hierarchy/{hierarchy_id}/catalogs/{catalog_id}/collections/{collection_id}/items", "get_virtual_hierarchy_items", ["GET"], {"response_class": _J}),
            ("/virtual/hierarchy/{hierarchy_id}/catalogs/{catalog_id}/collections/{collection_id}/search", "search_virtual_hierarchy_items", ["GET", "POST"], {"response_class": _J}),
        ]

        for path, handler_name, methods, kwargs in route_table:
            self.router.add_api_route(path, getattr(self, handler_name), methods=methods, **kwargs)

    @expose_static("stac")
    def provide_static_files(self) -> list[str]:
        """Exposes the internal static directory for the STAC browser."""
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        files = []
        for root, _, filenames in os.walk(static_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    async def _get_stac_config(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        db_resource: Optional[Any] = None,
    ) -> StacPluginConfig:
        configs_svc = await self._get_configs_service()
        return await configs_svc.get_config(
            StacPluginConfig, catalog_id, collection_id, ctx=DriverContext(db_resource=db_resource
        ))

    async def get_stac_root_catalog(
        self, request: Request, language: str = Depends(get_language)
    ):
        catalog_dict = await stac_generator.create_root_catalog(request, lang=language)
        return JSONResponse(content=catalog_dict)

    async def list_stac_catalogs(
        self,
        request: Request,
        limit: int = Query(100, ge=1, le=10000),
        offset: int = Query(0, ge=0),
        language: str = Depends(get_language),
    ):
        """Lists available STAC catalogs, filtered by catalog admin access for non-sysadmin roles."""
        catalogs_svc = await self._get_catalogs_service()
        catalogs = await catalogs_svc.list_catalogs(limit=limit, offset=offset, lang=language)

        # Role-based catalog visibility:
        #   sysadmin → all catalogs (no filter)
        #   admin (global, not sysadmin) → only catalogs where they have catalog-scoped roles
        #   anonymous / user → all public catalogs (policy middleware already controls access)
        principal = getattr(request.state, "principal", None)
        if principal and principal.roles:
            roles = set(principal.roles)
            _roles_cfg = IamRolesConfig()
            if _roles_cfg.admin_role_name in roles and _roles_cfg.sysadmin_role_name not in roles:
                try:
                    iam = get_protocol(AuthenticatorProtocol)
                    if iam and iam.storage:  # type: ignore[attr-defined]
                        accessible_ids = set(
                            await iam.storage.get_catalogs_for_identity(  # type: ignore[attr-defined]
                                principal.provider, principal.subject_id
                            )
                        )
                        catalogs = [c for c in catalogs if c.id in accessible_ids]
                except Exception as e:
                    logger.warning(f"Failed to filter catalogs by admin access: {e}")

        content = [
            stac_generator.create_catalog_summary(request, c, lang=language)
            for c in catalogs
        ]
        return JSONResponse(content=content)

    @expose_web_page(
        page_id="stac_browser",
        title="STAC Browser",
        icon="fa-layer-group",
        description="Explore satellite imagery and geospatial assets.",
    )
    async def provide_stac_browser(self, request: Request):
        return await self._serve_page_template("stac_browser.html")

    async def _serve_page_template(self, filename: str):
        from dynastore._version import VERSION
        file_path = os.path.join(os.path.dirname(__file__), "static", filename)
        if not os.path.exists(file_path):
            return Response(content=f"Template {filename} not found", status_code=404)
        with open(file_path, "r", encoding="utf-8") as f:
            return Response(content=f.read().replace("{{VERSION}}", VERSION), media_type="text/html")

    async def get_stac_catalog(
        self, catalog_id: str, request: Request, language: str = Depends(get_language)
    ):
        catalog_id = validate_sql_identifier(catalog_id)
        catalogs_svc = await self._get_catalogs_service()
        if not await catalogs_svc.get_catalog(catalog_id, lang=language):
            raise HTTPException(
                status_code=404, detail=f"Catalog '{catalog_id}' not found."
            )
        catalog_dict = await stac_generator.create_catalog(
            request, catalog_id=catalog_id, lang=language
        )
        return JSONResponse(content=catalog_dict)

    async def list_stac_collections(
        self, catalog_id: str, request: Request, language: str = Depends(get_language)
    ):
        catalog_id = validate_sql_identifier(catalog_id)
        catalogs_svc = await self._get_catalogs_service()
        try:
            catalog = await catalogs_svc.get_catalog(catalog_id, lang=language)
        except (ValueError, Exception):
            catalog = None
        if not catalog:
            raise HTTPException(
                status_code=404, detail=f"Catalog '{catalog_id}' not found."
            )
        # From stac_generator.py: create_collection is the only one, maybe there is no list?
        # Actually, OGC API - STAC requires /collections.
        # I'll check stac_generator.py for a listing method if it exists.
        # Assuming it is stac_generator.create_collections (guessed)
        try:
            # Let's check if stac_generator has a generic collections lister
            # If not, we might need to implement it.
            # Usually it's create_root_catalog -> provides links to collections.
            # But the spec also has /collections route.
            collections = await stac_generator.create_collections_catalog(
                request, catalog_id=catalog_id, lang=language
            )
            return JSONResponse(content=collections)
        except AttributeError:
            # Fallback if method doesn't exist yet
            raise HTTPException(
                status_code=501,
                detail="STAC Collection listing not yet implemented in generator.",
            )

    async def get_stac_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        language: str = Depends(get_language),
    ):
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        catalogs_svc = await self._get_catalogs_service()
        if not await catalogs_svc.get_collection(catalog_id, collection_id):
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{catalog_id}:{collection_id}' not found.",
            )
        collection = await stac_generator.create_collection(
            request, catalog_id=catalog_id, collection_id=collection_id, lang=language
        )
        if collection is None:
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{catalog_id}:{collection_id}' not found.",
            )
        from dynastore.modules.catalog.collection_pipeline_runner import (
            apply_collection_pipeline,
        )
        collection_dict = collection.to_dict()
        rewritten = await apply_collection_pipeline(
            catalog_id, collection_id, collection_dict, context={},
        )
        if rewritten is None:
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{catalog_id}:{collection_id}' not found.",
            )
        return JSONResponse(content=rewritten)

    # --- Write Endpoints ---

    async def create_stac_catalog(
        self,
        definition: STACCatalogRequest,
        language: str = Depends(get_language),
    ):
        try:
            # STAC precondition: at least one registered driver must declare
            # the STAC metadata domain, so a STAC envelope (stac_version,
            # stac_extensions, conforms_to, links, assets) actually lands
            # somewhere.  Default PG config satisfies this via the
            # composition wrappers CatalogPostgresqlDriver +
            # CollectionPostgresqlDriver, which compose their tier's STAC
            # sidecar via ``*CollectionPgSidecarRegistry`` try-import.
            # Deployments pointing routing configs at STAC-blind drivers
            # (e.g. ES-only without the stac extra installed) fail loudly
            # here rather than silently dropping the STAC slice on write.
            _assert_stac_capable_collection_stack()

            # We use STACCatalog (DTO) for validation but the catalogs_svc expects the structure to be merged
            # The definition is a Pydantic model with localized fields. model_dump() handles serialization.
            input_data = definition.model_dump(exclude_unset=True)
            use_lang = detect_use_lang(input_data, language)

            catalogs_svc = await self._get_catalogs_service()
            created_catalog_model = await catalogs_svc.create_catalog(
                input_data, lang=use_lang
            )
            localized_data, _ = stac_localize(created_catalog_model, language)
            return JSONResponse(
                content=localized_data, status_code=status.HTTP_201_CREATED
            )
        except Exception as e:
            return handle_or_raise(
                e,
                resource_name="Catalog",
                resource_id=definition.id,
                operation="STAC Catalog creation",
            )

    async def create_stac_collection(
        self,
        catalog_id: str,
        request_body: STACCollectionRequest,
        language: str = Depends(get_language),
    ):
        # Ensure that we pass the language and that request_body is converted to a dict including localized fields
        # STACCollection inherits from CoreCollection -> BaseMetadata -> LocalizedFieldsBase
        # model_dump will handle serialization
        try:
            input_data = request_body.model_dump(exclude_unset=True)

            # Write-time STAC validation (lenient — warnings only; pystac/stac-pydantic
            # are too strict on optional fields like `links` to gate the request).
            validate_stac_collection(input_data)

            use_lang = detect_use_lang(input_data, language)

            catalogs_svc = await self._get_catalogs_service()
            # Fail-fast guard: reject writes against catalogs whose
            # provisioning hasn't completed — the backing storage
            # doesn't exist yet (or provisioning failed).
            await self._require_catalog_ready(catalog_id, catalogs_svc=catalogs_svc)
            created_collection_model = await catalogs_svc.create_collection(
                catalog_id, input_data, lang=use_lang, stac_context=True
            )
            localized_data, _ = stac_localize(created_collection_model, language)
            return JSONResponse(
                content=localized_data, status_code=status.HTTP_201_CREATED
            )
        except Exception as e:
            return handle_or_raise(
                e,
                resource_name="Collection",
                resource_id=f"{catalog_id}:{request_body.id}",
                operation="STAC Collection creation",
            )

    async def replace_stac_catalog(
        self,
        catalog_id: str,
        definition: STACCatalogRequest,
        language: str = Depends(get_language),
    ):
        # OGC API Features Part 4 / STAC Transaction Extension: PUT replaces
        # the whole resource with the request body. Required-field validation
        # is enforced by ``STACCatalogRequest`` (the same model used on POST),
        # so a partial body is rejected at the framework boundary.
        if definition.id != catalog_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"Body 'id' ({definition.id!r}) must match path catalog_id "
                    f"({catalog_id!r})."
                ),
            )

        # ``exclude_unset=False``: include every model field so absent
        # optionals are written as None — true replace semantics.
        input_data = definition.model_dump(exclude_unset=False)
        input_data = normalize_i18n_for_replace(input_data, language)

        catalogs_svc = await self._get_catalogs_service()
        await self._require_catalog_ready(catalog_id, catalogs_svc=catalogs_svc)
        updated = await catalogs_svc.update_catalog(
            catalog_id, input_data, lang="*"
        )
        if not updated:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Catalog '{catalog_id}' not found.",
            )
        localized_data, _ = stac_localize(updated, language)
        return JSONResponse(content=localized_data)

    async def update_stac_catalog(
        self,
        catalog_id: str,
        definition: STACCatalogUpdate,
        language: str = Depends(get_language),
    ):
        input_data = definition.model_dump(exclude_unset=True)
        use_lang = detect_use_lang(input_data, language)

        catalogs_svc = await self._get_catalogs_service()
        # Fail-fast guard: can't mutate metadata on a catalog whose
        # backing storage isn't ready.
        await self._require_catalog_ready(catalog_id, catalogs_svc=catalogs_svc)
        updated_catalog_model = await catalogs_svc.update_catalog(
            catalog_id, input_data, lang=use_lang
        )
        if not updated_catalog_model:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Catalog '{catalog_id}' not found.",
            )
        localized_data, _ = stac_localize(updated_catalog_model, language)
        return JSONResponse(content=localized_data)

    async def delete_stac_catalog(
        self,
        catalog_id: str,
        force: bool = Query(
            False,
            description=(
                "Force hard deletion (drops the schema and all data, can't be "
                "undone). When false (default) the catalog is soft-deleted."
            ),
        ),
    ):
        catalogs_svc = await self._get_catalogs_service()
        if not await catalogs_svc.delete_catalog(catalog_id, force=force):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Catalog '{catalog_id}' not found.",
            )
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    async def replace_stac_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request_body: STACCollectionRequest,
        language: str = Depends(get_language),
    ):
        # OGC API Features Part 4 / STAC Transaction Extension: PUT replaces
        # the whole collection. ``STACCollectionRequest`` enforces required
        # STAC fields (id/type/license/extent/...) so partial bodies fail
        # before reaching the manager.
        if request_body.id != collection_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"Body 'id' ({request_body.id!r}) must match path collection_id "
                    f"({collection_id!r})."
                ),
            )

        input_data = request_body.model_dump(exclude_unset=False)
        validate_stac_collection(input_data)
        input_data = normalize_i18n_for_replace(input_data, language)

        catalogs_svc = await self._get_catalogs_service()
        await self._require_catalog_ready(catalog_id, catalogs_svc=catalogs_svc)
        updated = await catalogs_svc.update_collection(
            catalog_id, collection_id, input_data, lang="*"
        )
        if not updated:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Collection '{catalog_id}:{collection_id}' not found.",
            )
        localized_data, _ = stac_localize(updated, language)
        return JSONResponse(content=localized_data)

    async def update_stac_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request_body: STACCollectionUpdate,
        request: Request,
        language: str = Depends(get_language),
    ):
        input_data = request_body.model_dump(exclude_unset=True)

        # Validate the *resulting* state (post-merge), not the raw partial body.
        # PATCH sends a subset of fields; validating it in isolation fails
        # required-field checks that the merged collection would pass.
        existing_collection = await stac_generator.create_collection(
            request, catalog_id=catalog_id, collection_id=collection_id, lang=language
        )
        if existing_collection is not None:
            merged = {**existing_collection.to_dict(), **input_data}
            validate_stac_collection(merged)

        # Sidecars are now handled transparently by ItemsProtocol / LifecycleRegistry
        # No need to manually inject them here.

        use_lang = detect_use_lang(input_data, language)

        catalogs_svc = await self._get_catalogs_service()
        # Fail-fast guard: can't update a collection when the enclosing
        # catalog isn't ready.
        await self._require_catalog_ready(catalog_id, catalogs_svc=catalogs_svc)
        updated_collection_model = await catalogs_svc.update_collection(
            catalog_id, collection_id, input_data, lang=use_lang
        )
        if not updated_collection_model:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Collection '{catalog_id}:{collection_id}' not found.",
            )
        localized_data, _ = stac_localize(updated_collection_model, language)
        return JSONResponse(content=localized_data)

    async def delete_stac_collection(
        self,
        catalog_id: str,
        collection_id: str,
        force: bool = Query(
            False,
            description=(
                "Force hard deletion (drops the items table and metadata, "
                "can't be undone). When false (default) the collection is "
                "soft-deleted; the id can still be recreated, which resets it "
                "to a fresh collection."
            ),
        ),
    ):
        catalogs_svc = await self._get_catalogs_service()
        if not await catalogs_svc.delete_collection(catalog_id, collection_id, force=force):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Collection '{catalog_id}:{collection_id}' not found.",
            )
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    async def get_stac_collection_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        engine=Depends(get_async_engine),
        limit: int = Query(10, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        filter: Optional[str] = Query(None, description="CQL2-Text filter expression"),
        language: str = Depends(get_language),
    ):
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        catalogs_svc = await self._get_catalogs_service()

        # Single-field equality shorthand: any non-reserved query parameter is
        # treated as a ``?{property}={value}`` attribute filter and combined with
        # the explicit CQL2 ``filter`` into one expression. Both go through the
        # same validated CQL pipeline as the OGC Features ``/items`` endpoint —
        # property names are validated against the collection's queryable fields
        # (unknown → 400) and values are bound as parameters, never interpolated.
        from dynastore.extensions.tools.query import (
            OGC_RESERVED_QUERY_PARAMS,
            combine_cql_filters,
            maybe_dispatch_items_to_search_driver,
        )

        extra_filters = {
            key: value
            for key, value in request.query_params.items()
            if key not in OGC_RESERVED_QUERY_PARAMS and value != ""
        }
        cql_filter = combine_cql_filters(filter, extra_filters)

        async with managed_transaction(engine) as conn:
            collection_metadata = await catalogs_svc.get_collection(
                catalog_id, collection_id, ctx=DriverContext(db_resource=conn)
            )
            if not collection_metadata:
                raise HTTPException(
                    status_code=404, detail=f"Collection '{collection_id}' not found."
                )

            stac_config = await self._get_stac_config(
                catalog_id, collection_id, db_resource=conn
            )

            # ── Routing-aware items SEARCH-driver dispatch (#1047, #1311) ─────
            # Mirror the OGC Features / Records ``/items`` paths and STAC
            # ``/search``: for a structural-only listing (no CQL2 ``filter`` and
            # no ``?{property}={value}`` shorthand) resolve the items SEARCH
            # driver via routing and dispatch through its streaming
            # ``read_entities`` + ``count_entities`` contract. The helper threads
            # the HTTP ``request`` so an access-aware envelope driver gets the
            # caller's compiled read scope (fail-closed); it returns ``None`` —
            # and ``create_item_collection`` falls back to the PG
            # ``get_stac_items_paginated`` path unchanged — for a CQL/attribute
            # filter, a read-primary (PG) driver, a non-ES items driver, or any
            # dispatch error.
            #
            # LIMITATION (same as OGC Features/Records #1314): the PG fallback
            # has no row-level access-filter seam, so a *filtered* listing
            # (``?{property}={value}`` shorthand or CQL2 ``filter``) is served
            # by PG and does NOT apply the envelope driver's row-level ABAC.
            # This is not a regression (pre-#1311 every listing here went to PG
            # unfiltered); it only means filtered queries on an ABAC-protected
            # collection must use the STAC ``/search`` route, which routes
            # through the access-aware ``search_items`` path. Closing this gap
            # needs a PG access-filter seam or CQL2→ES translation on the
            # envelope driver — tracked under #1285/#1311.
            search_dispatch = None
            if not cql_filter:
                search_dispatch = await maybe_dispatch_items_to_search_driver(
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    limit=limit,
                    offset=offset,
                    has_complex_filter=False,
                    request=request,
                )

            try:
                result = await stac_generator.create_item_collection(
                    request,
                    conn,
                    schema=catalog_id,
                    table=collection_id,
                    limit=limit,
                    offset=offset,
                    stac_config=stac_config,
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    lang=language,
                    cql_filter=cql_filter,
                    search_dispatch=search_dispatch,
                )
            except ValueError as e:
                # Unknown property / malformed CQL → 400
                raise HTTPException(status_code=400, detail=str(e))
        return JSONResponse(content=result)

    async def _get_item_with_row(
        self,
        items_svc: Any,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        language: str,
        conn: Any,
        request: Optional[Request] = None,
    ) -> Tuple[Optional[Feature], Dict[str, Any]]:
        """Utility to get an item and its raw row via FeaturePipelineContext."""
        from dynastore.modules.storage.drivers.pg_sidecars.base import (
            FeaturePipelineContext,
            ConsumerType,
        )

        # PG row-level ABAC: compile access_filter for user-facing single-item
        # reads when the collection carries an access_envelope sidecar.
        af = None
        if request is not None:
            from dynastore.modules.storage.access_scope import (
                collection_uses_pg_access_envelope,
                compile_read_access_filter,
                principals_from_request_state,
            )

            if await collection_uses_pg_access_envelope(catalog_id, collection_id):
                principals, principal = principals_from_request_state(request)
                af = await compile_read_access_filter(
                    catalog_id=catalog_id,
                    collections=[collection_id],
                    principals=principals,
                    principal=principal,
                )

        context = FeaturePipelineContext(lang=language, consumer=ConsumerType.STAC)
        item = await items_svc.get_item(
            catalog_id=catalog_id,
            collection_id=collection_id,
            item_id=item_id,
            ctx=DriverContext(db_resource=conn),
            lang=language,
            context=context,
            access_filter=af,
        )
        if not item:
            return None, {}

        # Merge all published sidecar data into a single row-like dict
        row = {}
        for sc_id, entry in context.all_sidecars().items():
            data = entry._data if hasattr(entry, "_data") else entry
            if isinstance(data, dict):
                row.update(data)
        
        # Ensure core fields are in row for stac_generator
        if item.id:
            row["geoid"] = item.id
            row["id"] = item.id
        if item.bbox:
            row["bbox"] = item.bbox
            row.update({
                "bbox_xmin": item.bbox[0],
                "bbox_ymin": item.bbox[1],
                "bbox_xmax": item.bbox[2],
                "bbox_ymax": item.bbox[3],
            })
        
        return item, row

    async def get_stac_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        request: Request,
        engine=Depends(get_async_engine),
        language: str = Depends(get_language),
    ):
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        catalogs_svc = await self._get_catalogs_service()

        if not engine:
            from dynastore.models.protocols import DatabaseProtocol

            db_svc = get_protocol(DatabaseProtocol)
            if db_svc:
                engine = db_svc.engine

        async with managed_transaction(engine) as conn:
            # Use direct connection with ItemService to ensure proper sidecar filtering
            from dynastore.models.protocols import ItemsProtocol

            items_svc = get_protocol(ItemsProtocol)
            item, row = await self._get_item_with_row(
                items_svc, catalog_id, collection_id, item_id, language, conn,
                request=request,
            )

            if not item:
                raise HTTPException(
                    status_code=404, detail=f"Item {item_id} not found."
                )

            # Add handling for deleted items
            if item.properties.get("deleted_at") is not None:
                raise HTTPException(
                    status_code=404, detail=f"Item {item_id} not found."
                )

            stac_config = await self._get_stac_config(
                catalog_id, collection_id, db_resource=conn
            )

            response_item = await stac_generator.create_item_from_feature(
                request,
                catalog_id,
                collection_id,
                feature=item,
                stac_config=stac_config,
                lang=language,
            )
            if response_item is None:
                raise HTTPException(
                    status_code=500, detail="Failed to render STAC Item."
                )
            return JSONResponse(content=response_item.to_dict())

    async def add_stac_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_payload: STACItemOrItemCollection,
        request: Request,
        engine=Depends(get_async_engine),
        language: str = Depends(get_language),
    ):
        # Fail-fast guard: reject item writes when the enclosing catalog
        # isn't ready.  Runs BEFORE STAC validation so we surface the
        # real blocker (missing storage) rather than letting a driver
        # 500 deep down the stack.
        await self._require_catalog_ready(catalog_id)

        # Write-time STAC validation per item (lenient — warnings only)
        items_to_validate: list[STACItem] = (
            list(item_payload.features)
            if isinstance(item_payload, STACItemCollection)
            else [item_payload]
        )
        for item in items_to_validate:
            validate_stac_item(item.model_dump(by_alias=True, exclude_unset=True))

        from dynastore.modules.storage.driver_config import ItemsWritePolicy
        policy_source = (
            f"/configs/catalogs/{catalog_id}/collections/{collection_id}"
            f"/plugins/{ItemsWritePolicy.class_key()}"
        )

        try:
            async with managed_transaction(engine) as conn:
                accepted_rows, rejections, was_single, batch_size = (
                    await self._ingest_items(
                        catalog_id,
                        collection_id,
                        item_payload,
                        DriverContext(db_resource=conn),
                        policy_source,
                    )
                )
                if was_single and not rejections:
                    # Fetch config inside transaction for single-item STAC rendering
                    stac_config = await self._get_stac_config(
                        catalog_id, collection_id, db_resource=conn
                    )

        except HTTPException:
            raise
        except ValueError as e:
            logger.error(f"Validation error in add_stac_item: {e}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
        except Exception as e:
            return handle_or_raise(
                e,
                resource_name="STAC Item",
                resource_id=f"{catalog_id}:{collection_id}",
                operation="STAC Item creation",
            )

        if rejections:
            return self._build_rejection_response(accepted_rows, rejections, batch_size)

        if not was_single:
            return self._build_bulk_creation_response(accepted_rows)

        # Single-item path: render a full STAC Item response.
        # NOTE: the write transaction has already committed at this point.
        # Rendering failures are surfaced via handle_or_raise but do NOT
        # roll back the written item — it is safely in PG and the OUTBOX.
        try:
            new_row = accepted_rows[0]
            response_item = await stac_generator.create_item_from_feature(
                request=request,
                catalog_id=catalog_id,
                collection_id=collection_id,
                stac_config=stac_config,
                lang=language,
                feature=new_row,
            )
            if not response_item:
                raise HTTPException(
                    status_code=500,
                    detail="Failed to generate STAC Item from created row.",
                )
            logical_id = new_row.id or items_to_validate[0].id
            response_item.id = str(logical_id)
            return JSONResponse(
                content=response_item.to_dict(),
                status_code=status.HTTP_201_CREATED,
                headers={"Location": f"{get_url(request)}/{logical_id}"},
            )
        except HTTPException:
            raise
        except Exception as e:
            return handle_or_raise(
                e,
                resource_name="STAC Item",
                resource_id=f"{catalog_id}:{collection_id}",
                operation="STAC Item response rendering",
            )

    async def update_stac_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        item_payload: STACItem,
        request: Request,
        engine=Depends(get_async_engine),
        language: str = Depends(get_language),
    ):
        # Fail-fast guard: no item writes on a not-ready catalog.
        await self._require_catalog_ready(catalog_id)

        # Write-time STAC validation (lenient — warnings only)
        validate_stac_item(item_payload.model_dump(by_alias=True, exclude_unset=True))

        stac_item = item_payload.to_pystac()
        if not stac_item.collection_id:
            stac_item.collection_id = collection_id

        # Verify item_id matches payload if provided
        if stac_item.id and str(stac_item.id) != str(item_id):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Item ID in path must match item ID in body.",
            )

        # Ensure payload ID is set to path ID
        stac_item.id = item_id

        from dynastore.models.protocols import ItemsProtocol

        items_svc = get_protocol(ItemsProtocol)
        if not items_svc:
            raise HTTPException(status_code=500, detail="Items protocol not available.")

        catalogs_svc = await self._get_catalogs_service()

        try:
            # from .metadata_helpers import prune_managed_content # Removed
            from .stac_extension_protocol import StacExtensionProtocol

            # Prune managed content (assets/extensions) from the payload before storage
            # This is handled by sidecar prepare_upsert_payload (in-place pruning)
            providers = get_protocols(StacExtensionProtocol)

            async with managed_transaction(engine) as conn:
                # PUT-by-geoid resolution (#1367). Post-#1212 the canonical
                # surface-level item id IS the geoid, so a PUT path-id is
                # usually a geoid — not an external_id. ``upsert`` keys
                # conflict resolution on external_id; without translating
                # first, the geoid string never matches any stored
                # external_id and a fresh row is inserted alongside the
                # geoid-keyed one. Symmetric to the sidecar geoid lookup in
                # ``delete_item``: when the path-id IS a known geoid, swap
                # the payload id for that row's external_id so upsert
                # updates the right row in place. Falls through unchanged
                # when the path-id isn't a known geoid (create-on-PUT and
                # external-id-keyed PUT semantics preserved).
                resolved_external_id = await items_svc.resolve_external_id_by_geoid(
                    catalog_id,
                    collection_id,
                    str(item_id),
                    ctx=DriverContext(db_resource=conn),
                )
                if resolved_external_id is not None and resolved_external_id != str(item_id):
                    item_payload.id = resolved_external_id

                # Upsert the item
                new_row_or_list = await items_svc.upsert(
                    catalog_id,
                    collection_id,
                    items=item_payload,  # Pass STACItem model directly
                    ctx=DriverContext(db_resource=conn),
                )
                new_row = (
                    new_row_or_list
                    if not isinstance(new_row_or_list, list)
                    else new_row_or_list[0]
                )

                # Fetch config inside transaction
                stac_config = await self._get_stac_config(
                    catalog_id, collection_id, db_resource=conn
                )

            # Response generation OUTSIDE the transaction
            response_item = await stac_generator.create_item_from_feature(
                request=request,
                catalog_id=catalog_id,
                collection_id=collection_id,
                stac_config=stac_config,
                feature=new_row,
                lang=language,
            )

            if not response_item:
                raise HTTPException(
                    status_code=500,
                    detail="Failed to generate STAC Item from created row.",
                )

            # response_item.id = str(stac_item.id)

            return JSONResponse(
                content=response_item.to_dict(),
                status_code=status.HTTP_200_OK,  # 200 OK for update
            )
        except HTTPException:
            raise
        except ValueError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
        except Exception as e:
            return handle_or_raise(
                e,
                resource_name="STAC Item",
                resource_id=f"{catalog_id}:{collection_id}:{item_id}",
                operation="STAC Item update",
            )

    async def delete_stac_item(
        self,
        request: Request,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        engine=Depends(get_async_engine),
    ):
        logger.info(
            f"STAC: delete_stac_item called for catalog='{catalog_id}', collection='{collection_id}', item='{item_id}'"
        )
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)

        async with managed_transaction(engine) as conn:
            return await self._delete_item(
                catalog_id, collection_id, item_id, conn,
                caller_id=self._principal_caller_id(request),
            )

    async def search_items_post(
        self,
        request: Request,
        search_request: ItemSearchRequest,
        engine=Depends(get_async_engine),
        language: str = Depends(get_language),
        catalog_id: Optional[str] = None,
        f: str = Query("geojson", description="Output format: 'geojson' or 'geoparquet'"),
    ):
        # Path param overrides body when present (new canonical path includes catalog_id)
        if catalog_id:
            search_request = search_request.model_copy(update={"catalog_id": catalog_id})
        if not search_request.catalog_id:
            raise HTTPException(status_code=400, detail="catalog_id required")
        async with managed_transaction(engine) as conn:
            stac_config = await self._get_stac_config(
                search_request.catalog_id, db_resource=conn
            )
            try:
                from dynastore.modules.storage.access_scope import (
                    principals_from_request_state,
                )

                _principals, _principal = principals_from_request_state(request)
                rows, count, aggregations = await search_items(
                    conn, search_request, stac_config,
                    principals=_principals, principal=_principal,
                )
            except ValueError as exc:
                # Invalid filter (e.g. unknown queryable property) — surface as a
                # 400 instead of a 500, matching the ``/items`` filter path.
                raise HTTPException(status_code=400, detail=str(exc))

        from dynastore.models.shared_models import OutputFormatEnum
        if f in (OutputFormatEnum.GEOPARQUET, "geoparquet", "parquet"):
            from dynastore.tools.file_io import write_geoparquet
            from fastapi.responses import StreamingResponse

            def _geoparquet_stream():
                yield from write_geoparquet(iter(rows), srid=4326)

            return StreamingResponse(
                content=_geoparquet_stream(),
                media_type="application/geoparquet",
                headers={"Content-Disposition": 'attachment; filename="search.parquet"'},
            )

        # Release connection before PySTAC processing
        results = await stac_generator.create_search_results_collection(
            request,
            rows,
            count,
            search_request.limit,
            search_request.offset,
            stac_config,
            lang=language,
        )
        if aggregations:
            results["aggregations"] = aggregations
        return JSONResponse(content=results)

    async def search_stac_collections_post(
        self,
        request: Request,
        search_req: CollectionSearchRequest,
        engine=Depends(get_async_engine),
        language: str = Depends(get_language),
    ):
        try:
            async with managed_transaction(engine) as conn:
                collections, total_count = await search_collections(conn, search_req)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))

        # Release connection before PySTAC processing
        stac_collections = []
        for coll in collections:
            # Localize before creating PySTAC collection
            localized_coll, _ = stac_localize(coll, language)
            if coll.extent is None:
                continue
            stac_coll = pystac.Collection(
                id=str(localized_coll.get("id") or ""),
                description=str(localized_coll.get("description") or ""),
                title=localized_coll.get("title"),
                license=str(localized_coll.get("license") or ""),
                extent=pystac.Extent(
                    spatial=pystac.SpatialExtent(coll.extent.spatial.bbox),
                    temporal=pystac.TemporalExtent(coll.extent.temporal.interval),
                ),
            )
            # Inject language metadata
            if "language" in localized_coll:
                stac_coll.extra_fields["language"] = localized_coll["language"]
            if "languages" in localized_coll:
                stac_coll.extra_fields["languages"] = localized_coll["languages"]

            stac_collections.append(stac_coll.to_dict())
        return JSONResponse(
            content={
                "collections": stac_collections,
                "context": {
                    "limit": search_req.limit,
                    "matched": total_count,
                    "returned": len(stac_collections),
                },
            }
        )

    @router.post(
        "/catalogs/{catalog_id}/collections/{collection_id}/aggregate",
        response_class=JSONResponse,
    )
    async def aggregate_collection_items(
        catalog_id: str,  # type: ignore[reportGeneralTypeIssues]
        collection_id: str,
        request: Request,
        aggregation_request: AggregationRequest,
        engine=Depends(get_async_engine),
    ):
        """
        Executes aggregations on collection items.
        Supports OGC STAC Aggregation Extension.
        """
        from dynastore.extensions.stac import stac_aggregations

        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)

        async with managed_transaction(engine) as conn:
            # Get STAC config
            configs = get_protocol(ConfigsProtocol)
            if configs is None:
                raise HTTPException(status_code=500, detail="ConfigsProtocol not available")
            _cfg = await configs.get_config(
                StacPluginConfig, catalog_id, collection_id, ctx=DriverContext(db_resource=conn
            ))
            assert isinstance(_cfg, StacPluginConfig)
            stac_config = _cfg

            if not stac_config.aggregations:
                raise HTTPException(
                    status_code=403,
                    detail="Aggregations are not enabled for this collection.",
                )

            max_aggs = stac_config.aggregations.max_aggregations_per_request
            if len(aggregation_request.aggregations) > max_aggs:
                raise HTTPException(
                    status_code=400,
                    detail=f"Too many aggregations requested. Maximum is {max_aggs}.",
                )

            if not stac_config.aggregations.allow_custom:
                default_names = {
                    rule.name for rule in stac_config.aggregations.default_rules
                }
                requested_names = {
                    agg.name for agg in aggregation_request.aggregations
                }
                if not requested_names.issubset(default_names):
                    raise HTTPException(
                        status_code=403,
                        detail="Custom aggregations are not allowed. Use only pre-configured aggregations.",
                    )

            # Build WHERE clause from filters
            where_clauses = ["h.deleted_at IS NULL"]
            params = {}

            if aggregation_request.bbox:
                bbox = aggregation_request.bbox
                where_clauses.append(
                    "ST_Intersects(h.geom, ST_MakeEnvelope(:bbox_xmin, :bbox_ymin, :bbox_xmax, :bbox_ymax, 4326))"
                )
                params.update(
                    bbox_xmin=bbox[0],
                    bbox_ymin=bbox[1],
                    bbox_xmax=bbox[2],
                    bbox_ymax=bbox[3],
                )

            if aggregation_request.datetime:
                # Simple datetime filter (could be enhanced for intervals)
                where_clauses.append("s.attributes->>'datetime' = :datetime")
                params["datetime"] = aggregation_request.datetime

            where_sql = " AND ".join(where_clauses)

            # Execute aggregations
            collections = aggregation_request.collections or [collection_id]
            results = await stac_aggregations.execute_aggregations(
                conn,
                catalog_id,
                collections,
                aggregation_request.aggregations,
                where_sql,
                params,
            )

        return JSONResponse(content={"aggregations": results})
