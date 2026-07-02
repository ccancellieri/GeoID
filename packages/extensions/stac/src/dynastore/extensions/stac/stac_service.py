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

# dynastore/extensions/stac/stac_service.py

import logging
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, Tuple, List, FrozenSet

import pystac
from fastapi import FastAPI, APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import Response
from dynastore.extensions.tools.fast_api import AppJSONResponse as JSONResponse
from dynastore.models.driver_context import DriverContext
from dynastore.models.protocols import ConfigsProtocol


from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.web.decorators import expose_static
from dynastore.extensions.tools.db import get_async_engine
from dynastore.extensions.tools.query import parse_hints_param
from dynastore.extensions.tools.exception_handlers import handle_or_raise
from dynastore.modules.db_config.query_executor import (
    managed_transaction,
)
from dynastore.extensions.stac.search import (
    CollectionSearchRequest,
    ItemSearchRequest,
    search_collections,
    search_items,
)

from dynastore.modules.stac.stac_config import StacPluginConfig

from dynastore.tools.db import validate_sql_identifier
from dynastore.tools.geospatial import BboxDimensionality, parse_bbox_string
from .stac_models import STACCatalogRequest, stac_localize
from .stac_validator import validate_stac_item, validate_stac_collection
from dynastore.models.shared_models import Feature
from . import stac_generator
from .stac_models import (
    STACCollectionUpdate,
    STACItem,
    STACItemCollection,
    STACItemOrItemCollection,
    STACCollectionRequest,
    STACCatalogUpdate,
)
from .stac_aggregation_models import AggregationRequest
from dynastore.extensions.ogc_base import OGCServiceMixin, OGCTransactionMixin
from dynastore.extensions.tools.url import get_url
from dynastore.tools.discovery import get_protocol, get_protocols
from dynastore.models.localization import normalize_i18n_for_replace

logger = logging.getLogger(__name__)


def _reject_internal_id(value: str, prefix: str, resource_name: str) -> None:
    """Reject a public path-param id shaped like an internal storage id.

    The STAC REST surface is keyed on external ids only (#2354): an internal
    id (``{prefix}_{13 base32}``) must never resolve to a real resource here,
    even when a stale/lenient lookup further down the stack would find one —
    that would let a caller route around external-id-scoped IAM policies.
    Data-layer callers that already hold a resolved internal id never reach
    this function; it only guards the public route-handler entry points.
    """
    from dynastore.modules.catalog.catalog_service import is_internal_physical_name

    if is_internal_physical_name(value, prefix):
        raise HTTPException(
            status_code=404, detail=f"{resource_name} '{value}' not found."
        )


from dynastore.extensions.tools.language_utils import get_language
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


# Fields declared in STACCollectionRequest as explicit typed fields.
# Everything NOT in this set that appears in model_dump(extra="allow") is a
# STAC extension extra (e.g. cube:dimensions, themes, sci:citation, bands).
# providers and summaries are declared but stored in collection_stac, which
# requires StacStorageConfig to be active; we also fold them into extra_metadata
# as a fallback so they survive catalogs where collection_stac is not active.
_STAC_REQUEST_SCHEMA_FIELDS = frozenset({
    "type", "stac_version", "stac_extensions", "id",
    "title", "description", "keywords", "license",
    "providers", "extent", "summaries",
    "assets", "item_assets", "links", "extra_metadata",
})


def _pack_stac_extras(input_data: Dict[str, Any], language: str) -> Dict[str, Any]:
    """Move STAC extras and declared-but-stac-only fields into extra_metadata.

    Fields not in ``_STAC_REQUEST_SCHEMA_FIELDS`` are STAC extension extras
    (``cube:dimensions``, ``themes``, ``sci:citation``, etc.).  They are
    never stored in the core PG column set, so we fold them into
    ``extra_metadata`` where the read path already exposes them via
    ``collection.extra_fields`` (stac_generator lines 512-517).

    ``providers`` and ``summaries`` ARE in ``collection_stac``, but that
    sidecar is gated by ``StacStorageConfig``.  Folding them into
    ``extra_metadata`` as well makes them round-trip even without the preset,
    via the fallback in ``stac_generator.create_collection``.
    """
    extras: Dict[str, Any] = {}
    # Collect non-schema STAC extension extras
    for key in list(input_data.keys()):
        if key not in _STAC_REQUEST_SCHEMA_FIELDS:
            extras[key] = input_data.pop(key)
    # Fold providers/summaries/extent/stac_extensions into extras as fallback
    # storage so they round-trip via collection_core even when collection_stac
    # is not active (no StacStorageConfig).  The read path restores them via
    # stac_generator helpers before falling back to hardcoded defaults.
    # Only store non-empty values: an empty extent/stac_extensions carries no
    # useful data to preserve and should not force extra_metadata creation.
    for key in ("providers", "summaries"):
        if key in input_data and input_data[key] is not None:
            extras[key] = input_data[key]
    for key in ("extent", "stac_extensions"):
        val = input_data.get(key)
        if val:  # non-None and non-empty (empty dict/list skipped)
            extras[key] = val
    if not extras:
        return input_data
    # Merge into existing extra_metadata or create it
    existing = input_data.get("extra_metadata")
    if isinstance(existing, dict):
        from dynastore.models.localization import _LANGUAGE_METADATA
        if any(k in _LANGUAGE_METADATA for k in existing):
            # Already language-keyed: merge into the request language bucket
            bucket = existing.setdefault(language, {})
            bucket.update(extras)
        else:
            # Flat dict — merge directly; wrap_extra_metadata will language-key it
            existing.update(extras)
    else:
        input_data["extra_metadata"] = extras
    return input_data


STAC_API_URIS = [
    "https://api.stacspec.org/v1.0.0/core",
    "https://api.stacspec.org/v1.0.0/item-search",
    "https://api.stacspec.org/v1.1.0/item-search#sort",
    "https://api.stacspec.org/v1.0.0/collections",
    "https://api.stacspec.org/v1.0.0/ogcapi-features/extensions/transaction",
    "https://api.stacspec.org/v1.0.0/item-search/definition",
    "https://api.stacspec.org/v1.0.0/item-search#filter",
    "https://api.stacspec.org/v1.0.0/item-search#sort",
    "https://api.stacspec.org/v1.0.0/item-search#fields",
    "https://api.stacspec.org/v1.0.0/item-search#context",
    "https://api.stacspec.org/v1.0.0/item-search#aggregation",
    "https://api.stacspec.org/v1.0.0/item-search#query",
    "https://api.stacspec.org/v1.0.0/item-search#filter:cql-json",
    "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/filter",
    "http://www.opengis.net/spec/cql2/1.0/conf/cql2-text",
    "http://www.opengis.net/spec/cql2/1.0/conf/cql2-json",
    "http://www.opengis.net/spec/cql2/1.0/conf/basic-cql2",
    "https://api.stacspec.org/v1.0.0/children",
    # STAC Collection Search extension
    "https://api.stacspec.org/v1.0.0/collection-search",
    "http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/simple-query",
    "http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/simple-query#free-text",
    "http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/simple-query#filter",
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
        from dynastore.tools.discovery import register_plugin, unregister_plugin
        from .stac_contributor import LanguageStacContributor
        from .wmts_web_map_links import WmtsWebMapLinksContributor

        language_contributor = LanguageStacContributor()
        # Web-map-links enrichment registers here, not in the maps extension:
        # STAC item assembly runs in scope_catalog (which does not load maps),
        # so registering on the STAC service is what makes the ``wmts_tiles``
        # enrichment reach STAC reads. One instance satisfies both
        # AssetContributor and StacContributor.
        wmts_web_map_links_contributor = WmtsWebMapLinksContributor()
        register_plugin(language_contributor)
        register_plugin(wmts_web_map_links_contributor)
        try:
            # Policies declared via PolicyContributor; IAM forwards centrally.
            yield
        finally:
            unregister_plugin(language_contributor)
            unregister_plugin(wmts_web_map_links_contributor)

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
            # Cross-catalog Item Search over the platform public items alias —
            # the root catalog has always advertised rel=search → /search.
            ("/search", "search_items_global_get", ["GET"], {"response_class": _J}),
            ("/search", "search_items_global_post", ["POST"], {"response_class": _J}),
            ("/catalogs", "list_stac_catalogs", ["GET"], {"response_class": _J}),
            ("/catalogs/{catalog_id}", "get_stac_catalog", ["GET"], {"response_class": _J}),
            ("/catalogs/{catalog_id}/collections", "list_stac_collections", ["GET"], {"response_class": _J}),
            ("/catalogs/{catalog_id}/collections/{collection_id}", "get_stac_collection", ["GET"], {"response_class": _J}),
            # Write Operations
            ("/catalogs", "create_stac_catalog", ["POST"], {}),
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
            # Search Endpoints — STAC API standard
            # Item Search: GET and POST /catalogs/{catalog_id}/search
            ("/catalogs/{catalog_id}/search", "search_items_get", ["GET"], {"response_class": _J}),
            ("/catalogs/{catalog_id}/search", "search_items_post", ["POST"], {"response_class": _J}),
            # Non-standard paths removed: /collections-search, /collections/search (deprecated),
            # /search bare (deprecated). Capability is preserved via the standard paths above
            # (item search) and GET /catalogs/{catalog_id}/collections (collection search).
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

    async def _should_validate_on_write(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
    ) -> bool:
        """Return True only when write-time STAC validation is explicitly enabled.

        Defaults to False to avoid synchronous remote schema fetches on the
        async event loop during ingest (see #1884).
        """
        try:
            cfg = await self._get_stac_config(catalog_id, collection_id)
            return cfg.validate_on_write
        except Exception:
            return False

    # ------------------------------------------------------------------
    # OGCServiceMixin hook overrides — STAC-specific behaviour
    # ------------------------------------------------------------------

    def _validate_catalog_create(self) -> None:
        """Verify the deployment can persist a STAC catalog envelope."""
        _assert_stac_capable_collection_stack()

    async def _require_catalog_write_ready(
        self,
        catalog_id: str,
        catalogs_svc=None,
    ) -> None:
        """Reject writes against catalogs whose provisioning has not completed."""
        await self._require_catalog_ready(catalog_id, catalogs_svc=catalogs_svc)

    def _make_collection_create_kwargs(self) -> Dict[str, Any]:
        """Signal to the collection manager that this is a STAC-context write."""
        return {"stac_context": True}

    def _localize_resource(self, model: Any, language: str) -> Tuple[Dict[str, Any], Any]:
        """Use STAC-specific localization that preserves STAC envelope fields."""
        return stac_localize(model, language)

    async def _pre_update_collection_validate(
        self,
        catalog_id: str,
        collection_id: str,
        input_data: Dict[str, Any],
        request=None,
    ) -> None:
        """Validate the merged STAC collection before committing the PATCH.

        Fetches the current collection state and validates the dict that would
        result from merging ``input_data`` on top of it.  Validation is lenient
        (warnings only from ``validate_stac_collection``) because PATCH sends a
        partial body whose required fields are satisfied by the already-stored
        document.  Skipped when ``request`` is ``None`` or the existing
        collection cannot be fetched.
        """
        if request is None:
            return
        if not await self._should_validate_on_write(catalog_id, collection_id):
            return
        existing = await stac_generator.create_collection(
            request, catalog_id=catalog_id, collection_id=collection_id, lang="en"
        )
        if existing is not None:
            from .stac_validator import validate_stac_collection
            merged = {**existing.to_dict(), **input_data}
            validate_stac_collection(merged)

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
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        """Lists available STAC catalogs.

        Catalog visibility is governed entirely by the policy layer
        (IamMiddleware gates this route and the per-catalog routes); this
        handler performs no role-based response filtering. The admin
        catalog picker (GET /admin/catalogs) is the surface that narrows
        the list per caller.
        """
        catalogs_svc = await self._get_catalogs_service()
        catalogs = await catalogs_svc.list_catalogs(limit=limit, offset=offset, lang=language)

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
        self,
        catalog_id: str,
        request: Request,
        language: str = Depends(get_language),
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        catalog_id = validate_sql_identifier(catalog_id)
        _reject_internal_id(catalog_id, "c", "Catalog")
        catalogs_svc = await self._get_catalogs_service()
        if not await catalogs_svc.get_catalog(catalog_id, lang=language, hints=request_hints):
            raise HTTPException(
                status_code=404, detail=f"Catalog '{catalog_id}' not found."
            )
        catalog_dict = await stac_generator.create_catalog(
            request, catalog_id=catalog_id, lang=language, hints=request_hints,
        )
        return JSONResponse(content=catalog_dict)

    async def list_stac_collections(
        self,
        catalog_id: str,
        request: Request,
        engine=Depends(get_async_engine),
        language: str = Depends(get_language),
        request_hints: FrozenSet = Depends(parse_hints_param),
        # STAC Collection Search extension query params (GET /collections)
        bbox: Optional[str] = Query(
            None,
            description="Bounding box as comma-separated: minx,miny,maxx,maxy (EPSG:4326).",
        ),
        datetime: Optional[str] = Query(
            None,
            description="RFC 3339 date-time or interval.",
        ),
        q: Optional[str] = Query(
            None,
            description=(
                "Free-text search: comma-separated terms matched against "
                "collection id, title, and description."
            ),
        ),
        limit: int = Query(10, ge=1, le=1000, description="Maximum number of collections to return."),
        offset: int = Query(0, ge=0, description="Number of collections to skip."),
        sortby: Optional[str] = Query(
            None,
            description=(
                "Sort field with optional '+' (asc) or '-' (desc) prefix. "
                "Aliases: 'code'=id, 'label'=title. E.g. '+code', '-label'."
            ),
        ),
    ):
        """GET /catalogs/{catalog_id}/collections — list or search STAC collections.

        When any STAC Collection Search extension parameters are present (bbox,
        datetime, q, sortby) the request is routed through search_collections().
        With no search parameters the plain listing path is used
        (stac_generator.create_collections_catalog).

        Conforms to: https://api.stacspec.org/v1.0.0/collection-search and
        http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/simple-query.
        """
        catalog_id = validate_sql_identifier(catalog_id)
        _reject_internal_id(catalog_id, "c", "Catalog")
        catalogs_svc = await self._get_catalogs_service()
        try:
            catalog = await catalogs_svc.get_catalog(catalog_id, lang=language, hints=request_hints)
        except (ValueError, Exception):
            catalog = None
        if not catalog:
            raise HTTPException(
                status_code=404, detail=f"Catalog '{catalog_id}' not found."
            )

        # Detect whether any Collection Search params were supplied
        has_search_params = any([bbox, datetime, q, sortby])
        if not has_search_params and limit == 10 and offset == 0:
            # Plain listing — no search parameters
            try:
                collections = await stac_generator.create_collections_catalog(
                    request, catalog_id=catalog_id, lang=language, hints=request_hints,
                )
                return JSONResponse(content=collections)
            except AttributeError as e:
                raise HTTPException(
                    status_code=501,
                    detail="STAC Collection listing not yet implemented in generator.",
                ) from e

        # Collection Search path — parse params into CollectionSearchRequest
        parsed_bbox = None
        if bbox:
            try:
                parsed_bbox = parse_bbox_string(
                    bbox,
                    dimensionality=BboxDimensionality.STRICT_2D,
                    validate_geometry=False,
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

        parsed_q: Optional[List[str]] = (
            [t.strip() for t in q.split(",") if t.strip()] if q else None
        )

        search_req = CollectionSearchRequest(
            catalog_id=catalog_id,
            bbox=parsed_bbox,
            datetime=datetime,
            q=parsed_q,
            limit=limit,
            offset=offset,
            sortby=sortby,
        )

        try:
            async with managed_transaction(engine) as conn:
                collections_list, total_count = await search_collections(conn, search_req)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        stac_collections = []
        for coll in collections_list:
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
                    "offset": search_req.offset,
                    "matched": total_count,
                    "returned": len(stac_collections),
                },
            }
        )

    async def get_stac_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        language: str = Depends(get_language),
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        _reject_internal_id(catalog_id, "c", "Catalog")
        _reject_internal_id(collection_id, "col", "Collection")
        catalogs_svc = await self._get_catalogs_service()
        if not await catalogs_svc.get_collection(catalog_id, collection_id, hints=request_hints):
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{catalog_id}:{collection_id}' not found.",
            )
        collection = await stac_generator.create_collection(
            request, catalog_id=catalog_id, collection_id=collection_id, lang=language, hints=request_hints,
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
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        try:
            input_data = definition.model_dump(exclude_unset=True)
            # Pass input_data as both the payload and the exclude_unset dump so
            # detect_use_lang inside the shared body reads the same dict.
            # ``?hints=defer`` flows to create_catalog, deferring GCP storage
            # provisioning so the catalog is created core-only (see Hint.DEFER).
            return await self._ogc_create_catalog(
                input_data, input_data, language, None, hints=request_hints
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
        try:
            input_data = request_body.model_dump(exclude_unset=True)
            # Write-time STAC validation is opt-in (StacPluginConfig.validate_on_write,
            # default False): it is lenient (warnings only) and pystac's remote schema
            # fetch must never block the ingest hot path (#1884).
            if await self._should_validate_on_write(catalog_id):
                validate_stac_collection(input_data)
            # Fold STAC extras (cube:dimensions, themes, sci:citation, …) and the
            # declared-but-stac-only fields (providers, summaries) into extra_metadata
            # so they round-trip via collection_core even when collection_stac is not
            # active (no StacStorageConfig applied to the catalog).
            input_data = _pack_stac_extras(input_data, language)
            return await self._ogc_create_collection(
                catalog_id, input_data, language, None
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
        request: Request,
        language: str = Depends(get_language),
    ):
        # PUT replaces the whole resource.  A body ``id`` matching the path id
        # is a normal replace.  A different body ``id`` is allowed ONLY with
        # ``Prefer: handling=move`` (RFC 7240) — without that header STAC
        # Transaction mandates 400 (on_id_mismatch="reject").
        input_data = definition.model_dump(exclude_unset=False)
        input_data = normalize_i18n_for_replace(input_data, language)
        body_id = definition.id
        return await self._ogc_replace_catalog(
            catalog_id, input_data, language, None,
            request=request, body_id=body_id, on_id_mismatch="reject",
        )

    async def update_stac_catalog(
        self,
        catalog_id: str,
        definition: STACCatalogUpdate,
        request: Request,
        language: str = Depends(get_language),
    ):
        input_data = definition.model_dump(exclude_unset=True)
        body_id: Optional[str] = input_data.get("id")
        return await self._ogc_update_catalog(
            catalog_id, input_data, language, None,
            body_id=body_id, request=request, on_id_mismatch="reject",
        )

    async def delete_stac_catalog(
        self,
        catalog_id: str,
        request: Request,
        force: bool = Query(
            False,
            description=(
                "Force hard deletion (drops the schema and all data, can't be "
                "undone). When false (default) the catalog is soft-deleted."
            ),
        ),
    ):
        return await self._ogc_delete_catalog(catalog_id, force, None, request=request)

    async def replace_stac_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request_body: STACCollectionRequest,
        request: Request,
        language: str = Depends(get_language),
    ):
        # PUT replaces the whole collection.  A body ``id`` different from the
        # path id is allowed ONLY with ``Prefer: handling=move`` — without that
        # header STAC Transaction mandates 400 (on_id_mismatch="reject").
        body_id = request_body.id
        input_data = request_body.model_dump(exclude_unset=False)
        if await self._should_validate_on_write(catalog_id, collection_id):
            validate_stac_collection(input_data)
        input_data = _pack_stac_extras(input_data, language)
        input_data = normalize_i18n_for_replace(input_data, language)
        return await self._ogc_replace_collection(
            catalog_id, collection_id, input_data, language,
            request=request, body_id=body_id, on_id_mismatch="reject",
        )

    async def update_stac_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request_body: STACCollectionUpdate,
        request: Request,
        language: str = Depends(get_language),
    ):
        # Sidecars are handled transparently by ItemsProtocol / LifecycleRegistry.
        input_data = request_body.model_dump(exclude_unset=True)
        body_id: Optional[str] = input_data.get("id")
        # Fold any STAC extension extras in the PATCH payload (cube:dimensions,
        # themes, …) and providers/summaries into extra_metadata for round-trip.
        input_data = _pack_stac_extras(input_data, language)
        return await self._ogc_update_collection(
            catalog_id, collection_id, input_data, language, request,
            body_id=body_id, on_id_mismatch="reject",
        )

    async def delete_stac_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
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
        return await self._ogc_delete_collection(catalog_id, collection_id, force, None, request)

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
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        catalog_id = validate_sql_identifier(catalog_id)
        collection_id = validate_sql_identifier(collection_id)
        _reject_internal_id(catalog_id, "c", "Catalog")
        _reject_internal_id(collection_id, "col", "Collection")
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
            reject_unknown_filter_params,
            resolve_queryable_property_names,
        )
        from dynastore.modules.storage.hints import Hint

        extra_filters = {
            key: value
            for key, value in request.query_params.items()
            if key not in OGC_RESERVED_QUERY_PARAMS and value != ""
        }
        # Reject an unmapped filter name before it is folded into CQL and
        # dispatched: the SEARCH-driver fast path below has no CQL-parser
        # validation seam, so an unknown name would otherwise be silently
        # dropped from the ES query while a count computed elsewhere
        # disagreed with the (unfiltered) listing (#2682, same fix as the
        # OGC Features ``/items`` endpoint).
        if extra_filters:
            valid_names = await resolve_queryable_property_names(
                catalog_id, collection_id
            )
            reject_unknown_filter_params(extra_filters, valid_names)
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
            # Opt-in exact-geometry path: when the caller requests the
            # ``geometry_exact`` hint (``?hints=geometry_exact``) skip the
            # simplified-geometry ES fast-path so create_item_collection falls
            # through to get_stac_items_paginated, which reads directly from the
            # exact-capable driver (PostgreSQL).  With no such hint the ES
            # fast-path stays byte-for-byte unchanged.
            wants_exact = Hint.GEOMETRY_EXACT in request_hints
            search_dispatch = None
            if not cql_filter and not wants_exact:
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
                    hints=request_hints,
                )
            except ValueError as e:
                # Unknown property / malformed CQL → 400
                raise HTTPException(status_code=400, detail=str(e)) from e
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
        _reject_internal_id(catalog_id, "c", "Catalog")
        _reject_internal_id(collection_id, "col", "Collection")

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

        # Write-time STAC validation per item (lenient — warnings only).
        # Skipped by default (validate_on_write=False) to prevent blocking
        # network schema fetches on the async event loop during batch ingest.
        items_to_validate: list[STACItem] = (
            list(item_payload.features)
            if isinstance(item_payload, STACItemCollection)
            else [item_payload]
        )
        if await self._should_validate_on_write(catalog_id, collection_id):
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
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
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

        if await self._should_validate_on_write(catalog_id, collection_id):
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

        try:
            # Managed content (assets/extensions) is pruned from the payload by
            # the sidecar prepare_upsert_payload (in-place); no pruning here.

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
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)
            ) from e
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
        _reject_internal_id(catalog_id, "c", "Catalog")
        _reject_internal_id(collection_id, "col", "Collection")

        async with managed_transaction(engine) as conn:
            return await self._delete_item(
                catalog_id, collection_id, item_id, conn,
                caller_id=self._principal_caller_id(request),
            )

    async def search_items_get(
        self,
        request: Request,
        catalog_id: str,
        engine=Depends(get_async_engine),
        language: str = Depends(get_language),
        bbox: Optional[str] = Query(
            None,
            description="Bounding box as comma-separated: minx,miny,maxx,maxy (EPSG:4326).",
        ),
        datetime: Optional[str] = Query(
            None,
            description="RFC 3339 date-time or interval (e.g. 2021-01-01T00:00:00Z or 2021-01-01/2021-12-31).",
        ),
        ids: Optional[str] = Query(None, description="Comma-separated Item IDs."),
        collections: Optional[str] = Query(None, description="Comma-separated Collection IDs."),
        limit: int = Query(10, ge=1, le=1000, description="Maximum number of items to return."),
        offset: int = Query(0, ge=0, description="Number of items to skip."),
        filter: Optional[str] = Query(
            None,
            alias="filter",
            description="CQL2 filter expression. Defaults to CQL2-Text encoding unless filter-lang overrides.",
        ),
        filter_lang: str = Query(
            "cql2-text",
            alias="filter-lang",
            description="Filter encoding: 'cql2-text' (default for GET) or 'cql2-json'.",
        ),
        sortby: Optional[str] = Query(
            None,
            description=(
                "Comma-separated sort fields with optional '+' (asc) or '-' (desc) prefix. "
                "Example: '+datetime,-id'."
            ),
        ),
        f: str = Query("geojson", description="Output format: 'geojson' or 'geoparquet'."),
    ):
        """STAC API Item Search — GET /catalogs/{catalog_id}/search.

        Maps standard STAC GET query parameters into an ItemSearchRequest and
        delegates to the same engine path as the POST variant.  Per the STAC
        spec, GET filter defaults to cql2-text; callers may override via
        filter-lang=cql2-json.
        """
        # Parse bbox: comma-separated floats → 4-tuple
        parsed_bbox = None
        if bbox:
            try:
                parsed_bbox = parse_bbox_string(
                    bbox,
                    dimensionality=BboxDimensionality.STRICT_2D,
                    validate_geometry=False,
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

        # Parse ids: comma-separated → list
        parsed_ids: Optional[List[str]] = (
            [i.strip() for i in ids.split(",") if i.strip()] if ids else None
        )

        # Parse collections: comma-separated → list
        parsed_collections: Optional[List[str]] = (
            [c.strip() for c in collections.split(",") if c.strip()] if collections else None
        )

        # Parse sortby: comma-separated "+field,-field2" → list of individual entries
        parsed_sortby: Optional[List[str]] = None
        if sortby:
            parsed_sortby = [s.strip() for s in sortby.split(",") if s.strip()]

        # Parse filter: CQL2-Text/JSON string → AttributeFilter/QueryFilter
        parsed_filter = None
        if filter:
            if filter_lang == "cql2-json":
                import json as _json
                try:
                    filter_body = _json.loads(filter)
                except Exception as exc:
                    raise HTTPException(
                        status_code=400, detail="Invalid cql2-json filter"
                    ) from exc
                from pydantic import ValidationError as _VE
                try:
                    from dynastore.extensions.stac.search import QueryFilter as _QF
                    parsed_filter = _QF.model_validate(filter_body)
                except _VE:
                    try:
                        from dynastore.extensions.stac.search import AttributeFilter as _AF
                        parsed_filter = _AF.model_validate(filter_body)
                    except _VE as exc:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Could not parse cql2-json filter: {exc}",
                        ) from exc
            else:
                # cql2-text: parse via pygeofilter then encode to CQL2-JSON dict
                try:
                    import json as _json2
                    from pygeofilter.parsers.cql2_text import parse as _parse_cql2_text
                    from pygeofilter.backends.cql2_json import to_cql2 as _to_cql2_json_str
                    _ast = _parse_cql2_text(filter)
                    _cql2_json_dict = _json2.loads(_to_cql2_json_str(_ast))
                    from pydantic import ValidationError as _VE2
                    try:
                        from dynastore.extensions.stac.search import QueryFilter as _QF2
                        parsed_filter = _QF2.model_validate(_cql2_json_dict)
                    except _VE2:
                        try:
                            from dynastore.extensions.stac.search import AttributeFilter as _AF2
                            parsed_filter = _AF2.model_validate(_cql2_json_dict)
                        except _VE2 as exc:
                            raise HTTPException(
                                status_code=400,
                                detail=f"Could not parse cql2-text filter: {exc}",
                            ) from exc
                except ImportError as exc:
                    raise HTTPException(
                        status_code=501,
                        detail="CQL2-Text filter requires pygeofilter (not installed)",
                    ) from exc
                except HTTPException:
                    raise
                except Exception as exc:
                    raise HTTPException(
                        status_code=400, detail=f"Invalid cql2-text filter: {exc}"
                    ) from exc

        search_request = ItemSearchRequest(
            catalog_id=catalog_id,
            collections=parsed_collections,
            ids=parsed_ids,
            bbox=parsed_bbox,
            datetime=datetime,
            filter=parsed_filter,
            filter_lang="cql2-json" if filter else filter_lang,
            limit=limit,
            offset=offset,
            sortby=parsed_sortby,
        )

        async with managed_transaction(engine) as conn:
            stac_config = await self._get_stac_config(catalog_id, db_resource=conn)
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
                raise HTTPException(status_code=400, detail=str(exc)) from exc

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
                raise HTTPException(status_code=400, detail=str(exc)) from exc

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

    async def _global_search_response(
        self,
        request: Request,
        *,
        ids: Optional[List[str]],
        collections: Optional[List[str]],
        bbox: Optional[Any],
        intersects: Optional[Dict[str, Any]],
        datetime: Optional[str],
        sortby: Optional[List[str]],
        limit: int,
        offset: int,
        language: str,
    ):
        """Shared core of the cross-catalog ``/search`` GET and POST routes.

        Queries the platform public items alias (every per-catalog public
        items index — see ``modules/elasticsearch/global_search``) and
        serializes through the same ItemCollection presenter the
        catalog-scoped search uses, attributing each hit to its owning
        catalog. Item links are rendered with the platform-default STAC
        config: per-catalog render policies do not apply at this
        cross-catalog surface.
        """
        from dynastore.modules.elasticsearch.global_search import (
            search_public_items,
        )

        page = await search_public_items(
            ids=ids,
            collections=collections,
            bbox=bbox,
            intersects=intersects,
            datetime=datetime,
            sortby=sortby,
            limit=limit,
            offset=offset,
        )
        if page is None:
            raise HTTPException(
                status_code=501,
                detail=(
                    "Cross-catalog search requires the platform search "
                    "backend (Elasticsearch), which is not configured on "
                    "this deployment."
                ),
            )

        features: List[Feature] = []
        for hit in page.hits:
            feature = Feature.model_validate(hit.item)
            if feature.properties is None:
                feature.properties = {}
            # Cross-collection tracking consumed by the serializer — the same
            # markers the catalog-scoped dispatch injects.
            feature.properties["_catalog_id"] = hit.catalog_id or ""
            feature.properties["_collection_id"] = hit.item.get("collection") or ""
            features.append(feature)

        results = await stac_generator.create_search_results_collection(
            request,
            features,
            page.total,
            limit,
            offset,
            StacPluginConfig(),
            lang=language,
        )
        return JSONResponse(content=results)

    async def search_items_global_get(
        self,
        request: Request,
        language: str = Depends(get_language),
        bbox: Optional[str] = Query(
            None,
            description="Bounding box as comma-separated: minx,miny,maxx,maxy (EPSG:4326).",
        ),
        datetime: Optional[str] = Query(
            None,
            description="RFC 3339 date-time or interval (e.g. 2021-01-01T00:00:00Z or 2021-01-01/2021-12-31).",
        ),
        ids: Optional[str] = Query(None, description="Comma-separated Item IDs."),
        collections: Optional[str] = Query(None, description="Comma-separated Collection IDs."),
        limit: int = Query(10, ge=1, le=1000, description="Maximum number of items to return."),
        offset: int = Query(0, ge=0, description="Number of items to skip."),
        sortby: Optional[str] = Query(
            None,
            description=(
                "Comma-separated sort fields with optional '+' (asc) or '-' (desc) prefix. "
                "Example: '+datetime,-id'."
            ),
        ),
    ):
        """STAC API cross-catalog Item Search — GET /search.

        Discovery across every catalog's public items via the platform
        alias. Structural parameters only; CQL2 ``filter`` is not supported
        at this surface (use the catalog-scoped ``/catalogs/{id}/search``).
        """
        parsed_bbox = None
        if bbox:
            try:
                parsed_tuple = parse_bbox_string(
                    bbox,
                    dimensionality=BboxDimensionality.STRICT_2D,
                    validate_geometry=False,
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            assert parsed_tuple is not None  # bbox truthy above => allow_none path not hit
            parsed_bbox = list(parsed_tuple)

        parsed_ids = [i.strip() for i in ids.split(",") if i.strip()] if ids else None
        parsed_collections = (
            [c.strip() for c in collections.split(",") if c.strip()]
            if collections
            else None
        )
        parsed_sortby = (
            [s.strip() for s in sortby.split(",") if s.strip()] if sortby else None
        )

        return await self._global_search_response(
            request,
            ids=parsed_ids,
            collections=parsed_collections,
            bbox=parsed_bbox,
            intersects=None,
            datetime=datetime,
            sortby=parsed_sortby,
            limit=limit,
            offset=offset,
            language=language,
        )

    async def search_items_global_post(
        self,
        request: Request,
        search_request: ItemSearchRequest,
        language: str = Depends(get_language),
    ):
        """STAC API cross-catalog Item Search — POST /search.

        Accepts the same body as the catalog-scoped search minus
        ``catalog_id`` (scoping across catalogs is the point). A CQL2
        ``filter`` is rejected: filter translation is a per-catalog,
        queryables-aware concern served by ``/catalogs/{id}/search``.
        """
        if search_request.catalog_id:
            raise HTTPException(
                status_code=400,
                detail=(
                    "catalog_id is not accepted on the cross-catalog /search; "
                    "use /catalogs/{catalog_id}/search for a scoped search."
                ),
            )
        if search_request.filter is not None:
            raise HTTPException(
                status_code=501,
                detail=(
                    "CQL2 filter is not supported on the cross-catalog "
                    "/search; use /catalogs/{catalog_id}/search."
                ),
            )

        return await self._global_search_response(
            request,
            ids=search_request.ids,
            collections=search_request.collections,
            bbox=list(search_request.bbox) if search_request.bbox else None,
            intersects=search_request.intersects,
            datetime=search_request.datetime,
            sortby=search_request.sortby,
            limit=search_request.limit,
            offset=search_request.offset,
            language=language,
        )

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
            raise HTTPException(status_code=422, detail=str(exc)) from exc

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
        _reject_internal_id(catalog_id, "c", "Catalog")
        _reject_internal_id(collection_id, "col", "Collection")

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
