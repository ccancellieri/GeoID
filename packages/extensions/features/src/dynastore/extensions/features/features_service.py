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

# dynastore/extensions/features/features_service.py

from typing import Optional, List, Any, FrozenSet, Union, cast

import logging
import os

from dynastore.extensions.tools.ondemand_cache import ondemand_cache_lookup

import pygeofilter as _pygeofilter_scope_gate  # noqa: F401  # SCOPE gate: extension_features requires pygeofilter
_ = _pygeofilter_scope_gate  # silence pyright "unused" — load-bearing for SCOPE filtering

from dynastore.models.driver_context import DriverContext
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    Request,
    Response,
    status,
    FastAPI,
)
from sqlalchemy.ext.asyncio import AsyncConnection
from dynastore.extensions.tools.fast_api import AppJSONResponse as JSONResponse
from dynastore.extensions.tools.exception_handlers import handle_or_raise
from dynastore.models.localization import LocalizedText
from contextlib import asynccontextmanager
from dynastore.models.protocols import (
    ItemsProtocol,
    CRSProtocol,
)
from dynastore.tools.discovery import get_protocol
from dynastore.extensions.features.features_config import (
    FeaturesPluginConfig,
)
from dynastore.extensions.features import ogc_generator, ogc_models

from dynastore.models.shared_models import (
    Link,
    FunctionDescription,
    FunctionsResponse,
    Catalog,
)
from dynastore.models.ogc import Feature as _OGCFeature
from dynastore.extensions.tools.url import get_root_url, get_url
from dynastore.extensions.tools.language_utils import get_language
from dynastore.extensions.tools.response_i18n import (  # noqa: E402
    localize_model,
    localize_response_dict,
)
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.ogc_base import OGCServiceMixin, OGCTransactionMixin
from dynastore.extensions.web.decorators import expose_web_page, expose_static
from dynastore.extensions.tools.db import (
    get_async_connection,
    get_async_connection_bounded,
    get_async_engine,
)
from dynastore.modules.db_config.query_executor import DbResource, managed_transaction
import re
from dynastore.extensions.tools.formatters import OutputFormatEnum
from dynastore.extensions.tools.query import (  # noqa: E402
    parse_ogc_query_request,
    parse_hints_param,
    stream_ogc_features,
    resolve_items_read_policy,
    validate_filter_lang,
    resolve_geometry_flag_from_query,
    dispatch_or_stream_items,
)
from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType
from dynastore.modules.storage.hints import EXACT_READ_HINTS

logger = logging.getLogger(__name__)

from dynastore.models.protocols.crs import CRSProtocol

# Define the conformance classes this specific extension provides.
OGC_API_FEATURES_URIS = [
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
    "http://www.opengis.net/spec/ogcapi-features-2/1.0/conf/crs",
    "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/filter",
    "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/features-filter",
    "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/queryables",
    "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/sort",
    "http://www.opengis.net/spec/cql2/1.0/conf/cql2-text",
    "http://www.opengis.net/spec/ogcapi-features-4/1.0/conf/create-replace-delete",
]

# A static list of supported CQL2 functions. In a future implementation, this could be
# made dynamic based on the actual capabilities of the query backend.
SUPPORTED_CQL_FUNCTIONS = [
    FunctionDescription(
        name="S_Intersects",
        returns=["boolean"],
        arguments=[{"type": ["geometry", "geometry"]}],
    ),
    FunctionDescription(
        name="S_Equals",
        returns=["boolean"],
        arguments=[{"type": ["geometry", "geometry"]}],
    ),
    FunctionDescription(
        name="S_Disjoint",
        returns=["boolean"],
        arguments=[{"type": ["geometry", "geometry"]}],
    ),
    FunctionDescription(
        name="S_Touches",
        returns=["boolean"],
        arguments=[{"type": ["geometry", "geometry"]}],
    ),
    FunctionDescription(
        name="S_Within",
        returns=["boolean"],
        arguments=[{"type": ["geometry", "geometry"]}],
    ),
    FunctionDescription(
        name="S_Overlaps",
        returns=["boolean"],
        arguments=[{"type": ["geometry", "geometry"]}],
    ),
    FunctionDescription(
        name="S_Crosses",
        returns=["boolean"],
        arguments=[{"type": ["geometry", "geometry"]}],
    ),
    FunctionDescription(
        name="S_Contains",
        returns=["boolean"],
        arguments=[{"type": ["geometry", "geometry"]}],
    ),
]


class OGCFeaturesService(ExtensionProtocol, OGCServiceMixin, OGCTransactionMixin):
    priority: int = 100
    router: APIRouter

    # OGCServiceMixin class attributes
    conformance_uris = OGC_API_FEATURES_URIS
    prefix = "/features"
    protocol_title = "DynaStore OGC API Features"
    protocol_description = (
        "OGC API Features (Parts 1-4) with CQL2 filtering, multi-CRS support, "
        "queryables, sorting, and full CRUD transactions."
    )

    def configure_app(self, app: FastAPI):
        """Early configuration for the Features extension."""
        pass

    def __init__(self, app: Optional[FastAPI] = None):
        """Initializes the service and registers its routes."""
        super().__init__()
        self.app = app
        self.router = APIRouter(prefix="/features", tags=["OGC API - Features"])
        self._register_routes()

    def contribute(self, ref):
        """AssetContributor: emit a GeoJSON feature link for items."""
        from dynastore.models.protocols.asset_contrib import AssetLink
        if ref.item_id is None:
            return
        href = (
            f"{ref.base_url}{self.router.prefix}"
            f"/catalogs/{ref.catalog_id}/collections/{ref.collection_id}/items/{ref.item_id}"
        )
        yield AssetLink(
            key="geojson",
            href=href,
            title="OGC API Feature",
            media_type="application/geo+json",
            roles=("data",),
        )

    def _register_routes(self):
        """Registers all OGC API Features routes."""
        self.router.add_api_route(
            "/",
            self.get_landing_page,
            methods=["GET"],
            response_model=ogc_models.LandingPage,
        )
        self.router.add_api_route(
            "/conformance",
            self.get_conformance,
            methods=["GET"],
            response_model=ogc_models.Conformance,
        )
        self.router.add_api_route(
            "/functions",
            self.get_supported_functions,
            methods=["GET"],
            response_model=FunctionsResponse,
        )

        # --- Catalog Endpoints ---
        self.router.add_api_route(
            "/catalogs",
            self.list_catalogs,
            methods=["GET"],
            response_model=ogc_models.Catalogs,
        )
        self.router.add_api_route(
            "/catalogs",
            self.create_catalog,
            methods=["POST"],
            response_model=Catalog,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}",
            self.get_catalog,
            methods=["GET"],
            response_model=Catalog,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}",
            self.replace_catalog,
            methods=["PUT"],
            response_model=Catalog,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}",
            self.update_catalog,
            methods=["PATCH"],
            response_model=Catalog,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}",
            self.delete_catalog,
            methods=["DELETE"],
            status_code=status.HTTP_204_NO_CONTENT,
        )

        # --- Collection Endpoints ---
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections",
            self.list_collections_in_catalog,
            methods=["GET"],
            response_model=ogc_models.Collections,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections",
            self.create_collection,
            methods=["POST"],
            response_model=ogc_models.OGCCollection,
            status_code=status.HTTP_201_CREATED,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self.get_collection,
            methods=["GET"],
            response_model=ogc_models.OGCCollection,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self.replace_collection,
            methods=["PUT"],
            response_model=ogc_models.OGCCollection,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self.update_collection,
            methods=["PATCH"],
            response_model=ogc_models.OGCCollection,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self.delete_collection,
            methods=["DELETE"],
            status_code=status.HTTP_204_NO_CONTENT,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/queryables",
            self.get_queryables,
            methods=["GET"],
            response_model=ogc_models.Queryables,
        )

        # --- Item Endpoints ---
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/items",
            self.get_items,
            methods=["GET"],
            response_model=ogc_models.FeatureCollection,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/items",
            self.add_item,
            methods=["POST"],
            response_model=Union[ogc_models.Feature, ogc_models.BulkCreationResponse],
            status_code=status.HTTP_201_CREATED,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}",
            self.get_item,
            methods=["GET"],
            response_model=ogc_models.Feature,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}",
            self.replace_item,
            methods=["PUT"],
            response_model=ogc_models.Feature,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}",
            self.delete_item,
            methods=["DELETE"],
            status_code=status.HTTP_204_NO_CONTENT,
        )

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        self.register_policies()
        logger.info("OGCFeaturesService: policies registered.")
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

    async def _resolve_crs_srid(
        self, conn: DbResource, catalog_id: str, crs_uri: Optional[str]
    ) -> Optional[int]:
        """Resolves a CRS URI to an SRID using the CRSProtocol."""
        if not crs_uri:
            return None
        if "CRS84" in crs_uri.upper():
            return 4326
        match = re.search(r"[/|:](\d+)$", crs_uri)
        if match:
            return int(match.group(1))

        crs_mod = get_protocol(CRSProtocol)
        if crs_mod:
            crs_def = await crs_mod.get_crs_by_uri(conn, catalog_id, crs_uri)
            if crs_def and hasattr(crs_def, "srid"):
                return crs_def.srid
        return None

    async def _resolve_property_names(
        self, catalog_id: str, collection_id: str
    ) -> set:
        """Return the set of valid property names for a collection.

        Thin wrapper around the shared
        :func:`dynastore.extensions.tools.query.resolve_queryable_property_names`
        — the SSOT also used to validate the ad-hoc ``?{property}={value}``
        filter shorthand below and the STAC items endpoint's equivalent
        filter validation — kept as an instance method so existing callers
        (and tests that patch it) are unaffected.
        """
        from dynastore.extensions.tools.query import (
            resolve_queryable_property_names,
        )

        return await resolve_queryable_property_names(catalog_id, collection_id)

    async def get_landing_page(
        self, request: Request, language: str = Depends(get_language)
    ):
        landing_page = ogc_generator.create_landing_page(request, language=language)
        return JSONResponse(content=localize_model(landing_page, language))

    async def get_conformance(self, request: Request):
        """Returns the list of conformance classes (Part 1)."""
        return await self.ogc_conformance_handler(request)

    # --- Catalog Endpoints ---
    async def list_catalogs(
        self,
        request: Request,
        limit: int = Query(10, ge=1),
        offset: int = Query(0, ge=0),
        language: str = Depends(get_language),
    ):
        catalogs_svc = await self._get_catalogs_service()
        catalogs = await catalogs_svc.list_catalogs(
            limit=limit, offset=offset, lang=language
        )
        self_url = get_url(request)
        self_link = Link(href=self_url, rel="self", type="application/json")

        # Convert returned models to CatalogDefinition models and add links
        result_catalogs = []
        for catalog in catalogs:
            catalog_dict, _ = catalog.localize(language)
            # catalog_dict["id"] is the public external label (projected by
            # _serialize_public_id via model_dump inside localize()).
            cat_pub = catalog_dict["id"]
            # Add links to each catalog
            catalog_dict["links"] = [
                Link(
                    href=f"{self_url}/{cat_pub}", rel="self", type="application/json"
                ).model_dump(),
                Link(
                    href=f"{self_url}/{cat_pub}/collections",
                    rel="items",
                    type="application/json",
                ).model_dump(),
            ]
            result_catalogs.append(ogc_models.CatalogDefinition(**catalog_dict))

        return ogc_models.Catalogs(catalogs=result_catalogs, links=[self_link])

    async def create_catalog(
        self,
        definition: ogc_models.CatalogDefinition,
        conn: AsyncConnection = Depends(get_async_connection),
        language: str = Depends(get_language),
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        """Creates a new catalog, its data schema, and required table partitions."""
        try:
            catalog_data = {
                "id": definition.id,
                "title": definition.title,
                "description": definition.description,
                "keywords": definition.keywords,
                "license": definition.license,
                "extra_metadata": definition.extra_metadata,
            }
            input_dump = definition.model_dump(exclude_unset=True)
            # ``?hints=defer`` defers GCP storage provisioning (see Hint.DEFER).
            return await self._ogc_create_catalog(
                catalog_data, input_dump, language, conn, hints=request_hints
            )
        except Exception as e:
            return handle_or_raise(
                e,
                resource_name="Catalog",
                resource_id=definition.id,
                operation="OGC Features Catalog creation",
            )

    async def get_catalog(
        self, catalog_id: str, request: Request, language: str = Depends(get_language)
    ):
        catalog = await self._resolve_catalog_or_404(catalog_id, lang=language)

        catalog_dict, languages = catalog.localize(language)
        self_url = get_url(request)
        catalog_dict["links"] = [
            Link(href=self_url, rel="self", type="application/json").model_dump(),
            Link(
                href=f"{get_root_url(request)}/features/catalogs",
                rel="parent",
                type="application/json",
            ).model_dump(),
            Link(
                href=f"{self_url}/collections", rel="items", type="application/json"
            ).model_dump(),
        ]
        return JSONResponse(content=catalog_dict)

    async def replace_catalog(
        self,
        catalog_id: str,
        definition: ogc_models.CatalogDefinition,
        request: Request,
        conn: AsyncConnection = Depends(get_async_connection),
        language: str = Depends(get_language),
    ):
        """OGC API Features Part 4 — replace the whole catalog (PUT).

        Per OGC API Features Part 4 Req 11, a body ``id`` that differs from the
        path parameter is silently ignored and the path-addressed resource is
        replaced (on_id_mismatch="ignore").  To rename instead, send
        ``Prefer: handling=move``; the catalog is then renamed to the body id
        and the response carries ``Content-Location``, ``Link: rel=canonical``,
        and ``Preference-Applied: handling=move``.
        """
        from dynastore.models.localization import normalize_i18n_for_replace

        body_id = definition.id
        catalog_dict = definition.model_dump(exclude_unset=False)
        catalog_dict = normalize_i18n_for_replace(catalog_dict, language)
        return await self._ogc_replace_catalog(
            catalog_id, catalog_dict, language, conn,
            request=request, body_id=body_id, on_id_mismatch="ignore",
        )

    async def update_catalog(
        self,
        catalog_id: str,
        definition: ogc_models.CatalogDefinition,
        request: Request,
        conn: AsyncConnection = Depends(get_async_connection),
        language: str = Depends(get_language),
    ):
        """OGC API Features Part 4 — partial update of a catalog (PATCH).

        A body ``id`` that differs from the path parameter is silently ignored
        per OGC API Features Part 4 Req 11.  Send ``Prefer: handling=move`` to
        rename instead.
        """
        catalog_dict = definition.model_dump(exclude_unset=True)
        body_id: Optional[str] = catalog_dict.get("id")
        return await self._ogc_update_catalog(
            catalog_id, catalog_dict, language, conn,
            body_id=body_id, request=request, on_id_mismatch="ignore",
        )

    async def delete_catalog(
        self,
        catalog_id: str,
        request: Request,
        force: bool = Query(False),
        conn: AsyncConnection = Depends(get_async_connection),
    ):
        return await self._ogc_delete_catalog(catalog_id, force, conn, request=request)

    # --- Collection Endpoints ---
    async def list_collections_in_catalog(
        self,
        catalog_id: str,
        request: Request,
        limit: int = Query(10, ge=1),
        offset: int = Query(0, ge=0),
        language: str = Depends(get_language),
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        catalogs_svc = await self._get_catalogs_service()
        collections = await catalogs_svc.list_collections(
            catalog_id, lang=language, limit=limit, offset=offset, hints=request_hints,
        )
        # Convert returned models to OGCCollection models and add links
        ogc_collections = [
            ogc_models.OGCCollection(**c.localize(language)[0]) for c in collections
        ]

        self_url = get_url(request)
        self_link = Link(href=self_url, rel="self", type="application/json")
        parent_link = Link(
            href=f"{get_root_url(request)}/features/catalogs/{catalog_id}",
            rel="parent",
            type="application/json",
        )

        for collection in ogc_collections:
            collection.links.append(
                Link(
                    href=f"{self_url}/{collection.id}",
                    rel="self",
                    type="application/json",
                )
            )
            collection.links.append(
                Link(
                    href=f"{self_url}/{collection.id}/items",
                    rel="items",
                    type="application/json",
                )
            )

        # Part 2: Add global CRS list to Collections response
        supported_crs = ["http://www.opengis.net/def/crs/OGC/1.3/CRS84"]

        return ogc_models.Collections(
            collections=ogc_collections,
            links=[self_link, parent_link],
            crs=supported_crs,
        )

    async def get_supported_functions(
        self,
        request: Request,
        language: str = Depends(get_language),
    ):
        """Returns a list of supported filter functions (Part 3)."""
        # The list of functions is defined at the module level.
        # In a future implementation, this could be made dynamic based on backend capabilities.
        resp = FunctionsResponse(functions=SUPPORTED_CQL_FUNCTIONS)
        d = localize_response_dict(resp.model_dump(exclude_none=True), language)
        return JSONResponse(content=d)

    async def get_queryables(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        conn: AsyncConnection = Depends(get_async_connection),
        language: str = Depends(get_language),
    ):
        """Returns the filterable properties for a collection as a JSON Schema (Part 3)."""
        columns, driver_fields = await self._collect_queryable_fields(
            catalog_id, collection_id, conn
        )
        return await ogc_generator.create_queryables_response(
            request, catalog_id, collection_id, columns, language=language,
            driver_fields=driver_fields or None,
        )

    async def create_collection(
        self,
        catalog_id: str,
        collection_def: ogc_models.CollectionDefinition,
        language: str = Depends(get_language),
    ):
        """Creates a new collection in a catalog."""
        try:
            collection_dict = collection_def.model_dump(exclude_unset=True)
            # Pass None, not a request-scoped connection: `_ogc_create_collection`
            # / `create_collection` provision the collection's own partitions
            # internally and must do so on their own short-lived connection,
            # the same way STAC's create_stac_collection already does — not
            # under the caller's request transaction (#2831).
            return await self._ogc_create_collection(
                catalog_id, collection_dict, language, None
            )
        except Exception as e:
            return handle_or_raise(
                e,
                resource_name="Collection",
                resource_id=f"{catalog_id}:{collection_def.id}",
                operation="OGC Features Collection creation",
            )

    async def get_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        language: str = Depends(get_language),
        request_hints: FrozenSet = Depends(parse_hints_param),
    ):
        collection = await self._resolve_collection_or_404(
            catalog_id, collection_id,
            detail="Collection not found",
            lang=language, hints=request_hints,
        )

        # We need to construct the OGC response wrapper
        collection_dict, languages = collection.localize(language)
        ogc_collection = ogc_models.OGCCollection(**collection_dict)

        self_url = get_url(request)
        ogc_collection.links = [
            Link(href=self_url, rel="self", type="application/json"),
            Link(
                href=f"{get_root_url(request)}/features/catalogs/{catalog_id}/collections",
                rel="parent",
                type="application/json",
            ),
            Link(
                href=f"{self_url}/items",
                rel="items",
                type="application/geo+json",
                title=LocalizedText(en="Items in this collection"),
            ),
            Link(
                href=f"{self_url}/queryables",
                rel="queryables",
                type="application/schema+json",
                title=LocalizedText(en="Queryable properties"),
            ),
        ]

        # Run CollectionPipelineProtocol stages (e.g. StylesCollectionPipeline
        # merging item_assets defaults). Pipeline works on the fully-composed
        # collection document; a stage returning None drops the collection → 404.
        from dynastore.modules.catalog.collection_pipeline_runner import (
            apply_collection_pipeline,
        )
        collection_dict_out = (
            ogc_collection.model_dump(exclude_none=True)
            if hasattr(ogc_collection, "model_dump")
            else ogc_collection.dict(exclude_none=True)
        )
        rewritten = await apply_collection_pipeline(
            catalog_id, collection_id, collection_dict_out, context={},
        )
        if rewritten is None:
            raise HTTPException(status_code=404, detail="Collection not found")
        return JSONResponse(content=rewritten, status_code=status.HTTP_200_OK)

    async def replace_collection(
        self,
        catalog_id: str,
        collection_id: str,
        collection_def: ogc_models.CollectionDefinition,
        request: Request,
        language: str = Depends(get_language),
    ):
        """OGC API Features Part 4 — replace the whole collection (PUT).

        Per OGC API Features Part 4 Req 11, a body ``id`` that differs from the
        path parameter is silently ignored and the path-addressed resource is
        replaced (on_id_mismatch="ignore").  To rename instead, send
        ``Prefer: handling=move``; the collection is then renamed to the body id
        and the response carries ``Content-Location``, ``Link: rel=canonical``,
        and ``Preference-Applied: handling=move``.
        """
        from dynastore.models.localization import normalize_i18n_for_replace

        body_id = collection_def.id
        updates_dict = collection_def.model_dump(exclude_unset=False)
        updates_dict = normalize_i18n_for_replace(updates_dict, language)
        return await self._ogc_replace_collection(
            catalog_id, collection_id, updates_dict, language,
            request=request, body_id=body_id, on_id_mismatch="ignore",
        )

    async def update_collection(
        self,
        catalog_id: str,
        collection_id: str,
        collection_def: ogc_models.CollectionDefinition,
        request: Request,
        language: str = Depends(get_language),
    ):
        """OGC API Features Part 4 — partial update of a collection (PATCH).

        A body ``id`` that differs from the path parameter is silently ignored
        per OGC API Features Part 4 Req 11.  Send ``Prefer: handling=move`` to
        rename instead.
        """
        updates_dict = collection_def.model_dump(exclude_unset=True)
        body_id: Optional[str] = updates_dict.get("id")
        return await self._ogc_update_collection(
            catalog_id, collection_id, updates_dict, language, request,
            body_id=body_id, on_id_mismatch="ignore",
        )

    async def delete_collection(
        self,
        request: Request,
        catalog_id: str,
        collection_id: str,
        force: bool = Query(False),
        conn: AsyncConnection = Depends(get_async_connection),
    ):
        return await self._ogc_delete_collection(catalog_id, collection_id, force, conn, request)

    # --- Item Endpoints ---
    async def get_items(
        self,
        request: Request,
        catalog_id: str,
        collection_id: str,
        # Bounded, fail-fast pool acquire (#2933/#2948) — see get_item.
        conn: AsyncConnection = Depends(get_async_connection_bounded),
        limit: Optional[int] = Query(
            None,
            ge=1,
            description=(
                "The maximum number of features to return. Omitted falls back "
                "to the configured default; a value above the configured "
                "maximum is clamped, not rejected (OGC API - Features Part 1 "
                "Core /req/core/fc-limit-response-1)."
            ),
        ),
        offset: int = Query(
            0, ge=0, description="The offset of the first feature to return."
        ),
        bbox: Optional[str] = Query(
            None,
            description="Bounding box filter. Comma-separated: minx,miny,maxx,maxy",
        ),
        datetime_param: Optional[str] = Query(
            None,
            alias="datetime",
            description="Temporal filter. A single datetime or a '/' separated interval.",
        ),
        filter: Optional[str] = Query(
            None,
            description=(
                "A CQL2 filter expression. Encoding controlled by "
                "``filter-lang`` (``cql2-text`` default, ``cql2-json`` "
                "for a JSON-encoded payload)."
            ),
        ),
        filter_lang: str = Query(
            "cql2-text",
            alias="filter-lang",
            description=(
                "Language of the filter expression. Supported: 'cql2-text' "
                "(default) and 'cql2-json'."
            ),
        ),
        filter_crs: Optional[str] = Query(
            None,
            alias="filter-crs",
            description=(
                "URI of the CRS the geometric values in ``filter=`` are "
                "expressed in. Default = CRS84 (EPSG:4326)."
            ),
        ),
        properties: Optional[str] = Query(
            None,
            description=(
                "Comma-separated attribute names selecting which feature "
                "properties are returned. Each name must be a queryable "
                "property of the collection — unknown names return HTTP 400. "
                "An empty value (``?properties=``) returns only the "
                "OGC-mandatory fields (id, geometry, type, links). "
                "Omitted = all properties returned. Orthogonal to "
                "``skipGeometry``: ``properties`` narrows Feature.properties, "
                "``skipGeometry`` controls Feature.geometry. ``geom`` is not "
                "an accepted ``properties`` name."
            ),
        ),
        skip_geometry: Optional[bool] = Query(
            None,
            alias="skipGeometry",
            description=(
                "When true, the returned Features carry ``geometry: null`` "
                "and the resolved driver omits the geometry from its "
                "projection (PG drops the SELECT, ES adds ``geometry`` to "
                "``_source.excludes``). De-facto pygeoapi convention. "
                "Mutually exclusive with ``returnGeometry`` unless both are "
                "consistent. Default: false."
            ),
        ),
        return_geometry: Optional[bool] = Query(
            None,
            alias="returnGeometry",
            description=(
                "ESRI de-facto alias for ``skipGeometry``. ``returnGeometry=false`` "
                "is equivalent to ``skipGeometry=true``. Passing both with "
                "conflicting values returns HTTP 400."
            ),
        ),
        crs: Optional[str] = Query(None, description="CRS URI for output geometry."),
        bbox_crs: Optional[str] = Query(
            None, description="CRS URI for the bbox parameter."
        ),
        sortby: Optional[str] = Query(
            None,
            description="Sort order for features. Comma-separated list of properties. Use '-' for descending order (e.g., '-propertyA,+propertyB').",
        ),
        f: OutputFormatEnum = Query(
            OutputFormatEnum.GEOJSON,
            alias="f",
            description="The output format for the features.",
        ),
        request_hints: FrozenSet = Depends(parse_hints_param),
        language: str = Depends(get_language),
    ) -> Response:
        catalogs_svc = await self._get_catalogs_service()
        configs_svc = await self._get_configs_service()
        storage_svc = await self._get_storage_service()

        # Default language for internal check
        await self._resolve_collection_or_404(
            catalog_id, collection_id,
            detail=f"Collection '{collection_id}' not found or logically deleted.",
            lang="en",
        )

        # --- Caching Support ---

        _pc = await configs_svc.get_config(
            FeaturesPluginConfig, catalog_id=catalog_id, collection_id=collection_id,
            ctx=DriverContext(db_resource=conn
        ))
        assert isinstance(_pc, FeaturesPluginConfig)
        plugin_config: FeaturesPluginConfig = _pc

        # Resolve default/clamp the page size against the configured policy
        # (OGC API - Features Part 1 Core /req/core/fc-limit-response-1): an
        # over-max ``limit`` is capped, never rejected.
        from dynastore.extensions.tools.pagination import resolve_page_limit
        limit = resolve_page_limit(
            limit,
            default_limit=plugin_config.default_limit,
            max_limit=plugin_config.max_limit,
        )

        if plugin_config.cache_on_demand:
            cached = await ondemand_cache_lookup(
                storage_svc,
                cache_prefix="features_cache",
                catalog_id=catalog_id,
                params=dict(request.query_params),
                media_type="application/geo+json",
            )
            if cached is not None:
                return cached

        try:
            # --- Argument Parsing & SRID Resolution ---

            target_crs_srid = await self._resolve_crs_srid(conn, catalog_id, crs)
            bbox_crs_srid = await self._resolve_crs_srid(conn, catalog_id, bbox_crs)
            # Coerce non-string defaults (Query(...) sentinels seen in direct
            # unit-test calls) to ``None`` before SRID resolution.
            _filter_crs_arg = filter_crs if isinstance(filter_crs, str) else None
            filter_crs_srid = await self._resolve_crs_srid(
                conn, catalog_id, _filter_crs_arg
            )

            # --- ``filter-lang`` validation (#1385: accept cql2-json) ---
            fl_normalised = validate_filter_lang(filter_lang)

            # --- ``properties`` validation -----------------------------
            # Comma-separated attribute names. Validated against the
            # collection's queryable surface (same source as the Queryables
            # endpoint); unknown name → 400. Empty value (``?properties=``)
            # is an explicit request to strip all attribute properties down
            # to the OGC-mandatory fields — leaves ``select_fields`` as an
            # empty list, which the post-fetch projection honours.
            select_fields: Optional[List[str]] = None
            project_only_mandatory = False
            requested_properties: List[str] = []
            if isinstance(properties, str):
                requested_properties = [
                    p.strip() for p in properties.split(",") if p.strip()
                ]
                if properties == "" or not requested_properties:
                    project_only_mandatory = True
                    select_fields = []
                else:
                    select_fields = requested_properties

            # Single-field equality shorthand: any query parameter that is not a
            # reserved OGC parameter is treated as a `?{property}={value}` filter
            # on the collection's attributes. The value is bound as a query
            # parameter — never interpolated into SQL.
            from dynastore.extensions.tools.query import (
                OGC_RESERVED_QUERY_PARAMS,
                reject_unknown_filter_params,
            )

            extra_filters = {
                key: value
                for key, value in request.query_params.items()
                if key not in OGC_RESERVED_QUERY_PARAMS and value != ""
            }

            # Validate ``properties`` names and the ad-hoc ``?{property}=``
            # filter names against the same queryable surface, resolved once
            # and shared by both checks. An unknown filter name must 400
            # here — before any driver dispatch — so ``numberMatched`` and
            # the returned features always describe the same selection;
            # resolving it downstream in the CQL/driver layer let the PG
            # path 400 while a SEARCH driver silently dropped the unmapped
            # predicate and served an unfiltered listing (#2682).
            if requested_properties or extra_filters:
                valid = await self._resolve_property_names(catalog_id, collection_id)
                if valid and requested_properties:
                    unknown = [p for p in requested_properties if p not in valid]
                    if unknown:
                        raise HTTPException(
                            status_code=400,
                            detail=(
                                f"Unknown properties: {', '.join(sorted(unknown))}. "
                                f"Available: {', '.join(sorted(valid))}."
                            ),
                        )
                reject_unknown_filter_params(extra_filters, valid)

            # OGC Features /items prefers exact, full-precision geometry. The
            # routing hint EXACT_READ_HINTS passed to dispatch_or_stream_items
            # below selects the exact-geometry driver (today PG) when one is
            # registered. The hint is a preference, not a hard filter: on a
            # catalog with no exact-geometry driver (e.g. Elasticsearch-only)
            # the router relaxes it and falls back to the available reader, so
            # the list returns items with simplified geometry rather than an
            # empty response. Setting search_dispatch=None skips the ES
            # search fast-path and goes straight to stream_items with the hint,
            # which the relaxed reader still honours.
            search_dispatch: Optional[Any] = None

            # Resolve skipGeometry/returnGeometry from the two accepted forms.
            skip_geom_bool = resolve_geometry_flag_from_query(skip_geometry, return_geometry)

            request_obj = parse_ogc_query_request(
                bbox=bbox,
                datetime_param=datetime_param,
                sortby=sortby,
                filter=filter,
                limit=limit,
                offset=offset,
                bbox_crs_srid=bbox_crs_srid,
                include_total_count=True,
                extra_filters=extra_filters,
                filter_lang=fl_normalised,
                filter_crs_srid=filter_crs_srid,
                select_fields=select_fields,
                skip_geometry=skip_geom_bool,
            )

            # Execute search via protocol (streaming). ctx=None decouples from
            # the request connection to allow background streaming.
            # OGC Features /items always requests exact geometry: pass
            # EXACT_READ_HINTS so the router selects the exact-geometry driver
            # (today PG) regardless of which driver is registered first for READ.
            # When exact is requested the ES fast-path (search_dispatch) is None
            # because is_es_items_driver is False for the exact driver, so
            # dispatch_or_stream_items falls straight through to stream_items.
            items_protocol = cast(ItemsProtocol, catalogs_svc)
            query_response = await dispatch_or_stream_items(
                items_protocol,
                catalog_id=catalog_id,
                collection_id=collection_id,
                query_request=request_obj,
                consumer=ConsumerType.OGC_FEATURES,
                search_dispatch=search_dispatch,
                ctx=None,
                request=request,
                hints=EXACT_READ_HINTS,
            )

            count = query_response.total_count or 0
            root_url = get_root_url(request)
            base_url = str(request.url).split("?")[0]

            # --- Link Construction ---
            from dynastore.extensions.tools.pagination import build_pagination_links
            links = build_pagination_links(request, offset, limit, count)
            # Features-specific: add HTML alternate link
            links.append(
                Link(
                    href=f"{base_url}?f=html",
                    rel="alternate",
                    type="text/html",
                    title=LocalizedText(en="This document as HTML"),
                ),
            )

            # --- OGC post-processing wrapper ---
            from dynastore.extensions.features.ogc_generator import (
                _map_validity_to_ogc,
            )

            collection_url = (
                f"{root_url}/features/catalogs/{catalog_id}"
                f"/collections/{collection_id}"
            )

            # Resolve the projection set once per request. The post-fetch
            # narrowing is the universal fallback for drivers that do not
            # honour ``QueryRequest.select`` (e.g. Elasticsearch returns the
            # whole ``_source`` when the index has no source-filter mapping)
            # and for nested ``properties.foo`` paths that the SQL projection
            # cannot narrow. The driver-layer ``select`` threaded above
            # remains the fast path for PG; the ES driver pushes the same
            # projection down via ``_source.includes/excludes``.
            _projection_set: Optional[set] = None
            if select_fields is not None:
                _projection_set = set(select_fields)

            async def _ogc_post_process(items):
                # ``finally`` propagates an early close (e.g. the response
                # byte-budget cutoff in ``_stream_ogc_json``, #2681) down to
                # ``items`` — the raw driver stream — so its underlying DB
                # connection/transaction is released promptly instead of
                # waiting on garbage collection. ``async for`` alone does not
                # close the iterable it wraps on an early ``aclose()``.
                try:
                    async for feature in items:
                        if feature.properties:
                            # Map validity → start_datetime / end_datetime
                            _map_validity_to_ogc(feature.properties)

                            # Per-feature property projection — applied uniformly
                            # regardless of which driver served the listing.
                            if _projection_set is not None:
                                for key in list(feature.properties.keys()):
                                    if key not in _projection_set:
                                        feature.properties.pop(key, None)

                        # ``skipGeometry`` normalises Feature.geometry to ``null``
                        # at the service boundary. PG already drops the SELECT and
                        # ES adds ``geometry`` to ``_source.excludes``, so this is
                        # the safety net for hits that arrived from any path that
                        # missed the push-down (multi-driver pipelines, mocks, or
                        # cached/legacy index shapes). RFC 7946 explicitly permits
                        # ``"geometry": null`` on a Feature.
                        if skip_geom_bool:
                            feature.geometry = None

                        # Add OGC self/collection links
                        feature_id = feature.id
                        feature.links = [
                            Link(
                                href=f"{collection_url}/items/{feature_id}",
                                rel="self",
                                type="application/geo+json",
                            ),
                            Link(
                                href=collection_url,
                                rel="collection",
                                type="application/json",
                            ),
                        ]
                        yield feature
                finally:
                    aclose = getattr(items, "aclose", None)
                    if aclose is not None:
                        await aclose()

            query_response.items = _ogc_post_process(query_response.items)

            # --- Unified Streaming Response ---
            return stream_ogc_features(
                request=request,
                query_response=query_response,
                output_format=f,
                catalog_id=catalog_id,
                collection_id=collection_id,
                target_srid=target_crs_srid or 4326,
                links=links,
                language=language,
                offset=offset,
                max_response_bytes=plugin_config.max_response_bytes,
            )
        except Exception as e:
            return handle_or_raise(
                e,
                resource_name="Features",
                resource_id=f"{catalog_id}:{collection_id}",
                operation="get items",
            )

    async def get_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        request: Request,
        # Bounded, fail-fast pool acquire (#2933/#2948): under pool
        # saturation this returns 503 well before the request risks
        # riding the Cloud Run ceiling, instead of queuing for the
        # engine's full pool_timeout — same guard as STAC's item
        # GET-by-id / item search (#2947).
        conn: AsyncConnection = Depends(get_async_connection_bounded),
        language: str = Depends(get_language),
    ):
        catalogs_svc = await self._get_catalogs_service()
        items_protocol = cast(ItemsProtocol, catalogs_svc)

        # PG row-level ABAC: compile and inject access_filter when the collection
        # carries an access_envelope sidecar (user-facing single-item read).
        from dynastore.modules.storage.access_scope import (
            collection_uses_pg_access_envelope,
            compile_read_access_filter,
            principals_from_request_state,
        )

        af = None
        if await collection_uses_pg_access_envelope(catalog_id, collection_id):
            principals, principal = principals_from_request_state(request)
            af = await compile_read_access_filter(
                catalog_id=catalog_id,
                collections=[collection_id],
                principals=principals,
                principal=principal,
            )

        # Use ItemsProtocol to get the unified feature
        feature = await items_protocol.get_item(
            catalog_id, collection_id, item_id,
            ctx=DriverContext(db_resource=conn),
            access_filter=af,
        )

        if not feature:
            raise HTTPException(status_code=404, detail=f"Item '{item_id}' not found.")

        layer_config = await catalogs_svc.get_collection_config(
            catalog_id, collection_id, ctx=DriverContext(db_resource=conn)
        )
        read_policy = await resolve_items_read_policy(catalog_id, collection_id)
        root_url = get_root_url(request)
        ogc_feature = ogc_generator._db_row_to_ogc_feature(
            feature, catalog_id, collection_id, root_url, layer_config,
            read_policy=read_policy,
        )
        feature_dict = ogc_feature.model_dump(exclude_none=True, by_alias=True)
        feature_dict = localize_response_dict(feature_dict, language)
        return JSONResponse(content=feature_dict)

    async def add_item(
        self,
        catalog_id: str,
        collection_id: str,
        payload: ogc_models.FeatureOrFeatureCollection,
        request: Request,
        response: Response,
        conn: AsyncConnection = Depends(get_async_connection),
    ):
        from dynastore.modules.storage.driver_config import ItemsWritePolicy
        policy_source = (
            f"/configs/catalogs/{catalog_id}/collections/{collection_id}"
            f"/plugins/{ItemsWritePolicy.class_key()}"
        )
        accepted_rows, rejections, was_single, batch_size = await self._ingest_items(
            catalog_id,
            collection_id,
            payload,
            DriverContext(db_resource=conn),
            policy_source,
        )

        if rejections:
            return self._build_rejection_response(accepted_rows, rejections, batch_size)

        if was_single:
            root_url = get_root_url(request)
            new_row = cast(_OGCFeature, accepted_rows[0])
            feature_id = self._resolve_accepted_ids([new_row])[0]
            location_url = (
                f"{root_url}/features/catalogs/{catalog_id}"
                f"/collections/{collection_id}/items/{feature_id}"
            )
            feature = ogc_generator._db_row_to_ogc_feature(
                new_row, catalog_id, collection_id, root_url
            )
            return JSONResponse(
                content=feature.model_dump(exclude_none=True, by_alias=True),
                status_code=status.HTTP_201_CREATED,
                headers={"Location": location_url},
            )

        return self._build_bulk_creation_response(accepted_rows)

    async def replace_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        feature_def: ogc_models.FeatureDefinition,
        request: Request,
        conn: AsyncConnection = Depends(get_async_connection),
    ):
        if item_id != feature_def.id:
            raise HTTPException(
                status_code=400,
                detail=f"Item ID mismatch: path '{item_id}' vs payload '{feature_def.id}'.",
            )

        catalogs_svc = await self._get_catalogs_service()
        configs_svc = await self._get_configs_service()
        # 1. Get CatalogPluginConfig to correctly process the incoming feature payload.
        layer_config = await catalogs_svc.get_collection_config(
            catalog_id, collection_id
        )
        # 2. Delegate update via upsert (GeoJSON-centric protocol)
        updated_row = await catalogs_svc.upsert(
            catalog_id,
            collection_id,
            items=feature_def,
            ctx=DriverContext(db_resource=conn),
        )
        if not updated_row:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update item.",
            )

        # 3. Format the response.
        root_url = get_root_url(request)
        return ogc_generator._db_row_to_ogc_feature(
            updated_row, catalog_id, collection_id, root_url
        )

    async def delete_item(
        self,
        request: Request,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        engine=Depends(get_async_engine),
    ):
        async with managed_transaction(engine) as conn:
            return await self._delete_item(
                catalog_id, collection_id, item_id, conn,
                caller_id=self._principal_caller_id(request),
            )

    # ------------------------------------------------------------------
    # Web page contribution (WebPageContributor / StaticAssetProvider)
    # ------------------------------------------------------------------

    def get_web_pages(self):
        from dynastore.extensions.tools.web_collect import collect_web_pages
        return collect_web_pages(self)

    def get_static_assets(self):
        from dynastore.extensions.tools.web_collect import collect_static_assets
        return collect_static_assets(self)

    @expose_static("features")
    def provide_static_files(self) -> list:
        """Exposes the internal static directory for the Features browser."""
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        files = []
        for root, _, filenames in os.walk(static_dir):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files

    @expose_web_page(
        page_id="features_browser",
        title={"en": "Features", "fr": "Entités", "es": "Entidades"},
        icon="fa-draw-polygon",
        description={
            "en": "Browse and create vector features on a map.",
            "fr": "Explorer et créer des entités vectorielles sur une carte.",
            "es": "Explorar y crear entidades vectoriales en un mapa.",
        },
    )
    async def provide_features_browser(self, request: Request):
        return await self._serve_page_template("features_browser.html")

    async def _serve_page_template(self, filename: str):
        from dynastore._version import VERSION
        file_path = os.path.join(os.path.dirname(__file__), "static", filename)
        if not os.path.exists(file_path):
            return Response(content=f"Template {filename} not found", status_code=404)
        with open(file_path, "r", encoding="utf-8") as f:
            return Response(content=f.read().replace("{{VERSION}}", VERSION), media_type="text/html")
