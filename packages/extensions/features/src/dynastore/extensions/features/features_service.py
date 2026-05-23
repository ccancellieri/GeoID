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

# dynastore/extensions/features/features_service.py

from typing import Optional, List, Dict, Any, Tuple, Union, cast

import logging

import pygeofilter as _pygeofilter_scope_gate  # noqa: F401  # SCOPE gate: extension_features requires pygeofilter
_ = _pygeofilter_scope_gate  # silence pyright "unused" — load-bearing for SCOPE filtering

from dynastore.modules.db_config.exceptions import ImmutableConfigError
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
from dynastore.extensions.tools.exception_handlers import handle_exception
from dynastore.models.localization import LocalizedText
from contextlib import asynccontextmanager
import asyncio
from dynastore.models.protocols import (
    CatalogsProtocol,
    StorageProtocol,
    ConfigsProtocol,
    ItemsProtocol,
    CRSProtocol,
)
from dynastore.tools.discovery import get_protocol
from dynastore.extensions.features.features_config import (
    FeaturesPluginConfig,
)
from dynastore.extensions.features import ogc_generator, ogc_models
from .policies import register_features_policies

from datetime import datetime, timezone, UTC
from dynastore.models.shared_models import (
    Link,
    FunctionDescription,
    FunctionsResponse,
    ItemDataForDB,
    Catalog,
    Collection,
)
from dynastore.models.ogc import Feature as _OGCFeature
from dynastore.extensions.tools.url import get_root_url, get_url
from dynastore.extensions.tools.language_utils import get_language
from dynastore.extensions.tools.localization_utils import detect_use_lang
from pydantic import BaseModel, Field, ConfigDict, AliasChoices
from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.ogc_base import OGCServiceMixin, OGCTransactionMixin
from dynastore.extensions.tools.db import get_async_connection, get_async_engine
from dynastore.modules.db_config.tools import managed_transaction
from dynastore.modules.db_config.query_executor import DbResource
import re
from . import features_db
from dynastore.extensions.tools.formatters import OutputFormatEnum, format_response
from dynastore.extensions.tools.query import parse_ogc_query_request, stream_ogc_features, resolve_items_read_policy
from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType

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
        self._storage_protocol: Optional[StorageProtocol] = None
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
            status_code=status.HTTP_201_CREATED,
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

    def register_policies(self):
        register_features_policies()

    async def _get_storage_service(self) -> Optional[StorageProtocol]:
        """Helper to get the storage service protocol (Features-specific)."""
        if self._storage_protocol is None:
            self._storage_protocol = get_protocol(StorageProtocol)
        return self._storage_protocol

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

    async def get_landing_page(
        self, request: Request, language: str = Depends(get_language)
    ):
        landing_page = ogc_generator.create_landing_page(request, language=language)
        return JSONResponse(content=landing_page.model_dump())

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
            # Add links to each catalog
            catalog_dict["links"] = [
                Link(
                    href=f"{self_url}/{catalog.id}", rel="self", type="application/json"
                ).model_dump(),
                Link(
                    href=f"{self_url}/{catalog.id}/collections",
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
    ):
        """Creates a new catalog, its data schema, and required table partitions."""
        try:
            catalogs_svc = await self._get_catalogs_service()
            # Use id as code in the internal representation
            # Pass the definition fields individually as required by CatalogsProtocol
            catalog_data = {
                "id": definition.id,
                "title": definition.title,
                "description": definition.description,
                "keywords": definition.keywords,
                "license": definition.license,
                "extra_metadata": definition.extra_metadata,
            }

            # For creation, we check the input data dictionary constructed above
            # Note: definition.title/description might already be resolved/typed by Pydantic
            # We dump the model to check raw input structure
            input_dump = definition.model_dump(exclude_unset=True)

            use_lang = detect_use_lang(input_dump, language)

            created_catalog_model = await catalogs_svc.create_catalog(
                catalog_data=catalog_data,
                lang=use_lang,
                ctx=DriverContext(db_resource=conn),
            )
            localized_data, _ = created_catalog_model.localize(language)
            return JSONResponse(
                content=localized_data,
                status_code=status.HTTP_201_CREATED,
            )
        except Exception as e:
            _exc = handle_exception(
                e,
                resource_name="Catalog",
                resource_id=definition.id,
                operation="OGC Features Catalog creation",
            )
            if isinstance(_exc, HTTPException):
                raise _exc
            return _exc

    async def get_catalog(
        self, catalog_id: str, request: Request, language: str = Depends(get_language)
    ):
        catalogs_svc = await self._get_catalogs_service()
        catalog = await catalogs_svc.get_catalog(catalog_id, lang=language)
        if not catalog:
            raise HTTPException(
                status_code=404, detail=f"Catalog '{catalog_id}' not found."
            )

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
        conn: AsyncConnection = Depends(get_async_connection),
        language: str = Depends(get_language),
    ):
        """OGC API Features Part 4 — replace the whole catalog (PUT).

        ``CatalogDefinition`` enforces required fields (id/title/...) so
        partial bodies are rejected by Pydantic before the handler runs.
        """
        from dynastore.models.localization import normalize_i18n_for_replace

        if definition.id != catalog_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"Body 'id' ({definition.id!r}) must match path catalog_id "
                    f"({catalog_id!r})."
                ),
            )

        # exclude_unset=False -> absent optionals dump as None and clear
        # the corresponding columns. normalize_i18n_for_replace canonicalises
        # mixed-shape i18n input so lang='*' is unambiguous.
        catalog_dict = definition.model_dump(exclude_unset=False)
        catalog_dict = normalize_i18n_for_replace(catalog_dict, language)

        catalogs_svc = await self._get_catalogs_service()
        catalog = await catalogs_svc.update_catalog(
            catalog_id, catalog_dict, lang="*", ctx=DriverContext(db_resource=conn)
        )
        if not catalog:
            raise HTTPException(status_code=404, detail="Catalog not found")
        localized_data, _ = catalog.localize(language)
        return JSONResponse(content=localized_data)

    async def update_catalog(
        self,
        catalog_id: str,
        definition: ogc_models.CatalogDefinition,
        conn: AsyncConnection = Depends(get_async_connection),
        language: str = Depends(get_language),
    ):
        """Updates an existing catalog."""
        catalogs_svc = await self._get_catalogs_service()
        # Use id as code in the internal representation
        catalog_dict = definition.model_dump(exclude_unset=True)

        use_lang = detect_use_lang(catalog_dict, language)

        # Pass raw dict, manager handles validation
        catalog = await catalogs_svc.update_catalog(
            catalog_id, catalog_dict, lang=use_lang, ctx=DriverContext(db_resource=conn)
        )
        if not catalog:
            raise HTTPException(status_code=404, detail="Catalog not found")
        localized_data, _ = catalog.localize(language)
        return JSONResponse(content=localized_data)

    async def delete_catalog(
        self,
        catalog_id: str,
        force: bool = Query(False),
        conn: AsyncConnection = Depends(get_async_connection),
    ):
        catalogs_svc = await self._get_catalogs_service()
        # Catalog Protocol supports force deletion of catalogs
        if not await catalogs_svc.delete_catalog(
            catalog_id, force=force, ctx=DriverContext(db_resource=conn)
        ):
            raise HTTPException(
                status_code=404, detail=f"Catalog '{catalog_id}' not found."
            )
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # --- Collection Endpoints ---
    async def list_collections_in_catalog(
        self,
        catalog_id: str,
        request: Request,
        limit: int = Query(10, ge=1),
        offset: int = Query(0, ge=0),
        language: str = Depends(get_language),
    ):
        catalogs_svc = await self._get_catalogs_service()
        collections = await catalogs_svc.list_collections(
            catalog_id, lang=language, limit=limit, offset=offset
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

    async def get_supported_functions(self, request: Request):
        """Returns a list of supported filter functions (Part 3)."""
        # The list of functions is defined at the module level.
        # In a future implementation, this could be made dynamic based on backend capabilities.
        return FunctionsResponse(functions=SUPPORTED_CQL_FUNCTIONS)

    async def get_queryables(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        conn: AsyncConnection = Depends(get_async_connection),
        language: str = Depends(get_language),
    ):
        """Returns the filterable properties for a collection as a JSON Schema (Part 3)."""
        from dynastore.modules.storage.router import get_driver
        from dynastore.modules.storage.routing_config import Operation
        from dynastore.models.protocols.storage_driver import Capability

        columns: list = []
        driver_fields = None
        try:
            driver = await get_driver(Operation.READ, catalog_id, collection_id)
            if driver and hasattr(driver, "capabilities") and Capability.INTROSPECTION in driver.capabilities:
                schema_info = await driver.introspect_schema(
                    catalog_id, collection_id, db_resource=conn
                )
                columns = [entry.name for entry in schema_info] if schema_info else []
                # For non-PG drivers, get_entity_fields() returns richer FieldDefinition
                # objects (with types and capabilities) than introspect_schema alone.
                if hasattr(driver, "get_entity_fields"):
                    try:
                        driver_fields = await driver.get_entity_fields(
                            catalog_id, collection_id, entity_level="item"
                        )
                    except Exception:
                        driver_fields = None
        except (ValueError, Exception):
            pass

        return await ogc_generator.create_queryables_response(
            request, catalog_id, collection_id, columns, language=language,
            driver_fields=driver_fields or None,
        )

    async def create_collection(
        self,
        catalog_id: str,
        collection_def: ogc_models.CollectionDefinition,
        language: str = Depends(get_language),
        conn: AsyncConnection = Depends(get_async_connection),
    ):
        """Creates a new collection in a catalog."""
        try:
            catalogs_svc = await self._get_catalogs_service()
            # Use id as code in the internal representation
            collection_dict = collection_def.model_dump(exclude_unset=True)

            use_lang = detect_use_lang(collection_dict, language)

            # Pass raw dict, manager handles localization
            # Pass conn as db_resource for transactional integrity
            collection = await catalogs_svc.create_collection(
                catalog_id, collection_dict, lang=use_lang, ctx=DriverContext(db_resource=conn)
            )
            return JSONResponse(
                content=collection.model_dump(by_alias=True, exclude_none=True),
                status_code=status.HTTP_201_CREATED,
            )
        except Exception as e:
            _exc = handle_exception(
                e,
                resource_name="Collection",
                resource_id=f"{catalog_id}:{collection_def.id}",
                operation="OGC Features Collection creation",
            )
            if isinstance(_exc, HTTPException):
                raise _exc
            return _exc

    async def get_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        language: str = Depends(get_language),
    ):
        catalogs_svc = await self._get_catalogs_service()
        collection = await catalogs_svc.get_collection(
            catalog_id, collection_id, lang=language
        )
        if not collection:
            raise HTTPException(status_code=404, detail="Collection not found")

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
        language: str = Depends(get_language),
    ):
        """OGC API Features Part 4 — replace the whole collection (PUT).

        ``CollectionDefinition`` enforces required fields, so a partial
        body returns 422 before the handler runs.
        """
        from dynastore.models.localization import normalize_i18n_for_replace

        if collection_def.id != collection_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"Body 'id' ({collection_def.id!r}) must match path "
                    f"collection_id ({collection_id!r})."
                ),
            )

        updates_dict = collection_def.model_dump(exclude_unset=False)
        updates_dict = normalize_i18n_for_replace(updates_dict, language)

        catalogs_svc = await self._get_catalogs_service()
        updated_collection = await catalogs_svc.update_collection(
            catalog_id=catalog_id,
            collection_id=collection_id,
            updates=updates_dict,
            lang="*",
        )
        if not updated_collection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Collection '{catalog_id}:{collection_id}' not found.",
            )
        collection, _ = updated_collection.localize(language)
        return JSONResponse(content=collection, status_code=status.HTTP_200_OK)

    async def update_collection(
        self,
        catalog_id: str,
        collection_id: str,
        collection_def: ogc_models.CollectionDefinition,
        language: str = Depends(get_language),
    ):
        """Updates an existing collection's metadata."""
        catalogs_svc = await self._get_catalogs_service()
        updates_dict = collection_def.model_dump(exclude_unset=True)

        use_lang = detect_use_lang(updates_dict, language)

        updated_collection = await catalogs_svc.update_collection(
            catalog_id=catalog_id,
            collection_id=collection_id,
            updates=updates_dict,
            lang=use_lang,
        )
        if not updated_collection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Collection '{catalog_id}:{collection_id}' not found.",
            )
        collection, _ = updated_collection.localize(language)
        return JSONResponse(content=collection, status_code=status.HTTP_200_OK)

    async def delete_collection(
        self,
        catalog_id: str,
        collection_id: str,
        force: bool = Query(False),
        conn: AsyncConnection = Depends(get_async_connection),
    ):
        catalogs_svc = await self._get_catalogs_service()
        if not await catalogs_svc.delete_collection(
            catalog_id, collection_id, force, ctx=DriverContext(db_resource=conn)
        ):
            raise HTTPException(status_code=404, detail="Collection not found.")
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    # --- Item Endpoints ---
    async def get_items(
        self,
        request: Request,
        catalog_id: str,
        collection_id: str,
        conn: AsyncConnection = Depends(get_async_connection),
        limit: int = Query(
            10, ge=1, le=1000, description="The maximum number of features to return."
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
            None, description="A CQL2-TEXT filter expression."
        ),
        filter_lang: str = Query(
            "cql2-text",
            description="Language of the filter expression. Only 'cql2-text' is supported.",
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
    ) -> Response:
        catalogs_svc = await self._get_catalogs_service()
        configs_svc = await self._get_configs_service()
        storage_svc = await self._get_storage_service()

        collection_metadata = await catalogs_svc.get_collection(
            catalog_id, collection_id, lang="en"
        )  # Default language for internal check
        if not collection_metadata:
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{collection_id}' not found or logically deleted.",
            )

        # --- Caching Support ---

        _pc = await configs_svc.get_config(
            FeaturesPluginConfig, catalog_id=catalog_id, ctx=DriverContext(db_resource=conn
        ))
        assert isinstance(_pc, FeaturesPluginConfig)
        plugin_config: FeaturesPluginConfig = _pc

        cache_key = None
        if plugin_config.cache_on_demand and storage_svc:
            import hashlib

            # Generate a stable cache key based on sorted parameters
            params = dict(request.query_params)
            cache_params = {
                k: v
                for k, v in params.items()
                if k not in ("_", "access_token", "token")
            }
            param_str = "&".join(f"{k}={v}" for k, v in sorted(cache_params.items()))
            cache_key_hash = hashlib.md5(param_str.encode()).hexdigest()

            bucket_name = await storage_svc.get_storage_identifier(catalog_id)
            if bucket_name:
                cache_key = f"gs://{bucket_name}/features_cache/{cache_key_hash}"

                if await storage_svc.file_exists(cache_key):
                    logger.info(f"Features Cache HIT: {cache_key}")
                    import tempfile

                    with tempfile.NamedTemporaryFile() as tmp:
                        await storage_svc.download_file(cache_key, tmp.name)
                        tmp.seek(0)
                        cached_data = tmp.read()

                    return Response(
                        content=cached_data, media_type="application/geo+json"
                    )

        try:
            # --- Argument Parsing & SRID Resolution ---

            target_crs_srid = await self._resolve_crs_srid(conn, catalog_id, crs)
            bbox_crs_srid = await self._resolve_crs_srid(conn, catalog_id, bbox_crs)

            # Single-field equality shorthand: any query parameter that is not a
            # reserved OGC parameter is treated as a `?{property}={value}` filter
            # on the collection's attributes. The property name is validated
            # against the collection's queryable fields downstream (unknown →
            # 400) and the value is bound as a query parameter — never
            # interpolated into SQL.
            from dynastore.extensions.tools.query import (
                OGC_RESERVED_QUERY_PARAMS,
            )

            extra_filters = {
                key: value
                for key, value in request.query_params.items()
                if key not in OGC_RESERVED_QUERY_PARAMS and value != ""
            }

            # ── Routing-aware items SEARCH-driver dispatch (#1047) ────────
            # Mirror STAC ``/search`` (#1257): for structural-only listings
            # (no CQL2 ``filter`` / shorthand attribute filter, no non-4326
            # CRS reprojection) resolve the items SEARCH driver via routing
            # and dispatch through the backend-agnostic ``ItemSearchProtocol``
            # — public ES, the tenant-private ES index, or any future
            # search-capable driver. The helper returns ``None`` (→ the PG
            # ``stream_items`` path below) for CQL/attribute filters, a
            # read-primary (PG ``QUERY_FALLBACK_SOURCE``) driver, or a driver
            # without the structural-search capability. CRS reprojection is a
            # PG-only capability, so a non-4326 output/bbox CRS also defers.
            from dynastore.extensions.tools.query import (
                maybe_dispatch_items_to_search_driver,
            )

            has_complex_filter = bool(filter) or bool(extra_filters)
            wants_crs_reproject = (
                target_crs_srid not in (None, 4326)
                or bbox_crs_srid not in (None, 4326)
            )
            search_dispatch: Optional[Any] = None
            if not has_complex_filter and not wants_crs_reproject:
                parsed_bbox: Optional[List[float]] = None
                if bbox:
                    try:
                        _b = [float(v) for v in bbox.split(",")]
                        if len(_b) == 4:
                            parsed_bbox = _b
                    except ValueError:
                        parsed_bbox = None
                search_dispatch = await maybe_dispatch_items_to_search_driver(
                    catalog_id=catalog_id,
                    collection_id=collection_id,
                    bbox=parsed_bbox,
                    datetime=datetime_param,
                    limit=limit,
                    offset=offset,
                    has_complex_filter=False,
                )

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
            )

            # Execute search via protocol (streaming)
            items_protocol = cast(ItemsProtocol, catalogs_svc)
            if search_dispatch is not None:
                query_response = search_dispatch
            else:
                try:
                    query_response = await items_protocol.stream_items(
                        catalog_id=catalog_id,
                        collection_id=collection_id,
                        request=request_obj,
                        # Decouple from request connection to allow background streaming
                        ctx=None,
                        consumer=ConsumerType.OGC_FEATURES,
                    )
                except ValueError as e:
                    # Catch invalid properties/fields and return 400
                    raise HTTPException(status_code=400, detail=str(e))

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

            async def _ogc_post_process(items):
                async for feature in items:
                    if feature.properties:
                        # Map validity → start_datetime / end_datetime
                        _map_validity_to_ogc(feature.properties)
                        feature.properties.pop("_total_count", None)

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
            )
        except Exception as e:
            _exc = handle_exception(
                e,
                resource_name="Features",
                resource_id=f"{catalog_id}:{collection_id}",
                operation="get items",
            )
            if isinstance(_exc, HTTPException):
                raise _exc
            return _exc

    async def get_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        request: Request,
        conn: AsyncConnection = Depends(get_async_connection),
    ):
        catalogs_svc = await self._get_catalogs_service()
        items_protocol = cast(ItemsProtocol, catalogs_svc)

        # Use ItemsProtocol to get the unified feature
        feature = await items_protocol.get_item(
            catalog_id, collection_id, item_id, ctx=DriverContext(db_resource=conn)
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
        return JSONResponse(
            content=ogc_feature.model_dump(exclude_none=True, by_alias=True),
        )

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
        catalog_id: str,
        collection_id: str,
        item_id: str,
        engine=Depends(get_async_engine),
    ):
        async with managed_transaction(engine) as conn:
            return await self._delete_item(catalog_id, collection_id, item_id, conn)
