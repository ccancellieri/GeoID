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

"""OGC API - Records Part 1 extension for DynaStore.

Serves RECORDS-type collections as OGC API - Records catalogues.
Records are stored as ``Feature(geometry=None, properties={...})`` using
the standard sidecar pipeline (AttributesSidecar).  The GeometrySidecar
is skipped for RECORDS collections.

Delegates to ``CatalogsProtocol`` / ``ItemsProtocol`` for all CRUD —
no new storage layer is introduced.
"""

import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Union, cast

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request, Response, status
from dynastore.extensions.tools.fast_api import AppJSONResponse as JSONResponse
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.ogc_base import OGCServiceMixin, OGCTransactionMixin
from dynastore.extensions.tools.db import get_async_connection
from dynastore.extensions.tools.language_utils import get_language
from dynastore.extensions.tools.url import get_root_url
from dynastore.models.protocols import CatalogsProtocol, ItemsProtocol
from dynastore.models.shared_models import Link
from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType

from . import records_generator as gen
from . import records_models as rm
from .policies import records_policies, records_role_bindings
from dynastore.models.driver_context import DriverContext

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# OGC API - Records conformance URIs (OGC 20-004)
# ---------------------------------------------------------------------------

OGC_API_RECORDS_URIS = [
    "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/record-core",
    "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/record-collection",
    "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/json",
    "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/geojson",
    "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/sorting",
]


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class RecordsService(ExtensionProtocol, OGCServiceMixin, OGCTransactionMixin):
    """OGC API - Records Part 1 extension.

    Priority 150 — after STAC (100) and Features (100), before
    Dimensions (200).
    """

    priority: int = 150
    router: APIRouter

    # OGCServiceMixin class attributes
    conformance_uris = OGC_API_RECORDS_URIS
    prefix = "/records"
    protocol_title = "DynaStore OGC API - Records"
    protocol_description = "Access to catalog records via OGC API - Records"

    def __init__(self, app: Optional[FastAPI] = None):
        super().__init__()
        self.app = app
        self.router = APIRouter(prefix="/records", tags=["OGC API - Records"])
        self._register_routes()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        # Policies declared via PolicyContributor; IAM forwards centrally.
        yield

    def get_policies(self):
        return records_policies()

    def get_role_bindings(self):
        return records_role_bindings()

    # ------------------------------------------------------------------
    # Route registration
    # ------------------------------------------------------------------

    def _register_routes(self) -> None:
        # Landing page & conformance
        self.router.add_api_route(
            "/",
            self.get_landing_page,
            methods=["GET"],
            response_model=rm.LandingPage,
        )
        self.router.add_api_route(
            "/conformance",
            self.get_conformance,
            methods=["GET"],
            response_model=rm.Conformance,
        )

        # Collections (RECORDS-type only)
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections",
            self.list_collections,
            methods=["GET"],
            response_model=rm.RecordsCatalogCollections,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self.get_collection,
            methods=["GET"],
            response_model=rm.RecordsCatalogCollection,
        )

        # Record items
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/items",
            self.get_records,
            methods=["GET"],
            response_model=rm.RecordCollection,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/items",
            self.add_records,
            methods=["POST"],
            status_code=status.HTTP_201_CREATED,
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/items/{record_id}",
            self.get_record,
            methods=["GET"],
            response_model=rm.Record,
        )

    # ------------------------------------------------------------------
    # Landing page & conformance (delegated to OGCServiceMixin)
    # ------------------------------------------------------------------

    async def get_landing_page(self, request: Request) -> rm.LandingPage:
        return await self.ogc_landing_page_handler(request)

    async def get_conformance(self, request: Request) -> rm.Conformance:
        return await self.ogc_conformance_handler(request)

    # ------------------------------------------------------------------
    # Collections (filtered to RECORDS type)
    # ------------------------------------------------------------------

    async def list_collections(
        self,
        catalog_id: str,
        request: Request,
        language: str = Depends(get_language),
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ) -> rm.RecordsCatalogCollections:
        catalogs_svc = await self._get_catalogs_service()

        all_collections = await catalogs_svc.list_collections(
            catalog_id, lang=language, limit=limit, offset=offset,
        )
        if all_collections is None:
            raise HTTPException(status_code=404, detail=f"Catalog '{catalog_id}' not found.")

        root_url = get_root_url(request)

        # Filter to RECORDS-type collections
        records_collections = []
        for coll in all_collections:
            if await self._is_records_collection(catalog_id, coll):
                localized, _ = coll.localize(language) if hasattr(coll, "localize") else (coll, language)
                records_collections.append(
                    gen.collection_to_records_collection(localized, catalog_id, root_url)
                )

        links = [
            Link(href=str(request.url), rel="self", type="application/json"),
        ]
        return rm.RecordsCatalogCollections(
            collections=records_collections,
            links=links,
            numberMatched=len(records_collections),
            numberReturned=len(records_collections),
        )

    async def get_collection(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        language: str = Depends(get_language),
    ) -> rm.RecordsCatalogCollection:
        catalogs_svc = await self._get_catalogs_service()

        coll = await catalogs_svc.get_collection(catalog_id, collection_id, lang=language)
        if not coll:
            raise HTTPException(status_code=404, detail=f"Collection '{collection_id}' not found.")

        if not await self._is_records_collection(catalog_id, coll):
            raise HTTPException(status_code=404, detail=f"Collection '{collection_id}' is not a records collection.")

        root_url = get_root_url(request)
        localized, _ = coll.localize(language) if hasattr(coll, "localize") else (coll, language)
        return gen.collection_to_records_collection(localized, catalog_id, root_url)

    # ------------------------------------------------------------------
    # Record items
    # ------------------------------------------------------------------

    async def get_records(
        self,
        request: Request,
        catalog_id: str,
        collection_id: str,
        conn: AsyncConnection = Depends(get_async_connection),
        limit: int = Query(10, ge=1, le=1000, description="Maximum number of records to return."),
        offset: int = Query(0, ge=0, description="Offset of the first record to return."),
        filter: Optional[str] = Query(None, description="CQL2-TEXT filter expression."),
        sortby: Optional[str] = Query(None, description="Sort order (e.g., '-title,+created')."),
        q: Optional[str] = Query(None, description="Free-text search query."),
    ) -> Response:
        catalogs_svc = await self._get_catalogs_service()

        collection_meta = await catalogs_svc.get_collection(catalog_id, collection_id, lang="en")
        if not collection_meta:
            raise HTTPException(status_code=404, detail=f"Collection '{collection_id}' not found.")

        from dynastore.extensions.features.features_service import parse_ogc_query_request

        request_obj = parse_ogc_query_request(
            bbox=None,
            datetime_param=None,
            sortby=sortby,
            filter=filter,
            limit=limit,
            offset=offset,
            include_total_count=True,
        )

        # Free-text search — add as CQL2 LIKE filter on title if present
        if q:
            # Escape SQL special characters to prevent injection
            safe_q = q.replace("'", "''").replace("%", r"\%").replace("_", r"\_")
            ilike_expr = f"title ILIKE '%{safe_q}%' ESCAPE '\\'"
            if request_obj.cql_filter:
                request_obj.cql_filter = f"({request_obj.cql_filter}) AND {ilike_expr}"
            else:
                request_obj.cql_filter = ilike_expr

        items_protocol = cast(ItemsProtocol, catalogs_svc)
        try:
            from dynastore.models.driver_context import DriverContext
            query_response = await items_protocol.stream_items(
                catalog_id=catalog_id,
                collection_id=collection_id,
                request=request_obj,
                ctx=DriverContext(db_resource=conn) if conn is not None else None,
                consumer=ConsumerType.OGC_RECORDS,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        count = query_response.total_count or 0
        root_url = get_root_url(request)

        layer_config = await catalogs_svc.get_collection_config(
            catalog_id, collection_id, ctx=DriverContext(db_resource=conn),
        )

        records: List[rm.Record] = []
        async for feature in query_response:
            records.append(gen.db_row_to_record(feature, catalog_id, collection_id, root_url, layer_config))

        # Pagination links
        from dynastore.extensions.tools.pagination import build_pagination_links
        links = build_pagination_links(request, offset, limit, count)

        result = rm.RecordCollection(
            type="FeatureCollection",
            features=records,
            links=links,
            numberMatched=count,
            numberReturned=len(records),
        )
        return JSONResponse(
            content=result.model_dump(exclude_none=True),
            media_type="application/geo+json",
        )

    async def get_record(
        self,
        catalog_id: str,
        collection_id: str,
        record_id: str,
        request: Request,
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> rm.Record:
        catalogs_svc = await self._get_catalogs_service()
        items_protocol = cast(ItemsProtocol, catalogs_svc)

        feature = await items_protocol.get_item(
            catalog_id, collection_id, record_id, ctx=DriverContext(db_resource=conn),
        )
        if not feature:
            raise HTTPException(status_code=404, detail=f"Record '{record_id}' not found.")

        layer_config = await catalogs_svc.get_collection_config(
            catalog_id, collection_id, ctx=DriverContext(db_resource=conn),
        )
        root_url = get_root_url(request)
        record = gen.db_row_to_record(feature, catalog_id, collection_id, root_url, layer_config)
        return JSONResponse(
            content=record.model_dump(exclude_none=True),
        )

    async def add_records(
        self,
        catalog_id: str,
        collection_id: str,
        request: Request,
        conn: AsyncConnection = Depends(get_async_connection),
    ) -> Response:
        """Create/upsert records into a RECORDS-type collection."""
        body = await request.json()

        # Normalise to list and determine whether caller sent a single item.
        if isinstance(body, list):
            was_single = False
            items: list = body
        elif isinstance(body, dict) and body.get("type") == "FeatureCollection":
            was_single = False
            items = body.get("features", [])
        elif isinstance(body, dict):
            was_single = True
            items = [body]
        else:
            raise HTTPException(status_code=400, detail="Invalid request body.")

        # Ensure geometry is null for records
        for item in items:
            if isinstance(item, dict):
                item["geometry"] = None

        from dynastore.modules.storage.driver_config import ItemsWritePolicy
        policy_source = (
            f"/configs/catalogs/{catalog_id}/collections/{collection_id}"
            f"/plugins/{ItemsWritePolicy.class_key()}"
        )
        # Pass the normalised list so the mixin works with the same payload
        # shape regardless of original body format.
        accepted_rows, rejections, _was_single_mixin, batch_size = (
            await self._ingest_items(
                catalog_id,
                collection_id,
                items,
                DriverContext(db_resource=conn),
                policy_source,
            )
        )

        if rejections:
            return self._build_rejection_response(accepted_rows, rejections, batch_size)

        catalogs_svc = await self._get_catalogs_service()
        root_url = get_root_url(request)
        layer_config = await catalogs_svc.get_collection_config(
            catalog_id, collection_id, ctx=DriverContext(db_resource=conn),
        )

        if was_single:
            record = gen.db_row_to_record(
                accepted_rows[0], catalog_id, collection_id, root_url, layer_config
            )
            return JSONResponse(
                content=record.model_dump(exclude_none=True),
                status_code=status.HTTP_201_CREATED,
            )

        records = [
            gen.db_row_to_record(feat, catalog_id, collection_id, root_url, layer_config)
            for feat in accepted_rows
        ]
        collection = rm.RecordCollection(
            type="FeatureCollection",
            features=records,
            numberReturned=len(records),
        )
        return JSONResponse(
            content=collection.model_dump(exclude_none=True),
            status_code=status.HTTP_201_CREATED,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _is_records_collection(
        self, catalog_id: str, collection: Any
    ) -> bool:
        """Check if a collection is of RECORDS type via its driver config."""
        from dynastore.modules.storage.router import get_driver
        from dynastore.modules.storage.routing_config import Operation

        coll_id = collection.id if hasattr(collection, "id") else collection.get("id")
        if not coll_id:
            return False
        try:
            driver = await get_driver(Operation.READ, catalog_id, coll_id)
            config = await driver.get_driver_config(catalog_id, coll_id)
            return getattr(config, "collection_type", "VECTOR") == "RECORDS"
        except Exception:
            return False
