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
from typing import Any, FrozenSet, List, Optional, cast

import pygeofilter as _pygeofilter_scope_gate  # noqa: F401  # SCOPE gate: extension_records requires pygeofilter
_ = _pygeofilter_scope_gate  # silence pyright "unused" — load-bearing for SCOPE filtering

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request, Response, status
from dynastore.extensions.tools.fast_api import AppJSONResponse as JSONResponse
from sqlalchemy.ext.asyncio import AsyncConnection

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.ogc_base import OGCServiceMixin, OGCTransactionMixin
from dynastore.extensions.tools.db import get_async_connection
from dynastore.extensions.tools.language_utils import get_language
from dynastore.extensions.tools.url import get_root_url
from dynastore.extensions.tools.query import (  # noqa: E402
    parse_hints_param,
    resolve_items_read_policy,
)
from dynastore.modules.storage.hints import Hint  # noqa: E402
from dynastore.models.protocols import ItemsProtocol
from dynastore.models.shared_models import Link
from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType

from . import records_generator as gen
from . import records_models as rm
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

    def get_notebooks(self):
        try:
            from .notebooks import build_contributions
        except Exception:
            return []
        return build_contributions()

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

        # Filter to RECORDS-kind collections via the CollectionInfo SSOT
        # (shared OGCServiceMixin helper; sequential awaits — see its docstring).
        from dynastore.modules.catalog.catalog_config import CollectionKind

        records_collections = []
        for coll in await self._filter_collections_by_kind(
            catalog_id, all_collections, CollectionKind.RECORDS
        ):
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
        filter: Optional[str] = Query(
            None,
            description=(
                "CQL2 filter expression; encoding controlled by ``filter-lang``."
            ),
        ),
        filter_lang: str = Query(
            "cql2-text",
            alias="filter-lang",
            description="Filter encoding: 'cql2-text' (default) or 'cql2-json'.",
        ),
        filter_crs: Optional[str] = Query(
            None,
            alias="filter-crs",
            description=(
                "URI of the CRS the geometric values in ``filter=`` are "
                "expressed in. Default = CRS84."
            ),
        ),
        properties: Optional[str] = Query(
            None,
            description=(
                "Comma-separated property names; unknown name → 400, empty "
                "value strips all attribute properties. Orthogonal to "
                "``skipGeometry``."
            ),
        ),
        skip_geometry: Optional[bool] = Query(
            None,
            alias="skipGeometry",
            description=(
                "When true, returned records carry ``geometry: null`` and "
                "the resolved driver omits the geometry from its projection. "
                "De-facto pygeoapi convention. Mutually exclusive with "
                "``returnGeometry`` unless both are consistent. Default: false."
            ),
        ),
        return_geometry: Optional[bool] = Query(
            None,
            alias="returnGeometry",
            description=(
                "ESRI de-facto alias for ``skipGeometry``. "
                "``returnGeometry=false`` is equivalent to ``skipGeometry=true``. "
                "Passing both with conflicting values returns HTTP 400."
            ),
        ),
        sortby: Optional[str] = Query(None, description="Sort order (e.g., '-title,+created')."),
        q: Optional[str] = Query(None, description="Free-text search query."),
        request_hints: FrozenSet = Depends(parse_hints_param),
    ) -> Response:
        catalogs_svc = await self._get_catalogs_service()

        collection_meta = await catalogs_svc.get_collection(catalog_id, collection_id, lang="en")
        if not collection_meta:
            raise HTTPException(status_code=404, detail=f"Collection '{collection_id}' not found.")

        from dynastore.extensions.tools.query import (
            parse_ogc_query_request,
            maybe_dispatch_items_to_search_driver,
            validate_filter_lang,
            resolve_geometry_flag_from_query,
            dispatch_or_stream_items,
        )
        from dynastore.tools.discovery import get_protocol

        # ``filter-lang`` validation (Phase 1 of #1385 — accept cql2-json).
        fl_normalised = validate_filter_lang(filter_lang)

        # ``filter-crs`` resolution. Records does not expose a CRSProtocol
        # dependency (no per-collection projected geometries), so resolution
        # is the lightweight regex form: ``EPSG:<n>`` / ``CRS84`` literals
        # only. Unknown URIs leave the SRID as ``None`` (= CRS84 default).
        filter_crs_srid: Optional[int] = None
        if isinstance(filter_crs, str) and filter_crs:
            up = filter_crs.upper()
            if "CRS84" in up:
                filter_crs_srid = 4326
            else:
                import re as _re
                m = _re.search(r"[/|:](\d+)$", filter_crs)
                if m:
                    filter_crs_srid = int(m.group(1))

        # ``properties`` validation. Reuses ``ItemsProtocol.get_collection_fields``
        # (same source as the Features queryables endpoint) so the
        # validator surface is identical across services.
        select_fields: Optional[List[str]] = None
        if isinstance(properties, str):
            requested = [p.strip() for p in properties.split(",") if p.strip()]
            if properties == "" or not requested:
                select_fields = []
            else:
                valid_props: set = set()
                items_svc = get_protocol(ItemsProtocol)
                if items_svc is not None:
                    try:
                        all_fields = await items_svc.get_collection_fields(
                            catalog_id, collection_id
                        )
                        for fd in all_fields.values():
                            if not getattr(fd, "expose", True):
                                continue
                            final_name = getattr(fd, "alias", None) or getattr(fd, "name", None)
                            if final_name and final_name not in ("geoid", "geom"):
                                valid_props.add(final_name)
                    except Exception:
                        valid_props = set()
                if valid_props:
                    unknown = [p for p in requested if p not in valid_props]
                    if unknown:
                        raise HTTPException(
                            status_code=400,
                            detail=(
                                f"Unknown properties: {', '.join(sorted(unknown))}. "
                                f"Available: {', '.join(sorted(valid_props))}."
                            ),
                        )
                select_fields = requested

        # ── Routing-aware items SEARCH-driver dispatch (#1047) ────────────
        # Mirror STAC ``/search``: for a structural-only listing (no CQL2
        # ``filter`` and no free-text ``q``) resolve the items SEARCH driver
        # via routing and dispatch through its streaming ``read_entities`` +
        # ``count_entities`` contract — public ES, the tenant-private ES index,
        # or any future ES items driver. The helper returns ``None``
        # (→ the PG ``stream_items`` path below) for a CQL filter, a
        # read-primary (PG ``QUERY_FALLBACK_SOURCE``) driver, or a non-ES items
        # driver. Free-text ``q`` folds into a CQL ``ILIKE`` predicate, so it
        # defers to the PG path too.
        #
        # ``?hints=geometry_exact`` opt-in: when the caller requests the
        # geometry_exact hint the simplified-geometry ES fast-path is skipped;
        # the stream_items call below then carries the caller's hints to force
        # the exact-capable driver.  No such hint keeps the default behaviour
        # byte-for-byte.
        wants_exact = Hint.GEOMETRY_EXACT in request_hints
        has_complex_filter = bool(filter) or bool(q)
        search_dispatch = None
        if not has_complex_filter and not wants_exact:
            search_dispatch = await maybe_dispatch_items_to_search_driver(
                catalog_id=catalog_id,
                collection_id=collection_id,
                limit=limit,
                offset=offset,
                has_complex_filter=False,
                request=request,
            )

        # Resolve skipGeometry/returnGeometry from the two accepted forms.
        skip_geom_bool = resolve_geometry_flag_from_query(skip_geometry, return_geometry)

        request_obj = parse_ogc_query_request(
            bbox=None,
            datetime_param=None,
            sortby=sortby,
            filter=filter,
            limit=limit,
            offset=offset,
            include_total_count=True,
            filter_lang=fl_normalised,
            filter_crs_srid=filter_crs_srid,
            select_fields=select_fields,
            skip_geometry=skip_geom_bool,
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
        query_response = await dispatch_or_stream_items(
            items_protocol,
            catalog_id=catalog_id,
            collection_id=collection_id,
            query_request=request_obj,
            consumer=ConsumerType.OGC_RECORDS,
            search_dispatch=search_dispatch,
            ctx=DriverContext(db_resource=conn) if conn is not None else None,
            request=request,
            hints=request_hints,
        )

        count = query_response.total_count or 0
        root_url = get_root_url(request)

        layer_config = await catalogs_svc.get_collection_config(
            catalog_id, collection_id, ctx=DriverContext(db_resource=conn),
        )
        read_policy = await resolve_items_read_policy(catalog_id, collection_id)

        # Per-feature post-fetch projection — covers drivers that ignore
        # ``QueryRequest.select`` (e.g. ES) and the empty-properties case.
        projection_set: Optional[set] = None
        if select_fields is not None:
            projection_set = set(select_fields)

        records: List[rm.Record] = []
        async for feature in query_response:
            if projection_set is not None:
                props = getattr(feature, "properties", None)
                if isinstance(props, dict):
                    for key in list(props.keys()):
                        if key not in projection_set:
                            props.pop(key, None)
            # ``Record`` already emits ``geometry: null`` by construction
            # (records_generator builds ``rm.Record(geometry=None, ...)``),
            # so ``skip_geometry`` is mainly a hint to the driver layer here
            # (PG drops the geom SELECT, ES adds ``geometry`` to
            # ``_source.excludes``). The Feature-level normalisation below
            # keeps the contract honest if the generator ever evolves to
            # carry a geometry.
            if skip_geom_bool and hasattr(feature, "geometry"):
                try:
                    feature.geometry = None
                except Exception:
                    pass
            records.append(gen.db_row_to_record(feature, catalog_id, collection_id, root_url, layer_config, read_policy=read_policy))

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

        # PG row-level ABAC: compile access_filter for user-facing single-item
        # reads when the collection carries an access_envelope sidecar.
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

        feature = await items_protocol.get_item(
            catalog_id, collection_id, record_id,
            ctx=DriverContext(db_resource=conn),
            access_filter=af,
        )
        if not feature:
            raise HTTPException(status_code=404, detail=f"Record '{record_id}' not found.")

        layer_config = await catalogs_svc.get_collection_config(
            catalog_id, collection_id, ctx=DriverContext(db_resource=conn),
        )
        read_policy = await resolve_items_read_policy(catalog_id, collection_id)
        root_url = get_root_url(request)
        record = gen.db_row_to_record(feature, catalog_id, collection_id, root_url, layer_config, read_policy=read_policy)
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
        read_policy = await resolve_items_read_policy(catalog_id, collection_id)

        if was_single:
            record = gen.db_row_to_record(
                accepted_rows[0], catalog_id, collection_id, root_url, layer_config,
                read_policy=read_policy,
            )
            return JSONResponse(
                content=record.model_dump(exclude_none=True),
                status_code=status.HTTP_201_CREATED,
            )

        records = [
            gen.db_row_to_record(feat, catalog_id, collection_id, root_url, layer_config, read_policy=read_policy)
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
        """Check if a collection is RECORDS-kind via its ``CollectionInfo``.

        Delegates to :meth:`OGCServiceMixin._collection_kind`, the single
        source of truth since Phase 1.6 moved ``collection_type`` off the PG
        driver config onto the ``CollectionInfo`` plugin config.  Reading the
        kind off the driver config (the previous approach) silently returned
        the ``VECTOR`` default, hiding every RECORDS collection.
        """
        from dynastore.modules.catalog.catalog_config import CollectionKind

        coll_id = collection.id if hasattr(collection, "id") else collection.get("id")
        if not coll_id:
            return False
        return await self._collection_kind(catalog_id, coll_id) == CollectionKind.RECORDS
