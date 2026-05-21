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

import json
import logging
from typing import Dict, Any, Optional, List, Union, cast
from dynastore.modules import get_protocol
from dynastore.tools.discovery import get_protocols
from fastapi import (
    FastAPI,
    APIRouter,
    HTTPException,
    status,
    Body,
    Path,
    Query,
    Request,
)
from pydantic import BaseModel, Field, ConfigDict

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.extensions.ogc_base import OGCServiceMixin, OGCTransactionMixin
from dynastore.extensions.ogc_models_shared import (
    BulkCreationResponse,
    IngestionReport,
    SidecarRejection,
)
from dynastore.extensions.assets.conformance import ASSETS_CONFORMANCE_URIS
from dynastore.extensions.tools.fast_api import AppJSONResponse
from dynastore.models.protocols import (
    AssetsProtocol,
    AssetUploadProtocol,
    CatalogsProtocol,
    ConfigsProtocol,
    UploadTicket,
    UploadStatusResponse,
)
from dynastore.models.shared_models import Link
from dynastore.models.protocols.asset_download import AssetDownloadProtocol
from dynastore.tools.protocol_helpers import get_engine
from dynastore.extensions.tools.catalog_readiness import require_catalog_ready
from dynastore.extensions.tools.exception_handlers import handle_exception
from dynastore.models.query_builder import AssetFilter
from dynastore.modules.catalog.asset_service import (
    Asset,
    AssetCreate,
    AssetKind,
    AssetStatus,
    AssetUpdate,
    AssetUploadDefinition,
    AssetReference,
    AssetReferencedError,
    VirtualAssetCreate,
)
from dynastore.modules.catalog.asset_distributed import (
    AssetSidecarRejectedError,
    Scope,
    UpsertResult,
    upsert_asset,
)
from dynastore.modules.catalog.write_policy_assets import AssetsWritePolicy
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)



class SearchQuery(BaseModel):
    """Payload for advanced asset searching.

    Scope (catalog vs. collection) is taken from the request **path**, not
    the body — symmetric with every other asset operation. Use the
    catalog-scoped endpoint for catalog-tier assets, the collection-scoped
    endpoint for a collection, and the global endpoint for cross-catalog
    search. To filter on the ``collection_id`` column itself, pass an
    ``AssetFilter(field="collection_id", value=...)``.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    filters: List[AssetFilter] = Field(
        default_factory=list, description="List of granular filters to apply."
    )
    limit: int = Field(10, ge=1, le=100)
    offset: int = Field(0, ge=0)
class UploadRequest(BaseModel):
    """
    Request body for initiating a backend-agnostic asset upload session.

    After submitting this request the client receives an ``UploadTicket``
    containing a backend-specific URL and HTTP method to deliver the file.
    For GCS/S3 backends the client PUTs the file directly to the signed URL;
    for local/HTTP backends the client POSTs to the server-side proxy path.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    filename: str = Field(
        ...,
        description=(
            "Original filename including extension. "
            "Used to derive the storage path and content-type hints."
        ),
        examples=["LC09_L1TP_198030_20251225_02_T1.tif"],
    )
    content_type: Optional[str] = Field(
        None,
        description="MIME type of the file being uploaded.",
        examples=["image/tiff", "application/geo+json", "text/csv"],
    )
    asset: AssetUploadDefinition = Field(
        ...,
        description=(
            "Asset metadata for the file being uploaded. "
            "The URI is not yet known — the backend sets it after receiving the file."
        ),
    )


class AssetService(ExtensionProtocol, OGCServiceMixin, OGCTransactionMixin):
    priority: int = 100
    """
    Asset Service Extension — OGC API - Assets candidate.

    Exposes API endpoints for managing assets across catalogs and collections.
    Inherits :class:`OGCServiceMixin` for landing-page / conformance handlers
    and :class:`OGCTransactionMixin` for bulk-creation response builders so
    rejections surface as 207 ``IngestionReport`` bodies symmetric with the
    Features / Records / STAC OGC services.
    """

    # OGCServiceMixin class attributes
    conformance_uris = list(ASSETS_CONFORMANCE_URIS)
    prefix = "/assets"
    protocol_title = "DynaStore OGC API - Assets (draft)"
    protocol_description = (
        "Asset lifecycle management — upload sessions, write policies, "
        "versioning, virtual assets, and reference-cascade safety. "
        "Conformance URIs are draft proposals to the OGC SWG."
    )

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        yield

    def get_notebooks(self):
        try:
            from .notebooks import build_contributions
        except Exception:
            return []
        return build_contributions()

    def __init__(self, app: FastAPI):
        self.app = app
        self.router = APIRouter(prefix="/assets", tags=["Assets"])
        self._setup_routes()
        # Surface the asset write-policy PluginConfigs to the Configs API.
        # Importing the module triggers ``TypedModelRegistry.register(cls)``
        # in :class:`PluginConfig.__init_subclass__` — no explicit register
        # call needed; the import side-effect is the registration.
        from dynastore.modules.catalog import write_policy_assets as _wpa  # noqa: F401
        _ = _wpa.AssetsWritePolicy
        _ = _wpa.AssetWritePolicyDefaults

    def _setup_routes(self):
        # ---------------------------------------------------------------
        # OGC API - Assets landing page + conformance (registered BEFORE
        # parametric ``/catalogs/{catalog_id}`` routes so they don't get
        # shadowed by FastAPI's declaration-order match).
        # ---------------------------------------------------------------
        self.router.add_api_route(
            "/",
            self.ogc_landing_page_handler,
            methods=["GET"],
            summary="Landing Page (OGC API - Assets)",
        )
        self.router.add_api_route(
            "/conformance",
            self.ogc_conformance_handler,
            methods=["GET"],
            summary="Conformance Declaration (OGC API - Assets)",
        )
        # ---------------------------------------------------------------
        # Bulk POST routes (207 IngestionReport on partial rejection).
        # The trailing ``:bulk`` action segment cannot collide with the
        # ``{asset_id}`` parametric routes registered later because FastAPI
        # matches the literal ``assets:bulk`` path component first.
        # ---------------------------------------------------------------
        self.router.add_api_route(
            "/catalogs/{catalog_id}/assets:bulk",
            self.bulk_create_catalog_assets,
            methods=["POST"],
            summary="Bulk Create Catalog Assets",
            description=(
                "Atomic bulk asset creation for a catalog. Each payload is "
                "dispatched through the per-collection AssetsWritePolicy "
                "chain. Returns 201 ``BulkCreationResponse`` when every "
                "asset is accepted, or 207 ``IngestionReport`` when one or "
                "more assets are refused by the write-policy."
            ),
            responses={
                201: {"description": "All assets accepted."},
                207: {"description": "Partial or full rejection — see IngestionReport."},
            },
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/assets:bulk",
            self.bulk_create_collection_assets,
            methods=["POST"],
            summary="Bulk Create Collection Assets",
            description=(
                "Atomic bulk asset creation scoped to a collection. Same "
                "semantics as the catalog-level endpoint; the AssetsWritePolicy "
                "is resolved at the collection scope."
            ),
            responses={
                201: {"description": "All assets accepted."},
                207: {"description": "Partial or full rejection — see IngestionReport."},
            },
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}",
            self.list_catalog_assets,
            methods=["GET"],
            summary="List Catalog Assets",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}",
            self.create_catalog_asset,
            methods=["POST"],
            response_model=Asset,
            status_code=status.HTTP_201_CREATED,
            summary="Create Catalog Asset",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}",
            self.delete_catalog_assets,
            methods=["DELETE"],
            status_code=status.HTTP_204_NO_CONTENT,
            summary="Delete All Catalog Assets",
        )
        # Virtual asset register routes (catalog + collection scoped).
        # External-href registrations land here so they don't collide with the
        # physical-asset upload-create surface.
        self.router.add_api_route(
            "/catalogs/{catalog_id}/virtual-assets",
            self.create_catalog_virtual_asset,
            methods=["POST"],
            response_model=Asset,
            status_code=status.HTTP_201_CREATED,
            summary="Register Virtual Catalog Asset",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/virtual-assets",
            self.create_collection_virtual_asset,
            methods=["POST"],
            response_model=Asset,
            status_code=status.HTTP_201_CREATED,
            summary="Register Virtual Collection Asset",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self.list_collection_assets,
            methods=["GET"],
            summary="List Collection Assets",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self.create_collection_asset,
            methods=["POST"],
            response_model=Asset,
            status_code=status.HTTP_201_CREATED,
            summary="Create Collection Asset",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/assets/{asset_id}",
            self.get_catalog_asset,
            methods=["GET"],
            response_model=Asset,
            summary="Get Asset",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/assets/{asset_id}",
            self.update_catalog_asset,
            methods=["PUT"],
            response_model=Asset,
            summary="Replace Asset (full)",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/assets/{asset_id}",
            self.update_catalog_asset,
            methods=["PATCH"],
            response_model=Asset,
            summary="Update Asset (partial)",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/assets/{asset_id}",
            self.delete_catalog_asset_by_id,
            methods=["DELETE"],
            status_code=status.HTTP_204_NO_CONTENT,
            summary="Delete Asset",
            description=(
                "Deletes an asset by ID. "
                "Soft-delete (default) sets `status='deleted'`; hard-delete (`force=true`) removes "
                "the row entirely. Hard-deletion is **blocked** (HTTP 409) when the asset is "
                "owned by a storage backend (`owned_by` is set) and one or more non-cascading "
                "references remain. Use `GET .../references` to diagnose blocking references."
            ),
            responses={
                204: {"description": "Asset deleted."},
                404: {"description": "Asset not found."},
                409: {
                    "description": "Asset has blocking references. Hard-deletion is not allowed until they are removed.",
                    "content": {
                        "application/json": {
                            "example": {
                                "detail": {
                                    "message": "Asset 'my_asset' cannot be hard-deleted: 1 blocking reference(s) remain.",
                                    "asset_id": "my_asset",
                                    "blocking_references": [
                                        {
                                            "ref_type": "duckdb:table",
                                            "ref_id": "my_collection_table",
                                            "cascade_delete": False,
                                            "created_at": "2025-03-25T10:00:00Z",
                                        }
                                    ],
                                }
                            }
                        }
                    },
                },
            },
        )
        # Collection-level single asset routes
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_id}",
            self.get_collection_asset,
            methods=["GET"],
            response_model=Asset,
            summary="Get Collection Asset",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_id}",
            self.update_collection_asset,
            methods=["PUT"],
            response_model=Asset,
            summary="Replace Collection Asset (full)",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_id}",
            self.update_collection_asset,
            methods=["PATCH"],
            response_model=Asset,
            summary="Update Collection Asset (partial)",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_id}",
            self.delete_collection_asset_by_id,
            methods=["DELETE"],
            status_code=status.HTTP_204_NO_CONTENT,
            summary="Delete Collection Asset",
            description=(
                "Deletes a collection-scoped asset by ID. "
                "Hard-deletion (`force=true`) is blocked (HTTP 409) when blocking "
                "references remain. See `GET .../references` for details."
            ),
            responses={
                204: {"description": "Asset deleted."},
                404: {"description": "Asset not found."},
                409: {"description": "Asset has blocking references preventing hard-deletion."},
            },
        )
        # Collection delete all
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}",
            self.delete_collection_assets,
            methods=["DELETE"],
            status_code=status.HTTP_204_NO_CONTENT,
            summary="Delete All Collection Assets",
        )
        # Canonical asset search routes (path-scoped, routing-aware).
        # All three share one ``_run_scoped_search`` impl; the scope is
        # taken from the path. Search resolves the ``SEARCH`` asset driver
        # with a fallback to ``READ`` (see ``get_asset_search_driver``), so
        # an operator can route asset search to a dedicated index driver
        # (e.g. Elasticsearch) per catalog/collection without code changes.
        self.router.add_api_route(
            "/assets-search",
            self.advanced_search_global,
            methods=["POST"],
            summary="Advanced Asset Search (cross-catalog)",
            description=(
                "Cross-catalog asset search. Aggregates up to ``limit`` "
                "matches across the catalogs the caller can see. Resolves "
                "each catalog's routed SEARCH driver (READ fallback)."
            ),
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/assets-search",
            self.advanced_search,
            methods=["POST"],
            summary="Advanced Asset Search (scoped to catalog)",
            description=(
                "Search catalog-tier assets (those not bound to a "
                "collection). Resolves the catalog's routed SEARCH driver "
                "with a READ fallback."
            ),
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/assets-search",
            self.advanced_search_collection,
            methods=["POST"],
            summary="Advanced Asset Search (scoped to collection)",
            description=(
                "Search assets within a single collection. The "
                "``collection_id`` from the path is authoritative. Resolves "
                "the collection's routed SEARCH driver with a READ fallback."
            ),
        )
        # -----------------------------------------------------------------
        # Upload endpoints (backend-agnostic)
        # -----------------------------------------------------------------
        self.router.add_api_route(
            "/catalogs/{catalog_id}/upload",
            self.initiate_catalog_upload,
            methods=["POST"],
            response_model=UploadTicket,
            status_code=status.HTTP_200_OK,
            summary="Initiate Asset Upload (Catalog Level)",
            description=(
                "Starts a new upload session for a catalog-level asset. "
                "Returns an `UploadTicket` containing a backend-specific upload URL, "
                "the HTTP method to use (`PUT` for GCS/S3, `POST` for local), and any "
                "required headers. "
                "After delivering the file the backend registers the asset automatically "
                "(event-driven for GCS/S3, synchronous for local storage). "
                "Poll `/upload/{ticket_id}/status` to confirm registration."
            ),
            responses={
                200: {"description": "Upload session created. Use the returned ticket to upload the file."},
                503: {"description": "No upload backend available or storage not provisioned."},
                424: {"description": "Catalog storage provisioning failed."},
            },
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/upload",
            self.initiate_collection_upload,
            methods=["POST"],
            response_model=UploadTicket,
            status_code=status.HTTP_200_OK,
            summary="Initiate Asset Upload (Collection Level)",
            description=(
                "Starts a new upload session for an asset scoped to a collection. "
                "Identical to the catalog-level endpoint but the file is stored under "
                "the collection's path prefix. "
                "The `collection_id` from the path is authoritative over any value "
                "in the request body."
            ),
            responses={
                200: {"description": "Upload session created."},
                503: {"description": "No upload backend available or storage not provisioned."},
                424: {"description": "Catalog/collection storage provisioning failed."},
            },
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/upload/{ticket_id}/status",
            self.get_upload_status,
            methods=["GET"],
            response_model=UploadStatusResponse,
            status_code=status.HTTP_200_OK,
            summary="Get Upload Status",
            description=(
                "Polls the status of a previously initiated upload session.\n\n"
                "For **event-driven backends** (GCS, S3) the status transitions "
                "`pending → uploading → completed` asynchronously after the cloud event "
                "fires and the asset is registered.\n\n"
                "For **local/HTTP backends** the status is `completed` immediately "
                "after the server receives the file."
            ),
            responses={
                200: {"description": "Current upload status."},
                404: {"description": "Ticket not found or expired."},
            },
        )
        # -----------------------------------------------------------------
        # Asset reference endpoints
        # -----------------------------------------------------------------
        self.router.add_api_route(
            "/catalogs/{catalog_id}/assets/{asset_id}/references",
            self.list_asset_references,
            methods=["GET"],
            response_model=List[AssetReference],
            status_code=status.HTTP_200_OK,
            summary="List Asset References",
            description=(
                "Returns all active references to this asset from collections, "
                "DuckDB tables, Iceberg tables, or other drivers.\n\n"
                "References with `cascade_delete=false` will **block** hard-deletion "
                "of the asset (HTTP 409 Conflict). "
                "Use this endpoint to diagnose why a deletion was rejected."
            ),
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/assets/{asset_id}/references/{ref_type}/{ref_id}",
            self.remove_asset_reference,
            methods=["DELETE"],
            status_code=status.HTTP_204_NO_CONTENT,
            summary="Remove Asset Reference",
            description=(
                "Manually removes a reference to this asset.\n\n"
                "**Warning:** removing a blocking reference (`cascade_delete=false`) "
                "allows hard-deletion of the asset even if the referencing "
                "collection or table has **not** been dropped yet. "
                "Only call this after confirming the referencing entity has been cleaned up."
            ),
            responses={
                204: {"description": "Reference removed successfully."},
                404: {"description": "Asset or reference not found."},
            },
        )
        # -----------------------------------------------------------------
        # Asset download (backend-resolved 302 redirect; see
        # models/protocols/asset_download.py). Registered AFTER specific routes
        # so ``download`` does not shadow ``references`` / ``upload``.
        # -----------------------------------------------------------------
        self.router.add_api_route(
            "/catalogs/{catalog_id}/assets/{asset_id}/download",
            self.download_catalog_asset,
            methods=["GET"],
            status_code=status.HTTP_302_FOUND,
            summary="Download Asset",
            description=(
                "Redirects (302) to a backend-resolved download URL for the "
                "asset bytes: a short-lived GCS signed URL, the bearer-auth "
                "local-download route, or the asset's external URL. Optional "
                "``ttl`` (seconds) tunes signed-URL lifetime where supported."
            ),
            responses={
                302: {"description": "Redirect to the download URL."},
                404: {"description": "Asset not found or no download backend."},
            },
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_id}/download",
            self.download_collection_asset,
            methods=["GET"],
            status_code=status.HTTP_302_FOUND,
            summary="Download Collection Asset",
        )
        # ---------------------------------------------------------------
        # Bucket↔DB drift endpoints (Stage 6 reconcile surface).
        # The literal ``:drift`` action segment cannot collide with the
        # ``{asset_id}`` parametric routes. Read-only dry-run path; the
        # apply path is exposed via ``tasks/bucket-reconcile/execute``.
        # ---------------------------------------------------------------
        self.router.add_api_route(
            "/catalogs/{catalog_id}/assets:drift",
            self.get_catalog_assets_drift,
            methods=["GET"],
            summary="Inspect Bucket↔DB Drift (Catalog)",
            description=(
                "Read-only diff between bucket blobs and the assets table "
                "for the catalog. Returns a ``BucketReconcileReport`` with "
                "orphan_blob / ghost_row / stuck_pending counts plus a "
                "per-row ``drift_details`` list. Does not mutate state. "
                "Apply the diff via "
                "``POST /catalogs/{catalog_id}/tasks/bucket-reconcile/execute``."
            ),
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/assets:drift",
            self.get_collection_assets_drift,
            methods=["GET"],
            summary="Inspect Bucket↔DB Drift (Collection)",
            description=(
                "Same as the catalog-level drift endpoint, scoped to a "
                "single collection's prefix."
            ),
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/tasks/bucket-reconcile/execute",
            self.execute_catalog_bucket_reconcile,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            summary="Execute Bucket Reconcile (Catalog)",
            description=(
                "Apply-mode bucket↔DB reconciliation: imports orphan blobs "
                "as ACTIVE rows, marks ghost rows as failed (reason="
                "``missing_blob``), and fails stuck PENDING rows older "
                "than the TTL (reason=``upload_abandoned``). Executes "
                "synchronously and returns the ``BucketReconcileReport``."
            ),
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/tasks/bucket-reconcile/execute",
            self.execute_collection_bucket_reconcile,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            summary="Execute Bucket Reconcile (Collection)",
        )

    @property
    def assets(self) -> AssetsProtocol:
        svc = self.get_protocol(AssetsProtocol)
        if svc is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="AssetsProtocol implementation not available.",
            )
        return svc

    async def resolve_upload_driver(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
    ) -> Optional[AssetUploadProtocol]:
        """Resolve the upload backend for a given catalog/collection.

        Picks via ``AssetRoutingConfig.operations[UPLOAD]`` (auto-augmented
        with all discoverable ``AssetUploadProtocol`` impls) with first-match
        fallback when no UPLOAD entries resolve.
        """
        from dynastore.modules.storage.router import get_asset_upload_driver
        return await get_asset_upload_driver(catalog_id, collection_id)

    # =============================================================================
    #  CATALOG LEVEL OPERATIONS
    # =============================================================================


    async def list_catalog_assets(
        self,
        catalog_id: str = Path(..., description="The catalog ID"),
        limit: int = Query(10, ge=1, le=100),
        offset: int = Query(0, ge=0),
    ):
        """Returns a list of assets associated directly with the catalog (no collection)."""
        return await self.assets.list_assets(
            catalog_id=catalog_id, collection_id=None, limit=limit, offset=offset
        )


    async def create_catalog_asset(
        self,
        asset_in: AssetCreate, catalog_id: str = Path(..., description="The catalog ID")
    ):
        """Creates a new physical asset at the catalog level."""
        await require_catalog_ready(catalog_id)
        try:
            return await self.assets.create_asset(
                catalog_id=catalog_id, asset=asset_in, collection_id=None
            )
        except Exception as e:
            raise handle_exception(
                e,
                resource_name="Asset",
                resource_id=f"{catalog_id}:{asset_in.asset_id}",
                operation="Catalog asset creation",
            )

    async def create_catalog_virtual_asset(
        self,
        asset_in: VirtualAssetCreate, catalog_id: str = Path(..., description="The catalog ID")
    ):
        """Registers a virtual (external-href) asset at the catalog level."""
        await require_catalog_ready(catalog_id)
        try:
            return await self.assets.create_asset(
                catalog_id=catalog_id, asset=asset_in, collection_id=None
            )
        except Exception as e:
            raise handle_exception(
                e,
                resource_name="Asset",
                resource_id=f"{catalog_id}:{asset_in.asset_id}",
                operation="Catalog virtual-asset registration",
            )


    async def delete_catalog_assets(
        self,
        catalog_id: str = Path(..., description="The catalog ID"),
        force: bool = Query(
            False, description="True for Hard Delete, False for Soft Delete"
        ),
        propagate: bool = Query(
            False, description="Whether to propagate deletion to linked features"
        ),
    ):
        """Removes all assets belonging to the catalog."""
        await self.assets.delete_assets(
            catalog_id=catalog_id, hard=force, propagate=propagate
        )

    # =============================================================================
    #  CATALOG ASSET (SINGLE)
    # =============================================================================


    async def get_catalog_asset(
        self,
        request: Request,
        catalog_id: str = Path(..., description="The catalog ID"),
        asset_id: str = Path(..., description="The asset ID"),
    ):
        """Retrieves details for a specific asset in the catalog."""
        asset = await self.assets.get_asset(
            catalog_id=catalog_id, asset_id=asset_id, collection_id=None
        )
        if not asset:
            raise HTTPException(status_code=404, detail="Asset not found")
        asset.links = self._asset_links_for(asset, request)
        return asset


    async def update_catalog_asset(
        self,
        asset_in: AssetUpdate,
        catalog_id: str = Path(..., description="The catalog ID"),
        asset_id: str = Path(..., description="The asset ID"),
    ):
        """Updates the metadata of an existing catalog asset. All other fields are immutable."""
        await require_catalog_ready(catalog_id)
        try:
            return await self.assets.update_asset(
                catalog_id=catalog_id, asset_id=asset_id, update=asset_in
            )
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))


    async def delete_catalog_asset_by_id(
        self,
        catalog_id: str = Path(..., description="The catalog ID"),
        asset_id: str = Path(..., description="The asset ID"),
        force: bool = Query(
            False, description="True for Hard Delete, False for Soft Delete"
        ),
        propagate: bool = Query(
            False, description="Whether to propagate deletion to linked features"
        ),
    ):
        """Deletes a specific asset by ID."""
        try:
            success = await self.assets.delete_assets(
                catalog_id=catalog_id, asset_id=asset_id, hard=force, propagate=propagate
            )
        except AssetReferencedError as e:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message": str(e),
                    "asset_id": e.asset_id,
                    "blocking_references": [
                        r.model_dump(mode="json") for r in e.blocking_refs
                    ],
                },
            )
        if success == 0:
            raise HTTPException(status_code=404, detail="Asset not found")

    # =============================================================================
    #  COLLECTION LEVEL OPERATIONS
    # =============================================================================


    async def list_collection_assets(
        self,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        limit: int = Query(10, ge=1, le=100),
        offset: int = Query(0, ge=0),
    ):
        """Returns a list of assets associated with a specific collection."""
        return await self.assets.list_assets(
            catalog_id=catalog_id,
            collection_id=collection_id,
            limit=limit,
            offset=offset,
        )


    async def create_collection_asset(
        self,
        asset_in: AssetCreate,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
    ):
        """Creates a new physical asset associated with a specific collection."""
        await require_catalog_ready(catalog_id)
        # The body carries asset identity (asset_id, filename); collection
        # scope comes from the path. AssetCreate has no collection_id field
        # so there is no conflict to reconcile.
        try:
            return await self.assets.create_asset(
                catalog_id=catalog_id, asset=asset_in, collection_id=collection_id
            )
        except Exception as e:
            raise handle_exception(
                e,
                resource_name="Asset",
                resource_id=f"{catalog_id}:{collection_id}:{asset_in.asset_id}",
                operation="Collection asset creation",
            )

    async def create_collection_virtual_asset(
        self,
        asset_in: VirtualAssetCreate,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
    ):
        """Registers a virtual (external-href) asset under a specific collection."""
        await require_catalog_ready(catalog_id)
        try:
            return await self.assets.create_asset(
                catalog_id=catalog_id, asset=asset_in, collection_id=collection_id
            )
        except Exception as e:
            raise handle_exception(
                e,
                resource_name="Asset",
                resource_id=f"{catalog_id}:{collection_id}:{asset_in.asset_id}",
                operation="Collection virtual-asset registration",
            )


    async def delete_collection_assets(
        self,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        force: bool = Query(
            False, description="True for Hard Delete, False for Soft Delete"
        ),
        propagate: bool = Query(
            False, description="Whether to propagate deletion to linked features"
        ),
    ):
        """Removes all assets belonging to the specified collection."""
        await self.assets.delete_assets(
            catalog_id=catalog_id,
            collection_id=collection_id,
            hard=force,
            propagate=propagate,
        )

    # =============================================================================
    #  COLLECTION ASSET (SINGLE)
    # =============================================================================


    async def get_collection_asset(
        self,
        request: Request,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        asset_id: str = Path(..., description="The asset ID"),
    ):
        """Retrieves details for a specific asset in a collection."""
        asset = await self.assets.get_asset(
            catalog_id=catalog_id, asset_id=asset_id, collection_id=collection_id
        )
        if not asset:
            raise HTTPException(
                status_code=404, detail="Asset not found in this collection"
            )
        asset.links = self._asset_links_for(asset, request)
        return asset


    async def update_collection_asset(
        self,
        asset_in: AssetUpdate,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        asset_id: str = Path(..., description="The asset ID"),
    ):
        """Updates the metadata of an existing collection asset. All other fields are immutable."""
        await require_catalog_ready(catalog_id)
        try:
            return await self.assets.update_asset(
                catalog_id=catalog_id,
                asset_id=asset_id,
                update=asset_in,
                collection_id=collection_id,
            )
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))


    async def delete_collection_asset_by_id(
        self,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        asset_id: str = Path(..., description="The asset ID"),
        force: bool = Query(
            False, description="True for Hard Delete, False for Soft Delete"
        ),
        propagate: bool = Query(
            False, description="Whether to propagate deletion to linked features"
        ),
    ):
        """Deletes a specific asset by ID."""
        try:
            success = await self.assets.delete_assets(
                catalog_id=catalog_id,
                asset_id=asset_id,
                collection_id=collection_id,
                hard=force,
                propagate=propagate,
            )
        except AssetReferencedError as e:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message": str(e),
                    "asset_id": e.asset_id,
                    "blocking_references": [
                        r.model_dump(mode="json") for r in e.blocking_refs
                    ],
                },
            )
        if success == 0:
            raise HTTPException(status_code=404, detail="Asset not found")

    # =============================================================================
    #  OGC API - Assets: BULK CREATE + HATEOAS
    # =============================================================================

    async def bulk_create_catalog_assets(
        self,
        request: Request,
        catalog_id: str = Path(..., description="The catalog ID"),
        payloads: List[Union[AssetCreate, VirtualAssetCreate]] = Body(
            ...,
            description=(
                "List of asset create payloads. Each item must be either an "
                "``AssetCreate`` (kind=physical) or ``VirtualAssetCreate`` "
                "(kind=virtual). The discriminator is the ``kind`` field."
            ),
            min_length=1,
        ),
    ):
        """Bulk-create catalog-tier assets through the AssetsWritePolicy chain."""
        return await self._bulk_create_assets(
            request=request,
            catalog_id=catalog_id,
            collection_id=None,
            payloads=payloads,
        )

    async def bulk_create_collection_assets(
        self,
        request: Request,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        payloads: List[Union[AssetCreate, VirtualAssetCreate]] = Body(
            ...,
            description=(
                "List of asset create payloads scoped to this collection. "
                "Each item must be ``AssetCreate`` or ``VirtualAssetCreate``."
            ),
            min_length=1,
        ),
    ):
        """Bulk-create collection-scoped assets through the AssetsWritePolicy chain."""
        return await self._bulk_create_assets(
            request=request,
            catalog_id=catalog_id,
            collection_id=collection_id,
            payloads=payloads,
        )

    async def _bulk_create_assets(
        self,
        *,
        request: Request,
        catalog_id: str,
        collection_id: Optional[str],
        payloads: List[Union[AssetCreate, VirtualAssetCreate]],
    ):
        """Shared bulk-create implementation.

        Iterates each payload through the Stage 3 ``upsert_asset`` chain
        runner inside a single ``managed_transaction``. Per-row
        ``AssetSidecarRejectedError`` is collected into
        :class:`SidecarRejection` instead of aborting the batch — symmetric
        with the items-side OGC bulk surface.

        Returns:

        * **HTTP 201** with :class:`BulkCreationResponse` when every payload
          is accepted (no policy hits).
        * **HTTP 207** with :class:`IngestionReport` when one or more
          payloads are refused (``REFUSE_FAIL`` matched). The report carries
          ``accepted_ids`` for rows that did land plus structured
          ``rejections`` for the ones that didn't.
        """
        await require_catalog_ready(catalog_id)

        engine = get_engine()
        if engine is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database engine not available.",
            )

        catalogs_svc = cast(
            Optional[CatalogsProtocol], get_protocol(CatalogsProtocol)
        )
        if catalogs_svc is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Catalogs service unavailable.",
            )
        schema = await catalogs_svc.resolve_physical_schema(catalog_id)
        if not schema:
            raise HTTPException(
                status_code=status.HTTP_424_FAILED_DEPENDENCY,
                detail=f"No physical schema for catalog '{catalog_id}'.",
            )

        configs_svc = cast(
            Optional[ConfigsProtocol], get_protocol(ConfigsProtocol)
        )
        if configs_svc is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Configs service unavailable.",
            )

        policy = await configs_svc.get_config(
            AssetsWritePolicy,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )
        scope = Scope(
            schema=schema, catalog_id=catalog_id, collection_id=collection_id
        )
        policy_source = (
            f"/configs/catalogs/{catalog_id}"
            + (f"/collections/{collection_id}" if collection_id else "")
            + f"/plugins/{AssetsWritePolicy.class_key()}"
        )

        accepted_ids: List[str] = []
        accepted_rows: List[Dict[str, Any]] = []
        rejections: List[SidecarRejection] = []

        async with managed_transaction(engine) as conn:
            for payload in payloads:
                try:
                    result: UpsertResult = await upsert_asset(
                        conn,
                        scope,
                        payload,
                        policy,
                        initial_status=AssetStatus.ACTIVE,
                    )
                except AssetSidecarRejectedError as rej:
                    rejections.append(
                        SidecarRejection(
                            external_id=rej.asset_id,
                            sidecar_id=AssetsWritePolicy.class_key(),
                            matcher=rej.matcher,
                            reason=rej.reason,
                            message=rej.message,
                            policy_source=policy_source,
                        )
                    )
                    continue
                accepted_rows.append(result.row)
                accepted_ids.append(str(result.row.get("asset_id")))

        if rejections:
            report = IngestionReport(
                accepted_ids=accepted_ids,
                rejections=rejections,
                total=len(payloads),
            )
            return AppJSONResponse(
                content=report.model_dump(by_alias=True, exclude_none=True),
                status_code=status.HTTP_207_MULTI_STATUS,
            )

        return AppJSONResponse(
            content=BulkCreationResponse(ids=accepted_ids).model_dump(),
            status_code=status.HTTP_201_CREATED,
        )

    def _asset_links_for(self, asset: Asset, request: Request) -> List[Link]:
        """Build OGC HATEOAS links for a single-asset GET response.

        Always emits ``self``. Adds ``collection`` when the asset is
        collection-scoped, and ``alternate`` (download via the parametric
        process surface) for active physical assets.

        Returns relative-path hrefs; the FastAPI app + the registered
        root-path serve them naturally without any baked-in scheme/host.
        """
        from dynastore.extensions.tools.url import get_root_url

        root = get_root_url(request)
        base = f"{root}{self.prefix}/catalogs/{asset.catalog_id}"
        links: List[Link] = []

        if asset.collection_id:
            self_href = (
                f"{base}/collections/{asset.collection_id}"
                f"/assets/{asset.asset_id}"
            )
        else:
            self_href = f"{base}/assets/{asset.asset_id}"

        links.append(
            Link(
                href=self_href,
                rel="self",
                type="application/json",
                title="This asset",  # type: ignore[arg-type]
            )
        )

        if asset.collection_id:
            links.append(
                Link(
                    href=f"{base}/collections/{asset.collection_id}",
                    rel="collection",
                    type="application/json",
                    title="Parent collection",  # type: ignore[arg-type]
                )
            )

        if (
            asset.kind == AssetKind.PHYSICAL
            and asset.status == AssetStatus.ACTIVE
        ):
            if asset.collection_id:
                dl_base = (
                    f"{base}/collections/{asset.collection_id}"
                    f"/assets/{asset.asset_id}"
                )
            else:
                dl_base = f"{base}/assets/{asset.asset_id}"
            links.append(
                Link(
                    href=f"{dl_base}/download",
                    rel="alternate",
                    type="application/octet-stream",
                    title="Download asset bytes",  # type: ignore[arg-type]
                )
            )

        return links

    # =============================================================================
    #  SEARCH
    # =============================================================================


    async def _run_scoped_search(
        self,
        *,
        catalog_id: str,
        collection_id: Optional[str],
        query: SearchQuery,
        limit: Optional[int] = None,
        offset: int = 0,
        all_collections: bool = False,
    ) -> List[Asset]:
        """Shared search impl for the catalog / collection / global routes.

        Delegates to ``AssetsProtocol.search_assets``, which resolves the
        routed SEARCH driver (READ fallback) via ``get_asset_search_driver``.
        ``collection_id=None`` scopes to catalog-tier assets; a value scopes
        to that collection — symmetric with ``list_catalog_assets`` /
        ``list_collection_assets``. ``all_collections=True`` spans every
        collection plus the catalog tier under the catalog.
        """
        return await self.assets.search_assets(
            catalog_id=catalog_id,
            filters=query.filters,
            collection_id=collection_id,
            limit=limit if limit is not None else query.limit,
            offset=offset,
            all_collections=all_collections,
        )

    async def advanced_search(
        self,
        catalog_id: str = Path(..., description="The catalog ID"),
        query: SearchQuery = Body(...),
        all_collections: bool = Query(
            False,
            description=(
                "When false (default) search only catalog-tier assets (those "
                "bound to no collection). When true, search every asset under "
                "the catalog across all its collections and the catalog tier."
            ),
        ),
    ):
        """Granular POST-based search over a catalog.

        Defaults to catalog-tier assets; set ``all_collections=true`` to span
        every collection under the catalog in one call.
        """
        try:
            return await self._run_scoped_search(
                catalog_id=catalog_id,
                collection_id=None,
                query=query,
                offset=query.offset,
                all_collections=all_collections,
            )
        except Exception as e:
            logger.error(f"Search failed: {e}")
            raise HTTPException(status_code=400, detail=str(e))

    async def advanced_search_collection(
        self,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        query: SearchQuery = Body(...),
    ):
        """Granular POST-based search scoped to a single collection.

        The ``collection_id`` from the path is authoritative.
        """
        try:
            return await self._run_scoped_search(
                catalog_id=catalog_id,
                collection_id=collection_id,
                query=query,
                offset=query.offset,
            )
        except Exception as e:
            logger.error(f"Search failed: {e}")
            raise HTTPException(status_code=400, detail=str(e))

    async def advanced_search_global(
        self,
        request: Request,
        query: SearchQuery = Body(...),
        all_collections: bool = Query(
            False,
            description=(
                "When false (default) each catalog contributes only its "
                "catalog-tier assets. When true, span every collection plus "
                "the catalog tier in each catalog."
            ),
        ),
    ):
        """Cross-catalog search. Iterates accessible catalogs and aggregates matches.

        Aggregates up to `limit` matches across catalogs the caller can see.
        Authorization (if any) is enforced by `IamMiddleware` when the IAM
        extension is installed; this handler does not gate on IAM itself.
        """
        from dynastore.models.protocols import CatalogsProtocol
        catalogs_svc = cast(CatalogsProtocol, get_protocol(CatalogsProtocol))
        if catalogs_svc is None:
            raise HTTPException(status_code=503, detail="Catalog service unavailable.")

        try:
            catalogs = await catalogs_svc.list_catalogs(limit=1000)
        except Exception as e:
            logger.error(f"Global search: list_catalogs failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))

        aggregated: List[Asset] = []
        remaining = query.limit
        for cat in catalogs:
            if remaining <= 0:
                break
            cat_id = getattr(cat, "catalog_id", None) or getattr(cat, "id", None)
            if not cat_id:
                continue
            try:
                rows = await self._run_scoped_search(
                    catalog_id=cat_id,
                    collection_id=None,
                    query=query,
                    limit=remaining,
                    offset=0,
                    all_collections=all_collections,
                )
            except Exception as e:
                logger.warning(f"Global search: catalog={cat_id} failed: {e}")
                continue
            aggregated.extend(rows)
            remaining = query.limit - len(aggregated)

        return aggregated[: query.limit]

    # =============================================================================
    #  UPLOAD (BACKEND-AGNOSTIC)
    # =============================================================================

    async def initiate_catalog_upload(
        self,
        catalog_id: str = Path(..., description="Catalog that will own the uploaded asset."),
        body: UploadRequest = Body(
            ...,
            openapi_examples={
                "raster_geotiff": {
                    "summary": "Upload a GeoTIFF raster",
                    "description": "Upload a satellite scene. The GCS backend returns a signed resumable PUT URL.",
                    "value": {
                        "filename": "LC09_L1TP_198030_20251225_02_T1.tif",
                        "content_type": "image/tiff",
                        "asset": {
                            "asset_id": "LC09_198030_20251225",
                            "asset_type": "RASTER",
                            "metadata": {
                                "sensor": "OLI-2",
                                "cloud_cover": 5.2,
                                "acquisition_date": "2025-12-25T10:30:00Z",
                            },
                        },
                    },
                },
                "vector_geojson": {
                    "summary": "Upload a GeoJSON vector file",
                    "description": "Upload administrative boundaries for Italy.",
                    "value": {
                        "filename": "gadm_adm2_italy.geojson",
                        "content_type": "application/geo+json",
                        "asset": {
                            "asset_id": "italy_adm2",
                            "asset_type": "VECTORIAL",
                            "metadata": {"source": "GADM", "version": "4.1", "country": "ITA"},
                        },
                    },
                },
                "generic_csv": {
                    "summary": "Upload a generic CSV data file",
                    "description": "Upload a tabular dataset.",
                    "value": {
                        "filename": "stations_2025.csv",
                        "content_type": "text/csv",
                        "asset": {
                            "asset_id": "stations_2025",
                            "asset_type": "ASSET",
                            "metadata": {"year": 2025, "variable": "temperature"},
                        },
                    },
                },
            },
        ),
    ) -> UploadTicket:
        """
        Initiates an upload session for a catalog-level asset.

        Stage 4.1 atomic flow:

        1. Resolve the per-collection :class:`AssetsWritePolicy` via the
           configs waterfall.
        2. Run the policy chain + born-claimed PENDING INSERT in a single DB
           transaction. Policy ``REFUSE_FAIL`` raises
           :class:`AssetSidecarRejectedError` (mapped to a structured 409 by
           :class:`AssetSidecarRejectedExceptionHandler`); ``REFUSE`` /
           ``REFUSE_RETURN`` echo the existing row; ``UPDATE`` mutates
           metadata; ``NEW_VERSION`` archives + INSERTs.
        3. Mint the signed URL via the resolved upload driver — DB-free,
           post-commit.
        4. Stamp the ticket id back onto the row's ``metadata._upload`` so
           the Stage 4.2 finalize handler can correlate.

        On a URL-mint failure after the row was inserted, the row is
        soft-deleted in the same request to avoid a stuck PENDING that would
        block a retry of the same filename.
        """
        return await self._initiate_upload_with_policy(
            catalog_id=catalog_id,
            collection_id=None,
            body=body,
        )

    async def initiate_collection_upload(
        self,
        catalog_id: str = Path(..., description="Catalog that will own the uploaded asset."),
        collection_id: str = Path(..., description="Collection scope for the asset."),
        body: UploadRequest = Body(
            ...,
            openapi_examples={
                "raster_geotiff": {
                    "summary": "Upload a GeoTIFF to a collection",
                    "description": "Upload a Landsat scene to the 'landsat_scenes' collection.",
                    "value": {
                        "filename": "LC09_L1TP_198030_20251225_02_T1.tif",
                        "content_type": "image/tiff",
                        "asset": {
                            "asset_id": "LC09_198030_20251225",
                            "asset_type": "RASTER",
                            "metadata": {
                                "sensor": "OLI-2",
                                "cloud_cover": 5.2,
                                "acquisition_date": "2025-12-25T10:30:00Z",
                            },
                        },
                    },
                },
                "vector_collection": {
                    "summary": "Upload a vector dataset to a collection",
                    "description": "Upload a GeoPackage of drainage basins.",
                    "value": {
                        "filename": "drainage_basins_africa.gpkg",
                        "content_type": "application/geopackage+sqlite3",
                        "asset": {
                            "asset_id": "drainage_africa_2025",
                            "asset_type": "VECTORIAL",
                            "metadata": {"continent": "Africa", "source": "HydroSHEDS"},
                        },
                    },
                },
            },
        ),
    ) -> UploadTicket:
        """
        Initiates an upload session scoped to a collection.

        Same Stage 4.1 atomic flow as
        :meth:`initiate_catalog_upload` (policy gate + born-claimed PENDING
        INSERT + driver URL mint + post-commit ticket stamp). The
        ``collection_id`` from the path is authoritative; the
        :class:`AssetsWritePolicy` is resolved at the collection scope so
        per-collection overrides take precedence over catalog/platform tiers.
        """
        return await self._initiate_upload_with_policy(
            catalog_id=catalog_id,
            collection_id=collection_id,
            body=body,
        )

    async def _initiate_upload_with_policy(
        self,
        *,
        catalog_id: str,
        collection_id: Optional[str],
        body: UploadRequest,
    ) -> UploadTicket:
        """Shared catalog/collection upload-create implementation.

        Encapsulates the four-step Stage 4.1 flow so the two FastAPI route
        functions can stay thin and OpenAPI-friendly while sharing the exact
        same atomic semantics. The split between ``managed_transaction`` (for
        the policy + born-claimed INSERT) and the post-commit driver call is
        load-bearing: drivers must remain DB-free so they can be exercised
        without a live PG connection (Stage 4.0 contract).
        """
        await require_catalog_ready(catalog_id)

        # 1. Resolve the engine, schema, configs service.
        engine = get_engine()
        if engine is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database engine not available.",
            )

        catalogs_svc = cast(
            Optional[CatalogsProtocol], get_protocol(CatalogsProtocol)
        )
        if catalogs_svc is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Catalogs service unavailable.",
            )
        schema = await catalogs_svc.resolve_physical_schema(catalog_id)
        if not schema:
            raise HTTPException(
                status_code=status.HTTP_424_FAILED_DEPENDENCY,
                detail=f"No physical schema for catalog '{catalog_id}'.",
            )

        configs_svc = cast(
            Optional[ConfigsProtocol], get_protocol(ConfigsProtocol)
        )
        if configs_svc is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Configs service unavailable.",
            )

        # 2. Load the AssetsWritePolicy via the standard waterfall.
        policy = await configs_svc.get_config(
            AssetsWritePolicy,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )

        # 3. Resolve the upload driver early so a missing backend fails BEFORE
        #    we touch the DB.
        provider = await self.resolve_upload_driver(
            catalog_id, collection_id=collection_id
        )
        if not provider:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No upload backend is available. Configure a storage module (e.g. GCP, local).",
            )

        # 4. Build the AssetCreate from the upload request body. The asset_id
        #    / asset_type / metadata come from the embedded
        #    ``AssetUploadDefinition``; ``filename`` from the request top
        #    level. ``owned_by`` is left None at create time — the finalize
        #    event in Stage 4.2 will stamp it (e.g. ``"gcs"``).
        payload = AssetCreate(
            asset_id=body.asset.asset_id,
            asset_type=body.asset.asset_type,
            filename=body.filename,
            metadata=dict(body.asset.metadata) if body.asset.metadata else {},
            owned_by=None,
        )

        scope = Scope(
            schema=schema, catalog_id=catalog_id, collection_id=collection_id
        )

        # 5. Atomic policy + born-claimed PENDING INSERT.
        #    Any AssetSidecarRejectedError raised inside upsert_asset bubbles
        #    out of this transaction and is mapped to a structured 409 by
        #    AssetSidecarRejectedExceptionHandler.
        async with managed_transaction(engine) as conn:
            result: UpsertResult = await upsert_asset(
                conn,
                scope,
                payload,
                policy,
                initial_status=AssetStatus.PENDING,
            )

        # 6. Idempotent paths: if the policy short-circuited to an existing
        #    row, reconstruct its previously-issued ticket from the row's
        #    persisted ``metadata._upload`` blob (stamped at initiate time)
        #    and return it instead of minting a new one. The blob survives
        #    process restarts and is visible to every replica, so a resumed
        #    upload resolves regardless of which process initiated it.
        if result.action in {"refused", "returned_existing", "updated"}:
            existing_ticket = self._extract_existing_ticket(result.row)
            if existing_ticket:
                return existing_ticket
            # Fall through: row exists but no live ticket — caller observes
            # an idempotent 409 alternative would mask the success of the
            # upsert. We re-mint a URL so the client can resume.

        # 7. Mint the signed URL via the driver (DB-free, post-commit).
        try:
            ticket = await provider.initiate_upload(
                catalog_id=catalog_id,
                asset_def=body.asset,
                filename=body.filename,
                content_type=body.content_type,
                collection_id=collection_id,
            )
        except HTTPException:
            await self._rollback_pending_row(engine, scope, payload.asset_id)
            raise
        except Exception as exc:
            await self._rollback_pending_row(engine, scope, payload.asset_id)
            logger.error(
                "Upload URL mint failed for '%s/%s' (asset=%s): %s",
                catalog_id,
                collection_id or "_",
                payload.asset_id,
                exc,
                exc_info=True,
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Upload initiation failed: {exc}",
            )

        # 8. Stamp the ticket onto the row's metadata so the Stage 4.2
        #    finalize handler can correlate.
        await self._stamp_ticket_metadata(engine, scope, payload.asset_id, ticket)

        return ticket

    @staticmethod
    def _extract_existing_ticket(row: Dict[str, Any]) -> Optional[UploadTicket]:
        """Parse the ``metadata._upload`` blob written by step 8 back into an
        :class:`UploadTicket`. Returns ``None`` when the row never carried a
        ticket (legacy rows, or rows whose driver session has been GC'd).
        """
        meta = row.get("metadata")
        if not isinstance(meta, dict):
            return None
        upload = meta.get("_upload")
        if not isinstance(upload, dict):
            return None
        try:
            return UploadTicket(
                ticket_id=str(upload["ticket_id"]),
                upload_url=str(upload["upload_url"]),
                method=str(upload.get("method", "PUT")),
                headers=dict(upload.get("headers") or {}),
                expires_at=upload["expires_at"],
                backend=str(upload["backend"]),
            )
        except Exception:
            return None

    @staticmethod
    async def _stamp_ticket_metadata(
        engine: Any,
        scope: Scope,
        asset_id: str,
        ticket: UploadTicket,
    ) -> None:
        """JSON-merge the ticket fields into ``metadata._upload`` post-commit.

        ``metadata = metadata || CAST(:upload_meta AS jsonb)`` is the standard PG
        idiom for shallow merging — the existing keys are preserved and only
        the ``_upload`` slot is overwritten. Uses :class:`DQLQuery` so the
        same code path serves async (production) and sync (tests) engines.
        """
        upload_blob = {
            "_upload": {
                "ticket_id": ticket.ticket_id,
                "upload_url": ticket.upload_url,
                "method": ticket.method,
                "headers": dict(ticket.headers),
                "expires_at": ticket.expires_at.isoformat(),
                "backend": ticket.backend,
            }
        }
        sql = (
            f'UPDATE "{scope.schema}".assets '
            "SET metadata = metadata || CAST(:upload_meta AS jsonb), "
            "    updated_at = NOW() "
            "WHERE catalog_id = :catalog_id "
            "AND collection_id IS NOT DISTINCT FROM :collection_id "
            "AND asset_id = :asset_id"
        )
        async with managed_transaction(engine) as conn:
            await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                conn,
                catalog_id=scope.catalog_id,
                collection_id=scope.collection_id,
                asset_id=asset_id,
                upload_meta=json.dumps(upload_blob),
            )

    @staticmethod
    async def _rollback_pending_row(
        engine: Any, scope: Scope, asset_id: str
    ) -> None:
        """Soft-delete the just-inserted PENDING row when URL mint fails.

        Without this cleanup, the next upload attempt for the same filename
        would surface a spurious 409 (the orphan PENDING row would still
        match the ``FILENAME`` matcher) until Stage 6's reconcile sweep
        catches it — far too long a delay for an interactive REST flow.
        """
        sql = (
            f'UPDATE "{scope.schema}".assets '
            "SET status = 'deleted', updated_at = NOW() "
            "WHERE catalog_id = :catalog_id "
            "AND collection_id IS NOT DISTINCT FROM :collection_id "
            "AND asset_id = :asset_id "
            "AND status = 'pending'"
        )
        try:
            async with managed_transaction(engine) as conn:
                await DQLQuery(sql, result_handler=ResultHandler.NONE).execute(
                    conn,
                    catalog_id=scope.catalog_id,
                    collection_id=scope.collection_id,
                    asset_id=asset_id,
                )
        except Exception as cleanup_exc:
            logger.warning(
                "Failed to soft-delete orphan PENDING row for asset=%s: %s",
                asset_id,
                cleanup_exc,
                exc_info=True,
            )

    async def get_upload_status(
        self,
        catalog_id: str = Path(..., description="Catalog that owns the upload session."),
        ticket_id: str = Path(
            ...,
            description="Upload ticket ID returned by the initiate-upload endpoint.",
        ),
    ) -> UploadStatusResponse:
        """
        Polls the current lifecycle status of an upload session.

        Status values:

        | Value       | Meaning                                                                         |
        |-------------|---------------------------------------------------------------------------------|
        | `pending`   | Session created; client has not yet delivered the file.                         |
        | `uploading` | Transfer in progress (resumable / multipart).                                   |
        | `completed` | File received and asset registered in the catalog.                              |
        | `failed`    | Upload or registration failed; see ``error`` field.                             |
        | `cancelled` | Session expired or explicitly cancelled.                                        |
        """
        provider = await self.resolve_upload_driver(catalog_id)
        if not provider:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No upload backend is available.",
            )
        return await provider.get_upload_status(
            ticket_id=ticket_id,
            catalog_id=catalog_id,
        )

    # =============================================================================
    #  ASSET REFERENCES
    # =============================================================================

    async def list_asset_references(
        self,
        catalog_id: str = Path(..., description="Catalog scope."),
        asset_id: str = Path(..., description="Asset ID to inspect."),
    ) -> List[AssetReference]:
        """
        Returns all active references to this asset.

        Any entry with ``cascade_delete=false`` is a **blocking** reference:
        it prevents hard-deletion of the asset (HTTP 409 Conflict) until the
        referencing entity (collection, DuckDB table, etc.) is dropped and the
        reference is explicitly removed.
        """
        asset = await self.assets.get_asset(asset_id=asset_id, catalog_id=catalog_id)
        if not asset:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found.")
        return await self.assets.list_asset_references(
            asset_id=asset_id, catalog_id=catalog_id
        )

    async def remove_asset_reference(
        self,
        catalog_id: str = Path(..., description="Catalog scope."),
        asset_id: str = Path(..., description="Asset ID."),
        ref_type: str = Path(
            ...,
            description=(
                "Reference type discriminator (e.g. `collection`, `duckdb:table`, `iceberg:table`). "
                "Use the namespaced string value of the ``AssetReferenceType`` enum."
            ),
        ),
        ref_id: str = Path(
            ...,
            description="Owner-scoped identifier of the referencing entity (collection_id, table name, …).",
        ),
    ):
        """
        Manually removes a reference from this asset.

        **Use with caution.** Removing a blocking reference (`cascade_delete=false`)
        immediately unblocks hard-deletion even if the referencing driver has not
        yet cleaned up its data. Only call this **after** the referencing
        collection or table has been dropped.
        """
        from dynastore.models.shared_models import CoreAssetReferenceType, AssetReferenceType

        # Convert the path string to a typed enum value if it matches a known type
        typed_ref_type: AssetReferenceType
        try:
            typed_ref_type = CoreAssetReferenceType(ref_type)
        except (ValueError, TypeError):
            # Accept unknown values as raw strings (forward-compat with unknown driver types)
            typed_ref_type = cast(AssetReferenceType, ref_type)

        asset = await self.assets.get_asset(asset_id=asset_id, catalog_id=catalog_id)
        if not asset:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found.")

        await self.assets.remove_asset_reference(
            asset_id=asset_id,
            catalog_id=catalog_id,
            ref_type=typed_ref_type,
            ref_id=ref_id,
        )

    # =============================================================================
    #  ASSET DOWNLOAD
    # =============================================================================

    async def _load_asset(
        self,
        catalog_id: str,
        asset_id: str,
        collection_id: Optional[str],
    ):
        asset = await self.assets.get_asset(
            catalog_id=catalog_id, asset_id=asset_id, collection_id=collection_id
        )
        if not asset:
            raise HTTPException(status_code=404, detail="Asset not found")
        return asset

    async def _download_asset(
        self,
        catalog_id: str,
        asset_id: str,
        collection_id: Optional[str],
        ttl: Optional[int],
    ):
        from fastapi.responses import RedirectResponse

        asset = await self._load_asset(catalog_id, asset_id, collection_id)

        # Backend-resolved download (GCS signed URL, local bearer-auth route).
        for provider in get_protocols(AssetDownloadProtocol):
            if provider.applies_to(asset):
                url = await provider.resolve_download_url(asset, ttl)
                return RedirectResponse(url, status_code=status.HTTP_302_FOUND)

        # Externally-hosted assets: redirect straight to the public URL.
        target = asset.href or asset.uri
        if target and target.startswith(("http://", "https://")):
            return RedirectResponse(target, status_code=status.HTTP_302_FOUND)

        raise HTTPException(
            status_code=404,
            detail="No download backend available for this asset.",
        )

    async def download_catalog_asset(
        self,
        catalog_id: str = Path(..., description="The catalog ID"),
        asset_id: str = Path(..., description="The asset ID"),
        ttl: Optional[int] = Query(
            default=None,
            description="Signed-URL lifetime in seconds (backends that mint one).",
        ),
    ):
        """Redirects to a backend-resolved download URL for a catalog asset."""
        return await self._download_asset(catalog_id, asset_id, None, ttl)

    async def download_collection_asset(
        self,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        asset_id: str = Path(..., description="The asset ID"),
        ttl: Optional[int] = Query(
            default=None,
            description="Signed-URL lifetime in seconds (backends that mint one).",
        ),
    ):
        """Redirects to a backend-resolved download URL for a collection asset."""
        return await self._download_asset(catalog_id, asset_id, collection_id, ttl)

    # =============================================================================
    #  BUCKET↔DB DRIFT (Stage 6)
    # =============================================================================

    async def _run_reconcile(
        self,
        catalog_id: str,
        collection_id: Optional[str],
        *,
        apply: bool,
        pending_ttl_minutes: int,
    ):
        """Resolve schema/bucket/client and dispatch to ``reconcile_bucket``.

        Centralised so the four drift routes (catalog/collection × dry/apply)
        share one error-mapping path.
        """
        from dynastore.tasks.gcp.bucket_reconcile_task import (
            BucketReconcileInputs,
            reconcile_bucket,
            resolve_reconcile_context,
        )

        try:
            schema, bucket_name, storage_client, engine = (
                await resolve_reconcile_context(catalog_id)
            )
        except RuntimeError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=str(exc),
            )

        inputs = BucketReconcileInputs(
            catalog_id=catalog_id,
            collection_id=collection_id,
            apply=apply,
            pending_ttl_minutes=pending_ttl_minutes,
        )
        report = await reconcile_bucket(
            inputs,
            schema=schema,
            bucket_name=bucket_name,
            storage_client=storage_client,
            engine=engine,
        )
        return report.model_dump(mode="json")

    async def get_catalog_assets_drift(
        self,
        catalog_id: str = Path(..., description="The catalog ID"),
        pending_ttl_minutes: int = Query(
            60,
            ge=1,
            le=1440,
            description=(
                "PENDING rows older than this threshold are reported as "
                "``stuck_pending`` drift entries."
            ),
        ),
    ):
        """Read-only bucket↔DB diff for a catalog (no mutations)."""
        await require_catalog_ready(catalog_id)
        return await self._run_reconcile(
            catalog_id,
            None,
            apply=False,
            pending_ttl_minutes=pending_ttl_minutes,
        )

    async def get_collection_assets_drift(
        self,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        pending_ttl_minutes: int = Query(60, ge=1, le=1440),
    ):
        """Read-only bucket↔DB diff scoped to a collection."""
        await require_catalog_ready(catalog_id)
        return await self._run_reconcile(
            catalog_id,
            collection_id,
            apply=False,
            pending_ttl_minutes=pending_ttl_minutes,
        )

    async def execute_catalog_bucket_reconcile(
        self,
        catalog_id: str = Path(..., description="The catalog ID"),
        pending_ttl_minutes: int = Query(60, ge=1, le=1440),
    ):
        """Apply the bucket↔DB diff for a catalog (synchronous)."""
        await require_catalog_ready(catalog_id)
        return await self._run_reconcile(
            catalog_id,
            None,
            apply=True,
            pending_ttl_minutes=pending_ttl_minutes,
        )

    async def execute_collection_bucket_reconcile(
        self,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        pending_ttl_minutes: int = Query(60, ge=1, le=1440),
    ):
        """Apply the bucket↔DB diff for a collection (synchronous)."""
        await require_catalog_ready(catalog_id)
        return await self._run_reconcile(
            catalog_id,
            collection_id,
            apply=True,
            pending_ttl_minutes=pending_ttl_minutes,
        )
