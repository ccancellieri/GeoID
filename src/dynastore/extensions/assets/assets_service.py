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

import logging
from typing import Dict, Any, Optional, List, Annotated, cast
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
    BackgroundTasks,
)
from pydantic import UUID4, BaseModel, Field, AliasChoices, ConfigDict

from dynastore.extensions.protocols import ExtensionProtocol
from dynastore.models.protocols import AssetsProtocol, AssetUploadProtocol, UploadTicket, UploadStatusResponse
from dynastore.models.protocols.asset_process import (
    AssetProcessDescriptor,
    AssetProcessOutput,
    AssetProcessProtocol,
)
from dynastore.tools.protocol_helpers import get_engine
from dynastore.extensions.tools.catalog_readiness import require_catalog_ready
from dynastore.extensions.tools.exception_handlers import handle_exception
from fastapi import Depends
from dynastore.modules.catalog.catalog_module import CatalogModule
from dynastore.modules.catalog.asset_service import (
    Asset,
    AssetBase,
    AssetFilter,
    AssetUpdate,
    AssetUploadDefinition,
    AssetReference,
    AssetReferencedError,
)
from dynastore.modules.catalog.asset_tasks_spi import AssetTasksSPI
from dynastore.modules.processes.models import (
    ExecuteRequest,
    StatusInfo,
    JobControlOptions,
    Process,
)
from contextlib import asynccontextmanager
import uuid

logger = logging.getLogger(__name__)



class SearchQuery(BaseModel):
    """Payload for advanced asset searching."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    filters: List[AssetFilter] = Field(
        default_factory=list, description="List of granular filters to apply."
    )
    collection_id: Annotated[
        Optional[str],
        Field(None, description="Optional scope to a specific collection."),
    ]
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


class AssetService(ExtensionProtocol):
    priority: int = 100
    """
    Asset Service Extension.
    Exposes API endpoints for managing assets across catalogs and collections.
    """

    def __init__(self, app: FastAPI):
        self.app = app
        self.router = APIRouter(prefix="/assets", tags=["Assets"])
        self._setup_routes()
        # Register the backend-agnostic UploadAssetProcess so the parametric
        # /processes/{id}/execution dispatch surface picks it up alongside
        # the backend-owned ``download`` processes from modules/{gcp,local}.
        from dynastore.extensions.assets.asset_processes import UploadAssetProcess
        from dynastore.tools.discovery import register_plugin
        self._upload_process = UploadAssetProcess()
        register_plugin(self._upload_process)

    def _setup_routes(self):
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
            methods=["PUT", "PATCH"],
            response_model=Asset,
            summary="Update Asset",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/assets/{asset_id}",
            self.delete_catalog_asset_by_id,
            methods=["DELETE"],
            status_code=status.HTTP_204_NO_CONTENT,
            summary="Delete Asset",
            description=(
                "Deletes an asset by ID. "
                "Soft-delete (default) sets `deleted_at`; hard-delete (`force=true`) removes "
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
            methods=["PUT", "PATCH"],
            response_model=Asset,
            summary="Update Collection Asset",
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
        # Canonical asset search routes
        self.router.add_api_route(
            "/assets-search",
            self.advanced_search_global,
            methods=["POST"],
            summary="Advanced Asset Search (cross-catalog)",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/assets-search",
            self.advanced_search,
            methods=["POST"],
            summary="Advanced Asset Search (scoped to catalog)",
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
        self.router.add_api_route(
            "/catalogs/{catalog_id}/assets/{asset_id}/tasks",
            self.list_catalog_asset_tasks,
            methods=["GET"],
            response_model=List[Process],
            summary="List Tasks for Catalog Asset",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/assets/{asset_id}/tasks/{task_id}/execute",
            self.execute_catalog_asset_task,
            methods=["POST"],
            status_code=status.HTTP_201_CREATED,
            summary="Execute Task on Catalog Asset",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_id}/tasks",
            self.list_collection_asset_tasks,
            methods=["GET"],
            response_model=List[Process],
            summary="List Tasks for Collection Asset",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_id}/tasks/{task_id}/execute",
            self.execute_collection_asset_task,
            methods=["POST"],
            status_code=status.HTTP_201_CREATED,
            summary="Execute Task on Collection Asset",
        )
        # -----------------------------------------------------------------
        # Parametric asset-process dispatch (see models/protocols/asset_process.py).
        # Registered AFTER all specific routes so ``{process_id}`` does not shadow
        # ``references`` / ``tasks`` / ``upload``. FastAPI matches in declaration order.
        # -----------------------------------------------------------------
        self.router.add_api_route(
            "/catalogs/{catalog_id}/assets/{asset_id}/processes",
            self.list_catalog_asset_processes,
            methods=["GET"],
            response_model=List[AssetProcessDescriptor],
            summary="List Asset Processes",
            description=(
                "Enumerates processes that can be invoked on this asset "
                "(e.g. ``download``, ``ingest``, ``validate``). Each entry "
                "reports ``applicable=false`` with a ``reason`` when the "
                "process is registered but cannot run on this specific asset."
            ),
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_id}/processes",
            self.list_collection_asset_processes,
            methods=["GET"],
            response_model=List[AssetProcessDescriptor],
            summary="List Collection Asset Processes",
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/assets/{asset_id}/processes/{process_id}/execution",
            self.invoke_catalog_asset_process,
            methods=["POST"],
            response_model=AssetProcessOutput,
            summary="Invoke Asset Process",
            description=(
                "Executes a registered asset process (``download``, ``ingest``, "
                "``validate``, etc.) on this asset. OGC API - Processes "
                "style URL: parameters go in the JSON body. A ``302`` "
                "redirect is returned when the process yields ``type=redirect``."
            ),
            responses={
                200: {"description": "Process executed; returns ``AssetProcessOutput``."},
                302: {"description": "Redirect to the asset's external URL."},
                404: {"description": "Asset or process not found."},
                409: {"description": "Process not applicable to this asset."},
            },
        )
        self.router.add_api_route(
            "/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_id}/processes/{process_id}/execution",
            self.invoke_collection_asset_process,
            methods=["POST"],
            response_model=AssetProcessOutput,
            summary="Invoke Collection Asset Process",
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
        *,
        hint: Optional[str] = None,
    ) -> Optional[AssetUploadProtocol]:
        """Resolve the upload backend for a given catalog/collection.

        Picks via ``AssetRoutingConfig.operations[UPLOAD]`` (auto-augmented
        with all discoverable ``AssetUploadProtocol`` impls) with first-match
        fallback when no UPLOAD entries resolve.
        """
        from dynastore.modules.storage.router import get_asset_upload_driver
        return await get_asset_upload_driver(
            catalog_id, collection_id, hint=hint,
        )

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
        asset_in: AssetBase, catalog_id: str = Path(..., description="The catalog ID")
    ):
        """Creates a new asset at the catalog level."""
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
        catalog_id: str = Path(..., description="The catalog ID"),
        asset_id: str = Path(..., description="The asset ID"),
    ):
        """Retrieves details for a specific asset in the catalog."""
        asset = await self.assets.get_asset(
            catalog_id=catalog_id, asset_id=asset_id, collection_id=None
        )
        if not asset:
            raise HTTPException(status_code=404, detail="Asset not found")
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
        asset_in: AssetBase,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
    ):
        """Creates a new asset associated with a specific collection."""
        await require_catalog_ready(catalog_id)
        # Ensure the asset object has the correct collection linkage if passed, or override
        # We rely on arguments passed to create_asset, but asset_in might have conflicting info?
        # AssetBase doesn't have collection_id, so it's fine.
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
    #  SEARCH
    # =============================================================================


    async def advanced_search(
        self,
        catalog_id: str = Path(..., description="The catalog ID"),
        query: SearchQuery = Body(...),
    ):
        """
        Granular POST-based search using the advanced query builder.
        """
        try:
            return await self.assets.search_assets(
                catalog_id=catalog_id,
                filters=query.filters,
                collection_id=query.collection_id,
                limit=query.limit,
                offset=query.offset,
            )
        except Exception as e:
            logger.error(f"Search failed: {e}")
            raise HTTPException(status_code=400, detail=str(e))

    async def advanced_search_global(
        self,
        request: Request,
        query: SearchQuery = Body(...),
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
                rows = await self.assets.search_assets(
                    catalog_id=cat_id,
                    filters=query.filters,
                    collection_id=query.collection_id,
                    limit=remaining,
                    offset=0,
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

        Returns an ``UploadTicket`` with a backend-specific upload URL.
        For **GCS/S3** backends, PUT the file directly to ``upload_url`` using
        the provided ``method`` and ``headers``.
        For **local/HTTP** backends, POST the file to the server-side proxy path.

        After delivery the backend registers the asset automatically.
        Poll ``GET /assets/catalogs/{catalog_id}/upload/{ticket_id}/status``
        to confirm registration.
        """
        await require_catalog_ready(catalog_id)
        provider = await self.resolve_upload_driver(catalog_id, collection_id=None)
        if not provider:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No upload backend is available. Configure a storage module (e.g. GCP, local).",
            )
        try:
            return await provider.initiate_upload(
                catalog_id=catalog_id,
                asset_def=body.asset,
                filename=body.filename,
                content_type=body.content_type,
                collection_id=None,
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Upload initiation failed for catalog '{catalog_id}': {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Upload initiation failed: {e}",
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

        Identical to the catalog-level upload endpoint but the file is stored
        under the collection's path prefix.  The ``collection_id`` from the
        path takes precedence.
        """
        await require_catalog_ready(catalog_id)
        provider = await self.resolve_upload_driver(catalog_id, collection_id=collection_id)
        if not provider:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No upload backend is available. Configure a storage module (e.g. GCP, local).",
            )
        try:
            return await provider.initiate_upload(
                catalog_id=catalog_id,
                asset_def=body.asset,
                filename=body.filename,
                content_type=body.content_type,
                collection_id=collection_id,
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(
                f"Upload initiation failed for '{catalog_id}/{collection_id}': {e}",
                exc_info=True,
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Upload initiation failed: {e}",
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
    #  ASSET TASKS (SPI)
    # =============================================================================

    @staticmethod
    async def _get_spi_instance(
        config: Any, app_state: object
    ) -> Optional[AssetTasksSPI]:
        """
        Resolves an AssetTasksSPI instance from a TaskConfig.
        If the instance is not already created, it attempts a lightweight instantiation
        if the task class implements the SPI.
        """
        # 1. Check if already instantiated
        if config.instance and isinstance(config.instance, AssetTasksSPI):
            return config.instance

        # 2. If not instantiated, check if the class implements the SPI and is not a placeholder
        cls = config.cls
        if cls and not getattr(cls, "is_placeholder", False):
            try:
                # Use issubclass for explicit inheritance or Protocol check
                if issubclass(cls, AssetTasksSPI):
                    logger.debug(
                        f"Task class '{config.name}' implements AssetTasksSPI. Attempting on-demand instantiation."
                    )
                    # Instantiate (mimicking dynastore.tasks.manage_tasks logic)
                    if cls.__init__ is not object.__init__:
                        import inspect

                        sig = inspect.signature(cls.__init__)
                        factory = cast(Any, cls)
                        if "app_state" in sig.parameters:
                            instance = factory(app_state=app_state)
                        else:
                            instance = factory()
                    else:
                        instance = cls()

                    # Note: We don't cache this back to config.instance here to avoid
                    # side-effects on the global task registry, but we return it for use.
                    return instance
            except Exception as e:
                logger.debug(
                    f"Could not check or instantiate task class '{config.name}' for SPI: {e}"
                )

        return None

    @staticmethod
    async def _get_available_tasks_for_asset(
        asset: Asset, app_state: object
    ) -> List[Any]:
        from dynastore.tasks import get_all_task_configs

        configs = get_all_task_configs()

        available = []
        logger.info(
            f"Finding tasks for asset {asset.asset_id} (type: {asset.asset_type})..."
        )

        for name, config in configs.items():
            logger.debug(f"Checking task: {name} (module: {config.module_name})")

            # Use the robust instance resolver
            instance = await AssetService._get_spi_instance(config, app_state)

            if instance:
                try:
                    logger.debug(f"Calling can_run_on_asset for task '{name}'...")
                    if await instance.can_run_on_asset(asset):
                        logger.info(f"Task '{name}' can run on asset {asset.asset_id}.")
                        if config.definition:
                            available.append(config.definition)
                        else:
                            # Fallback: try to get it from the instance if missing in config
                            if hasattr(instance, "get_process_definition"):
                                available.append(instance.get_process_definition())
                            else:
                                logger.warning(
                                    f"Task '{name}' matched but has no process definition."
                                )
                    else:
                        logger.debug(
                            f"Task '{name}' cannot run on asset {asset.asset_id}."
                        )
                except Exception as e:
                    logger.error(
                        f"Error checking SPI for task '{config.name}': {e}",
                        exc_info=True,
                    )
            else:
                logger.debug(
                    f"Task '{name}' does not implement AssetTasksSPI implementation (instance: {config.instance is not None})."
                )

        return available


    async def list_catalog_asset_tasks(
        self,
        request: Request,
        catalog_id: str = Path(..., description="The catalog ID"),
        asset_id: str = Path(..., description="The asset ID"),
    ):
        """Lists tasks that can be executed on this catalog-level asset."""
        try:
            if self.assets is None:
                logger.error("self.assets is None! Extension not initialized properly?")
                raise HTTPException(
                    status_code=500, detail="AssetService not initialized properly"
                )

            asset = await self.assets.get_asset(catalog_id=catalog_id, asset_id=asset_id)
            if not asset:
                raise HTTPException(status_code=404, detail="Asset not found")
            return await AssetService._get_available_tasks_for_asset(
                asset, request.app.state
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.critical(
                f"UNHANDLED EXCEPTION in list_catalog_asset_tasks: {e}", exc_info=True
            )
            raise HTTPException(status_code=500, detail=f"Internal Server Error: {e}")


    async def list_collection_asset_tasks(
        self,
        request: Request,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        asset_id: str = Path(..., description="The asset ID"),
    ):
        """Lists tasks that can be executed on this collection-level asset."""
        asset = await self.assets.get_asset(
            catalog_id=catalog_id, asset_id=asset_id, collection_id=collection_id
        )
        if not asset:
            raise HTTPException(status_code=404, detail="Asset not found")
        return await AssetService._get_available_tasks_for_asset(
            asset, request.app.state
        )


    async def execute_catalog_asset_task(
        self,
        request: Request,
        background_tasks: BackgroundTasks,
        catalog_id: str = Path(..., description="The catalog ID"),
        asset_id: str = Path(..., description="The asset ID"),
        task_id: str = Path(
            ..., description="The task (process) ID", examples=["gdal"]
        ),
        mode: Optional[JobControlOptions] = Query(
            None, description="Execution mode preference."
        ),
        execution_request: ExecuteRequest = Body(
            ...,
            examples=[
                {
                    "inputs": {
                        "asset_metadata": {"description": "Custom override"},
                        "other_param": "value",
                    }
                }
            ],
            description="Task execution parameters. Use 'inputs' to pass arguments or override metadata.",
        ),
    ):
        """Triggers a background task on the specified catalog asset."""
        asset = await self.assets.get_asset(catalog_id=catalog_id, asset_id=asset_id)
        if not asset:
            raise HTTPException(status_code=404, detail="Asset not found")

        principal = getattr(request.state, "principal", None)
        caller_id_val = principal.id if principal else "nouser"
        return await AssetService._execute_asset_task(
            request,
            background_tasks,
            asset,
            task_id,
            execution_request,
            str(caller_id_val),
            mode,
        )


    async def execute_collection_asset_task(
        self,
        request: Request,
        background_tasks: BackgroundTasks,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        asset_id: str = Path(..., description="The asset ID"),
        task_id: str = Path(
            ..., description="The task (process) ID", examples=["ingestion"]
        ),
        mode: Optional[JobControlOptions] = Query(
            None, description="Execution mode preference."
        ),
        execution_request: ExecuteRequest = Body(
            ...,
            examples=[
                {
                    "inputs": {
                        "catalog_id": "target_catalog_id",
                        "collection_id": "target_collection_id",
                        "ingestion_request": {
                            "asset": {"asset_id": "asset_id_example"},
                            "column_mapping": {
                                "external_id": "station_id",
                                "csv_lat_column": "latitude",
                                "csv_lon_column": "longitude",
                            },
                        },
                    }
                }
            ],
            description="Task execution parameters. Use 'inputs' to pass arguments or override metadata.",
        ),
    ):
        """Triggers a background task on the specified collection asset."""
        asset = await self.assets.get_asset(
            catalog_id=catalog_id, asset_id=asset_id, collection_id=collection_id
        )
        if not asset:
            raise HTTPException(status_code=404, detail="Asset not found")

        principal = getattr(request.state, "principal", None)
        caller_id_val = principal.id if principal else "nouser"
        return await AssetService._execute_asset_task(
            request,
            background_tasks,
            asset,
            task_id,
            execution_request,
            str(caller_id_val),
            mode,
        )

    @staticmethod
    async def _execute_asset_task(
        request: Request,
        background_tasks: BackgroundTasks,
        asset: Asset,
        task_id: str,
        execution_request: ExecuteRequest,
        caller_id: str,
        mode: Optional[JobControlOptions] = None,
    ):
        # We use the processes_module to execute the process
        import dynastore.modules.processes.processes_module as processes_module

        # We might need to inject the asset into the execution request inputs if the task expects it
        # Requirement: "each task through the SPI may accept or not the asset as input for the process"
        # We'll let the user provide it in 'inputs', or we can automatically inject it if it's not there?
        # Most likely, the runner of the process will be triggered.

        engine = get_engine()
        if engine is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database engine not available.",
            )

        # SPI Adapter Layer: try to get the task instance to call its specialized adapter if available.
        from dynastore.tasks import get_all_task_configs

        configs = get_all_task_configs()
        config = configs.get(task_id)

        task_instance = None
        if config:
            task_instance = await AssetService._get_spi_instance(
                config, request.app.state
            )

        logger.debug(
            f"Executing asset task '{task_id}' on asset '{asset.asset_id}'. SPI check starting..."
        )

        if task_instance:
            # 1. Initial Guard: Check if the task can run on this specific asset
            logger.debug(
                f"Task '{task_id}' implements AssetTasksSPI. Checking compatibility..."
            )
            if not await task_instance.can_run_on_asset(asset):
                logger.warning(
                    f"Task '{task_id}' rejected asset '{asset.asset_id}' via can_run_on_asset."
                )
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Task '{task_id}' is not compatible with the selected asset (type: {asset.asset_type}).",
                )

            # 2. Standard Metadata Injection: Always provide a baseline context
            logger.debug(
                f"Performing standard metadata injection for task '{task_id}'."
            )

            # Always attach the asset object to the execution request
            # execution_request.asset = asset # REMOVED: asset object is no longer part of the DTO

            inputs = execution_request.inputs or {}
            if isinstance(inputs, dict):
                inputs.setdefault("asset_uri", str(asset.uri))
                inputs.setdefault("asset_id", asset.asset_id)
                # inputs.setdefault("asset_code", asset.asset_id) # Backwards compat
                inputs.setdefault("catalog_id", asset.catalog_id)
                inputs.setdefault("collection_id", asset.collection_id)
                inputs.setdefault("asset_type", asset.asset_type.value)
                execution_request.inputs = inputs

            # 3. Adaptation Layer: Let the SPI specialized logic further adapt or map fields
            logger.debug(
                f"Task '{task_id}' implements AssetTasksSPI. Adapting request..."
            )
            execution_request = await task_instance.get_execution_request(
                asset, execution_request
            )
        else:
            logger.warning(
                f"Task instance for '{task_id}' not found or doesn't implement SPI. Injection logic skipped."
            )

        try:
            # We assume 'task_id' here refers to the OGC Process ID
            result = await processes_module.execute_process(
                process_id=task_id,
                execution_request=execution_request,
                engine=engine,
                caller_id=caller_id,
                preferred_mode=mode,
                background_tasks=background_tasks,
            )
            # Handle result (Task object for async)
            from dynastore.modules.tasks.models import Task

            if isinstance(result, Task):
                # Return 201 with Location (we'd need to link to jobs which is in processes extension)
                # For now, let's return a basic status info
                return {
                    "jobID": str(result.task_id),
                    "status": result.status,
                    "message": "Task created successfully",
                }
            return result
        except Exception as e:
            logger.exception(f"Asset task execution failed: {e}")
            raise HTTPException(status_code=400, detail=str(e))

    # =============================================================================
    #  PARAMETRIC ASSET-PROCESS DISPATCH
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

    def _find_process(self, process_id: str) -> Optional[AssetProcessProtocol]:
        for p in get_protocols(AssetProcessProtocol):
            if getattr(p, "process_id", None) == process_id:
                return p
        return None

    async def list_catalog_asset_processes(
        self,
        catalog_id: str = Path(..., description="The catalog ID"),
        asset_id: str = Path(..., description="The asset ID"),
    ) -> List[AssetProcessDescriptor]:
        """Enumerates processes applicable to the asset (catalog scope)."""
        asset = await self._load_asset(catalog_id, asset_id, None)
        return [await p.describe(asset) for p in get_protocols(AssetProcessProtocol)]

    async def list_collection_asset_processes(
        self,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        asset_id: str = Path(..., description="The asset ID"),
    ) -> List[AssetProcessDescriptor]:
        """Enumerates processes applicable to the asset (collection scope)."""
        asset = await self._load_asset(catalog_id, asset_id, collection_id)
        return [await p.describe(asset) for p in get_protocols(AssetProcessProtocol)]

    async def _invoke_process(
        self,
        request: Request,
        catalog_id: str,
        asset_id: str,
        process_id: str,
        collection_id: Optional[str],
    ):
        asset = await self._load_asset(catalog_id, asset_id, collection_id)

        # Fallback for external-URL assets with no registered process: redirect to the URI.
        process = self._find_process(process_id)
        if process is None:
            if process_id == "download" and asset.uri.startswith(("http://", "https://")):
                from fastapi.responses import RedirectResponse

                return RedirectResponse(asset.uri, status_code=status.HTTP_302_FOUND)
            raise HTTPException(
                status_code=404,
                detail=f"Asset process {process_id!r} is not registered.",
            )

        try:
            params = await request.json()
        except Exception:
            params = {}
        if not isinstance(params, dict):
            raise HTTPException(
                status_code=400, detail="Request body must be a JSON object."
            )

        result = await process.execute(asset, params)

        if result.type == "redirect" and result.url:
            from fastapi.responses import RedirectResponse

            return RedirectResponse(result.url, status_code=status.HTTP_302_FOUND)
        return result

    async def invoke_catalog_asset_process(
        self,
        request: Request,
        catalog_id: str = Path(..., description="The catalog ID"),
        asset_id: str = Path(..., description="The asset ID"),
        process_id: str = Path(
            ...,
            description="The asset process ID (e.g. ``download``, ``ingest``).",
            examples=["download"],
        ),
    ):
        """Dispatches to the registered ``AssetProcessProtocol`` implementor."""
        return await self._invoke_process(
            request, catalog_id, asset_id, process_id, collection_id=None
        )

    async def invoke_collection_asset_process(
        self,
        request: Request,
        catalog_id: str = Path(..., description="The catalog ID"),
        collection_id: str = Path(..., description="The collection ID"),
        asset_id: str = Path(..., description="The asset ID"),
        process_id: str = Path(
            ...,
            description="The asset process ID (e.g. ``download``, ``ingest``).",
            examples=["download"],
        ),
    ):
        """Dispatches to the registered ``AssetProcessProtocol`` implementor."""
        return await self._invoke_process(
            request, catalog_id, asset_id, process_id, collection_id=collection_id
        )
