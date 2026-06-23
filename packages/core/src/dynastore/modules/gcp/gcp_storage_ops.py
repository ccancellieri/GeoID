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

import logging
from typing import Optional, Dict, Any

from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols import (
    ConfigsProtocol,
    CatalogsProtocol,
    UploadTicket,
    UploadStatus,
    UploadStatusResponse,
)

logger = logging.getLogger(__name__)


class GcpStorageOpsMixin:
    """Mixin providing StorageProtocol delegations and AssetUploadProtocol for GCPModule."""

    # AssetUploadProtocol surface
    driver_id: str = "gcs"
    supports_versioning: bool = False

    # --- Host interface stubs (provided by GCPModule) ---
    _upload_tickets: Dict[str, Dict[str, Any]]
    _credentials: Optional[Any]
    _storage_client: Optional[Any]

    def get_bucket_service(self) -> Any: ...
    def get_storage_client(self) -> Any: ...

    def upload_available(self) -> bool:
        """AssetUploadProtocol upload-readiness signal.

        Returns ``False`` when the GCP module never finished initialising
        (no usable credentials, or the storage client could not be built —
        the common on-premise / no-GCS-credentials case). The asset upload
        router uses this to skip GCS as the upload backend and fall through to
        a ready backend (e.g. local disk) instead of selecting GCS by
        registration order and then failing every ``initiate_upload`` with
        "GCPModule has not been initialized".

        Intentionally distinct from the module-wide ``is_available()``
        discovery gate, which stays ``True`` so the module remains discoverable
        as a ``StorageProtocol`` provider (runtime calls then raise a clear
        RuntimeError instead of silently resolving to None). This only scopes
        the readiness check to the upload role.
        """
        return self._credentials is not None and self._storage_client is not None

    # --- StorageProtocol Implementation ---

    async def get_storage_identifier(self, catalog_id: str) -> Optional[str]:
        """StorageProtocol: Returns the bucket name associated with a catalog."""
        return await self.get_bucket_service().get_storage_identifier(catalog_id)

    async def get_catalog_storage_path(self, catalog_id: str) -> Optional[str]:
        """StorageProtocol: Returns the storage path (e.g., gs://...) for a catalog."""
        return await self.get_bucket_service().get_catalog_storage_path(catalog_id)

    async def upload_file(
        self, source_path: str, target_path: str, content_type: Optional[str] = None
    ) -> str:
        """StorageProtocol: Uploads a local file to storage."""
        return await self.get_bucket_service().upload_file(
            source_path, target_path, content_type=content_type
        )

    async def upload_file_content(
        self, target_path: str, content: bytes, content_type: Optional[str] = None
    ) -> str:
        """StorageProtocol: Uploads content (bytes) directly to storage."""
        return await self.get_bucket_service().upload_file_content(
            target_path, content, content_type=content_type
        )

    async def download_file(self, source_path: str, target_path: str) -> None:
        """StorageProtocol: Downloads a file from storage to local."""
        return await self.get_bucket_service().download_file(source_path, target_path)

    async def file_exists(self, path: str) -> bool:
        """StorageProtocol: Checks if a file exists in storage."""
        return await self.get_bucket_service().file_exists(path)

    async def delete_file(self, path: str) -> None:
        """StorageProtocol: Deletes a file from storage."""
        return await self.get_bucket_service().delete_file(path)

    async def download_bytes_range(self, path: str, offset: int, length: int) -> bytes:
        """StorageProtocol: Efficient GCS byte-range download."""
        return await self.get_bucket_service().download_bytes_range(path, offset, length)

    async def ensure_storage_for_catalog(
        self, catalog_id: str, conn: Optional[Any] = None
    ) -> Optional[str]:
        """StorageProtocol: Ensures that storage exists for a catalog, creating it if it doesn't."""
        return await self.get_bucket_service().ensure_storage_for_catalog(
            catalog_id, conn=conn
        )

    async def drop_storage(
        self, catalog_id: str, conn: Optional[Any] = None
    ) -> bool:
        """StorageProtocol: Remove all binary storage resources for a catalog (bucket deletion)."""
        return await self.get_bucket_service().drop_storage(
            catalog_id, conn=conn
        )

    async def prepare_upload_target(
        self, catalog_id: str, collection_id: Optional[str] = None
    ):
        """
        Ensures that the target catalog and, if provided, collection exist before an
        operation like an upload. This will create them just-in-time if they don't exist.
        """
        return await self.get_bucket_service().prepare_upload_target(
            catalog_id, collection_id
        )

    async def get_collection_storage_path(
        self, catalog_id: str, collection_id: str
    ) -> Optional[str]:
        """Returns the GCS path for a collection's folder (e.g., gs://bucket-name/collections/my-collection/)."""
        return await self.get_bucket_service().get_collection_storage_path(
            catalog_id, collection_id
        )

    def generate_bucket_name(self, physical_id: str) -> str:
        """Generates the deterministic bucket name for a catalog.

        ``physical_id`` must be the immutable physical identifier resolved via
        ``CatalogsProtocol.resolve_physical_id`` before calling this method.
        """
        return self.get_bucket_service().generate_bucket_name(physical_id)

    # -------------------------------------------------------------------------
    # AssetUploadProtocol implementation
    # -------------------------------------------------------------------------

    async def initiate_upload(
        self,
        catalog_id: str,
        asset_def: "Any",
        filename: str,
        content_type: Optional[str] = None,
        collection_id: Optional[str] = None,
        origin: Optional[str] = None,
    ) -> "UploadTicket":
        """
        Prepares a GCS resumable upload session and returns a backend-agnostic
        ``UploadTicket``.  The client PUTs the file directly to ``upload_url``
        (a GCS signed resumable-session URI).  After the upload completes the
        GCS Pub/Sub OBJECT_FINALIZE event hits the inline finalize activator
        (``modules/gcp/gcp_finalize_activator.activate``) which transitions
        the PENDING ``assets`` row (born during ``initiate_upload``) to
        ACTIVE in a single transaction — no task hop.

        The ticket is stored in ``_upload_tickets`` so ``get_upload_status`` can
        later check whether the asset was registered.
        """
        from datetime import datetime, timezone, timedelta
        import google.api_core.exceptions
        from dynastore.modules.gcp.errors import (
            GcpFailedDependencyError,
            GcpInternalError,
            GcpServiceUnavailableError,
        )
        from dynastore.modules.gcp.tools import bucket as bucket_tool
        from dynastore.modules.gcp.gcp_config import (
            GcpCollectionBucketConfig,
        )
        from dynastore.tools.identifiers import generate_uuidv7

        catalogs_provider = get_protocol(CatalogsProtocol)
        config_provider = get_protocol(ConfigsProtocol)
        if not catalogs_provider:
            raise GcpServiceUnavailableError("Catalogs service unavailable.")

        # Ensure the target catalog/collection exist (JIT provisioning gate)
        await self.prepare_upload_target(
            catalog_id=catalog_id,
            collection_id=collection_id,
        )

        catalog = await catalogs_provider.get_catalog(catalog_id)
        if catalog.provisioning_status != "ready":
            if catalog.provisioning_status == "failed":
                raise GcpFailedDependencyError(
                    f"Catalog '{catalog_id}' provisioning failed; storage not available."
                )
            raise GcpServiceUnavailableError(
                f"Catalog '{catalog_id}' storage is still being provisioned. Retry shortly.",
                retry_after=30,
            )

        bucket_name = await self.get_storage_identifier(catalog_id)
        if not bucket_name:
            # The catalog claims ready but has no backing bucket — the stored
            # provisioning status is stale. Demote it (best-effort, must not mask
            # the error below) so the operator can see the broken state and
            # reprovision via POST /catalog/catalogs/{catalog_id}/reprovision.
            # ``catalogs_provider`` was already resolved and guarded above.
            try:
                await catalogs_provider.mark_provisioning_step(
                    catalog_id, "gcp_bucket", "failed"
                )
            except Exception as demote_exc:
                logger.warning(
                    "initiate_upload: could not demote catalog '%s' after "
                    "missing-bucket detection: %s",
                    catalog_id, demote_exc,
                )
            raise GcpFailedDependencyError(
                f"Catalog '{catalog_id}' is marked ready but its backing GCS bucket is "
                f"missing — the catalog has been flagged for re-provisioning. Re-run "
                f"provisioning via POST /catalog/catalogs/{catalog_id}/reprovision "
                f"(if eventing previously failed on a permission error, also ensure the "
                f"Pub/Sub IAM grant pubsub.topics.attachSubscription is in place first)."
            )

        storage_client = self.get_storage_client()
        bucket = storage_client.bucket(bucket_name)

        if collection_id:
            # Resolve the immutable physical_id for the collection so the GCS
            # object path remains stable even when the logical collection id
            # is renamed.
            _collection_physical_id: Optional[str] = None
            _catalogs_upload = get_protocol(CatalogsProtocol)
            if _catalogs_upload:
                _collection_physical_id = await _catalogs_upload.resolve_physical_id(
                    catalog_id, collection_id, allow_missing=True
                )
            blob_path = bucket_tool.get_blob_path_for_collection_file(
                _collection_physical_id or collection_id, filename
            )
        else:
            blob_path = bucket_tool.get_blob_path_for_catalog_file(filename)

        blob = bucket.blob(blob_path)

        # Build custom metadata — merge collection defaults with asset metadata
        final_metadata: Dict[str, Any] = dict(asset_def.metadata) if asset_def.metadata else {}
        if collection_id and config_provider:
            try:
                coll_cfg = await config_provider.get_config(
                    GcpCollectionBucketConfig, catalog_id, collection_id
                )
                if isinstance(coll_cfg, GcpCollectionBucketConfig) and coll_cfg.custom_metadata_defaults:
                    final_metadata = {**coll_cfg.custom_metadata_defaults, **final_metadata}
            except Exception:
                pass  # config is optional — proceed without it

        final_metadata["asset_id"] = asset_def.asset_id
        final_metadata["asset_type"] = asset_def.asset_type.value if hasattr(asset_def.asset_type, "value") else str(asset_def.asset_type)
        # TODO: Consider making content_type injection configurable via GcpCollectionBucketConfig
        # if content_type:
        #     final_metadata["content_type"] = content_type
        blob.metadata = {k: str(v) if not isinstance(v, str) else v for k, v in final_metadata.items()}

        try:
            # The Origin supplied here CORS-stamps the resumable session:
            # GCS only adds Access-Control-Allow-Origin to responses on the
            # session URI when the initiating POST carried a matching Origin
            # header (bucket CORS config does not apply to session URIs).
            session_uri = blob.create_resumable_upload_session(
                content_type=content_type or "application/octet-stream",
                origin=origin,
            )
        except google.api_core.exceptions.GoogleAPICallError as e:
            raise GcpServiceUnavailableError(
                f"GCS upload session creation failed: {e}"
            ) from e

        if not session_uri:
            raise GcpInternalError(
                "GCS did not return a session URI for resumable upload."
            )

        ticket_id = str(generate_uuidv7())
        expires_at = datetime.now(timezone.utc) + timedelta(hours=1)

        self._upload_tickets[ticket_id] = {
            "asset_id": asset_def.asset_id,
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "expires_at": expires_at,
        }

        logger.info(
            f"GCPModule.initiate_upload: ticket '{ticket_id}' created for asset "
            f"'{asset_def.asset_id}' in catalog '{catalog_id}'."
        )

        return UploadTicket(
            ticket_id=ticket_id,
            upload_url=session_uri,
            method="PUT",
            headers={"Content-Type": content_type or "application/octet-stream"},
            expires_at=expires_at,
            backend="gcs",
        )

    async def get_upload_status(
        self,
        ticket_id: str,
        catalog_id: str,
    ) -> "UploadStatusResponse":
        """
        Polls the status of an upload session.

        Status is determined by checking whether the asset row has been
        promoted to ACTIVE (the inline finalize activator does this when the
        Pub/Sub OBJECT_FINALIZE notification arrives).
        The ticket is removed from the in-memory store once the upload is
        confirmed complete or the session has expired.
        """
        from datetime import datetime, timezone
        from dynastore.modules import get_protocol
        from dynastore.models.protocols import AssetsProtocol

        ticket = self._upload_tickets.get(ticket_id)
        if not ticket:
            raise ValueError(f"Upload ticket '{ticket_id}' not found or expired.")

        # Expire stale tickets
        if datetime.now(timezone.utc) > ticket["expires_at"]:
            del self._upload_tickets[ticket_id]
            raise ValueError(f"Upload ticket '{ticket_id}' has expired.")

        asset_id = ticket["asset_id"]
        ticket_catalog_id = ticket["catalog_id"]
        collection_id = ticket.get("collection_id")

        # Verify the caller is querying the right catalog
        if ticket_catalog_id != catalog_id:
            raise ValueError(
                f"Upload ticket '{ticket_id}' not found in catalog '{catalog_id}'."
            )

        assets = get_protocol(AssetsProtocol)
        if not assets:
            return UploadStatusResponse(
                ticket_id=ticket_id,
                status=UploadStatus.PENDING,
            )

        asset = await assets.get_asset(
            asset_id=asset_id,
            catalog_id=catalog_id,
            collection_id=collection_id,
        )

        if asset:
            # Asset registered — clean up the ticket
            del self._upload_tickets[ticket_id]
            return UploadStatusResponse(
                ticket_id=ticket_id,
                status=UploadStatus.COMPLETED,
                asset_id=asset_id,
            )

        return UploadStatusResponse(
            ticket_id=ticket_id,
            status=UploadStatus.PENDING,
        )
