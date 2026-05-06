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

"""
Asset upload protocol definitions.

Overview
--------
``AssetUploadProtocol`` is a backend-agnostic interface for initiating and
polling asset uploads.  Implementations exist for every storage backend; the
client code and the REST API layer use only this contract and remain ignorant
of the underlying storage technology.

Upload flows
~~~~~~~~~~~~
**Event-driven backends (GCS, S3)**::

    from dynastore.modules.storage.router import get_asset_upload_driver
    from dynastore.modules.catalog.asset_service import AssetUploadDefinition, AssetTypeEnum

    # Routed lookup: respects AssetRoutingConfig.operations[UPLOAD] per
    # catalog/collection. Falls back to first-registered backend when no
    # UPLOAD entry resolves (matches the legacy get_protocol behaviour).
    upload = await get_asset_upload_driver(catalog_id="imagery_catalog")

    ticket = await upload.initiate_upload(
        catalog_id="imagery_catalog",
        asset_def=AssetUploadDefinition(
            asset_id="LC09_198030_20251225",
            asset_type=AssetTypeEnum.RASTER,
            metadata={"sensor": "OLI-2", "cloud_cover": 5.2},
        ),
        filename="LC09_L1TP_198030_20251225_02_T1.tif",
        content_type="image/tiff",
        collection_id="landsat_scenes",
    )
    # ticket.method  = "PUT"
    # ticket.upload_url = "https://storage.googleapis.com/...?upload_id=..."
    # ticket.headers = {"Content-Type": "image/tiff"}
    # ticket.backend = "gcs"

    # Client PUTs the file directly to GCS (no server hop):
    #   PUT ticket.upload_url
    #   headers: ticket.headers
    #   body: <file bytes>

    # GCS fires OBJECT_FINALIZE → GcsStorageEventTask → create_asset(owned_by="gcs")

    # Poll until status == "completed":
    from dynastore.models.protocols import UploadStatus
    response = await upload.get_upload_status(
        ticket_id=ticket.ticket_id,
        catalog_id="imagery_catalog",
    )
    assert response.status == UploadStatus.COMPLETED
    print("Registered asset_id:", response.asset_id)

**Server-side backends (local disk)**::

    ticket = await upload.initiate_upload(
        catalog_id="field_data",
        asset_def=AssetUploadDefinition(
            asset_id="gadm_adm2_italy",
            asset_type=AssetTypeEnum.VECTORIAL,
            metadata={"source": "GADM", "version": "4.1"},
        ),
        filename="gadm_adm2_italy.geojson",
        content_type="application/geo+json",
    )
    # ticket.method     = "POST"
    # ticket.upload_url = "/local-upload/<ticket_id>"
    # ticket.backend    = "local"

    # Client POSTs the file to the local proxy endpoint.
    # The endpoint streams to staging, calls complete_upload() synchronously,
    # and the asset is registered immediately → status is already COMPLETED.

REST API equivalent
~~~~~~~~~~~~~~~~~~~
::

    # Catalog-level upload:
    POST /assets/catalogs/{catalog_id}/upload
    {
        "filename": "scene.tif",
        "content_type": "image/tiff",
        "asset": {"asset_id": "scene_001", "asset_type": "RASTER", "metadata": {}}
    }
    # → 200 UploadTicket

    # Collection-level upload:
    POST /assets/catalogs/{catalog_id}/collections/{collection_id}/upload

    # Poll status:
    GET /assets/catalogs/{catalog_id}/upload/{ticket_id}/status
    # → 200 UploadStatusResponse  (status: pending | uploading | completed | failed)

``complete_upload`` is intentionally **not** part of this protocol — it is
driver-internal and triggered differently per backend.  The client only needs
to initiate the session and optionally poll its status.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Dict, Optional, Protocol, runtime_checkable

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from dynastore.modules.catalog.asset_service import Asset, AssetUploadDefinition


class UploadStatus(str, Enum):
    """Lifecycle states of an upload session."""

    PENDING = "pending"
    """Upload session created; client has not yet started delivering the file."""

    UPLOADING = "uploading"
    """File transfer is in progress (applicable to resumable/multipart uploads)."""

    COMPLETED = "completed"
    """File received and asset registered in the catalog."""

    FAILED = "failed"
    """Upload or asset registration failed; see error details if available."""

    CANCELLED = "cancelled"
    """Upload was explicitly cancelled by the client or the ticket expired."""


class UploadTicket(BaseModel):
    """
    Backend-agnostic upload descriptor returned to the client.

    The client uses ``upload_url``, ``method``, and ``headers`` to deliver the
    file directly to the appropriate storage backend:

    * **GCS / S3** — PUT to the signed resumable URL.
    * **Local / HTTP proxy** — POST multipart to the server-side endpoint.

    After delivering the file the client may poll
    ``GET /assets/catalogs/{id}/upload/{ticket_id}/status`` to confirm the
    asset has been registered (required for event-driven backends).
    """

    ticket_id: str = Field(
        ...,
        description=(
            "Unique opaque identifier for this upload session. "
            "Use it to poll the upload status endpoint."
        ),
    )
    upload_url: str = Field(
        ...,
        description=(
            "Target URL for the file upload. "
            "For GCS/S3 this is a pre-signed URL that expires at ``expires_at``. "
            "For local/HTTP backends this is a server-side proxy path."
        ),
    )
    method: str = Field(
        ...,
        description="HTTP method to use when delivering the file.",
        examples=["PUT", "POST"],
    )
    headers: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "HTTP headers that MUST be included in the upload request "
            "(e.g. ``Content-Type``, ``x-goog-resumable``)."
        ),
    )
    expires_at: datetime = Field(
        ...,
        description=(
            "UTC timestamp after which the upload URL / session is no longer valid. "
            "Initiate a new upload if this time has passed."
        ),
    )
    backend: str = Field(
        ...,
        description="Storage backend that issued this ticket.",
        examples=["gcs", "s3", "local", "http"],
    )


class UploadStatusResponse(BaseModel):
    """Response model for the upload status polling endpoint."""

    ticket_id: str = Field(..., description="The upload session identifier.")
    status: UploadStatus = Field(..., description="Current lifecycle state of the upload.")
    asset_id: Optional[str] = Field(
        default=None,
        description="Asset ID once registration is complete (``status=completed``).",
    )
    error: Optional[str] = Field(
        default=None,
        description="Human-readable error message when ``status=failed``.",
    )


@runtime_checkable
class AssetUploadProtocol(Protocol):
    """
    Backend-agnostic protocol for initiating asset uploads.

    Implementations handle the backend-specific details (signed URLs, staging
    paths, quota checks) while exposing a uniform interface to the API layer.

    Two implementations ship in-tree:

    * ``GcpStorageOpsMixin`` (``modules/gcp/gcp_storage_ops.py``) —
      ``driver_id="gcs"``, GCS resumable signed URLs, asynchronous activation
      via the ``OBJECT_FINALIZE`` Pub/Sub event handled inline by
      ``extensions/gcp/gcp_events.py``.
    * ``LocalUploadModule`` (``modules/local/local_upload.py``) —
      ``driver_id="local"``, server-side staging directory, synchronous
      activation when the client POSTs to the staging endpoint.

    Driver selection: ``modules/storage/router.get_asset_upload_driver()``
    consults ``AssetRoutingConfig.operations[UPLOAD]`` (auto-augmented with
    every discoverable ``AssetUploadProtocol`` impl) and returns the first
    matching backend. With no per-catalog routing configured, GCP wins when
    the GCP module is installed (registration order, GCP module lifespan
    runs before ``LocalUploadModule``); otherwise Local. ``None`` only when
    no backend is registered at all — the REST layer surfaces this as 503.

    Asset registration is **not** owned by this protocol. Drivers mint upload
    URLs and accept bytes; the assets table is owned by the catalog layer
    (``upsert_asset`` for the policy-gated PENDING insert at upload-create
    time, and the inline Pub/Sub finalize handler for the PENDING→ACTIVE
    transition). ``complete_upload`` is intentionally not part of this
    protocol.
    """

    driver_id: str
    """Stable, lowercase, snake-case identifier for this backend.

    Used by ``AssetRoutingConfig.operations[UPLOAD]`` for explicit per-catalog
    routing and by REST clients for diagnostic purposes. Examples: ``"gcs"``,
    ``"local"``.
    """

    supports_versioning: bool
    """Whether this backend can preserve a previous version of an asset's
    bytes when a new upload completes for the same logical asset.

    Read by ``AssetsWritePolicy`` when ``on_conflict=NEW_VERSION``: the
    chain runner refuses with ``reason="versioning_unsupported"`` if a
    matching policy is configured but the resolved driver returns False
    here. Defaults to ``False`` for both shipped impls — Stage 6's
    reconcile path is the recovery surface for restoring an older blob.
    """

    async def initiate_upload(
        self,
        catalog_id: str,
        asset_def: "AssetUploadDefinition",
        filename: str,
        content_type: Optional[str] = None,
        collection_id: Optional[str] = None,
    ) -> UploadTicket:
        """
        Prepares an upload session and returns a ticket the client uses to
        deliver the file.

        The implementing backend is responsible for:

        * Ensuring the target catalog / collection exist (JIT creation if needed).
        * Generating a signed URL or reserving a staging path.
        * Storing the ticket so ``get_upload_status`` can serve it.
        * Setting ``owned_by`` on the asset when registering it after upload.

        Args:
            catalog_id: Catalog that will own the asset.
            asset_def: Asset metadata (id, type, custom metadata).  The URI is
                not yet known at this point — it will be set by the backend when
                the file is received.
            filename: Original filename (used to derive the storage path and
                content-type hints).
            content_type: MIME type of the file being uploaded.
            collection_id: Optional collection scope for the asset.

        Returns:
            ``UploadTicket`` containing the upload URL and instructions.

        Raises:
            HTTPException 503: If the storage backend is not available.
            HTTPException 424: If the catalog storage provisioning failed.
            HTTPException 503 (Retry-After): If still provisioning.
        """
        ...

    async def get_upload_status(
        self,
        ticket_id: str,
        catalog_id: str,
    ) -> UploadStatusResponse:
        """
        Returns the current status of an upload session.

        For event-driven backends (GCS, S3) the status transitions
        ``pending → uploading → completed`` asynchronously after the cloud
        event fires and the asset is registered.

        For server-side backends (local, HTTP) the status is ``completed``
        immediately after the server receives the file.

        Args:
            ticket_id: The ticket identifier returned by ``initiate_upload``.
            catalog_id: Catalog scope (used to resolve tenant context).

        Returns:
            ``UploadStatusResponse`` with the current state and optional asset ID.

        Raises:
            HTTPException 404: If the ticket is not found or has expired.
        """
        ...
