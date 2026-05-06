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
Local-disk upload backend — server-side file receive + asset registration.

Upload flow
-----------
1. Client calls ``POST /assets/catalogs/{id}/upload`` (or the collection variant).
2. ``assets_service.py`` resolves the upload backend via
   ``router.get_asset_upload_driver(catalog_id, collection_id)`` (which honours
   ``AssetRoutingConfig.operations[UPLOAD]`` and falls back to the first-
   registered backend) and calls ``initiate_upload`` → returns
   ``UploadTicket(backend="local", method="POST",
   upload_url="/local-upload/{ticket_id}")``.
3. Client POSTs the file (multipart) to ``/local-upload/{ticket_id}``.
4. The endpoint streams bytes to ``STAGING_DIR/{ticket_id}/{filename}``, then calls
   ``complete_upload(ticket_id)`` synchronously:
   - moves the file to ``ASSET_DIR/{catalog_id}/{filename}``
   - registers the asset via ``AssetsProtocol.create_asset(owned_by="local")``
   - marks the ticket COMPLETED
5. Client calls ``GET /assets/catalogs/{id}/upload/{ticket_id}/status`` → COMPLETED.

Configuration (environment variables)
--------------------------------------
``LOCAL_UPLOAD_STAGING_DIR``
    Temporary directory for in-flight uploads.
    Default: ``/tmp/dynastore/uploads/staging``
``LOCAL_ASSET_DIR``
    Permanent storage root for registered local assets.
    Default: ``/tmp/dynastore/assets``
"""

import logging
import os
import shutil
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional

from dynastore.modules.protocols import ModuleProtocol

logger = logging.getLogger(__name__)

_TICKET_TTL_HOURS = 1


class LocalUploadModule(ModuleProtocol):
    """
    Implements ``AssetUploadProtocol`` for on-premise / local-disk deployments.

    The client delivers the file to a server-side endpoint (``POST
    /local-upload/{ticket_id}``); asset registration is synchronous — by the time
    the POST response is returned the asset exists in the catalog and the ticket
    status is already ``COMPLETED``.

    An instance of this class is registered as a plugin during ``lifespan`` so
    it appears in ``AssetRoutingConfig.operations[UPLOAD]`` (auto-augmented
    from discoverable ``AssetUploadProtocol`` impls). Operators select it for
    a given catalog by pinning ``"LocalUploadModule"`` in routing config.
    The upload endpoint is added directly to the FastAPI app during the same
    lifespan phase.
    """

    # Non-foundational: a missing python-multipart or path-write failure
    # should disable local upload but never abort the catalog.
    priority: int = 50

    def __init__(
        self,
        staging_dir: Optional[str] = None,
        asset_dir: Optional[str] = None,
    ) -> None:
        self._staging_root = Path(
            staging_dir
            or os.environ.get("LOCAL_UPLOAD_STAGING_DIR", "/tmp/dynastore/uploads/staging")
        )
        self._asset_root = Path(
            asset_dir
            or os.environ.get("LOCAL_ASSET_DIR", "/tmp/dynastore/assets")
        )
        # ticket_id → {asset_id, catalog_id, collection_id, filename, expires_at, status, asset_id_result}
        self._upload_tickets: Dict[str, Dict[str, Any]] = {}

    # -------------------------------------------------------------------------
    # Lifespan
    # -------------------------------------------------------------------------

    @asynccontextmanager
    async def lifespan(self, app_state: Any) -> AsyncIterator[None]:
        from dynastore.tools.discovery import register_plugin, unregister_plugin
        from dynastore.modules.local.asset_processes import LocalDownloadAssetProcess

        self._staging_root.mkdir(parents=True, exist_ok=True)
        self._asset_root.mkdir(parents=True, exist_ok=True)
        logger.info(
            f"LocalUploadModule: staging={self._staging_root}, assets={self._asset_root}"
        )

        # Modules receive ``app.state`` (a starlette State / SimpleNamespace),
        # not the FastAPI app itself. ``main.py`` exposes the app via
        # ``app.state.app`` precisely so route-registering modules like this
        # one can reach it. Skip route registration if absent (worker
        # contexts use ``SimpleNamespace`` with no .app — uploads aren't
        # meaningful there).
        fastapi_app = getattr(app_state, "app", None)
        if fastapi_app is not None:
            self._register_route(fastapi_app)
            self._register_download_route(fastapi_app)
        else:
            logger.warning(
                "LocalUploadModule: app.state.app missing; skipping HTTP "
                "route registration (worker context?)."
            )

        # Register the local download asset process so the parametric asset
        # router discovers it via get_protocols(AssetProcessProtocol).
        self._download_process = LocalDownloadAssetProcess()
        register_plugin(self)
        register_plugin(self._download_process)
        try:
            yield
        finally:
            unregister_plugin(self._download_process)
            unregister_plugin(self)
            logger.info("LocalUploadModule: unregistered.")

    def _register_route(self, app: Any) -> None:
        """Adds ``POST /local-upload/{ticket_id}`` to the FastAPI app.

        FastAPI's multipart support requires ``python-multipart``; without it
        ``UploadFile = File(...)`` route registration raises at definition
        time. Skip cleanly so deployments that don't ship multipart (most
        slim API-only scopes) keep working — local upload simply isn't
        available there.
        """
        try:
            import multipart  # noqa: F401  — availability probe only
        except ImportError:
            logger.warning(
                "LocalUploadModule: python-multipart not installed — skipping "
                "POST /local-upload/{ticket_id} registration. Install the "
                "extra to enable local file uploads."
            )
            return

        from fastapi import File, HTTPException, UploadFile, status as http_status
        from dynastore.models.protocols import UploadStatusResponse, UploadStatus

        async def receive_upload(
            ticket_id: str,
            file: UploadFile = File(..., description="File to upload."),
        ) -> UploadStatusResponse:
            """
            Receives the file for a previously initiated local upload session.

            Streams the bytes to staging, registers the asset synchronously, and
            returns ``UploadStatusResponse(status=completed)`` immediately.
            """
            ticket = self._upload_tickets.get(ticket_id)
            if not ticket:
                raise HTTPException(
                    status_code=http_status.HTTP_404_NOT_FOUND,
                    detail=f"Upload ticket '{ticket_id}' not found or expired.",
                )
            if datetime.now(timezone.utc) > ticket["expires_at"]:
                self._upload_tickets.pop(ticket_id, None)
                raise HTTPException(
                    status_code=http_status.HTTP_404_NOT_FOUND,
                    detail=f"Upload ticket '{ticket_id}' has expired.",
                )

            ticket["status"] = UploadStatus.UPLOADING

            # 1. Stream to staging
            staging_path = self._staging_root / ticket_id / ticket["filename"]
            staging_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with staging_path.open("wb") as fh:
                    while chunk := await file.read(1 << 20):  # 1 MiB chunks
                        fh.write(chunk)
            except Exception as exc:
                ticket["status"] = UploadStatus.FAILED
                logger.error(f"LocalUploadModule: write to staging failed: {exc}")
                raise HTTPException(
                    status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to write upload to staging: {exc}",
                )

            # 2. Register asset (moves file + creates DB record)
            try:
                asset = await self._complete_upload(ticket_id, staging_path, ticket)
            except Exception as exc:
                ticket["status"] = UploadStatus.FAILED
                logger.error(f"LocalUploadModule: complete_upload failed: {exc}")
                raise HTTPException(
                    status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Asset registration failed: {exc}",
                )

            return UploadStatusResponse(
                ticket_id=ticket_id,
                status=UploadStatus.COMPLETED,
                asset_id=asset.asset_id,
            )

        app.add_api_route(
            "/local-upload/{ticket_id}",
            receive_upload,
            methods=["POST"],
            response_model=UploadStatusResponse,
            summary="Receive Local Upload",
            description=(
                "Server-side file receive endpoint for local-disk upload sessions. "
                "Stream the file as ``multipart/form-data``. "
                "The asset is registered synchronously — the response is already "
                "``status=completed`` when this endpoint returns."
            ),
            tags=["Assets"],
        )
        logger.info("LocalUploadModule: registered POST /local-upload/{ticket_id}")

    def _register_download_route(self, app: Any) -> None:
        """Adds ``GET /local-download/{catalog_id}/{asset_id}`` to the FastAPI app.

        Streams the underlying file referenced by the asset row to the
        client. Authentication is provided by the global ``auth`` extension's
        middleware (same protection model as ``POST /local-upload/...``); no
        signed URLs are needed because bytes never leave the auth perimeter.

        The asset's URI must start with ``file://`` and resolve to a path
        under :attr:`_asset_root` (path-traversal guard).
        """
        from fastapi import HTTPException, status as http_status
        from fastapi.responses import FileResponse
        from urllib.parse import unquote, urlparse

        asset_root = self._asset_root.resolve()

        async def serve_local_asset(catalog_id: str, asset_id: str) -> FileResponse:
            from dynastore.tools.discovery import get_protocol
            from dynastore.models.protocols import AssetsProtocol

            assets = get_protocol(AssetsProtocol)
            if not assets:
                raise HTTPException(
                    status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="AssetsProtocol not available.",
                )

            asset = await assets.get_asset(
                catalog_id=catalog_id, asset_id=asset_id, collection_id=None,
            )
            if asset is None:
                raise HTTPException(
                    status_code=http_status.HTTP_404_NOT_FOUND,
                    detail=f"Asset '{asset_id}' not found in catalog '{catalog_id}'.",
                )
            if asset.owned_by != "local" or not (asset.uri and asset.uri.startswith("file://")):
                raise HTTPException(
                    status_code=http_status.HTTP_409_CONFLICT,
                    detail="Asset is not locally owned.",
                )

            parsed = urlparse(asset.uri)
            file_path = Path(unquote(parsed.path)).resolve()
            try:
                file_path.relative_to(asset_root)
            except ValueError:
                logger.error(
                    "LocalUploadModule: refusing to serve %s — outside asset root %s",
                    file_path, asset_root,
                )
                raise HTTPException(
                    status_code=http_status.HTTP_403_FORBIDDEN,
                    detail="Asset path is outside the local asset store.",
                )
            if not file_path.is_file():
                raise HTTPException(
                    status_code=http_status.HTTP_404_NOT_FOUND,
                    detail=f"Local file backing asset '{asset_id}' not found on disk.",
                )

            return FileResponse(
                path=str(file_path),
                filename=file_path.name,
            )

        app.add_api_route(
            "/local-download/{catalog_id}/{asset_id}",
            serve_local_asset,
            methods=["GET"],
            summary="Serve Local Asset",
            description=(
                "Streams the local file backing the asset. Used as the redirect "
                "target of ``LocalDownloadAssetProcess.execute()``. Auth is "
                "provided by the global ``auth`` middleware."
            ),
            tags=["Assets"],
        )
        logger.info(
            "LocalUploadModule: registered GET /local-download/{catalog_id}/{asset_id}"
        )

    # -------------------------------------------------------------------------
    # AssetUploadProtocol implementation
    # -------------------------------------------------------------------------

    async def initiate_upload(
        self,
        catalog_id: str,
        asset_def: Any,
        filename: str,
        content_type: Optional[str] = None,
        collection_id: Optional[str] = None,
    ) -> Any:
        """
        Reserves a staging slot and returns an ``UploadTicket`` directing the
        client to ``POST /local-upload/{ticket_id}``.

        Unlike GCS, no cloud storage is provisioned — the ticket is ready
        immediately.
        """
        from dynastore.models.protocols import UploadTicket

        ticket_id = str(uuid.uuid4())
        expires_at = datetime.now(timezone.utc) + timedelta(hours=_TICKET_TTL_HOURS)

        # Pre-create the staging directory so the receive endpoint can write
        staging_dir = self._staging_root / ticket_id
        staging_dir.mkdir(parents=True, exist_ok=True)

        from dynastore.models.protocols import UploadStatus

        self._upload_tickets[ticket_id] = {
            "asset_id": asset_def.asset_id,
            "catalog_id": catalog_id,
            "collection_id": collection_id,
            "filename": filename,
            "content_type": content_type or "application/octet-stream",
            "asset_def": asset_def,
            "expires_at": expires_at,
            "status": UploadStatus.PENDING,
        }

        logger.info(
            f"LocalUploadModule.initiate_upload: ticket '{ticket_id}' created "
            f"for asset '{asset_def.asset_id}' in catalog '{catalog_id}'."
        )

        headers: Dict[str, str] = {}
        if content_type:
            headers["Content-Type"] = content_type

        return UploadTicket(
            ticket_id=ticket_id,
            upload_url=f"/local-upload/{ticket_id}",
            method="POST",
            headers=headers,
            expires_at=expires_at,
            backend="local",
        )

    async def get_upload_status(
        self,
        ticket_id: str,
        catalog_id: str,
    ) -> Any:
        """
        Returns the current status of a local upload session.

        Because ``complete_upload`` is called synchronously inside the POST
        endpoint, the status is typically already ``COMPLETED`` by the time the
        client polls this endpoint.
        """
        from fastapi import HTTPException, status as http_status
        from dynastore.models.protocols import UploadStatus, UploadStatusResponse

        ticket = self._upload_tickets.get(ticket_id)
        if not ticket:
            raise HTTPException(
                status_code=http_status.HTTP_404_NOT_FOUND,
                detail=f"Upload ticket '{ticket_id}' not found or expired.",
            )

        if datetime.now(timezone.utc) > ticket["expires_at"]:
            self._upload_tickets.pop(ticket_id, None)
            raise HTTPException(
                status_code=http_status.HTTP_404_NOT_FOUND,
                detail=f"Upload ticket '{ticket_id}' has expired.",
            )

        if ticket["catalog_id"] != catalog_id:
            raise HTTPException(
                status_code=http_status.HTTP_404_NOT_FOUND,
                detail=f"Upload ticket '{ticket_id}' not found in catalog '{catalog_id}'.",
            )

        current_status = ticket.get("status", UploadStatus.PENDING)

        if current_status == UploadStatus.COMPLETED:
            asset_id = ticket.get("asset_id_result") or ticket["asset_id"]
            self._upload_tickets.pop(ticket_id, None)
            return UploadStatusResponse(
                ticket_id=ticket_id,
                status=UploadStatus.COMPLETED,
                asset_id=asset_id,
            )

        if current_status == UploadStatus.FAILED:
            return UploadStatusResponse(
                ticket_id=ticket_id,
                status=UploadStatus.FAILED,
                error=ticket.get("error", "Upload failed."),
            )

        return UploadStatusResponse(
            ticket_id=ticket_id,
            status=current_status,
        )

    # -------------------------------------------------------------------------
    # Internal — called synchronously from the receive endpoint
    # -------------------------------------------------------------------------

    async def _complete_upload(
        self,
        ticket_id: str,
        staging_path: Path,
        ticket: Dict[str, Any],
    ) -> Any:
        """
        Moves the staged file to permanent storage and registers the asset.

        Not part of ``AssetUploadProtocol`` — called by the upload endpoint
        immediately after streaming is complete.
        """
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols import AssetsProtocol, UploadStatus
        from dynastore.modules.catalog.asset_service import AssetCreate, AssetTypeEnum

        catalog_id: str = ticket["catalog_id"]
        collection_id: Optional[str] = ticket.get("collection_id")
        filename: str = ticket["filename"]
        asset_def = ticket["asset_def"]

        # Determine permanent path: {asset_root}/{catalog_id}/{collection or _catalog_tier}/{filename}
        # The on-disk segment for catalog-tier assets uses a stable internal
        # marker so two catalog-tier filenames don't collide; the marker
        # never reaches DTOs or the DB (which use NULL collection_id).
        scope = collection_id or "_catalog_tier"
        permanent_dir = self._asset_root / catalog_id / scope
        permanent_dir.mkdir(parents=True, exist_ok=True)
        permanent_path = permanent_dir / filename

        # If a file with the same name already exists, overwrite (idempotent re-upload)
        shutil.move(str(staging_path), str(permanent_path))

        # Clean up the empty staging subdirectory
        try:
            staging_path.parent.rmdir()
        except OSError:
            pass  # Non-empty or already gone — ignore

        uri = permanent_path.as_uri()  # file:///...

        assets = get_protocol(AssetsProtocol)
        if not assets:
            raise RuntimeError("AssetsProtocol not available for local asset registration.")

        asset_type = asset_def.asset_type if hasattr(asset_def, "asset_type") else AssetTypeEnum.ASSET
        metadata = dict(asset_def.metadata) if hasattr(asset_def, "metadata") and asset_def.metadata else {}

        asset_base = AssetCreate(
            asset_id=asset_def.asset_id,
            filename=filename,
            uri=uri,
            asset_type=asset_type,
            metadata=metadata,
            owned_by="local",
        )

        asset = await assets.create_asset(
            catalog_id=catalog_id,
            asset=asset_base,
            collection_id=collection_id,
        )

        ticket["status"] = UploadStatus.COMPLETED
        ticket["asset_id_result"] = asset.asset_id
        logger.info(
            f"LocalUploadModule: asset '{asset.asset_id}' registered at {uri} "
            f"(catalog={catalog_id}, collection={collection_id})."
        )
        return asset
