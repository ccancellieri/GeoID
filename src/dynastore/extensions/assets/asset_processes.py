"""Cross-backend asset processes (Phase 3).

The ``download`` processes are backend-owned (GCS knows V4 signing, local
knows the bearer-auth route) and live in ``modules/{gcp,local}/asset_processes.py``.

The ``upload`` process is backend-agnostic — it asks the routed
``AssetUploadProtocol`` driver for an :class:`UploadTicket` and folds the
ticket into an :class:`AssetProcessOutput`. Same OGC-Processes-aligned
``/processes/{id}/execution`` surface, no per-backend conditional.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, cast

from fastapi import HTTPException, status

from dynastore.models.protocols.asset_process import (
    AssetProcessDescriptor,
    AssetProcessOutput,
    HTTPMethod,
)
from dynastore.modules.catalog.asset_service import Asset

logger = logging.getLogger(__name__)


class UploadAssetProcess:
    """``upload`` process — symmetric mirror of the ``download`` processes.

    Implementation duck-types ``AssetProcessProtocol``. Routes through
    :func:`dynastore.modules.storage.router.get_asset_upload_driver` so the
    same per-catalog UPLOAD routing slot that powers ``POST /catalogs/{id}/upload``
    drives this surface too — no GCS/local branching here.
    """

    process_id = "upload"
    http_method: HTTPMethod = "POST"

    async def describe(self, asset: Asset) -> AssetProcessDescriptor:
        # Applicable when the asset has been registered but has no bytes yet
        # (uri empty/None). Re-uploading bytes for an existing asset is a
        # separate operation handled via ``PUT /assets/{id}`` style flows.
        applicable = not bool(asset.uri)
        return AssetProcessDescriptor(
            process_id=self.process_id,
            title="Upload",
            description=(
                "Returns the routed UploadTicket (signed URL or local-upload "
                "endpoint) the client must PUT/POST the file bytes to. Picks "
                "the backend from AssetRoutingConfig.operations[UPLOAD]."
            ),
            http_method=self.http_method,
            applicable=applicable,
            reason=(
                None if applicable
                else "Asset already has a uri; re-upload not supported here."
            ),
            parameters_schema={
                "type": "object",
                "properties": {
                    "filename": {
                        "type": "string",
                        "description": "Override filename for the upload ticket.",
                    },
                    "content_type": {
                        "type": "string",
                        "description": "Override MIME type.",
                    },
                },
                "additionalProperties": False,
            },
        )

    async def execute(
        self, asset: Asset, params: Dict[str, Any]
    ) -> AssetProcessOutput:
        if asset.uri:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    "upload process not applicable: asset already bound to a "
                    f"uri ({asset.uri!r})."
                ),
            )

        from dynastore.modules.catalog.asset_service import (
            AssetUploadDefinition,
        )
        from dynastore.modules.storage.router import get_asset_upload_driver

        provider = await get_asset_upload_driver(
            asset.catalog_id, asset.collection_id,
        )
        if provider is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=(
                    "No upload backend registered for "
                    f"{asset.catalog_id}/{asset.collection_id or '<catalog-tier>'}."
                ),
            )

        filename = str(params.get("filename") or asset.asset_id)
        content_type = params.get("content_type") or "application/octet-stream"
        asset_def = AssetUploadDefinition(
            asset_id=asset.asset_id,
            asset_type=asset.asset_type,
            metadata=asset.metadata or {},
        )

        ticket = await provider.initiate_upload(
            catalog_id=asset.catalog_id,
            asset_def=asset_def,
            filename=filename,
            content_type=content_type,
            collection_id=asset.collection_id,
        )

        # UploadTicket.method is a free str ("PUT" / "POST"); narrow for the
        # Literal constraint on AssetProcessOutput.method.
        method_str = (ticket.method or "POST").upper()
        if method_str not in ("GET", "POST", "PUT", "DELETE"):
            method_str = "POST"
        return AssetProcessOutput(
            type="signed_url",
            url=ticket.upload_url,
            method=cast(HTTPMethod, method_str),
            headers=dict(ticket.headers or {}),
            expires_at=ticket.expires_at.isoformat() if ticket.expires_at else None,
        )
