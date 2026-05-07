"""Local-disk asset processes.

Mirrors :mod:`dynastore.modules.gcp.asset_processes` for the local backend.
Currently exposes a single process:

* ``download`` — for assets whose ``owned_by == "local"`` and whose ``uri``
  is a ``file://`` path. Returns a redirect to the bearer-auth-protected
  ``GET /local-download/{catalog_id}/{asset_id}`` route registered by
  :class:`LocalUploadModule`.

Local bytes never leave the auth perimeter, so no signed-URL machinery is
needed (no HMAC, no key storage, no rotation, no clock-skew TTL).
Authentication is enforced by the ``auth`` extension's middleware on the
download route the same way it protects ``POST /local-upload/{ticket_id}``.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import HTTPException, status

from dynastore.models.protocols.asset_process import (
    AssetProcessDescriptor,
    AssetProcessOutput,
    HTTPMethod,
)
from dynastore.modules.catalog.asset_service import Asset

logger = logging.getLogger(__name__)


class LocalDownloadAssetProcess:
    """``download`` process for locally-owned assets.

    Structurally implements ``AssetProcessProtocol`` without inheriting from
    it (matches DynaStore's capability-protocol convention).
    """

    process_id = "download"
    http_method: HTTPMethod = "GET"

    async def describe(self, asset: Asset) -> AssetProcessDescriptor:
        applicable = (
            asset.owned_by == "local"
            and bool(asset.uri)
            and asset.uri.startswith("file://")
        )
        return AssetProcessDescriptor(
            process_id=self.process_id,
            title="Download",
            description=(
                "Redirects to the bearer-auth-protected local-download route. "
                "The route streams the underlying file from the on-disk asset "
                "store; no signed URL is needed because bytes never leave the "
                "auth perimeter."
            ),
            http_method=self.http_method,
            applicable=applicable,
            reason=None if applicable else "Asset is not locally owned.",
            parameters_schema=None,
        )

    async def execute(
        self, asset: Asset, params: Dict[str, Any]
    ) -> AssetProcessOutput:
        if asset.owned_by != "local" or not asset.uri or not asset.uri.startswith("file://"):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    "download process not applicable: asset is not locally "
                    "owned."
                ),
            )

        return AssetProcessOutput(
            type="redirect",
            url=f"/local-download/{asset.catalog_id}/{asset.asset_id}",
            method="GET",
        )
