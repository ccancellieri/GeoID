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
"""GCS-backed asset processes.

Currently exposes a single process:

* ``download`` — generates a short-lived V4 signed GET URL for assets whose
  ``owned_by == "gcs"`` and whose ``uri`` points to a GCS blob.

Registered by ``GCPModule`` at lifespan so the parametric asset router can
discover it via ``get_protocols(AssetProcessProtocol)``.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from fastapi import HTTPException, status

from dynastore.models.protocols import CloudIdentityProtocol, CloudStorageClientProtocol
from dynastore.models.protocols.asset_process import (
    AssetProcessDescriptor,
    AssetProcessOutput,
    HTTPMethod,
)
from dynastore.modules.catalog.asset_service import Asset
from dynastore.modules.gcp.tools.signed_urls import generate_gcs_signed_url

logger = logging.getLogger(__name__)

DEFAULT_DOWNLOAD_TTL_SECONDS = 86_400
MIN_DOWNLOAD_TTL_SECONDS = 60
MAX_DOWNLOAD_TTL_SECONDS = 604_800  # 7 days — GCS v4 signing hard ceiling


class GcsDownloadAssetProcess:
    """``download`` process for GCS-owned assets.

    Structurally implements ``AssetProcessProtocol`` without inheriting from it
    (matches DynaStore's capability-protocol convention).
    """

    process_id = "download"
    http_method: HTTPMethod = "POST"

    def __init__(
        self,
        client_provider: CloudStorageClientProtocol,
        identity_provider: Optional[CloudIdentityProtocol] = None,
        max_ttl_seconds: int = MAX_DOWNLOAD_TTL_SECONDS,
    ):
        self._client_provider = client_provider
        self._identity_provider = identity_provider
        self._max_ttl_seconds = min(max_ttl_seconds, MAX_DOWNLOAD_TTL_SECONDS)

    async def describe(self, asset: Asset) -> AssetProcessDescriptor:
        applicable = (
            asset.owned_by == "gcs"
            and bool(asset.uri)
            and asset.uri.startswith("gs://")
        )
        return AssetProcessDescriptor(
            process_id=self.process_id,
            title="Download",
            description=(
                "Generates a short-lived V4 signed GET URL the client can "
                "follow to download the underlying GCS blob."
            ),
            http_method=self.http_method,
            applicable=applicable,
            reason=None if applicable else "Asset is not owned by GCS.",
            parameters_schema={
                "type": "object",
                "properties": {
                    "ttl": {
                        "type": "integer",
                        "minimum": MIN_DOWNLOAD_TTL_SECONDS,
                        "maximum": self._max_ttl_seconds,
                        "default": DEFAULT_DOWNLOAD_TTL_SECONDS,
                        "description": "Signed-URL lifetime in seconds.",
                    }
                },
                "additionalProperties": False,
            },
        )

    async def execute(self, asset: Asset, params: Dict[str, Any]) -> AssetProcessOutput:
        if asset.owned_by != "gcs" or not asset.uri or not asset.uri.startswith("gs://"):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="download process not applicable: asset is not GCS-owned.",
            )

        ttl = int(params.get("ttl", DEFAULT_DOWNLOAD_TTL_SECONDS))
        if ttl < MIN_DOWNLOAD_TTL_SECONDS or ttl > self._max_ttl_seconds:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"ttl must be between {MIN_DOWNLOAD_TTL_SECONDS} and "
                    f"{self._max_ttl_seconds} seconds (got {ttl})."
                ),
            )

        expiration = timedelta(seconds=ttl)
        assert asset.uri is not None  # narrowed by the guard above
        signed_url = await generate_gcs_signed_url(
            asset.uri,
            method="GET",
            expiration=expiration,
            client_provider=self._client_provider,
            identity_provider=self._identity_provider,
            check_exists=True,
        )
        if signed_url is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"GCS blob for asset {asset.asset_id!r} no longer exists.",
            )

        expires_at = (datetime.now(timezone.utc) + expiration).isoformat()
        return AssetProcessOutput(
            type="signed_url",
            url=signed_url,
            method="GET",
            expires_at=expires_at,
        )
