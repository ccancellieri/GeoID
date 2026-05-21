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
"""GCS-backed asset download.

Implements :class:`AssetDownloadProtocol` for assets whose ``owned_by == "gcs"``
and whose ``uri`` points to a GCS blob: mints a short-lived V4 signed GET URL
the ``GET /assets/.../download`` route 302-redirects to. Registered by
``GCPModule`` at lifespan; discovered via ``get_protocols(AssetDownloadProtocol)``.
"""
from __future__ import annotations

import logging
from datetime import timedelta
from typing import Optional

from fastapi import HTTPException, status

from dynastore.models.protocols import CloudIdentityProtocol, CloudStorageClientProtocol
from dynastore.modules.catalog.asset_service import Asset
from dynastore.modules.gcp.tools.signed_urls import generate_gcs_signed_url

logger = logging.getLogger(__name__)

DEFAULT_DOWNLOAD_TTL_SECONDS = 86_400
MIN_DOWNLOAD_TTL_SECONDS = 60
MAX_DOWNLOAD_TTL_SECONDS = 604_800  # 7 days — GCS v4 signing hard ceiling


class GcsAssetDownload:
    """Resolves a V4 signed GET URL for GCS-owned assets."""

    def __init__(
        self,
        client_provider: CloudStorageClientProtocol,
        identity_provider: Optional[CloudIdentityProtocol] = None,
        max_ttl_seconds: int = MAX_DOWNLOAD_TTL_SECONDS,
    ):
        self._client_provider = client_provider
        self._identity_provider = identity_provider
        self._max_ttl_seconds = min(max_ttl_seconds, MAX_DOWNLOAD_TTL_SECONDS)

    def applies_to(self, asset: Asset) -> bool:
        return (
            asset.owned_by == "gcs"
            and bool(asset.uri)
            and asset.uri.startswith("gs://")
        )

    async def resolve_download_url(
        self, asset: Asset, ttl: Optional[int] = None
    ) -> str:
        if not self.applies_to(asset):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="download not applicable: asset is not GCS-owned.",
            )

        ttl_seconds = int(ttl) if ttl is not None else DEFAULT_DOWNLOAD_TTL_SECONDS
        if ttl_seconds < MIN_DOWNLOAD_TTL_SECONDS or ttl_seconds > self._max_ttl_seconds:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=(
                    f"ttl must be between {MIN_DOWNLOAD_TTL_SECONDS} and "
                    f"{self._max_ttl_seconds} seconds (got {ttl_seconds})."
                ),
            )

        assert asset.uri is not None  # narrowed by applies_to above
        signed_url = await generate_gcs_signed_url(
            asset.uri,
            method="GET",
            expiration=timedelta(seconds=ttl_seconds),
            client_provider=self._client_provider,
            identity_provider=self._identity_provider,
            check_exists=True,
        )
        if signed_url is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"GCS blob for asset {asset.asset_id!r} no longer exists.",
            )
        return signed_url
