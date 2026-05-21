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
"""Asset download capability.

``download`` is an asset-resource action (like ``upload``), not an OGC process:
the asset REST layer exposes ``GET /assets/.../{asset_id}/download`` and
302-redirects to a backend-resolved URL. Each storage backend registers an
:class:`AssetDownloadProtocol` implementation (GCS signed URL, local
bearer-auth route, ...) discovered via ``get_protocols(AssetDownloadProtocol)``.
The router picks the first implementation whose :meth:`applies_to` returns
``True`` for the asset.
"""
from __future__ import annotations

from typing import Optional, Protocol, TYPE_CHECKING, runtime_checkable

if TYPE_CHECKING:
    from dynastore.modules.catalog.asset_service import Asset


@runtime_checkable
class AssetDownloadProtocol(Protocol):
    """Backend capability that resolves a redirect target for downloading an asset."""

    def applies_to(self, asset: "Asset") -> bool:
        """Return ``True`` when this backend can serve ``asset``'s bytes.

        Must not raise — backends opt out (e.g. wrong ``owned_by`` or URI
        scheme) by returning ``False`` so the router can try the next one.
        """
        ...

    async def resolve_download_url(
        self, asset: "Asset", ttl: Optional[int] = None
    ) -> str:
        """Return the URL the client should be redirected to (302).

        Args:
            asset: The target asset (already fetched + authorized by the router).
            ttl: Optional lifetime hint in seconds for backends that mint
                short-lived URLs (e.g. GCS signed URLs). Ignored otherwise.

        Raises:
            HTTPException 400: ``ttl`` out of the backend's allowed range.
            HTTPException 404: the underlying object no longer exists.
            HTTPException 409: the asset is not downloadable by this backend.
        """
        ...
