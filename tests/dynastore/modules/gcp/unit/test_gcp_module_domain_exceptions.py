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

"""Unit tests that the GCP module raises domain exceptions — not HTTPException.

These tests exercise the raises in ``asset_downloads.py`` and the ticket-status
path of ``gcp_storage_ops.py`` without touching FastAPI at all, confirming that
the fastapi import was removed (issue #1969).
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# asset_downloads.py — GcsAssetDownload
# ---------------------------------------------------------------------------


class TestGcsAssetDownloadRaises:
    """GcsAssetDownload.resolve_download_url must raise domain errors only."""

    def _make_asset(
        self,
        owned_by: str = "gcs",
        uri: str = "gs://my-bucket/path/to/file.tif",
        asset_id: str = "asset-1",
    ):
        from unittest.mock import MagicMock

        asset = MagicMock()
        asset.owned_by = owned_by
        asset.uri = uri
        asset.asset_id = asset_id
        return asset

    def _make_download(self):
        from unittest.mock import MagicMock
        from dynastore.modules.gcp.asset_downloads import GcsAssetDownload

        return GcsAssetDownload(client_provider=MagicMock(), identity_provider=None)

    @pytest.mark.asyncio
    async def test_not_gcs_owned_raises_conflict_error(self) -> None:
        from dynastore.modules.storage.errors import ConflictError

        dl = self._make_download()
        asset = self._make_asset(owned_by="local", uri="file:///tmp/x")
        with pytest.raises(ConflictError, match="not GCS-owned"):
            await dl.resolve_download_url(asset)

    @pytest.mark.asyncio
    async def test_ttl_too_small_raises_value_error(self) -> None:
        from dynastore.modules.gcp.asset_downloads import MIN_DOWNLOAD_TTL_SECONDS

        dl = self._make_download()
        asset = self._make_asset()
        with pytest.raises(ValueError, match="ttl must be between"):
            await dl.resolve_download_url(asset, ttl=MIN_DOWNLOAD_TTL_SECONDS - 1)

    @pytest.mark.asyncio
    async def test_ttl_too_large_raises_value_error(self) -> None:
        from dynastore.modules.gcp.asset_downloads import MAX_DOWNLOAD_TTL_SECONDS

        dl = self._make_download()
        asset = self._make_asset()
        with pytest.raises(ValueError, match="ttl must be between"):
            await dl.resolve_download_url(asset, ttl=MAX_DOWNLOAD_TTL_SECONDS + 1)

    @pytest.mark.asyncio
    async def test_blob_gone_raises_value_error_not_found(self) -> None:
        from unittest.mock import AsyncMock, patch

        dl = self._make_download()
        asset = self._make_asset()

        with patch(
            "dynastore.modules.gcp.asset_downloads.generate_gcs_signed_url",
            new=AsyncMock(return_value=None),
        ):
            with pytest.raises(ValueError, match="no longer exists"):
                await dl.resolve_download_url(asset)

    @pytest.mark.asyncio
    async def test_no_fastapi_import_in_module(self) -> None:
        """Guard: importing asset_downloads must not drag in fastapi."""
        import sys

        # Remove cached module to force a fresh import
        for key in list(sys.modules):
            if "asset_downloads" in key and "gcp" in key:
                del sys.modules[key]

        import dynastore.modules.gcp.asset_downloads  # noqa: F401

        # fastapi is fine in sys.modules (it's in extensions) but must not
        # come from gcp.asset_downloads itself — grep ensures no top-level import
        import inspect
        src = inspect.getsource(dynastore.modules.gcp.asset_downloads)
        assert "from fastapi" not in src
        assert "import fastapi" not in src


# ---------------------------------------------------------------------------
# gcp_storage_ops.py — get_upload_status ticket checks
# ---------------------------------------------------------------------------


class TestGetUploadStatusDomainRaises:
    """get_upload_status raises ValueError for missing/expired/wrong-catalog tickets."""

    def _make_mixin(self):
        """Build a minimal GcpStorageOpsMixin-like object with an empty ticket store."""
        from dynastore.modules.gcp.gcp_storage_ops import GcpStorageOpsMixin

        class _Stub(GcpStorageOpsMixin):
            _upload_tickets = {}
            _credentials = None
            _storage_client = None

            def get_bucket_service(self):
                raise NotImplementedError

            def get_storage_client(self):
                raise NotImplementedError

        return _Stub()

    @pytest.mark.asyncio
    async def test_missing_ticket_raises_value_error(self) -> None:
        stub = self._make_mixin()
        with pytest.raises(ValueError, match="not found or expired"):
            await stub.get_upload_status(ticket_id="no-such-id", catalog_id="cat")

    @pytest.mark.asyncio
    async def test_expired_ticket_raises_value_error(self) -> None:
        from datetime import datetime, timezone, timedelta

        stub = self._make_mixin()
        stub._upload_tickets["tk-1"] = {
            "asset_id": "a1",
            "catalog_id": "cat",
            "collection_id": None,
            "expires_at": datetime.now(timezone.utc) - timedelta(hours=2),
        }
        with pytest.raises(ValueError, match="expired"):
            await stub.get_upload_status(ticket_id="tk-1", catalog_id="cat")

    @pytest.mark.asyncio
    async def test_wrong_catalog_raises_value_error(self) -> None:
        from datetime import datetime, timezone, timedelta

        stub = self._make_mixin()
        stub._upload_tickets["tk-2"] = {
            "asset_id": "a1",
            "catalog_id": "real-cat",
            "collection_id": None,
            "expires_at": datetime.now(timezone.utc) + timedelta(hours=1),
        }
        with pytest.raises(ValueError, match="not found in catalog 'wrong-cat'"):
            await stub.get_upload_status(ticket_id="tk-2", catalog_id="wrong-cat")
