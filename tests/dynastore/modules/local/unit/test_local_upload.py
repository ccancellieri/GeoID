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

"""
Unit tests for LocalUploadModule.

Covers (no DB, no filesystem side-effects via tmp_path):
- AssetUploadProtocol isinstance compliance
- initiate_upload: ticket shape, staging dir created, ticket stored
- get_upload_status: PENDING, COMPLETED (cleans up), FAILED, 404 (unknown), expiry, catalog mismatch
- _complete_upload: moves file, calls create_asset, sets ticket COMPLETED
- Ticket expiry logic in both get_upload_status and receive endpoint path
"""

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.local.local_upload import LocalUploadModule
from dynastore.models.protocols import AssetUploadProtocol, UploadStatus, UploadStatusResponse


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_module(tmp_path: Path) -> LocalUploadModule:
    staging = tmp_path / "staging"
    assets = tmp_path / "assets"
    return LocalUploadModule(staging_dir=str(staging), asset_dir=str(assets))


def _make_asset_def(asset_id: str = "asset_001") -> MagicMock:
    d = MagicMock()
    d.asset_id = asset_id
    d.asset_type = "ASSET"
    d.metadata = {}
    return d


# ---------------------------------------------------------------------------
# Protocol compliance
# ---------------------------------------------------------------------------


class TestProtocolCompliance:
    def test_isinstance_asset_upload_protocol(self, tmp_path):
        m = _make_module(tmp_path)
        assert isinstance(m, AssetUploadProtocol)

    def test_has_initiate_upload(self, tmp_path):
        m = _make_module(tmp_path)
        assert callable(getattr(m, "initiate_upload", None))

    def test_has_get_upload_status(self, tmp_path):
        m = _make_module(tmp_path)
        assert callable(getattr(m, "get_upload_status", None))


# ---------------------------------------------------------------------------
# initiate_upload
# ---------------------------------------------------------------------------


class TestInitiateUpload:
    @pytest.mark.asyncio
    async def test_returns_upload_ticket(self, tmp_path):
        m = _make_module(tmp_path)
        ticket = await m.initiate_upload(
            catalog_id="cat1",
            asset_def=_make_asset_def(),
            filename="scene.tif",
        )
        assert ticket.backend == "local"
        assert ticket.method == "POST"
        assert ticket.upload_url.startswith("/local-upload/")
        assert ticket.expires_at > datetime.now(timezone.utc)

    @pytest.mark.asyncio
    async def test_ticket_id_in_url(self, tmp_path):
        m = _make_module(tmp_path)
        ticket = await m.initiate_upload(
            catalog_id="cat1",
            asset_def=_make_asset_def(),
            filename="f.geojson",
        )
        assert ticket.ticket_id in ticket.upload_url

    @pytest.mark.asyncio
    async def test_staging_dir_created(self, tmp_path):
        m = _make_module(tmp_path)
        ticket = await m.initiate_upload(
            catalog_id="cat1",
            asset_def=_make_asset_def(),
            filename="f.tif",
        )
        staging_dir = m._staging_root / ticket.ticket_id
        assert staging_dir.is_dir()

    @pytest.mark.asyncio
    async def test_ticket_stored_in_memory(self, tmp_path):
        m = _make_module(tmp_path)
        ticket = await m.initiate_upload(
            catalog_id="cat1",
            asset_def=_make_asset_def("my_asset"),
            filename="f.tif",
        )
        assert ticket.ticket_id in m._upload_tickets
        stored = m._upload_tickets[ticket.ticket_id]
        assert stored["asset_id"] == "my_asset"
        assert stored["catalog_id"] == "cat1"
        assert stored["status"] == UploadStatus.PENDING

    @pytest.mark.asyncio
    async def test_content_type_in_headers(self, tmp_path):
        m = _make_module(tmp_path)
        ticket = await m.initiate_upload(
            catalog_id="cat1",
            asset_def=_make_asset_def(),
            filename="f.tif",
            content_type="image/tiff",
        )
        assert ticket.headers.get("Content-Type") == "image/tiff"

    @pytest.mark.asyncio
    async def test_collection_id_stored(self, tmp_path):
        m = _make_module(tmp_path)
        ticket = await m.initiate_upload(
            catalog_id="cat1",
            asset_def=_make_asset_def(),
            filename="f.tif",
            collection_id="coll_a",
        )
        assert m._upload_tickets[ticket.ticket_id]["collection_id"] == "coll_a"

    @pytest.mark.asyncio
    async def test_ttl_is_one_hour(self, tmp_path):
        m = _make_module(tmp_path)
        before = datetime.now(timezone.utc)
        ticket = await m.initiate_upload("cat1", _make_asset_def(), "f.tif")
        after = datetime.now(timezone.utc)
        expected_min = before + timedelta(hours=1) - timedelta(seconds=1)
        expected_max = after + timedelta(hours=1) + timedelta(seconds=1)
        assert expected_min <= ticket.expires_at <= expected_max


# ---------------------------------------------------------------------------
# get_upload_status
# ---------------------------------------------------------------------------


class TestGetUploadStatus:
    def _seed_ticket(
        self,
        m: LocalUploadModule,
        *,
        ticket_id: str = "t1",
        catalog_id: str = "cat1",
        status: UploadStatus = UploadStatus.PENDING,
        expired: bool = False,
    ) -> str:
        expires_at = (
            datetime.now(timezone.utc) - timedelta(seconds=1)
            if expired
            else datetime.now(timezone.utc) + timedelta(hours=1)
        )
        m._upload_tickets[ticket_id] = {
            "asset_id": "asset_001",
            "catalog_id": catalog_id,
            "collection_id": None,
            "filename": "f.tif",
            "expires_at": expires_at,
            "status": status,
        }
        return ticket_id

    @pytest.mark.asyncio
    async def test_pending(self, tmp_path):
        m = _make_module(tmp_path)
        tid = self._seed_ticket(m, status=UploadStatus.PENDING)
        resp = await m.get_upload_status(ticket_id=tid, catalog_id="cat1")
        assert resp.status == UploadStatus.PENDING
        assert resp.asset_id is None

    @pytest.mark.asyncio
    async def test_uploading(self, tmp_path):
        m = _make_module(tmp_path)
        tid = self._seed_ticket(m, status=UploadStatus.UPLOADING)
        resp = await m.get_upload_status(ticket_id=tid, catalog_id="cat1")
        assert resp.status == UploadStatus.UPLOADING

    @pytest.mark.asyncio
    async def test_completed_returns_asset_id_and_removes_ticket(self, tmp_path):
        m = _make_module(tmp_path)
        tid = self._seed_ticket(m, status=UploadStatus.COMPLETED)
        m._upload_tickets[tid]["asset_id_result"] = "registered_asset"
        resp = await m.get_upload_status(ticket_id=tid, catalog_id="cat1")
        assert resp.status == UploadStatus.COMPLETED
        assert resp.asset_id == "registered_asset"
        assert tid not in m._upload_tickets  # cleaned up

    @pytest.mark.asyncio
    async def test_failed_returns_error(self, tmp_path):
        m = _make_module(tmp_path)
        tid = self._seed_ticket(m, status=UploadStatus.FAILED)
        m._upload_tickets[tid]["error"] = "disk full"
        resp = await m.get_upload_status(ticket_id=tid, catalog_id="cat1")
        assert resp.status == UploadStatus.FAILED
        assert resp.error == "disk full"

    @pytest.mark.asyncio
    async def test_unknown_ticket_raises_404(self, tmp_path):
        from fastapi import HTTPException
        m = _make_module(tmp_path)
        with pytest.raises(HTTPException) as exc_info:
            await m.get_upload_status(ticket_id="nonexistent", catalog_id="cat1")
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_expired_ticket_raises_404_and_removes(self, tmp_path):
        from fastapi import HTTPException
        m = _make_module(tmp_path)
        tid = self._seed_ticket(m, expired=True)
        with pytest.raises(HTTPException) as exc_info:
            await m.get_upload_status(ticket_id=tid, catalog_id="cat1")
        assert exc_info.value.status_code == 404
        assert tid not in m._upload_tickets

    @pytest.mark.asyncio
    async def test_catalog_mismatch_raises_404(self, tmp_path):
        from fastapi import HTTPException
        m = _make_module(tmp_path)
        self._seed_ticket(m, catalog_id="cat1")
        with pytest.raises(HTTPException) as exc_info:
            await m.get_upload_status(ticket_id="t1", catalog_id="cat_wrong")
        assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# _complete_upload — internal method
# ---------------------------------------------------------------------------


class TestCompleteUpload:
    @pytest.mark.asyncio
    async def test_moves_file_to_permanent(self, tmp_path):
        m = _make_module(tmp_path)

        # Create a fake staged file
        staging_file = tmp_path / "staging" / "t1" / "scene.tif"
        staging_file.parent.mkdir(parents=True)
        staging_file.write_bytes(b"TIFF_DATA")

        ticket = {
            "asset_id": "scene_001",
            "catalog_id": "cat1",
            "collection_id": None,
            "filename": "scene.tif",
            "asset_def": _make_asset_def("scene_001"),
            "status": UploadStatus.UPLOADING,
        }

        mock_asset = MagicMock()
        mock_asset.asset_id = "scene_001"
        mock_assets = MagicMock()
        mock_assets.create_asset = AsyncMock(return_value=mock_asset)

        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_assets,
        ):
            asset = await m._complete_upload("t1", staging_file, ticket)

        # File was moved
        permanent = m._asset_root / "cat1" / "_catalog_tier" / "scene.tif"
        assert permanent.exists()
        assert not staging_file.exists()
        assert asset.asset_id == "scene_001"

    @pytest.mark.asyncio
    async def test_uri_is_file_scheme(self, tmp_path):
        m = _make_module(tmp_path)

        staging_file = tmp_path / "staging" / "t2" / "f.tif"
        staging_file.parent.mkdir(parents=True)
        staging_file.write_bytes(b"x")

        ticket = {
            "asset_id": "a1",
            "catalog_id": "cat1",
            "collection_id": None,
            "filename": "f.tif",
            "asset_def": _make_asset_def("a1"),
            "status": UploadStatus.UPLOADING,
        }

        captured_uri = {}

        async def _capture_create(catalog_id, asset=None, collection_id=None):
            captured_uri["uri"] = asset.uri
            m2 = MagicMock()
            m2.asset_id = "a1"
            return m2

        mock_assets = MagicMock()
        mock_assets.create_asset = _capture_create

        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_assets,
        ):
            await m._complete_upload("t2", staging_file, ticket)

        assert captured_uri["uri"].startswith("file:///")

    @pytest.mark.asyncio
    async def test_owned_by_local(self, tmp_path):
        m = _make_module(tmp_path)

        staging_file = tmp_path / "staging" / "t3" / "f.tif"
        staging_file.parent.mkdir(parents=True)
        staging_file.write_bytes(b"x")

        ticket = {
            "asset_id": "a1",
            "catalog_id": "cat1",
            "collection_id": None,
            "filename": "f.tif",
            "asset_def": _make_asset_def("a1"),
            "status": UploadStatus.UPLOADING,
        }

        captured_asset_base = {}

        async def _capture(catalog_id, asset=None, collection_id=None):
            captured_asset_base["obj"] = asset
            m2 = MagicMock()
            m2.asset_id = "a1"
            return m2

        mock_assets = MagicMock()
        mock_assets.create_asset = _capture

        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_assets,
        ):
            await m._complete_upload("t3", staging_file, ticket)

        assert captured_asset_base["obj"].owned_by == "local"

    @pytest.mark.asyncio
    async def test_ticket_marked_completed(self, tmp_path):
        m = _make_module(tmp_path)

        staging_file = tmp_path / "staging" / "t4" / "f.tif"
        staging_file.parent.mkdir(parents=True)
        staging_file.write_bytes(b"x")

        ticket = {
            "asset_id": "a1",
            "catalog_id": "cat1",
            "collection_id": None,
            "filename": "f.tif",
            "asset_def": _make_asset_def("a1"),
            "status": UploadStatus.UPLOADING,
        }

        mock_asset = MagicMock()
        mock_asset.asset_id = "a1"
        mock_assets = MagicMock()
        mock_assets.create_asset = AsyncMock(return_value=mock_asset)

        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_assets,
        ):
            await m._complete_upload("t4", staging_file, ticket)

        assert ticket["status"] == UploadStatus.COMPLETED
        assert ticket["asset_id_result"] == "a1"

    @pytest.mark.asyncio
    async def test_collection_id_scopes_path(self, tmp_path):
        m = _make_module(tmp_path)

        staging_file = tmp_path / "staging" / "t5" / "f.tif"
        staging_file.parent.mkdir(parents=True)
        staging_file.write_bytes(b"x")

        ticket = {
            "asset_id": "a1",
            "catalog_id": "cat1",
            "collection_id": "my_collection",
            "filename": "f.tif",
            "asset_def": _make_asset_def("a1"),
            "status": UploadStatus.UPLOADING,
        }

        mock_asset = MagicMock()
        mock_asset.asset_id = "a1"
        mock_assets = MagicMock()
        mock_assets.create_asset = AsyncMock(return_value=mock_asset)

        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_assets,
        ):
            await m._complete_upload("t5", staging_file, ticket)

        permanent = m._asset_root / "cat1" / "my_collection" / "f.tif"
        assert permanent.exists()
