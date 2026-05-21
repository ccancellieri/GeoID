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
- initiate_upload: ticket shape, staging dir created, ticket persisted
- get_upload_status: PENDING, COMPLETED (cleans up), FAILED, 404 (unknown),
  expiry, catalog mismatch
- _complete_upload: moves file, finalize/create_asset, persists COMPLETED
- Cross-replica visibility: a ticket written via one module instance's store
  resolves through a *different* instance's store (the multi-worker /
  multi-replica fix — the registry is no longer process-local).

The PostgreSQL-backed :class:`LocalUploadTicketStore` is replaced by an
in-memory fake (``FakeTicketStore``) patched onto ``LocalUploadModule._tickets``
so these stay pure unit tests; the SQL store itself is exercised in
``test_local_upload_store.py``.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.local.local_upload import LocalUploadModule
from dynastore.models.protocols import (
    AssetUploadProtocol,
    UploadStatus,
)


# ---------------------------------------------------------------------------
# Fake shared store — in-memory stand-in for the PG-backed registry
# ---------------------------------------------------------------------------


class FakeTicketStore:
    """In-memory async stand-in for ``LocalUploadTicketStore``.

    Mirrors the persistent store's public surface (put/get/update/delete) and
    its TTL-on-read semantics. A single instance is shared across module
    instances in a test so the cross-replica behaviour can be asserted.
    """

    def __init__(self) -> None:
        self.rows: Dict[str, Dict[str, Any]] = {}

    async def put(self, ticket_id: str, ticket: Dict[str, Any]) -> None:
        self.rows[ticket_id] = dict(ticket)

    async def get(self, ticket_id: str) -> Optional[Dict[str, Any]]:
        row = self.rows.get(ticket_id)
        if row is None:
            return None
        if datetime.now(timezone.utc) > row["expires_at"]:
            self.rows.pop(ticket_id, None)
            return None
        return dict(row)

    async def update(self, ticket_id: str, fields: Dict[str, Any]) -> None:
        row = self.rows.get(ticket_id)
        if row is not None:
            row.update(fields)

    async def delete(self, ticket_id: str) -> None:
        self.rows.pop(ticket_id, None)

    async def prune_expired(self) -> None:
        now = datetime.now(timezone.utc)
        for tid in [t for t, r in self.rows.items() if r["expires_at"] < now]:
            self.rows.pop(tid, None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_module(tmp_path: Path, store: FakeTicketStore) -> LocalUploadModule:
    staging = tmp_path / "staging"
    assets = tmp_path / "assets"
    m = LocalUploadModule(staging_dir=str(staging), asset_dir=str(assets))
    # The autouse fixture routes ``self._tickets`` to ``self._fake_store``.
    object.__setattr__(m, "_fake_store", store)
    return m


@pytest.fixture(autouse=True)
def _route_property_to_fake(monkeypatch):
    """Route ``self._tickets`` to a per-instance fake store when one is set.

    ``LocalUploadModule._tickets`` is a property that resolves a fresh
    PG-backed store from the live engine. Tests set ``self._fake_store`` (an
    in-memory ``FakeTicketStore``); when absent the property falls through to
    the real engine resolver (exercised by ``TestEngineGuard``).
    """
    real_prop = LocalUploadModule._tickets

    def _tickets(self):
        fake = getattr(self, "_fake_store", None)
        if fake is not None:
            return fake
        return real_prop.fget(self)

    monkeypatch.setattr(LocalUploadModule, "_tickets", property(_tickets))
    yield


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
        m = _make_module(tmp_path, FakeTicketStore())
        assert isinstance(m, AssetUploadProtocol)

    def test_has_initiate_upload(self, tmp_path):
        m = _make_module(tmp_path, FakeTicketStore())
        assert callable(getattr(m, "initiate_upload", None))

    def test_has_get_upload_status(self, tmp_path):
        m = _make_module(tmp_path, FakeTicketStore())
        assert callable(getattr(m, "get_upload_status", None))


# ---------------------------------------------------------------------------
# initiate_upload
# ---------------------------------------------------------------------------


class TestInitiateUpload:
    @pytest.mark.asyncio
    async def test_returns_upload_ticket(self, tmp_path):
        m = _make_module(tmp_path, FakeTicketStore())
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
        m = _make_module(tmp_path, FakeTicketStore())
        ticket = await m.initiate_upload(
            catalog_id="cat1",
            asset_def=_make_asset_def(),
            filename="f.geojson",
        )
        assert ticket.ticket_id in ticket.upload_url

    @pytest.mark.asyncio
    async def test_staging_dir_created(self, tmp_path):
        m = _make_module(tmp_path, FakeTicketStore())
        ticket = await m.initiate_upload(
            catalog_id="cat1",
            asset_def=_make_asset_def(),
            filename="f.tif",
        )
        staging_dir = m._staging_root / ticket.ticket_id
        assert staging_dir.is_dir()

    @pytest.mark.asyncio
    async def test_ticket_persisted_in_store(self, tmp_path):
        store = FakeTicketStore()
        m = _make_module(tmp_path, store)
        ticket = await m.initiate_upload(
            catalog_id="cat1",
            asset_def=_make_asset_def("my_asset"),
            filename="f.tif",
        )
        stored = await store.get(ticket.ticket_id)
        assert stored is not None
        assert stored["asset_id"] == "my_asset"
        assert stored["catalog_id"] == "cat1"
        assert stored["status"] == UploadStatus.PENDING

    @pytest.mark.asyncio
    async def test_content_type_in_headers(self, tmp_path):
        m = _make_module(tmp_path, FakeTicketStore())
        ticket = await m.initiate_upload(
            catalog_id="cat1",
            asset_def=_make_asset_def(),
            filename="f.tif",
            content_type="image/tiff",
        )
        assert ticket.headers.get("Content-Type") == "image/tiff"

    @pytest.mark.asyncio
    async def test_collection_id_stored(self, tmp_path):
        store = FakeTicketStore()
        m = _make_module(tmp_path, store)
        ticket = await m.initiate_upload(
            catalog_id="cat1",
            asset_def=_make_asset_def(),
            filename="f.tif",
            collection_id="coll_a",
        )
        stored = await store.get(ticket.ticket_id)
        assert stored["collection_id"] == "coll_a"

    @pytest.mark.asyncio
    async def test_ttl_is_one_hour(self, tmp_path):
        m = _make_module(tmp_path, FakeTicketStore())
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
    async def _seed_ticket(
        self,
        store: FakeTicketStore,
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
        await store.put(
            ticket_id,
            {
                "asset_id": "asset_001",
                "catalog_id": catalog_id,
                "collection_id": None,
                "filename": "f.tif",
                "expires_at": expires_at,
                "status": status,
            },
        )
        return ticket_id

    @pytest.mark.asyncio
    async def test_pending(self, tmp_path):
        store = FakeTicketStore()
        m = _make_module(tmp_path, store)
        tid = await self._seed_ticket(store, status=UploadStatus.PENDING)
        resp = await m.get_upload_status(ticket_id=tid, catalog_id="cat1")
        assert resp.status == UploadStatus.PENDING
        assert resp.asset_id is None

    @pytest.mark.asyncio
    async def test_uploading(self, tmp_path):
        store = FakeTicketStore()
        m = _make_module(tmp_path, store)
        tid = await self._seed_ticket(store, status=UploadStatus.UPLOADING)
        resp = await m.get_upload_status(ticket_id=tid, catalog_id="cat1")
        assert resp.status == UploadStatus.UPLOADING

    @pytest.mark.asyncio
    async def test_completed_returns_asset_id_and_removes_ticket(self, tmp_path):
        store = FakeTicketStore()
        m = _make_module(tmp_path, store)
        tid = await self._seed_ticket(store, status=UploadStatus.COMPLETED)
        await store.update(tid, {"asset_id_result": "registered_asset"})
        resp = await m.get_upload_status(ticket_id=tid, catalog_id="cat1")
        assert resp.status == UploadStatus.COMPLETED
        assert resp.asset_id == "registered_asset"
        assert await store.get(tid) is None  # cleaned up

    @pytest.mark.asyncio
    async def test_failed_returns_error(self, tmp_path):
        store = FakeTicketStore()
        m = _make_module(tmp_path, store)
        tid = await self._seed_ticket(store, status=UploadStatus.FAILED)
        await store.update(tid, {"error": "disk full"})
        resp = await m.get_upload_status(ticket_id=tid, catalog_id="cat1")
        assert resp.status == UploadStatus.FAILED
        assert resp.error == "disk full"

    @pytest.mark.asyncio
    async def test_unknown_ticket_raises_404(self, tmp_path):
        from fastapi import HTTPException
        m = _make_module(tmp_path, FakeTicketStore())
        with pytest.raises(HTTPException) as exc_info:
            await m.get_upload_status(ticket_id="nonexistent", catalog_id="cat1")
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_expired_ticket_raises_404_and_removes(self, tmp_path):
        from fastapi import HTTPException
        store = FakeTicketStore()
        m = _make_module(tmp_path, store)
        tid = await self._seed_ticket(store, expired=True)
        with pytest.raises(HTTPException) as exc_info:
            await m.get_upload_status(ticket_id=tid, catalog_id="cat1")
        assert exc_info.value.status_code == 404
        assert await store.get(tid) is None

    @pytest.mark.asyncio
    async def test_catalog_mismatch_raises_404(self, tmp_path):
        from fastapi import HTTPException
        store = FakeTicketStore()
        m = _make_module(tmp_path, store)
        await self._seed_ticket(store, catalog_id="cat1")
        with pytest.raises(HTTPException) as exc_info:
            await m.get_upload_status(ticket_id="t1", catalog_id="cat_wrong")
        assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# _complete_upload — internal method
# ---------------------------------------------------------------------------


class TestCompleteUpload:
    """Covers the create_asset *fallback* path (no born-claimed PENDING row).

    These tests stub ``finalize_pending_upload`` to return ``None`` so the
    legacy ``create_asset`` registration runs; the activation path (a PENDING
    row matched and flipped to ACTIVE) is covered by
    ``TestFinalizePendingActivation`` below.
    """

    @pytest.mark.asyncio
    async def test_moves_file_to_permanent(self, tmp_path):
        store = FakeTicketStore()
        m = _make_module(tmp_path, store)

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
        await store.put(
            "t1", {**ticket, "expires_at": datetime.now(timezone.utc) + timedelta(hours=1)}
        )

        mock_asset = MagicMock()
        mock_asset.asset_id = "scene_001"
        mock_assets = MagicMock()
        mock_assets.finalize_pending_upload = AsyncMock(return_value=None)
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
        store = FakeTicketStore()
        m = _make_module(tmp_path, store)

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
        await store.put(
            "t2", {**ticket, "expires_at": datetime.now(timezone.utc) + timedelta(hours=1)}
        )

        captured_uri = {}

        async def _capture_create(catalog_id, asset=None, collection_id=None):
            captured_uri["uri"] = asset.uri
            m2 = MagicMock()
            m2.asset_id = "a1"
            return m2

        mock_assets = MagicMock()
        mock_assets.finalize_pending_upload = AsyncMock(return_value=None)
        mock_assets.create_asset = _capture_create

        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_assets,
        ):
            await m._complete_upload("t2", staging_file, ticket)

        assert captured_uri["uri"].startswith("file:///")

    @pytest.mark.asyncio
    async def test_owned_by_local(self, tmp_path):
        store = FakeTicketStore()
        m = _make_module(tmp_path, store)

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
        await store.put(
            "t3", {**ticket, "expires_at": datetime.now(timezone.utc) + timedelta(hours=1)}
        )

        captured_asset_base = {}

        async def _capture(catalog_id, asset=None, collection_id=None):
            captured_asset_base["obj"] = asset
            m2 = MagicMock()
            m2.asset_id = "a1"
            return m2

        mock_assets = MagicMock()
        mock_assets.finalize_pending_upload = AsyncMock(return_value=None)
        mock_assets.create_asset = _capture

        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_assets,
        ):
            await m._complete_upload("t3", staging_file, ticket)

        assert captured_asset_base["obj"].owned_by == "local"

    @pytest.mark.asyncio
    async def test_ticket_marked_completed(self, tmp_path):
        store = FakeTicketStore()
        m = _make_module(tmp_path, store)

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
        await store.put(
            "t4", {**ticket, "expires_at": datetime.now(timezone.utc) + timedelta(hours=1)}
        )

        mock_asset = MagicMock()
        mock_asset.asset_id = "a1"
        mock_assets = MagicMock()
        mock_assets.finalize_pending_upload = AsyncMock(return_value=None)
        mock_assets.create_asset = AsyncMock(return_value=mock_asset)

        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_assets,
        ):
            await m._complete_upload("t4", staging_file, ticket)

        # The persisted row reflects COMPLETED + the registered asset id.
        stored = await store.get("t4")
        assert stored["status"] == UploadStatus.COMPLETED
        assert stored["asset_id_result"] == "a1"

    @pytest.mark.asyncio
    async def test_collection_id_scopes_path(self, tmp_path):
        store = FakeTicketStore()
        m = _make_module(tmp_path, store)

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
        await store.put(
            "t5", {**ticket, "expires_at": datetime.now(timezone.utc) + timedelta(hours=1)}
        )

        mock_asset = MagicMock()
        mock_asset.asset_id = "a1"
        mock_assets = MagicMock()
        mock_assets.finalize_pending_upload = AsyncMock(return_value=None)
        mock_assets.create_asset = AsyncMock(return_value=mock_asset)

        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_assets,
        ):
            await m._complete_upload("t5", staging_file, ticket)

        permanent = m._asset_root / "cat1" / "my_collection" / "f.tif"
        assert permanent.exists()


# ---------------------------------------------------------------------------
# Shared ticket store — cross-replica / multi-worker visibility (issue #1171)
# ---------------------------------------------------------------------------


class TestSharedTicketStore:
    """The registry is no longer a process-local ``ClassVar`` dict — it is the
    PostgreSQL-backed :class:`LocalUploadTicketStore`. A ticket stamped by the
    process handling ``initiate_upload`` MUST be resolvable by any other
    process handling the ``POST /local-upload/{ticket_id}`` receive or the
    status poll. Modelled here with one shared fake store behind two module
    instances.
    """

    @pytest.mark.asyncio
    async def test_ticket_from_one_instance_visible_to_another(self, tmp_path):
        shared = FakeTicketStore()

        # Instance A is the resolved upload driver; it stores the ticket.
        instance_a = _make_module(tmp_path / "a", shared)
        ticket = await instance_a.initiate_upload(
            catalog_id="cat1",
            asset_def=_make_asset_def("shared_asset"),
            filename="f.tif",
        )

        # Instance B — a *different replica* — reads the same shared backend.
        instance_b = _make_module(tmp_path / "b", shared)
        seen = await instance_b._tickets.get(ticket.ticket_id)
        assert seen is not None
        assert seen["asset_id"] == "shared_asset"

        # And B's status endpoint resolves it too (no 404).
        resp = await instance_b.get_upload_status(
            ticket_id=ticket.ticket_id, catalog_id="cat1",
        )
        assert resp.status == UploadStatus.PENDING


# ---------------------------------------------------------------------------
# Activation path — born-claimed PENDING row flips to ACTIVE
# ---------------------------------------------------------------------------


class TestFinalizePendingActivation:
    """When a born-claimed PENDING row exists, ``_complete_upload`` activates
    it via ``finalize_pending_upload`` (uri + owned_by=local) instead of
    colliding through ``create_asset`` — which would leave the row stuck
    pending with a NULL uri and 404 the download.
    """

    @pytest.mark.asyncio
    async def test_activates_existing_pending_row(self, tmp_path):
        store = FakeTicketStore()
        m = _make_module(tmp_path, store)

        staging_file = tmp_path / "staging" / "t-act" / "f.tif"
        staging_file.parent.mkdir(parents=True)
        staging_file.write_bytes(b"BYTES")

        ticket = {
            "asset_id": "a1",
            "catalog_id": "cat1",
            "collection_id": None,
            "filename": "f.tif",
            "asset_def": _make_asset_def("a1"),
            "status": UploadStatus.UPLOADING,
        }
        await store.put(
            "t-act",
            {**ticket, "expires_at": datetime.now(timezone.utc) + timedelta(hours=1)},
        )

        activated = MagicMock()
        activated.asset_id = "a1"
        activated.uri = "file:///does/not/matter"
        activated.owned_by = "local"

        captured: dict = {}

        async def _finalize(catalog_id, **kwargs):
            captured.update(kwargs)
            captured["catalog_id"] = catalog_id
            return activated

        mock_assets = MagicMock()
        mock_assets.finalize_pending_upload = _finalize
        # create_asset MUST NOT be called when activation succeeds.
        mock_assets.create_asset = AsyncMock(
            side_effect=AssertionError("create_asset must not run on activation")
        )

        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_assets,
        ):
            asset = await m._complete_upload("t-act", staging_file, ticket)

        assert asset.asset_id == "a1"
        assert captured["owned_by"] == "local"
        assert captured["ticket_id"] == "t-act"
        assert captured["asset_id"] == "a1"
        assert captured["uri"].startswith("file:///")
        # size_bytes is derived from the staged file (5 bytes).
        assert captured["size_bytes"] == 5
        stored = await store.get("t-act")
        assert stored["status"] == UploadStatus.COMPLETED
        assert stored["asset_id_result"] == "a1"

    @pytest.mark.asyncio
    async def test_falls_back_to_create_when_no_pending_row(self, tmp_path):
        store = FakeTicketStore()
        m = _make_module(tmp_path, store)

        staging_file = tmp_path / "staging" / "t-fb" / "f.tif"
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
        await store.put(
            "t-fb",
            {**ticket, "expires_at": datetime.now(timezone.utc) + timedelta(hours=1)},
        )

        created = MagicMock()
        created.asset_id = "a1"

        mock_assets = MagicMock()
        mock_assets.finalize_pending_upload = AsyncMock(return_value=None)
        mock_assets.create_asset = AsyncMock(return_value=created)

        with patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_assets,
        ):
            asset = await m._complete_upload("t-fb", staging_file, ticket)

        assert asset.asset_id == "a1"
        mock_assets.create_asset.assert_awaited_once()
        # The created asset is owned_by=local with a file:// uri.
        _, kwargs = mock_assets.create_asset.call_args
        assert kwargs["asset"].owned_by == "local"
        assert kwargs["asset"].uri.startswith("file:///")


# ---------------------------------------------------------------------------
# Engine-unavailable guard
# ---------------------------------------------------------------------------


class TestEngineGuard:
    @pytest.mark.asyncio
    async def test_tickets_property_raises_without_engine(self, tmp_path):
        # No fake patched in → property falls through to the real engine
        # resolver, which returns None in a unit context.
        m = LocalUploadModule(
            staging_dir=str(tmp_path / "s"), asset_dir=str(tmp_path / "a")
        )
        with patch(
            "dynastore.tools.protocol_helpers.get_engine", return_value=None
        ):
            with pytest.raises(RuntimeError, match="database engine unavailable"):
                _ = m._tickets
