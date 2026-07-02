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

"""Tests for `tile_blob_storage.StorageTileWriter` — the generic
StorageProtocol-backed tile-cache writer, driven entirely through a fake
in-memory StorageProtocol (no real GCS/local disk)."""
from __future__ import annotations

from typing import Dict, List, Optional

import pytest

from dynastore.modules.tiles.tile_blob_storage import StorageTileWriter, TileUrlSignerProtocol


class _FakeStorage:
    """Minimal in-memory StorageProtocol stand-in."""

    def __init__(self) -> None:
        self._objects: Dict[str, bytes] = {}
        self.upload_calls: List[str] = []

    async def upload_file_content(self, target_path: str, content: bytes, content_type=None) -> str:
        self._objects[target_path] = content
        self.upload_calls.append(target_path)
        return target_path

    async def download_file_content(self, path: str) -> Optional[bytes]:
        return self._objects.get(path)

    async def file_exists(self, path: str) -> bool:
        return path in self._objects

    async def delete_file(self, path: str) -> None:
        self._objects.pop(path, None)

    async def list_prefix(self, base_uri: str, prefix: str) -> List[str]:
        full_prefix = f"{base_uri}/{prefix}"
        return [k for k in self._objects if k.startswith(full_prefix)]


class _FakeSigner:
    supported_schemes = frozenset({"gs"})

    def __init__(self, url: Optional[str]) -> None:
        self._url = url
        self.signed: List[str] = []

    async def sign(self, object_uri: str) -> Optional[str]:
        self.signed.append(object_uri)
        return self._url


@pytest.mark.asyncio
async def test_save_then_get_round_trips():
    storage = _FakeStorage()
    writer = StorageTileWriter(storage, "gs://bucket", "tiles/collections")

    uri = await writer.save_tile("cat", "coll", "WebMercatorQuad", 5, 17, 11, b"mvt-bytes", "mvt")
    assert uri == "gs://bucket/tiles/collections/coll/WebMercatorQuad/5/17/11.mvt"

    got = await writer.get_tile("cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt")
    assert got == b"mvt-bytes"


@pytest.mark.asyncio
async def test_get_tile_miss_returns_none():
    storage = _FakeStorage()
    writer = StorageTileWriter(storage, "gs://bucket", "tiles/collections")
    assert await writer.get_tile("cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt") is None


@pytest.mark.asyncio
async def test_check_tile_exists_reflects_saved_state():
    storage = _FakeStorage()
    writer = StorageTileWriter(storage, "gs://bucket", "tiles/collections")
    assert await writer.check_tile_exists("cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt") is False
    await writer.save_tile("cat", "coll", "WebMercatorQuad", 5, 17, 11, b"x", "mvt")
    assert await writer.check_tile_exists("cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt") is True


@pytest.mark.asyncio
async def test_delete_tile_removes_object_idempotently():
    storage = _FakeStorage()
    writer = StorageTileWriter(storage, "gs://bucket", "tiles/collections")
    await writer.save_tile("cat", "coll", "WebMercatorQuad", 5, 17, 11, b"x", "mvt")
    assert await writer.delete_tile("cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt") is True
    assert await writer.check_tile_exists("cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt") is False
    # Deleting an already-absent tile is still a success (idempotent).
    assert await writer.delete_tile("cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt") is True


@pytest.mark.asyncio
async def test_delete_tiles_for_collection_uses_list_prefix():
    storage = _FakeStorage()
    writer = StorageTileWriter(storage, "gs://bucket", "tiles/collections")
    await writer.save_tile("cat", "coll", "WebMercatorQuad", 5, 17, 11, b"a", "mvt")
    await writer.save_tile("cat", "coll", "WebMercatorQuad", 6, 34, 22, b"b", "mvt")
    await writer.save_tile("cat", "other-coll", "WebMercatorQuad", 5, 17, 11, b"c", "mvt")

    deleted = await writer.delete_tiles_for_collection("cat", "coll")
    assert deleted == 2
    assert await writer.check_tile_exists("cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt") is False
    # Other collection untouched.
    assert await writer.check_tile_exists("cat", "other-coll", "WebMercatorQuad", 5, 17, 11, "mvt") is True


@pytest.mark.asyncio
async def test_get_tile_url_returns_none_without_signer():
    storage = _FakeStorage()
    writer = StorageTileWriter(storage, "gs://bucket", "tiles/collections")
    assert await writer.get_tile_url("cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt") is None


@pytest.mark.asyncio
async def test_get_tile_url_delegates_to_scheme_matching_signer(monkeypatch):
    import dynastore.modules.tiles.tile_blob_storage as tbs

    signer = _FakeSigner("https://signed.example/x")
    monkeypatch.setattr(tbs, "get_protocols", lambda proto: [signer] if proto is TileUrlSignerProtocol else [])

    storage = _FakeStorage()
    writer = StorageTileWriter(storage, "gs://bucket", "tiles/collections")
    url = await writer.get_tile_url("cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt")

    assert url == "https://signed.example/x"
    assert signer.signed == ["gs://bucket/tiles/collections/coll/WebMercatorQuad/5/17/11.mvt"]


@pytest.mark.asyncio
async def test_get_tile_url_ignores_signer_for_non_matching_scheme(monkeypatch):
    import dynastore.modules.tiles.tile_blob_storage as tbs

    signer = _FakeSigner("https://should-not-be-used")
    monkeypatch.setattr(tbs, "get_protocols", lambda proto: [signer] if proto is TileUrlSignerProtocol else [])

    storage = _FakeStorage()
    writer = StorageTileWriter(storage, "file:///data/tiles", "tiles/collections")
    url = await writer.get_tile_url("cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt")

    assert url is None
    assert signer.signed == []
