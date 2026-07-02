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

"""Tests for `LocalStorageOps` — the local-disk `StorageProtocol`
implementation, and its round-trip through `StorageTileWriter`."""
from __future__ import annotations

import pytest

from dynastore.modules.local.local_storage_ops import LocalStorageOps
from dynastore.modules.tiles.tile_blob_storage import StorageTileWriter


def test_supported_schemes_claims_file():
    assert LocalStorageOps.supported_schemes == frozenset({"file"})


@pytest.mark.asyncio
async def test_ensure_storage_for_catalog_creates_directory_and_returns_uri(tmp_path):
    ops = LocalStorageOps(root=str(tmp_path))
    uri = await ops.ensure_storage_for_catalog("cat1")
    assert uri == (tmp_path / "cat1").as_uri()
    assert (tmp_path / "cat1").is_dir()


@pytest.mark.asyncio
async def test_get_storage_identifier_none_before_ensure(tmp_path):
    ops = LocalStorageOps(root=str(tmp_path))
    assert await ops.get_storage_identifier("cat1") is None
    await ops.ensure_storage_for_catalog("cat1")
    assert await ops.get_storage_identifier("cat1") == (tmp_path / "cat1").as_uri()


@pytest.mark.asyncio
async def test_upload_download_file_content_round_trip(tmp_path):
    ops = LocalStorageOps(root=str(tmp_path))
    base = await ops.ensure_storage_for_catalog("cat1")
    target = f"{base}/tiles/collections/coll/WebMercatorQuad/5/17/11.mvt"

    await ops.upload_file_content(target, b"tile-bytes")
    assert await ops.file_exists(target) is True
    assert await ops.download_file_content(target) == b"tile-bytes"

    await ops.delete_file(target)
    assert await ops.file_exists(target) is False
    assert await ops.download_file_content(target) is None


@pytest.mark.asyncio
async def test_download_bytes_range_reads_partial_content(tmp_path):
    ops = LocalStorageOps(root=str(tmp_path))
    base = await ops.ensure_storage_for_catalog("cat1")
    target = f"{base}/blob.bin"
    await ops.upload_file_content(target, b"0123456789")

    chunk = await ops.download_bytes_range(target, 3, 4)
    assert chunk == b"3456"


@pytest.mark.asyncio
async def test_list_prefix_and_drop_storage(tmp_path):
    ops = LocalStorageOps(root=str(tmp_path))
    base = await ops.ensure_storage_for_catalog("cat1")
    await ops.upload_file_content(f"{base}/tiles/coll/a.mvt", b"a")
    await ops.upload_file_content(f"{base}/tiles/coll/b.mvt", b"b")
    await ops.upload_file_content(f"{base}/tiles/other/c.mvt", b"c")

    coll_objects = await ops.list_prefix(base, "tiles/coll")
    assert len(coll_objects) == 2

    ok = await ops.drop_storage("cat1")
    assert ok is True
    assert not (tmp_path / "cat1").exists()


@pytest.mark.asyncio
async def test_storage_tile_writer_round_trips_through_local_storage_ops(tmp_path):
    """Step 8 verification: StorageTileWriter round-trips through
    LocalStorageOps on a tmpdir, no cloud dependency."""
    ops = LocalStorageOps(root=str(tmp_path))
    base_uri = await ops.ensure_storage_for_catalog("cat1")
    writer = StorageTileWriter(ops, base_uri, "tiles/collections")

    uri = await writer.save_tile("cat1", "coll", "WebMercatorQuad", 5, 17, 11, b"mvt-bytes", "mvt")
    assert uri == f"{base_uri}/tiles/collections/coll/WebMercatorQuad/5/17/11.mvt"
    assert await writer.check_tile_exists("cat1", "coll", "WebMercatorQuad", 5, 17, 11, "mvt") is True
    assert await writer.get_tile("cat1", "coll", "WebMercatorQuad", 5, 17, 11, "mvt") == b"mvt-bytes"

    deleted = await writer.delete_tiles_for_collection("cat1", "coll")
    assert deleted == 1
    assert await writer.check_tile_exists("cat1", "coll", "WebMercatorQuad", 5, 17, 11, "mvt") is False


@pytest.mark.asyncio
async def test_file_exists_and_delete_are_safe_on_missing_targets(tmp_path):
    ops = LocalStorageOps(root=str(tmp_path))
    missing = (tmp_path / "nope.bin").as_uri()
    assert await ops.file_exists(missing) is False
    await ops.delete_file(missing)  # must not raise
