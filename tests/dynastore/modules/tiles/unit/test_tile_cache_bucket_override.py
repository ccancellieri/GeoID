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

"""Tests for the cache_bucket_override feature in TileBucketPreseedStorage.

Validates that:
- When cache_bucket_override is set, save_tile and read paths use that bucket
  with a catalog-namespaced key prefix (per-catalog isolation in shared bucket).
- When cache_bucket_override is None (default), the old behavior is preserved:
  bucket is resolved via StorageProtocol.
- The catalog_id written into the override bucket path is the external (logical)
  id passed into the call, never anything internal.
"""
from __future__ import annotations

from typing import Optional, Type
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.plugin_config import PluginConfig
from dynastore.modules.tiles.tiles_config import TilesCachingConfig


# ---------------------------------------------------------------------------
# TilesCachingConfig field validation
# ---------------------------------------------------------------------------


def test_cache_bucket_override_defaults_to_none():
    cfg = TilesCachingConfig()
    assert cfg.cache_bucket_override is None
    assert cfg.cache_bucket_prefix is None


def test_cache_bucket_override_accepts_valid_bucket_name():
    cfg = TilesCachingConfig(cache_bucket_override="fao-aip-geospatial-review-data")
    assert cfg.cache_bucket_override == "fao-aip-geospatial-review-data"


def test_cache_bucket_prefix_accepts_valid_prefix():
    cfg = TilesCachingConfig(
        cache_bucket_override="some-bucket",
        cache_bucket_prefix="tiles",
    )
    assert cfg.cache_bucket_prefix == "tiles"


def test_cache_bucket_prefix_rejects_leading_slash():
    with pytest.raises(Exception):
        TilesCachingConfig(
            cache_bucket_override="some-bucket",
            cache_bucket_prefix="/leading",
        )


def test_cache_bucket_override_too_short_rejected():
    with pytest.raises(Exception):
        TilesCachingConfig(cache_bucket_override="ab")  # min_length=3


# ---------------------------------------------------------------------------
# _resolve_bucket helper
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_bucket_uses_override_bucket():
    """When cache_bucket_override is set, override bucket + namespaced prefix returned."""
    from dynastore.modules.gcp.tiles_storage import _resolve_bucket

    cfg = TilesCachingConfig(
        cache_bucket_override="some-bucket",
        cache_bucket_prefix="tiles",
    )
    storage_provider = MagicMock()
    storage_provider.get_storage_identifier = AsyncMock(side_effect=AssertionError(
        "should not be called when override is set"
    ))

    bucket, prefix = await _resolve_bucket(cfg, "my-catalog", storage_provider)
    assert bucket == "some-bucket"
    assert prefix == "tiles/my-catalog"


@pytest.mark.asyncio
async def test_resolve_bucket_override_defaults_prefix_to_key_prefix():
    """cache_bucket_prefix=None falls back to key_prefix for the override path."""
    from dynastore.modules.gcp.tiles_storage import _resolve_bucket

    cfg = TilesCachingConfig(
        cache_bucket_override="some-bucket",
        cache_bucket_prefix=None,
        key_prefix="tiles/collections",
    )
    storage_provider = MagicMock()
    bucket, prefix = await _resolve_bucket(cfg, "cat1", storage_provider)
    assert bucket == "some-bucket"
    assert prefix == "tiles/collections/cat1"


@pytest.mark.asyncio
async def test_resolve_bucket_default_reads_storage_provider():
    """No override: get_storage_identifier is called (read path)."""
    from dynastore.modules.gcp.tiles_storage import _resolve_bucket

    cfg = TilesCachingConfig()  # no override
    storage_provider = MagicMock()
    storage_provider.get_storage_identifier = AsyncMock(return_value="catalog-bucket")

    bucket, prefix = await _resolve_bucket(cfg, "cat1", storage_provider)
    assert bucket == "catalog-bucket"
    assert prefix == "tiles/collections"
    storage_provider.get_storage_identifier.assert_awaited_once_with("cat1")


@pytest.mark.asyncio
async def test_resolve_bucket_default_ensure_calls_ensure_storage():
    """No override with ensure=True: ensure_storage_for_catalog is called (write path)."""
    from dynastore.modules.gcp.tiles_storage import _resolve_bucket

    cfg = TilesCachingConfig()
    storage_provider = MagicMock()
    storage_provider.ensure_storage_for_catalog = AsyncMock(return_value="prov-bucket")

    bucket, prefix = await _resolve_bucket(cfg, "cat1", storage_provider, ensure=True)
    assert bucket == "prov-bucket"
    storage_provider.ensure_storage_for_catalog.assert_awaited_once_with("cat1")


# ---------------------------------------------------------------------------
# Stub helpers for TileBucketPreseedStorage tests
# ---------------------------------------------------------------------------


class _StubPlatformConfigsProtocol:
    is_platform_manager = True

    def __init__(self, cfg: Optional[TilesCachingConfig]) -> None:
        self._cfg = cfg

    async def get_config(self, config_cls: Type[PluginConfig], ctx=None) -> PluginConfig:
        if self._cfg is None or config_cls is not TilesCachingConfig:
            return TilesCachingConfig()
        return self._cfg

    async def set_config(self, *a, **kw) -> None: ...
    async def list_configs(self): return {}


def _install_config(monkeypatch, cfg: TilesCachingConfig):
    stub = _StubPlatformConfigsProtocol(cfg)
    from dynastore.models.protocols import platform_configs as pc_mod
    from dynastore.tools import discovery

    def fake_get_protocol(proto, *a, **kw):
        if proto is pc_mod.PlatformConfigsProtocol:
            return stub
        return None

    monkeypatch.setattr(discovery, "get_protocol", fake_get_protocol)


# ---------------------------------------------------------------------------
# TileBucketPreseedStorage: save_tile with override bucket
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_save_tile_uses_override_bucket(monkeypatch):
    """save_tile writes to override bucket with catalog-namespaced key."""
    from dynastore.modules.gcp.tiles_storage import TileBucketPreseedStorage

    catalog_id = "my-catalog"
    cfg = TilesCachingConfig(
        cache_bucket_override="shared-bucket",
        cache_bucket_prefix="tiles",
    )
    _install_config(monkeypatch, cfg)

    # Mock GCS objects
    blob_mock = MagicMock()
    blob_mock.upload_from_string = MagicMock()
    blob_mock.patch = MagicMock()

    bucket_mock = MagicMock()
    bucket_mock.blob = MagicMock(return_value=blob_mock)

    storage_client_mock = MagicMock()
    storage_client_mock.bucket = MagicMock(return_value=bucket_mock)

    client_provider_mock = MagicMock()
    client_provider_mock.get_storage_client = MagicMock(return_value=storage_client_mock)

    # StorageProtocol should NOT be consulted for override path
    storage_provider_mock = MagicMock()
    storage_provider_mock.ensure_storage_for_catalog = AsyncMock(
        side_effect=AssertionError("StorageProtocol must not be consulted when override is set")
    )

    storage = TileBucketPreseedStorage()
    storage._get_storage_provider = MagicMock(return_value=storage_provider_mock)
    storage._get_client_provider = MagicMock(return_value=client_provider_mock)

    with patch("dynastore.modules.gcp.tiles_storage.run_in_thread", new=AsyncMock(return_value=None)):
        result = await storage.save_tile(
            catalog_id, "admin0", "WebMercatorQuad", 5, 17, 11, b"data", "mvt"
        )

    # bucket() must have been called with the override bucket name
    storage_client_mock.bucket.assert_called_once_with("shared-bucket")
    # blob key must include catalog_id for namespace isolation
    blob_key = bucket_mock.blob.call_args[0][0]
    assert blob_key == "tiles/my-catalog/admin0/WebMercatorQuad/5/17/11.mvt"
    assert result is not None
    assert "shared-bucket" in result


@pytest.mark.asyncio
async def test_get_tile_uses_override_bucket(monkeypatch):
    """get_tile reads from override bucket with catalog-namespaced key."""
    from dynastore.modules.gcp.tiles_storage import TileBucketPreseedStorage

    catalog_id = "my-catalog"
    cfg = TilesCachingConfig(
        cache_bucket_override="shared-bucket",
        cache_bucket_prefix="tiles",
    )
    _install_config(monkeypatch, cfg)

    tile_data = b"tile-bytes"

    blob_mock = MagicMock()
    blob_mock.download_as_bytes = MagicMock(return_value=tile_data)

    bucket_mock = MagicMock()
    bucket_mock.blob = MagicMock(return_value=blob_mock)

    storage_client_mock = MagicMock()
    storage_client_mock.bucket = MagicMock(return_value=bucket_mock)

    client_provider_mock = MagicMock()
    client_provider_mock.get_storage_client = MagicMock(return_value=storage_client_mock)

    storage_provider_mock = MagicMock()
    storage_provider_mock.get_storage_identifier = AsyncMock(
        side_effect=AssertionError("StorageProtocol must not be consulted when override is set")
    )

    storage = TileBucketPreseedStorage()
    storage._get_storage_provider = MagicMock(return_value=storage_provider_mock)
    storage._get_client_provider = MagicMock(return_value=client_provider_mock)

    with patch("dynastore.modules.gcp.tiles_storage.run_in_thread", new=AsyncMock(return_value=tile_data)):
        result = await storage.get_tile(
            catalog_id, "admin0", "WebMercatorQuad", 5, 17, 11, "mvt"
        )

    storage_client_mock.bucket.assert_called_once_with("shared-bucket")
    blob_key = bucket_mock.blob.call_args[0][0]
    assert blob_key == "tiles/my-catalog/admin0/WebMercatorQuad/5/17/11.mvt"
    assert result == tile_data


@pytest.mark.asyncio
async def test_no_override_falls_back_to_storage_protocol(monkeypatch):
    """Without override, bucket comes from StorageProtocol (old behavior)."""
    from dynastore.modules.gcp.tiles_storage import TileBucketPreseedStorage

    cfg = TilesCachingConfig()  # no override
    _install_config(monkeypatch, cfg)

    storage_provider_mock = MagicMock()
    storage_provider_mock.get_storage_identifier = AsyncMock(return_value=None)

    storage = TileBucketPreseedStorage()
    storage._get_storage_provider = MagicMock(return_value=storage_provider_mock)
    storage._get_client_provider = MagicMock()

    result = await storage.get_tile("cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt")

    # No bucket -> miss
    assert result is None
    storage_provider_mock.get_storage_identifier.assert_awaited_once_with("cat")


@pytest.mark.asyncio
async def test_save_tile_no_override_bucket_free_catalog_logs_error(monkeypatch):
    """Without override, save_tile on a bucket-free catalog logs error but doesn't raise."""
    from dynastore.modules.gcp.tiles_storage import TileBucketPreseedStorage

    cfg = TilesCachingConfig()  # no override
    _install_config(monkeypatch, cfg)

    storage_provider_mock = MagicMock()
    storage_provider_mock.ensure_storage_for_catalog = AsyncMock(return_value=None)

    storage = TileBucketPreseedStorage()
    storage._get_storage_provider = MagicMock(return_value=storage_provider_mock)
    storage._get_client_provider = MagicMock()

    # Should not raise; error is swallowed (cache failures must not crash)
    result = await storage.save_tile("cat", "coll", "WebMercatorQuad", 5, 17, 11, b"data", "mvt")
    assert result is None


# ---------------------------------------------------------------------------
# Key namespacing: verify catalog_id isolation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_two_catalogs_use_distinct_key_prefixes():
    """Two catalogs in the same override bucket produce disjoint blob paths."""
    from dynastore.modules.gcp.tiles_storage import _resolve_bucket, _build_blob_path

    cfg = TilesCachingConfig(
        cache_bucket_override="shared-bucket",
        cache_bucket_prefix="tiles",
    )
    storage_provider = MagicMock()

    _, prefix_a = await _resolve_bucket(cfg, "catalog-a", storage_provider)
    _, prefix_b = await _resolve_bucket(cfg, "catalog-b", storage_provider)

    path_a = _build_blob_path(prefix_a, "admin0", "WebMercatorQuad", 5, 17, 11, "mvt")
    path_b = _build_blob_path(prefix_b, "admin0", "WebMercatorQuad", 5, 17, 11, "mvt")

    assert path_a != path_b
    assert "catalog-a" in path_a
    assert "catalog-b" in path_b
    assert path_a == "tiles/catalog-a/admin0/WebMercatorQuad/5/17/11.mvt"
    assert path_b == "tiles/catalog-b/admin0/WebMercatorQuad/5/17/11.mvt"
