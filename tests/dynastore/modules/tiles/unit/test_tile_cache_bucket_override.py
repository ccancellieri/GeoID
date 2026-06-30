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

"""Tests for the external tile-cache bucket override in TileBucketPreseedStorage.

The override is a GCP-specific concern read from ``GcpTileCacheConfig`` (the
config classified in the proxy tree by the protocol it backs), NOT from the
backend-agnostic ``TilesCachingConfig``. ``_resolve_bucket`` reads it through
the module-level ``_external_gcp_cache`` helper, so these tests drive the
override by patching that helper.

Validates that:
- When a cache_bucket is configured for a catalog, save_tile and read paths
  use that bucket. The effective prefix is ``cache_prefix`` verbatim when set,
  else ``{key_prefix}/{catalog_id}`` so per-catalog isolation is preserved in a
  shared bucket.
- The write path verifies the external bucket exists (geoid never provisions
  it) and raises when it does not.
- When no cache_bucket is configured (default), the old behavior is preserved:
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
from dynastore.modules.gcp.gcp_config import GcpTileCacheConfig


# ---------------------------------------------------------------------------
# GcpTileCacheConfig field validation
# ---------------------------------------------------------------------------


def test_gcp_tile_cache_config_defaults_to_none():
    cfg = GcpTileCacheConfig()
    assert cfg.cache_bucket is None
    assert cfg.cache_prefix is None


def test_gcp_tile_cache_config_accepts_bucket_and_prefix():
    cfg = GcpTileCacheConfig(cache_bucket="fao-aip-geospatial-review-data",
                             cache_prefix="demo_data/file/cat1")
    assert cfg.cache_bucket == "fao-aip-geospatial-review-data"
    assert cfg.cache_prefix == "demo_data/file/cat1"


# ---------------------------------------------------------------------------
# _resolve_bucket helper — external (GcpTileCacheConfig) path
# ---------------------------------------------------------------------------


def _patch_external(monkeypatch, *, bucket, prefix, exists=True):
    """Patch the module-level external-cache + existence helpers."""
    from dynastore.modules.gcp import tiles_storage as ts
    monkeypatch.setattr(ts, "_external_gcp_cache",
                        AsyncMock(return_value=(bucket, prefix)))
    monkeypatch.setattr(ts, "_external_bucket_exists",
                        AsyncMock(return_value=exists))


@pytest.mark.asyncio
async def test_resolve_bucket_uses_external_bucket_with_verbatim_prefix(monkeypatch):
    """cache_prefix set -> override bucket + that prefix verbatim (no catalog fold)."""
    from dynastore.modules.gcp.tiles_storage import _resolve_bucket

    _patch_external(monkeypatch, bucket="some-bucket", prefix="tiles/my-catalog")
    cfg = TilesCachingConfig()
    storage_provider = MagicMock()
    storage_provider.get_storage_identifier = AsyncMock(side_effect=AssertionError(
        "should not be called when override is set"
    ))

    bucket, prefix = await _resolve_bucket(cfg, "my-catalog", storage_provider)
    assert bucket == "some-bucket"
    assert prefix == "tiles/my-catalog"


@pytest.mark.asyncio
async def test_resolve_bucket_external_prefix_none_folds_catalog(monkeypatch):
    """cache_prefix=None falls back to {key_prefix}/{catalog_id} for the override path."""
    from dynastore.modules.gcp.tiles_storage import _resolve_bucket

    _patch_external(monkeypatch, bucket="some-bucket", prefix=None)
    cfg = TilesCachingConfig(key_prefix="tiles/collections")
    storage_provider = MagicMock()
    bucket, prefix = await _resolve_bucket(cfg, "cat1", storage_provider)
    assert bucket == "some-bucket"
    assert prefix == "tiles/collections/cat1"


@pytest.mark.asyncio
async def test_resolve_bucket_external_missing_on_write_raises(monkeypatch):
    """Write path verifies the external bucket exists; raises when it does not."""
    from dynastore.modules.gcp.tiles_storage import _resolve_bucket

    _patch_external(monkeypatch, bucket="ghost-bucket", prefix=None, exists=False)
    cfg = TilesCachingConfig()
    storage_provider = MagicMock()
    with pytest.raises(RuntimeError, match="does not exist"):
        await _resolve_bucket(cfg, "cat1", storage_provider, ensure=True)


@pytest.mark.asyncio
async def test_resolve_bucket_external_read_skips_existence_check(monkeypatch):
    """Read path must not probe existence (cheap, and a missing bucket = miss)."""
    from dynastore.modules.gcp import tiles_storage as ts
    from dynastore.modules.gcp.tiles_storage import _resolve_bucket

    monkeypatch.setattr(ts, "_external_gcp_cache",
                        AsyncMock(return_value=("some-bucket", "p")))
    probe = AsyncMock(return_value=False)
    monkeypatch.setattr(ts, "_external_bucket_exists", probe)

    bucket, prefix = await _resolve_bucket(TilesCachingConfig(), "cat1", MagicMock())
    assert bucket == "some-bucket"
    probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_resolve_bucket_default_reads_storage_provider(monkeypatch):
    """No override: get_storage_identifier is called (read path)."""
    from dynastore.modules.gcp.tiles_storage import _resolve_bucket

    _patch_external(monkeypatch, bucket=None, prefix=None)
    cfg = TilesCachingConfig()
    storage_provider = MagicMock()
    storage_provider.get_storage_identifier = AsyncMock(return_value="catalog-bucket")

    bucket, prefix = await _resolve_bucket(cfg, "cat1", storage_provider)
    assert bucket == "catalog-bucket"
    assert prefix == "tiles/collections"
    storage_provider.get_storage_identifier.assert_awaited_once_with("cat1")


@pytest.mark.asyncio
async def test_resolve_bucket_default_ensure_calls_ensure_storage(monkeypatch):
    """No override with ensure=True: ensure_storage_for_catalog is called (write path)."""
    from dynastore.modules.gcp.tiles_storage import _resolve_bucket

    _patch_external(monkeypatch, bucket=None, prefix=None)
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
# TileBucketPreseedStorage: save_tile / get_tile with override bucket
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_save_tile_uses_override_bucket(monkeypatch):
    """save_tile writes to override bucket with catalog-namespaced key."""
    from dynastore.modules.gcp.tiles_storage import TileBucketPreseedStorage

    catalog_id = "my-catalog"
    _install_config(monkeypatch, TilesCachingConfig())
    _patch_external(monkeypatch, bucket="shared-bucket", prefix="tiles/my-catalog")

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

    storage_client_mock.bucket.assert_called_once_with("shared-bucket")
    blob_key = bucket_mock.blob.call_args[0][0]
    assert blob_key == "tiles/my-catalog/admin0/WebMercatorQuad/5/17/11.mvt"
    assert result is not None
    assert "shared-bucket" in result


@pytest.mark.asyncio
async def test_get_tile_uses_override_bucket(monkeypatch):
    """get_tile reads from override bucket with catalog-namespaced key."""
    from dynastore.modules.gcp.tiles_storage import TileBucketPreseedStorage

    catalog_id = "my-catalog"
    _install_config(monkeypatch, TilesCachingConfig())
    _patch_external(monkeypatch, bucket="shared-bucket", prefix="tiles/my-catalog")

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

    _install_config(monkeypatch, TilesCachingConfig())
    _patch_external(monkeypatch, bucket=None, prefix=None)

    storage_provider_mock = MagicMock()
    storage_provider_mock.get_storage_identifier = AsyncMock(return_value=None)

    storage = TileBucketPreseedStorage()
    storage._get_storage_provider = MagicMock(return_value=storage_provider_mock)
    storage._get_client_provider = MagicMock()

    result = await storage.get_tile("cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt")

    assert result is None
    storage_provider_mock.get_storage_identifier.assert_awaited_once_with("cat")


@pytest.mark.asyncio
async def test_save_tile_no_override_bucket_free_catalog_logs_error(monkeypatch):
    """Without override, save_tile on a bucket-free catalog logs error but doesn't raise."""
    from dynastore.modules.gcp.tiles_storage import TileBucketPreseedStorage

    _install_config(monkeypatch, TilesCachingConfig())
    _patch_external(monkeypatch, bucket=None, prefix=None)

    storage_provider_mock = MagicMock()
    storage_provider_mock.ensure_storage_for_catalog = AsyncMock(return_value=None)

    storage = TileBucketPreseedStorage()
    storage._get_storage_provider = MagicMock(return_value=storage_provider_mock)
    storage._get_client_provider = MagicMock()

    result = await storage.save_tile("cat", "coll", "WebMercatorQuad", 5, 17, 11, b"data", "mvt")
    assert result is None


# ---------------------------------------------------------------------------
# Key namespacing: verify catalog_id isolation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_two_catalogs_use_distinct_key_prefixes(monkeypatch):
    """Two catalogs in the same override bucket produce disjoint blob paths.

    The preset folds catalog_id into cache_prefix, so each catalog gets a
    distinct verbatim prefix even when sharing a source-file folder."""
    from dynastore.modules.gcp import tiles_storage as ts
    from dynastore.modules.gcp.tiles_storage import _resolve_bucket, _build_blob_path

    async def fake_external(catalog_id):
        return "shared-bucket", f"tiles/{catalog_id}"

    monkeypatch.setattr(ts, "_external_gcp_cache", fake_external)
    cfg = TilesCachingConfig()
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
