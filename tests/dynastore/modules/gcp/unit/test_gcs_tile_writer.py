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

"""Tests for the GCS tile writer (``GcsTileWriterConfig`` + its factory).

The override is a GCP-specific concern read from ``GcsTileWriterConfig``
itself (``bucket``/``prefix`` fields), OR synthesized from the legacy
``GcpTileCacheConfig.cache_bucket``/``cache_prefix`` by
``tiles_writers.resolve_effective_writers`` (tested separately).

Validates that:
- An explicit ``bucket`` is used with ``prefix`` verbatim when set, else
  ``{key_prefix}/{catalog_id}`` so per-catalog isolation is preserved in a
  shared bucket.
- The write path (``ensure=True``) verifies the explicit bucket exists
  (geoid never provisions it) and raises when it does not.
- The read path does NOT probe existence (cheap; a missing bucket is
  naturally a miss further down).
- No ``bucket`` configured (default) resolves via the catalog's own managed
  bucket through ``StorageProtocol``.
- No ``StorageProtocol`` registered -> factory returns ``None``
  (unavailable), not an exception.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.modules.tiles.tiles_config import TilesCachingConfig


def _patch_bucket_exists(monkeypatch, *, exists: bool):
    from dynastore.modules.gcp import tiles_storage as gts
    monkeypatch.setattr(gts, "external_bucket_exists", AsyncMock(return_value=exists))


def _patch_storage_provider(monkeypatch, provider):
    from dynastore.modules.gcp import tiles_storage as gts
    monkeypatch.setattr(gts, "get_storage_for_scheme", lambda scheme: provider if scheme == "gs" else None)


@pytest.mark.asyncio
async def test_explicit_bucket_with_verbatim_prefix(monkeypatch):
    """bucket + prefix set -> writer targets that bucket/prefix verbatim (no catalog fold)."""
    from dynastore.modules.gcp.tiles_storage import GcsTileWriterConfig, _gcs_writer_factory

    storage = MagicMock()
    storage.get_storage_identifier = AsyncMock(side_effect=AssertionError(
        "should not be called when an explicit bucket is set"
    ))
    _patch_storage_provider(monkeypatch, storage)
    _patch_bucket_exists(monkeypatch, exists=True)

    config = GcsTileWriterConfig(bucket="some-bucket", prefix="tiles/my-catalog")
    writer = await _gcs_writer_factory(config, TilesCachingConfig(), "my-catalog", False)

    assert writer is not None
    assert writer._base_uri == "gs://some-bucket"
    assert writer._prefix == "tiles/my-catalog"


@pytest.mark.asyncio
async def test_explicit_bucket_prefix_none_folds_catalog(monkeypatch):
    """prefix unset on an explicit bucket -> {key_prefix}/{catalog_id}."""
    from dynastore.modules.gcp.tiles_storage import GcsTileWriterConfig, _gcs_writer_factory

    storage = MagicMock()
    _patch_storage_provider(monkeypatch, storage)
    _patch_bucket_exists(monkeypatch, exists=True)

    config = GcsTileWriterConfig(bucket="some-bucket")
    cfg = TilesCachingConfig(key_prefix="tiles/collections")
    writer = await _gcs_writer_factory(config, cfg, "cat1", False)

    assert writer._base_uri == "gs://some-bucket"
    assert writer._prefix == "tiles/collections/cat1"


@pytest.mark.asyncio
async def test_explicit_bucket_missing_on_write_raises(monkeypatch):
    """Write path (ensure=True) verifies the explicit bucket exists; raises when it does not."""
    from dynastore.modules.gcp.tiles_storage import GcsTileWriterConfig, _gcs_writer_factory

    storage = MagicMock()
    _patch_storage_provider(monkeypatch, storage)
    _patch_bucket_exists(monkeypatch, exists=False)

    config = GcsTileWriterConfig(bucket="ghost-bucket")
    with pytest.raises(RuntimeError, match="does not exist"):
        await _gcs_writer_factory(config, TilesCachingConfig(), "cat1", True)


@pytest.mark.asyncio
async def test_explicit_bucket_read_skips_existence_check(monkeypatch):
    """Read path (ensure=False) must not probe existence."""
    from dynastore.modules.gcp.tiles_storage import GcsTileWriterConfig, _gcs_writer_factory

    storage = MagicMock()
    _patch_storage_provider(monkeypatch, storage)
    probe = AsyncMock(return_value=False)
    from dynastore.modules.gcp import tiles_storage as gts
    monkeypatch.setattr(gts, "external_bucket_exists", probe)

    config = GcsTileWriterConfig(bucket="some-bucket")
    writer = await _gcs_writer_factory(config, TilesCachingConfig(), "cat1", False)

    assert writer is not None
    probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_bucket_reads_managed_bucket_via_storage_protocol(monkeypatch):
    """No explicit bucket: get_storage_identifier is called (read path)."""
    from dynastore.modules.gcp.tiles_storage import GcsTileWriterConfig, _gcs_writer_factory

    storage = MagicMock()
    storage.get_storage_identifier = AsyncMock(return_value="catalog-bucket")
    _patch_storage_provider(monkeypatch, storage)

    config = GcsTileWriterConfig()
    cfg = TilesCachingConfig(key_prefix="tiles/collections")
    writer = await _gcs_writer_factory(config, cfg, "cat1", False)

    assert writer._base_uri == "gs://catalog-bucket"
    assert writer._prefix == "tiles/collections"
    storage.get_storage_identifier.assert_awaited_once_with("cat1")


@pytest.mark.asyncio
async def test_no_bucket_ensure_calls_ensure_storage(monkeypatch):
    """No explicit bucket + ensure=True: ensure_storage_for_catalog is called (write path)."""
    from dynastore.modules.gcp.tiles_storage import GcsTileWriterConfig, _gcs_writer_factory

    storage = MagicMock()
    storage.ensure_storage_for_catalog = AsyncMock(return_value="prov-bucket")
    _patch_storage_provider(monkeypatch, storage)

    config = GcsTileWriterConfig()
    writer = await _gcs_writer_factory(config, TilesCachingConfig(), "cat1", True)

    assert writer._base_uri == "gs://prov-bucket"
    storage.ensure_storage_for_catalog.assert_awaited_once_with("cat1")


@pytest.mark.asyncio
async def test_no_storage_protocol_returns_none(monkeypatch):
    """No StorageProtocol claims 'gs' -> factory reports unavailable (None), not an exception."""
    from dynastore.modules.gcp.tiles_storage import GcsTileWriterConfig, _gcs_writer_factory

    _patch_storage_provider(monkeypatch, None)

    config = GcsTileWriterConfig()
    writer = await _gcs_writer_factory(config, TilesCachingConfig(), "cat1", False)
    assert writer is None


@pytest.mark.asyncio
async def test_two_catalogs_use_distinct_key_prefixes(monkeypatch):
    """Two catalogs sharing an explicit bucket produce disjoint blob paths."""
    from dynastore.modules.gcp.tiles_storage import GcsTileWriterConfig, _gcs_writer_factory
    from dynastore.modules.tiles.tile_blob_storage import _build_blob_path

    storage = MagicMock()
    _patch_storage_provider(monkeypatch, storage)
    _patch_bucket_exists(monkeypatch, exists=True)

    cfg = TilesCachingConfig()
    writer_a = await _gcs_writer_factory(GcsTileWriterConfig(bucket="shared-bucket"), cfg, "catalog-a", False)
    writer_b = await _gcs_writer_factory(GcsTileWriterConfig(bucket="shared-bucket"), cfg, "catalog-b", False)

    path_a = _build_blob_path(writer_a._prefix, "admin0", "WebMercatorQuad", 5, 17, 11, "mvt")
    path_b = _build_blob_path(writer_b._prefix, "admin0", "WebMercatorQuad", 5, 17, 11, "mvt")

    assert path_a != path_b
    assert "catalog-a" in path_a
    assert "catalog-b" in path_b
