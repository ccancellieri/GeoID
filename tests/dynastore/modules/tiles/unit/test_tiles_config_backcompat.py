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

"""Back-compat validation + tile-writer selection tests.

Covers:
- Old configs (``TilesPreseedConfig.storage_priority``, ``GcpTileCacheConfig.
  cache_bucket``/``cache_prefix``) still validate unchanged.
- ``tiles_writers.resolve_effective_writers``: default injection with/without
  a StorageProtocol provider, and back-compat synthesis from
  ``GcpTileCacheConfig``/``storage_priority``.
- ``tiles_writers.select_tile_writer``: first-available-wins selection,
  fallback past an unavailable candidate (with a logged reason), loud
  failure when nothing is available, and hint-based preference among
  available candidates.
"""
from __future__ import annotations

import logging

import pytest

from dynastore.modules.tiles.tiles_config import TilesCachingConfig, TilesPreseedConfig
from dynastore.modules.tiles.tiles_writers import (
    TileWriterConfig,
    resolve_effective_writers,
    select_tile_writer,
)
from dynastore.modules.storage.hints import Hint


# ---------------------------------------------------------------------------
# Old configs still validate
# ---------------------------------------------------------------------------


def test_storage_priority_default_still_validates():
    cfg = TilesPreseedConfig()
    assert cfg.storage_priority == ["bucket", "pg"]


def test_storage_priority_pg_first_still_validates():
    cfg = TilesPreseedConfig(storage_priority=["pg"])
    assert cfg.storage_priority == ["pg"]


def test_gcp_tile_cache_config_still_validates():
    from dynastore.modules.gcp.gcp_config import GcpTileCacheConfig

    cfg = GcpTileCacheConfig(cache_bucket="fao-aip-geospatial-review-data", cache_prefix="demo/cat1")
    assert cfg.cache_bucket == "fao-aip-geospatial-review-data"
    assert cfg.cache_prefix == "demo/cat1"


def test_tiles_caching_config_writers_defaults_to_none():
    cfg = TilesCachingConfig()
    assert cfg.writers is None


# ---------------------------------------------------------------------------
# resolve_effective_writers: default injection + back-compat synthesis
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_default_injection_pg_only_without_storage_protocol(monkeypatch):
    import dynastore.modules as dmodules

    monkeypatch.setattr(dmodules, "get_protocol", lambda proto: None)
    writers = await resolve_effective_writers(TilesCachingConfig(), "cat1")
    assert [w.class_key() for w in writers] == ["pg_tile_writer_config"]


@pytest.mark.asyncio
async def test_default_injection_gcs_first_pg_fallback_with_storage_protocol(monkeypatch):
    import dynastore.modules as dmodules
    from dynastore.models.protocols import StorageProtocol

    def fake_get_protocol(proto):
        if proto is StorageProtocol:
            return object()
        return None

    monkeypatch.setattr(dmodules, "get_protocol", fake_get_protocol)
    monkeypatch.setattr("dynastore.tools.discovery.get_protocol", lambda proto: None)

    import dynastore.modules.gcp.tiles_storage  # noqa: F401 — registers gcs_tile_writer_config

    writers = await resolve_effective_writers(TilesCachingConfig(), "cat1")
    assert [w.class_key() for w in writers] == ["gcs_tile_writer_config", "pg_tile_writer_config"]


@pytest.mark.asyncio
async def test_legacy_gcp_tile_cache_config_synthesizes_gcs_writer(monkeypatch):
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.modules.gcp.gcp_config import GcpTileCacheConfig

    import dynastore.modules.gcp.tiles_storage  # noqa: F401 — registers gcs_tile_writer_config

    legacy_cfg = GcpTileCacheConfig(cache_bucket="legacy-bucket", cache_prefix="legacy/prefix")

    class _Stub:
        async def get_config(self, cls, catalog_id=None):
            assert cls is GcpTileCacheConfig
            return legacy_cfg

    monkeypatch.setattr(
        "dynastore.tools.discovery.get_protocol",
        lambda proto: _Stub() if proto is ConfigsProtocol else None,
    )

    writers = await resolve_effective_writers(TilesCachingConfig(), "cat1")
    assert len(writers) == 1
    assert writers[0].class_key() == "gcs_tile_writer_config"
    assert writers[0].bucket == "legacy-bucket"
    assert writers[0].prefix == "legacy/prefix"


@pytest.mark.asyncio
async def test_storage_priority_pg_first_soft_maps_to_pg_writer(monkeypatch):

    monkeypatch.setattr("dynastore.tools.discovery.get_protocol", lambda proto: None)

    writers = await resolve_effective_writers(
        TilesCachingConfig(), "cat1", storage_priority=["pg"],
    )
    assert [w.class_key() for w in writers] == ["pg_tile_writer_config"]


@pytest.mark.asyncio
async def test_explicit_writers_list_wins_over_all_defaults():
    from dynastore.modules.tiles.tiles_writers import PgTileWriterConfig

    cfg = TilesCachingConfig(writers=[PgTileWriterConfig(enabled=False)])
    writers = await resolve_effective_writers(cfg, "cat1")
    assert len(writers) == 1
    assert writers[0].enabled is False


# ---------------------------------------------------------------------------
# select_tile_writer: first-available-wins, fallback, loud failure, hints
# ---------------------------------------------------------------------------


class _FakeConfigA(TileWriterConfig):
    pass


class _FakeConfigB(TileWriterConfig):
    pass


class _FakeConfigC(TileWriterConfig):
    pass


@pytest.fixture(autouse=True)
def _register_fakes():
    """Register three fake writers once per test, tolerating re-registration
    across test runs in the same process (module-level registry)."""
    from dynastore.modules.tiles import tiles_writers as tw

    async def a_factory(config, cfg, catalog_id, ensure):
        return "writer-A"

    async def b_factory(config, cfg, catalog_id, ensure):
        return "writer-B"

    async def unavailable_factory(config, cfg, catalog_id, ensure):
        return None

    tw._factory_registry[_FakeConfigA.class_key()] = (a_factory, frozenset())
    tw._factory_registry[_FakeConfigB.class_key()] = (b_factory, frozenset({Hint.DURABLE}))
    tw._factory_registry[_FakeConfigC.class_key()] = (unavailable_factory, frozenset())
    yield


@pytest.mark.asyncio
async def test_first_available_wins_with_no_hints():
    writers = [_FakeConfigA(), _FakeConfigB()]
    key, instance = await select_tile_writer(writers, TilesCachingConfig(), "cat1", ensure=False)
    assert key == "_fake_config_a"
    assert instance == "writer-A"


@pytest.mark.asyncio
async def test_first_unavailable_falls_back_to_second_with_logged_reason(caplog):
    writers = [_FakeConfigC(), _FakeConfigA()]
    with caplog.at_level(logging.INFO, logger="dynastore.modules.tiles.tiles_writers"):
        key, instance = await select_tile_writer(writers, TilesCachingConfig(), "cat1", ensure=False)
    assert key == "_fake_config_a"
    assert instance == "writer-A"
    messages = " ".join(r.getMessage() for r in caplog.records)
    assert "_fake_config_c" in messages
    assert "did not resolve" in messages


@pytest.mark.asyncio
async def test_no_writer_available_raises_loud_error():
    writers = [_FakeConfigC()]
    with pytest.raises(RuntimeError, match="_fake_config_c"):
        await select_tile_writer(writers, TilesCachingConfig(), "cat1", ensure=False)


@pytest.mark.asyncio
async def test_hint_elevates_second_listed_available_writer():
    writers = [_FakeConfigA(), _FakeConfigB()]
    key, instance = await select_tile_writer(
        writers, TilesCachingConfig(), "cat1", ensure=False, hints=frozenset({Hint.DURABLE}),
    )
    assert key == "_fake_config_b"
    assert instance == "writer-B"


@pytest.mark.asyncio
async def test_hinted_writer_unavailable_falls_back_to_first_available(caplog):
    # C declares no hint affinity and is unavailable; A is available with no
    # affinity — requesting DURABLE (which nothing available claims) falls
    # back to the first available candidate.
    writers = [_FakeConfigC(), _FakeConfigA()]
    with caplog.at_level(logging.INFO, logger="dynastore.modules.tiles.tiles_writers"):
        key, instance = await select_tile_writer(
            writers, TilesCachingConfig(), "cat1", ensure=False, hints=frozenset({Hint.DURABLE}),
        )
    assert key == "_fake_config_a"
    assert instance == "writer-A"
    messages = " ".join(r.getMessage() for r in caplog.records)
    assert "no available writer matched hints" in messages
