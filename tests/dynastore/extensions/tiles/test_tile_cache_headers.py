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

"""Tests for `X-Tile-Cache` / `X-Tile-Source` response headers on `_try_cached_tile`.

Issue #475 Slice 2: every tile response advertises whether it was served
from the bucket cache (`hit`) or freshly generated (`miss`), with a
companion `X-Tile-Source` identifying the cache layer. Operators can
verify "second hit served from bucket" via curl without parsing logs.

Coverage at the route level (`get_vector_tile` -> postgis miss path) is
exercised by the existing tiles integration suite — these unit tests
pin the helper that owns the hit-redirect / hit-proxy responses.
"""
from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.responses import RedirectResponse

from dynastore.extensions.tiles.tiles_service import TilesService


@pytest.mark.asyncio
async def test_bucket_redirect_hit_carries_cache_headers():
    """Signed-URL redirect path: header + 307 + Location preserved."""
    provider = MagicMock()
    provider.get_tile_url = AsyncMock(
        return_value="https://storage.googleapis.com/bkt/tiles/c/WMQ/5/17/11.mvt?sig=…"
    )
    provider.get_tile = AsyncMock(return_value=None)

    resp = await TilesService._try_cached_tile(
        provider, "cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt",
        start_time=time.perf_counter(),
    )

    assert resp is not None
    assert isinstance(resp, RedirectResponse)
    assert resp.status_code == 307
    assert resp.headers["X-Tile-Cache"] == "hit"
    assert resp.headers["X-Tile-Source"] == "bucket_redirect"


@pytest.mark.asyncio
async def test_bucket_proxy_hit_carries_cache_headers():
    """Proxy-bytes path (signed URLs unavailable): header + body returned."""
    provider = MagicMock()
    provider.get_tile_url = AsyncMock(return_value=None)
    provider.get_tile = AsyncMock(return_value=b"\x1a\x07proxied")

    resp = await TilesService._try_cached_tile(
        provider, "cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt",
        start_time=time.perf_counter(),
    )

    assert resp is not None
    assert resp.status_code == 200
    assert resp.body == b"\x1a\x07proxied"
    assert resp.headers["X-Tile-Cache"] == "hit"
    assert resp.headers["X-Tile-Source"] == "bucket_proxy"
    assert resp.headers["content-type"] == "application/vnd.mapbox-vector-tile"


@pytest.mark.asyncio
async def test_full_miss_returns_none_for_caller_to_regenerate():
    """Neither url nor tile -> None; caller falls through to PostGIS generation."""
    provider = MagicMock()
    provider.get_tile_url = AsyncMock(return_value=None)
    provider.get_tile = AsyncMock(return_value=None)

    resp = await TilesService._try_cached_tile(
        provider, "cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt",
        start_time=time.perf_counter(),
    )
    assert resp is None


@pytest.mark.asyncio
async def test_redirect_signing_failure_falls_back_to_proxy_then_miss():
    """Signing raises in redirect mode → proxy fallback; tile absent → None."""
    provider = MagicMock()
    provider.get_tile_url = AsyncMock(side_effect=RuntimeError("GCS unavailable"))
    provider.get_tile = AsyncMock(return_value=None)

    resp = await TilesService._try_cached_tile(
        provider, "cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt",
        start_time=time.perf_counter(),
        serve_mode="redirect",
    )
    # Proxy was attempted (fallback), tile absent → cache miss
    provider.get_tile.assert_called_once()
    assert resp is None


@pytest.mark.asyncio
async def test_redirect_signing_failure_falls_back_to_proxy_hit():
    """Signing raises in redirect mode → proxy fallback returns tile bytes."""
    provider = MagicMock()
    provider.get_tile_url = AsyncMock(side_effect=RuntimeError("IAM signing denied"))
    provider.get_tile = AsyncMock(return_value=b"\x1a\x05bytes")

    resp = await TilesService._try_cached_tile(
        provider, "cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt",
        start_time=time.perf_counter(),
        serve_mode="redirect",
    )
    assert resp is not None
    assert resp.status_code == 200
    assert resp.headers["X-Tile-Source"] == "bucket_proxy"


@pytest.mark.asyncio
async def test_proxy_mode_skips_signed_url():
    """serve_mode='proxy' never calls get_tile_url — streams bytes directly."""
    provider = MagicMock()
    provider.get_tile_url = AsyncMock(
        return_value="https://storage.googleapis.com/bkt/tile.mvt?sig=x"
    )
    provider.get_tile = AsyncMock(return_value=b"\x1a\x03mvt")

    resp = await TilesService._try_cached_tile(
        provider, "cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt",
        start_time=time.perf_counter(),
        serve_mode="proxy",
    )
    provider.get_tile_url.assert_not_called()
    assert resp is not None
    assert resp.status_code == 200
    assert resp.headers["X-Tile-Source"] == "bucket_proxy"


@pytest.mark.asyncio
async def test_proxy_mode_miss_returns_none():
    """serve_mode='proxy' + empty bucket → None (cache miss)."""
    provider = MagicMock()
    provider.get_tile_url = AsyncMock(return_value="https://should-not-be-called")
    provider.get_tile = AsyncMock(return_value=None)

    resp = await TilesService._try_cached_tile(
        provider, "cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt",
        start_time=time.perf_counter(),
        serve_mode="proxy",
    )
    provider.get_tile_url.assert_not_called()
    assert resp is None


@pytest.mark.asyncio
async def test_hit_log_line_uses_structured_key_value_format(caplog):
    """`tile_cache event=hit source=bucket_proxy …` — Kibana-parseable."""
    import logging
    provider = MagicMock()
    provider.get_tile_url = AsyncMock(return_value=None)
    provider.get_tile = AsyncMock(return_value=b"xx")

    with caplog.at_level(logging.INFO, logger="dynastore.extensions.tiles.tiles_service"):
        await TilesService._try_cached_tile(
            provider, "admin0", "boundaries", "WebMercatorQuad", 5, 17, 11, "mvt",
            start_time=time.perf_counter(),
        )

    hit_lines = [
        r.getMessage() for r in caplog.records
        if "tile_cache event=hit" in r.getMessage()
    ]
    assert len(hit_lines) == 1
    msg = hit_lines[0]
    assert "source=bucket_proxy" in msg
    assert "catalog=admin0" in msg
    assert "collection=boundaries" in msg
    assert "z=5 x=17 y=11" in msg
    assert "duration_ms=" in msg
    assert "bytes=2" in msg
