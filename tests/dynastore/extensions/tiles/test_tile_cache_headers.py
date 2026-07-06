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


# ---------------------------------------------------------------------------
# Signing-path visibility: WARNING when redirect mode falls back to proxy
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_redirect_mode_url_none_then_proxy_hit_logs_warning(caplog):
    """When serve_mode=redirect but get_tile_url returns None (no exception) AND
    proxy finds the tile, a WARNING is logged so operators can diagnose why
    redirect is not working (blob.exists() permission issue on the bucket)."""
    import logging
    provider = MagicMock()
    provider.get_tile_url = AsyncMock(return_value=None)
    provider.get_tile = AsyncMock(return_value=b"\x1a\x03mvt")

    with caplog.at_level(logging.WARNING, logger="dynastore.extensions.tiles.tiles_service"):
        resp = await TilesService._try_cached_tile(
            provider, "catalog1", "coll1", "WebMercatorQuad", 5, 17, 11, "mvt",
            start_time=time.perf_counter(),
            serve_mode="redirect",
        )

    # Proxy served the tile (fallback)
    assert resp is not None
    assert resp.status_code == 200
    assert resp.headers["X-Tile-Source"] == "bucket_proxy"

    # WARNING must name the misconfiguration so operators know where to look
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert warnings, "Expected a WARNING when proxy serves a tile in redirect mode"
    assert any("serve_mode=redirect" in w and "proxy" in w for w in warnings)


@pytest.mark.asyncio
async def test_redirect_mode_proxy_hit_warns_about_blob_exists_or_sign(caplog):
    """Specific text check: warning mentions SA permissions so operators can act."""
    import logging
    provider = MagicMock()
    provider.get_tile_url = AsyncMock(return_value=None)
    provider.get_tile = AsyncMock(return_value=b"\x1a\x03mvt")

    with caplog.at_level(logging.WARNING, logger="dynastore.extensions.tiles.tiles_service"):
        await TilesService._try_cached_tile(
            provider, "cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt",
            start_time=time.perf_counter(),
            serve_mode="redirect",
        )

    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    combined = " ".join(warnings)
    assert "roles/storage.objectViewer" in combined or "blob.exists" in combined or "signBlob" in combined, (
        f"WARNING must guide the operator to the IAM fix; got: {combined!r}"
    )


@pytest.mark.asyncio
async def test_redirect_mode_url_none_no_proxy_hit_no_warning(caplog):
    """If redirect mode → url=None AND proxy also misses, no WARNING fires
    (it is just a normal cache miss, not a misconfiguration)."""
    import logging
    provider = MagicMock()
    provider.get_tile_url = AsyncMock(return_value=None)
    provider.get_tile = AsyncMock(return_value=None)  # genuine miss

    with caplog.at_level(logging.WARNING, logger="dynastore.extensions.tiles.tiles_service"):
        resp = await TilesService._try_cached_tile(
            provider, "cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt",
            start_time=time.perf_counter(),
            serve_mode="redirect",
        )

    assert resp is None
    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert not warnings, f"No WARNING expected on a genuine cache miss, got: {warnings}"


@pytest.mark.asyncio
async def test_redirect_signed_url_exception_type_in_warning(caplog):
    """When get_tile_url raises, the exception TYPE name appears in the warning."""
    import logging
    provider = MagicMock()
    provider.get_tile_url = AsyncMock(side_effect=ValueError("bad SA email"))
    provider.get_tile = AsyncMock(return_value=None)

    with caplog.at_level(logging.WARNING, logger="dynastore.extensions.tiles.tiles_service"):
        await TilesService._try_cached_tile(
            provider, "cat", "coll", "WebMercatorQuad", 5, 17, 11, "mvt",
            start_time=time.perf_counter(),
            serve_mode="redirect",
        )

    warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert warnings, "Expected WARNING on signing exception"
    assert any("ValueError" in w for w in warnings), (
        f"Exception class name should appear in warning; got {warnings}"
    )


# ---------------------------------------------------------------------------
# Raster render-cache path (`_try_render_cache`) honors per-request serve_mode.
# Redirect-averse clients (e.g. QGIS) can force `serve=proxy` on map tiles too.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_render_cache_redirect_default_uses_signed_url():
    """Default serve_mode='redirect': signed URL → 307 (offload to bucket)."""
    provider = MagicMock()
    provider.get_tile_url = AsyncMock(
        return_value="https://storage.googleapis.com/bkt/map/c/default/WMQ/5/17/11.png?sig=…"
    )
    provider.get_tile = AsyncMock(return_value=None)
    cfg = MagicMock()
    cfg.ttl_seconds = 60

    resp = await TilesService._try_render_cache(
        provider, "cat", "map/coll/default/WMQ/5/17/11.png", "WebMercatorQuad",
        5, 17, 11, "png", time.perf_counter(), cfg,
    )

    assert isinstance(resp, RedirectResponse)
    assert resp.status_code == 307
    assert resp.headers["X-Render-Source"] == "bucket_redirect"


@pytest.mark.asyncio
async def test_render_cache_proxy_mode_skips_signed_url():
    """serve_mode='proxy' never resolves a signed URL — streams bytes (QGIS-safe)."""
    provider = MagicMock()
    provider.get_tile_url = AsyncMock(
        return_value="https://storage.googleapis.com/bkt/map/tile.png?sig=x"
    )
    provider.get_tile = AsyncMock(return_value=b"\x89PNGbytes")
    cfg = MagicMock()
    cfg.ttl_seconds = 60

    resp = await TilesService._try_render_cache(
        provider, "cat", "map/coll/default/WMQ/5/17/11.png", "WebMercatorQuad",
        5, 17, 11, "png", time.perf_counter(), cfg,
        serve_mode="proxy",
    )

    provider.get_tile_url.assert_not_called()
    assert resp is not None
    assert resp.status_code == 200
    assert resp.body == b"\x89PNGbytes"
    assert resp.headers["X-Render-Source"] == "bucket_proxy"
