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

"""Unit tests for the maps cold-boot cache warm-up (geoid#3207).

Uses the same ``osgeo`` stubbing technique as
``test_resolve_target_srid.py`` so ``maps_service`` (which imports
``renderer.py``, an unconditional GDAL import) is importable in a dev venv
without a real GDAL install.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import AsyncMock, MagicMock

import pytest


def _install_osgeo_stubs() -> bool:
    if "osgeo" in sys.modules:
        return False
    osgeo = types.ModuleType("osgeo")
    osgeo.__version__ = "3.x.stub"  # type: ignore[attr-defined]

    gdal = types.ModuleType("osgeo.gdal")
    gdal.Dataset = object  # type: ignore[attr-defined]
    gdal.GetDriverByName = lambda n: None  # type: ignore[attr-defined]
    gdal.VSIFOpenL = lambda *a: None  # type: ignore[attr-defined]
    gdal.VSIFSeekL = lambda *a: None  # type: ignore[attr-defined]
    gdal.VSIFTellL = lambda *a: 0  # type: ignore[attr-defined]
    gdal.VSIFReadL = lambda *a: b""  # type: ignore[attr-defined]
    gdal.VSIFCloseL = lambda *a: None  # type: ignore[attr-defined]
    gdal.Unlink = lambda *a: None  # type: ignore[attr-defined]
    gdal.RasterizeLayer = lambda *a, **kw: None  # type: ignore[attr-defined]

    ogr = types.ModuleType("osgeo.ogr")
    ogr.Geometry = object  # type: ignore[attr-defined]
    ogr.Layer = object  # type: ignore[attr-defined]
    ogr.Feature = lambda *a: None  # type: ignore[attr-defined]
    ogr.GetDriverByName = lambda n: None  # type: ignore[attr-defined]
    ogr.wkbLineString = 2  # type: ignore[attr-defined]
    ogr.wkbMultiPolygon = 6  # type: ignore[attr-defined]

    osr = types.ModuleType("osgeo.osr")
    osr.SpatialReference = object  # type: ignore[attr-defined]

    sys.modules.update({
        "osgeo": osgeo,
        "osgeo.gdal": gdal,
        "osgeo.ogr": ogr,
        "osgeo.osr": osr,
    })
    return True


def _uninstall_osgeo_stubs() -> None:
    for name in ("osgeo.osr", "osgeo.ogr", "osgeo.gdal", "osgeo"):
        sys.modules.pop(name, None)


_we_installed_osgeo_stubs = _install_osgeo_stubs()

from dynastore.extensions.maps import maps_service as ms  # noqa: E402

if _we_installed_osgeo_stubs:
    _uninstall_osgeo_stubs()


@pytest.mark.asyncio
async def test_warm_maps_caches_fetches_render_and_tiles_config(monkeypatch):
    """Warm-up front-loads RenderCachingConfig plus the platform-tier
    TilesConfig / TilesCachingConfig reads the render/tile hot path would
    otherwise pay cold."""
    render_config_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(ms, "_load_render_caching_config", render_config_mock)

    mgr = MagicMock()
    mgr.get_config = AsyncMock(return_value=None)
    monkeypatch.setattr(ms, "get_protocol", lambda proto: mgr)

    await ms._warm_maps_caches()

    render_config_mock.assert_awaited_once()
    assert mgr.get_config.await_count == 2
    fetched_classes = {call.args[0].__name__ for call in mgr.get_config.await_args_list}
    assert fetched_classes == {"TilesConfig", "TilesCachingConfig"}


@pytest.mark.asyncio
async def test_warm_maps_caches_skips_platform_config_when_protocol_absent(monkeypatch):
    """No PlatformConfigsProtocol registered (e.g. DB-free unit test /
    minimal deployment) -- warm-up degrades to a no-op past the render
    config fetch instead of raising."""
    render_config_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(ms, "_load_render_caching_config", render_config_mock)
    monkeypatch.setattr(ms, "get_protocol", lambda proto: None)

    await ms._warm_maps_caches()  # must not raise

    render_config_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_warm_maps_caches_one_config_failure_does_not_abort_the_rest(monkeypatch):
    """Each platform-tier fetch degrades independently -- TilesConfig
    raising must not stop TilesCachingConfig from still being warmed."""
    monkeypatch.setattr(ms, "_load_render_caching_config", AsyncMock(return_value=None))

    mgr = MagicMock()

    async def _get_config(cls, *a, **kw):
        if cls.__name__ == "TilesConfig":
            raise RuntimeError("boom")
        return None

    mgr.get_config = AsyncMock(side_effect=_get_config)
    monkeypatch.setattr(ms, "get_protocol", lambda proto: mgr)

    await ms._warm_maps_caches()  # must not raise

    assert mgr.get_config.await_count == 2


def test_maps_warmup_timeout_seconds_default_and_override(monkeypatch):
    monkeypatch.delenv("DYNASTORE_MAPS_WARMUP_TIMEOUT_SECONDS", raising=False)
    assert ms._maps_warmup_timeout_seconds() == 20

    monkeypatch.setenv("DYNASTORE_MAPS_WARMUP_TIMEOUT_SECONDS", "5")
    assert ms._maps_warmup_timeout_seconds() == 5
