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

"""Asset-URI to driver-config bridge + cascade-delete guard (#377)."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest


def _driver():
    from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver
    return ItemsDuckdbDriver()


def _config(**kw):
    from dynastore.modules.storage.driver_config import ItemsDuckdbDriverConfig
    return ItemsDuckdbDriverConfig(**kw)


def test_asset_uri_to_path_strips_file_scheme():
    D = _driver()
    assert D._asset_uri_to_path("file:///data/x.gpkg") == "/data/x.gpkg"
    assert D._asset_uri_to_path("gs://bucket/x.parquet") == "gs://bucket/x.parquet"
    assert D._asset_uri_to_path("https://h/x.geojson") == "https://h/x.geojson"
    assert D._asset_uri_to_path(None) is None


@pytest.mark.asyncio
async def test_resolve_asset_path_binds_asset_uri():
    D = _driver()
    cfg = _config(asset_id="asset-1", format="gpkg")
    fake_asset = SimpleNamespace(uri="gs://b/data.gpkg", href=None)
    assets = SimpleNamespace(get_asset=AsyncMock(return_value=fake_asset))
    with patch("dynastore.tools.discovery.get_protocol", return_value=assets):
        out = await D._resolve_asset_path(cfg, "cat1", "col1")
    assert out.path == "gs://b/data.gpkg"
    assets.get_asset.assert_awaited_once_with("asset-1", "cat1", "col1")


@pytest.mark.asyncio
async def test_resolve_asset_path_noop_without_asset_id():
    D = _driver()
    cfg = _config(path="/local/x.parquet", format="parquet")
    # No assets protocol should even be consulted.
    with patch("dynastore.tools.discovery.get_protocol", return_value=None) as gp:
        out = await D._resolve_asset_path(cfg, "cat1", "col1")
    assert out.path == "/local/x.parquet"
    gp.assert_not_called()


@pytest.mark.asyncio
async def test_resolve_asset_path_missing_asset_keeps_existing_path():
    D = _driver()
    cfg = _config(asset_id="gone", path="/fallback.parquet", format="parquet")
    assets = SimpleNamespace(get_asset=AsyncMock(return_value=None))
    with patch("dynastore.tools.discovery.get_protocol", return_value=assets):
        out = await D._resolve_asset_path(cfg, "cat1", "col1")
    assert out.path == "/fallback.parquet"


@pytest.mark.asyncio
async def test_register_asset_guard_uses_cascade_delete_false():
    D = _driver()
    add_ref = AsyncMock()
    assets = SimpleNamespace(add_asset_reference=add_ref)
    with patch("dynastore.tools.discovery.get_protocol", return_value=assets):
        await D._register_asset_guard("asset-1", "cat1", "col1")
    add_ref.assert_awaited_once()
    # cascade_delete must be False (protective — blocks hard-delete of the file).
    _, kwargs = add_ref.await_args
    assert kwargs.get("cascade_delete") is False


@pytest.mark.asyncio
async def test_register_asset_guard_is_best_effort():
    """A failure to register the guard must not raise into provisioning."""
    D = _driver()
    assets = SimpleNamespace(add_asset_reference=AsyncMock(side_effect=RuntimeError("boom")))
    with patch("dynastore.tools.discovery.get_protocol", return_value=assets):
        await D._register_asset_guard("asset-1", "cat1", "col1")  # must not raise
