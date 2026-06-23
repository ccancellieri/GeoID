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

"""Asset-URI to driver-config bridge + cascade-delete guard (#377, #2296)."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest


def _driver():
    from dynastore.modules.storage.drivers.duckdb import ItemsDuckdbDriver

    class _TestableDriver(ItemsDuckdbDriver):
        def get_config_service(self):
            return None

    return _TestableDriver()


def _config(**kw):
    from dynastore.modules.storage.driver_config import ItemsDuckdbDriverConfig
    return ItemsDuckdbDriverConfig(**kw)


_PHYS_ID = "01966b7f-0000-7000-8000-000000000001"


def test_asset_uri_to_path_strips_file_scheme():
    D = _driver()
    assert D._asset_uri_to_path("file:///data/x.gpkg") == "/data/x.gpkg"
    assert D._asset_uri_to_path("gs://bucket/x.parquet") == "gs://bucket/x.parquet"
    assert D._asset_uri_to_path("https://h/x.geojson") == "https://h/x.geojson"
    assert D._asset_uri_to_path(None) is None


# ---------------------------------------------------------------------------
# Legacy path: asset_id present, no asset_physical_id
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_resolve_asset_path_binds_asset_uri():
    """First resolve: uses logical asset_id and stamps physical_id on result."""
    D = _driver()
    cfg = _config(asset_id="asset-1", format="gpkg")
    fake_asset = SimpleNamespace(uri="gs://b/data.gpkg", href=None)
    assets = SimpleNamespace(
        get_asset=AsyncMock(return_value=fake_asset),
        resolve_asset_physical_id=AsyncMock(return_value=_PHYS_ID),
        resolve_asset_logical_id=AsyncMock(return_value="asset-1"),
    )
    with patch("dynastore.tools.discovery.get_protocol", return_value=assets):
        out = await D._resolve_asset_path(cfg, "cat1", "col1")
    assert out is not None
    assert out.path == "gs://b/data.gpkg"
    # physical_id must be stamped for future rename-safe dispatch
    assert out.asset_physical_id == _PHYS_ID  # pyright: ignore[reportAttributeAccessIssue]
    assets.get_asset.assert_awaited_once_with("asset-1", "cat1", "col1")
    assets.resolve_asset_physical_id.assert_awaited_once_with("cat1", "asset-1", "col1")


@pytest.mark.asyncio
async def test_resolve_asset_path_noop_without_asset_id():
    D = _driver()
    cfg = _config(path="/local/x.parquet", format="parquet")
    # No assets protocol should even be consulted.
    with patch("dynastore.tools.discovery.get_protocol", return_value=None) as gp:
        out = await D._resolve_asset_path(cfg, "cat1", "col1")
    assert out is not None
    assert out.path == "/local/x.parquet"
    gp.assert_not_called()


@pytest.mark.asyncio
async def test_resolve_asset_path_missing_asset_keeps_existing_path():
    D = _driver()
    cfg = _config(asset_id="gone", path="/fallback.parquet", format="parquet")
    assets = SimpleNamespace(
        get_asset=AsyncMock(return_value=None),
        resolve_asset_physical_id=AsyncMock(return_value=None),
        resolve_asset_logical_id=AsyncMock(return_value=None),
    )
    with patch("dynastore.tools.discovery.get_protocol", return_value=assets):
        out = await D._resolve_asset_path(cfg, "cat1", "col1")
    assert out is not None
    assert out.path == "/fallback.parquet"


# ---------------------------------------------------------------------------
# Rename-safe path: asset_physical_id already persisted
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_resolve_asset_path_uses_physical_id_when_set():
    """When asset_physical_id is persisted, dispatch resolves via physical_id
    so a renamed logical asset_id never strands the config."""
    D = _driver()
    # Config carries physical_id + the OLD logical asset_id (stale after rename).
    cfg = _config(asset_id="old-name", asset_physical_id=_PHYS_ID, format="parquet")
    fake_asset = SimpleNamespace(uri="gs://b/data.parquet", href=None)
    assets = SimpleNamespace(
        # resolve_asset_logical_id returns the NEW (current) logical id.
        resolve_asset_logical_id=AsyncMock(return_value="new-name"),
        get_asset=AsyncMock(return_value=fake_asset),
        # resolve_asset_physical_id must NOT be called on the rename-safe path.
        resolve_asset_physical_id=AsyncMock(return_value=_PHYS_ID),
    )
    with patch("dynastore.tools.discovery.get_protocol", return_value=assets):
        out = await D._resolve_asset_path(cfg, "cat1", "col1")
    assert out is not None
    assert out.path == "gs://b/data.parquet"
    # Dispatched via the resolved live logical id, not the stale stored one.
    assets.resolve_asset_logical_id.assert_awaited_once_with("cat1", _PHYS_ID)
    assets.get_asset.assert_awaited_once_with("new-name", "cat1", "col1")
    # physical_id is already set — no second resolve needed.
    assets.resolve_asset_physical_id.assert_not_awaited()


@pytest.mark.asyncio
async def test_resolve_asset_path_physical_id_missing_falls_back():
    """When physical_id is persisted but the asset was deleted (returns None
    from resolve_asset_logical_id), fall back to the configured path."""
    D = _driver()
    cfg = _config(asset_id="old-name", asset_physical_id=_PHYS_ID,
                  path="/fallback.parquet", format="parquet")
    assets = SimpleNamespace(
        resolve_asset_logical_id=AsyncMock(return_value=None),
        get_asset=AsyncMock(),
        resolve_asset_physical_id=AsyncMock(),
    )
    with patch("dynastore.tools.discovery.get_protocol", return_value=assets):
        out = await D._resolve_asset_path(cfg, "cat1", "col1")
    assert out is not None
    assert out.path == "/fallback.parquet"
    assets.get_asset.assert_not_awaited()


@pytest.mark.asyncio
async def test_resolve_asset_path_no_physical_id_resolve_fails_gracefully():
    """When physical_id resolve returns None (new asset not yet persisted),
    the config is returned unchanged (physical_id not stamped)."""
    D = _driver()
    cfg = _config(asset_id="asset-x", format="parquet")
    fake_asset = SimpleNamespace(uri="gs://b/x.parquet", href=None)
    assets = SimpleNamespace(
        get_asset=AsyncMock(return_value=fake_asset),
        resolve_asset_physical_id=AsyncMock(return_value=None),
        resolve_asset_logical_id=AsyncMock(return_value=None),
    )
    with patch("dynastore.tools.discovery.get_protocol", return_value=assets):
        out = await D._resolve_asset_path(cfg, "cat1", "col1")
    assert out is not None
    assert out.path == "gs://b/x.parquet"
    # physical_id resolve returned None — should not be stamped
    assert out.asset_physical_id is None  # pyright: ignore[reportAttributeAccessIssue]


@pytest.mark.asyncio
async def test_resolve_asset_path_persists_stamp_via_set_config():
    """On first resolve the stamped config is written back to ConfigsProtocol
    so subsequent dispatches take the rename-safe branch."""
    from dynastore.models.protocols.configs import ConfigsProtocol  # noqa: F401

    D = _driver()
    cfg = _config(asset_id="asset-1", format="gpkg")
    fake_asset = SimpleNamespace(uri="gs://b/data.gpkg", href=None)
    assets = SimpleNamespace(
        get_asset=AsyncMock(return_value=fake_asset),
        resolve_asset_physical_id=AsyncMock(return_value=_PHYS_ID),
        resolve_asset_logical_id=AsyncMock(return_value="asset-1"),
    )
    configs = SimpleNamespace(set_config=AsyncMock(return_value=None))

    def _get_protocol(cls):
        if cls is ConfigsProtocol:
            return configs
        return assets

    with patch("dynastore.tools.discovery.get_protocol", side_effect=_get_protocol):
        out = await D._resolve_asset_path(cfg, "cat1", "col1")

    assert out is not None
    assert out.asset_physical_id == _PHYS_ID  # pyright: ignore[reportAttributeAccessIssue]
    # Verify the stamped config was persisted.
    configs.set_config.assert_awaited_once()
    call_args = configs.set_config.await_args
    assert call_args is not None
    _, kw = call_args
    assert kw.get("catalog_id") == "cat1"
    assert kw.get("collection_id") == "col1"
    assert kw.get("check_immutability") is False


@pytest.mark.asyncio
async def test_resolve_asset_path_persist_failure_does_not_fail_dispatch():
    """A set_config failure on write-back must not prevent the current
    dispatch from returning the stamped config."""
    from dynastore.models.protocols.configs import ConfigsProtocol  # noqa: F401

    D = _driver()
    cfg = _config(asset_id="asset-1", format="gpkg")
    fake_asset = SimpleNamespace(uri="gs://b/data.gpkg", href=None)
    assets = SimpleNamespace(
        get_asset=AsyncMock(return_value=fake_asset),
        resolve_asset_physical_id=AsyncMock(return_value=_PHYS_ID),
        resolve_asset_logical_id=AsyncMock(return_value="asset-1"),
    )
    configs = SimpleNamespace(set_config=AsyncMock(side_effect=RuntimeError("db gone")))

    def _get_protocol(cls):
        if cls is ConfigsProtocol:
            return configs
        return assets

    with patch("dynastore.tools.discovery.get_protocol", side_effect=_get_protocol):
        out = await D._resolve_asset_path(cfg, "cat1", "col1")

    # Dispatch must still succeed with the stamped physical_id.
    assert out is not None
    assert out.path == "gs://b/data.gpkg"
    assert out.asset_physical_id == _PHYS_ID  # pyright: ignore[reportAttributeAccessIssue]


# ---------------------------------------------------------------------------
# Guard registration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_register_asset_guard_uses_cascade_delete_false():
    D = _driver()
    add_ref = AsyncMock()
    assets = SimpleNamespace(add_asset_reference=add_ref)
    with patch("dynastore.tools.discovery.get_protocol", return_value=assets):
        await D._register_asset_guard("asset-1", "cat1", "col1")
    add_ref.assert_awaited_once()
    # cascade_delete must be False (protective — blocks hard-delete of the file).
    assert add_ref.await_args is not None
    _, kwargs = add_ref.await_args
    assert kwargs.get("cascade_delete") is False


@pytest.mark.asyncio
async def test_register_asset_guard_is_best_effort():
    """A failure to register the guard must not raise into provisioning."""
    D = _driver()
    assets = SimpleNamespace(add_asset_reference=AsyncMock(side_effect=RuntimeError("boom")))
    with patch("dynastore.tools.discovery.get_protocol", return_value=assets):
        await D._register_asset_guard("asset-1", "cat1", "col1")  # must not raise
