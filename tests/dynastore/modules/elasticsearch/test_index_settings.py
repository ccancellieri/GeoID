"""Tests for `ElasticsearchIndexConfig` and its index-settings helpers.

The PluginConfig waterfall feeds `index.mapping.total_fields.limit` into
each `indices.create` call (issue #489). Defaults must stay well above
the ES 1000 default that prompted the issue; live overrides through the
configs framework must take effect; and a missing protocol layer must
fall back to defaults rather than raising at index-create time.
"""
from __future__ import annotations

from typing import Optional, Type

import pytest

from dynastore.modules.db_config.plugin_config import PluginConfig
from dynastore.modules.elasticsearch.index_config import (
    ElasticsearchIndexConfig,
    get_assets_index_settings,
    get_items_index_settings,
    get_private_items_index_settings,
)


class _StubPlatformConfigsProtocol:
    is_platform_manager = True

    def __init__(self, cfg: Optional[ElasticsearchIndexConfig]) -> None:
        self._cfg = cfg

    async def get_config(
        self,
        config_cls: Type[PluginConfig],
        ctx=None,
    ) -> PluginConfig:
        if self._cfg is None or config_cls is not ElasticsearchIndexConfig:
            return ElasticsearchIndexConfig()
        return self._cfg

    async def set_config(self, *a, **kw) -> None: ...
    async def list_configs(self): return {}


@pytest.fixture
def install_stub(monkeypatch):
    """Replace `get_protocol(PlatformConfigsProtocol)` with a stub."""

    def _install(cfg: Optional[ElasticsearchIndexConfig]):
        stub = _StubPlatformConfigsProtocol(cfg)
        from dynastore.models.protocols import platform_configs as pc_mod
        from dynastore.modules.elasticsearch import index_config as ic_mod

        def fake_get_protocol(proto, *a, **kw):
            if proto is pc_mod.PlatformConfigsProtocol:
                return stub
            return None

        monkeypatch.setattr(ic_mod, "get_protocol", fake_get_protocol, raising=False)
        from dynastore.tools import discovery
        monkeypatch.setattr(discovery, "get_protocol", fake_get_protocol)
        return stub

    return _install


def test_defaults_above_es_ceiling():
    cfg = ElasticsearchIndexConfig()
    assert cfg.items_total_fields_limit == 2000
    assert cfg.assets_total_fields_limit == 1500
    assert cfg.private_items_total_fields_limit == 1500


@pytest.mark.parametrize("field", [
    "items_total_fields_limit",
    "assets_total_fields_limit",
    "private_items_total_fields_limit",
])
def test_below_es_default_rejected(field):
    with pytest.raises(Exception):
        ElasticsearchIndexConfig(**{field: 500})


@pytest.mark.asyncio
async def test_helpers_fall_back_when_no_protocol(monkeypatch):
    from dynastore.tools import discovery
    monkeypatch.setattr(discovery, "get_protocol", lambda *a, **kw: None)
    assert (await get_items_index_settings())["index.mapping.total_fields.limit"] == 2000
    assert (await get_assets_index_settings())["index.mapping.total_fields.limit"] == 1500
    assert (await get_private_items_index_settings())["index.mapping.total_fields.limit"] == 1500


@pytest.mark.asyncio
async def test_helpers_use_live_config(install_stub):
    install_stub(
        ElasticsearchIndexConfig(
            items_total_fields_limit=5000,
            assets_total_fields_limit=3000,
            private_items_total_fields_limit=2500,
        )
    )
    assert (await get_items_index_settings())["index.mapping.total_fields.limit"] == 5000
    assert (await get_assets_index_settings())["index.mapping.total_fields.limit"] == 3000
    assert (await get_private_items_index_settings())["index.mapping.total_fields.limit"] == 2500


@pytest.mark.asyncio
async def test_helpers_fall_back_on_protocol_error(monkeypatch):
    class Boom:
        is_platform_manager = True
        async def get_config(self, *a, **kw):
            raise RuntimeError("boom")
        async def set_config(self, *a, **kw): ...
        async def list_configs(self): return {}

    from dynastore.tools import discovery
    from dynastore.models.protocols import platform_configs as pc_mod
    monkeypatch.setattr(
        discovery,
        "get_protocol",
        lambda p, *a, **kw: Boom() if p is pc_mod.PlatformConfigsProtocol else None,
    )
    assert (await get_items_index_settings())["index.mapping.total_fields.limit"] == 2000
    assert (await get_assets_index_settings())["index.mapping.total_fields.limit"] == 1500
    assert (await get_private_items_index_settings())["index.mapping.total_fields.limit"] == 1500
