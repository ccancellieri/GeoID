"""get_items_search_driver — routing-aware items SEARCH with READ fallback.

The items search path resolves ``ItemsRoutingConfig.operations[SEARCH]``
first and falls back to ``operations[READ]`` when no SEARCH driver is
configured (the zero-config default). Mirrors ``get_asset_search_driver``
and underpins the routing-aware geoid lookup in issue #989.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dynastore.modules.storage import router as router_mod
from dynastore.modules.storage.routing_config import Operation


def _resolved(name):
    return [SimpleNamespace(driver=name, on_failure=None, write_mode=None)]


@pytest.mark.asyncio
async def test_uses_search_driver_when_configured(monkeypatch):
    async def fake_resolve(operation, *_a, **_k):
        if operation == Operation.SEARCH:
            return _resolved("search_driver")
        return _resolved("read_driver")

    monkeypatch.setattr(router_mod, "resolve_drivers", fake_resolve)
    resolved = await router_mod.get_items_search_driver("cat", "col")
    assert resolved.driver == "search_driver"


@pytest.mark.asyncio
async def test_falls_back_to_read_when_no_search(monkeypatch):
    async def fake_resolve(operation, *_a, **_k):
        if operation == Operation.SEARCH:
            return []
        return _resolved("read_driver")

    monkeypatch.setattr(router_mod, "resolve_drivers", fake_resolve)
    resolved = await router_mod.get_items_search_driver("cat", "col")
    assert resolved.driver == "read_driver"


@pytest.mark.asyncio
async def test_raises_when_neither_search_nor_read(monkeypatch):
    async def fake_resolve(*_a, **_k):
        return []

    monkeypatch.setattr(router_mod, "resolve_drivers", fake_resolve)
    with pytest.raises(ValueError, match="SEARCH/READ"):
        await router_mod.get_items_search_driver("cat", "col")
