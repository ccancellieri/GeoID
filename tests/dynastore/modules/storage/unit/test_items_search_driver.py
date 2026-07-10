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

"""get_items_search_driver — derived search pool: INDEX lane preferred,
READ lane fallback.

Search is not a configured operation (#2494): the items search path
resolves ``ItemsRoutingConfig.operations[INDEX]`` first and falls back to
``operations[READ]`` when the INDEX lane is empty (the zero-config
default). Mirrors ``get_asset_search_driver`` and underpins the
routing-aware geoid lookup in issue #989.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dynastore.modules.storage import router as router_mod
from dynastore.modules.storage.hints import Hint
from dynastore.modules.storage.routing_config import Operation


def _resolved(name):
    return [SimpleNamespace(driver=name, on_failure=None)]


@pytest.mark.asyncio
async def test_uses_index_lane_driver_when_configured(monkeypatch):
    async def fake_resolve(operation, *_a, **_k):
        if operation == Operation.INDEX:
            return _resolved("index_driver")
        return _resolved("read_driver")

    monkeypatch.setattr(router_mod, "resolve_drivers", fake_resolve)
    resolved = await router_mod.get_items_search_driver("cat", "col")
    assert resolved.driver == "index_driver"


@pytest.mark.asyncio
async def test_falls_back_to_read_when_index_lane_empty(monkeypatch):
    async def fake_resolve(operation, *_a, **_k):
        if operation == Operation.INDEX:
            return []
        return _resolved("read_driver")

    monkeypatch.setattr(router_mod, "resolve_drivers", fake_resolve)
    resolved = await router_mod.get_items_search_driver("cat", "col")
    assert resolved.driver == "read_driver"


@pytest.mark.asyncio
async def test_raises_when_neither_index_nor_read(monkeypatch):
    async def fake_resolve(*_a, **_k):
        return []

    monkeypatch.setattr(router_mod, "resolve_drivers", fake_resolve)
    with pytest.raises(ValueError, match="INDEX and READ"):
        await router_mod.get_items_search_driver("cat", "col")


@pytest.mark.asyncio
async def test_index_pool_ranks_hint_search_tagged_driver_first(monkeypatch):
    """Within the INDEX-lane pool, an entry whose driver declares
    ``Hint.SEARCH`` in ``supported_hints`` is preferred over one that
    doesn't — even when it is not first in declared order."""

    class _PlainIndexDriver:
        supported_hints = frozenset()

    class _SearchPreferredDriver:
        supported_hints = frozenset({Hint.SEARCH})

    async def fake_resolve(operation, *_a, **_k):
        if operation == Operation.INDEX:
            return [
                SimpleNamespace(driver=_PlainIndexDriver(), on_failure=None),
                SimpleNamespace(driver=_SearchPreferredDriver(), on_failure=None),
            ]
        return _resolved("read_driver")

    monkeypatch.setattr(router_mod, "resolve_drivers", fake_resolve)
    resolved = await router_mod.get_items_search_driver("cat", "col")
    assert isinstance(resolved.driver, _SearchPreferredDriver)
