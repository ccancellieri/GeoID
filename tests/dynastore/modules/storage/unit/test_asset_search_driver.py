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

"""get_asset_search_driver — derived search (INDEX lane first, READ fallback).

There is no configured SEARCH operation. The asset search path resolves
``AssetRoutingConfig.operations[INDEX]`` first and falls back to
``operations[READ]`` when no INDEX driver is configured (the zero-config
default). Mirrors the collection-tier derived-search fallback and the
routing-aware lookup design in issue #989.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dynastore.modules.storage import router as router_mod
from dynastore.modules.storage.routing_config import Operation


def _resolved(name):
    return [SimpleNamespace(driver=name, on_failure=None)]


@pytest.mark.asyncio
async def test_uses_index_driver_when_configured(monkeypatch):
    async def fake_resolve(operation, *_a, **_k):
        if operation == Operation.INDEX:
            return _resolved("index_driver")
        return _resolved("read_driver")

    monkeypatch.setattr(router_mod, "resolve_drivers", fake_resolve)
    driver = await router_mod.get_asset_search_driver("cat", "col")
    assert driver == "index_driver"


@pytest.mark.asyncio
async def test_falls_back_to_read_when_no_index(monkeypatch):
    async def fake_resolve(operation, *_a, **_k):
        if operation == Operation.INDEX:
            return []
        return _resolved("read_driver")

    monkeypatch.setattr(router_mod, "resolve_drivers", fake_resolve)
    driver = await router_mod.get_asset_search_driver("cat", "col")
    assert driver == "read_driver"


@pytest.mark.asyncio
async def test_raises_when_neither_index_nor_read(monkeypatch):
    async def fake_resolve(*_a, **_k):
        return []

    monkeypatch.setattr(router_mod, "resolve_drivers", fake_resolve)
    with pytest.raises(ValueError, match="INDEX and READ lanes"):
        await router_mod.get_asset_search_driver("cat", "col")
