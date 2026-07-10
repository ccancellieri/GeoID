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

"""#3177 — the tiles lifespan must survive scopes without the stac extension.

``tiles/stac_contributor.py`` imports the shared tileability gate from
``dynastore.extensions.stac._map_tiles_gate``. ``scope_maps`` ships tiles
without the stac extension, so on the maps image that import raises
ModuleNotFoundError; before the guard this killed the whole TilesService
lifespan at boot and left the service without tiles. These tests pin that
the lifespan skips the STAC contributor when the stac extension is absent
and still registers/unregisters it when present.
"""

from __future__ import annotations

import sys
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import dynastore.extensions.tiles.tiles_service as svc_mod
from dynastore.extensions.tiles.tiles_service import TilesService


@asynccontextmanager
async def _lifespan_harness():
    """Drive TilesService.lifespan with everything after the guarded
    import stubbed out, yielding the discovery register/unregister mocks."""
    service = TilesService.__new__(TilesService)
    writer = MagicMock()
    writer.stop = AsyncMock()
    register = MagicMock()
    unregister = MagicMock()

    caching_cfg = MagicMock(
        cache_writer_buffer_max_bytes=1024, cache_writer_workers=1
    )
    with (
        patch.object(svc_mod, "TileCacheWriter", return_value=writer),
        patch(
            "dynastore.tools.discovery.register_plugin", register
        ),
        patch(
            "dynastore.tools.discovery.unregister_plugin", unregister
        ),
        patch(
            "dynastore.modules.presets.cold_boot.register_cold_boot_contributor",
            MagicMock(),
        ),
        patch(
            "dynastore.modules.tiles.tiles_config._load_caching_config",
            AsyncMock(return_value=caching_cfg),
        ),
    ):
        async with service.lifespan(app=MagicMock()):
            yield register, unregister


@pytest.mark.asyncio
async def test_lifespan_skips_stac_contributor_when_stac_absent():
    # None in sys.modules makes any import of the name raise ImportError,
    # mimicking an image built from a scope without extension_stac. The
    # contributor module itself must be evicted so its body re-executes.
    absent = {
        "dynastore.extensions.tiles.stac_contributor": None,
        "dynastore.extensions.stac": None,
        "dynastore.extensions.stac._map_tiles_gate": None,
    }
    with patch.dict(sys.modules, absent):
        async with _lifespan_harness() as (register, unregister):
            register.assert_not_called()
    unregister.assert_not_called()


@pytest.mark.asyncio
async def test_lifespan_registers_and_unregisters_contributor_when_stac_present():
    pytest.importorskip("dynastore.extensions.stac._map_tiles_gate")
    from dynastore.extensions.tiles.stac_contributor import TilesStacContributor

    async with _lifespan_harness() as (register, unregister):
        assert register.call_count == 1
        contributor = register.call_args.args[0]
        assert isinstance(contributor, TilesStacContributor)
        unregister.assert_not_called()
    unregister.assert_called_once_with(contributor)
