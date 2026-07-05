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

"""The tile-metadata rebuild path must route its own connection acquire
through the bounded, ``PoolSaturationError``-aware path (#3023).

Before this fix, ``get_tile_resolution_params``/``get_collection_source_srid``
opened their own connection via a plain ``managed_transaction(engine)`` --
unbounded by the short, live-configurable fail-fast deadline the render
connection already gets. A saturated pool on this path would ride the
engine's own (much longer) ``pool_timeout`` instead of failing fast. Now both
acquire via ``managed_transaction(engine, acquire_timeout=...)``, the same
helper (``acquire_engine_connection_bounded``) the render connection uses,
so a timeout here raises ``PoolSaturationError`` -- mapped to 503 +
Retry-After by the existing exception handler -- well before the engine's
own ceiling.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from dynastore.modules.db_config.exceptions import PoolSaturationError
from dynastore.modules.tiles import tiles_module


@pytest.mark.asyncio
async def test_get_tile_resolution_params_acquire_is_bounded():
    """A saturated pool on the metadata acquire raises PoolSaturationError,
    bounded by the fail-fast timeout rather than the engine's own
    (much longer) pool_timeout."""
    tiles_module.get_tile_resolution_params.cache_clear()

    # A real (lazily-constructed, no actual connection) AsyncEngine so
    # managed_transaction's isinstance(db_resource, AsyncEngine) dispatch
    # takes the bounded-acquire branch.
    engine = create_async_engine("postgresql+asyncpg://u:p@localhost/db")

    async def _fast_fail_fast_timeout() -> float:
        return 0.01

    async def _never_connects(*_args, **_kwargs):
        await asyncio.sleep(10)

    try:
        with (
            patch.object(tiles_module, "_get_engine", return_value=engine),
            patch.object(
                tiles_module,
                "_read_live_fg_acquire_timeout",
                _fast_fail_fast_timeout,
            ),
            patch(
                "dynastore.modules.db_config.query_executor."
                "_acquire_async_engine_connection",
                new=AsyncMock(side_effect=_never_connects),
            ),
        ):
            with pytest.raises(PoolSaturationError):
                await tiles_module.get_tile_resolution_params("cat1", "col1")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_get_collection_source_srid_acquire_is_bounded():
    """Same bounded-acquire hardening for get_collection_source_srid, the
    other @cached rebuild the metadata path can trigger."""
    tiles_module.get_collection_source_srid.cache_clear()

    engine = create_async_engine("postgresql+asyncpg://u:p@localhost/db")

    async def _fast_fail_fast_timeout() -> float:
        return 0.01

    async def _never_connects(*_args, **_kwargs):
        await asyncio.sleep(10)

    try:
        with (
            patch.object(tiles_module, "_get_engine", return_value=engine),
            patch.object(
                tiles_module,
                "_read_live_fg_acquire_timeout",
                _fast_fail_fast_timeout,
            ),
            patch(
                "dynastore.modules.db_config.query_executor."
                "_acquire_async_engine_connection",
                new=AsyncMock(side_effect=_never_connects),
            ),
        ):
            with pytest.raises(PoolSaturationError):
                await tiles_module.get_collection_source_srid("cat1", "col1")
    finally:
        await engine.dispose()
