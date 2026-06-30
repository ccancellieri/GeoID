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

"""The geometry_columns SRID lookup must survive a mid-flight disconnect.

Regression: a maps tile request 500'd with asyncpg InterfaceError
("connection is closed") thrown while executing the cheap
``SELECT srid FROM geometry_columns`` read inside
``get_collection_source_srid``. The pooled connection passed pool_pre_ping at
checkout, then was killed server-side BEFORE the execute (a TOCTOU mid-flight
disconnect). pool_pre_ping only guards checkout.

The fix runs that pure system-catalog read against the ENGINE directly so it
flows through the executor's engine path, which retries once on a transient
DatabaseConnectionError (the reclassified asyncpg disconnect). This test pins
that contract: first acquired connection dies on .execute, second succeeds,
the SRID is returned, and exactly one retry occurred.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from asyncpg.exceptions import InterfaceError as AsyncpgInterfaceError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from dynastore.modules.storage.hints import Hint
from dynastore.modules.tiles import tiles_module


def _good_srid_conn(srid: int) -> AsyncMock:
    """Async connection mock whose .execute returns a result yielding ``srid``.

    The SRID query uses ResultHandler.SCALAR_ONE_OR_NONE → r.scalar_one_or_none().
    """
    result = MagicMock()
    result.scalar_one_or_none.return_value = srid
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=result)
    conn.in_transaction = MagicMock(return_value=False)
    conn.rollback = AsyncMock()
    conn.invalidate = AsyncMock()
    conn.close = AsyncMock()
    return conn


def _dead_conn() -> AsyncMock:
    """Async connection mock whose .execute raises asyncpg InterfaceError."""
    conn = AsyncMock()
    conn.execute = AsyncMock(side_effect=AsyncpgInterfaceError("connection is closed"))
    conn.in_transaction = MagicMock(return_value=False)
    conn.rollback = AsyncMock()
    conn.invalidate = AsyncMock()
    conn.close = AsyncMock()
    return conn


@pytest.mark.asyncio
async def test_source_srid_read_retries_once_on_mid_flight_disconnect() -> None:
    """The geometry_columns SRID read recovers from a TOCTOU disconnect via engine retry."""
    tiles_module.get_collection_source_srid.cache_clear()

    # A real (object-only) AsyncEngine so isinstance(engine, AsyncEngine) is True
    # and DQLQuery.execute(engine, ...) routes through the retrying engine path.
    engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/db")

    dead = _dead_conn()
    good = _good_srid_conn(32631)

    acquire_calls: list = []

    async def _fake_acquire(eng: AsyncEngine) -> AsyncMock:
        acquire_calls.append(eng)
        return dead if len(acquire_calls) == 1 else good

    fake_driver = AsyncMock()
    fake_driver.location = AsyncMock(
        return_value=SimpleNamespace(
            identifiers={"schema": "cat1_data", "table": "col1"}
        )
    )

    # No source_crs configured → the fallback physical-lookup path (the SRID
    # read) runs. config_service.get_config is mocked, so the managed_transaction
    # `conn` is never actually touched at the wire level.
    fake_catalogs = SimpleNamespace(
        configs=AsyncMock(get_config=AsyncMock(return_value=SimpleNamespace())),
    )
    get_driver_mock = AsyncMock(return_value=fake_driver)
    fake_driver_ctx = lambda **kw: kw  # noqa: E731 — local stub

    with (
        patch.object(tiles_module, "_get_engine", return_value=engine),
        # Stub the outer managed_transaction (used only for the mocked config
        # read); the SRID DQLQuery now runs against the engine, not this conn.
        patch("dynastore.modules.tiles.tiles_module.managed_transaction") as mt,
        patch("dynastore.modules.tiles.tiles_module.get_protocol",
              return_value=fake_catalogs),
        patch("dynastore.modules.storage.router.get_driver", new=get_driver_mock),
        patch.object(tiles_module, "DriverContext", new=fake_driver_ctx),
        # Drive the engine-path acquire so the first wire dies, the second works.
        patch(
            "dynastore.modules.db_config.query_executor._acquire_async_engine_connection",
            side_effect=_fake_acquire,
        ),
    ):
        mt.return_value.__aenter__ = AsyncMock(return_value=AsyncMock())
        mt.return_value.__aexit__ = AsyncMock(return_value=None)

        result = await tiles_module.get_collection_source_srid("cat1", "col1")

    # The SRID from the SECOND (healthy) connection is returned.
    assert result == 32631
    # Exactly one retry → two engine-path acquisitions for the SRID read.
    assert len(acquire_calls) == 2
    # The dead wire was invalidated before being discarded.
    dead.invalidate.assert_awaited_once()
    # Routing still pinned Hint.TILES (unchanged behaviour).
    _args, kwargs = get_driver_mock.await_args
    assert kwargs.get("hints") == frozenset({Hint.TILES})
