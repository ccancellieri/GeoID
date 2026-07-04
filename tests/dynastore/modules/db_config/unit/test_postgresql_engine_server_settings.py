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

"""Per-catalog physical Postgres engines get server-side timeouts (#2898).

``PostgresqlEngineConfig.engine_init`` builds a dedicated ``asyncpg.Pool``
for each engine instance -- this is dormant until F.4c multi-engine dispatch
lands, but ships with zero ``server_settings`` today, meaning a stuck query
or a leaked transaction on that pool has no server-side timeout at all
(unlike the shared serving engine, which clamps its statement_timeout below
the load-balancer deadline via #2906). This pins the fix: the per-catalog
engine now carries the same lock-safety + clamped statement_timeout
``server_settings`` as the shared serving engine.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

from dynastore.modules.db_config.db_config import DBConfig
from dynastore.modules.db_config.engine_config import PostgresqlEngineConfig


async def test_engine_init_passes_server_settings_with_clamped_statement_timeout() -> None:
    cfg = PostgresqlEngineConfig()
    with patch(
        "asyncpg.create_pool", new_callable=AsyncMock
    ) as mock_create_pool:
        await cfg.engine_init()

    mock_create_pool.assert_awaited_once()
    _args, kwargs = mock_create_pool.call_args
    server_settings = kwargs["server_settings"]
    assert server_settings["lock_timeout"] == DBConfig.lock_timeout
    assert server_settings["idle_in_transaction_session_timeout"] == (
        DBConfig.idle_in_transaction_session_timeout
    )
    # Clamped below (or at) the serving ceiling -- never the raw, possibly
    # disabled ("0") or above-ceiling DB_STATEMENT_TIMEOUT value.
    ceiling = DBConfig.serving_statement_timeout_ceiling_seconds
    assert server_settings["statement_timeout"] == f"{ceiling}s"


async def test_engine_init_clamps_an_above_ceiling_statement_timeout() -> None:
    """A configured statement_timeout above the ceiling is clamped down,
    matching the #2906 mechanism the shared serving engine already uses."""
    cfg = PostgresqlEngineConfig()
    with patch(
        "dynastore.modules.db_config.db_config.DBConfig.statement_timeout",
        "90s",
    ), patch(
        "asyncpg.create_pool", new_callable=AsyncMock
    ) as mock_create_pool:
        await cfg.engine_init()

    _args, kwargs = mock_create_pool.call_args
    ceiling = DBConfig.serving_statement_timeout_ceiling_seconds
    assert kwargs["server_settings"]["statement_timeout"] == f"{ceiling}s"


async def test_engine_init_still_passes_pool_sizing_kwargs() -> None:
    """Surgical addition -- pool_size/timeout wiring must be unaffected."""
    cfg = PostgresqlEngineConfig(pool_size=17, pool_timeout_sec=12)
    with patch(
        "asyncpg.create_pool", new_callable=AsyncMock
    ) as mock_create_pool:
        await cfg.engine_init()

    _args, kwargs = mock_create_pool.call_args
    assert kwargs["min_size"] == 1
    assert kwargs["max_size"] == 17
    assert kwargs["timeout"] == 12
