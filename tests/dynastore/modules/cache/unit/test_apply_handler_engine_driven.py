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

"""Pin the Valkey apply-handler registration contract — engine-driven only.

``CacheModule`` acquires its Valkey client exclusively via
``app_state.engine_cache.get("valkey_engine")``.  When that succeeds, the
``ValkeyEngineConfig`` apply handler MUST be registered so a later ``PUT
/configs/plugins/valkey_engine_config`` triggers a live reconnect (#818).
When ``app_state.engine_cache`` is absent (or the engine has no connection
configured), the module degrades to ``LocalAsyncCacheBackend`` and there is
no live backend to reconnect — the handler must NOT be registered in that
case, and the lifespan must still exit cleanly.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.cache.cache_module import CacheModule, _on_valkey_engine_config_change
from dynastore.modules.db_config.engine_config import ValkeyEngineConfig


@pytest.mark.asyncio
async def test_apply_handler_registered_on_engine_driven_success(monkeypatch):
    """Engine path succeeds → apply handler registered, then unregistered
    symmetrically on lifespan exit.
    """
    import dynastore.tools.cache_valkey as cv
    monkeypatch.setattr(cv, "_CACHE_DEPS_OK", True)

    fake_backend = MagicMock()
    fake_backend.info = AsyncMock(
        return_value={
            "server": {"redis_version": "7.2.4", "redis_mode": "standalone"},
            "memory": {"used_memory_human": "1M"},
        }
    )
    fake_backend.topology = AsyncMock(return_value={"is_cluster": False})
    fake_backend.close = AsyncMock(return_value=None)

    manager = MagicMock()
    manager.register_backend = MagicMock()
    manager.unregister_backend = MagicMock()

    engine_cache = MagicMock()
    engine_cache.get = AsyncMock(return_value=object())

    app_state = MagicMock()
    app_state.engine_cache = engine_cache
    app_state.engine_snapshot_refresh_task = None

    pre_handlers = list(ValkeyEngineConfig.get_apply_handlers())

    with (
        patch(
            "dynastore.tools.cache_valkey.ValkeyCacheBackend",
            return_value=fake_backend,
        ),
        patch(
            "dynastore.tools.cache.get_cache_manager",
            return_value=manager,
        ),
    ):
        module = CacheModule(app_state=app_state)
        async with module.lifespan(app_state):
            inside_handlers = list(ValkeyEngineConfig.get_apply_handlers())
            assert _on_valkey_engine_config_change in inside_handlers, (
                "apply handler must be registered once the engine-driven "
                "backend is live; without it, PUT "
                "/configs/plugins/valkey_engine_config persists the change "
                "but no live reconnect fires (#818)."
            )

    post_handlers = list(ValkeyEngineConfig.get_apply_handlers())
    assert _on_valkey_engine_config_change not in post_handlers, (
        "apply handler must be unregistered on lifespan exit; otherwise "
        "stale handlers leak across process-restart-equivalent test runs."
    )
    assert post_handlers == pre_handlers


@pytest.mark.asyncio
async def test_apply_handler_not_registered_without_engine_cache():
    """No ``app_state.engine_cache`` → degrades to local cache; nothing to
    reconnect, so the apply handler must not be registered.
    """
    app_state = MagicMock()
    app_state.engine_cache = None
    app_state.engine_snapshot_refresh_task = None

    pre_handlers = list(ValkeyEngineConfig.get_apply_handlers())

    module = CacheModule(app_state=app_state)
    entered = False
    async with module.lifespan(app_state):
        entered = True
        assert _on_valkey_engine_config_change not in list(
            ValkeyEngineConfig.get_apply_handlers()
        )

    assert entered
    assert list(ValkeyEngineConfig.get_apply_handlers()) == pre_handlers
