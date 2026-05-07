#    Copyright 2025 FAO
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

from __future__ import annotations

import logging
from contextlib import asynccontextmanager, AsyncExitStack
from typing import Any, Optional, Protocol, runtime_checkable

from dynastore.modules import ModuleProtocol
from dynastore.tools.discovery import register_plugin, unregister_plugin

# Side-effect import — ensures the F.1 engine PluginConfig classes
# (PostgresqlEngineConfig, ElasticsearchEngineConfig, etc.) register in
# TypedModelRegistry at module-load time so the configs API surfaces
# them under platform.engines.*.  DBConfigModule loads at priority=0,
# so this is the earliest reliable trigger.
from . import engine_config as _engine_config  # noqa: F401
from .db_config import DBConfig
from .engine_instance_cache import EngineInstanceCache
from .engine_resolver import build_engine_snapshot, make_resolver
from .platform_config_service import PlatformConfigService

logger = logging.getLogger(__name__)


@runtime_checkable
class DBConfigAppState(Protocol):
    db_config: Optional[DBConfig]
    engine: Any
    sync_engine: Any
    engine_cache: Optional[EngineInstanceCache]


class DBConfigModule(ModuleProtocol):
    priority: int = 0
    
    def __init__(self):
        logger.info("DBConfigModule: Initialized.")

    def get_config(self) -> DBConfig:
        """Returns the current database configuration."""
        cfg = self.app_state.db_config
        if cfg is None:
            raise RuntimeError("DBConfigModule.get_config() called before lifespan start")
        return cfg

    async def _build_engine_cache(
        self, pcfg: PlatformConfigService
    ) -> EngineInstanceCache:
        """Snapshot platform engines + return a started EngineInstanceCache."""
        snapshot = await build_engine_snapshot(pcfg)
        cache = EngineInstanceCache(engine_resolver=make_resolver(snapshot))
        cache.start_background_sweep()
        return cache

    @staticmethod
    async def _teardown_engine_cache(app_state: DBConfigAppState) -> None:
        """Close the engine cache + drop the reference from app_state."""
        cache = getattr(app_state, "engine_cache", None)
        if cache is None:
            return
        try:
            await cache.close()
        finally:
            app_state.engine_cache = None

    @asynccontextmanager
    async def lifespan(self, app_state: DBConfigAppState):
        """Manages the configuration's presence in the app state during the app's lifecycle."""
        logger.info("DBConfigModule: Lifespan starting up.")
        self.app_state = app_state
        
        async with AsyncExitStack() as stack:
            # 1. Platform configuration
            app_state.db_config = DBConfig()
            
            # 2. Platform Config Manager (Plugin)
            # Read engine directly from app_state to avoid circular dependency via get_engine(),
            # which would scan all DatabaseProtocol providers (including TilesModule) before
            # the DB engine has been set up.
            engine = (
                getattr(app_state, "engine", None)
                or getattr(app_state, "sync_engine", None)
            )
            
            pcfg = PlatformConfigService(engine=engine)
            await stack.enter_async_context(pcfg.lifespan(app_state))
            register_plugin(pcfg)
            stack.callback(unregister_plugin, pcfg)

            # 3. Engine instance cache (Cycle F.6) — snapshots platform-tier
            # engine configs at boot, exposes lazy-instantiating cache.
            # Until F.4c lands, no driver consumes this in production paths,
            # but admin tooling + tests use it via app_state.engine_cache.
            engine_cache = await self._build_engine_cache(pcfg)
            app_state.engine_cache = engine_cache
            stack.push_async_callback(self._teardown_engine_cache, app_state)

            yield

        # Stack callbacks fire on ``async with`` exit — clear ``db_config``
        # AFTER teardown so any engine_release impl reading DBConfig (or
        # the DSN derived from it) still sees a populated app_state.
        if hasattr(self.app_state, 'db_config'):
            self.app_state.db_config = None

        logger.info("DBConfigModule: Lifespan shutting down.")
