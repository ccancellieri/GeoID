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

import logging
from contextlib import asynccontextmanager, AsyncExitStack
from typing import Optional, Protocol, runtime_checkable, Any
from dynastore.modules import ModuleProtocol
from .db_config import DBConfig

logger = logging.getLogger(__name__)

from dynastore.tools.discovery import register_plugin, unregister_plugin
from .platform_config_service import PlatformConfigService

# Side-effect import — ensures the F.1 engine PluginConfig classes
# (PostgresqlEngineConfig, ElasticsearchEngineConfig, etc.) register in
# TypedModelRegistry at module-load time so the configs API surfaces
# them under platform.engines.*.  DBConfigModule loads at priority=0,
# so this is the earliest reliable trigger.
from . import engine_config as _engine_config  # noqa: F401


@runtime_checkable
class DBConfigAppState(Protocol):
    db_config: Optional[DBConfig]
    engine: Any
    sync_engine: Any


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

            yield
            
            if hasattr(self.app_state, 'db_config'):
                self.app_state.db_config = None
                
        logger.info("DBConfigModule: Lifespan shutting down.")
