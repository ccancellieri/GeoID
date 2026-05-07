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

from dynastore.modules.protocols import ModuleProtocol
from dynastore.tools.discovery import register_plugin, unregister_plugin

logger = logging.getLogger(__name__)


class STATS(ModuleProtocol):
    priority: int = 100

    @asynccontextmanager
    async def lifespan(self, app_state):
        logger.info("Initializing stats module...")

        async with AsyncExitStack() as stack:
            # Instantiate the ES driver (no PG tables needed)
            from .elasticsearch_storage import ElasticsearchStatsDriver

            driver = ElasticsearchStatsDriver()
            await stack.enter_async_context(driver.lifespan(app_state))
            register_plugin(driver)
            stack.callback(unregister_plugin, driver)

            # Setup facade service
            from .service import STATS_SERVICE

            await stack.enter_async_context(STATS_SERVICE.lifespan(app_state))
            register_plugin(STATS_SERVICE)
            stack.callback(unregister_plugin, STATS_SERVICE)

            yield

        logger.info("Stats module shut down.")
