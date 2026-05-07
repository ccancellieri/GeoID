#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    you may obtain a copy of the License at
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

# dynastore/modules/styles/styles_module.py

import logging
from contextlib import asynccontextmanager
from dynastore.modules import ModuleProtocol, get_protocol
from dynastore.models.protocols import DatabaseProtocol
from dynastore.modules.db_config import maintenance_tools
from dynastore.modules.db_config.query_executor import managed_transaction, DDLQuery

logger = logging.getLogger(__name__)

# The schema for the table owned by this module. Note the use of PARTITION BY LIST.
STYLES_SCHEMA = """
    CREATE TABLE IF NOT EXISTS styles.styles (
        id UUID DEFAULT gen_random_uuid(),
        catalog_id VARCHAR NOT NULL,
        collection_id VARCHAR NOT NULL,
        style_id VARCHAR NOT NULL, -- User-defined style identifier
        title VARCHAR,
        description TEXT,
        keywords TEXT[],
        stylesheets JSONB, -- Array of StyleSheet objects
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (catalog_id, id),
        UNIQUE (catalog_id, collection_id, style_id)
    ) PARTITION BY LIST (catalog_id);
"""
class StylesModule(ModuleProtocol):
    priority: int = 100
    @asynccontextmanager
    async def lifespan(self, app_state: object):
        """
        Initializes the styles module. It creates the parent 'styles' table
        and attaches a trigger to call the centrally-managed 'manage_partition'
        function, which is assumed to have been created by the DBService.
        """
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None
        
        if not engine:
            logger.critical("StylesModule cannot initialize: database engine not found.")
            yield; return
        
        logger.info("StylesModule: Initializing schema...")
        try:
            async with managed_transaction(engine) as conn:
                async with maintenance_tools.acquire_startup_lock(conn, "styles_module"):
                    await maintenance_tools.ensure_schema_exists(conn, "styles")
                    await DDLQuery(STYLES_SCHEMA).execute(conn)

            logger.info("StylesModule: Initialization complete.")
        except Exception as e:
            logger.error(f"CRITICAL: StylesModule initialization failed: {e}", exc_info=True)
        
        yield
