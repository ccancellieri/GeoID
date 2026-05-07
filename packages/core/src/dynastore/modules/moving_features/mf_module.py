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
from contextlib import asynccontextmanager

from dynastore.models.protocols import DatabaseProtocol
from dynastore.modules import ModuleProtocol, get_protocol
from dynastore.modules.db_config import maintenance_tools
from dynastore.modules.db_config.query_executor import DDLQuery, managed_transaction

logger = logging.getLogger(__name__)

MOVING_FEATURES_DDL = """
    CREATE TABLE IF NOT EXISTS moving_features.moving_features (
        id UUID DEFAULT gen_random_uuid(),
        catalog_id VARCHAR NOT NULL,
        collection_id VARCHAR NOT NULL,
        feature_type VARCHAR NOT NULL DEFAULT 'Feature',
        properties JSONB NOT NULL DEFAULT '{}',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (catalog_id, id)
    ) PARTITION BY LIST (catalog_id);
"""

TEMPORAL_GEOMETRIES_DDL = """
    CREATE TABLE IF NOT EXISTS moving_features.temporal_geometries (
        id UUID DEFAULT gen_random_uuid(),
        mf_id UUID NOT NULL,
        catalog_id VARCHAR NOT NULL,
        datetimes TIMESTAMPTZ[] NOT NULL,
        coordinates JSONB NOT NULL,
        crs VARCHAR NOT NULL DEFAULT 'http://www.opengis.net/def/crs/OGC/1.3/CRS84',
        trs VARCHAR NOT NULL DEFAULT 'http://www.opengis.net/def/uom/ISO-8601/0/Gregorian',
        interpolation VARCHAR NOT NULL DEFAULT 'Linear',
        properties JSONB DEFAULT '{}',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (catalog_id, id)
    ) PARTITION BY LIST (catalog_id);
"""


class MovingFeaturesModule(ModuleProtocol):
    priority: int = 100

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None

        if not engine:
            logger.critical("MovingFeaturesModule cannot initialize: database engine not found.")
            yield
            return

        logger.info("MovingFeaturesModule: Initializing schema...")
        try:
            async with managed_transaction(engine) as conn:
                async with maintenance_tools.acquire_startup_lock(conn, "moving_features_module"):
                    await maintenance_tools.ensure_schema_exists(conn, "moving_features")
                    await DDLQuery(MOVING_FEATURES_DDL).execute(conn)
                    await DDLQuery(TEMPORAL_GEOMETRIES_DDL).execute(conn)

            logger.info("MovingFeaturesModule: Initialization complete.")
        except Exception as e:
            logger.error("CRITICAL: MovingFeaturesModule initialization failed: %s", e, exc_info=True)

        yield
