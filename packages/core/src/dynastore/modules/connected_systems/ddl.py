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

"""DDL and module lifecycle for OGC API - Connected Systems tables.

Follows the Styles module pattern: inline DDL strings, PARTITION BY LIST
(catalog_id), idempotent CREATE TABLE IF NOT EXISTS executed under a startup
advisory lock at module lifespan.
"""

import logging
from contextlib import asynccontextmanager

from dynastore.models.protocols import DatabaseProtocol
from dynastore.modules import ModuleProtocol, get_protocol
from dynastore.modules.db_config import maintenance_tools
from dynastore.modules.db_config.locking_tools import check_constraint_exists
from dynastore.modules.db_config.query_executor import DDLQuery, DbResource

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL constants
# ---------------------------------------------------------------------------

CONSYS_SYSTEMS_DDL = """
    CREATE TABLE IF NOT EXISTS consys.systems (
        id            UUID          NOT NULL DEFAULT gen_random_uuid(),
        -- catalog_id holds the immutable internal catalog id (not the public external id).
        -- Partitioned on this value so rows survive catalog renames transparently.
        catalog_id    VARCHAR       NOT NULL,
        system_id     VARCHAR       NOT NULL,
        name          VARCHAR       NOT NULL,
        description   TEXT,
        type          VARCHAR       NOT NULL DEFAULT 'Sensor',
        geometry      GEOMETRY(GEOMETRY, 4326),
        properties    JSONB         NOT NULL DEFAULT '{}',
        stac_collection_id VARCHAR,
        created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        updated_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        PRIMARY KEY (catalog_id, id),
        UNIQUE (catalog_id, system_id)
    ) PARTITION BY LIST (catalog_id);
"""

CONSYS_DEPLOYMENTS_DDL = """
    CREATE TABLE IF NOT EXISTS consys.deployments (
        id          UUID        NOT NULL DEFAULT gen_random_uuid(),
        -- catalog_id holds the immutable internal catalog id (not the public external id).
        catalog_id  VARCHAR     NOT NULL,
        system_id   UUID        NOT NULL,
        name        VARCHAR     NOT NULL,
        description TEXT,
        time_start  TIMESTAMPTZ,
        time_end    TIMESTAMPTZ,
        geometry    GEOMETRY(GEOMETRY, 4326),
        properties  JSONB       NOT NULL DEFAULT '{}',
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (catalog_id, id)
    ) PARTITION BY LIST (catalog_id);
"""

CONSYS_DATASTREAMS_DDL = """
    CREATE TABLE IF NOT EXISTS consys.datastreams (
        id                  UUID        NOT NULL DEFAULT gen_random_uuid(),
        -- catalog_id holds the immutable internal catalog id (not the public external id).
        catalog_id          VARCHAR     NOT NULL,
        datastream_id       VARCHAR     NOT NULL,
        system_id           UUID        NOT NULL,
        name                VARCHAR     NOT NULL,
        description         TEXT,
        observed_property   VARCHAR     NOT NULL,
        unit_of_measurement VARCHAR     NOT NULL,
        properties          JSONB       NOT NULL DEFAULT '{}',
        created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (catalog_id, id),
        UNIQUE (catalog_id, datastream_id)
    ) PARTITION BY LIST (catalog_id);
"""

CONSYS_OBSERVATIONS_DDL = """
    CREATE TABLE IF NOT EXISTS consys.observations (
        id               UUID        NOT NULL DEFAULT gen_random_uuid(),
        -- catalog_id holds the immutable internal catalog id (not the public external id).
        catalog_id       VARCHAR     NOT NULL,
        datastream_id    UUID        NOT NULL,
        phenomenon_time  TIMESTAMPTZ NOT NULL,
        result_time      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        result_value     NUMERIC     NOT NULL,
        result_quality   FLOAT,
        parameters       JSONB       NOT NULL DEFAULT '{}',
        PRIMARY KEY (catalog_id, id)
    ) PARTITION BY LIST (catalog_id);
"""

CONSYS_OBSERVATIONS_IDX_DDL = """
    CREATE INDEX IF NOT EXISTS observations_phenomenon_time_idx
    ON consys.observations USING BRIN (phenomenon_time);
"""

CONSYS_SYSTEMS_GEOM_IDX_DDL = """
    CREATE INDEX IF NOT EXISTS systems_geometry_idx
    ON consys.systems USING GIST (geometry);
"""

CONSYS_DEPLOYMENTS_GEOM_IDX_DDL = """
    CREATE INDEX IF NOT EXISTS deployments_geometry_idx
    ON consys.deployments USING GIST (geometry);
"""

CONSYS_FK_DATASTREAM_SYSTEM_DDL = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_datastream_system'
    ) THEN
        ALTER TABLE consys.datastreams
        ADD CONSTRAINT fk_datastream_system
        FOREIGN KEY (system_id, catalog_id)
        REFERENCES consys.systems(id, catalog_id)
        ON DELETE CASCADE;
    END IF;
END $$;
"""

CONSYS_FK_OBSERVATION_DATASTREAM_DDL = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_observation_datastream'
    ) THEN
        ALTER TABLE consys.observations
        ADD CONSTRAINT fk_observation_datastream
        FOREIGN KEY (datastream_id, catalog_id)
        REFERENCES consys.datastreams(id, catalog_id)
        ON DELETE CASCADE;
    END IF;
END $$;
"""

CONSYS_FK_DEPLOYMENT_SYSTEM_DDL = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_deployment_system'
    ) THEN
        ALTER TABLE consys.deployments
        ADD CONSTRAINT fk_deployment_system
        FOREIGN KEY (system_id, catalog_id)
        REFERENCES consys.systems(id, catalog_id)
        ON DELETE CASCADE;
    END IF;
END $$;
"""


# ---------------------------------------------------------------------------
# Module
# ---------------------------------------------------------------------------

class ConnectedSystemsModule(ModuleProtocol):
    """Lifecycle manager for the OGC API - Connected Systems tables."""

    priority: int = 110  # after db_config (10), db (20), catalog (50), styles (100)

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        db = get_protocol(DatabaseProtocol)
        engine = db.engine if db else None

        if not engine:
            logger.critical("ConnectedSystemsModule: database engine not found — skipping init.")
            yield
            return

        logger.info("ConnectedSystemsModule: initialising schema...")

        def _check_fk_datastream_system(conn):
            return check_constraint_exists(conn, "fk_datastream_system")

        def _check_fk_observation_datastream(conn):
            return check_constraint_exists(conn, "fk_observation_datastream")

        def _check_fk_deployment_system(conn):
            return check_constraint_exists(conn, "fk_deployment_system")

        async def _init_consys_storage(conn: DbResource) -> None:
            await maintenance_tools.ensure_schema_exists(conn, "consys")
            await DDLQuery(CONSYS_SYSTEMS_DDL).execute(conn)
            await DDLQuery(CONSYS_DEPLOYMENTS_DDL).execute(conn)
            await DDLQuery(CONSYS_DATASTREAMS_DDL).execute(conn)
            await DDLQuery(CONSYS_OBSERVATIONS_DDL).execute(conn)
            await DDLQuery(CONSYS_OBSERVATIONS_IDX_DDL).execute(conn)
            await DDLQuery(CONSYS_SYSTEMS_GEOM_IDX_DDL).execute(conn)
            await DDLQuery(CONSYS_DEPLOYMENTS_GEOM_IDX_DDL).execute(conn)
            # These DO-block DDLQuery statements are invisible to
            # ddl_inference._infer_existence_check (it only recognizes
            # statements starting with CREATE), so without an explicit
            # check_query the peer-race recovery in
            # DDLExecutor._try_peer_race_recovery_async never fires for a
            # concurrent-DDL duplicate_object (42710) on these constraints.
            await DDLQuery(
                CONSYS_FK_DATASTREAM_SYSTEM_DDL,
                check_query=_check_fk_datastream_system,
            ).execute(conn)
            await DDLQuery(
                CONSYS_FK_OBSERVATION_DATASTREAM_DDL,
                check_query=_check_fk_observation_datastream,
            ).execute(conn)
            await DDLQuery(
                CONSYS_FK_DEPLOYMENT_SYSTEM_DDL,
                check_query=_check_fk_deployment_system,
            ).execute(conn)

        try:
            await maintenance_tools.run_startup_ddl_tolerating_lock_timeout(
                engine, "connected_systems_module", _init_consys_storage,
            )
            logger.info("ConnectedSystemsModule: initialisation complete.")
        except Exception as exc:
            logger.critical("ConnectedSystemsModule initialization failed: %s", exc, exc_info=True)
            raise

        yield
