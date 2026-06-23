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

import logging
from dynastore.modules.db_config.query_executor import (
    DbResource,
    DDLQuery,
)

logger = logging.getLogger(__name__)

ASSET_CLEANUP_SQL = """
CREATE OR REPLACE FUNCTION catalog.asset_cleanup() RETURNS TRIGGER AS $$
DECLARE
    hub_physical_table TEXT := TG_ARGV[0];
    asset_id_val TEXT;
    remaining_count INTEGER;
    target_collection_id TEXT;
BEGIN
    -- Only proceed if the asset_id actually changed or was deleted
    IF (TG_OP = 'UPDATE' AND NEW.asset_id IS NOT DISTINCT FROM OLD.asset_id) THEN
        RETURN NULL;
    END IF;

    asset_id_val := OLD.asset_id;

    -- If asset_id is null, nothing to cleanup
    IF asset_id_val IS NULL THEN
        RETURN NULL;
    END IF;

    -- Resolve Logical Collection ID from the hub table name.  The hub table
    -- is named after the collection's immutable physical_id; the collections
    -- registry maps that back to the (renamable) logical collection id.
    EXECUTE format(
        'SELECT id FROM %I.collections WHERE physical_id = $1 LIMIT 1',
        TG_TABLE_SCHEMA
    )
    INTO target_collection_id
    USING hub_physical_table;

    -- If we can't resolve the collection, we can't safely clean up
    IF target_collection_id IS NULL THEN
        RETURN NULL;
    END IF;

    -- Check if the asset is still referenced in this table (the sidecar)
    EXECUTE format('SELECT 1 FROM %I.%I WHERE asset_id = $1 LIMIT 1', TG_TABLE_SCHEMA, TG_TABLE_NAME)
    INTO remaining_count
    USING asset_id_val;

    -- If remaining_count is NULL (no rows found), then proceed.
    -- The partition key is collection_physical_id (the hub table name passed
    -- as TG_ARGV[0]); include it so the planner can prune to the right
    -- partition without a full-table scan.
    IF remaining_count IS NULL THEN
        EXECUTE format(
            'DELETE FROM %I.assets WHERE asset_id = $1 AND collection_physical_id = $2',
            TG_TABLE_SCHEMA
        )
        USING asset_id_val, hub_physical_table;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
"""


CATALOG_SCHEMA_DDL = 'CREATE SCHEMA IF NOT EXISTS "catalog";'


async def ensure_stored_procedures(conn: DbResource) -> None:
    """Ensures all required stored procedures exist in the ``catalog`` schema.

    Always runs ``CREATE OR REPLACE FUNCTION``: the previous existence-check
    short-circuit (``check_query=check_all``) caused stale function bodies
    to persist across deploys when only the SQL body changed but the
    function name did not — e.g. the ``class_key`` rename from
    ``ItemsPostgresqlDriverConfig`` to ``ItemsPostgresqlDriver`` (driven
    by ``TypedDriver``'s wire-key derivation) silently broke the
    ``trg_asset_cleanup`` cascade until the function was manually dropped.
    ``CREATE OR REPLACE FUNCTION`` is cheap and idempotent, and keeps the
    function body in lockstep with the Python source on every deploy.

    This is also reached from the per-tenant trigger-install path
    (``AssetService.ensure_asset_cleanup_trigger``), so it defensively
    ensures the ``catalog`` schema itself rather than assuming an earlier
    module created it.
    """
    # Ensure the catalog schema exists before creating functions in it.
    await DDLQuery(CATALOG_SCHEMA_DDL).execute(conn)

    # Use a single DDLQuery for all procedures. DDLQuery handles splitting and atomic locking.
    await DDLQuery(ASSET_CLEANUP_SQL).execute(conn)

    # Durable maintenance-schedule table (tasks.maintenance_schedule). It owns
    # its own schema guard (the tasks schema is provisioned by TasksModule).
    from dynastore.modules.catalog.db_init.maintenance_schedule import (
        ensure_maintenance_schedule,
    )
    await ensure_maintenance_schedule(conn)

    logger.info("Catalog stored procedures ensured.")
