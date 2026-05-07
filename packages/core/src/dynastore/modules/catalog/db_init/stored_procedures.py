import logging
import asyncio
from dynastore.modules.db_config.query_executor import (
    DbResource,
    DDLQuery,
)
from dynastore.modules.db_config.locking_tools import (
    acquire_lock_if_needed,
    check_function_exists,
)

logger = logging.getLogger(__name__)

UPDATE_COLLECTION_EXTENTS_SQL = """
CREATE OR REPLACE FUNCTION platform.update_collection_extents()
RETURNS TRIGGER AS $$
DECLARE
    master_table_name TEXT := TG_ARGV[0];
    schema_name TEXT := TG_TABLE_SCHEMA;
    collection_id_val TEXT;
    new_spatial_extent JSONB;
    new_temporal_extent JSONB;
BEGIN
    EXECUTE format(
        'SELECT collection_id FROM %I.collection_configs '
        'WHERE class_key = ''ItemsPostgresqlDriver'' '
        'AND config_data->>''physical_table'' = $1 LIMIT 1',
        schema_name
    )
    INTO collection_id_val
    USING master_table_name;

    IF collection_id_val IS NULL THEN
        RETURN NULL;
    END IF;

    EXECUTE format('SELECT jsonb_build_object(''bbox'', ARRAY[ST_Extent(geom)]) FROM %I.%I', schema_name, master_table_name)
    INTO new_spatial_extent;

    EXECUTE format('SELECT jsonb_build_object(''interval'', ARRAY[ARRAY[MIN(valid_from), MAX(valid_to)]]) FROM %I.%I', schema_name, master_table_name)
    INTO new_temporal_extent;

    IF new_spatial_extent IS NOT NULL AND new_temporal_extent IS NOT NULL THEN
        EXECUTE format(
            'INSERT INTO %I.metadata (collection_id, extent)
             VALUES ($1, $2)
             ON CONFLICT (collection_id) DO UPDATE SET extent = EXCLUDED.extent',
            schema_name
        ) USING collection_id_val,
                jsonb_build_object('spatial', new_spatial_extent, 'temporal', new_temporal_extent);
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
"""

ASSET_CLEANUP_SQL = """
CREATE OR REPLACE FUNCTION platform.asset_cleanup() RETURNS TRIGGER AS $$
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
    
    -- Resolve Logical Collection ID from Hub Physical Table Name
    -- The collection_configs table stores driver config with physical_table in JSONB.
    EXECUTE format(
        'SELECT collection_id FROM %I.collection_configs '
        'WHERE class_key = ''ItemsPostgresqlDriver'' '
        'AND config_data->>''physical_table'' = $1 LIMIT 1',
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

    -- If remaining_count is NULL (no rows found), then proceed
    IF remaining_count IS NULL THEN
        -- assets PK is (collection_id, asset_id); pass both so the planner can prune the partition.
        EXECUTE format('DELETE FROM %I.assets WHERE asset_id = $1 AND collection_id = $2', TG_TABLE_SCHEMA)
        USING asset_id_val, target_collection_id;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
"""


PLATFORM_SCHEMA_DDL = 'CREATE SCHEMA IF NOT EXISTS "platform";'


async def ensure_stored_procedures(conn: DbResource) -> None:
    """Ensures all required stored procedures exist in the platform schema.

    Always runs ``CREATE OR REPLACE FUNCTION``: the previous existence-check
    short-circuit (``check_query=check_all``) caused stale function bodies
    to persist across deploys when only the SQL body changed but the
    function name did not — e.g. the ``class_key`` rename from
    ``ItemsPostgresqlDriverConfig`` to ``ItemsPostgresqlDriver`` (driven
    by ``TypedDriver``'s wire-key derivation) silently broke the
    ``trg_asset_cleanup`` cascade until the function was manually dropped.
    ``CREATE OR REPLACE FUNCTION`` is cheap and idempotent, and keeps the
    function body in lockstep with the Python source on every deploy.
    """
    # Ensure the platform schema exists before creating functions in it
    await DDLQuery(PLATFORM_SCHEMA_DDL).execute(conn)

    # Use a single DDLQuery for all procedures. DDLQuery handles splitting and atomic locking.
    await DDLQuery(
        UPDATE_COLLECTION_EXTENTS_SQL + ASSET_CLEANUP_SQL,
    ).execute(conn)

    logger.info("Catalog stored procedures ensured.")
