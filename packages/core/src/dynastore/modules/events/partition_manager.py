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

# dynastore/modules/events/partition_manager.py

import logging
from datetime import datetime, timedelta, timezone
from typing import Tuple
from dynastore.modules.db_config.query_executor import (DDLQuery, DQLQuery,
                                                        ResultHandler, DbResource, managed_transaction)

logger = logging.getLogger(__name__)

# --- Configuration Constants ---
# These values can be externalized to environment variables if needed.
RETENTION_DAYS = 30
PRECREATE_DAYS = 7
PARTITION_INTERVAL = "daily" # Controls the partition granularity. "daily" or "monthly"
_EVENTS_SCHEMA = "events"
BASE_TABLE_NAME = f'"{_EVENTS_SCHEMA}".events'
# --- End Configuration ---

def _get_partition_bare_name(for_date: datetime) -> str:
    """Generates the unqualified partition table name based on the interval."""
    if PARTITION_INTERVAL == "daily":
        return f'events_{for_date.strftime("%Y_%m_%d")}'
    elif PARTITION_INTERVAL == "monthly":
        return f'events_{for_date.strftime("%Y_%m")}'
    else:
        raise ValueError(f"Unsupported partition interval: {PARTITION_INTERVAL}")

def _get_qualified_partition_name(for_date: datetime) -> str:
    """Returns the schema-qualified partition table name."""
    return f'"{_EVENTS_SCHEMA}".{_get_partition_bare_name(for_date)}'

def _get_partition_bounds(for_date: datetime) -> Tuple[str, str]:
    """Calculates the SQL-formatted date bounds for a new partition."""
    if PARTITION_INTERVAL == "daily":
        start_date = for_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=1)
    elif PARTITION_INTERVAL == "monthly":
        start_date = for_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        # Find first day of next month
        if start_date.month == 12:
            end_date = start_date.replace(year=start_date.year + 1, month=1)
        else:
            end_date = start_date.replace(month=start_date.month + 1)
    else:
        raise ValueError(f"Unsupported partition interval: {PARTITION_INTERVAL}")

    return start_date.isoformat(), end_date.isoformat()

async def _create_partition_if_not_exists(conn: DbResource, for_date: datetime):
    """
    Atomically creates a new partition for the given date if it doesn't exist.
    """
    bare_name = _get_partition_bare_name(for_date)
    qualified_name = _get_qualified_partition_name(for_date)
    start_bound, end_bound = _get_partition_bounds(for_date)

    sql = f"""
    CREATE TABLE IF NOT EXISTS {qualified_name}
    PARTITION OF {BASE_TABLE_NAME}
    FOR VALUES FROM ('{start_bound}') TO ('{end_bound}');
    """

    # Add indexes to the new partition. This is idempotent.
    index_sql_status = f"""
    CREATE INDEX IF NOT EXISTS idx_{bare_name}_status_locked
    ON {qualified_name} (status, locked_until)
    WHERE status IN ('PENDING', 'FAILED');
    """
    index_sql_events = f"""
    CREATE INDEX IF NOT EXISTS idx_{bare_name}_event_type
    ON {qualified_name} (event_type);
    """
    
    try:
        # We don't need to wrap this in a transaction, DDLQuery handles it
        await DDLQuery(sql).execute(conn)
        await DDLQuery(index_sql_status).execute(conn)
        await DDLQuery(index_sql_events).execute(conn)
        logger.debug(f"Ensured partition exists: {qualified_name}")
    except Exception as e:
        # This can happen in a race condition, it's safe to ignore
        if "concurrently" in str(e) or "duplicate" in str(e):
             logger.warning(f"Race condition while creating {qualified_name}, ignoring: {e}")
        else:
            logger.error(f"Failed to create partition {qualified_name}: {e}", exc_info=True)


async def _drop_old_partitions(conn: DbResource):
    """
    Finds and drops partitions older than the RETENTION_DAYS.
    """
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    
    # This query inspects the system catalogs to find old partitions
    find_old_sql = """
    SELECT
        child.relname AS partition_name,
        pg_catalog.pg_get_expr(child.relpartbound, child.oid, true) AS partition_bound
    FROM pg_class parent
    JOIN pg_inherits inh ON inh.inhparent = parent.oid
    JOIN pg_class child ON inh.inhrelid = child.oid
    WHERE parent.relname = :base_table
    """
    
    query = DQLQuery(find_old_sql, result_handler=ResultHandler.ALL_DICTS)
    partitions = await query.execute(conn, base_table=BASE_TABLE_NAME)
    
    for part in partitions:
        name = part['partition_name']
        bound_expr = part['partition_bound']
        
        # Parse the 'TO' value from the bound expression, e.g., "FOR VALUES FROM ('2025-10-01') TO ('2025-10-02')"
        try:
            # A bit of string parsing to extract the 'TO' bound
            to_val_str = bound_expr.split("TO (")[1].split(")")[0].strip("'")
            to_date = datetime.fromisoformat(to_val_str).replace(tzinfo=timezone.utc)
            
            # If the partition's end date is before the cutoff, drop it.
            if to_date < cutoff_date:
                logger.info(f"Dropping old partition: {name} (ends at {to_date})")
                await DDLQuery(f'DROP TABLE IF EXISTS "{_EVENTS_SCHEMA}"."{name}";').execute(conn)
                
        except Exception as e:
            logger.warning(f"Could not parse or drop partition {name} from bound '{bound_expr}': {e}")


async def manage_event_partitions(engine: DbResource):
    """
    The main maintenance function to be called by the module's lifespan.
    It pre-creates future partitions and drops old ones.
    """
    today = datetime.now(timezone.utc)
    
    async with managed_transaction(engine) as conn:
        # 1. Pre-create future partitions
        logger.info(f"EventsModule: Pre-creating partitions for the next {PRECREATE_DAYS} days...")
        for i in range(PRECREATE_DAYS):
            target_date = today + timedelta(days=i)
            await _create_partition_if_not_exists(conn, target_date)
            
        # 2. Drop old partitions
        logger.info(f"EventsModule: Dropping partitions older than {RETENTION_DAYS} days...")
        await _drop_old_partitions(conn)
        
        logger.info("EventsModule: Partition maintenance complete.")
