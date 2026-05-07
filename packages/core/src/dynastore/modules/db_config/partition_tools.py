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
import hashlib
from datetime import datetime, date, timedelta
from typing import Any, Optional, Tuple, List
from pydantic import BaseModel

from .query_executor import (
    DDLQuery, DQLQuery, DbConnection, DbResource, ResultHandler, managed_transaction
)
from .locking_tools import acquire_startup_lock

logger = logging.getLogger(__name__)
class PartitionDefinition(BaseModel):
    """Defines a single level in a partition hierarchy."""
    strategy: str
    value: Any
    interval: Optional[str] = None
    sub_partition_def: Optional[Tuple[str, str]] = None

async def ensure_hierarchical_partitions_exist(
    conn: DbConnection,
    base_table_name: str,
    schema: str,
    definitions: List[PartitionDefinition],
    parent_table_name: Optional[str] = None,
    parent_table_schema: Optional[str] = None
):
    """
    Ensures a multi-level partition hierarchy exists.
    """
    current_table_name = base_table_name
    current_parent_name = parent_table_name
    current_parent_schema = parent_table_schema

    # ensure_partition_exists handles its own transaction management
    for i, definition in enumerate(definitions):
        new_partition_name, _ = await ensure_partition_exists(
            conn, 
            table_name=current_table_name, 
            strategy=definition.strategy, 
            partition_value=definition.value, 
            schema=schema, 
            interval=definition.interval, 
            parent_table_name=current_parent_name,
            parent_table_schema=current_parent_schema,
            sub_partition_def=definition.sub_partition_def
        )
        if not new_partition_name:
            raise RuntimeError(f"Failed to create or retrieve partition name for table '{current_table_name}' with value '{definition.value}'.")
        
        # For the next level:
        # 1. The base name for the new partition calculation is the current partition's name
        current_table_name = new_partition_name
        # 2. The parent for the next level is the partition we just created
        current_parent_name = new_partition_name
        # 3. The schema of the parent for the next level is the same as the partition schema
        current_parent_schema = schema


async def ensure_partition_exists(
    conn: DbResource, table_name: str, strategy: str, partition_value: Any,
    schema: str = "platform", interval: Optional[str] = None, parent_table_name: Optional[str] = None, parent_table_schema: Optional[str] = None,
    sub_partition_def: Optional[Tuple[str, str]] = None
) -> Tuple[Optional[str], Optional[str]]:
    """ (internal)
    Ensures a partition exists for a given value using robust concurrency locking.
    Optimized with existence check to avoid unnecessary lock acquisition.
    """
    partition_name: Optional[str] = None
    create_sql: Optional[str] = None
    
    try:
        # 1. Calculate Name and SQL (Pure logic, no DB)
        partition_name, create_sql = _get_partition_ddl(table_name, schema, strategy, partition_value, interval, parent_table_name, parent_table_schema, sub_partition_def)
        
        if create_sql and partition_name:
            from .locking_tools import check_table_exists

            # Check if partition already exists before acquiring lock
            async def check_partition():
                return await check_table_exists(conn, partition_name, schema)

            # Use the centralized DDLQuery system
            await DDLQuery(
                create_sql,
                check_query=check_partition,
            ).execute(conn)
            logger.debug(f"Partition '{schema}.{partition_name}' verified/created.")

    except Exception as e:
        # PostgreSQL error code 42P07 ("duplicate_table") is harmless here due to IF NOT EXISTS,
        # but the lock largely prevents us from even hitting it.
        orig = getattr(e, 'orig', None)
        if partition_name and ("already exists" in str(e) or (orig is not None and getattr(orig, 'pgcode', None) == '42P07')):
            logger.debug(f"Partition '{partition_name}' existed (concurrently created).")
        else:
            logger.error(f"Failed to ensure partition '{partition_name or 'unknown'}' exists: {e}", exc_info=True)
            raise
            
    return partition_name, create_sql


def _get_partition_ddl(
    table_name: str, schema: str, strategy: str, partition_value: Any,
    interval: Optional[str] = None, parent_table_name: Optional[str] = None, parent_table_schema: Optional[str] = None,
    sub_partition_def: Optional[Tuple[str, str]] = None
) -> Tuple[Optional[str], Optional[str]]:
    """Constructs the partition name and the CREATE TABLE DDL statement."""

    # Determine the fully qualified name of the parent table.
    # If parent_table_schema is provided, use it. Otherwise, assume the parent is in the same schema as the partition.
    parent_schema = parent_table_schema or schema
    parent_name = parent_table_name or table_name
    
    # Always build the fully qualified name explicitly.
    partition_of_clause = f'PARTITION OF "{parent_schema}"."{parent_name}"'
    
    sub_partition_clause = ""
    if sub_partition_def:
        sub_strategy, sub_column = sub_partition_def
        sub_partition_clause = f" PARTITION BY {sub_strategy.upper()} ({sub_column})"

    if strategy.upper() == 'LIST':
        # The value for FOR VALUES IN must be the actual partition_value.
        # The partition name should be a stable, sanitized identifier, not the logical code.
        # We use a hash of the partition_value to create a unique, valid SQL identifier for the name.
        # This addresses the request to use a system "ID" (the hash) for the name, while the "code" remains the value.
        value_for_clause = str(partition_value)
        partition_value_hash = hashlib.sha256(value_for_clause.encode('utf-8')).hexdigest()[:16]
        partition_name = f"{table_name}_p_{partition_value_hash}"
        create_sql = f'CREATE TABLE IF NOT EXISTS "{schema}"."{partition_name}" {partition_of_clause} FOR VALUES IN (\'{value_for_clause}\'){sub_partition_clause};'
        return partition_name, create_sql

    elif strategy.upper() == 'RANGE':
        if not isinstance(partition_value, (datetime, date)):
            raise TypeError("RANGE partitioning requires a datetime or date partition_value.")
        
        is_datetime = isinstance(partition_value, datetime)

        if interval == 'monthly':
            start_date = partition_value.replace(day=1, hour=0, minute=0, second=0, microsecond=0) if is_datetime else datetime(partition_value.year, partition_value.month, 1)
            end_date = (start_date.replace(day=28) + timedelta(days=4)).replace(day=1)
            partition_name = f"{table_name}_{start_date.strftime('%Y_%m')}"
        elif interval == 'yearly':
            start_date = partition_value.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0) if is_datetime else datetime(partition_value.year, 1, 1)
            end_date = start_date.replace(year=start_date.year + 1)
            partition_name = f"{table_name}_{start_date.strftime('%Y')}"
        else:
            raise ValueError(f"Unsupported RANGE interval: {interval}")

        create_sql = f'CREATE TABLE IF NOT EXISTS "{schema}"."{partition_name}" {partition_of_clause} FOR VALUES FROM (\'{start_date.isoformat()}\') TO (\'{end_date.isoformat()}\'){sub_partition_clause};'
        return partition_name, create_sql

    else:
        raise ValueError(f"Unsupported partitioning strategy: {strategy}")

async def ensure_list_hash_partitions(
    conn: DbConnection,
    parent_table_fqn: str,
    partition_schema: str,
    list_partition_value: str,
    hash_partition_column: str,
    num_hash_partitions: int = 8
) -> None:
    """
    Ensures a two-level partitioning structure exists: LIST -> HASH.

    This is a common pattern for multi-tenant systems where data is first
    isolated by a tenant ID (LIST) and then sharded by a high-cardinality
    key (HASH) to prevent single large partitions.

    Args:
        conn: The database connection.
        parent_table_fqn: Fully qualified name of the top-level parent table (e.g., "catalog.collections").
        partition_schema: The schema where the new partitions will be created (typically the catalog_id).
        list_partition_value: The value for the LIST partition (typically the catalog_id).
        hash_partition_column: The column name to use for HASH sub-partitioning (e.g., "id").
        num_hash_partitions: The number of HASH sub-partitions to create.
    """
    from .locking_tools import check_table_exists
    
    # The intermediate table has the same name as the parent, but in the partition_schema
    _, base_table_name = parent_table_fqn.split('.')
    intermediate_partition_fqn = f'"{partition_schema}"."{base_table_name}"'

    # Define existence check for the intermediate partition
    async def check_intermediate():
        return await check_table_exists(conn, base_table_name, partition_schema)
    
    # 1. Create the intermediate LIST partition, defining its HASH sub-partitioning.
    list_partition_sql = f"""
    CREATE TABLE IF NOT EXISTS {intermediate_partition_fqn}
    PARTITION OF {parent_table_fqn}
    FOR VALUES IN ('{list_partition_value}')
    PARTITION BY HASH ({hash_partition_column})
    """
    
    # 2. Add final HASH sub-partitions to the same block
    statements = [list_partition_sql]
    for i in range(num_hash_partitions):
        hash_partition_name = f"{base_table_name}_p{i}"
        statements.append(f"""
        CREATE TABLE IF NOT EXISTS "{partition_schema}"."{hash_partition_name}"
        PARTITION OF {intermediate_partition_fqn}
        FOR VALUES WITH (MODULUS {num_hash_partitions}, REMAINDER {i})
        """)
    
    # Execute as a single coordinated block
    await DDLQuery(
        ";\n".join(statements),
        check_query=check_intermediate,
    ).execute(conn)
    logger.info(f"Ensured LIST->HASH partitions for {parent_table_fqn} in schema '{partition_schema}'.")