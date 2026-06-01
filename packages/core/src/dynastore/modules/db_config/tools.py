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
import secrets
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from typing import Optional, Any, Union, Dict
from dynastore.modules.db_config.db_config import DBConfig
from dynastore.modules.db_config.query_executor import (
    DbResource,
    DbConnection,
    DDLQuery,
    DQLQuery,
    ResultHandler,
)
from dynastore.modules.db_config import maintenance_tools
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, JSON
from sqlalchemy.dialects.postgresql import UUID, TSTZRANGE
from geoalchemy2 import Geometry
from dynastore.tools.cache import cached


def normalize_db_url(url: str, is_async: bool = False) -> str:
    """
    Normalizes a database URL for use with sync (psycopg2) or async (asyncpg) drivers.

    It ensures the correct protocol prefix and converts driver-specific
    parameters (like ssl vs sslmode).
    """
    if not url:
        return url

    # Strip shell quotes that may leak from .env parsing (shlex.quote wraps in '')
    url = url.strip("'\"")

    # 1. Handle Protocol
    if is_async:
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    else:
        if url.startswith("postgresql+asyncpg://"):
            url = url.replace("postgresql+asyncpg://", "postgresql://", 1)

    # 2. Handle SSL Parameters
    # asyncpg uses 'ssl'
    # psycopg2 uses 'sslmode'
    if is_async:
        # Convert sslmode=... to ssl=...
        if "sslmode=" in url:
            url = url.replace("sslmode=", "ssl=")
    else:
        # Convert ssl=... to sslmode=...
        if "ssl=" in url:
            url = url.replace("ssl=", "sslmode=")

    return url


async def ensure_init_db(resource: DbResource):
    """Initializes the database base extensions.

    Every step is wrapped in ``retry_on_invalidated_connection`` so a
    transient DB drop during dev startup (db_entrypoint_dev.sh reset) does
    not abort the foundational DatastoreModule lifespan.  ``ensure_db_extension``
    embeds its own retry; the platform-config initializer (which issues raw
    DDL directly) is wrapped here at the call site so the entire bootstrap
    sequence is uniformly resilient.
    """
    await maintenance_tools.ensure_db_extension(resource, "postgis")
    await maintenance_tools.ensure_db_extension(resource, "postgis_topology")
    await maintenance_tools.ensure_db_extension(resource, "btree_gist")
    await maintenance_tools.ensure_db_extension(resource, "btree_gin")
    # pgcrypto provides ``digest()`` used by the geometries / attributes
    # sidecar GENERATED columns to maintain the SHA256 *_hash columns
    # without application-side write code.
    await maintenance_tools.ensure_db_extension(resource, "pgcrypto")
    # pg_trgm powers lexical fuzzy matching (``similarity()`` / ``%`` operator).
    # Used by the dimension Similarity conformance class to rank materialized
    # dimension members by trigram similarity against their member labels.
    await maintenance_tools.ensure_db_extension(resource, "pg_trgm")

    # --- Initialize Platform Config Storage ---
    from dynastore.modules.db_config.platform_config_service import PlatformConfigService

    await maintenance_tools.retry_on_invalidated_connection(
        lambda: PlatformConfigService.initialize_storage(resource),
        label="PlatformConfigService.initialize_storage",
    )


def get_config(app_state) -> DBConfig:
    """Returns the current database configuration."""
    return app_state.db_config


@asynccontextmanager
async def isolated_transaction(
    conn: DbConnection,
) -> AsyncGenerator[None, None]:
    """
    Creates an isolated sub-transaction using a SAVEPOINT.

    This is critical for running DDL statements that might fail harmlessly
    (e.g., CREATE TYPE ... IF NOT EXISTS). If such a statement fails,
    PostgreSQL would normally abort the entire parent transaction.

    This context manager wraps the operation in a SAVEPOINT. If any exception
    occurs within the `with` block, it rolls back to the savepoint, which
    clears the error state from the connection, allowing the parent transaction
    to proceed without being poisoned.

    Usage:
        try:
            async with isolated_transaction(conn):
                await DDLQuery("CREATE TYPE ...").execute(conn)
        except DuplicateObjectError:
            logger.debug("Type already exists, continuing.")

    """
    savepoint_name = f"sp_{secrets.token_hex(6)}"
    await DDLQuery(f"SAVEPOINT {savepoint_name}").execute(conn)
    rolled_back = False
    try:
        yield
    except Exception:
        rolled_back = True
        await DDLQuery(f"ROLLBACK TO SAVEPOINT {savepoint_name}").execute(conn)
        raise  # Re-raise the exception for the caller to handle
    if not rolled_back:
        await DDLQuery(f"RELEASE SAVEPOINT {savepoint_name}").execute(conn)


# --- Reflection Tools ---

_get_table_columns_query = DQLQuery(
    "SELECT column_name, data_type, udt_name FROM information_schema.columns WHERE table_schema = :schema AND table_name = :table;",
    result_handler=ResultHandler.ALL,
)


def map_pg_type_to_sqlalchemy_type(
    pg_type: Union[str, Any], udt_name: Optional[str] = None
) -> Optional[Any]:
    """Maps PostgreSQL data_type strings to SQLAlchemy types."""
    if not isinstance(pg_type, str):
        # Handle PostgresType enum or similar
        pg_type = str(getattr(pg_type, "value", pg_type))

    pg_type = pg_type.lower()
    if pg_type == "user-defined" and udt_name == "geometry":
        return Geometry
    if pg_type in ("character varying", "text", "character"):
        return String
    if pg_type in ("integer", "smallint", "bigint"):
        return Integer
    if pg_type in ("double precision", "numeric", "real"):
        return Float
    if pg_type == "boolean":
        return Boolean
    if pg_type.startswith("timestamp"):
        return DateTime
    if pg_type == "uuid":
        return UUID
    if pg_type == "date":
        return DateTime  # Or Date if you want to be more specific
    if pg_type == "jsonb":
        return JSON
    if pg_type == "tstzrange":
        return TSTZRANGE
    # Return None for types we don't want to map (e.g., geometry, jsonb)
    # jsonb will be handled by the layer_config logic
    return None


def map_pg_to_json_type(pg_type: Union[str, Any]) -> str:
    """
    Map PostgreSQL type (string or Enum) to JSON Schema type.
    Centralized utility used by OGC Features, WFS, and Sidecars.
    """
    if not isinstance(pg_type, str):
        pg_type = str(getattr(pg_type, "value", pg_type))

    pg_type = pg_type.lower()

    if any(t in pg_type for t in ["int", "serial", "bigint"]):
        return "integer"
    if any(t in pg_type for t in ["float", "numeric", "double", "real", "decimal"]):
        return "number"
    if "bool" in pg_type:
        return "boolean"
    if "json" in pg_type:
        return "object"
    # Dates, UUIDs, Text are typically 'string' in JSON Schema
    return "string"


@cached(maxsize=128, namespace="field_mapping", ignore=["conn"])
async def get_dynamic_field_mapping(
    conn: DbResource, schema: str, table: str
) -> Dict[str, Column]:
    """
    Retrieves all 'flat' columns as a dictionary of SQLAlchemy Column objects.
    """
    try:
        result = await _get_table_columns_query.execute(
            conn, schema=schema, table=table
        )
        field_mapping = {}
        for row in result:
            col_name, pg_type, udt_name = row[0], row[1], row[2]
            sa_type = map_pg_type_to_sqlalchemy_type(pg_type, udt_name)
            if sa_type:
                # Create a SQLAlchemy Column object for this field
                field_mapping[col_name] = Column(col_name, sa_type)

        return field_mapping
    except Exception as e:
        logging.getLogger(__name__).error(
            f"Failed to dynamically get field mapping for {schema}.{table}: {e}",
            exc_info=True,
        )
        return {}
