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

import asyncio
import logging
import datetime
from dateutil.relativedelta import relativedelta
from typing import Literal

from sqlalchemy.exc import OperationalError as SAOperationalError

from dynastore.modules.db_config.exceptions import UniqueViolationError
from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    DbResource,
    ResultHandler,
)
from dynastore.modules.db_config.partition_tools import ensure_partition_exists
from dynastore.modules.db_config.locking_tools import (
    acquire_startup_lock,  # noqa: F401  # re-export: kept for external/plugin consumers of maintenance_tools.acquire_startup_lock
    run_startup_ddl_tolerating_lock_timeout,  # noqa: F401  # re-export: consumed as maintenance_tools.run_startup_ddl_tolerating_lock_timeout by 6 module lifespans (crs, notebooks, styles, proxy, connected_systems, moving_features)
)

logger = logging.getLogger(__name__)

# Public surface of this maintenance facade. Listing names here (in addition to
# the per-line F401-suppression directive) makes the cross-package re-exports
# below machine-checkable and survives a ``ruff --fix`` F401 sweep: ruff never
# reports — and never strips — an import that appears in ``__all__`` (refs #1747,
# #1547).
# ``acquire_startup_lock`` in particular is referenced only as
# ``maintenance_tools.acquire_startup_lock`` (never inside this module), so
# without this protection a per-file F401 pass removes it and breaks startup for
# every module lifespan that acquires the lock (the #1705 -> #1745 regression).
# ``DDLQuery``/``DQLQuery``/``DbResource``/``ResultHandler`` are re-exported back
# into ``locking_tools`` via this facade; ``check_cron_job_exists`` is consumed
# by tenant cron callers.
__all__ = [
    # cross-package re-exports (load-bearing — see note above)
    "acquire_startup_lock",
    "run_startup_ddl_tolerating_lock_timeout",
    "DDLQuery",
    "DQLQuery",
    "DbResource",
    "ResultHandler",
    # functions defined in this module
    "retry_on_invalidated_connection",
    "ensure_db_extension",
    "ensure_schema_exists",
    "ensure_enum_type",
    "ensure_future_partitions",
]


async def retry_on_invalidated_connection(
    coro_factory,
    *,
    label: str,
    attempts: int = 3,
    initial_delay: float = 0.5,
):
    """Run ``coro_factory()`` with a SQLAlchemy disconnect retry policy.

    Scoped strictly to ``OperationalError.connection_invalidated == True`` —
    the marker SQLAlchemy sets when its pool decides the connection is dead
    (e.g. psycopg2's ``server closed the connection unexpectedly`` during a
    dev DB bounce).  Other ``OperationalError``s and unrelated exceptions
    fail fast so real bugs are not masked.

    ``coro_factory`` is a *callable* returning a fresh awaitable on each
    attempt — coroutine objects are single-shot and cannot be re-awaited.
    """
    delay = initial_delay
    for attempt in range(1, attempts + 1):
        try:
            return await coro_factory()
        except SAOperationalError as e:
            if not getattr(e, "connection_invalidated", False) or attempt == attempts:
                logger.error(f"{label} failed: {e}")
                raise
            logger.warning(
                f"{label}: connection invalidated (DB likely restarting); "
                f"retrying in {delay:.1f}s ({attempt}/{attempts - 1})."
            )
            await asyncio.sleep(delay)
            delay *= 2


async def ensure_db_extension(conn: DbResource, extension_name: str):
    """
    Ensures a database extension exists.

    Foundational modules (DatastoreModule) call this in their lifespan before
    accepting traffic.  Dev compositions reset the DB mid-startup
    (db_entrypoint_dev.sh), which can drop the connection between Layer 2
    pool warm-up and this DDL — see ``retry_on_invalidated_connection``.
    """
    logger.info(f"Ensuring extension '{extension_name}' exists...")
    await retry_on_invalidated_connection(
        lambda: DDLQuery(
            f'CREATE EXTENSION IF NOT EXISTS "{extension_name}" CASCADE;'
        ).execute(conn),
        label=f"ensure_db_extension('{extension_name}')",
    )


async def ensure_schema_exists(conn: DbResource, schema_name: str):
    """
    Ensures a database schema exists using centralized DDL coordination.

    Defensive against a known cold-start race (un-fao/GeoID#821): on
    multi-worker startup, ``DDLExecutor``'s advisory-lock coordination can
    miss a peer's ``CREATE SCHEMA`` commit on the post-wait re-check
    (suspected MVCC snapshot age inside the inner transaction). The loser
    then issues ``CREATE SCHEMA`` and trips ``pg_namespace_nspname_index``.
    Catching that specific UniqueViolation is safe — the schema already
    exists, which is the function's post-condition.
    """
    try:
        await DDLQuery(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"').execute(conn)
    except UniqueViolationError as e:
        original = getattr(e, "original_exception", None)
        constraint = getattr(original, "constraint_name", None)
        if constraint != "pg_namespace_nspname_index":
            raise
        logger.warning(
            "ensure_schema_exists: peer worker created schema '%s' after our "
            "DDLExecutor re-check (cold-start advisory-lock race, "
            "un-fao/GeoID#821); treating as success.",
            schema_name,
        )
    logger.info(f"Schema '{schema_name}' verified/ready.")


async def ensure_enum_type(conn: DbResource, type_name: str, labels: list[str]):
    """
    Safely ensures a custom ENUM type exists, handling concurrent creation.

    Uses a ``check_query`` against ``pg_type`` to skip creation when the type
    already exists in the target namespace. This avoids races between
    concurrent startup hooks that would otherwise raise
    ``UniqueViolationError`` on ``pg_type_typname_nsp_index`` and poison the
    surrounding transaction.
    """
    from dynastore.tools.db import validate_sql_identifier
    validate_sql_identifier(type_name)
    escaped_labels = [label.replace("'", "''") for label in labels]
    label_str = ", ".join([f"'{label}'" for label in escaped_labels])
    create_type_sql = f'CREATE TYPE "{type_name}" AS ENUM ({label_str});'

    await DDLQuery(
        create_type_sql,
        check_query=(
            "SELECT 1 FROM pg_type t "
            "JOIN pg_namespace n ON n.oid = t.typnamespace "
            "WHERE t.typname = :type_name "
            "AND n.nspname = ANY (current_schemas(true))"
        ),
    ).execute(conn, type_name=type_name)
    logger.info(f"ENUM type '{type_name}' verified/ready.")


async def ensure_future_partitions(
    conn: DbResource,
    schema: str,
    table: str,
    interval: Literal["daily", "monthly", "yearly"] = "monthly",
    periods_ahead: int = 1,
    column: str = "timestamp",
    strategy: Literal["RANGE"] = "RANGE",
):
    """Ensures future time-based partitions exist for a table."""
    try:
        today = datetime.date.today()
        interval_map = {
            "monthly": relativedelta(months=1),
            "yearly": relativedelta(years=1),
            "weekly": relativedelta(weeks=1),
            "daily": relativedelta(days=1),
        }
        delta = interval_map.get(
            interval, relativedelta(months=1)
        )  # Default to monthly

        for i in range(periods_ahead + 1):
            target_date = today + (delta * i)
            # Safe call, ensure_partition_exists should handle basic concurrency,
            # but we wrap here too just in case.
            await ensure_partition_exists(
                conn,
                table_name=table,
                schema=schema,
                strategy=strategy,
                partition_value=target_date,
                interval=interval,
            )
        logger.info(
            f"Ensured {interval} partitions for {schema}.{table} up to {periods_ahead} periods ahead."
        )

    except Exception as e:
        import traceback

        traceback.print_exc()
        logger.error(f"Failed to ensure future partitions for {schema}.{table}: {e}")
        # Explicit request to re-raise to crash testing if this fails
        raise e


