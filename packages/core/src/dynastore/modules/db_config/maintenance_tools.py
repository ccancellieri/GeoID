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
import os
import datetime
from dateutil.relativedelta import relativedelta
from typing import Literal
from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    DbResource,
    ResultHandler,
    DbEngine,
    DbConnection,
)
from dynastore.modules.db_config.partition_tools import ensure_partition_exists
from dynastore.modules.db_config.locking_tools import (
    acquire_startup_lock,
    check_schema_exists,
    acquire_lock_if_needed,
    check_cron_job_exists,
    check_extension_exists,
)

logger = logging.getLogger(__name__)

# --- Legacy Compatibility (Deprecating) ---


async def execute_ddl_block(conn: DbResource, ddl_block: str, **kwargs):
    """
    Deprecated: Use DDLQuery(ddl_block).execute(conn, **params) instead.
    This wrapper delegates to DDLQuery which handles splitting and locking centrally.
    """
    import warnings

    warnings.warn(
        "execute_ddl_block is deprecated and will be removed in a future version. "
        "Use DDLQuery(ddl_block).execute(conn, **params) instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    await DDLQuery(ddl_block).execute(conn, **kwargs)


async def ensure_db_extension(conn: DbResource, extension_name: str):
    """
    Ensures a database extension exists.
    """
    try:
        logger.info(f"Ensuring extension '{extension_name}' exists...")
        await DDLQuery(
            f'CREATE EXTENSION IF NOT EXISTS "{extension_name}" CASCADE;'
        ).execute(conn)
    except Exception as e:
        logger.error(f"Failed to ensure extension '{extension_name}': {e}")
        raise


async def ensure_schema_exists(conn: DbResource, schema_name: str):
    """
    Ensures a database schema exists using centralized DDL coordination.
    """
    await DDLQuery(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"').execute(conn)
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


async def register_retention_policy(
    conn: DbResource,
    schema: str,
    table: str,
    policy: str = "prune",
    interval: str = "daily",
    retention_period: str = "12 months",
    column: str = "timestamp",
    schedule_cron: str = "0 3 * * 0",
):
    try:
        from dynastore.tools.db import validate_sql_identifier
        validate_sql_identifier(schema)
        validate_sql_identifier(table)

        func_name = f"maintain_partitions_{schema}_{table}"
        job_name = f"{policy}_{schema}_{table}"

        # Check if policy already exists before acquiring lock
        async def check_policy_exists():
            from .locking_tools import check_cron_job_exists

            return await check_cron_job_exists(conn, job_name)

        # Use the centralized DDLQuery system
        # IMPORTANT: We must quote identifiers because schema might be mixed-case (Base62)
        create_func_sql = f"""
        CREATE OR REPLACE FUNCTION "{schema}"."{func_name}"() RETURNS void AS $$
        DECLARE
            row RECORD;
            cutoff_date DATE;
        BEGIN
            -- Bound AccessExclusiveLock wait: if a partition is being scanned
            -- (SKIP LOCKED consumers), fail this DROP fast and let the next
            -- cron tick retry. Without this, DROP could wait unboundedly and
            -- pile up cron workers behind a long scan.
            SET LOCAL lock_timeout = '10s';
            cutoff_date := date_trunc('{interval}', NOW()) - INTERVAL '{retention_period}';
            -- nspname match in pg_namespace is a literal string comparison, so case must match exactly.
            FOR row IN SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = '{schema}' AND c.relkind = 'r' AND c.relname ~ '^{table}_\\d{{4}}_\\d{{2}}$' LOOP
                DECLARE
                    date_str TEXT;
                    part_date DATE;
                BEGIN
                    date_str := substring(row.relname from '\\d{{4}}_\\d{{2}}$');
                    part_date := to_date(date_str, 'YYYY_MM');
                    IF part_date < cutoff_date THEN
                        RAISE NOTICE 'Pruning old partition: %.%', '{schema}', row.relname;
                        -- Quote the schema in the dynamic SQL
                        EXECUTE format('DROP TABLE "{schema}".%I', row.relname);
                    END IF;
                EXCEPTION WHEN OTHERS THEN
                    -- 55P03 (lock_timeout) lands here too — log-and-continue so
                    -- one contended partition can't block the whole sweep.
                    RAISE WARNING 'Failed to process partition %.%: %', '{schema}', row.relname, SQLERRM;
                END;
            END LOOP;
        END;
        $$ LANGUAGE plpgsql;
        """

        # We wrap the multi-statement block in a DDLQuery.
        # DDLExecutor will handle the splitting respecting the functional body ($$).
        policy_ddl = f"""
        {create_func_sql};
        
        -- SAFE UNSCHEDULE
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = '{job_name}') THEN
                PERFORM cron.unschedule('{job_name}');
            END IF;
        END;
        $$;
        
        -- Schedule via DQL (cron.schedule returns value)
        SELECT cron.schedule('{job_name}', '{schedule_cron}', $CMD$SELECT "{schema}"."{func_name}"()$CMD$);
        """

        await DDLQuery(
            policy_ddl,
            check_query=check_policy_exists,
        ).execute(conn)

        logger.info(f"Registered retention policy for {schema}.{table}")
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise e


async def register_partition_creation_policy(
    conn: DbResource,
    schema: str,
    table: str,
    interval: Literal["monthly", "yearly"] = "monthly",
    periods_ahead: int = 3,
    schedule_cron: str = "0 2 1 * *",
):
    """
    Registers a pg_cron job that creates future partitions automatically.

    This complements ensure_future_partitions (which runs at startup) by
    ensuring partitions are created even if the application runs continuously
    without restarts for longer than the initial periods_ahead window.

    Args:
        conn: Database connection or engine.
        schema: Schema containing the partitioned table.
        table: Partitioned table name.
        interval: Partition interval — "monthly" or "yearly".
        periods_ahead: Number of future periods to ensure exist on each run.
        schedule_cron: Cron expression for the job. Default: 1st of each month at 2 AM.
    """
    try:
        from dynastore.tools.db import validate_sql_identifier
        validate_sql_identifier(schema)
        validate_sql_identifier(table)

        func_name = f"create_partitions_{schema}_{table}"
        job_name = f"partcreate_{schema}_{table}"

        async def check_job_exists():
            return await check_cron_job_exists(conn, job_name)

        if interval == "monthly":
            # Generate partition names like {table}_YYYY_MM with monthly bounds
            create_func_sql = f"""
            CREATE OR REPLACE FUNCTION "{schema}"."{func_name}"() RETURNS void AS $$
            DECLARE
                i INT;
                target_date DATE;
                start_date TIMESTAMPTZ;
                end_date TIMESTAMPTZ;
                part_name TEXT;
            BEGIN
                FOR i IN 0..{periods_ahead} LOOP
                    target_date := date_trunc('month', NOW()) + (i || ' months')::INTERVAL;
                    start_date := target_date;
                    end_date := target_date + INTERVAL '1 month';
                    part_name := '{table}_' || to_char(target_date, 'YYYY_MM');
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = '{schema}' AND c.relname = part_name
                    ) THEN
                        EXECUTE format(
                            'CREATE TABLE IF NOT EXISTS "{schema}".%I PARTITION OF "{schema}"."{table}" FOR VALUES FROM (%L) TO (%L)',
                            part_name, start_date::TEXT, end_date::TEXT
                        );
                        RAISE NOTICE 'Created partition: {schema}.%', part_name;
                    END IF;
                END LOOP;
            END;
            $$ LANGUAGE plpgsql;
            """
        elif interval == "yearly":
            create_func_sql = f"""
            CREATE OR REPLACE FUNCTION "{schema}"."{func_name}"() RETURNS void AS $$
            DECLARE
                i INT;
                target_date DATE;
                start_date TIMESTAMPTZ;
                end_date TIMESTAMPTZ;
                part_name TEXT;
            BEGIN
                FOR i IN 0..{periods_ahead} LOOP
                    target_date := date_trunc('year', NOW()) + (i || ' years')::INTERVAL;
                    start_date := target_date;
                    end_date := target_date + INTERVAL '1 year';
                    part_name := '{table}_' || to_char(target_date, 'YYYY');
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = '{schema}' AND c.relname = part_name
                    ) THEN
                        EXECUTE format(
                            'CREATE TABLE IF NOT EXISTS "{schema}".%I PARTITION OF "{schema}"."{table}" FOR VALUES FROM (%L) TO (%L)',
                            part_name, start_date::TEXT, end_date::TEXT
                        );
                        RAISE NOTICE 'Created partition: {schema}.%', part_name;
                    END IF;
                END LOOP;
            END;
            $$ LANGUAGE plpgsql;
            """
        else:
            raise ValueError(f"Unsupported interval for partition creation policy: {interval}")

        policy_ddl = f"""
        {create_func_sql};

        -- SAFE UNSCHEDULE
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = '{job_name}') THEN
                PERFORM cron.unschedule('{job_name}');
            END IF;
        END;
        $$;

        SELECT cron.schedule('{job_name}', '{schedule_cron}', $CMD$SELECT "{schema}"."{func_name}"()$CMD$);
        """

        await DDLQuery(
            policy_ddl,
            check_query=check_job_exists,
        ).execute(conn)

        logger.info(f"Registered partition creation policy for {schema}.{table} ({interval}, {periods_ahead} ahead, schedule: {schedule_cron})")
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise e


async def ensure_global_cron_cleanup(
    conn: DbResource, schedule_cron: str = "0 4 * * *"
):
    """
    Registers a global maintenance job to clean up orphaned cron jobs.
    This job runs daily and removes cron jobs for schemas/tables that no longer exist.
    """
    func_name = "cleanup_orphaned_cron_jobs"
    job_name = "system_cleanup_orphaned_cron_jobs"

    # Define existence check for the lock wrapper
    async def check_cleanup_job_exists():
        return await check_cron_job_exists(conn, job_name)

    cleanup_ddl = f"""
    CREATE SCHEMA IF NOT EXISTS "platform";
    CREATE OR REPLACE FUNCTION platform.cleanup_orphaned_cron_jobs() RETURNS void AS $$
    DECLARE
        job_rec RECORD;
        target_schema TEXT;
        schema_exists BOOLEAN;
    BEGIN
        FOR job_rec IN SELECT jobid, jobname FROM cron.job LOOP
            -- Extract tenant schema name (s_ + 8 base36 chars) from anywhere in the job name.
            -- Tenant cron jobs use formats like:
            --   prune_s_abc12345_access_logs
            --   partcreate_s_abc12345_stats_aggregates
            --   monthly_cleanup_logs_s_abc12345
            --   archive_catalog_events_s_abc12345
            -- Expanded [0-9a-z] x8 avoids {8} which clashes with TemplateQueryBuilder
            target_schema := (regexp_match(job_rec.jobname, '(^|_)(s_[0-9a-z][0-9a-z][0-9a-z][0-9a-z][0-9a-z][0-9a-z][0-9a-z][0-9a-z])(_|$)'))[2];

            IF target_schema IS NOT NULL THEN
                SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = target_schema) INTO schema_exists;

                IF NOT schema_exists THEN
                    RAISE NOTICE 'Removing orphaned cron job % (schema % gone)', job_rec.jobname, target_schema;
                    PERFORM cron.unschedule(job_rec.jobid);
                END IF;
            END IF;
        END LOOP;
    END;
    $$ LANGUAGE plpgsql;

    -- SAFE UNSCHEDULE
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = '{job_name}') THEN
            PERFORM cron.unschedule('{job_name}');
        END IF;
    END;
    $$;

    SELECT cron.schedule('{job_name}', '{schedule_cron}', $CMD$SELECT platform.{func_name}()$CMD$);
    """

    # Always execute: CREATE OR REPLACE FUNCTION must run to update the function
    # body if it changed. The cron.schedule() call inside uses IF EXISTS + unschedule
    # to handle idempotency.
    await DDLQuery(cleanup_ddl).execute(conn)
    logger.info("Registered global orphaned cron job cleanup task.")


async def register_cron_job(
    conn: DbResource, job_name: str, schedule: str, command: str
):
    """
    Registers a generic pg_cron job safely using the check-before-lock pattern.
    """
    command = command.strip().rstrip(";")
    # Escape single quotes for safe interpolation into PL/pgSQL string literals
    safe_job_name = job_name.replace("'", "''")
    safe_schedule = schedule.replace("'", "''")
    # Escape dollar-quote delimiter inside command to prevent early termination
    safe_command = command.replace("$CMD$", "$$CMD$$")

    async def check_exists():
        from .locking_tools import check_cron_job_exists

        return await check_cron_job_exists(conn, job_name)

    cron_ddl = f"""
    -- SAFE UNSCHEDULE
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = '{safe_job_name}') THEN
            PERFORM cron.unschedule('{safe_job_name}');
        END IF;
    END;
    $$;

    SELECT cron.schedule('{safe_job_name}', '{safe_schedule}', $CMD${safe_command}$CMD$);
    """

    await DDLQuery(cron_ddl, check_query=check_exists).execute(conn)
