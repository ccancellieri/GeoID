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

# dynastore/modules/tasks/workclass_ddl.py

"""DDL for the two global hot-plane workclass tables.

``tasks.events``
    Global events queue replacing ``events.events``.  Partitioned daily by
    ``day`` (a DATE column equal to ``created_at::date``).  Tenancy is
    column-based: ``catalog_id NULL`` means platform-wide / PLATFORM scope.
    Enforces lowercase ``scope`` at the DB level (CHECK constraint) — legacy
    ``events.events`` stored mixed-case values; this table requires lowercase
    from day one (see PR #1804).

``tasks.storage``
    Global storage-operation outbox replacing per-tenant ``storage_outbox``
    tables.  Partitioned daily by ``day``.  Tenancy is column-based via
    ``catalog_id`` (the logical tenant identifier; physical PG schema is
    derived at the boundary).  Generalised across entity tiers:
    ``entity_kind`` (item|collection|catalog|asset) distinguishes the tier;
    ``entity_id`` holds the tier-specific identifier.

Both tables:
- Use ``PARTITION BY RANGE (day)`` with a plain ``shard`` / ``driver_id``
  column (NOT a second partition level — flat RANGE only).
- Include a fairness partial index leading with ``catalog_id`` so
  per-tenant claim queries get index-only scans without cross-tenant
  interference.
- Ship a DEFAULT partition so inserts never fail on an out-of-range day
  (e.g. clock skew, far-future test data).
- Are accompanied by a daily PL/pgSQL create-ahead function
  (``create_partitions_{schema}_events`` /
  ``create_partitions_{schema}_storage``) that opens a 30-day window,
  and a daily retention function
  (``maintain_partitions_{schema}_events`` /
  ``maintain_partitions_{schema}_storage``) that drops day-leaves older
  than 30 days.  Both windows are intentionally short — these tables are
  queues, not archives.

Partition naming convention: ``events_YYYY_MM_DD`` /
``storage_YYYY_MM_DD``.  The retention regex
``'^events_\\d{4}_\\d{2}_\\d{2}$'`` (and equivalent for
storage) matches ONLY daily leaves, never the parent table or the
DEFAULT partition.

``ensure_workclass_storage_exists(conn, schema)`` runs at
``TasksModule.lifespan`` startup under the same ``acquire_startup_lock``
guard as ``ensure_task_storage_exists``.  Both are called sequentially in
the same startup block.
"""

import logging

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    ResultHandler,
    DbResource,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tuneable constants — change here only (referenced by DDL strings and tests).
# ---------------------------------------------------------------------------

# Number of daily partitions to create ahead of today.  30 days is generous
# enough to survive a month-long supervisor outage while keeping the leaf
# count bounded.  The loop bound is (AHEAD - 1) for 0-based inclusive range.
_WORKCLASS_CREATE_AHEAD_DAYS: int = 30

# Daily leaves older than this many days are dropped by the retention function.
# Events and storage ops are consumed within seconds to minutes; 30 days is a
# safety margin for replay / debugging before rows are considered archivable.
_WORKCLASS_RETENTION_DAYS: int = 30

# ---------------------------------------------------------------------------
# events table DDL
# ---------------------------------------------------------------------------

EVENTS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.events (
    event_id        UUID            NOT NULL,
    day             DATE            NOT NULL,
    shard           SMALLINT        NOT NULL,
    catalog_id      TEXT,
    scope           TEXT            NOT NULL DEFAULT 'platform'
                        CHECK (scope = lower(scope)),
    event_type      TEXT            NOT NULL,
    status          TEXT            NOT NULL DEFAULT 'PENDING',
    payload         JSONB           NOT NULL DEFAULT '{}'::jsonb,
    claim_version   INTEGER         NOT NULL DEFAULT 0,
    owner_id        TEXT,
    locked_until    TIMESTAMPTZ,
    retry_count     INTEGER         NOT NULL DEFAULT 0,
    max_retries     INTEGER,
    error_message   TEXT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    processed_at    TIMESTAMPTZ,
    PRIMARY KEY (day, event_id)
) PARTITION BY RANGE (day);
"""

EVENTS_DEFAULT_PARTITION_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.events_default
    PARTITION OF {schema}.events DEFAULT;
"""

EVENTS_INDEXES_DDL = """
-- Fairness partial index: leads with (catalog_id, created_at) so per-tenant
-- drain queries get an index-only scan without cross-tenant interference.
-- Partial keeps the index small — only PENDING rows are eligible for claiming.
CREATE INDEX IF NOT EXISTS idx_events_fairness
    ON {schema}.events (catalog_id, created_at)
    WHERE status = 'PENDING';
-- Shard index: enables shard-affine drain workers to restrict their scan.
CREATE INDEX IF NOT EXISTS idx_events_shard
    ON {schema}.events (shard, status, created_at);
"""

# ---------------------------------------------------------------------------
# storage table DDL
# ---------------------------------------------------------------------------

STORAGE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.storage (
    op_id           UUID            NOT NULL,
    day             DATE            NOT NULL,
    catalog_id      TEXT            NOT NULL,
    driver_id       TEXT            NOT NULL,
    collection_id   TEXT,
    entity_kind     TEXT            NOT NULL DEFAULT 'item',
    entity_id       TEXT,
    op              TEXT            NOT NULL,
    status          TEXT            NOT NULL DEFAULT 'ready',
    ready_at        TIMESTAMPTZ     NOT NULL DEFAULT now(),
    op_payload      JSONB           NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key TEXT,
    claim_version   INTEGER         NOT NULL DEFAULT 0,
    claimed_by      TEXT,
    claimed_at      TIMESTAMPTZ,
    attempts        INTEGER         NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    PRIMARY KEY (day, op_id)
) PARTITION BY RANGE (day);
"""

STORAGE_DEFAULT_PARTITION_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.storage_default
    PARTITION OF {schema}.storage DEFAULT;
"""

STORAGE_INDEXES_DDL = """
-- Fairness partial index: leads with (catalog_id, ready_at) so per-tenant
-- drain workers claim the oldest ready ops first without cross-tenant noise.
CREATE INDEX IF NOT EXISTS idx_storage_fairness
    ON {schema}.storage (catalog_id, ready_at)
    WHERE status = 'ready';
-- Driver index: enables driver-affine workers to restrict their scan.
CREATE INDEX IF NOT EXISTS idx_storage_driver
    ON {schema}.storage (driver_id, catalog_id, status, ready_at);
"""

# ---------------------------------------------------------------------------
# Partition-management functions — DAILY, distinct from the MONTHLY functions
# in tasks_module.py that serve tasks.tasks.
#
# These are plain (non-f) strings so {schema} survives until DDLQuery
# substitutes it. DDLQuery substitutes via str.replace("{schema}", value) —
# NOT str.format() — so regex bounds MUST use SINGLE braces (\d{4}). A doubled
# brace (\d{{4}}) is not collapsed by .replace; PostgreSQL would receive the
# literal \d{{4}}, which matches no partition name, so the retention DROP would
# silently never fire and leaves would accumulate forever. Verified against
# live PG: single-brace \d{4} matches events_YYYY_MM_DD, doubled does not.
# The loop bound (0..29) and INTERVAL ('30 days') are hardcoded to match
# _WORKCLASS_CREATE_AHEAD_DAYS=30 and _WORKCLASS_RETENTION_DAYS=30.
# Update both the constants AND the SQL strings if the windows change.
# ---------------------------------------------------------------------------

# events: create-ahead (daily leaves, 30 days window — 0..29 inclusive)
EVENTS_PARTCREATE_FUNC_DDL = """
CREATE OR REPLACE FUNCTION "{schema}"."create_partitions_{schema}_events"() RETURNS void AS $$
DECLARE
    i INT;
    target_date DATE;
    next_date DATE;
    part_name TEXT;
BEGIN
    -- Create daily leaf partitions from today through 30 days ahead (0-based, inclusive).
    -- Window is intentionally bounded: events rows are consumed within
    -- seconds to minutes, so a 30-day window is a generous safety margin.
    FOR i IN 0..29 LOOP
        target_date := CURRENT_DATE + (i || ' days')::INTERVAL;
        next_date   := target_date + INTERVAL '1 day';
        part_name   := 'events_' || to_char(target_date, 'YYYY_MM_DD');
        IF NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = '{schema}' AND c.relname = part_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS "{schema}".%I '
                'PARTITION OF "{schema}".events '
                'FOR VALUES FROM (%L) TO (%L)',
                part_name,
                target_date::TEXT,
                next_date::TEXT
            );
            RAISE NOTICE 'Created partition: {schema}.%', part_name;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
"""

# events: retention (drop daily leaves older than 30 days)
EVENTS_RETENTION_FUNC_DDL = """
CREATE OR REPLACE FUNCTION "{schema}"."maintain_partitions_{schema}_events"() RETURNS void AS $$
DECLARE
    row RECORD;
    cutoff_date DATE;
    date_str TEXT;
    part_date DATE;
    default_deleted BIGINT;
    prune_count INT;
    prune_list TEXT;
BEGIN
    -- Bound AccessExclusiveLock wait: if a partition is being actively scanned,
    -- fail fast and let the next supervisor tick retry rather than stalling.
    SET LOCAL lock_timeout = '10s';
    cutoff_date := CURRENT_DATE - INTERVAL '30 days';
    -- Pre-flight (#2106): announce the prune at LOG level so it is never silent.
    -- The per-partition message below is NOTICE, suppressed by the server log at
    -- the default log_min_messages=WARNING; this LOG line records how many daily
    -- leaf partitions and which names this tick will DROP.
    SELECT count(*), string_agg(c.relname, ', ' ORDER BY c.relname)
      INTO prune_count, prune_list
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = '{schema}' AND c.relkind = 'r'
        AND c.relname ~ '^events_\\d{4}_\\d{2}_\\d{2}$'
        AND to_date(substring(c.relname from '\\d{4}_\\d{2}_\\d{2}$'), 'YYYY_MM_DD') < cutoff_date;
    IF prune_count > 0 THEN
        RAISE LOG 'partition retention [{schema}.events]: dropping % daily partition(s) older than % : %', prune_count, cutoff_date, prune_list;
    END IF;
    -- Match ONLY daily leaf partitions (events_YYYY_MM_DD).  The regex
    -- explicitly excludes the parent table name and the DEFAULT partition.
    FOR row IN
        SELECT relname FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '{schema}'
          AND c.relkind = 'r'
          AND c.relname ~ '^events_\\d{4}_\\d{2}_\\d{2}$'
    LOOP
        BEGIN
            date_str := substring(row.relname from '\\d{4}_\\d{2}_\\d{2}$');
            part_date := to_date(date_str, 'YYYY_MM_DD');
            IF part_date < cutoff_date THEN
                RAISE NOTICE 'Pruning old partition: {schema}.%', row.relname;
                EXECUTE format('DROP TABLE "{schema}".%I', row.relname);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Failed to process partition {schema}.%: %', row.relname, SQLERRM;
        END;
    END LOOP;
    -- Drain stale rows from the DEFAULT partition (clock skew / far-future events).
    DELETE FROM "{schema}".events_default
    WHERE day < (CURRENT_DATE - INTERVAL '30 days');
    GET DIAGNOSTICS default_deleted = ROW_COUNT;
    IF default_deleted > 0 THEN
        RAISE NOTICE 'Pruned % row(s) from {schema}.events_default', default_deleted;
    END IF;
END;
$$ LANGUAGE plpgsql;
"""

# storage: create-ahead (daily leaves, 30 days window — 0..29 inclusive)
STORAGE_PARTCREATE_FUNC_DDL = """
CREATE OR REPLACE FUNCTION "{schema}"."create_partitions_{schema}_storage"() RETURNS void AS $$
DECLARE
    i INT;
    target_date DATE;
    next_date DATE;
    part_name TEXT;
BEGIN
    FOR i IN 0..29 LOOP
        target_date := CURRENT_DATE + (i || ' days')::INTERVAL;
        next_date   := target_date + INTERVAL '1 day';
        part_name   := 'storage_' || to_char(target_date, 'YYYY_MM_DD');
        IF NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = '{schema}' AND c.relname = part_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS "{schema}".%I '
                'PARTITION OF "{schema}".storage '
                'FOR VALUES FROM (%L) TO (%L)',
                part_name,
                target_date::TEXT,
                next_date::TEXT
            );
            RAISE NOTICE 'Created partition: {schema}.%', part_name;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
"""

# storage: retention (drop daily leaves older than 30 days)
STORAGE_RETENTION_FUNC_DDL = """
CREATE OR REPLACE FUNCTION "{schema}"."maintain_partitions_{schema}_storage"() RETURNS void AS $$
DECLARE
    row RECORD;
    cutoff_date DATE;
    date_str TEXT;
    part_date DATE;
    default_deleted BIGINT;
    prune_count INT;
    prune_list TEXT;
BEGIN
    SET LOCAL lock_timeout = '10s';
    cutoff_date := CURRENT_DATE - INTERVAL '30 days';
    -- Pre-flight (#2106): announce the prune at LOG level so it is never silent.
    -- The per-partition message below is NOTICE, suppressed by the server log at
    -- the default log_min_messages=WARNING; this LOG line records how many daily
    -- leaf partitions and which names this tick will DROP.
    SELECT count(*), string_agg(c.relname, ', ' ORDER BY c.relname)
      INTO prune_count, prune_list
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = '{schema}' AND c.relkind = 'r'
        AND c.relname ~ '^storage_\\d{4}_\\d{2}_\\d{2}$'
        AND to_date(substring(c.relname from '\\d{4}_\\d{2}_\\d{2}$'), 'YYYY_MM_DD') < cutoff_date;
    IF prune_count > 0 THEN
        RAISE LOG 'partition retention [{schema}.storage]: dropping % daily partition(s) older than % : %', prune_count, cutoff_date, prune_list;
    END IF;
    FOR row IN
        SELECT relname FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '{schema}'
          AND c.relkind = 'r'
          AND c.relname ~ '^storage_\\d{4}_\\d{2}_\\d{2}$'
    LOOP
        BEGIN
            date_str := substring(row.relname from '\\d{4}_\\d{2}_\\d{2}$');
            part_date := to_date(date_str, 'YYYY_MM_DD');
            IF part_date < cutoff_date THEN
                RAISE NOTICE 'Pruning old partition: {schema}.%', row.relname;
                EXECUTE format('DROP TABLE "{schema}".%I', row.relname);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Failed to process partition {schema}.%: %', row.relname, SQLERRM;
        END;
    END LOOP;
    DELETE FROM "{schema}".storage_default
    WHERE day < (CURRENT_DATE - INTERVAL '30 days');
    GET DIAGNOSTICS default_deleted = ROW_COUNT;
    IF default_deleted > 0 THEN
        RAISE NOTICE 'Pruned % row(s) from {schema}.storage_default', default_deleted;
    END IF;
END;
$$ LANGUAGE plpgsql;
"""


# ---------------------------------------------------------------------------
# Startup ensure
# ---------------------------------------------------------------------------


async def ensure_workclass_storage_exists(conn: DbResource, schema: str) -> None:
    """Provision ``tasks.events`` and ``tasks.storage`` partitioned tables.

    Called once at ``TasksModule.lifespan`` startup under the same
    ``acquire_startup_lock`` guard as ``ensure_task_storage_exists``.
    All DDL statements are idempotent (``CREATE TABLE IF NOT EXISTS``,
    ``CREATE INDEX IF NOT EXISTS``, ``CREATE OR REPLACE FUNCTION``).

    Steps executed in order:
    1. Create parent tables (``PARTITION BY RANGE (day)``).
    2. Attach DEFAULT partitions — absorbs out-of-range days; safe under
       concurrent writes (PostgreSQL only checks for a conflicting DEFAULT).
    3. Create fairness + operational indexes.
    4. Provision create-ahead and retention PL/pgSQL functions
       (``CREATE OR REPLACE`` — always up to date on re-deploy).
    5. Call the create-ahead function once to materialise the initial
       ``_WORKCLASS_CREATE_AHEAD_DAYS``-day leaf window so the dispatcher
       can write immediately after startup.

    The ``{schema}`` placeholder in every DDL string is substituted by
    ``DDLQuery(...).execute(conn, schema=schema)`` — the same mechanism
    used by ``ensure_task_storage_exists`` throughout ``tasks_module.py``.
    """
    # 1. Parent tables
    await DDLQuery(EVENTS_TABLE_DDL).execute(conn, schema=schema)
    await DDLQuery(STORAGE_TABLE_DDL).execute(conn, schema=schema)

    # 2. DEFAULT partitions — absorbs out-of-range days; idempotent on re-deploy.
    await DDLQuery(EVENTS_DEFAULT_PARTITION_DDL).execute(conn, schema=schema)
    await DDLQuery(STORAGE_DEFAULT_PARTITION_DDL).execute(conn, schema=schema)

    # 3. Indexes
    await DDLQuery(EVENTS_INDEXES_DDL).execute(conn, schema=schema)
    await DDLQuery(STORAGE_INDEXES_DDL).execute(conn, schema=schema)

    # 4. Maintenance functions (CREATE OR REPLACE — always up to date)
    await DDLQuery(EVENTS_PARTCREATE_FUNC_DDL).execute(conn, schema=schema)
    await DDLQuery(EVENTS_RETENTION_FUNC_DDL).execute(conn, schema=schema)
    await DDLQuery(STORAGE_PARTCREATE_FUNC_DDL).execute(conn, schema=schema)
    await DDLQuery(STORAGE_RETENTION_FUNC_DDL).execute(conn, schema=schema)

    # 5. Materialise initial day window by calling the create-ahead functions once.
    await DQLQuery(
        f'SELECT "{schema}"."create_partitions_{schema}_events"()',
        result_handler=ResultHandler.NONE,
    ).execute(conn)
    await DQLQuery(
        f'SELECT "{schema}"."create_partitions_{schema}_storage"()',
        result_handler=ResultHandler.NONE,
    ).execute(conn)

    logger.info(
        "TasksModule: provisioned workclass storage (events + storage) "
        "for schema %r with %d-day create-ahead window and %d-day retention.",
        schema,
        _WORKCLASS_CREATE_AHEAD_DAYS,
        _WORKCLASS_RETENTION_DAYS,
    )
