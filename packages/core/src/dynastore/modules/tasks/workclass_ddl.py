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
# Shared partition-management PL/pgSQL template — create-ahead + retention.
#
# One Python-side template renders all three workclass partition-function
# pairs (events/daily, storage/daily, tasks/monthly in tasks_module.py) from
# (table, granularity, window, retention). Previously each pair was a
# hand-copied raw-SQL near-duplicate; the daily pair (events/storage) was
# byte-identical modulo table name, and the monthly pair (tasks.tasks) added
# a second, differently-shaped copy for month-grained partitions.
#
# The rendered strings are plain (non-f) text so {schema} survives until
# DDLQuery substitutes it via str.replace("{schema}", value) — NOT
# str.format() — so regex bounds MUST use SINGLE braces (\d{4}). A doubled
# brace (\d{{4}}) is not collapsed by .replace; PostgreSQL would receive the
# literal \d{{4}}, which matches no partition name, so the retention DROP
# would silently never fire and leaves would accumulate forever. Verified
# against live PG: single-brace \d{4} matches events_YYYY_MM_DD, doubled
# does not. Template substitution below therefore also avoids str.format()
# entirely (which would require escaping every literal brace) — it uses
# str.replace() against @MARKER@ tokens that cannot collide with {schema},
# \d{4}, or the %I/%L specifiers passed to PostgreSQL's own format().
# ---------------------------------------------------------------------------

_GRANULARITY_SPECS: dict[str, dict[str, str]] = {
    "day": {
        "date_format": "YYYY_MM_DD",
        "name_regex": r"\d{4}_\d{2}_\d{2}",
        "step_unit": "days",
        "period_unit": "day",
        "granularity_label": "daily",
        # Partition-key column is DATE, so cutoff/drain compare against
        # CURRENT_DATE directly.
        "cutoff_expr": "CURRENT_DATE",
        "default_partition_column": "day",
        "default_partition_expr": "CURRENT_DATE",
        "value_decls": "    target_date DATE;\n    next_date DATE;\n",
        "compute_block": (
            "        target_date := CURRENT_DATE + (i || ' days')::INTERVAL;\n"
            "        next_date   := target_date + INTERVAL '1 day';\n"
        ),
        "from_expr": "target_date::TEXT",
        "to_expr": "next_date::TEXT",
    },
    "month": {
        "date_format": "YYYY_MM",
        "name_regex": r"\d{4}_\d{2}",
        "step_unit": "months",
        "period_unit": "month",
        "granularity_label": "monthly",
        # date_trunc('day', ...) — NOT date_trunc('month', ...) and NOT the
        # invalid unit 'daily' (see #1998). Partitions are monthly, but the
        # cutoff only needs day precision; truncating to 'month' would move
        # the cutoff to the 1st of the current month, pruning up to 29 days
        # earlier than the retention window promises.
        "cutoff_expr": "date_trunc('day', NOW())",
        "default_partition_column": "timestamp",
        "default_partition_expr": "NOW()",
        "value_decls": "    target_date DATE;\n    start_date TIMESTAMPTZ;\n    end_date TIMESTAMPTZ;\n",
        "compute_block": (
            "        target_date := date_trunc('month', NOW()) + (i || ' months')::INTERVAL;\n"
            "        start_date := target_date;\n"
            "        end_date := target_date + INTERVAL '1 month';\n"
        ),
        "from_expr": "start_date::TEXT",
        "to_expr": "end_date::TEXT",
    },
}

_PARTCREATE_TEMPLATE = """
CREATE OR REPLACE FUNCTION "{schema}"."create_partitions_{schema}_@TABLE@"() RETURNS void AS $$
DECLARE
    i INT;
@VALUE_DECLS@    part_name TEXT;
BEGIN
    -- Create @GRANULARITY_LABEL@ leaf partitions from today through @WINDOW@ @STEP_UNIT@ ahead (0-based, inclusive).
    -- Window is intentionally bounded: @TABLE@ rows are consumed within
    -- seconds to minutes, so a @WINDOW@-@PERIOD_UNIT@ window is a generous safety margin.
    FOR i IN 0..@WINDOW_BOUND@ LOOP
@COMPUTE_BLOCK@        part_name   := '@TABLE_PREFIX@' || to_char(target_date, '@DATE_FORMAT@');
        IF NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = '{schema}' AND c.relname = part_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS "{schema}".%I '
                'PARTITION OF "{schema}".@TABLE@ '
                'FOR VALUES FROM (%L) TO (%L)',
                part_name,
                @FROM_EXPR@,
                @TO_EXPR@
            );
            RAISE NOTICE 'Created partition: {schema}.%', part_name;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
"""

_RETENTION_TEMPLATE = """
CREATE OR REPLACE FUNCTION "{schema}"."maintain_partitions_{schema}_@TABLE@"() RETURNS void AS $$
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
    cutoff_date := @CUTOFF_EXPR@ - INTERVAL '@RETENTION@ @STEP_UNIT@';
    -- Pre-flight (#2106): announce the prune at LOG level so it is never silent.
    -- The per-partition message below is NOTICE, suppressed by the server log at
    -- the default log_min_messages=WARNING; this LOG line records how many @GRANULARITY_LABEL@
    -- leaf partitions and which names this tick will DROP.
    SELECT count(*), string_agg(c.relname, ', ' ORDER BY c.relname)
      INTO prune_count, prune_list
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = '{schema}' AND c.relkind = 'r'
        AND c.relname ~ '^@TABLE@_@NAME_REGEX@$'
        AND to_date(substring(c.relname from '@NAME_REGEX@$'), '@DATE_FORMAT@') < cutoff_date;
    IF prune_count > 0 THEN
        RAISE LOG 'partition retention [{schema}.@TABLE@]: dropping % @GRANULARITY_LABEL@ partition(s) older than % : %', prune_count, cutoff_date, prune_list;
    END IF;
    -- Match ONLY @GRANULARITY_LABEL@ leaf partitions (@TABLE_PREFIX@@DATE_FORMAT@).  The regex
    -- explicitly excludes the parent table name and the DEFAULT partition.
    FOR row IN
        SELECT relname FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '{schema}'
          AND c.relkind = 'r'
          AND c.relname ~ '^@TABLE@_@NAME_REGEX@$'
    LOOP
        BEGIN
            date_str := substring(row.relname from '@NAME_REGEX@$');
            part_date := to_date(date_str, '@DATE_FORMAT@');
            IF part_date < cutoff_date THEN
                RAISE NOTICE 'Pruning old partition: {schema}.%', row.relname;
                EXECUTE format('DROP TABLE "{schema}".%I', row.relname);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Failed to process partition {schema}.%: %', row.relname, SQLERRM;
        END;
    END LOOP;
    -- Drain stale rows from the DEFAULT partition (clock skew / far-future @TABLE@).
    DELETE FROM "{schema}".@TABLE@_default
    WHERE @DEFAULT_COLUMN@ < (@DEFAULT_EXPR@ - INTERVAL '@RETENTION@ @STEP_UNIT@');
    GET DIAGNOSTICS default_deleted = ROW_COUNT;
    IF default_deleted > 0 THEN
        RAISE NOTICE 'Pruned % row(s) from {schema}.@TABLE@_default', default_deleted;
    END IF;
END;
$$ LANGUAGE plpgsql;
"""


def _fill(template: str, mapping: dict[str, str]) -> str:
    """Substitute ``@MARKER@`` tokens (str.replace — see module note above)."""
    rendered = template
    for key, value in mapping.items():
        rendered = rendered.replace(f"@{key}@", value)
    return rendered


def render_partition_create_ahead_ddl(*, table: str, granularity: str, window: int) -> str:
    """Render the create-ahead PL/pgSQL function DDL for one workclass table.

    ``granularity`` selects "day" or "month" leaf partitioning; ``window`` is
    the number of periods (days or months) to materialise ahead of today,
    0-based inclusive (``window=30`` renders ``FOR i IN 0..29``). The
    returned string still carries the literal ``{schema}`` placeholder for
    ``DDLQuery`` to substitute.
    """
    spec = _GRANULARITY_SPECS[granularity]
    mapping = {
        "TABLE": table,
        "TABLE_PREFIX": f"{table}_",
        "GRANULARITY_LABEL": spec["granularity_label"],
        "WINDOW": str(window),
        "WINDOW_BOUND": str(window - 1),
        "STEP_UNIT": spec["step_unit"],
        "PERIOD_UNIT": spec["period_unit"],
        "DATE_FORMAT": spec["date_format"],
        "VALUE_DECLS": spec["value_decls"],
        "COMPUTE_BLOCK": spec["compute_block"],
        "FROM_EXPR": spec["from_expr"],
        "TO_EXPR": spec["to_expr"],
    }
    return _fill(_PARTCREATE_TEMPLATE, mapping)


def render_partition_retention_ddl(*, table: str, granularity: str, retention: int) -> str:
    """Render the retention PL/pgSQL function DDL for one workclass table.

    Drops leaf partitions (and drains the DEFAULT partition) older than
    ``retention`` periods (days or months, per ``granularity``). The
    returned string still carries the literal ``{schema}`` placeholder for
    ``DDLQuery`` to substitute.
    """
    spec = _GRANULARITY_SPECS[granularity]
    mapping = {
        "TABLE": table,
        "TABLE_PREFIX": f"{table}_",
        "GRANULARITY_LABEL": spec["granularity_label"],
        "RETENTION": str(retention),
        "STEP_UNIT": spec["step_unit"],
        "DATE_FORMAT": spec["date_format"],
        "NAME_REGEX": spec["name_regex"],
        "CUTOFF_EXPR": spec["cutoff_expr"],
        "DEFAULT_COLUMN": spec["default_partition_column"],
        "DEFAULT_EXPR": spec["default_partition_expr"],
    }
    return _fill(_RETENTION_TEMPLATE, mapping)


# events: create-ahead (daily leaves, 30 days window — 0..29 inclusive)
EVENTS_PARTCREATE_FUNC_DDL = render_partition_create_ahead_ddl(
    table="events", granularity="day", window=_WORKCLASS_CREATE_AHEAD_DAYS
)

# events: retention (drop daily leaves older than 30 days)
EVENTS_RETENTION_FUNC_DDL = render_partition_retention_ddl(
    table="events", granularity="day", retention=_WORKCLASS_RETENTION_DAYS
)

# storage: create-ahead (daily leaves, 30 days window — 0..29 inclusive)
STORAGE_PARTCREATE_FUNC_DDL = render_partition_create_ahead_ddl(
    table="storage", granularity="day", window=_WORKCLASS_CREATE_AHEAD_DAYS
)

# storage: retention (drop daily leaves older than 30 days)
STORAGE_RETENTION_FUNC_DDL = render_partition_retention_ddl(
    table="storage", granularity="day", retention=_WORKCLASS_RETENTION_DAYS
)


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
