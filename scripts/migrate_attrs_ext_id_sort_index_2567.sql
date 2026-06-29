-- One-time offline migration: add the external_id sort index to every existing
-- collection's attributes sidecar table.
--
-- Context (issue #2567 — slow default /items listing):
--   GET /features/catalogs/{cat}/collections/{col}/items?limit=10 (no sort
--   specified) takes ~2.5-3s on a 1.6M-row collection.  The default sort is
--   ORDER BY sc_attributes.external_id ASC, applied via the query optimizer.
--
--   The existing unique identity index on the attributes sidecar table is
--   "idx_{table}_ext_id" ON (geoid, external_id) — geoid is the LEADING column,
--   so PostgreSQL cannot use it for ORDER BY external_id.  The planner is forced
--   to hash-join hub->attrs and then sort the full result before applying LIMIT.
--
--   The fix is a plain B-tree index "idx_{table}_ext_id_sort" ON (external_id).
--   The query optimizer now passes join_type="INNER" to the attrs sidecar only on
--   the default-sort path (no explicit user sort), so the planner can choose:
--   index scan attrs in external_id order → nested-loop to hub PK → stream N
--   rows → done.  No filesort needed.
--
--   New collections get the index automatically from the updated get_ddl().
--   This script adds it retroactively to every existing attributes sidecar table.
--
-- ============================================================================
-- STEP 0 — ORPHAN PRE-FLIGHT CHECK
-- ============================================================================
--
-- The INNER JOIN optimisation in query_optimizer.py applies only on the
-- default-sort path (no explicit user sort).  If hub rows exist without a
-- matching attrs row ("orphans"), those items would be silently absent from
-- the default-sort listing after the code change is deployed.  This step
-- surfaces any such orphans BEFORE the sort index is created, so operators
-- can reconcile them first.
--
-- If orphan_hub_rows > 0 is reported for a collection:
--   1. Investigate why those hub rows have no attrs counterpart (re-ingest
--      failed, partial write before a schema change, etc.).
--   2. Either re-ingest the affected items or hard-delete the orphan hub rows.
--   3. Re-run this migration once the collection is clean.
--
-- NOTE: the INNER-sort optimisation only affects the default ORDER BY listing.
-- Filtered queries and explicit-sort queries continue to use LEFT JOIN and are
-- therefore NOT affected by orphan rows.  However, the listing gap is still
-- worth fixing before trusting the default-sort path.
--
-- The query below generates one SELECT per hub table that has a corresponding
-- attrs sidecar, then executes them all via \gexec.
--
SELECT
    format(
        $$SELECT %L AS location, count(*) AS orphan_hub_rows
FROM %I.%I h
LEFT JOIN %I.%I sc ON h.geoid = sc.geoid
WHERE sc.geoid IS NULL
HAVING count(*) > 0$$,
        t.table_schema || '.' || t.table_name,
        t.table_schema, t.table_name,
        t.table_schema, t.table_name || '_attributes'
    )
FROM information_schema.tables t
WHERE t.table_type   = 'BASE TABLE'
  AND t.table_schema LIKE 's\_%' ESCAPE '\'
  AND t.table_name   ~ '^t_[0-9a-z]+$'
  AND to_regclass(
          quote_ident(t.table_schema) || '.'
          || quote_ident(t.table_name || '_attributes')
      ) IS NOT NULL
ORDER BY t.table_schema, t.table_name
\gexec

-- ============================================================================
-- STEP 1 — NON-DEFAULT external_id_field VISIBILITY CHECK
-- ============================================================================
--
-- This migration targets the standard "external_id" column name (the default
-- FeatureAttributeSidecarConfig.external_id_field value).  Collections
-- configured with a non-standard external_id_field name (e.g. "my_ext_id")
-- will not get a sort index here and the default-sort optimisation will not
-- fire for them.
--
-- The query below lists attrs tables that exist but lack an "external_id"
-- column so operators can identify non-standard collections.  For each
-- reported table, create the sort index manually with the correct column:
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_{table}_ext_id_sort"
--   ON {schema}."{table}" (<actual_external_id_column>);
--
SELECT
    t.table_schema || '.' || t.table_name AS attrs_table,
    'WARNING: no external_id column — sort index not created; see migration comment' AS note
FROM information_schema.tables t
WHERE t.table_type   = 'BASE TABLE'
  AND t.table_schema LIKE 's\_%' ESCAPE '\'
  AND t.table_name   ~ '^t_[0-9a-z]+_attributes$'
  AND NOT EXISTS (
      SELECT 1
      FROM information_schema.columns c
      WHERE c.table_schema = t.table_schema
        AND c.table_name   = t.table_name
        AND c.column_name  = 'external_id'
  )
ORDER BY t.table_schema, t.table_name;

-- ============================================================================
-- STEP 2 — CREATE SORT INDEXES (CONCURRENTLY, non-blocking)
-- ============================================================================
--
-- For every attrs table that HAS an "external_id" column, emit a
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS statement and execute it via
-- \gexec.  psql's \gexec runs each output row as a separate top-level SQL
-- command in autocommit mode — required because CREATE INDEX CONCURRENTLY
-- cannot run inside a transaction block.
--
-- Monitor active builds:
--   SELECT relid::regclass, phase, blocks_done, blocks_total
--   FROM pg_stat_progress_create_index;
--
-- Usage (must be run outside a transaction block — psql default autocommit):
--   psql "$DSN" -f scripts/migrate_attrs_ext_id_sort_index_2567.sql
--
-- Safety model:
--   * CREATE INDEX CONCURRENTLY — non-blocking; reads and writes continue.
--   * IF NOT EXISTS — fully idempotent; safe to re-run.
--   * Only schemas matching s\_% (tenant schemas) are touched.
--   * Only tables matching t_%_attributes with an "external_id" column.

SELECT
    format(
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS %I ON %I.%I (external_id)',
        'idx_' || t.table_name || '_ext_id_sort',
        t.table_schema,
        t.table_name
    )
FROM information_schema.tables t
JOIN information_schema.columns c
    ON  c.table_schema = t.table_schema
    AND c.table_name   = t.table_name
    AND c.column_name  = 'external_id'
WHERE t.table_type   = 'BASE TABLE'
  AND t.table_schema LIKE 's\_%' ESCAPE '\'
  AND t.table_name   ~ '^t_[0-9a-z]+_attributes$'
ORDER BY t.table_schema, t.table_name
\gexec
