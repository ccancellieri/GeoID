-- One-off cleanup: drop the orphaned `gcp`, `platform`, and `events` schemas.
--
-- Context (issue #1807 — schema/module consolidation):
--   The application stopped creating and using these three schemas once their
--   contents were relocated:
--     * gcp.catalog_buckets        -> GcpCatalogBucketConfig (config-backed)
--     * gcp.reconciliation_events  -> dead (never had a consumer)
--     * platform.maintenance_schedule -> tasks.maintenance_schedule
--     * platform.asset_cleanup()      -> catalog.asset_cleanup() (identical body)
--     * platform.update_collection_extents() -> dead
--     * events.event_subscriptions -> tasks.event_subscriptions
--     * events.events (+ partitions), events.notify_event_ready() -> dead
--   Fresh-provisioned databases never get these schemas. Databases provisioned
--   before the consolidation still physically carry them as orphans. This drops
--   them. Per the no-in-place-DDL-from-the-app rule, the application must not do
--   this itself — it is an operator step, run once per existing database as part
--   of the release.
--
-- IMPORTANT — why this is not a bare `DROP SCHEMA platform CASCADE`:
--   Tenant `<schema>.<hub>_attributes` tables provisioned before the cut still
--   carry a `trg_asset_cleanup` trigger bound to `platform.asset_cleanup`. The
--   app only wires the new `catalog.asset_cleanup` on newly provisioned tables,
--   so existing tenants are still on the old function. A bare CASCADE drop would
--   delete those live triggers and silently disable asset cleanup on every
--   existing tenant. This script therefore re-points each such trigger to the
--   identical `catalog.asset_cleanup` BEFORE dropping `platform`.
--
-- Safe to re-run (idempotent): once schemas are gone the guards short-circuit,
-- the re-point loop finds nothing, and the DROPs are IF EXISTS no-ops. Safe on a
-- never-had-them database too.
--
-- Usage:
--   psql "$DSN" -f scripts/cleanup_dead_schemas_1807.sql

BEGIN;

-- 0. Pre-flight guards. Any failure aborts the whole transaction so nothing is
--    dropped on a database where the relocation targets are not in place yet.
DO $$
DECLARE
    v_cnt int;
BEGIN
    -- The replacement cleanup function must exist before we re-point triggers.
    IF to_regprocedure('catalog.asset_cleanup()') IS NULL THEN
        RAISE EXCEPTION
            'catalog.asset_cleanup() is missing — deploy the consolidated build before running this cleanup.';
    END IF;

    -- platform.maintenance_schedule was relocated to tasks.maintenance_schedule;
    -- the supervisor repopulates it at startup. Refuse to drop platform until the
    -- target is present and populated, so we never lose the only schedule.
    IF to_regclass('tasks.maintenance_schedule') IS NULL THEN
        RAISE EXCEPTION
            'tasks.maintenance_schedule is absent — relocation target not provisioned; boot the consolidated build first.';
    END IF;
    SELECT count(*) INTO v_cnt FROM tasks.maintenance_schedule;
    IF v_cnt = 0 THEN
        RAISE EXCEPTION
            'tasks.maintenance_schedule is empty — supervisor has not registered job cadences yet; boot the consolidated build first.';
    END IF;

    -- bucket_name moved to config without a backfill. If a database still holds
    -- bucket rows they would be lost; require them migrated to config first.
    IF to_regclass('gcp.catalog_buckets') IS NOT NULL THEN
        SELECT count(*) INTO v_cnt FROM gcp.catalog_buckets;
        IF v_cnt > 0 THEN
            RAISE EXCEPTION
                'gcp.catalog_buckets still has % row(s) — migrate bucket names to GcpCatalogBucketConfig before dropping.', v_cnt;
        END IF;
    END IF;

    -- Webhook subscriptions moved to tasks.event_subscriptions. Refuse to drop
    -- the old table while it still carries subscriptions.
    IF to_regclass('events.event_subscriptions') IS NOT NULL THEN
        SELECT count(*) INTO v_cnt FROM events.event_subscriptions;
        IF v_cnt > 0 THEN
            RAISE EXCEPTION
                'events.event_subscriptions still has % row(s) — recreate them against tasks.event_subscriptions before dropping.', v_cnt;
        END IF;
    END IF;
END $$;

-- 1. Snapshot the triggers about to be re-pointed (for audit/log capture).
SELECT
    count(*)                                           AS triggers_to_repoint,
    count(DISTINCT n.nspname)                          AS tenant_schemas_affected
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_proc p ON p.oid = t.tgfoid
WHERE NOT t.tgisinternal
  AND p.pronamespace::regnamespace::text = 'platform'
  AND p.proname = 'asset_cleanup';

-- 2. Re-point every tenant `trg_asset_cleanup` trigger from platform.asset_cleanup
--    to the identical catalog.asset_cleanup, preserving timing/events/arguments.
--    pg_get_triggerdef() yields the exact statement; we swap only the function name.
DO $$
DECLARE
    r   RECORD;
    def TEXT;
BEGIN
    FOR r IN
        SELECT t.oid,
               t.tgname,
               n.nspname AS tbl_schema,
               c.relname AS tbl_name
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_proc p ON p.oid = t.tgfoid
        WHERE NOT t.tgisinternal
          AND p.pronamespace::regnamespace::text = 'platform'
          AND p.proname = 'asset_cleanup'
    LOOP
        def := replace(
            pg_get_triggerdef(r.oid),
            'platform.asset_cleanup',
            'catalog.asset_cleanup'
        );
        EXECUTE format('DROP TRIGGER %I ON %I.%I', r.tgname, r.tbl_schema, r.tbl_name);
        EXECUTE def;
    END LOOP;
END $$;

-- 3. Hard guard: no trigger on a table OUTSIDE the dead schemas may still depend
--    on a function inside them. Such a trigger (e.g. a tenant trg_asset_cleanup we
--    failed to re-point) would be silently CASCADE-dropped, disabling live
--    behavior — abort instead. Triggers whose own table lives inside a dead schema
--    (e.g. events.events partitions -> events.notify_event_ready) are expected and
--    drop cleanly together with that schema, so they are excluded here.
DO $$
DECLARE
    v_cnt int;
BEGIN
    SELECT count(*) INTO v_cnt
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_proc p ON p.oid = t.tgfoid
    WHERE NOT t.tgisinternal
      AND p.pronamespace::regnamespace::text IN ('platform', 'gcp', 'events')
      AND n.nspname NOT IN ('platform', 'gcp', 'events');
    IF v_cnt > 0 THEN
        RAISE EXCEPTION
            'Aborting: % trigger(s) on live tables still bound to functions in platform/gcp/events.', v_cnt;
    END IF;
END $$;

-- 4. Drop the now-orphaned schemas. CASCADE only reaches their own tables,
--    partitions, functions, and internal TOAST relations — nothing live depends
--    on them (verified: no external FK, no external pg_depend edge).
DROP SCHEMA IF EXISTS gcp CASCADE;
DROP SCHEMA IF EXISTS platform CASCADE;
DROP SCHEMA IF EXISTS events CASCADE;

COMMIT;
