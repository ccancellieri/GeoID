-- Operator cleanup: drop orphaned per-collection items tables left behind by a
-- failed / rolled-back collection hard-delete.
--
-- Context:
--   Each PostgreSQL-backed collection gets a hub table `t_<base>` plus its
--   sidecars `t_<base>_attributes`, `t_<base>_geometries`,
--   `t_<base>_item_metadata`, `t_<base>_stac_metadata`, all living inside the
--   catalog's tenant schema `s_<base>`. The authoritative record of which hub a
--   live collection owns is the per-tenant pin in
--   `"<tenant_schema>".collection_configs` → `config_data->>'physical_table'`.
--
--   Before the PG-only-transaction hard-delete refactor, a collection delete
--   whose external (ES/GCS) teardown idled the transaction past
--   `idle_in_transaction_session_timeout` could roll back AFTER the
--   `collection_configs` pin row was deleted but BEFORE the physical tables were
--   dropped — leaving the `t_<base>*` tables as orphans with no pin pointing at
--   them. They consume no correctness (nothing reads them) but accumulate as
--   dead storage. This script finds and drops them.
--
--   Per the no-in-place-DDL-from-the-app rule, the application must never do
--   this; it is an operator step run against the database out of band.
--
-- Safety model:
--   * A `t_<base>` hub is considered an orphan ONLY when NO row in that same
--     tenant schema's `collection_configs` pins it as `physical_table`. The pin
--     is WriteOnce and set during collection creation, so a live or
--     mid-provision collection always has its pin — only genuinely abandoned
--     tables qualify.
--   * Only schemas matching the tenant pattern `s\_%` are touched. Control-plane
--     schemas (catalog, tasks, iam, configs, public, …) are never considered.
--   * Only tables matching the hub/sidecar naming convention are considered;
--     arbitrary tables in a tenant schema are never dropped.
--   * A tenant schema with NO `collection_configs` table is SKIPPED entirely
--     (we cannot determine its live set, so we drop nothing).
--
-- DRY-RUN BY DEFAULT. With no flag the script only RAISEs NOTICE for every hub
-- it WOULD drop and commits nothing destructive. To actually drop, pass
-- `-v do_drop=true`.
--
-- Usage:
--   Preview (safe):   psql "$DSN" -f scripts/cleanup_orphan_items_tables.sql
--   Execute drops:    psql "$DSN" -v do_drop=true -f scripts/cleanup_orphan_items_tables.sql
--   One schema only:  psql "$DSN" -v target_schema=s_ql98bdk4 -v do_drop=true -f scripts/cleanup_orphan_items_tables.sql
--
-- Idempotent: re-running finds nothing once orphans are gone.

\if :{?do_drop}
\else
  \set do_drop false
\endif
\if :{?target_schema}
\else
  \set target_schema ''
\endif

BEGIN;

-- Push the psql vars into GUCs the DO block can read.
SELECT set_config('cleanup.do_drop', :'do_drop', false);
SELECT set_config('cleanup.target_schema', :'target_schema', false);

DO $$
DECLARE
    v_do_drop      boolean := lower(coalesce(current_setting('cleanup.do_drop', true), 'false')) IN ('true', 't', '1', 'yes', 'on');
    v_target       text    := nullif(current_setting('cleanup.target_schema', true), '');
    r_schema       RECORD;
    r_hub          RECORD;
    v_has_configs  boolean;
    v_dropped_tbls int := 0;
    v_dropped_hubs int := 0;
    v_schemas_seen int := 0;
    v_orphan_total int := 0;
BEGIN
    RAISE NOTICE 'cleanup_orphan_items_tables: mode=%, target_schema=%',
        CASE WHEN v_do_drop THEN 'EXECUTE (will drop)' ELSE 'DRY-RUN (no drops)' END,
        coalesce(v_target, '<all tenant schemas>');

    FOR r_schema IN
        SELECT nspname
        FROM pg_namespace
        WHERE nspname LIKE 's\_%'
          AND (v_target IS NULL OR nspname = v_target)
        ORDER BY nspname
    LOOP
        v_schemas_seen := v_schemas_seen + 1;

        -- Require the per-tenant collection_configs table; skip otherwise so we
        -- never drop in a schema whose live set we cannot read.
        SELECT to_regclass(format('%I.collection_configs', r_schema.nspname)) IS NOT NULL
          INTO v_has_configs;
        IF NOT v_has_configs THEN
            RAISE NOTICE '  [skip] %: no collection_configs table — cannot determine live set', r_schema.nspname;
            CONTINUE;
        END IF;

        -- Orphan hubs: hub-named tables in this schema whose base is NOT pinned
        -- as a physical_table by any collection_configs row in this same schema.
        FOR r_hub IN EXECUTE format($q$
            WITH live AS (
                SELECT DISTINCT config_data->>'physical_table' AS hub
                FROM %1$I.collection_configs
                WHERE config_data ? 'physical_table'
                  AND nullif(config_data->>'physical_table', '') IS NOT NULL
            ),
            hubs AS (
                -- bare hub tables: t_<base36>, no underscore suffix
                SELECT c.relname AS hub
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %1$L
                  AND c.relkind = 'r'
                  AND c.relname ~ '^t_[0-9a-z]+$'
                UNION
                -- derive hub from sidecar tables: strip the known suffix
                SELECT regexp_replace(c.relname, '_(attributes|geometries|item_metadata|stac_metadata)$', '') AS hub
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %1$L
                  AND c.relkind = 'r'
                  AND c.relname ~ '^t_[0-9a-z]+_(attributes|geometries|item_metadata|stac_metadata)$'
            )
            SELECT h.hub
            FROM hubs h
            LEFT JOIN live l ON l.hub = h.hub
            WHERE l.hub IS NULL
            ORDER BY h.hub
        $q$, r_schema.nspname)
        LOOP
            v_orphan_total := v_orphan_total + 1;

            IF v_do_drop THEN
                -- Drop the hub and every table named like a sidecar of it.
                -- Hub last would be ideal, but CASCADE on each handles FK order.
                DECLARE
                    r_tbl RECORD;
                BEGIN
                    FOR r_tbl IN
                        SELECT c.relname
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = r_schema.nspname
                          AND c.relkind = 'r'
                          AND (c.relname = r_hub.hub OR c.relname LIKE r_hub.hub || '\_%')
                    LOOP
                        EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE',
                                       r_schema.nspname, r_tbl.relname);
                        v_dropped_tbls := v_dropped_tbls + 1;
                    END LOOP;
                END;
                v_dropped_hubs := v_dropped_hubs + 1;
                RAISE NOTICE '  [drop] %.% (hub + sidecars)', r_schema.nspname, r_hub.hub;
            ELSE
                RAISE NOTICE '  [would-drop] %.% (hub + sidecars)', r_schema.nspname, r_hub.hub;
            END IF;
        END LOOP;
    END LOOP;

    RAISE NOTICE 'cleanup_orphan_items_tables: schemas_scanned=% orphan_hubs=% (% hubs / % tables actually dropped)',
        v_schemas_seen, v_orphan_total, v_dropped_hubs, v_dropped_tbls;
END $$;

COMMIT;
