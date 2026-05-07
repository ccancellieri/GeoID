#!/bin/bash
#
# db_reset.sh — inspect or wipe all app-owned schemas in the PostgreSQL DB.
#
# Cleanup tiers (see plan synchronous-enchanting-hearth):
#   * Boot tier (THIS SCRIPT) — Postgres-level; drops user schemas, wipes
#     orphan cron jobs, preserves/truncates the `keycloak` schema. Invoked by
#     src/dynastore/scripts/db_entrypoint_dev.sh at dev/test container start,
#     or directly via `src/dynastore/scripts/db_reset.sh`.
#   * Test tier — application-aware Python cleanup driven by
#     tests/dynastore/test_utils/cleanup_db.py (CleanupRegistry). Does NOT
#     drop schemas; operates on known table/schema patterns only.
#
# DynaStore modules create schemas on startup via CREATE SCHEMA IF NOT EXISTS.
# A clean DB means next startup rebuilds every schema from current DDL — useful
# for verifying migrations, events rewrites, or catching stale-schema drift.
#
# System/preserved schemas are sourced from src/dynastore/scripts/reset_policy.env
# ($PRESERVED_SCHEMAS); system cron jobs likewise ($SYSTEM_CRON_JOBS).
#
# Usage:
#   ./src/dynastore/scripts/db_reset.sh check                 # list non-system schemas + sizes
#   ./src/dynastore/scripts/db_reset.sh drop SCHEMA           # drop a specific schema
#   ./src/dynastore/scripts/db_reset.sh reset [--yes]         # drop user schemas (confirm unless --yes)
#                                      [--keycloak]    # also TRUNCATE tables in keycloak schema
#                                                      # (forces realm re-import on next keycloak start)
#                                      [--keep-cron]   # do NOT wipe cron.job rows
#
# Connection resolution (first match wins):
#   1. $DATABASE_URL
#   2. Running container named in $DB_CONTAINER (default: geoid_db)
#   3. Default localhost:54320 (dev docker-compose mapping)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/reset_policy.env"

cmd="${1:-check}"
shift || true

DB_CONTAINER="${DB_CONTAINER:-geoid_db}"

run_psql() {
    local sql="$1"
    if [[ -n "${DATABASE_URL:-}" ]]; then
        psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "$sql"
    elif command -v docker >/dev/null 2>&1 && docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^${DB_CONTAINER}$"; then
        docker exec "$DB_CONTAINER" psql -U testuser -d gis_dev -v ON_ERROR_STOP=1 -c "$sql"
    else
        psql "postgresql://testuser:testpassword@localhost:54320/gis_dev" -v ON_ERROR_STOP=1 -c "$sql"
    fi
}

# Build SQL IN-list from a whitespace-separated policy variable.
quote_list() {
    local out="" item
    for item in $1; do
        [[ -n "$out" ]] && out+=","
        out+="'$item'"
    done
    printf '%s' "$out"
}

PRESERVED_IN="$(quote_list "$PRESERVED_SCHEMAS")"
SYSTEM_CRON_IN="$(quote_list "$SYSTEM_CRON_JOBS")"

list_schemas_sql="SELECT nspname FROM pg_namespace
 WHERE nspname NOT IN (${PRESERVED_IN})
   AND nspname NOT LIKE 'pg_temp_%'
   AND nspname NOT LIKE 'pg_toast_temp_%'
 ORDER BY nspname;"

case "$cmd" in
    check)
        echo "=== non-system schemas ==="
        run_psql "$list_schemas_sql"
        echo
        echo "=== schema sizes ==="
        run_psql "SELECT n.nspname AS schema,
                         pg_size_pretty(sum(pg_total_relation_size(c.oid))::bigint) AS size
                  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE n.nspname NOT IN (${PRESERVED_IN})
                    AND n.nspname NOT LIKE 'pg_temp_%'
                    AND n.nspname NOT LIKE 'pg_toast_temp_%'
                  GROUP BY n.nspname ORDER BY n.nspname;"
        ;;
    drop)
        arg="${1:-}"
        [[ -n "$arg" ]] || { echo "Error: drop requires a schema name"; exit 2; }
        for reserved in $PRESERVED_SCHEMAS; do
            if [[ "$arg" == "$reserved" ]]; then
                echo "Refusing to drop preserved schema: $arg"; exit 2
            fi
        done
        case "$arg" in
            pg_*) echo "Refusing to drop system schema: $arg"; exit 2 ;;
        esac
        run_psql "DROP SCHEMA IF EXISTS \"$arg\" CASCADE;"
        echo "Dropped schema: $arg"
        ;;
    reset)
        assume_yes=0
        wipe_keycloak=0
        keep_cron=0
        for flag in "$@"; do
            case "$flag" in
                --yes) assume_yes=1 ;;
                --keycloak) wipe_keycloak=1 ;;
                --keep-cron) keep_cron=1 ;;
                *) echo "Unknown flag for reset: $flag"; exit 2 ;;
            esac
        done
        if [[ $assume_yes -eq 0 ]]; then
            read -r -p "DROP every non-system schema CASCADE — wipes ALL app data. Continue? [y/N] " ans
            [[ "$ans" == "y" || "$ans" == "Y" ]] || { echo "Aborted."; exit 1; }
        fi
        schemas=$(run_psql "$list_schemas_sql" | awk 'NR>2 && NF==1 && $1!="" {print $1}' | grep -Ev '^\(' || true)
        if [[ -n "$schemas" ]]; then
            echo "Dropping:"; echo "$schemas" | sed 's/^/  - /'
            drop_sql=""
            while IFS= read -r s; do
                [[ -z "$s" ]] && continue
                drop_sql+="DROP SCHEMA IF EXISTS \"$s\" CASCADE;"$'\n'
            done <<< "$schemas"
            run_psql "$drop_sql"
        else
            echo "No user schemas to drop."
        fi

        # Wipe orphaned cron jobs (jobs for now-gone schemas). System jobs preserved.
        if [[ $keep_cron -eq 0 ]]; then
            echo "Wiping non-system cron.job rows (preserving: $SYSTEM_CRON_JOBS)..."
            run_psql "DELETE FROM cron.job WHERE jobname NOT IN (${SYSTEM_CRON_IN});"
        fi

        # Idempotent recreate — covers the rare case where someone dropped it manually.
        run_psql "CREATE SCHEMA IF NOT EXISTS keycloak;"

        if [[ $wipe_keycloak -eq 1 ]]; then
            echo "Dropping and recreating schema 'keycloak' (forces Liquibase fresh-init + realm re-import)..."
            run_psql "DROP SCHEMA IF EXISTS keycloak CASCADE; CREATE SCHEMA keycloak;"
        else
            # Detect corrupt Liquibase state: tables exist but databasechangelog is missing.
            # This happens when databasechangelog was dropped manually while keycloak tables remain.
            # Liquibase would then fail with "relation X already exists" on every restart.
            # Fix: drop and recreate the schema so Liquibase starts fresh.
            corrupt=$(run_psql "SELECT CASE
                WHEN EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='keycloak' AND tablename='client')
                 AND NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='keycloak' AND tablename='databasechangelog')
                THEN 'yes' ELSE 'no' END;" | awk 'NR==3{print $1}')
            if [[ "$corrupt" == "yes" ]]; then
                echo "WARNING: keycloak schema is in a corrupt Liquibase state (tables exist but databasechangelog is missing). Dropping and recreating schema..."
                run_psql "DROP SCHEMA keycloak CASCADE; CREATE SCHEMA keycloak;"
            fi
        fi
        echo "Reset complete. Restart dynastore to rebuild schemas."
        ;;
    *)
        echo "Unknown command: $cmd"
        echo "Usage: $0 {check|drop SCHEMA|reset [--yes] [--keycloak] [--keep-cron]}"
        exit 2
        ;;
esac
