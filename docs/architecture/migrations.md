# The Migration System

This document describes the database migration architecture of Agro-Informatics Platform (AIP) — Catalog Services. The system provides safe, auditable, and admin-controlled evolution of both the global schema and per-tenant collection schemas.

## Design Principles

### No Auto-Apply at Startup

Earlier versions auto-applied migrations during service startup, which created a race between deploying code and evolving a shared database. The current design separates concerns:

- **Startup**: calls `check_migration_status()` — reads the migration manifest hash in a single SELECT, logs a warning if any migration is pending, and returns immediately. No DDL is executed.
- **Migration apply**: triggered only by an admin via the REST API or the migrations dashboard. The `run_migrations()` path is protected by a PostgreSQL advisory lock so concurrent admin triggers are safe.

This means a new deployment can start serving traffic immediately, and the operator decides when to apply pending migrations.

### Advisory Locking

`run_migrations()` acquires `pg_advisory_xact_lock(hashtext('dynastore_migration'))` before executing any DDL. Only one process across the entire cluster can run migrations at a time; competing callers block until the lock is released, then detect there is nothing left to apply.

### Tamper Detection

Every applied migration script is stored in `public.schema_migrations` with a SHA-256 hash. On each startup the runner re-hashes all scripts registered in memory and compares them to the stored hashes. If a previously applied script has been modified on disk, `DRIFT_DETECTED` is returned and the service logs an error — no further migrations are applied until the drift is resolved.

### Fast-Path Manifest Hash

To avoid reading the filesystem on every cold start, the runner pre-computes a single manifest hash from all registered script hashes. This value is stored in `public.schema_migrations` under the synthetic key `__manifest__`. A fast-path check compares the in-memory manifest hash against the stored value in one `SELECT`. When they match, startup completes without scanning any migration files.

---

## Two Migration Scopes

### Global Migrations

Applied to the `public` (or a nominated global) schema. These cover shared infrastructure: the `tasks` tables, `catalogs`, `collections`, `schema_migrations` itself, and any cross-tenant indexes or functions.

Global migrations are registered by module name:

```python
from dynastore.modules.db_config.migration_runner import register_module_migrations

register_module_migrations("catalog", "dynastore.modules.catalog.migrations")
```

Script naming follows the `v{NNNN}__{description}.sql` convention. Scripts are discovered from the package via `importlib.resources` and executed in version-number order.

### Tenant Migrations

Applied to each tenant's own PostgreSQL schema (e.g., `s_abc123`). These cover tenant-specific tables: `collection_configs`, any per-tenant indexes, and future additions to the tenant shell.

Tenant migrations are registered separately:

```python
from dynastore.modules.db_config.migration_runner import register_tenant_migrations

register_tenant_migrations("catalog", "dynastore.modules.catalog.tenant_migrations")
```

Scripts use `{schema}` as a placeholder for the tenant schema name:

```sql
-- v0001__add_schema_hash.sql
ALTER TABLE {schema}.collection_configs
    ADD COLUMN IF NOT EXISTS schema_hash VARCHAR(64);
```

On migration apply, the runner iterates all rows in `catalog.catalogs`, substitutes `{schema}`, and tracks applied versions in a per-tenant `{schema}.schema_migrations` table with the same structure as the global one.

---

## Tracking Table

```
public.schema_migrations
├── id            SERIAL PRIMARY KEY
├── version       VARCHAR(10)      -- e.g. "v0002"
├── module        VARCHAR(255)     -- e.g. "db_config", "tasks"
├── description   TEXT             -- derived from filename
├── script_hash   VARCHAR(64)      -- SHA-256 of script content
├── rollback_sql  TEXT             -- content of paired _rollback.sql (nullable)
├── applied_at    TIMESTAMPTZ      -- when this version was applied
└── applied_by    VARCHAR(255)     -- service NAME env var
```

The synthetic `__manifest__` row stores the combined hash of all registered scripts for the fast-path check.

---

## Rollback Convention

For each `v{NNNN}__description.sql` forward script, an optional `v{NNNN}__rollback.sql` can be placed in the same package. The rollback SQL is stored in the `rollback_sql` column at apply time. Rollback is triggered via the admin API:

```
POST /admin/migrations/rollback
{"module": "db_config", "version": "v0002"}
```

The `StructuralMigrationTask` executes the stored rollback SQL in the same advisory-locked transaction.

---

## Dry-Run Mode

Both `run_migrations()` and the admin API support `dry_run=True`. In dry-run mode, the runner resolves the list of pending scripts and returns their SQL without executing anything. The migrations dashboard uses this to show a preview before the operator clicks Apply.

---

## Admin Trigger Flow

```
Admin clicks "Apply" in dashboard
  → POST /admin/migrations/apply {"scope": "all", "dry_run": false}
  → AdminService enqueues StructuralMigrationTask
  → Worker picks up task
  → run_migrations(engine, scope="all", dry_run=False)
      → acquire pg_advisory_xact_lock(...)
      → execute pending global scripts
      → for each tenant schema: execute pending tenant scripts
      → release lock
  → Task status → COMPLETED
  → Dashboard polls task status and refreshes
```

---

## Relationship to per-collection schema

This document covers DDL for **shared** infrastructure tables. A
collection's *own* physical table is a separate concern: its columns are
created once at provisioning (`CREATE TABLE IF NOT EXISTS`), and the
application never issues in-place `ALTER TABLE` against it. Drift between
a collection's declared schema and its physical table is reconciled
read-only; adding a column for real requires a fresh (re)provision. See
[Schema Evolution & Drift](../components/schema_evolution.md) and
[Items Schema](../components/items_schema.md).

---

## Files

| Path | Purpose |
|------|---------|
| `src/dynastore/modules/db_config/migration_runner.py` | Core runner: registration, status check, apply, tenant iteration |
| `src/dynastore/modules/db_config/migrations/` | Global SQL scripts (`v{NNNN}__*.sql`) |
| `src/dynastore/tasks/structural_migration/task.py` | TaskProtocol wrapper — runs migrations from the task queue |
| `src/dynastore/extensions/admin/migration_routes.py` | REST API endpoints for status, pending, apply, history, rollback |
| `src/dynastore/extensions/admin/static/migrations_panel.html` | Web dashboard |
| `src/dynastore/modules/catalog/migrations/v0001__catalog_schema.sql` | Creates `catalog` schema + `catalog.catalogs` + `catalog.shared_properties` |
| `src/dynastore/modules/catalog/tenant_migrations/v0001__config_tables.sql` | Per-tenant `catalog_configs` + `collection_configs` tables |
| `src/dynastore/tasks/schema_migration/` | Safe data migration pipeline (export → backup-rename → recreate → import) |
