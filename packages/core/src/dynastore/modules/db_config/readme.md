# DynaStore Database Migration Framework

## Overview

The migration framework lives in `dynastore.modules.db_config.migration_runner` and
provides production-safe, versioned, multi-application database schema upgrades.

---

## Design Principles

| Principle | Implementation |
|-----------|---------------|
| **Speed-first** | Ultra-fast startup: compare a 16-char SHA-256 hash stored in DB before reading any SQL file |
| **Conservative** | Every script runs in its own transaction; failure rolls back and blocks module startup |
| **Additive-only** | Scripts must use `ADD COLUMN IF NOT EXISTS`, `CREATE TABLE IF NOT EXISTS`, etc. |
| **Tamper-safe** | SHA-256 checksums; modifying an applied script raises `MigrationChecksumError` |
| **Multi-app** | Multiple Cloud Run services share one DB; each gets its own row in `app_state` |
| **Cross-app notify** | `pg_notify` broadcasts on `dynastore.migrations` when a migration runs |
| **Concurrent-safe** | PG advisory lock acquired **only** when pending migrations are detected |

---

## Startup Sequence (Cloud Run)

```
Startup
  │
  ├─ Compute expected manifest HASH in-process ← no file reads, no DB
  │   (from registered package resource names only)
  │
  ├─ CREATE TABLE IF NOT EXISTS app_state, schema_migrations ← idempotent, fast
  │
  ├─ SELECT manifest_hash FROM app_state WHERE app_key = $NAME
  │                                       ← single indexed lookup
  │
  ├─ MATCH? ────────────── YES ─────────► touch last_seen_at, return (< 2ms)
  │
  └─ MISMATCH or NULL ──────────────────► SLOW PATH
        │
        ├─ Scan SQL files, compute checksums
        ├─ Acquire pg_advisory_lock (blocks concurrent migrators)
        ├─ Verify checksums of already-applied scripts
        ├─ Apply pending scripts (each in its own transaction)
        ├─ pg_notify 'dynastore.migrations' → warn other instances
        ├─ Upsert app_state (module_manifest + manifest_hash)
        └─ Release advisory lock
```

---

## Module Registration

Every module that needs migrations registers at import time in its `__init__.py`:

```python
from dynastore.modules.db_config.migration_runner import register_module_migrations

register_module_migrations(
    "catalog",                                        # stable unique name
    "dynastore.modules.catalog.migrations",           # Python package with *.sql files
)
```

The `core` module (`dynastore.modules.db_config.migrations`) is always registered first.

---

## SQL Script Naming Convention

```
v####__short_description_with_underscores.sql
```

| Component | Rule |
|-----------|------|
| `v####` | Zero-padded four-digit integer, e.g. `v0001`, `v0002` |
| `__` | Double-underscore separator |
| description | Snake-case, human-readable description |

Examples:
```
v0001__init.sql
v0002__add_manifest_hash.sql
v0001__provisioning_status.sql
v0002__stac_columns.sql
```

---

## Writing a Migration

### Rules

1. **Additive-only** — prefer `ADD COLUMN IF NOT EXISTS`, `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`.
2. **Never DROP in a forward migration** — use a separate decommission migration after a safe rollout period.
3. **Never edit an applied script** — the checksum guard will block all startups with a fatal error.
4. **One concern per script** — keep scripts focused and small.
5. **Guard for missing tables** — wrap in `DO $$ BEGIN IF EXISTS(...) THEN … END IF; END; $$;` when the table may not exist.
6. **Idempotent** — safe to re-run against a DB that partially failed mid-migration.

### Template

```sql
-- =============================================================================
--  {Module} Migration v#### — {Short description}
--  Owned by: dynastore.modules.{module_name}
--
--  {Longer description of what this migration does and why.}
--  Idempotent: safe to apply more than once.
-- =============================================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = '{schema}' AND table_name = '{table}'
    ) THEN
        ALTER TABLE {schema}.{table}
            ADD COLUMN IF NOT EXISTS new_col JSONB;

        RAISE NOTICE '{module} v####: new_col added to {schema}.{table}.';
    ELSE
        RAISE NOTICE '{module} v####: {schema}.{table} not found — skipping.';
    END IF;
END;
$$;
```

---

## Database Tables

### `public.schema_migrations`

Authoritative history of every applied script across all applications.

| Column | Type | Description |
|--------|------|-------------|
| `module` | VARCHAR(100) | Module name (e.g. `catalog`, `core`) |
| `version` | VARCHAR(10) | Script version (e.g. `v0002`) |
| `description` | VARCHAR(255) | Human-readable description from filename |
| `applied_at` | TIMESTAMPTZ | When it was applied |
| `applied_by` | VARCHAR(200) | `app_key` of the instance that applied it |
| `checksum` | VARCHAR(64) | SHA-256 of the SQL content |

### `public.app_state`

Per-application startup and version tracking.

| Column | Type | Description |
|--------|------|-------------|
| `app_key` | VARCHAR(200) PK | Application identifier (`NAME` env var) |
| `module_manifest` | JSONB | `{module: max_version_applied}` dict |
| `manifest_hash` | VARCHAR(16) | Fast-path 16-char hash for O(1) startup check |
| `app_version` | VARCHAR(50) | Optional application release version |
| `last_seen_at` | TIMESTAMPTZ | Updated on every startup (stale detector) |

---

## Cross-Application Notifications

When an instance applies new migrations it broadcasts on the PostgreSQL channel
`dynastore.migrations`:

```json
{"app_key": "api-service", "manifest": {"catalog": "v0002", "core": "v0002"}}
```

Other running instances can subscribe to this channel to:
- Log a warning that a peer has applied new migrations
- Gracefully restart to pick up the new schema
- Trigger an alert in monitoring

### Subscribing (asyncpg example)

```python
async def on_migration_event(conn, pid, channel, payload):
    data = json.loads(payload)
    logger.warning(
        f"[{data['app_key']}] applied migrations: {data['manifest']}. "
        "Consider restarting to pick up new schema changes."
    )

await conn.add_listener("dynastore.migrations", on_migration_event)
```

---

## Failure Behaviour

If a script fails:
- The script's transaction is **rolled back** automatically.
- `MigrationError` is raised, propagating up through `run_migrations()`.
- The module calling `run_migrations()` should **not start** — fail fast.
- `schema_migrations` will not contain the failed version.
- On the next startup the same delta will be attempted again (advisory lock
  ensures only one instance tries at a time).

---

## Version Progression Example

```
DB starts at v0000 (no schema_migrations table)

core   v0001 → Enable PostgreSQL extensions
core   v0002 → Add manifest_hash to app_state
catalog v0001 → Add provisioning_status to catalog.catalogs
catalog v0002 → STAC compliance columns (conforms_to, links, assets, stac_version, …)
```

---

## Admin-Only Trigger (No Auto-Apply)

Startup calls `check_migration_status()` and **never applies DDL**. It returns one of:

| Status | Meaning |
|--------|---------|
| `UP_TO_DATE` | Manifest hash matches — nothing to do |
| `PENDING_MIGRATIONS` | New scripts registered — operator must apply |
| `DRIFT_DETECTED` | Applied script checksum changed — fatal, operator must resolve |

Apply pending migrations via the admin REST API or the migrations dashboard:

```
POST /admin/migrations/apply
{"scope": "all", "dry_run": false}
```

---

## Tenant Migrations

Register a package of SQL scripts to be applied to every existing tenant schema:

```python
from dynastore.modules.db_config.migration_runner import register_tenant_migrations

register_tenant_migrations("catalog", "dynastore.modules.catalog.tenant_migrations")
```

Scripts use `{schema}` as a placeholder:

```sql
-- v0001__add_schema_hash.sql
ALTER TABLE {schema}.collection_configs
    ADD COLUMN IF NOT EXISTS schema_hash VARCHAR(64);
```

Applied versions are tracked in `{schema}.schema_migrations` per tenant.

---

## Rollback Convention

Place a `v{NNNN}__rollback.sql` alongside each forward script. The content is stored in `schema_migrations.rollback_sql` at apply time. Trigger rollback via:

```
POST /admin/migrations/rollback
{"module": "db_config", "version": "v0003"}
```

---

## Dry-Run

Return pending SQL without executing:

```python
from dynastore.modules.db_config.migration_runner import run_migrations

pending_sql = await run_migrations(engine, dry_run=True)
```


---

## Registered Migration Packages

| Module | Scope | Package | SQL Scripts |
|--------|-------|---------|-------------|
| `catalog` | global | `dynastore.modules.catalog.migrations` | `v0001__catalog_schema.sql` — `catalog` schema, `catalog.catalogs`, `catalog.shared_properties` |
| `catalog` | tenant | `dynastore.modules.catalog.tenant_migrations` | `v0001__config_tables.sql` — per-tenant `catalog_configs`, `collection_configs` |

Bootstrap tables (`public.schema_migrations`, `public.app_state`, `{schema}.schema_migrations`) are created by `_ensure_tracking_tables()` — inline DDL that runs before any SQL script, not tracked by the migration system itself.
