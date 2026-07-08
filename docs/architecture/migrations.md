# Database Schema Establishment

This document describes how the database schema comes to exist in DynaStore.
The versioned-SQL migration framework (`migration_runner.py`, `schema_migrations`
table, `v{NNNN}__*.sql` scripts, advisory lock, admin migrate/rollback endpoints,
and migrations dashboard) was **removed entirely** in commit `854c559b`
("feat(iam)!: remove migration framework; kill hardcoded role strings", 2026-04-17).
Nothing in this document describes that removed system.

---

## Hard Invariant

**The application never issues in-place DDL** — no `ALTER TABLE … ADD/DROP/RENAME
COLUMN`, no type changes, no backfill loops — against an already-materialized
table. Schema is established only at (re)provision time via idempotent
`CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS` statements.
There is no versioned migration runner, no `schema_migrations` tracking table,
no admin `migrate` or `rollback` REST endpoints, and no migrations dashboard.
See [Schema Evolution & Drift](../components/schema_evolution.md) for the
implications on per-collection schema drift.

---

## Global Schema (Module Startup)

Global shared-infrastructure tables are created **once per startup** by
idempotent DDL called from the respective module lifespans. No advisory lock
or version tracking is needed because every statement uses `IF NOT EXISTS`.

### Platform config tables (`configs` schema)

`PlatformConfigService.initialize_storage` (called from `DBConfigModule.lifespan`
via `tools.py:ensure_init_db`) issues:

```python
# packages/core/src/dynastore/modules/db_config/typed_store/ddl.py
PLATFORM_SCHEMAS_DDL   # CREATE SCHEMA IF NOT EXISTS configs;
                       # CREATE TABLE IF NOT EXISTS configs.platform_configs (…)
                       # CREATE INDEX IF NOT EXISTS …
```

Source: `packages/core/src/dynastore/modules/db_config/typed_store/ddl.py`
(constants `PLATFORM_SCHEMAS_DDL`, `tenant_configs_ddl`).
JSON schemas are not stored in a `configs.schemas` registry table; each config
row stores its `schema_id` version tag, and the current schema is generated from
the registered `PluginConfig` class.

### Catalog registry and metadata tables (`catalog` schema)

`CatalogModule.lifespan` creates:

```python
# packages/core/src/dynastore/modules/catalog/catalog_module.py  ~line 375
await ensure_schema_exists(conn, "catalog")
await DDLQuery(CATALOGS_TABLE_DDL + SHARED_PROPERTIES_SCHEMA).execute(conn)
await ensure_global_core_tables(conn)   # catalog.catalog_core, catalog.catalog_stac
await ensure_stored_procedures(conn)
```

`CATALOGS_TABLE_DDL` defines `catalog.catalogs` (the platform-wide catalog
registry). `SHARED_PROPERTIES_SCHEMA` defines `catalog.shared_properties`.
Source: `packages/core/src/dynastore/modules/catalog/catalog_module.py`
(constants `CATALOGS_TABLE_DDL`, `SHARED_PROPERTIES_SCHEMA`).

---

## Tenant Schema (Provisioning Time)

Per-tenant tables are created **once**, when a catalog is provisioned (via
`CatalogService.create_catalog`). The provisioner calls `_build_tenant_core_ddl_batch`
which returns a `DDLBatch`:

```python
# packages/core/src/dynastore/modules/catalog/catalog_service.py  line 95
def _build_tenant_core_ddl_batch(schema: str) -> "DDLBatch":
    ...
    return DDLBatch(
        sentinel=DDLQuery(tenant_configs_sql, check_query=_check_sentinel),
        steps=[
            DDLQuery(TENANT_COLLECTIONS_DDL),   # {schema}.collections
            CREATE_ROLES_TABLE,                 # {schema}.roles
            CREATE_ROLE_HIERARCHY_TABLE,        # {schema}.role_hierarchy
            CREATE_GRANTS_TABLE,                # {schema}.grants
            DDLQuery(tenant_configs_sql, ...),  # {schema}.catalog_configs
                                                # {schema}.collection_configs
        ],
    )
```

`DDLBatch` uses a sentinel check (`collection_configs` existence) as a warm-path
fast-skip: if the last table already exists the entire batch is skipped in one
round-trip. Source:
`packages/core/src/dynastore/modules/catalog/catalog_service.py` (~line 95–136).

After the core batch, additional per-tenant tables are created by
`@lifecycle_registry.sync_catalog_initializer` hooks registered by
other modules (assets, tiles, IAM, proxy, indexing outbox, …).

Per-collection hub and sidecar tables are created by
`ItemsPostgresqlDriver.ensure_storage` at collection provisioning time, again via
`CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS`. The application
**never** `ALTER`s an existing collection table; adding a column for real requires
a fresh (re)provision. Source:
`packages/core/src/dynastore/modules/storage/drivers/postgresql.py` (~line 741).

---

## Config Payload Migration DAG

There is **no DDL migration runner**, but there is a lightweight in-memory
mechanism for evolving stored JSON config payloads: the
`dynastore.tools.typed_store.migrations` module. This is entirely separate
from database schema and does not write to any tracking table.

When a `PluginConfig` row is read from the database and its stored `schema_id`
(a SHA-256 content hash of the Pydantic model's JSON schema) differs from the
current class's `schema_id`, a BFS traversal across registered `@migrates`
functions transforms the stored dict to the current shape — in-process, with
no DDL, no DB write. Source:
`packages/core/src/dynastore/tools/typed_store/migrations.py`.

An operator-facing CLI (`python -m dynastore.modules.db_config.typed_store.cli audit`)
reports any stored `schema_id` values that have no registered migrator path to
the current class schema.

---

## Summary Table

| Scope | When established | Mechanism | Key source |
|-------|-----------------|-----------|------------|
| `configs` schema + platform-config tables | Module startup (idempotent) | `PLATFORM_SCHEMAS_DDL` via `PlatformConfigService.initialize_storage` | `modules/db_config/typed_store/ddl.py` |
| `catalog` schema + catalog registry | Module startup (idempotent) | `CATALOGS_TABLE_DDL` + `SHARED_PROPERTIES_SCHEMA` in `CatalogModule.lifespan` | `modules/catalog/catalog_module.py` |
| Per-tenant core tables | Catalog provisioning | `_build_tenant_core_ddl_batch` DDLBatch | `modules/catalog/catalog_service.py` |
| Per-collection hub/sidecar tables | Collection provisioning | `ItemsPostgresqlDriver.ensure_storage` | `modules/storage/drivers/postgresql.py` |
| Config payload shape evolution | Read time (in-process, no DDL) | `@migrates` DAG in `typed_store.migrations` | `tools/typed_store/migrations.py` |

There is no versioned SQL runner, no `schema_migrations` table, no per-module
`migrations/` SQL packages, and no admin REST or dashboard for triggering migrations.

---

## See Also

- [Schema Evolution & Drift](../components/schema_evolution.md) — per-collection
  schema drift, read-only reconciliation, and the no-in-place-DDL rule.
- [Items Schema](../components/items_schema.md) — declarative field surface and
  freeze semantics.
- `packages/core/src/dynastore/modules/db_config/readme.md` — `db_config` module
  overview, `TypedStore` config persistence, and the config payload migration DAG.
