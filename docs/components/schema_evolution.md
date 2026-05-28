# Schema Evolution & Drift

How a collection's physical storage changes — or refuses to change —
after the collection has been provisioned. The short version: **the
application never issues in-place DDL** (`ALTER TABLE … ADD/DROP/RENAME
COLUMN`, type changes, backfills) against a materialized collection
table. Drift between the declared schema and the physical table is
surfaced and **reconciled read-only**; actually adding or changing a
physical column happens only through a fresh (re)provision or an
out-of-band migration.

> An earlier version of this page described a `SchemaEvolutionEngine`
> with `introspect_collection` / `diff` / `apply_safe_ops`, an
> `EvolutionPlan`, and `/admin/schemas/.../evolve|migrate` endpoints.
> **None of that exists in the codebase** and the "apply safe `ALTER
> TABLE` directly" model it promoted contradicts the project's hard
> no-in-place-DDL invariant. This page now documents the actual
> mechanism (rewrite tracked by #1521).

## The model

1. **Declaration.** A collection's field structure is declared in its
   [`items_schema`](items_schema.md) config (frozen at the collection
   tier once the items table has rows).
2. **Materialization.** Physical columns are created **once, at
   provisioning**, via `CREATE TABLE IF NOT EXISTS` (and `CREATE INDEX
   IF NOT EXISTS`). These are the only DDL the app emits, and they are
   idempotent no-ops on an already-materialized table.
3. **Drift.** Because step 2 is a no-op on an existing table, a config
   change that adds a column (e.g. promoting a field to a COLUMNAR
   attributes-sidecar column) does **not** reach a table that already
   exists. The persisted config can then advertise a column the physical
   table lacks.
4. **Read-only reconciliation.** Rather than `ALTER` the table to match
   the config, dynastore reconciles the **config down to physical
   reality**: on `ensure_storage`, a COLUMNAR attribute schema is
   introspected against `information_schema.columns` and any entry with
   no backing column is dropped from the resolved config (with a WARN).
   This keeps the read path from emitting `SELECT "missing_col"` and
   500-ing (`UndefinedColumnError`), and it never mutates the table
   (#1489). The dropped field degrades to the read-side silent-skip until
   a column is genuinely added.
5. **Adding a column for real.** Requires a fresh (re)provision of the
   collection — there is no app-issued `ALTER` path. Plan the schema
   before ingesting data.

This is enforced consistently with the `items_schema` validate-time
guards: the storage-realizability check simulates the write-side bridge
at config-save (config-only, no DDL) so an unrealizable schema is
rejected up front rather than discovered as empty tiles weeks later.

## Where physical DDL *does* happen

- **Provisioning** — `CREATE TABLE / CREATE INDEX IF NOT EXISTS` for a
  collection's hub + sidecar tables. By design, idempotent, additive.
- **Shared-infrastructure migrations** — DDL for *shared* tables
  (config tables, tracking tables) goes through the versioned migration
  system (`{schema}.schema_migrations`, `v{NNNN}__*.sql` scripts, applied
  out-of-band, not auto-applied at startup). This is a separate concern
  from per-collection schema and is documented in
  `src/dynastore/modules/db_config/readme.md`.
- **Engine-native evolution** — the `Capability.SCHEMA_EVOLUTION` flag
  (`models/protocols/storage_driver.py`) is advertised only by drivers
  whose *engine* supports evolution natively (e.g. the Iceberg driver).
  That is the storage engine evolving its own schema, not the
  application issuing PostgreSQL DDL.

## See also

- [Items Schema](items_schema.md) — the declarative field surface and
  its freeze/validate semantics.
- [Sidecar Configurations](sidecar_configs.md) — COLUMNAR vs JSONB
  attribute storage and the write-side bridge that promotes fields to
  columns.
- `src/dynastore/modules/db_config/readme.md` — the real versioned
  migration system for shared-infrastructure tables.
- `src/dynastore/modules/storage/drivers/postgresql.py` —
  `ensure_storage` + the read-only `information_schema` reconciliation.
