# Schema Evolution Engine

The `SchemaEvolutionEngine` provides safe, introspection-based evolution of per-collection physical tables. It lives in `src/dynastore/modules/catalog/schema_evolution.py` and is used by the admin API to detect drift between a collection's current database schema and its target `CollectionPluginConfig`.

## Problem

Collection physical tables are created from a `CollectionPluginConfig` at provisioning time. When an operator updates a collection's configuration (e.g., adds a new attribute column, changes an H3 resolution, or switches geometry storage), the existing table must be evolved to match the new spec. Doing this incorrectly can corrupt or lose data.

The engine separates changes into two categories:

- **Safe operations** — can be applied directly as `ALTER TABLE` statements with no risk of data loss.
- **Unsafe operations** — require an export → rename → recreate → import cycle before the old table is dropped.

---

## Introspection

```python
engine = SchemaEvolutionEngine()
items_schema = await engine.introspect_collection(db_engine, schema, physical_table)
```

`introspect_collection()` queries:

- `information_schema.columns` — column names and UDT types for every table in the collection (hub + sidecars).
- `pg_catalog.pg_indexes` → `pg_get_indexdef()` — full index definition strings.
- `pg_catalog.pg_constraint` → `pg_get_constraintdef()` — full constraint definitions.

The result is a `ItemsSchema` containing a `TableSchema` per table, each with typed `ColumnInfo` objects.

---

## Diff

```python
plan: EvolutionPlan = engine.diff(
    current=items_schema,
    target_config=new_plugin_config,
    physical_table=hub_table,
    partition_keys=["catalog_id", "collection_id"],
    partition_key_types={"catalog_id": "VARCHAR", "collection_id": "VARCHAR"},
)
```

`diff()` compares the introspected schema against the target column set derived from the new `CollectionPluginConfig`. It produces an `EvolutionPlan` with two lists:

### Safe Operations (`plan.safe_ops`)

| `OpType` | DDL produced |
|----------|-------------|
| `ADD_COLUMN` | `ALTER TABLE … ADD COLUMN IF NOT EXISTS …` |
| `CREATE_INDEX` | `CREATE INDEX IF NOT EXISTS …` |
| `ADD_CONSTRAINT` | `ALTER TABLE … ADD CONSTRAINT IF NOT EXISTS …` |

### Unsafe Operations (`plan.unsafe_ops`)

| `OpType` | Reason |
|----------|--------|
| `DROP_COLUMN` | Data loss risk |
| `ALTER_COLUMN_TYPE` | Type coercion may fail or silently truncate |
| `DROP_INDEX` | Indicates a config regression — review needed |
| `CHANGE_PARTITION_KEY` | Requires full table rebuild |
| `CHANGE_PARTITION_KEY_TYPE` | Requires full table rebuild |

`plan.is_safe` is `True` when `unsafe_ops` is empty. `plan.requires_export_import` is `True` when any unsafe op is present.

---

## Type Compatibility

The engine is intentionally conservative. `_type_compatible()` maps PostgreSQL UDT names to canonical SQL type families:

```
int4, int2, int8  → INTEGER
float4, float8    → FLOAT
varchar, text     → VARCHAR / TEXT (considered compatible)
bool              → BOOLEAN
jsonb             → JSONB
geometry          → GEOMETRY (exact spec compared)
```

Any mapping that cannot be confirmed compatible is classified as `ALTER_COLUMN_TYPE` (unsafe) rather than silently skipping.

---

## Applying Safe Operations

```python
applied: List[str] = await engine.apply_safe_ops(db_engine, schema, plan)
```

Executes each safe DDL statement inside a single transaction. Returns the list of SQL strings that were executed. If any statement fails the transaction rolls back automatically.

Unsafe operations are never executed by `apply_safe_ops` — they require the full export-import pipeline managed by the schema migration task.

---

## Schema Hash (Drift Detection)

After a collection is physically created, `collection_service.py` computes:

```python
schema_hash = sha256(json.dumps(config.model_dump(), sort_keys=True)).hexdigest()
```

This hash is stored in `{schema}.collection_configs` under the `schema_hash` column. The schema health endpoint (`GET /admin/schemas/{catalog_id}/health`) compares stored hashes against the current configs to surface collections that have drifted from their last-provisioned state.

---

## Admin API

| Endpoint | Description |
|----------|-------------|
| `GET /admin/schemas/{catalog_id}/health` | Schema health for all collections: drift status, safe/unsafe ops |
| `POST /admin/schemas/{catalog_id}/{collection_id}/evolve` | Preview or apply safe evolution ops |
| `POST` | `/admin/schemas/{catalog_id}/{collection_id}/migrate` | Trigger safe export-import migration for unsafe changes |
| `GET` | `/admin/schemas/{catalog_id}/{collection_id}/backups` | List backup tables left by previous migrations |
| `DELETE` | `/admin/schemas/{catalog_id}/{collection_id}/backups/{timestamp}` | Drop backup tables (explicit cleanup) |

Set `apply_safe=true` in the evolve request body to execute safe operations immediately. Unsafe operations are always returned in the response for operator review.

---

## Safe Data Migration Endpoint

When `plan.requires_export_import` is `True`, use the schema migration endpoint to trigger the full export-import pipeline:

```
POST /admin/schemas/{catalog_id}/{collection_id}/migrate
Content-Type: application/json

{"dry_run": false}
```

This enqueues a `SchemaMigrationTask` which:
1. Exports all data to Parquet (WKB hex for geometry)
2. Renames originals to `{table}_bkp_{timestamp}`
3. Recreates tables with the new schema
4. Imports data with column mapping
5. Verifies row counts

See [schema_migration task README](../../tasks/schema_migration/README.md) for full pipeline details.

### Backup Management

```
GET    /admin/schemas/{catalog_id}/{collection_id}/backups              — list backup tables
DELETE /admin/schemas/{catalog_id}/{collection_id}/backups/{timestamp}  — explicit cleanup
```

---

## Files

| Path | Purpose |
|------|---------|
| `src/dynastore/modules/catalog/schema_evolution.py` | `SchemaEvolutionEngine`, `EvolutionPlan`, `SchemaOp`, `ItemsSchema` |
| `src/dynastore/modules/catalog/collection_service.py` | Schema hash storage after physical table creation |
| `src/dynastore/extensions/admin/migration_routes.py` | Schema health + evolve API endpoints |
| `tests/dynastore/modules/catalog/unit/test_schema_evolution.py` | Unit tests: diff, type compat, safe/unsafe classification |
