# The Database Layer

This document describes the database architecture of Agro-Informatics Platform (AIP) - Catalog Services. The database is an active, integral component leveraging native PostgreSQL features to provide massive multi-tenancy scales out-of-the-box.

## PostgreSQL as a Geospatial Engine

The choice of PostgreSQL + PostGIS provides a fast, standard-compliant geospatial engine capable of operations ranging from B-Tree string indexing to spatial R-Tree spatial joins (`GIST`).

A significant component is the usage of the `JSONB` data type, allowing for unstructured/semi-structured attributes attached to feature data that can be indexed efficiently by the `GIN` (Generalized Inverted Index) format.

### Required Extensions (fresh-database bootstrap)

Before any geometry-typed or columnar write can succeed, a small set of base PostgreSQL extensions must exist in the target database:

| Extension | Why it is needed |
|-----------|------------------|
| `postgis` | the `geometry` type and all spatial operators / GIST indexing |
| `postgis_topology` | topology support used by advanced geometry operations |
| `btree_gist` | exclusion constraints mixing scalar + range/geometry columns |
| `btree_gin` | GIN indexing of scalar columns alongside JSONB |
| `pgcrypto` | `digest()` backing the `*_hash` GENERATED columns on the geometry/attributes sidecars |
| `pg_trgm` | trigram `similarity()` used by the dimension Similarity conformance class |

This list is the single source of truth `BASE_DB_EXTENSIONS` in `modules/db_config/tools.py`.

**Self-heal on startup.** `DBService` calls `ensure_base_extensions()` on its async engine during lifespan startup. The call is guarded by a Valkey-cached presence probe keyed by database identity (`host:port/dbname`), so the steady-state cost across the multi-pod / multi-Cloud-Run fleet is a single cache read, not repeated DDL on every boot. A freshly-provisioned or repointed database (sentinel extension absent) always re-probes and re-bootstraps rather than inheriting a stale "present" marker. The call is best-effort: a failure never aborts foundational startup.

**Managed PostgreSQL requires a one-time superuser bootstrap.** On a self-hosted Postgres or a local Docker stack where the application role is a superuser, the self-heal above creates the extensions automatically — no operator action needed. On **managed PostgreSQL (AlloyDB, Cloud SQL)** the application role is intentionally *not* a superuser, and these instances gate `CREATE EXTENSION postgis` (and several others) on superuser; the privilege cannot be granted to a normal role. There the self-heal correctly fails soft (logging a warning, writes will fail with `type "geometry" does not exist`), and the extensions must be created once by a superuser when the database is first provisioned — or whenever an environment is repointed to a fresh, empty database. Run, as the instance superuser (e.g. the `postgres` / `alloydbsuperuser` role), against the target database:

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
```

This is the same privilege class as `pg_cron`: the application can tolerate its absence and degrade gracefully, but only a superuser can install it. Provisioning a new managed database (or repointing an existing environment to one) must include this bootstrap step.

### Connection Pooling

DynaStore runs two **independent** database stacks. They share configuration (via `db_config`) but never share an engine, a pool, or a driver. A given process belongs to exactly one stack:

| Tier         | Module                  | Driver       | Pool kind                | Resolves via SCOPE alias |
|--------------|-------------------------|--------------|--------------------------|--------------------------|
| API services | `db` (`DBService`)      | `asyncpg`    | async pool               | `db_async`               |
| I/O-bound workers | `db` (`DBService`) | `asyncpg` + psycopg3 (events) | async pool | `task_base_async` SCOPE alias |
| CPU-bound workers | `datastore` (`DatastoreModule`) | `psycopg2` | sync engine | `db_sync` |
| Shared layer | `db_config` (`DBConfigModule`) | n/a (config) | sets `app_state.db_config` for both | included by both aliases |

- **Async pool (services).** The FastAPI application (`main.py`) uses `asyncpg`. Multi-tenant request fan-out runs on the primary event loop with high concurrency and zero per-request thread overhead.
- **I/O-bound streaming workers.** Cloud Run Jobs whose hot path is server-side cursor streaming (export_features, dwh_join) use `task_base_async` instead of `task_base`. This installs `asyncpg` (via `module_db`) and `psycopg3` (via `module_events` for LISTEN/NOTIFY), but not `psycopg2-binary`: `db_sync`/`module_datastore` are excluded, `module_storage_postgresql` is an empty marker, and neither `pyiceberg[sql-postgres]` nor any other dep in these extras pulls psycopg2. `DatastoreModule` (psycopg2-backed) is absent from the image; discovery silently skips it via the same ImportError guard as async-only test runs. `AsyncConnection.stream()` works natively without `asyncio.to_thread` workarounds.
- **CPU-bound workers.** Cloud Run Jobs where compute dominates (ingestion/GDAL, gdal, tiles_preseed/morecantile, dimensions_materialize) use `psycopg2` only. A job is a one-shot, single-threaded process; a second async pool gives no concurrency win and doubles pool footprint per execution. **These jobs intentionally do not install `asyncpg`.**

When an async service points `DATABASE_URL` at a transaction-mode pooler, set
`DB_POOLING_MODE=transaction_pooler`. That mode keeps asyncpg startup parameters
pooler-safe and reapplies lock-safety timeouts at transaction start. PostgreSQL
`LISTEN` is different because it is session-scoped: if low-latency cross-pod
notifications are required through the shared notification hub, configure
`DB_LISTEN_DATABASE_URL` with a direct backend DSN. Without it, the hub uses its
periodic health-beat fallback instead of attaching LISTEN to a transaction
pooler session.

### Import-time isolation (load-bearing invariant)

The two stacks are isolated at *import time*, not only at runtime. The shared `db_config` module loads in **both** environments and is responsible for populating `app_state.db_config`, which the sync `DatastoreModule` reads on lifespan startup. Therefore:

> **Nothing under `packages/core/src/dynastore/modules/db_config/` may top-level-`import asyncpg`, `from asyncpg import ...`, or `sqlalchemy.ext.asyncio`.**

Any async-only symbol used inside this module tree (exception classes, type hints, connection-pool primitives) must be either:
1. guarded with `try: import asyncpg / except ImportError:` at module scope and given an empty-tuple / `Any` fallback, or
2. deferred to function-local imports that only async callers reach.

This rule extends transitively: any module imported at top-level *from* `db_config/*` inherits the constraint.

**Why this matters.** `dynastore.tools.discovery.register_plugin` wraps each entry-point load in `try/except ImportError`. On failure it emits `WARNING - Skipping dynastore.modules plugin '<name>': No module named '<dep>'` and **silently drops the entry-point**. The plugin never instantiates, its lifespan never runs, and downstream consumers crash later with an `AttributeError` masquerading as a startup failure of an unrelated module — typically surfaced in Cloud Run job logs as `RuntimeError: CRITICAL: Foundational module 'DatastoreModule' failed during startup. Aborting.`

The same skip rule is correct for genuinely async-only modules (`db`, `catalog`, `tiles`) — they should be absent from sync-image module lists. The bug shape is when a **shared** module like `db_config` is dropped because a transitive import pulled in an async-only dependency.

**Reviewer checklist.** When reviewing any PR that touches `packages/core/src/dynastore/modules/db_config/**`:
- `git diff` for `import asyncpg`, `from asyncpg`, `from sqlalchemy.ext.asyncio` — any hit at module top-level is a blocker.
- If the PR adds a new module under `db_config/`, confirm its top-level imports work in a venv without `asyncpg`.

## Partitioning and Multi-Tenancy

Agro-Informatics Platform (AIP) - Catalog Services supports true multi-tenancy at massive scale without index bloat or the "noisy neighbor" problem by leveraging PostgreSQL Declarative Table Partitioning. A single tenant executing a massive query is hardware-isolated out to their specific partition boundary.

### The "Lazy" On-Demand Trigger Strategy

Agro-Informatics Platform (AIP) - Catalog Services delegates partition orchestration to the database via triggers, not complex Python logic locking statements.

An intercepting trigger `create_partition_if_not_exists()` is bound to large unified tables (like `collections` or `styles`).
1. When an application `INSERT` is triggered.
2. The trigger executes in PG, sniffing the `catalog_id` partition key. 
3. If the partition exists, it routes the insert.
4. If it doesn't, it atomically generates a `CREATE TABLE ... PARTITION OF` statement instantly safely blocking concurrent attempts, and applies the insert. 

### Partition Types
- **LIST Partitioning**: Used for multi-tenant metadata (`collections`, `styles`, `assets`). The key is `catalog_id`.
- **RANGE Partitioning**: Used for time-series infrastructure logs (`tasks`). A worker ensures a partition exists for a specific Month. Drop a partition, and years of data vanishes cleanly.

## Core Schema Models

Our architecture defines explicit Pydantic information models in `dynastore.models.shared_models` mapping directly to the DB Schema.

### Catalog and Collections
- `catalogs`: Root entity for data isolation (`id VARCHAR PRIMARY KEY`).
- `collections`: Metadata for data groups (`id`, `catalog_id` as keys for list partitioning). 

### The `LayerConfig` pattern
The distinguishing factor between a **Logical** and **Physical** Collection is the `layer_config` JSONB column. 
- **Logical** (Metadata Grouping): `layer_config` is `NULL`. They cannot store vector data. 
- **Physical** (Database Layered): `layer_config` is active. This config handles parameters for how data gets written (e.g., `versioning_behavior`, `geometry_storage`, `indexing`). The catalog spins up actual separate PostGIS tables for these.

### Standard Vector Feature Table
A blueprint `layer_config` dictates that physical table collections are spawned in matching schemas (`"tenant_id"."layer_name"`).
Rows within physical collections store standard meta like `id`, `external_id`, `geoid` (primary UUID version marker), `valid_from`/`valid_to` (for versioning support), generic `attributes` (JSONB) and `geom` (PostGIS `GEOMETRY` objects).

## Global Task & Event Tables

The task and event infrastructure uses a **single global schema** (`tasks`) rather than per-tenant tables. This design scales to hundreds of thousands of catalogs without requiring expensive schema discovery on every dispatcher cycle.

### `tasks.tasks` — Global Task Queue

All tasks across all tenants live in one table, partitioned by `timestamp` (monthly RANGE partitions).

```
tasks.tasks
├── task_id           UUID          (PK with timestamp)
├── schema_name       VARCHAR(255)  -- tenant identifier ("s_abc123" or "system")
├── scope             VARCHAR(50)   -- CATALOG | SYSTEM | ASSET
├── execution_mode    VARCHAR       -- SYNCHRONOUS | ASYNCHRONOUS
├── task_type         VARCHAR       -- e.g. "ingestion", "gcp_provision"
├── status            VARCHAR       -- PENDING → ACTIVE → COMPLETED/FAILED/DEAD_LETTER
├── dedup_key         VARCHAR(512)  -- optional, UNIQUE partial index
├── inputs/outputs    JSONB
├── locked_until      TIMESTAMPTZ   -- visibility timeout for heartbeat
├── last_heartbeat_at TIMESTAMPTZ   -- batched heartbeat timestamp
├── owner_id          VARCHAR(255)  -- which instance claimed it
├── retry_count       INT           -- current retry count
├── max_retries       INT           -- maximum retries before dead-letter
└── timestamp         TIMESTAMPTZ   -- creation time, partition key
```

The `schema_name` column identifies the tenant; `scope` distinguishes CATALOG / SYSTEM / ASSET tasks. The composite primary key `(timestamp, task_id)` aligns with the RANGE partition key.

**Key indexes:**
- `idx_tasks_queue` on `(status, task_type, execution_mode, locked_until)` WHERE status IN ('PENDING', 'ACTIVE') — optimizes the claim query
- `idx_tasks_dedup` UNIQUE on `(dedup_key, timestamp)` WHERE dedup_key IS NOT NULL AND status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER') — per-partition dedup; cross-partition dedup enforced at application layer
- `idx_tasks_task_id` on `(task_id)` — enables complete/fail/heartbeat lookups without full partition scan

**Table options:** `fillfactor = 70` to enable HOT (Heap-Only Tuple) updates for heartbeat and status transitions.

**Trigger:** On INSERT with status='PENDING', fires `pg_notify('new_task_queued', task_type)` to wake the dispatcher.

### `tasks.events` — Global Event Outbox

A durable outbox for domain events, also monthly RANGE-partitioned by `created_at`.

```
tasks.events
├── event_id          UUID          (PK with created_at)
├── event_type        VARCHAR       -- e.g. "task.failed", "catalog.deleted"
├── scope             VARCHAR(50)   -- PLATFORM | CATALOG | COLLECTION | ASSET
├── schema_name       VARCHAR(255)
├── collection_id     VARCHAR(255)
├── payload           JSONB
├── status            VARCHAR       -- PENDING → PROCESSING → (deleted on ack)
├── dedup_key         VARCHAR(512)
├── retry_count       INT
└── created_at        TIMESTAMPTZ   -- partition key
```

Consumed events are **deleted** after successful processing. Failed events are retried up to 5 times before being moved to `DEAD_LETTER`.

**Key indexes:**
- `idx_events_queue` on `(status, created_at)` WHERE status = 'PENDING' — consumer claim
- `idx_events_dedup` UNIQUE on `(dedup_key, created_at)` WHERE dedup_key IS NOT NULL AND status NOT IN ('DEAD_LETTER')
- `idx_events_event_id` on `(event_id)` — enables ack/nack lookups without full partition scan

**Trigger:** On INSERT fires `pg_notify('dynastore_events_channel', event_type)` to wake the event consumer.

### Partition Management

Partition lifecycle is managed by startup DDL plus the leader-elected
maintenance supervisor:

1. **Startup creation** (`ensure_future_partitions`): On every application startup, partitions are created 12 months ahead (monthly) or 5-10 years ahead (yearly). This covers normal operations where services restart periodically.

2. **Scheduled creation and retention** (`tasks.maintenance_schedule`): Global task, event, and storage workclass partitions are maintained by supervisor jobs such as `task_partition_create`, `task_retention`, `events_retention`, and `storage_retention`. The schedule table records cadence, running state, and the latest outcome for each job.

3. **Stale-run recovery**: If a leader dies mid-job, the supervisor clears stale `running_since` values with a per-row threshold of `min(600s, max(180s, interval_seconds * 3))`. Short-cadence jobs recover quickly while daily jobs still recover within ten minutes.

4. **Control-plane cleanup**: The same supervisor prunes small operational tables (`configs.leader_lease`, `configs.task_capability_registry`, `configs.instance_liveness`) by age in bounded batches. These tables are retained as PostgreSQL source-of-truth state, not moved to a cache-only subsystem.

**Locking:** Partition creation uses `CREATE TABLE IF NOT EXISTS` — metadata-only DDL with no lock on the parent table or sibling partitions. Partition drops take `ACCESS EXCLUSIVE` on the child partition only; queries on other partitions are unaffected.

| Data | Default Retention | Mechanism |
|---|---|---|
| COMPLETED/FAILED tasks | 30 days | Monthly partition DROP |
| DEAD_LETTER tasks | 90 days | Separate retention window |
| Consumed events | Immediate | DELETE after processing |
| Dead letter events | 30 days | Supervisor retention |

### All Time-Partitioned Tables

| Module | Schema | Table | Column | Interval | Startup Ahead | Scheduled Maintenance | Retention |
|---|---|---|---|---|---|---|---|
| Tasks | tasks | tasks | timestamp | monthly | 12 months | supervisor create/retention jobs | 1 month |
| Tasks | tasks | events | created_at | monthly | 12 months | supervisor create/retention jobs | 1 month |
| Stats | catalog | access_logs | timestamp | monthly | 12 months | module maintenance | 3 months |
| Stats | catalog | stats_aggregates | period_start | yearly | 5 years | module maintenance | 5 years |
| Stats | tenant | access_logs | timestamp | monthly | 12 months | module maintenance | 3 months |
| Stats | tenant | stats_aggregates | period_start | yearly | 10 years | module maintenance | 10 years |
| Proxy | tenant | url_analytics | timestamp | monthly | 12 months | module maintenance | 1 month |
| Proxy | proxy | url_analytics | timestamp | monthly | 1 month | module maintenance | 6 months |

### Atomic Claiming with SKIP LOCKED

Task claiming uses `SELECT FOR UPDATE SKIP LOCKED` to ensure exactly-once delivery across all instances:

```sql
UPDATE tasks.tasks
SET status = 'ACTIVE', locked_until = :locked_until, owner_id = :owner_id
WHERE (timestamp, task_id) = (
    SELECT timestamp, task_id FROM tasks.tasks
    WHERE status = 'PENDING'
      AND (locked_until IS NULL OR locked_until <= NOW())
      AND (
          (execution_mode = 'ASYNCHRONOUS' AND task_type = ANY(:async_types))
          OR
          (execution_mode = 'SYNCHRONOUS' AND task_type = ANY(:sync_types))
      )
    ORDER BY timestamp ASC LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING *;
```

The `:async_types` and `:sync_types` parameters come from the runner capability map, so each service only claims tasks it can actually execute.

## PostgreSQL Schema Map

No application table may reside in the `public` schema. All objects are organized into dedicated schemas:

| Schema | Owner | Contents |
|--------|-------|----------|
| `platform` | `db_config` | Global tracking: `schema_migrations`, `app_state`. Shared stored procedures: `update_collection_extents()`, `asset_cleanup()`, `cleanup_orphaned_cron_jobs()`. |
| `catalog` | `catalog` | `catalogs` (the registry of all tenants), `shared_properties`. |
| `iam` | `iam` | Global auth: `principals`, `identity_links`, `policies`, `roles`, `role_hierarchy`, `refresh_tokens`, `audit_log`. |
| `tasks` | `tasks` | Global task queue: `tasks` (RANGE-partitioned), `events` (RANGE-partitioned), `maintenance_schedule`, and durable webhook `event_subscriptions`. |
| `configs` | `configs` | Platform-level configuration overrides plus small control-plane tables for leader leases, task-capability liveness, and instance liveness. |
| `consys` | connected systems | Connected Systems feature tables. An empty enabled-feature schema is not stale by itself; it may be waiting for the first resource using that feature. |
| `tiles` | `tiles` | Tile cache metadata and statistics. |
| `proxy` | `proxy` | URL proxy analytics (RANGE-partitioned). |
| `s_{base62}` | `catalog` (tenant) | Per-tenant schema auto-generated on catalog creation. Contains `collections`, `assets`, physical feature tables, `catalog_configs`, `collection_configs`, sidecars, tenant `access_logs`, `stats_aggregates`, tenant `url_analytics`, tenant `policies`, and `roles`. |

### Schema Bootstrap

Tenant tables are created idempotently on first access by
`catalog_service.create_catalog` (inline DDL for schema, `collections`,
`catalog_configs`, `collection_configs`, `collection_core`,
`collection_stac`) and the
`@lifecycle_registry.sync_catalog_initializer` hooks (module-specific
tables — assets, tiles, proxy, IAM, log_manager, …). DDL is authored with
`CREATE TABLE IF NOT EXISTS`/`ALTER TABLE ... IF NOT EXISTS` so concurrent
workers converge on the same target state without a versioned migration runner.

### Control-Plane Tables In `configs`

The `configs` schema is the platform metadata schema, not only the user-facing
configuration store. It also holds small, cross-service control-plane tables
whose rows are written by every service instance and read by background
coordination loops:

- `configs.leader_lease` — transaction-safe leader-election rows. A row records
  which instance currently owns a named background role and expires naturally
  when the owner stops renewing it.
- `configs.task_capability_registry` — observed task ownership facts keyed by
  service and task key. This complements `TaskRoutingConfig`: routing says
  where a task should run, while the registry says which deployed services are
  currently alive and able to run it.
- `configs.instance_liveness` — per-process heartbeat rows used by database
  session cleanup. This is intentionally separate from leader leases because
  most instances never hold a leader role.

These tables are expected to stay small. Stale rows are safe to prune by age or
clear during a full maintenance window; the next healthy service instances will
re-publish them. They should not be dropped in a running deployment, because
startup, routing diagnostics, leader election, and session cleanup depend on the
relations existing.
