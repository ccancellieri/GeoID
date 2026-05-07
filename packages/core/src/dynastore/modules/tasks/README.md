# Tasks Module

Background job queue for DynaStore. Tasks are stored in one global PG table
(`{DYNASTORE_TASK_SCHEMA}.tasks`, default `tasks.tasks`) RANGE-partitioned by
`timestamp` (monthly). Multi-tenancy is modelled via columns, not per-tenant tables.

## Architecture

```
tasks.tasks  PARTITION BY RANGE (timestamp)
  ├─ schema_name   VARCHAR   tenant discriminator (PG schema slug)
  ├─ task_id       UUID      row identifier
  ├─ scope         VARCHAR   CATALOG | SYSTEM | ASSET
  ├─ caller_id     VARCHAR   originator (user id or "system:platform")
  ├─ task_type     VARCHAR   registered runner key
  ├─ dedup_key     VARCHAR   optional; per-tenant uniqueness in non-terminal rows
  └─ ...
```

A single global dispatcher claims tasks across all tenants via `FOR UPDATE SKIP LOCKED`.
Runners receive the `schema_name` so they can operate in the correct tenant context.

## Key Components

| File | Purpose |
|---|---|
| `tasks_module.py` | `TasksModule` (priority=15), DDL, CRUD (`enqueue`, `get_task`, `update_task`, `list_tasks`, `claim_batch`) |
| `dispatcher.py` | Global dispatcher; claim loop → runner dispatch |
| `queue.py` | `TaskQueue`; pg_notify LISTEN with polling fallback |
| `execution.py` | `ExecutionEngine`; routes tasks to registered runners by type |
| `runners.py` | `BaseTaskRunner` base class |
| `models.py` | `Task`, `TaskCreate`, `TaskUpdate` Pydantic models |
| `tasks_config.py` | `TasksPluginConfig` (`queue_poll_interval`) |

## Tenant Isolation

- All CRUD operations (`get_task`, `update_task`) filter on **both** `task_id` and
  `schema_name`. A task_id alone is not tenant-safe.
- The dedup unique index `idx_tasks_dedup` is keyed on `(schema_name, dedup_key, timestamp)`.
  Two tenants sharing the same `dedup_key` do not collide.
- The cross-partition dedup guard in `enqueue()` is also scoped by `schema_name`.

## Initialization

On startup `TasksModule` (priority=15, before `CatalogModule` at 20):

1. Acquires an advisory lock for the duration of all DDL — prevents concurrent-revision
   races on Cloud Run rolling deploys.
2. Creates the `tasks` table, indexes, and pg_notify triggers if absent.
3. Ensures 12 monthly future partitions exist and registers retention cron jobs.
4. Asserts the current-month partition is present before starting the dispatcher.

## Task Attribution

Every task carries a `caller_id`:
- **User tasks** — the authenticated user's identifier.
- **System tasks** — `system:platform` (set by `ApiKeyModule`).

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DYNASTORE_TASK_SCHEMA` | `tasks` | Schema that holds the global tasks table |
| `DYNASTORE_QUEUE_POLL_INTERVAL` | `30.0` | Fallback poll interval in seconds |
