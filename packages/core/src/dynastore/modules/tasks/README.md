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
| `execution.py` | `ExecutionEngine`; routes tasks to registered runners, resolves terminal Actions (`on_success`/`on_failure`/`on_timeout`) |
| `runners.py` | `BaseTaskRunner` base + `BackgroundRunner` (in-process async); `CapabilityMap` claim gate |
| `routing/` | `TaskRoutingConfig`, `RunnerTarget`, `ExecHint`; the cloud/onprem matrix + presets; `resolved_targets` / `select_target` |
| `registry/` | Durable task-capability registry (what each deployed service can run) + `reconcile_routing_capabilities` starvation check |
| `models.py` | `Task`, `TaskCreate`, `TaskUpdate` Pydantic models |
| `tasks_config.py` | `TasksPluginConfig` (poll interval, dispatcher batch size, task timeout, ...) |

## Task Routing

Which service claims a task, and how it runs, is governed by `TaskRoutingConfig`
(a platform-tier `PluginConfig`, `class_key = task_routing_config`, address
`("platform", "tasks")`) — not by env vars or hard-coded rules. It holds two
ordered candidate maps:

```
tasks:     { task_key: [RunnerTarget, ...] }   # system tasks (cascade_cleanup, requeue_dead_letter_tasks, ...)
processes: { task_key: [RunnerTarget, ...] }   # OGC / user processes (gdal, dwh_join, tiles_export, ...)
```

`resolved_targets(task_key)` consults the `tasks` bucket first, then `processes`,
returning the ordered candidate list. When **both** maps are empty the
`_materialize_if_empty` validator builds the full cloud matrix from the live task
registry, so an empty seed still yields a complete, consumer-bearing config
(visible at `GET /configs/?resolved=true`).

The dispatcher claim gate uses the same routing selection model as execution:
`CapabilityMap.refresh()` evaluates the selected `RunnerTarget` for each task
and advertises the task only when this process has that target's `runner` type.
This matters for offloaded work such as `catalog_provision`: under the cloud
profile it is claimable only by a service that has a matching `gcp_cloud_run`
runner. Under the on-prem profile the target runner is `background`, so the
same task remains claimable by the in-process worker tier.

### RunnerTarget

| Field | Meaning |
|---|---|
| `consumers` | Service tiers allowed to claim it (`["catalog"]`, `["maps"]`, `["worker"]`, ...). Empty ⇒ any capable service. |
| `runner` | `background` (in-process async) or `gcp_cloud_run` (delegated Cloud Run Job). |
| `options` | Per-target knobs, e.g. `{"timeout_seconds": 3600}`. |
| `hints` | `ExecHint` set: `INTERACTIVE`, `SYNC`, `BACKGROUND`, `OFFLOAD`, `HEAVY`. |
| `on_success` / `on_failure` / `on_timeout` | Terminal `Action` per outcome. |

An `Action` is `{action: ActionVerb, process?, hints, payload}` where
`ActionVerb ∈ {ROUTE, REPORT, DEAD_LETTER, FAIL}` (`ROUTE` requires a `process` —
a follow-on task to enqueue). Defaults: `on_success = REPORT`,
`on_failure = DEAD_LETTER`, `on_timeout = DEAD_LETTER`. `on_timeout` is distinct
from `on_failure`, so a timed-out offload can be re-routed without being treated
as a logic error.

### Deployment profiles (presets)

`build_routing_matrix` materializes three profiles; every target it emits carries
a non-empty `consumers` list — the structural guard against a task with no
claimer:

- **cloud** — processes offload to GCP Cloud Run Jobs (`gcp_cloud_run`, hints
  `{OFFLOAD, HEAVY}`) unless lightweight; system tasks and lightweight processes
  stay in-process (`background`).
- **onprem** — every task/process runs as a `background` worker on the `worker`
  tier; heavy processes get the `HEAVY` hint for a dedicated worker pool.
- **review** — mirrors cloud, except `gdal` runs in-process on the catalog pod
  (`background`, `["catalog"]`, hints `{BACKGROUND, INTERACTIVE}`) for review/local
  images; the production catalog image is unchanged.

Apply a profile from the admin presets UI (platform scope) or the `/admin/presets`
API — **dry-run → apply → rollback**. The presets register as
`CloudTaskRoutingPreset` / `OnpremTaskRoutingPreset` at `PresetTier.PLATFORM`.
(`DYNASTORE_TASK_ROUTING_PRESET=review` is a retired alias, honoured as `cloud`.)

### Discovery & diagnostics

| Endpoint | Returns |
|---|---|
| `GET /configs/tasks/catalogue` | The task/process registry (kind-split). |
| `GET /configs/tasks/runners` | Runners registered on this service. |
| `GET /configs/tasks/capabilities` | Reconcile report; a non-empty `starving` list is a routed-but-unclaimable gap. |

### Runbook: a task never executes

If an enqueued task sits unclaimed it is **starving** — routed to a runner or
service that is not registered/deployed here. Diagnose:

1. `GET /configs/tasks/capabilities` — a `starving` entry names the task and the
   runner(s) it was routed to; the same condition is logged at WARNING
   (`... will STARVE ...`) by the startup reconcile.
2. Pick the fix:
   - **Routing typo / wrong tier** → edit the target's `consumers` / `runner` in
     `TaskRoutingConfig` (configs API) or re-apply a preset.
   - **Missing deployment** → deploy the runner the process is routed to (e.g. the
     `gcp_cloud_run` Cloud Run Job).

This is the failure mode behind the historical silent-starvation bug: it is now
loud (a WARN plus the `starving` report) instead of an indefinitely PENDING row.

## Tenant Isolation

- All CRUD operations (`get_task`, `update_task`) filter on **both** `task_id` and
  `schema_name`. A task_id alone is not tenant-safe.
- The dedup unique index `idx_tasks_dedup` is keyed on `(schema_name, dedup_key, timestamp)`.
  Two tenants sharing the same `dedup_key` do not collide.
- The cross-partition dedup guard in `enqueue()` is also scoped by `schema_name`.

## Claim Leases

`tasks.tasks.locked_until` is the queue visibility timeout. Claiming a PENDING
row sets `status = 'ACTIVE'`, `owner_id`, and `locked_until`; runners heartbeat
or extend that lease while work continues. The task reaper moves expired ACTIVE
rows back to PENDING, or to DEAD_LETTER when retry limits are exhausted. It is a
lease column, not a permanent ownership flag.

## Initialization

On startup `TasksModule` (priority=15, before `CatalogModule` at 20):

1. Acquires an advisory lock for the duration of all DDL — prevents concurrent-revision
   races on rolling deploys. If that outer startup lock times out, the module
   replays idempotent startup DDL in a scoped fallback that skips only the nested
   per-query advisory wait; ordinary DDL errors still fail startup.
2. Creates the `tasks` table, indexes, pg_notify triggers, and the DEFAULT
   partition if absent. Warm starts also repair `tasks.tasks_default` with a
   separate `to_regclass` check so the sentinel-skipped DDL batch cannot leave
   retention pointing at a missing default partition.
3. Ensures 12 monthly future partitions exist.
4. Asserts the current-month partition is present before starting the dispatcher.

## Maintenance Schedule

Periodic task maintenance is driven by `tasks.maintenance_schedule`; it is the
single scheduler table for the in-process maintenance supervisor. Each row has a
`job_name`, cadence (`interval_seconds`), the latest run outcome, and
`running_since` while a leader is executing it. The supervisor reclaims stale
`running_since` values with a row-derived threshold:
`min(600s, max(180s, interval_seconds * 3))`. Short-cadence jobs such as
`task_reaper` recover after about three minutes; daily jobs still recover within
ten minutes.

Optional jobs are dependency-aware. `iam_prune` is registered only when the IAM
tables exist, and `es_logs_retention` is registered only when the OpenSearch
client dependency is available. If an old schedule row remains, the job records
`last_status = 'skipped'` instead of creating recurring errors.

The supervisor also owns `control_plane_retention`, a daily bounded cleanup job
for small control-plane tables:

- `configs.leader_lease` rows whose leases are comfortably expired.
- `configs.task_capability_registry` rows older than the configured capability
  publisher TTL multiplied by ten.
- `configs.instance_liveness` rows older than twice the zombie-session liveness
  window, with a one-hour minimum.

These rows are operational state. They should remain in PostgreSQL because they
coordinate durable task routing, leadership, and session safety, but they are
prunable by age and should stay roughly bounded by deployed services/instances.

## Task Attribution

Every task carries a `caller_id`:
- **User tasks** — the authenticated user's identifier.
- **System tasks** — `system:platform` (set by `ApiKeyModule`).

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DYNASTORE_TASK_SCHEMA` | `tasks` | Schema that holds the global tasks table |
| `DYNASTORE_QUEUE_POLL_INTERVAL` | `30.0` | Fallback poll interval in seconds |
