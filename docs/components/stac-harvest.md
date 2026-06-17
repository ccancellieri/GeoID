# STAC Catalog Harvest (`stac_harvest`)

> See also: [The STAC Extension](stac.md) for the read-only STAC API surface that exposes the items this process ingests.

The `stac_harvest` OGC process walks a remote STAC catalog (collections + items
via `rel=next` cursor pagination), maps each source collection to a local
dynastore collection, and bulk-upserts the items. Upserts are idempotent —
re-running the process updates items in place. It can optionally register each
item asset `href` as a virtual asset (the href only, never the bytes).

Harvest is the recommended way to (re)populate a catalog through the **canonical
write path**, so the resulting per-catalog index is built from the current
`build_item_mapping` (the bounded `system` / `stats` canonical vocabulary is
typed and searchable). This sidesteps the "rebuild an existing index" problem:
instead of reindexing, you harvest items in with the desired storage backend.

## Process summary

| | |
|---|---|
| Process ID | `stac_harvest` |
| Scope | catalog (`ProcessScope.CATALOG`) |
| Execution endpoint | `POST /processes/catalogs/{catalog_id}/processes/stac_harvest/execution` |
| Job control | `async-execute`, `sync-execute` |
| Output transmission | `reference` |

`{catalog_id}` in the path is the **target** catalog and must already exist —
harvest creates the *collections* but not the catalog. Create the target first
with `POST /stac/catalogs` (`id` + `description` are required).

## Running the process

`stac_harvest` is a standard OGC API Processes execution — there is no admin
endpoint or special tooling. The flow is **discover → execute (async) → poll the
job**. The [worked example](#worked-example) gives the exact calls; this section
is the reference for each step.

### 1. Discover

| Action | Request |
|---|---|
| List the processes available on a catalog | `GET /processes/catalogs/{catalog_id}/processes` |
| Inspect `stac_harvest` and its input JSON Schema | `GET /processes/processes/stac_harvest` |
| List the jobs launched on a catalog | `GET /processes/catalogs/{catalog_id}/jobs` |
| Get one job's status / result | `GET /processes/catalogs/{catalog_id}/jobs/{jobID}` |

The process description is catalog-agnostic, so the inspect path is
`…/processes/processes/stac_harvest` (no `{catalog_id}`).

### 2. Permissions

Execution is **not** gated by a process-level IAM policy — the caller principal
is recorded only for job attribution (`caller_id`). On the **dev** catalog
service, which runs without IAM enforcement, execution is open and **no token is
required**. Behind the platform gateway the usual tenant/auth middleware still
applies, so send your normal bearer token (scoped to the target catalog) in
`review` / production.

### 3. Execute: async vs sync

| Mode | `Prefer` header | Behaviour |
|---|---|---|
| Async (default) | `Prefer: respond-async` | Returns a `jobID` immediately; the harvest runs in the background (see [execution mode](#execution-mode-cloud-run-job-vs-in-process-background-task) below). Poll the job for completion. Use for anything but the smallest harvests. |
| Sync | `Prefer: respond-sync` | Blocks until the harvest finishes and returns the result inline. Only for small harvests (a handful of items). |

The execution body is a JSON object with a single `inputs` member (see
[Inputs](#inputs)). Always send it with a request that sets `Content-Length`
(curl's `-d` / `--data` does this automatically); a chunked POST with no length
can be rejected by the gateway before it reaches the service.

## Inputs

| Input | Type | Default | Meaning |
|---|---|---|---|
| `catalog_url` | string | — | Base URL of the **source** STAC catalog. Must expose `/collections` and `/collections/{id}/items`. `http(s)://`, trailing slash trimmed. |
| `target_catalog` | string | — | ID of the local dynastore catalog to write into (normally equal to the `{catalog_id}` in the path). |
| `max_collections` | integer | `0` | Maximum source collections to harvest (`0` = all). |
| `max_items` | integer | `0` | Maximum items per collection (`0` = all). |
| `with_assets` | boolean | `true` | Register each item asset `href` as a virtual asset (stores only the href). |
| `storage_backend` | `es` \| `es_pg` \| `pg` | `es` | Item storage routing for the target catalog (see below). |

### `storage_backend` — the routing preset

On the first collection it writes, harvest pins the target catalog's item
routing according to `storage_backend` (an idempotent preset application). This
is the single knob that decides where harvested items live and how soon they are
searchable:

| Backend | Item WRITE | Item READ | Searchability | Use when |
|---|---|---|---|---|
| `es` (default) | Elasticsearch (direct) | Elasticsearch (direct) | **Immediate** — no async drain | You want items searchable as soon as the harvest finishes; ES is the system of record for this catalog. |
| `es_pg` | Postgres (primary) | Postgres | After the async ES secondary-index write drains | Default platform routing — durable PG primary with an ES secondary search index. |
| `pg` | Postgres only | Postgres | N/A (no ES) | No search index needed; PG-only catalog. |

Because `es` writes (and reads) items directly to Elasticsearch, the per-catalog
ES index is created from the current `build_item_mapping`, so the canonical
`system.*` / `stats.*` fields and the root `external_id` / `asset_id` keyword
lanes are typed and queryable immediately.

## Execution mode: Cloud Run Job vs in-process background task

How an async harvest *runs* is governed by the **task-routing deployment
preset** (`cloud` / `review` / `onprem`) plus the process's "lightweight"
classification — not by anything in the request body.

- **`async-execute`** (`Prefer: respond-async`) — dispatched to a runner per the
  routing matrix:
  - **`cloud` / `review` preset:** a *non-lightweight* process offloads to a
    **Cloud Run Job** (`gcp_cloud_run` runner). Only the two lightweight
    processes (`requeue_dead_letter_tasks`, `tiles_invalidate`) stay in-process.
  - **`onprem` preset:** *every* process runs in-process (`background` runner) —
    never a Cloud Run Job.
- **`sync-execute`** (`Prefer: respond-sync`) — runs in-process **synchronously**
  on the catalog service and blocks until done. Suitable only for small harvests.

`stac_harvest` is **not** lightweight, so under `cloud` / `review` the routing
matrix *wants* it on `gcp_cloud_run`. There is one more requirement.

### Running harvest as a Cloud Run Job

The `gcp_cloud_run` runner maps a `task_type` to a job via `load_job_config()`,
which **discovers deployed Cloud Run Jobs by their `TASK_TYPE` env** (TTL 900s).
A Cloud Run Job with `TASK_TYPE: stac_harvest` must therefore be deployed for
harvest to run as a job. If no such job exists, the job map has no `stac_harvest`
entry and dispatch falls through to the lower-priority in-process `background`
runner — i.e. the harvest still runs, but inside the catalog **service**, not as
a Job.

All task Jobs share a single container image; `GcpJobRunner` overrides the
container `--args` per execution with `[<task_type>, <TaskPayload JSON>, --schema,
<schema>]`. So enabling job-mode is purely a deploy-manifest addition — no new
code. In the deployment repository's `apps.base.yml`, add a Job block mirroring
an existing one (e.g. the elasticsearch indexer), changing only the identity and:

```yaml
  geospatial-stac-harvest-job:
    # ... same image / SA / DB pool / VPC as the other *-job blocks ...
    TASK_TYPE: "stac_harvest"
```

Add the matching `svc_geospatial_stac_harvest_job` input + deploy-matrix entry to
`deploy.yml`, then deploy to dev:

```bash
gh workflow run deploy.yml -f environment=dev -f svc_geospatial_stac_harvest_job=true
```

Once the Job is live, `load_job_config` auto-discovers it and subsequent async
harvests route to the Job (verify with `GET /configs/tasks/runners` — the entry
should appear under `gcp_cloud_run.declared_tasks`).

### Running harvest as a background task

This is the default today (no `stac_harvest` Job deployed): an async harvest runs
in-process on the catalog service. It is the *only* mode under the `onprem`
preset. It is appropriate for small/medium harvests; for large catalogs, deploy
the Job so a long harvest does not occupy a request-serving pod.

## Worked example

```bash
BASE='https://<host>/geospatial/<env>/api/catalog'
TC='my_target_catalog'

# 0. (optional) Inspect the process and its input schema
curl -s "$BASE/processes/processes/stac_harvest" | jq '.inputs'

# 1. Create the target catalog (must exist before harvest)
curl -s -X POST "$BASE/stac/catalogs" -H 'Content-Type: application/json' \
  -d "{\"id\":\"$TC\",\"description\":\"Harvest target\"}"

# 2. Execute the harvest asynchronously (ES backend, register assets, small limits)
curl -s -X POST \
  "$BASE/processes/catalogs/$TC/processes/stac_harvest/execution" \
  -H 'Content-Type: application/json' -H 'Prefer: respond-async' \
  -d "{\"inputs\":{
        \"catalog_url\":\"$BASE/stac/catalogs/<source_catalog>\",
        \"target_catalog\":\"$TC\",
        \"storage_backend\":\"es\",
        \"with_assets\":true,
        \"max_collections\":1,
        \"max_items\":5
      }}"
# -> 200 { "jobID": "...", "status": "accepted" }

# 3. Poll the job until it finishes
curl -s "$BASE/processes/catalogs/$TC/jobs/<jobID>"
# -> { "status": "successful",
#      "message": "collections=1/1 items_written=5 items_failed=0 virtual_assets=5 backend=es" }
```

The job `message` reports `collections=<written>/<seen> items_written=N
items_failed=N virtual_assets=N backend=<es|es_pg|pg>`.

## Notes & gotchas

- **Target catalog must pre-exist.** Harvest upserts collections and items into
  it, but does not create the catalog.
- **`with_assets` stores only the href** (a virtual asset). Bytes are never
  copied.
- **Idempotent.** Re-running over the same source updates items in place — safe
  to retry.
- **Metadata-only sources harvest zero items.** A source catalog that exposes
  collections but no items (`numberMatched: 0`) yields `items_written=0`; point
  `catalog_url` at a source whose collections actually contain items.
- **Source can be another dynastore STAC API.** `catalog_url` =
  `…/stac/catalogs/<source_catalog>` lets you re-harvest one catalog into another
  with a different `storage_backend`.

## Related

- [Processes Module](processes.md) — OGC API Processes, runner system, sync/async.
- [Tasks & Events](tasks.md) — the task ledger, dispatch, and Cloud Run Job runner.
- [Elasticsearch](elasticsearch.md) — item index mapping, canonical `system` /
  `stats` vocabulary, reindex tasks.
