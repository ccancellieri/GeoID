# Catalog lifecycle — readiness, stress testing & recovery

This runbook answers three operational questions for catalog/collection lifecycle:

1. **How do I know — with certainty — that a freshly created catalog is ready to use?**
2. **How healthy is the create → use → destroy loop under load (failure rate)?**
3. **How do I recover when a catalog create or delete leaves something broken behind?**

It pairs with two tools in this repo:

- `scripts/catalog_lifecycle_stress.py` — the lifecycle stress harness (section 2).
- `scripts/cleanup_orphan_items_tables.sql` — orphan items-table cleanup (section 3.3).

---

## 1. Being 100% sure a catalog is ready

Catalog creation is asynchronous. `POST /stac/catalogs` returns `201` immediately, but the
GCS bucket and the managed Pub/Sub eventing channel are provisioned by a background task.
Using the catalog before that finishes produces confusing `409 "still provisioning"` errors
on collection/asset writes.

### The authoritative readiness check

Poll `GET /catalog/catalogs/{catalog_id}` and treat the catalog as ready **only when both**
of these hold:

- `provisioning_status == "ready"`, **and**
- the embedded `task` (the provision task) has `status == "COMPLETED"`.

```bash
BASE=https://data.review.fao.org/geospatial/dev/api/catalog
curl -s "$BASE/catalog/catalogs/$CAT" | jq '{provisioning_status, task: .task.status}'
# ready when: {"provisioning_status":"ready","task":"COMPLETED"}
```

Treat `provisioning_status == "failed"` (or a provision task in `FAILED` / `DEAD_LETTER`) as a
hard failure — see section 3.4. Typical ready time on dev is **30–60 s**; allow a generous
timeout (the stress harness defaults to 180 s).

### The caveat that bites: "ready" does not prove eventing works

A catalog flips to `ready` even when its **eventing** provisioning step finished `degraded`
(for example, Pub/Sub IAM not yet granted on the project). `provisioning_status` alone
therefore does **not** guarantee that bucket-finalize / asset events will fire.

The only trustworthy proof that eventing provisioned correctly is to **exercise it**: upload a
real file and confirm the asset is created. (Confirming the *event* via the API is currently
unreliable — see the observability note at the end.) The stress harness automates this with
its `upload_asset` phase.

### Readiness checklist, in order

1. `POST /stac/catalogs` → expect `201`.
2. Poll `GET /catalog/catalogs/{id}` until `provisioning_status == "ready"` **and**
   `task.status == "COMPLETED"`.
3. (If eventing matters for your use) upload a real asset and confirm it lists under
   `GET /assets/catalogs/{id}/collections/{col}` — this proves the bucket + signed-URL path
   actually works, not just that the row says `ready`.

---

## 2. Stress testing the lifecycle & reading the failure rate

`scripts/catalog_lifecycle_stress.py` runs N independent catalog lifecycles concurrently and
reports a per-phase failure rate. Each iteration: create catalog → wait ready → create
collections → (optionally) upload a real asset + check for its event → hard-delete each
collection → hard-delete the catalog → assert it 404s. Every iteration is **self-cleaning**:
teardown runs even after a mid-flight failure, and any catalog it could not delete is reported
under `leaked_catalogs`.

### Run it

```bash
python scripts/catalog_lifecycle_stress.py \
  --base https://data.review.fao.org/geospatial/dev/api/catalog \
  --iterations 20 --concurrency 4 --collections 2 --upload
```

All knobs also read from env (for a Cloud Run job): `STRESS_BASE`, `STRESS_ITERATIONS`,
`STRESS_CONCURRENCY`, `STRESS_COLLECTIONS`, `STRESS_UPLOAD`, `STRESS_TOKEN`,
`STRESS_READY_TIMEOUT`, `STRESS_DELETE_TIMEOUT`, `STRESS_EVENT_TIMEOUT`, `STRESS_POLL`,
`STRESS_RUN_TAG`. The process exits non-zero if any **hard** phase failed, so it goes red in CI
/ a Cloud Run job.

### Reading the report

The report prints a per-phase table plus a machine-readable `JSON {...}` line. Phases marked
`~` are **advisory** (excluded from the pass/fail verdict and the exit code). Today the only
advisory phase is `verify_event`, because asset eventing is not yet surfaced through the events
API (see the observability note) — a missing asset event must not mask an otherwise-clean
lifecycle.

```
per-phase  (~ = advisory, excluded from verdict):
  create_catalog     0/20 fail (0.0%)  ...
  wait_ready         0/20 fail (0.0%)  ...
  create_collection  0/20 fail (0.0%)  ...
  upload_asset       0/20 fail (0.0%)  ...
 ~verify_event      20/20 fail (100.0%) reasons=['no asset event ...']   <- advisory
  delete_collection  0/20 fail (0.0%)  ...
  delete_catalog     0/20 fail (0.0%)  ...
```

### Baseline observed on dev (2026-06-17, before the robustness fixes)

A 5-iteration, concurrency-3, upload-on run found:

- **Lifecycle phases: clean** except one transient failure on `delete_catalog` —
  `asyncpg LockNotAvailableError` (lock timeout) under concurrent catalog deletes, which left
  one leaked catalog. A plain retry of the delete succeeded. Overall failure rate was driven
  entirely by that one transient lock race (20% = 1/5).
- `verify_event` advisory-failed on every iteration (asset eventing not surfaced).

Both are addressed:

- The catalog hard-delete now drops the tenant schema through a bounded `lock_timeout` with
  retry on lock conflict (`safe_drop_relation`), so a transient cross-delete lock wait
  self-heals instead of returning a `500` and leaking the catalog.
- Hard-delete now force-cleans the deterministic default Pub/Sub topic/subscription even when
  the catalog config never persisted `topic_path` (a crashed provision), so topics don't leak
  to collide as "already exists" on a later same-id create.

After these fixes, expect lifecycle-phase failure rates at or near **0%** under this load;
`verify_event` stays advisory until asset eventing is surfaced.

---

## 3. Recovery — restoring from a broken create/delete

### 3.1 A catalog you could not delete (`leaked_catalogs` non-empty, or a `500` on delete)

Symptom: `DELETE /stac/catalogs/{id}?force=true` returned `500` with
`asyncpg.exceptions.LockNotAvailableError: canceling statement due to lock timeout`, and the
catalog is still present (`provisioning_status: ready`).

This is **transient lock contention** on the shared system catalog (`pg_depend`) between
concurrent `DROP SCHEMA CASCADE` operations — not corruption. The catalog row and schema are
intact.

Recovery: **just retry the delete.** It succeeds once the competing delete releases its locks.

```bash
curl -s -X DELETE "$BASE/stac/catalogs/$CAT?force=true" -w '\n%{http_code}\n'
# expect 204; confirm gone:
curl -s -o /dev/null -w '%{http_code}\n' "$BASE/stac/catalogs/$CAT"   # expect 404
```

With the lock-retry fix deployed, the server retries internally and this `500` should no longer
surface; the manual retry remains the recovery for any older revision.

### 3.2 An orphaned Pub/Sub topic ("already exists" on create)

Symptom on catalog create: a log line like
`Managed Pub/Sub topic 'projects/<proj>/topics/ds-<catalog>-events' already exists.`

This is **benign**: topic creation is idempotent (it adopts the existing topic), and topics are
one-per-catalog with a deterministic name, so they do not accumulate per cycle. It does,
however, indicate a topic that a prior delete failed to tear down — usually because the catalog
config never persisted `topic_path` (a provision that crashed mid-setup).

The hard-delete force-cleanup fix removes the deterministic default topic by name on every
delete, so this self-corrects going forward. To remove a lingering orphan topic manually:

```bash
gcloud pubsub topics delete ds-<catalog>-events       --project fao-aip-geospatial-review
gcloud pubsub subscriptions delete ds-<catalog>-events-sub --project fao-aip-geospatial-review  # if present
```

(Deleting the topic also drops its subscriptions. `NotFound` is fine — nothing to clean.)

### 3.3 Orphaned per-collection items tables (PG storage left behind)

Symptom: a tenant schema `s_<base>` still contains `t_<base>{,_attributes,_geometries,
_item_metadata,_stac_metadata}` tables for a collection that no longer exists, while
Elasticsearch shows 0 documents. These are leftovers from a pre-refactor collection delete that
rolled back after dropping the `collection_configs` pin but before dropping the tables.

They are harmless (nothing reads them) but waste storage. Clean them with the operator script,
which is **dry-run by default** — it only reports what it would drop:

```bash
# 1. Preview (safe, no drops):
psql "$DSN" -f scripts/cleanup_orphan_items_tables.sql

# 2. Scope to one schema and execute:
psql "$DSN" -v target_schema=s_ql98bdk4 -v do_drop=true -f scripts/cleanup_orphan_items_tables.sql

# 3. Or sweep every tenant schema:
psql "$DSN" -v do_drop=true -f scripts/cleanup_orphan_items_tables.sql
```

A `t_<base>` hub is dropped **only** when no row in that schema's `collection_configs` pins it
as `physical_table`, so live and mid-provision collections are never touched. The script skips
any schema lacking a `collection_configs` table. See the script header for the full safety
model. (Hard-deleting the owning catalog also clears these, since it drops the whole schema.)

### 3.4 A catalog stuck in `provisioning` or `failed`

- `provisioning` that never reaches `ready`: inspect the provision task via
  `GET /catalog/catalogs/{id}` (the `task` block) and the catalog logs
  (`GET /catalog/logs/catalogs/{id}`). A `DEAD_LETTER` provision task usually means a transient
  GCP error (e.g. a Pub/Sub `attachSubscription 403`); requeue the dead-letter task or
  hard-delete and recreate.
- `failed`: the catalog is not usable. Hard-delete it (`DELETE …?force=true`) and recreate.
  Because the failure may have left partial GCP/PG resources, the orphan-topic (3.2) and
  orphan-table (3.3) cleanups are the belt-and-braces follow-up.

---

## Observability note — what `/logs` and `/events` do and don't show

`/logs` (PostgreSQL `system_logs` + per-tenant `logs`) and `/events` (the `tasks.events`
outbox) are **separate stores**. When triaging lifecycle issues, know the current coverage:

- **Catalog** lifecycle is well covered: `catalog_creation`, `catalog_hard_deletion`,
  `gcp.destroy.start` / `gcp.eventing.teardown` / `gcp.destroy.success` all appear in
  `GET /logs/system` and `GET /logs/catalogs/{id}`.
- **Collection** delete events reach `/events`, but **`collection_creation` is not emitted**
  anywhere, and collection deletes do not write a `/logs` row.
- **Asset** create/update/delete currently produce **no durable** log or event row (the REST
  path does not carry a DB connection into the emitter, so the outbox write is skipped). This
  is why the stress harness treats `verify_event` as advisory.
- **Pub/Sub topic** create/adopt/delete are logged at `debug`/stdlib level only and do **not**
  reach the `/logs` store, so the benign "already exists" line is visible only in Cloud Run
  stdout.
- The catalog-scoped `GET /events/catalogs/{id}/events` returns `[]` for platform-scoped
  lifecycle events (they are stored with a null top-level `schema_id`); use `GET /events/system`
  and filter on the catalog id until that filter is widened.

Closing these gaps (emit `collection_creation`, make asset events durable, route Pub/Sub
lifecycle to `/logs`, fix the catalog-scoped event filter) is tracked separately; until then,
prefer `GET /logs/system` and `GET /events/system` for end-to-end lifecycle triage.
