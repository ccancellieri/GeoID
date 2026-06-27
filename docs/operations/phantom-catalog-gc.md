# Phantom Catalog GC — Operator Runbook

A phantom catalog is a row in `catalog.catalogs` whose `external_id` accidentally
received an internal-id-shaped value (`c_<13 base32>`), created by a now-fixed bug.
Each phantom owns real GCP resources (Postgres schema, GCS bucket, Pub/Sub
topic + subscription) and must be cleaned up at the data layer, keyed on the
internal `id` PK only — never on the `external_id`.

## Entry point

```
python -m dynastore.scripts.gc_phantom_catalogs [OPTIONS]
```

| Flag | Effect |
|---|---|
| *(none)* | Dry run — prints what would happen, makes no changes |
| `--execute` | Performs teardown: GCP resources → DROP SCHEMA → DELETE row |
| `--allow-gcp-skip` | Allows schema/row drop even when GCP protocols are absent (use only if GCP resources were already manually deleted) |
| `--ids ID1 ID2 …` | Target specific internal catalog IDs (each must be internal-id-shaped or it is skipped) |
| `--ids-file FILE` | Read internal IDs from a text file, one per line (lines starting with `#` are ignored) |

## Database connection

The script resolves `DATABASE_URL` from (first match wins):

1. `DATABASE_URL` environment variable
2. `/dynastore/env/.env` — the Secret Manager mount used by Cloud Run jobs

## GCP module requirement

`--execute` **without `--allow-gcp-skip`** refuses to drop the PG schema or catalog
row when StorageProtocol / EventingProtocol are not registered in the current
process. This prevents orphaning the GCS bucket and Pub/Sub resources if the
script is run without the GCP module stack initialised.

To clean up fully, the script must run in a process that has the GCP module
loaded (a Cloud Run Job with the full dynastore SCOPE including the GCP
extension), or the GCP resources must be deleted manually first and
`--allow-gcp-skip` passed.

## Cloud Run Job deployment

The deploy configuration for the Cloud Run Job (container image, environment
variables, service account, SCOPE, schedule) belongs in the **dynastore deploy
wrapper** (`dynastore/.github/workflows/deploy.yml` and its Cloud Run Job
definitions), not in this repository. See issue #2365 for the full Job spec.

### What the Job needs

| Item | Value |
|---|---|
| Container image | Same `dynastore` image as the catalog service (same SCOPE) |
| Entrypoint override | `python -m dynastore.scripts.gc_phantom_catalogs --execute` |
| `DATABASE_URL` | Secret Manager mount at `/dynastore/env/.env` (same as catalog service) |
| `SCOPE` | Must include `gcp` extension so StorageProtocol and EventingProtocol are registered |
| `SERVICE_URL` | Internal catalog service URL (needed by provisioner on startup; same as catalog service env) |
| IAM service account | Same as the catalog service (needs GCS admin + Pub/Sub admin on phantom resources) |
| Execution mode | One-shot Cloud Run Job (not a Service) — exits 0 on success, non-zero on any teardown failure |

### Local dry run (no GCP)

```bash
DATABASE_URL=postgresql://... \
  python -m dynastore.scripts.gc_phantom_catalogs
```

Prints detected phantoms and the actions that would be taken; makes no changes.

### Manual targeted cleanup

```bash
DATABASE_URL=postgresql://... \
  python -m dynastore.scripts.gc_phantom_catalogs \
  --ids c_3kp7rmn4bcdef c_2abc3defg4hij \
  --execute
```

### If GCP resources were already manually deleted

```bash
DATABASE_URL=postgresql://... \
  python -m dynastore.scripts.gc_phantom_catalogs \
  --ids c_3kp7rmn4bcdef \
  --allow-gcp-skip \
  --execute
```

## Safety invariants

- The script keys exclusively on the internal `id` PK (the schema name). It
  never accepts `external_id` as a target, even via `--ids`.
- Only rows whose `external_id` matches `^c_[2-9a-x]{13}$` are touched; real
  catalog rows are skipped even if passed explicitly.
- GCS blob deletion is irreversible. The GCS SDK handles continuation
  internally; do not add application-level retry on partial deletion.
- Dry run is always the default. `--execute` must be passed explicitly.
