# UI Walkthrough — Alternative-UI Integration Series

Four notebooks that walk a UI builder through the dynastore surface they need:

| # | Notebook | What it shows |
|---|---|---|
| 01 | `01_setup_collection.ipynb` | Catalog + collection create, GDAL schema introspection, columnar geometry stats (area + length materialized as columns), write policy refusing duplicate `asset_id`. Each PATCH is followed by a configured-vs-effective delta cell so you can see what you own vs what you inherit. |
| 02 | `02_upload_with_reporter.ipynb` | Signed-URL `init-upload`, `PUT` bytes, asset registration (auto via Pub/Sub on remote, manual on local), ingestion process driven by `GcsDetailedReporter` writing JSONLines under `gs://{catalog_bucket}/{collection_id}/reports/`. Re-ingest demonstrates the write-policy refusal flowing into the report. |
| 03 | `03_read_search_features_tiles.ipynb` | `POST /search` (cursor pagination, ES-primary), `GET /features/.../items` (CQL2 + bbox + sort, PG-backed), `GET …/tiles/{tms}/{z}/{x}/{y}.mvt`. |
| 04 | `04_dwh_join_async_export.ipynb` | Async `dwh_join` Process (id `dwh_join`, scope `COLLECTION`, `ASYNC_EXECUTE`) using the canonical query `SELECT * FROM dwh` from `tests/dynastore/extensions/dwh/test_dwh_repro.py`. Submit, poll, fetch result. Last cell shows the synchronous `/dwh/join` endpoint for inline joins. |

Order matters: 01 stands up the catalog/collection that 02–04 reuse via `RUN_ID`/`CATALOG_ID`/`COLLECTION_ID` env vars.

## Conventions

- **Auth is optional.** If `DYNASTORE_TOKEN` (or `DYNASTORE_SYSADMIN_TOKEN` / `DYNASTORE_ADMIN_TOKEN`) is set, the notebooks attach a Bearer token. Otherwise they run anonymous. On a stack with deny-by-default IAM you'll need a token with sysadmin reach.
- **Catalog bucket reuse.** The GCP plugin provisions a bucket at catalog-creation time. Notebook 02 discovers it via `GET /gcp/buckets/catalogs/{cid}` and uses `gs://{bucket}/{collection}/reports/` as the simulated DWH landing zone. No extra bucket provisioning is needed.
- **Pub/Sub gating.** Notebook 02 detects local stacks (`localhost`/`127.0.0.1`) and falls back to manual asset registration because the `OBJECT_FINALIZE` push subscription cannot reach a host without a `K_SERVICE`. Run 02 against the review env to exercise the pub/sub path.
- **Minimum config + delta.** Each PATCH cell sets only the fields it needs and immediately renders the explicit-vs-effective config + per-field source via `/configs/.../{ClassName}/effective`. Inherited defaults are visible, not hidden.
- **Canonical fixtures.** Notebook 04 uses the dwh extension's test fixture verbatim (`SELECT * FROM dwh`, `join_col`). Substitute a real query against a sandbox project for an end-to-end remote demo.

## Environment variables

| Variable | Default | Notes |
|---|---|---|
| `DYNASTORE_BASE_URL` | `http://localhost:8080` | Set to `https://data.review.fao.org/geospatial/v2/api` for review. |
| `DYNASTORE_TOKEN` | _(empty)_ | Optional. Falls back to `DYNASTORE_SYSADMIN_TOKEN` / `DYNASTORE_ADMIN_TOKEN`. |
| `RUN_ID` | random 8-char hex | Set to reuse the same ephemeral catalog across runs. |
| `CATALOG_ID` | `uw_<RUN_ID>` | Override to point at an existing catalog. |
| `COLLECTION_ID` | `col_<RUN_ID>` | Override to point at an existing collection. |
| `DWH_PROJECT_ID` | `p` | Notebook 04 only. Override with a real BQ project to run a real query. |

## Running

```bash
cd notebook_showcase/notebooks/ui_walkthrough
# Local dev (sysadmin token via the dev keycloak)
export DYNASTORE_BASE_URL=http://localhost:8080
export DYNASTORE_TOKEN=$(docker exec geoid_web curl -s -X POST \
  "http://keycloak:8080/realms/geoid/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password&client_id=geoid-api&client_secret=geoid-api-secret&username=testadmin&password=testpassword" \
  | python -c "import json,sys;print(json.load(sys.stdin)['access_token'])")

jupyter nbconvert --to notebook --execute --inplace 01_setup_collection.ipynb
jupyter nbconvert --to notebook --execute --inplace 02_upload_with_reporter.ipynb
jupyter nbconvert --to notebook --execute --inplace 03_read_search_features_tiles.ipynb
jupyter nbconvert --to notebook --execute --inplace 04_dwh_join_async_export.ipynb
```

For review, set `DYNASTORE_BASE_URL` and a sysadmin `DYNASTORE_TOKEN` for that env, then re-run 02 to exercise the pub/sub-driven asset auto-creation path.

## Cleanup

01 leaves the ephemeral catalog around so 02–04 can reuse it. Hard-delete after the series:

```bash
curl -X DELETE -H "Authorization: Bearer $DYNASTORE_TOKEN" \
  "$DYNASTORE_BASE_URL/stac/catalogs/$CATALOG_ID?force=true"
```

## Fixtures

`fixtures/sample.geojson` — three polygon features (Rome, Paris, London) with `asset_id`, `name`, `datetime` properties. Used by 01 (GDAL schema fallback) and 02 (upload payload).
