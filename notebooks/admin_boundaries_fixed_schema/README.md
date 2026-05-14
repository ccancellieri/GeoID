# Admin Boundaries — Fixed-Schema Columnar Walkthrough

End-to-end walkthrough demonstrating the recipe described in
[issue #447](https://github.com/un-fao/GeoID/issues/447):

1. Collection optimised for **columnar / OTF** storage (DuckDB or Iceberg)
2. **Fixed schema** derived from `gdalinfo` on a sample shapefile / GeoJSON
3. GCS uploads with the **same schema** and a **distinct `asset_id` per file**
   (duplicates refused at DB level via `assets_identity_uq`)
4. **`external_id = "code"` mandatory** (`require_external_id=true`,
   `external_id_field="properties.code"`)
5. **2-D geometry statistics** (area, length, centroid, circularity, …)
6. **Geometry fix** (`invalid_geometry_policy=attempt_fix`,
   `target_dimension=force_2d`, `srid_mismatch_policy=transform`)
7. **`GcsDetailedReporter`** writing JSONL to a separate scratch bucket
8. PG-sidecar tiles, cached on bucket
9. **STAC + OGC Features search via Elasticsearch**
   (`geometry_simplified` hint, default since PR #185)

## Files

| Path | Role |
|---|---|
| `walkthrough.ipynb` | Single end-to-end notebook (lift-cells composition of 7 sources) |
| `fixtures/admin_boundaries.geojson` | 3-feature country sample with `code` external-id field |

## Source notebooks

The cells were lifted from the showcase suite on branch
`worktree-sidecars-collapse-and-snapshot` under `notebook_showcase/notebooks/`:

| Step | Source |
|---|---|
| 0 — gdalinfo → schema   | `stac_extensions/01_gdal_to_stac_transformer.ipynb` |
| 1 — catalog             | `_setup/00_ensure_demo_catalog.ipynb` |
| 2 — collection sidecars | `catalog/02_create_collection_with_layerconfig.ipynb` + `write_policy/01_external_id_deduplication.ipynb` |
| 3 — routing             | `routing/01_pg_primary_es_index_parquet_backup.ipynb` |
| 4 — bucket + Pub/Sub    | `gcp/01_bucket_init_upload_and_ingest.ipynb` |
| 5 — reporter            | `ui_walkthrough/02_upload_with_reporter.ipynb` |
| 6 — search + tiles      | `ui_walkthrough/03_read_search_features_tiles.ipynb` + `queryables/01_cql2_and_collection_search.ipynb` |

## Environment

Set in `.env` (or export before launching the kernel):

```bash
DYNASTORE_BASE_URL=https://data.review.fao.org/geospatial/v2/api
DYNASTORE_SYSADMIN_TOKEN=<sysadmin bearer>
# optional
CATALOG_ID=admin-boundaries-demo
COLLECTION_ID=countries
REPORT_BUCKET=geoid-review-temp        # scratch bucket, NOT the catalog bucket
```

Local dev pointed at `http://localhost:8080` works for everything except the
GCS upload + Pub/Sub steps (the notebook auto-skips with `_GCP_ENABLED=False`).

## Platform gaps (deferred — not blocking this walkthrough's PR)

The notebook documents three gaps that need real platform work before the
walkthrough can be promised to customers without caveats. Each is called out in
a `**Gap A/B/C**` markdown cell at the relevant step:

- **Gap A** — by design, there is **no** bespoke `bootstrap-schema` endpoint.
  The platform's contract is that every HTTP surface is an OGC conformance
  class; bootstrapping a fixed schema is a composition of two existing
  surfaces (issue #473, PR #477). Step 0b in `walkthrough.ipynb` shows the
  full sequence: `POST .../assets/{id}/processes/gdal/execution`
  (OGC API - Processes) → local OGR→Postgres type map →
  `GET`/`PUT /configs/.../plugins/items_postgresql_driver` (PluginConfig API).
  The PluginConfig PUT works at either **collection scope**
  (`/configs/catalogs/{cat}/collections/{col}/plugins/{plugin_id}`, used in
  the notebook) or **catalog scope**
  (`/configs/catalogs/{cat}/plugins/{plugin_id}`, sysadmin) when the same
  schema should apply by default to every collection in the catalog.
  The cell is gated by `BOOTSTRAP_FROM = None` and is documentary; users
  copy-paste the snippet to bootstrap from a real asset.
  **Regression-risk:** because the cell is gated off, the notebook test
  pass never drives the Process→PluginConfig sequence — changes to the
  `gdal` Process payload, the OGR→PG type map, the PluginConfig PUT
  shape, or the sync/async response contract will not be caught by CI
  from this notebook. Cover with a `TestClient` integration test if you
  depend on this bootstrap path.
- **Gap B** — OTF as **WRITE-primary** is not live-tested in review env;
  the notebook keeps PG as primary and notes how to swap.
- **Gap C** — L2 bucket-cache config (`TilesCachingConfig`: `key_prefix` +
  `ttl_seconds`) and per-response observability (`X-Tile-Cache`,
  `X-Tile-Source` headers) **ship on geoid**. Per-catalog bucket name is
  discoverable via `GET /admin/catalogs/{cat}`. Residual scope on #475:
  fao-maps-titiler upstream wiring (F1), Kibana cache-hit-ratio panel
  (F2), review-env smoke (F6).

Tracker issues: Gap A → [#671](https://github.com/un-fao/GeoID/issues/671)
(TestClient integration for bootstrap-schema composition), Gap B →
[#474](https://github.com/un-fao/GeoID/issues/474) (OTF write-primary live
test), Gap C → [#475](https://github.com/un-fao/GeoID/issues/475)
(residual: titiler / Kibana panel / review-env smoke).
