# Cycle F use cases — config API end-to-end walkthrough

Four notebooks reproducing the user-stated real-world scenarios that exercise
the post-Cycle-F config API.  Each notebook is **independently runnable** and
uses its own collection (sidecars are immutable post-creation, so cross-UC
sharing is not possible).

## How to run

Both local-dev and review env are supported.  Pick the right `DYNASTORE_BASE_URL`:

```bash
# Local dev
export DYNASTORE_BASE_URL=http://localhost:8080

# Review env
export DYNASTORE_BASE_URL=https://data.review.fao.org/geospatial/v2/api/catalog
```

Set a token if you need authenticated PATCH/PUT writes.  Anonymous works for
read-only flows but every config write requires a sysadmin Bearer:

```bash
export DYNASTORE_TOKEN=$(curl -s -X POST \
  "${IDP_PUBLIC_URL:-http://localhost:8180/realms/geoid}/protocol/openid-connect/token" \
  -d "client_id=geoid-api" \
  -d "client_secret=${IDP_CLIENT_SECRET}" \
  -d "username=${IDP_ADMIN_USER:-admin}" \
  -d "password=${IDP_ADMIN_PASSWORD}" \
  -d "grant_type=password" | jq -r .access_token)
```

Then open any notebook in JupyterLab → **Run All**.  Each notebook ends with
a teardown cell that deletes its catalog so re-runs are idempotent.

## Notebook index

| # | Notebook | Scenario | Sidecars / drivers | Write policy / routing |
|---|---|---|---|---|
| 1 | `01_uc1_pg_full_sidecars_routing.ipynb` | PG with all 4 sidecars + 2D stats + dual-search (ES public + PG) | geometries (statistics ON), attributes (`asset_id_field`), item_metadata, stac_metadata | `external_id_field=properties.code` (on `items_write_policy`), `on_conflict=new_version`, `enable_validity=true`, routing SEARCH = ES `geometry_simplified` + PG `geometry_exact` |
| 2 | `02_uc2_schema_patch_multiversion.ipynb` | Schema enforcement → 207 IngestionReport + multi-version from 2 assets | geometries, attributes | ItemsSchema with `code`/`name` mandatory, `external_id_field=properties.code` (on `items_write_policy`), `on_conflict=new_version` |
| 3 | `03_uc3_private_es.ipynb` | PG + private ES; SEARCH dispatches to private ES + PG; privacy probe | geometries, attributes | routing SEARCH = `items_elasticsearch_private_driver` (simplified) + `items_postgresql_driver` (exact) |
| 4 | `04_uc4_asset_refusal.ipynb` | Asset duplicate refusal config round-trip | n/a (asset tier) | `AssetsWritePolicy.on_conflict` toggled `refuse_return` → `refuse_fail`; verifies both behaviors |

## Known limitations / unverified bits

These notebooks have **not** yet been run end-to-end against a live env
with an authenticated token.  The shapes were derived from review-env
OpenAPI introspection + the existing `ui_walkthrough/01-02` notebooks
(which are battle-tested), but the following remain unverified:

- **Hint dispatch is internal, not user-toggleable.** Earlier drafts of
  these notebooks tried `?hint=geometry_simplified` query params; that
  parameter does not exist.  The router derives hints from query
  characteristics (`extensions/stac/search.py::filter_hints`).  The
  notebooks now demonstrate config + verifying-by-readback only.
- **STAC items list endpoint `?filter=…` / `?external_id=…` not
  available.**  The list endpoint takes only `limit`, `offset`, `lang`.
  Filter-by-attribute uses the platform `/search/catalogs/{cat}/items-
  search` POST surface (CQL2 body); UC2 currently does list+grep for
  brevity.
- **Asset registration shape**: `POST /assets/catalogs/{cat}/collections/
  {coll}` with JSON body `{asset_id, asset_type, uri, metadata}` — no
  multipart upload (binaries land in GCS via signed URL → OBJECT_FINALIZE
  pub/sub auto-registration; see `ui_walkthrough/02_upload_with_reporter
  .ipynb`).
- **`asset_write_policy_defaults` vs `assets_write_policy`** at catalog
  scope: UC4 uses the defaults class (idiomatic for catalog-tier
  posture); both work, but the defaults form omits collection-tier
  matchers.
- **`/effective` endpoint does NOT exist.**  Use `?resolved=true` on the
  GET plugin endpoint to retrieve the waterfall-resolved shape.

A full live-validation pass is tracked as a follow-up (re-run all four
notebooks against review env with a sysadmin Bearer; confirm response
codes + body shapes match the assertions).  Until that pass lands,
treat the notebooks as **structured-and-buildable** rather than
**proven-correct**.

## What this exercises

- **`PATCH /configs/catalogs/{cat}/collections/{coll}/plugins/{plugin_id}`** — collection-scope writes for items configs
- **`PATCH /configs/catalogs/{cat}/plugins/{plugin_id}`** — catalog-scope writes (e.g. asset write policy)
- **`GET /configs/catalogs/{cat}/collections/{coll}/?strict=true&resolved=true`** — slim view of owned configs
- **`GET /configs/.../plugins/{plugin_id}/effective`** — per-field source attribution
- **`POST /stac/catalogs/{cat}/collections/{coll}/items`** — ingestion (success, partial 207, conflict)
- **`GET /stac/catalogs/{cat}/collections/{coll}/items?hint=…`** — hint-based dispatch
- **Multi-instance refs** — covered in `../storage_drivers/04_engines_and_multi_instance.ipynb`

## Cycle F architecture references

- `docs/components/configs_api.md` — full configs API reference (endpoints, query params, HATEOAS link catalog, scope strictness rules)
- `docs/components/storage_drivers.md` — driver class registry, sidecars, routing, hints
- `docs/components/sidecar_configs.md` — per-sidecar field reference with defaults
- `docs/components/platform_engines.md` — engines layer at `platform.protocols.storage.*`

## Related notebooks

- `notebook_showcase/notebooks/ui_walkthrough/01_setup_collection.ipynb` — minimum-config walkthrough (the source pattern this folder builds on)
- `notebook_showcase/notebooks/storage_drivers/04_engines_and_multi_instance.ipynb` — multi-instance driver refs (UC1, UC2, UC3 from the F.6 use-case set)
- `notebook_showcase/notebooks/write_policy/` — narrower write-policy regression scenarios

## Cleanup

Each notebook teardown cell:

```python
client.delete(f"/stac/catalogs/{CATALOG_ID}", params={"force": "true"})
```

If a run was interrupted before teardown, list ephemeral catalogs:

```bash
curl -sS "${DYNASTORE_BASE_URL}/stac/catalogs" \
  -H "Authorization: Bearer $DYNASTORE_TOKEN" | jq '.catalogs[].id'
```

…and delete any matching `cf_uc[1-4]_*` ids.
