# Admin Boundaries — Fixed-Schema Fixture

Sample data for the fixed-schema columnar recipe described in
[issue #447](https://github.com/un-fao/GeoID/issues/447). Previously lived
under `notebooks/admin_boundaries_fixed_schema/` alongside an interactive
walkthrough notebook; that notebook was retired with the teaching-notebook
consolidation (catalog/collection lifecycle is now covered by the
`catalog_lifecycle_with_presets` platform notebook and ingestion by
`ingestion_virtual_asset`, both seeded into JupyterLite at startup), so the
fixture moved here to sit with the test that actually uses it.

## Files

| Path | Role |
|---|---|
| `admin_boundaries.geojson` | 3-feature country sample with `code` external-id field (used by `test_bootstrap_schema_composition.py`) |

## Platform gaps (tracked)

- **Gap A** — by design, there is **no** bespoke `bootstrap-schema` endpoint.
  The platform's contract is that every HTTP surface is an OGC conformance
  class; bootstrapping a fixed schema is a composition of two existing
  surfaces (issue #473, PR #477): `POST .../processes/gdal/execution` with
  `{"inputs": {"asset_id": "..."}}` (OGC API - Processes) → local
  OGR→Postgres type map → `GET`/`PUT /configs/.../plugins/items_postgresql_driver`
  (PluginConfig API), at collection or catalog scope. The sequence is covered
  by `tests/dynastore/extensions/configs/integration/test_bootstrap_schema_composition.py`.
  Tracker: [#671](https://github.com/un-fao/GeoID/issues/671).
- **Gap B** — OTF as **WRITE-primary** is not live-tested in the shared env.
  Tracker: [#474](https://github.com/un-fao/GeoID/issues/474).
- **Gap C** — L2 bucket-cache config and per-response observability ship on
  geoid; residual scope is fao-maps-titiler upstream wiring, the Kibana
  cache-hit-ratio panel, and the shared-env smoke.
  Tracker: [#475](https://github.com/un-fao/GeoID/issues/475).
