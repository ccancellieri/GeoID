# The Joins Extension

The `joins` extension implements a working-draft of **OGC API – Joins Part 1**
for DynaStore. It lets a client enrich a primary DynaStore collection (any
READ-capable storage driver) with attributes from a secondary source (another
registered collection or an ad-hoc BigQuery table), joining on a shared column
and returning the merged result as a feature stream.

Both sides of the join body filter via **OGC CQL2** — the same standard filter
language used by OGC API - Features Part 3. Generators emit one shape;
operators learn one filter dialect.

The legacy `/dwh` endpoint (predates OGC API - Joins) remains supported and
installs automatically alongside `/join` when `dynastore[dwh]` is used. See
[§ Legacy DWH endpoint](#legacy-dwh-endpoint) below.

---

## URL structure

```
/join/                                                                           landing page
/join/conformance                                                                OGC conformance
/join/catalogs/{catalog_id}/collections/{collection_id}/join  GET  describe join capabilities
/join/catalogs/{catalog_id}/collections/{collection_id}/join  POST execute join → FeatureCollection
```

OGC conformance declared:
```
http://www.opengis.net/spec/ogcapi-joins-1/0.0/conf/core
```

---

## Request body (`POST .../join`)

```json
{
  "secondary": { ... },
  "join": {
    "primary_column": "geoid",
    "secondary_column": "geoid",
    "enrichment": true
  },
  "primary_filter": {
    "cql": "area > 100 AND ADM2_PCODE LIKE 'TG%'",
    "cql_lang": "cql2-text"
  },
  "projection": {
    "with_geometry": true,
    "destination_crs": 4326,
    "attributes": ["geoid", "ADM2_PCODE"]
  },
  "paging": { "limit": 100, "offset": 0 },
  "output": { "format": "geojson", "encoding": "utf-8" }
}
```

### `secondary` — two variants (discriminated by `driver`)

**`NamedSecondarySpec`** — reference a registered collection in the same catalog:
```json
{ "driver": "registered", "ref": "<collection_id>" }
```

**`BigQuerySecondarySpec`** — per-request BigQuery target (table or view):
```json
{
  "driver": "bigquery",
  "target": {
    "project_id": "<gcp-project>",
    "dataset_id": "dynastore_ingestion",
    "table_name": "adm2"
  },
  "filter": {
    "cql": "temprature > 25 AND rain_fall < 100",
    "cql_lang": "cql2-text"
  }
}
```

### Filter symmetry — primary and secondary speak the same language

Both `JoinRequest.primary_filter` and `BigQuerySecondarySpec.filter` use the
same `PrimaryFilterSpec` DTO (CQL2 text or JSON). Both are applied **before**
the join executor sees rows, so they reduce the secondary index size and the
primary stream length independently:

| Side | Where it's applied | Translator |
|---|---|---|
| primary (`primary_filter`) | Pushed to the primary driver via `QueryRequest.cql_filter`. PG driver uses `modules/tools/cql.py`. | pygeofilter SQLAlchemy backend (PG-flavoured) |
| secondary (`secondary.filter`, BQ only) | Translated to a BigQuery `WHERE` clause appended to `SELECT * FROM <fqn>`. | pygeofilter SQL backend with BQ-flavoured overrides — see `modules/joins/bq_filter.py` |

Spatial CQL2 ops (`S_INTERSECTS`, `S_WITHIN`, `S_CONTAINS`, `S_DISJOINT`,
`S_TOUCHES`, `S_CROSSES`) translate identically on both sides — both
PostGIS and BigQuery accept the mixed-case `ST_*` names. CQL2 `BBOX(minx,
miny, maxx, maxy)` literals translate to PostGIS `ST_GeomFromText` for
the PG primary, and to BigQuery `ST_GeogFromText` for the BQ secondary.

### Field reference

| Field | Type | Default | Notes |
|---|---|---|---|
| `secondary` | `NamedSecondarySpec \| BigQuerySecondarySpec` | required | Discriminated by `driver`. |
| `secondary.filter` (BQ only) | `PrimaryFilterSpec` | `null` | CQL2 filter applied to the BQ rows BEFORE the join. |
| `join.primary_column` | `str` | required | Looked up first in `feature.properties`, then on `feature.id`. |
| `join.secondary_column` | `str` | required | Same lookup as primary. |
| `join.enrichment` | `bool` | `true` | Merge secondary properties into the joined feature. |
| `primary_filter.cql` | `str` | — | CQL2 expression (text or JSON form per `cql_lang`). |
| `primary_filter.cql_lang` | `"cql2-text" \| "cql2-json"` | `"cql2-text"` | CQL2 dialect. |
| `projection.with_geometry` | `bool` | `true` | Omit geometry when false. |
| `projection.destination_crs` | `int` | `4326` | Output EPSG code. |
| `projection.attributes` | `list[str] \| null` | `null` (= all) | Subset of PRIMARY attributes; join key always preserved. |
| `paging.limit` | `int` (1–10000) | `100` | Joined-feature cap. |
| `paging.offset` | `int` (≥0) | `0` | Page offset into the joined stream. |
| `output.format` | enum | `"geojson"` | `geojson \| json \| csv \| geopackage \| parquet`. |
| `output.encoding` | `str` | `"utf-8"` | Text encoding. |

---

## Filter language — CQL2

CQL2 (Common Query Language v2) is OGC API - Features Part 3 / OGC API -
Filter. It's a typed expression language for filtering features over an HTTP
API surface. Two encodings:

### `cql2-text` — human-readable infix

```sql
area > 100
ADM2_PCODE LIKE 'TG%'
ADM2_PCODE IN ('TG0309', 'TG0403', 'TG0410')
geoid = '019dd465-2e4f-7f61-b83e-80f74a43ac44'
S_INTERSECTS(geom, BBOX(0, 5, 2, 8))
S_WITHIN(geom, BBOX(-10, -5, 10, 15))
area > 100 AND temprature > 25
NOT (rain_fall < 50 OR is_dry IS NULL)
```

### `cql2-json` — AST-as-JSON form (for generators)

```json
{
  "op": "and",
  "args": [
    {"op": ">", "args": [{"property": "area"}, 100]},
    {"op": "s_intersects", "args": [
      {"property": "geom"},
      {"bbox": [0, 5, 2, 8]}
    ]}
  ]
}
```

### CQL2 = SQL-injection guard

The CQL2 parser is the security boundary — arbitrary user input only reaches
the BigQuery / PostgreSQL planner if it parses to a valid pygeofilter AST.
Bad syntax is rejected up-front with a parser error pointing at the bad
token. Unknown column references are rejected with the available-columns list.

This is why DynaStore went CQL2 over raw SQL on the new `/join` surface,
and why `/dwh`'s legacy `where` field is permanently disabled (the legacy
`dwh_query` is still raw SQL — it predates the safer surface).

---

## Driver discovery — zero-config routing

You don't need to PUT a `CollectionRoutingConfig` for every catalog/collection
pair before calling `/join`. The platform-default `CollectionRoutingConfig`
already lists `ItemsPostgresqlDriver` under `operations[READ]`; the `/join`
service asks the router for a `READ` driver with `hint="join"`, and the router
matches drivers whose `supported_hints` set includes `"join"`.

Both the PG driver (`ItemsPostgresqlDriver`) and the BQ driver
(`ItemsBigQueryDriver`) self-declare `"join"` in their `supported_hints`, so:

- A zero-config deployment routes `/join` to PG (the platform default).
- An operator who pins a BQ-backed collection's READ driver via PUT routes
  `/join` to BQ (and the same registration also serves `/features`).
- An operator can pin different drivers for `/join` vs `/features` by
  populating `OperationDriverEntry.hints` with the hint each route should
  match (strict-match wins when entry hints are populated).

---

## Join pipeline

```
POST /join/.../join
        │
        ├─ 1. resolve_drivers("READ", catalog, collection, hint="join")
        │     → first-matching primary driver (PG by default, BQ if pinned)
        ├─ 2. resolve secondary source
        │     NamedSecondarySpec     → resolve_drivers("READ", secondary ref)
        │     BigQuerySecondarySpec  → stream_bigquery_secondary(target, filter)
        │                              ├─ if filter set: cql_to_bq_where()
        │                              └─ injects WHERE into SELECT * FROM <fqn>
        ├─ 3. executor.index_secondary(secondary_stream)
        │     → materialize secondary into dict {secondary_column → properties}
        │     (key falls back to feat.id when secondary_column is the row id)
        ├─ 4. primary_driver.read_entities(request=QueryRequest(cql_filter=primary_filter))
        │     → async iterator of Feature
        └─ 5. executor.run_join(primary_stream, secondary_index, join_spec, projection)
              → inner join: lookup secondary_index[primary_join_key]
              (key falls back to feat.id when primary_column is the row id)
              → if enrichment=true, merge secondary properties into feature.properties
              → StreamingResponse FeatureCollection + _join_meta
```

### Response envelope

```json
{
  "type": "FeatureCollection",
  "features": [ ... ],
  "_join_meta": {
    "secondary_rows_materialized": 40,
    "joined_features": 40
  }
}
```

`_join_meta.secondary_rows_materialized` is the size of the secondary index
held in memory during the join. `_join_meta.joined_features` is the number of
features yielded after the inner-join filter (= matching primary rows). Use
the secondary `filter` to cap memory pressure when the secondary side is large.

---

## Cookbook

The recipes below illustrate common patterns using a sample catalog
(`adm2_catalog/adm2_collection`) where the PG primary holds administrative
region features (uuid `geoid` per row) and a BigQuery secondary table is
keyed by the same `geoid` UUID with `temprature` and `rain_fall` columns.

### 1. Minimal /join — BQ secondary, no filters

```bash
BASE=https://<your-deployment>/api/maps
curl -X POST "$BASE/join/catalogs/adm2_catalog/collections/adm2_collection/join" \
  -H "Content-Type: application/json" \
  -d '{
    "secondary": {
      "driver": "bigquery",
      "target": {"project_id":"<gcp-project>","dataset_id":"dynastore_ingestion","table_name":"adm2"}
    },
    "join": {"primary_column":"geoid","secondary_column":"geoid","enrichment":true},
    "projection": {"with_geometry":false,"attributes":["geoid","ADM2_PCODE"]},
    "paging": {"limit":100,"offset":0}
  }'
```

### 2. /join with a BQ-side filter

```bash
curl -X POST "$BASE/join/catalogs/adm2_catalog/collections/adm2_collection/join" \
  -H "Content-Type: application/json" \
  -d '{
    "secondary": {
      "driver": "bigquery",
      "target": {"project_id":"<gcp-project>","dataset_id":"dynastore_ingestion","table_name":"adm2"},
      "filter": {"cql": "temprature > 25 AND rain_fall < 100", "cql_lang": "cql2-text"}
    },
    "join": {"primary_column":"geoid","secondary_column":"geoid","enrichment":true},
    "projection": {"with_geometry":false,"attributes":["geoid","ADM2_PCODE","temprature"]},
    "paging": {"limit":100,"offset":0}
  }'
```

### 3. /join with PG-primary filter AND BQ-secondary filter (compound)

Both filters apply BEFORE the join. Net effect is the AND of the two —
"PG features that match X joined to BQ rows that match Y".

```bash
curl -X POST "$BASE/join/catalogs/adm2_catalog/collections/adm2_collection/join" \
  -H "Content-Type: application/json" \
  -d '{
    "secondary": {
      "driver": "bigquery",
      "target": {"project_id":"<gcp-project>","dataset_id":"dynastore_ingestion","table_name":"adm2"},
      "filter": {"cql": "temprature > 25", "cql_lang": "cql2-text"}
    },
    "join": {"primary_column":"geoid","secondary_column":"geoid","enrichment":true},
    "primary_filter": {"cql": "area > 100", "cql_lang": "cql2-text"},
    "projection": {"with_geometry":false,"attributes":["geoid","ADM2_PCODE","area","temprature"]},
    "paging": {"limit":100,"offset":0}
  }'
```

### 4. /join with a spatial filter (BBOX intersection)

```bash
curl -X POST "$BASE/join/catalogs/adm2_catalog/collections/adm2_collection/join" \
  -H "Content-Type: application/json" \
  -d '{
    "secondary": {
      "driver": "bigquery",
      "target": {"project_id":"<gcp-project>","dataset_id":"dynastore_ingestion","table_name":"adm2"}
    },
    "join": {"primary_column":"geoid","secondary_column":"geoid","enrichment":true},
    "primary_filter": {"cql": "S_INTERSECTS(geom, BBOX(0, 5, 2, 8))", "cql_lang": "cql2-text"},
    "projection": {"with_geometry":true,"destination_crs":4326,"attributes":["geoid","ADM2_PCODE"]},
    "paging": {"limit":100,"offset":0}
  }'
```

### 5. /dwh equivalent — the legacy SELECT-statement form

The legacy `/dwh/.../join` endpoint accepts a raw BigQuery `SELECT` in
`dwh_query`. Use this when you need SQL power CQL2 cannot express
(multi-table BQ JOIN, GROUP BY, window functions, CTEs, PIVOT…).

```bash
curl -X POST "$BASE/dwh/catalogs/adm2_catalog/join" \
  -H "Content-Type: application/json" \
  -d '{
    "dwh_project_id": "<gcp-project>",
    "dwh_query": "SELECT geoid, temprature, rain_fall FROM `<gcp-project>.dynastore_ingestion.adm2` WHERE temprature > 25",
    "collection": "adm2_collection",
    "with_geometry": false,
    "dwh_join_column": "geoid",
    "join_column": "geoid",
    "geospatial_attributes": ["ADM2_PCODE"],
    "attributes": ["geoid"],
    "output_format": "geojson",
    "output_encoding": "utf-8",
    "limit": 100,
    "offset": 0,
    "destination_crs": 4326
  }'
```

The same query with a multi-table BQ JOIN (impossible to express in CQL2):

```bash
curl -X POST "$BASE/dwh/catalogs/adm2_catalog/join" \
  -H "Content-Type: application/json" \
  -d '{
    "dwh_project_id": "<gcp-project>",
    "dwh_query": "WITH hot AS (SELECT geoid, temprature FROM `<gcp-project>.dynastore_ingestion.adm2` WHERE temprature > 25) SELECT h.geoid, h.temprature, p.population FROM hot h LEFT JOIN `<gcp-project>.dynastore_ingestion.population` p USING (geoid)",
    "collection": "adm2_collection",
    "with_geometry": false,
    "dwh_join_column": "geoid",
    "join_column": "geoid",
    "attributes": ["geoid"],
    "output_format": "geojson",
    "limit": 100,
    "offset": 0,
    "destination_crs": 4326
  }'
```

The OGC equivalent of the multi-table case is to build the same logic as a
**BigQuery view** in your own project, then point the OGC `secondary.target`
at the view. The OGC body stays simple and standard, all the SQL power lives
in BQ where the planner caches the view definition.

---

## Error responses

| HTTP | Body shape | Cause |
|---|---|---|
| 400 | `{"detail": "Invalid primary_filter: Invalid CQL2 (cql2-text): No terminal matches '!' …"}` | Primary `cql` doesn't parse as CQL2. |
| 400 | `{"detail": "Invalid CQL filter: Unknown properties: ADM2_PCODE. Available properties: area, …"}` | Primary `cql` references a column not in the field-mapping. |
| 422 | `{"detail": "… Invalid CQL2 (cql2-text): … | Error: …"}` | Secondary `filter.cql` doesn't parse — caught by Pydantic via `_build_secondary_context`. |
| 422 | `{"detail": [{"loc": ["body", "..."], "msg": "Field required", …}]}` | Pydantic schema violation on the request body. |
| 404 | `{"detail": "No READ driver registered for {cat}/{col}. Configure a CollectionRoutingConfig before /join."}` | The collection has no items driver mapped under `operations[READ]`. This should not fire on default deploys (the platform-default routing config lists the PG driver under READ). |
| 500 | `{"detail": "BigQuery service not available (GCP module not loaded)."}` | The service is not shipping `module_gcp` in its SCOPE. Ensure the maps/join service scope includes the GCP module. |

---

## Collection link contribution

`JoinsLinkContributor` injects a `rel="join"` link into collection metadata
responses, advertising the join endpoint URL for any collection that has at
least one READ driver registered.

---

## Key files

| File | Responsibility |
|---|---|
| `extensions/joins/joins_service.py` | FastAPI router; GET describe + POST execute (priority 180) |
| `extensions/joins/config.py` | `JoinsPluginConfig` — `enabled` flag only |
| `extensions/joins/link_contrib.py` | Adds `rel="join"` link to collection metadata |
| `modules/joins/executor.py` | Driver-agnostic inner join: `index_secondary()` + `run_join()` |
| `modules/joins/bq_secondary.py` | Wraps `ItemsBigQueryDriver` for per-request BQ targets; translates `secondary.filter` to a BQ `WHERE` clause |
| `modules/joins/bq_filter.py` | CQL2 → BigQuery WHERE translator (subclass of pygeofilter SQLEvaluator with backtick identifier quoting + BQ GEOGRAPHY literals) |
| `modules/joins/models.py` | Pydantic request / response DTOs (one source of truth for OpenAPI auto-docs) |
| `modules/storage/router.py` | Hint resolution — empty `entry.hints` falls back to `driver.supported_hints` |

---

## Legacy DWH endpoint

`extensions/dwh/` is a pre-OGC-API-Joins surface that predates the `/join`
extension. It remains supported for backwards compatibility and is installed
automatically when `dynastore[dwh]` is used (which also installs `dynastore[joins]`).

| Route | Description |
|---|---|
| `POST /dwh/join` | Deprecated — catalog id in request body, no catalog scope in the URL |
| `POST /dwh/catalogs/{catalog_id}/join` | Per-catalog join (OGC aligned path) |
| `POST /dwh/catalogs/{catalog_id}/tiles/{z}/{x}/{y}/join.{format}` | Per-catalog MVT tile join (OGC aligned path) |

Key differences from `/join`:

- **BigQuery only** — secondary is always a BQ query string; no named secondary.
- **Raw SQL `dwh_query`** — full BigQuery SQL statement, not constrained to
  CQL2. Useful when you need multi-table JOIN / GROUP BY / window functions /
  CTEs / PIVOT inside the secondary side.
- **`where` field disabled** — the per-request PG-side `where` parameter on
  `/dwh` was retired for SQL-injection safety. Use the OGC `/join` with
  `primary_filter` (CQL2) for primary-side filtering.
- **MVT tiling** — the tiled join endpoint clips features to a tile bbox and
  returns Mapbox Vector Tile (MVT/PBF) content. The `/join` extension has no
  tiled variant yet.
- **No OGC conformance** — `/dwh` is not an OGC API endpoint and declares no
  conformance URIs.

Migration path: replace `POST /dwh/catalogs/{id}/join` with
`POST /join/catalogs/{id}/collections/{col}/join` using a
`BigQuerySecondarySpec` secondary, plus `secondary.filter` and
`primary_filter` in CQL2. Drop to `/dwh` only when the secondary needs raw
BQ SQL beyond CQL2's expressive range — and even then, prefer baking the
SQL into a BigQuery view so the OGC `/join` body stays standard.

---

## Packaging

```toml
joins = ["google-cloud-bigquery", "db-dtypes", "pandas"]
dwh   = ["dynastore[joins]"]
```

`pip install dynastore[dwh]` installs both `/dwh` and `/join` endpoints.
`pip install dynastore[joins]` installs `/join` only.

Both endpoints additionally need `module_gcp` for BigQuery support — the
`maps` service includes it via `dwh_grp`.

---

## Known limitations

1. **Inner join only** — features in the primary stream with no matching row
   in the secondary index are dropped. LEFT JOIN semantics are planned but
   not yet implemented.
2. **Secondary materialized in memory** — the full secondary source is indexed
   before streaming the primary. Use `secondary.filter` (BQ) or pre-filter at
   source for very large secondary tables.
3. **JSONB property filtering** — CQL2 currently filters on top-level relational
   columns (the field-mapping the PG driver advertises). Per-collection custom
   attributes inside the `properties` JSONB blob (e.g. `ADM2_PCODE`,
   `validTo`) require a per-collection field-mapping to be filterable —
   tracked separately.
4. **No paging on secondary** — `NamedSecondarySpec` materializes all secondary
   rows without a limit.
5. **BigQuerySecondarySpec credentials** — per-request BQ targets use
   `CloudIdentityProtocol` (ADC); Secret-wrapped per-collection credentials
   are not yet supported for secondary specs (Phase 4e).
6. **No streaming output formats** — GeoPackage and Parquet outputs
   accumulate the full result in memory before writing.
7. **No CQL2-side spatial GEOGRAPHY validation** — spatial CQL2 ops on a BQ
   table that doesn't have a GEOGRAPHY column surface as a BigQuery error
   from `BigQueryService.execute_query`, not a 400 from the joins handler.

