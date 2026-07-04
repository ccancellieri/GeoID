# The Records Extension

The `records` extension implements **OGC API – Records Part 1** for DynaStore.
Records are geometry-less features (documents, metadata items, catalogue
entries) stored in standard vector collections whose `collection_type` is set
to `"RECORDS"`.

---

## URL structure

```
/records/                                                                       landing page
/records/conformance                                                            OGC conformance
/records/catalogs/{catalog_id}/collections                                      list collections
/records/catalogs/{catalog_id}/collections/{collection_id}                      collection metadata
/records/catalogs/{catalog_id}/collections/{collection_id}/items                list / create records
/records/catalogs/{catalog_id}/collections/{collection_id}/items/{record_id}    get single record
/records/dimensions                                                             list dimension-backed collections (platform tier)
/records/dimensions/{dim_id}                                                    dimension collection metadata (platform tier)
/records/dimensions/{dim_id}/items                                             list dimension members as records (platform tier)
/records/dimensions/{dim_id}/items/{record_id}                                 get single dimension member as a record (platform tier)
```

Dimension-backed RECORDS collections are materialized under the internal
`_dimensions_` sentinel catalog (see `dynastore.models.dimensions.DIMENSIONS_CATALOG_ID`),
which is platform-tier data, not a real per-tenant catalog. The `/records/dimensions/...`
routes above are the canonical, genuine platform-tier shape for reaching them — the
sentinel catalog id never appears in a link or a documented URL (#2957). The generic
`/records/catalogs/{catalog_id}/...` routes still technically resolve requests made
with `catalog_id=_dimensions_` for existing consumers, but every self/collection link
returned by the API always uses the `/records/dimensions/...` shape regardless of
which path was used to reach it.

OGC conformance declared:
```
http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/core
http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/record-core
http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/record-collection
http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/json
http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/geojson
http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/sorting
```

---

## Data model

Records are stored as standard `Feature` rows with `geometry = NULL` in the
existing PostgreSQL vector storage tables. No separate schema is created.

The `collection_type = "RECORDS"` flag lives in the collection's driver
configuration (not in a DB column), so the Records extension filters
collections at the application layer by inspecting each collection's driver
config.

---

## Request parameters

**List records** (`GET .../items`):

| Parameter | Type | Default | Description |
|---|---|---|---|
| `limit` | int | 10 | Max records returned (1–1000) |
| `offset` | int | 0 | Pagination offset |
| `filter` | str | none | CQL2-TEXT filter expression |
| `sortby` | str | none | Sort order, e.g. `-title,+created` |
| `q` | str | none | Free-text search on `title` (ILIKE) |

**List collections** (`GET .../collections`):

| Parameter | Type | Default | Description |
|---|---|---|---|
| `limit` | int | 100 | Max collections returned (1–1000) |
| `offset` | int | 0 | Pagination offset |
| `language` | str | Accept-Language | Localisation hint |

---

## Request pipeline

### GET .../items

```
GET /records/catalogs/{cat}/{coll}/items?limit=10&offset=0&q=ndvi
        │
        ├─ 1. CatalogsProtocol.get_collection(cat, coll) → 404 if missing
        ├─ 2. parse_ogc_query_request(filter, sortby, limit, offset)
        │     → if q: append `title ILIKE '%ndvi%'` CQL2 filter
        ├─ 3. ItemsProtocol.stream_items(cat, coll, request_obj,
        │                                consumer=ConsumerType.OGC_RECORDS)
        │     → async iterable of Features (geometry=null)
        ├─ 4. For each Feature: records_generator.db_row_to_record()
        │     → extracts time from validity/valid_from/valid_to/start_datetime/end_datetime
        │     → builds Record with self + collection links
        └─ 5. RecordCollection (GeoJSON FeatureCollection, geometry always null)
```

### POST .../items

```
POST /records/catalogs/{cat}/{coll}/items
        │   body: single record dict | list of dicts | FeatureCollection
        │
        ├─ 1. Normalise to list; set geometry=None on all items
        ├─ 2. OGCTransactionMixin._ingest_items()
        │     → validate write policy → upsert via storage driver
        └─ 3. 207 Partial Content if any rejections; 201 Created otherwise
```

Key files:

| File | Responsibility |
|---|---|
| `extensions/records/records_service.py` | Router, orchestration (priority 150) |
| `extensions/records/records_models.py` | `Record`, `RecordProperties`, `RecordCollection`, `RecordsCatalogCollection` |
| `extensions/records/records_generator.py` | `db_row_to_record()`, `collection_to_records_collection()` |
| `extensions/records/config.py` | `RecordsPluginConfig` (`enabled` flag only) |
| `extensions/records/policies.py` | `records_public_access` policy (GET + OPTIONS) |

---

## Output

Always `application/geo+json` (GeoJSON FeatureCollection with `geometry: null`).
No content-negotiation for other encodings.

---

## Dependencies

No additional pip packages. Reuses:
- `models.protocols.CatalogsProtocol` / `ItemsProtocol`
- `extensions.features.features_service.parse_ogc_query_request`
- `extensions.tools.transaction.OGCTransactionMixin._ingest_items`
- `modules.storage.drivers.pg_sidecars.base.ConsumerType`

---

## Known limitations

1. **Free-text search is title-only** — `?q=` generates `title ILIKE '%value%'`;
   other record properties are not searched.
2. **`?q=` string escaping** — the sanitised value is interpolated into CQL2
   text (`'` → `''`, `%` → `\%`, `_` → `\_`) rather than using parameterised
   binding; safe in practice but fragile.
3. **N+1 on list_collections** — `_is_records_collection()` calls
   `get_driver()` + `get_driver_config()` individually for each collection;
   large catalogs incur N+1 config lookups.
4. **No DELETE or PATCH** — individual records cannot be deleted or partially
   updated via the Records API.
5. **No spatial filter** — `bbox` is passed as `None` in
   `parse_ogc_query_request`; spatial queries are not supported.
6. **`cube:dimensions` link injection truncated** — `collection_to_records_collection()`
   breaks after the first dimension in a multi-dimension collection.
7. **`RecordsCatalogCollection.crs` never populated** — the model field exists
   but the generator never sets it.
