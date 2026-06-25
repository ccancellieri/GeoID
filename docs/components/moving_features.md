# The Moving Features Extension

## Implementation Status

**OGC Conformance**: Part 1

**Status**: ⚠️ Partial Implementation

**Known Gaps**:
- [Spatial queries on trajectories](https://github.com/un-fao/GeoID/issues/2401) - No bbox/geometry filter on tgsequence endpoints
- [FK constraint on mf_id](https://github.com/un-fao/GeoID/issues/XXXX) - No database-level foreign key (cascade in service layer)

**Last Updated**: 2025-06-26

The `moving_features` extension implements **OGC API – Moving Features Part 1**
(approved February 2026) for DynaStore. It stores and retrieves features whose
position changes over time as sequences of (timestamp, coordinate) pairs.

---

## URL structure

```
/movingfeatures/                                                           landing page
/movingfeatures/conformance                                                OGC conformance
/movingfeatures/catalogs/{catalog_id}/collections                          list MF collections
/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}          collection metadata
/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items    list / create features
/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items/{mf_id}           get / delete feature
/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}/items/{mf_id}/tgsequence  list / add trajectory
```

OGC conformance declared:
```
http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/core
http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/mf-collection
http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/tgsequence
```

---

## Data model

Two PostgreSQL tables in the `moving_features` schema, both partitioned by
`LIST(catalog_id)` and created on extension startup:

**`moving_features.moving_features`**

| Column | Type | Description |
|---|---|---|
| `id` | `UUID` | Auto-generated primary key |
| `catalog_id` | `VARCHAR` | Partition key |
| `collection_id` | `VARCHAR` | Owning collection |
| `feature_type` | `VARCHAR` | Always `"Feature"` |
| `properties` | `JSONB` | Arbitrary feature metadata |
| `created_at` | `TIMESTAMPTZ` | Creation timestamp |
| `updated_at` | `TIMESTAMPTZ` | Set on insert; never updated by current code |

**`moving_features.temporal_geometries`**

| Column | Type | Description |
|---|---|---|
| `id` | `UUID` | Auto-generated primary key |
| `mf_id` | `UUID` | Parent moving feature (no FK constraint) |
| `catalog_id` | `VARCHAR` | Partition key |
| `datetimes` | `TIMESTAMPTZ[]` | Ordered timestamps of trajectory points |
| `coordinates` | `JSONB` | Ordered position array (parallel to datetimes) |
| `crs` | `VARCHAR` | Default `OGC/1.3/CRS84` |
| `trs` | `VARCHAR` | Default ISO-8601/Gregorian |
| `interpolation` | `VARCHAR` | `Linear`, `Step`, `Quadratic`, `Cubic` |
| `properties` | `JSONB` | Optional per-sequence metadata |

---

## API operations

| Method | Path suffix | Description |
|---|---|---|
| `GET` | `/items` | Paginated list (`limit`, `offset`) |
| `POST` | `/items` | Create feature — body: `{ feature_type, properties }` |
| `GET` | `/items/{mf_id}` | Retrieve single feature |
| `DELETE` | `/items/{mf_id}` | Delete feature + all its temporal geometries |
| `GET` | `/items/{mf_id}/tgsequence` | List trajectory sequences (filterable by `dt_start`, `dt_end`) |
| `POST` | `/items/{mf_id}/tgsequence` | Add a trajectory sequence |

No `PUT`/`PATCH` endpoints exist — the API is currently append-only (plus
DELETE).

### Temporal geometry filter

`GET /tgsequence?dt_start=…&dt_end=…` uses:

```sql
EXISTS (SELECT 1 FROM unnest(datetimes) AS t WHERE t >= :dt_start AND t <= :dt_end)
```

This returns sequences that overlap the window but returns the **full**
`datetimes` + `coordinates` arrays, not a clipped subset.

---

## Write pipeline

### Create moving feature

```
POST /movingfeatures/.../items
        │
        ├─ 1. validate_sql_identifier(catalog_id, collection_id)
        ├─ 2. _require_catalog_ready(catalog_id)
        ├─ 3. catalog_module.get_collection() → 404 if missing
        ├─ 4. ensure_partition_exists("moving_features") — on-demand LIST partition
        └─ 5. mf_db.create_moving_feature() → INSERT … RETURNING * → 201
```

### Add temporal geometry

```
POST /movingfeatures/.../tgsequence
        │
        ├─ 1. Validate len(datetimes) == len(coordinates) → 400 if mismatch
        ├─ 2. mf_db.get_moving_feature() → 404 if missing / wrong collection
        ├─ 3. ensure_partition_exists("temporal_geometries")
        └─ 4. mf_db.create_temporal_geometry() → INSERT … RETURNING * → 201
```

Key files:

| File | Responsibility |
|---|---|
| `extensions/moving_features/mf_service.py` | FastAPI router, orchestration (priority 100) |
| `extensions/moving_features/config.py` | `MovingFeaturesPluginConfig` (only `enabled` flag) |
| `modules/moving_features/models.py` | `MovingFeature`, `TemporalGeometry`, `InterpolationEnum` |
| `modules/moving_features/db.py` | All SQL as `DQLQuery`/`DDLQuery` objects |
| `modules/moving_features/mf_module.py` | Startup DDL — schema + partitioned table creation |

---

## Query parameters

| Parameter | Endpoint | Type | Description |
|---|---|---|---|
| `limit` | list collections, list items | `int` (1–1000, default 100) | Page size |
| `offset` | list collections, list items | `int` (≥ 0, default 0) | Page offset |
| `dt_start` | list tgsequence | ISO 8601 datetime | Filter sequences with any timestamp ≥ dt_start |
| `dt_end` | list tgsequence | ISO 8601 datetime | Filter sequences with any timestamp ≤ dt_end |

---

## Known limitations

1. **No FK constraint on `mf_id`** — temporal geometries reference their
   parent via `mf_id` (UUID) but no database-level foreign key exists. The
   cascade on feature delete is implemented manually in the service layer and
   can be bypassed by direct DB writes or mid-transaction failure.
2. **`updated_at` never updated** — the column is set on insert and no `UPDATE`
   path exists; it is not meaningful for tracking modifications.
3. **No PUT/PATCH** — feature properties and trajectory sequences cannot be
   corrected after creation; only DELETE is available.
4. **Temporal array not clipped on query** — `tgsequence` temporal filters use
   `EXISTS … unnest(datetimes)` but return complete arrays. Clients must
   post-filter to extract positions within the requested window.
5. **Coordinates stored as JSONB** — trajectory points are not in a PostGIS
   `GEOMETRY` type, so PostGIS spatial indexes cannot be used for proximity or
   bbox queries on trajectories.
6. **Partition DDL on every write** — `ensure_partition_exists` is called on
   each create/append. Already-existing partitions make this a no-op, but it
   adds one DDL round-trip per write.
7. **`MovingFeaturesService` not exported from `__init__.py`** — inconsistent
   with DGGS and EDR which both export their service class from `__init__.py`.
8. **No spatial query on tgsequence** — no bbox or geometry filter is available
   when listing temporal geometries for a feature.
