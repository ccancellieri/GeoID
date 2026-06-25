# The DGGS Extension

## Implementation Status

**OGC Conformance**: Part 1

**Status**: ✅ Production-Ready

**Known Gaps**:
- [Antimeridian handling for H3](https://github.com/un-fao/GeoID/issues/XXXX) - Cells straddling antimeridian produce incorrect bboxes

**Last Updated**: 2025-06-26

The `dggs` extension implements **OGC API – Discrete Global Grid Systems
(DGGS) Part 1** for DynaStore. It aggregates vector collection features into
DGGS zones (hexagonal H3 or quadrilateral S2 cells) and returns per-zone
statistics in GeoJSON.

Two DGGRS (Discrete Global Grid Reference Systems) are supported:

| ID | Library | Cell shape | Notes |
|---|---|---|---|
| `H3` | `h3` | Hexagonal | Hierarchical, 0–15 resolutions |
| `S2` | `s2sphere` | Quadrilateral | Hierarchical, 0–30 levels |

---

## URL structure

```
/dggs/                                                          landing page
/dggs/conformance                                               OGC conformance
/dggs/dggs-list                                                 list DGGRS
/dggs/dggs-list/{dggsId}                                        DGGRS metadata
/dggs/collections                                               all DGGS-capable collections
/dggs/catalogs/{catalog_id}/collections/{collection_id}/dggs            aggregate to zones
/dggs/catalogs/{catalog_id}/collections/{collection_id}/dggs/{zoneId}   single-zone query
```

OGC conformance declared:
```
https://www.opengis.net/spec/ogcapi-dggs-1/1.0/conf/core
https://www.opengis.net/spec/ogcapi-dggs-1/1.0/conf/data-retrieval
https://www.opengis.net/spec/ogcapi-dggs-1/1.0/conf/zone-query
```

---

## Request parameters

| Parameter | Alias | Default | Description |
|---|---|---|---|
| `dggs-id` | `dggs_id` | `H3` | DGGRS selector: `H3` or `S2` |
| `zone-level` | `zone_level` | 5 | Cell resolution (H3: 0–15, S2: 0–30) |
| `bbox` | — | none | Spatial filter `xmin,ymin,xmax,ymax` WGS-84 |
| `datetime` | — | none | ISO 8601 temporal filter |
| `parameter-name` | `parameter_name` | none | Comma-separated property names to aggregate |

`DGGSConfig` (per-catalog/per-collection) governs:

```python
default_resolution: int = 5
max_resolution: int = 10
max_features_per_request: int = 10_000
```

---

## Aggregation pipeline

```
GET /dggs/catalogs/{cat}/{col}/dggs?dggs-id=H3&zone-level=5&bbox=...
        │
        ├─ 1. Parse bbox → build_query_for_bbox() (GIST scan) or
        │                   build_global_query() (no filter)
        ├─ 2. CatalogsProtocol.search_items() → List[GeoJSON Feature]
        │     (bounded by max_features_per_request)
        ├─ 3. aggregate_features():
        │     Fast path: read pre-computed sidecar column h3_resN / s2_resN (BIGINT)
        │     Slow path: extract centroid → compute cell ID on-the-fly
        │     → count + numeric mean per cell
        └─ 4. Build DGGSFeatureCollection (GeoJSON polygon per cell)
```

For the `/{zoneId}` endpoint an indexed B-tree lookup on the sidecar column is
used when available (`build_query_for_zone_indexed`), falling back to a GIST
bbox scan derived from the cell boundary.

Key files:

| File | Responsibility |
|---|---|
| `extensions/dggs/dggs_service.py` | FastAPI router, request orchestration (priority 170) |
| `extensions/dggs/config.py` | `DGGSConfig` — resolution limits, max features |
| `modules/dggs/aggregator.py` | In-memory zone aggregation (fast/slow path) |
| `modules/dggs/h3_indexer.py` | H3 cell arithmetic (lazy-imports `h3`) |
| `modules/dggs/s2_indexer.py` | S2 cell arithmetic (lazy-imports `s2sphere`) |
| `modules/dggs/zone_query.py` | `QueryRequest` builders for PostGIS |
| `modules/dggs/models.py` | `DGGRSInfo`, `DGGSFeature`, `DGGSFeatureCollection` |

---

## Output

`application/geo+json` — `DGGSFeatureCollection` (GeoJSON FeatureCollection).
Each feature represents one DGGS zone and carries:

```json
{
  "type": "Feature",
  "geometry": { "type": "Polygon", "coordinates": [...] },
  "properties": {
    "zone_id": "8928308280fffff",
    "resolution": 5,
    "count": 42,
    "avg_ndvi": 0.63
  }
}
```

---

## Dependencies

| Package | Extra | Purpose |
|---|---|---|
| `h3` | `extension_dggs` | H3 hexagonal indexing |
| `s2sphere` | `extension_dggs` | S2 quadrilateral indexing |

Both packages are lazy-imported so the extension loads without them when
`extension_dggs` is not installed; requests will fail at runtime with a clear
import error.

---

## Known limitations

1. **No antimeridian handling for H3** — `build_query_for_zone` computes bbox
   from H3 polygon vertices using `min/max`. Cells straddling the antimeridian
   produce incorrect bounding boxes. S2 is correct (`rect_bound_for_cell` with
   full-range fallback).
2. **Temporal filter supports all interval formats (PR pending)** — open intervals
   (`"2024-01-01/.."`) and other interval formats are now supported at the DB layer
   (awaiting merge).
3. **`bbox_to_cells` utilities unused** — both indexers implement this helper
   but the service uses GIST bbox scans with post-filtering instead of
   covering-cell pre-computation.
4. **`get_collections` not filtered** — it lists all collections regardless of
   whether they have geometry.
5. **Aggregation is in-process** — all features are loaded into Python memory
   before aggregation; the only bound is `max_features_per_request` (default
   10 000).
