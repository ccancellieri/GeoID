# The STAC Extension

The `stac` extension provides a SpatioTemporal Asset Catalog (STAC) compliant interface. Its philosophy is to act as a rich, standardized, **read-only** discovery layer on top of the data managed by the core modules.

It is not intended for data modification. It purely exists to enable powerful search and discovery workflows for STAC-aware clients.

> See also: [STAC Catalog Harvest](stac-harvest.md) for bulk ingestion of items from remote STAC catalogs into the data layer this extension exposes.

## The `stac_generator`

The core logic of this extension resides in the `stac_generator`. Its operations manage the translation from Agro-Informatics Platform (AIP) - Catalog Services's internal, generic models to STAC-compliant JSON objects via the `pystac` standard library.

### Workflow Example (`get_stac_collection`)
1. Fetches the generic `Collection` model by calling `catalog_module.get_collection`.
2. Executes parallel calls against `shared_queries` to fetch granular, dynamic DB queries computing true real-time spatial and temporal extents (bypassing static declarations).
3. Punts everything to the generator. `pystac` builds a valid `Collection` map object merging dynamic endpoints and applying correct JSON schemas linking back into `self` and `root` objects establishing navigating structure.

## STAC Search — Spatial Filter Implementation

`POST /stac/search` supports `bbox` and `intersects` filters. These are implemented as raw SQL clauses that reference the `geom` column in the geometry sidecar. Because the `QueryOptimizer` only JOINs a sidecar when its fields appear in `SELECT`, `WHERE`, or `ORDER BY`, the search layer forces the geometry sidecar into the query plan by appending a cheap selection:

```python
FieldSelection(field="geom", transformation="ST_SRID", alias="_srid")
```

`ST_SRID` is included in the `ALLOWED_TRANSFORMATIONS` allowlist in `query_builder.py`. Removing it from the allowlist (or forgetting to add it) causes all spatial searches to return `400 Bad Request`.

## The `asset_factory`
A key feature of the generator is the `add_dynamic_assets` function. This is a forward-looking mechanism for service chaining.

If additional extensions are active in the system environment (for example, a `tiles` API):
1. The generator iterates the context before finalize.
2. Identifies a physical data pipeline capability.
3. Automatically synthesizes a direct JSON HTTP link `asset` node in the collection root pointing directly at `.../tiles/{collection_id}/{z}/{x}/{y}`.

This creates self-assembling ecosystems where client scanners can autonomously figure out all ways to interact with data representations.
