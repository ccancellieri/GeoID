# The 3D GeoVolumes Extension

## Implementation Status

**OGC Conformance**: Working Draft (Part 1)

**Status**: 🚧 Working Draft

**Known Gaps**:
- [Config resolution from ConfigsProtocol](https://github.com/un-fao/GeoID/issues/2404) - Per-collection config not wired up
- [Persistent tile cache](https://github.com/un-fao/GeoID/issues/2407) - In-memory only, lost on restart
- [EmptyBoundsSource default](https://github.com/un-fao/GeoID/issues/XXXX) - SidecarBoundsSource must be explicitly registered

**Last Updated**: 2025-06-26

The `volumes` extension implements a working-draft of **OGC API – 3D GeoVolumes
Part 1** for DynaStore. It serves 3D tile content (B3DM + glTF 2.0) derived
from feature geometry stored in PostGIS, suitable for consumption by Cesium,
deck.gl, and other 3D Tiles 1.0 clients.

---

## URL structure

```
/volumes/                                                                        landing page
/volumes/conformance                                                             OGC conformance
/volumes/catalogs/{catalog_id}/collections                                       list 3D containers
/volumes/catalogs/{catalog_id}/collections/{collection_id}                       single 3D container
/volumes/catalogs/{catalog_id}/collections/{collection_id}/cityjsonseq           CityJSONSeq stream
/volumes/catalogs/{catalog_id}/collections/{collection_id}/3dtiles/tileset.json  Cesium tileset descriptor
/volumes/catalogs/{catalog_id}/collections/{collection_id}/3dtiles/tiles/{id}.b3dm  B3DM tile content
/volumes/catalogs/{catalog_id}/collections/{collection_id}/3dtiles/tiles/{id}.glb   GLB tile content
/volumes/catalogs/{catalog_id}/collections/{collection_id}/3dtiles/metadata      collection metadata + links
```

OGC conformance declared:
```
http://www.opengis.net/spec/ogcapi-3d-geovolumes-1/0.0/conf/core
http://www.opengis.net/spec/ogcapi-3d-geovolumes-1/0.0/conf/3dtiles
http://www.opengis.net/spec/ogcapi-3d-geovolumes-1/0.0/conf/tileset
http://www.opengis.net/spec/ogcapi-3d-geovolumes-1/0.0/conf/spatialquery
```

---

## Configuration

**`VolumesConfig`** (`extensions/volumes/config.py`) — per-collection tunables:

| Field | Default | Description |
|---|---|---|
| `max_features_per_tile` | 10 000 | Octree leaf capacity |
| `max_tree_depth` | 20 | Maximum octree depth |
| `on_demand_cache_ttl_s` | 3600 | In-memory tileset cache TTL |
| `default_height_attr` | `"height"` | Feature attribute used for extrusion height |
| `root_geometric_error` | 500.0 | Cesium geometric error at the root tile |
| `refinement_ratio` | 4.0 | Error factor between successive tree levels |
| `default_extrusion_height` | 10.0 | Fallback height when `default_height_attr` is absent |
| `supported_formats` | `["b3dm", "glb"]` | Tile formats advertised in `tileset.json` |

---

## Tile pipeline

### `tileset.json`

```
GET /volumes/.../tileset.json
        │
        ├─ 1. BoundsSourceProtocol.stream_bounds(catalog, collection)
        │     → async iterator of (feature_id, x_min, x_max, y_min, y_max, z_min, z_max)
        ├─ 2. tileset_builder.build_tileset()
        │     → recursive octree partitioning (widest-axis midpoint split)
        │     → Cesium 3D Tiles 1.0 JSON descriptor with content URIs
        └─ 3. StreamingResponse (application/json) — result cached in-memory for TTL
```

### B3DM / GLB tile

```
GET /volumes/.../tiles/{tile_id}.b3dm (or .glb)
        │
        ├─ 1. tileset_builder.find_leaf(tile_id) → leaf node + feature ID list
        ├─ 2. GeometryFetcherProtocol.fetch_geometries(feature_ids)
        │     → WKB geometry + height attribute per feature
        ├─ 3. mesh_builder.build_mesh_from_geometries()
        │     → shapely WKB → extruded triangle meshes (centroid-fan triangulation)
        ├─ 4. writers.glb.pack_glb()
        │     → POSITION + index accessors → glTF 2.0 Binary payload
        └─ 5a. writers.b3dm.pack_b3dm()   → B3DM envelope (feature table + GLB)
           5b. (or return GLB directly for .glb requests)
```

---

## Protocol seams

Two `BoundsSourceProtocol` implementations are available:

| Implementation | Registration | Data source |
|---|---|---|
| `EmptyBoundsSource` | Default (always active) | Returns empty stream; tileset root has no children |
| `SidecarBoundsSource` | Opt-in via `register_sidecar_bounds_source()` at startup | Queries `ST_XMin/XMax/YMin/YMax/ZMin/ZMax` on `ST_Force3D(geom)` joined through the PostGIS geometries sidecar |

When `SidecarBoundsSource` is registered, it also registers a companion
`SidecarGeometryFetcher` that resolves WKB geometry and the
`default_height_attr` column for mesh construction.

---

## Key files

| File | Responsibility |
|---|---|
| `extensions/volumes/volumes_service.py` | FastAPI router; tileset caching; pipeline orchestration (priority 170) |
| `extensions/volumes/config.py` | `VolumesConfig` per-collection tunables |
| `extensions/volumes/platform_bounds_source.py` | Factory registering `SidecarBoundsSource` + `SidecarGeometryFetcher` at startup |
| `modules/volumes/tileset_builder.py` | Recursive octree partitioning; `tileset.json` JSON descriptor |
| `modules/volumes/bounds.py` | `FeatureBounds` dataclass + 3D bbox merge |
| `modules/volumes/sidecar_bounds.py` | PostGIS-backed `BoundsSourceProtocol` |
| `modules/volumes/geometry_fetcher.py` | PostGIS-backed `GeometryFetcherProtocol` |
| `modules/volumes/mesh_builder.py` | WKB → extruded triangle mesh (shapely ≥ 2.0) |
| `modules/volumes/writers/glb.py` | Triangle mesh → GLB binary (glTF 2.0) |
| `modules/volumes/writers/b3dm.py` | GLB → B3DM envelope (3D Tiles 1.0) |
| `modules/volumes/writers/tileset_json.py` | Tileset dict → streamed JSON |
| `models/protocols/bounds_source.py` | `BoundsSourceProtocol` interface |
| `models/protocols/geometry_fetcher.py` | `GeometryFetcherProtocol` interface |

---

## CityJSON ingestion

Collections that carry CityJSON provenance extras (`cityjson:version` or `geovolumes:enabled`)
are exposed as 3D containers via the GeoVolumes Core API. The `cityjson_ingest` module:

- Parses CityJSONSeq (`.city.jsonl`) and plain CityJSON (`.city.json`) files.
- Dequantizes vertices and reprojects to WGS84 (EPSG:4326) using the header's EPSG code.
- Maps each CityObject to a DynaStore item with `geometry` (MultiPolygon footprint),
  `citygml_type`, `lod`, `height`, `zmin`, `zmax`, and raw `cityjson` blob in extras.
- Bulk-creates items in configurable batches (best-effort; failures recorded as warnings).
- Raises `ValueError` before any DB work when the header carries no usable EPSG code.

The `geovolumes_demo` preset seeds a `demo-3d / denhaag` catalog with the Den Haag LoD2
CityJSON dataset (2498 buildings, EPSG:7415). 3D Tiles are served at runtime by the
volumes extension — no offline task is enqueued.

---

## Globe browser page

A MapLibre GL + deck.gl globe page is served at `/web/volumes/volumes_browser.html`.
It fetches the GeoVolumes container listing, renders 3D bounding-box extents as extruded
polygons (deck.PolygonLayer), loads 3D Tiles for the selected container (deck.Tile3DLayer),
and overlays building footprints from the Features API (deck.GeoJsonLayer).

---

## Known limitations

1. **Octree only** — space partitioning is axis-aligned midpoint split on the
   widest axis. BSP and grid schemes are not available.
2. **In-memory tileset cache** — cached per service instance; lost on restart.
   No shared cache (Redis/GCS) yet.
3. **Config not resolved from ConfigsProtocol** — `VolumesConfig` is
   constructed with defaults at request time; per-collection config stored in
   the platform jsonb is not yet wired up.
4. **Z-up CRS assumed** — no reprojection; consumers must handle CRS alignment
   in the Cesium scene.
5. **Axis-aligned bounding boxes only** — oriented bounding boxes (lower
   geometric error) are a planned optimisation.
6. **EmptyBoundsSource is the default** — the tileset returns an empty root
   unless `SidecarBoundsSource` is explicitly registered at deployment startup.
7. **Memory-backed tile mesh** — each tile request re-fetches geometry from
   PostGIS; no tile-level persistent cache.
