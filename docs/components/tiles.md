# The Tiles Extension

The `tiles` extension provides **OGC API – Tiles** for DynaStore. It generates
Mapbox Vector Tiles (MVT / PBF) on-the-fly from PostGIS and also serves
TMS-aligned raster map tiles for raster collections. Vector tiles use a
two-level cache (L1 in-process LRU, L2 GCS or PostGIS bucket) and full CQL2
filtering; rendered raster tiles use the shared tile storage provider.

For arbitrary-bbox raster maps see [maps.md](maps.md).
For **PMTiles** (planned) see the PMTiles section of [maps.md](maps.md).

---

## URL structure

```
/tiles/tileMatrixSets                                     list built-in + custom TMS
/tiles/tileMatrixSets/{tmsId}                             TMS definition
/tiles/{dataset}/tileMatrixSets                           create custom TMS (POST)
/tiles/{dataset}/tiles/{z}/{x}/{y}.mvt                    vector tile (WebMercatorQuad default)
/tiles/catalogs/{dataset}/tiles/{z}/{x}/{y}.mvt           catalog-centric, default TMS
/tiles/{dataset}/tiles/{tmsId}/{z}/{x}/{y}.{format}       vector tile with explicit TMS
/tiles/catalogs/{dataset}/tiles/{tmsId}/{z}/{x}/{y}.{fmt} catalog-centric with TMS
/tiles/catalogs/{dataset}/collections/{collection}/map/tiles/{tmsId}/{z}/{x}/{y}.png
/tiles/catalogs/{dataset}/collections/{collection}/styles/{style}/map/tiles/{tmsId}/{z}/{x}/{y}.png
/tiles/{dataset}/tiles/cache                              invalidate tile cache (DELETE)
```

Formats accepted: `mvt`, `pbf` (aliases; both return
`application/vnd.mapbox-vector-tile`).

---

## Tile generation pipeline

```
GET /tiles/{dataset}/tiles/{tmsId}/{z}/{x}/{y}.mvt
        │
        ├─ 1. Config check (TilesConfig — enabled flag, cache settings)
        ├─ 2. L1 cache lookup (in-process LRU, ttl=60 s, jitter=5 s)
        ├─ 3. L2 cache lookup (GCS or PostGIS provider — redirect or proxy)
        ├─ 4. Validate TMS + matrix + tile bounds
        ├─ 5. Resolve SRID (pyproj + catalog CRS registry)
        ├─ 6. Resolve collection metadata (tiles_module, cached)
        ├─ 7. PostGIS ST_AsMVT query (tiles_db.get_features_as_mvt_filtered)
        ├─ 8. Background: save tile to L2 provider
        └─ 9. Return MVT bytes (ETag, Cache-Control via WebModuleProtocol)
```

Key files:

| File | Responsibility |
|---|---|
| `extensions/tiles/tiles_service.py` | FastAPI router, cache orchestration |
| `modules/tiles/tiles_db.py` | PostGIS `ST_AsMVT` query builder |
| `modules/tiles/tiles_module.py` | TMS registry, SRID resolution, storage SPI |
| `modules/tiles/tms_definitions.py` | Built-in TMS definitions (WebMercatorQuad, WorldCRS84Quad) |
| `modules/tiles/tiles_config.py` | `TilesConfig`, `TilesPreseedConfig` |
| `modules/tiles/tiles_models.py` | `TileMatrixSet`, `TileMatrixSetRef`, `Link` |
| `extensions/tiles/presets/__init__.py` | Policy registration via `PolicyContributorPreset` (`tiles_public_access` → `anonymous`) |

---

## TileMatrixSet registry

The registry is shared across both the Tiles and Maps extensions via
`modules/tiles/tiles_module.py`. Two built-in TMS are always available:

| ID | CRS | Zoom levels | Origin |
|---|---|---|---|
| `WebMercatorQuad` | EPSG:3857 | 0–24 | Top-left, Mercator |
| `WorldCRS84Quad` | CRS84 (EPSG:4326) | 0–22 | Top-left, geographic |

Custom TMS can be registered per-dataset (`POST /tiles/{dataset}/tileMatrixSets`)
and are stored in the catalog DB. The Maps extension resolves TMS through the
same registry; it does **not** maintain a separate copy.

---

## Caching

### L1 — in-process LRU

The live MVT render path uses an in-process LRU cache with a 60 second TTL
and jitter. Cache variation includes the resolved collections, TMS, target
SRID, z/x/y, temporal filter, CQL filter, `filter-lang`, `filter-crs`,
subset, simplification, and simplification algorithm. The database connection
is excluded from the key.

### L2 — storage provider

Configured via `TilesPreseedConfig.storage_priority` (default `["bucket",
"pg"]`). Providers:

| Priority | Provider | Mechanism |
|---|---|---|
| `bucket` | GCS (`modules/gcp/`) | Signed URL redirect (307) or proxy |
| `pg` | PostGIS tile table | Direct byte fetch |

Cache is populated asynchronously via `background_tasks.add_task(provider.save_tile, ...)` after every L2 miss.

Raster map-tile routes render COG-backed collections through rio-tiler and then
write rendered bytes through the same `TileStorageProtocol`. The COG source is
resolved through routed item storage first; if a raster collection has no item
row available to the renderer, collection-level `assets` or `item_assets` with a
`data` or `coverage` href can provide the raster source. A requested
`style-url=<http-or-https SLD URL>` or a preserved source `rel=sld` link
participates in the render cache key, so changing the stylesheet URL produces a
separate cached tile set.

### L2 bucket layout — `TilesCachingConfig`

The bucket-backed cache's object-key prefix and per-object `Cache-Control`
TTL are surfaced through the standard PluginConfig waterfall
(`PUT /configs/plugins/tiles_caching_config`):

| Field | Default | Effect |
|---|---|---|
| `enabled` | `true` | Master switch for the L2 bucket cache. When `false`: every read returns as a miss, `save_tile` is a no-op (tiles still serve from PostGIS; deletes still execute). For "disable tile serving entirely" use `TilesConfig.enabled`. |
| `key_prefix` | `tiles/collections` | Object key is `{key_prefix}/{collection_id}/{tms_id}/{z}/{x}/{y}.{format}` |
| `ttl_seconds` | `31536000` (1 year) | `Cache-Control: public, max-age=<ttl_seconds>` on every tile object |

Bucket selection is auto-derived per-catalog from
`StorageProtocol.ensure_storage_for_catalog(catalog_id)` and is not
operator-tunable here — exposing an override would break the
per-catalog isolation invariants enforced by the catalog lifecycle.
Changing `key_prefix` orphans existing cached tiles (they stay under
the old prefix until the bucket TTL evicts them).

### Cache observability — response headers + structured logs

Every tile response advertises whether it was served from cache:

| Response | `X-Tile-Cache` | `X-Tile-Source` |
|---|---|---|
| Bucket signed-URL redirect (307) | `hit` | `bucket_redirect` |
| Bucket proxy bytes (200) | `hit` | `bucket_proxy` |
| PMTiles archive fallback (200) | `hit` | `pmtiles_archive` |
| PostGIS generation (200, bytes) | `miss` | `postgis` |
| PostGIS generation (204, empty) | `miss` | `postgis` |

INFO-level structured logs are emitted on the same logger
(`dynastore.extensions.tiles.tiles_service`) in the project-standard
`event=… key=value …` shape:

```
tile_cache event=hit  source=bucket_proxy     catalog=… collection=… z=… x=… y=… duration_ms=… bytes=…
tile_cache event=miss source=postgis          catalog=… collection=… z=… x=… y=… duration_ms=… bytes=…
```

### Cache invalidation

```
DELETE /tiles/{dataset}/tiles/cache?collections=col1,col2
```

Omitting `collections` invalidates the entire catalog. The handler calls
`tms_manager.invalidate_collection_tiles` or
`tms_manager.invalidate_catalog_tiles`, which fan out to all registered
storage providers.

---

## Filtering

| Parameter | Type | Description |
|---|---|---|
| `collections` | `str` | Comma-separated collection IDs (required) |
| `datetime` | `str` | ISO 8601 temporal filter |
| `filter` | `str` | CQL2 filter expression |
| `filter-lang` | `str` | `cql2-text` (default) or `cql2-json` |
| `filter-crs` | `str` | CRS URI for geometry literals in `filter`; defaults to CRS84 / EPSG:4326 semantics |
| `subset` | `str` | Custom dimension filter |
| `simplification` | `float` | Douglas-Peucker tolerance |
| `simplification_by_zoom` | `str` | JSON `{zoom: tolerance}` map |
| `simplification_algorithm` | `enum` | `topology_preserving` (default) \| `douglas_peucker` |
| `disable_cache` | `bool` | Skip all cache layers |
| `refresh_cache` | `bool` | Invalidate tile before fetching |

---

## OGC conformance declared

```
http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/core
http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tileset
http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tilesets-list
http://www.opengis.net/spec/tms/2.0/conf/tilematrixset
http://www.opengis.net/spec/tms/2.0/conf/json-tilematrixset
http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/mvt
```

---

## AssetContributor link

When a STAC or Features item is served, `TilesService.contribute()` appends a
vector tile XYZ template link to the asset link set:

```json
{
  "key": "vector_tiles",
  "href": "/tiles/{catalog_id}/tiles/{z}/{x}/{y}.mvt?collections={collection_id}",
  "title": "Vector Tiles (MVT)",
  "media_type": "application/vnd.mapbox-vector-tile",
  "roles": ["tiles"]
}
```

---

## Relation to other extensions

```
Tiles ── reads geometry from ──► PostGIS (tiles_db → ST_AsMVT)
      ── owns TMS registry   ──► modules/tiles/tiles_module  ◄── also used by Maps
      ── L2 cache store      ──► modules/gcp (GCS) + PostGIS
      ── CQL2 filtering      ──► pygeofilter[backend-sqlalchemy]
      ── SRID resolution     ──► pyproj + modules/crs

Maps  ── delegates TMS to    ──► modules/tiles/tiles_module (shared)
      ── renders raster from ──► same geometry fetch path (maps_db)
```
