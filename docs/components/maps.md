# The Maps Extension

The `maps` extension is DynaStore's unified **spatial data serving** layer. Its
goal is to expose the same underlying vector and raster data through three
complementary tile formats over a single, coherent URL tree:

| Output kind | Format | Use case |
|---|---|---|
| Raster map (arbitrary bbox) | PNG / JPEG / GeoTIFF | WMS-style map image for legacy clients |
| Raster tile (TMS-aligned) | PNG / JPEG | Tiled background layer for web maps |
| Vector tile | MVT / PBF | Interactive, client-side-styled web maps |
| Cloud-optimised tile archive | PMTiles | Serverless tile distribution from GCS/S3 |

---

## URL structure

```
/maps/                                         landing page
/maps/{dataset}                                dataset overview
/maps/{dataset}/map                            raster map (bbox-driven)
/maps/{dataset}/map/tiles                      list available TileMatrixSets
/maps/{dataset}/map/tiles/{tmsId}              TileMatrixSet definition
/maps/{dataset}/map/tiles/{tmsId}/{z}/{x}/{y}  raster tile
```

Vector tile (`/tiles`) and PMTiles (`/tiles/{id}.pmtiles`) endpoints live in
the **Tiles extension** (see [tiles.md](tiles.md)) but share the same
`TileMatrixSet` registry managed by `modules/tiles/tiles_module.py`.

---

## Raster rendering pipeline

```
GET /maps/{dataset}/map?collections=...&bbox=...&crs=...&width=...&height=...
        │
        ├─ 1. Validate dataset + collections (catalog_module)
        ├─ 2. Resolve bbox CRS (pyproj / modules/crs)
        ├─ 3. Render vector collections from geometries or raster collections from a COG asset
        ├─ 4. Fetch style from the Styles extension or a source-linked SLD URL
        ├─ 5. Render PNG (renderer.render_map_image or rio-tiler, depending on collection kind)
        └─ 6. Convert PNG → JPEG | GeoTIFF (format_convert)
```

Key files:

| File | Responsibility |
|---|---|
| `extensions/maps/maps_service.py` | FastAPI router, request orchestration |
| `extensions/maps/maps_db.py` | PostGIS geometry fetch optimised for rendering |
| `extensions/maps/renderer.py` | GDAL/OGR synchronous render (run in process pool) |
| `extensions/maps/format_convert.py` | PNG → JPEG / GeoTIFF post-processing |
| `extensions/maps/maps_config.py` | Per-catalog Maps config (enabled, defaults) |
| `extensions/maps/policies.py` | Policy registration (auth, rate limits) |

Rendering is CPU-bound; the service keeps a module-level `ProcessPoolExecutor`
so it never blocks the async event loop.

---

## Raster tiling

```
GET /maps/{dataset}/map/tiles/{tmsId}/{z}/{x}/{y}
        │
        ├─ 1. Resolve TileMatrixSet (built-in + custom DB via tms_module)
        ├─ 2. Compute tile BBOX from TMS origin + cell size
        ├─ 3. Validate collections
        ├─ 4. Fetch geometries (maps_db) at tile resolution
        └─ 5. Render PNG (same pipeline as /map)
```

TileMatrixSets built in: `WebMercatorQuad` (EPSG:3857) and
`WorldCRS84Quad` (CRS84). Custom TMS can be registered per-dataset via the
Tiles extension (`POST /tiles/{dataset}/tileMatrixSets`).

OGC conformance declared:
```
http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/core
http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/dataset-map
http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/styled-map
http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/png
http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/jpeg
http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/geotiff
http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/tilesets-map
```

---

## PMTiles

PMTiles archives (`application/x-pmtiles`) are referenced in
`modules/gdal/service.py` as a known raster MIME type and appear in the
`template/` asset template as a `pmtile` metadata source.  A dedicated serving
endpoint is **not yet implemented**; the planned route is:

```
GET /maps/{dataset}/tiles/{collection}.pmtiles          full archive
GET /maps/{dataset}/tiles/{collection}.pmtiles/{z}/{x}/{y}  single tile
```

The PMTiles byte-range protocol is served without a custom server: the client
fetches ranges directly from a GCS-signed URL (redirect model) or the endpoint
proxies the ranges. Both patterns are supported by the existing GCS storage
provider in `modules/gcp/`.

---

## Styling

The Maps extension integrates with the **Styles extension**
(`extensions/styles/`) for optional per-render styling. Supported stylesheets:

- `SLD_1.1` — parsed and applied by the GDAL renderer
- `MapboxGL` — passed through to the renderer for client-side fallback

Raster map requests also accept `style-url=<http-or-https SLD URL>`. Raster
rendering resolves the source COG through routed item storage, so an
Elasticsearch-only or otherwise optimized read driver can provide the item when
the PostgreSQL item driver is not active for the collection. If no item row is
available, raster rendering can still use collection-level `assets` or
`item_assets` with a `data` or `coverage` href. When `style-url` is omitted,
the same source lookup is used to find a preserved `rel=sld` link or SLD-looking
style asset. The fetched SLD body is cached through the platform cache backend,
so deployments with Valkey use Valkey for that style-body cache. The rendered
map or tile bytes remain cached through the normal tile storage path.

If no style is requested the renderer uses default fill/stroke rules from
`MapsConfig`.

---

## Known architectural issues

### 1. TMS endpoint duplication

`maps_service.py` registers its own `/map/tiles` and `/map/tiles/{tmsId}`
routes that re-implement the same TMS lookup already provided by
`TilesService`. Both call `tms_manager.list_custom_tms` and
`BUILTIN_TILE_MATRIX_SETS` directly. The Maps routes should delegate to the
shared `modules/tiles/tiles_module.py` functions rather than duplicating the
lookup logic.

### 2. PMTiles gap

`modules/gdal/service.py` declares `application/x-pmtiles` as a known MIME
type and the asset template emits PMTiles metadata URLs, but there is no HTTP
endpoint in Maps (or Tiles) that actually serves byte-range requests or
redirects to the GCS archive.

### 3. ProcessPoolExecutor per-extension

The render pool is owned by `MapsService` class-level state. If Maps is
deployed alongside a high-concurrency Tiles workload, the pool competes for
CPU. By default each Gunicorn worker creates at most
`floor(process_cpu_count / GUNICORN_WORKERS)` render processes, with a minimum
of one. Set `DYNASTORE_MAPS_PROCESS_POOL_WORKERS` to override that per-worker
pool size. Consider extracting the pool into a shared rendering module if Maps
and Tiles need one global render budget.

---

## Filtering

The `/map/tiles/{tmsId}/{z}/{x}/{y}` endpoint accepts the same query
parameters as the vector tile endpoint:

| Parameter | Type | Description |
|---|---|---|
| `collections` | `str` | Comma-separated collection IDs (required) |
| `datetime` | `str` | ISO 8601 temporal filter |
| `subset` | `str` | Custom dimension filter (`key=value`) |
| `style` | `str` | Style name (resolved via Styles extension) |
| `bgcolor` | `str` | Background colour for opaque renders |
| `transparent` | `bool` | Transparent background (default `true`) |

---

## Relation to other extensions

```
Maps ──── reads geometry from ──► PostGIS (via maps_db)
     ──── resolves TMS from   ──► modules/tiles/tiles_module
     ──── resolves style from ──► modules/styles/db
     ──── resolves CRS from   ──► modules/crs / pyproj
     ──── renders via         ──► modules/gdal (GDAL 3.12)

Tiles ─── reads geometry from ──► PostGIS (via modules/tiles/tiles_db)
      ─── owns TMS registry   ──► modules/tiles/tiles_module
      ─── serves MVT/PBF only

PMTiles ─ planned             ──► modules/gcp GCS redirect
```
