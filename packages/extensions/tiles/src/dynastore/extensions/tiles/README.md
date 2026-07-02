# OGC API - Tiles Extension

This extension provides standardized access to geospatial vector and map tiles following the [OGC API - Tiles](https://ogcapi.ogc.org/tiles/) specification.

## 🏗️ Tiles Pre-seeding Process

Tile pre-seeding is a background process that generates and stores tiles in advance to ensure low-latency serving and high performance.

### ⚙️ Configuration

Pre-seeding is configured via the `tiles_preseed` plugin configuration (ID: `tiles_preseed`).

#### [TilesPreseedConfig](src/dynastore/modules/tiles/tiles_config.py)
| Field | Type | Description |
|-------|------|-------------|
| `enabled` | `bool` | Enables/disables the pre-seeding task (Default: `True`). |
| `target_tms_ids` | `List[str]` | TMS IDs to pre-seed (e.g., `["WebMercatorQuad"]`). |
| `formats` | `List[str]` | Output formats (e.g., `["mvt"]`). |
| `bboxes` | `List[BBox]` | Optional spatial subsets to limit seeding. If unset, the bbox defaults to the collection's STAC spatial extent (not the whole world) and the zoom range is capped — see `preseed_max_zoom`. |
| `storage_priority` | `List[str]` | **Deprecated, no-op** — kept only so existing configs keep validating. Storage selection now lives on [`TilesCachingConfig.writers`](src/dynastore/modules/tiles/tiles_config.py) (see below). |
| `collections_to_preseed` | `List[str]` | Specific collections to seed (optional). |
| `preseed_max_zoom` | `Optional[int]` | Caps the max zoom preseeded when no explicit bbox is configured (default cap: 8). |
| `preseed_tile_timeout_seconds` | `int` | Per-tile PostgreSQL statement timeout during preseeding (Default: `30`). A tile that exceeds this is skipped, not fatal to the run. |

#### [TilesCachingConfig.writers](src/dynastore/modules/tiles/tiles_config.py) — where tiles are cached

`writers` is an ordered list of typed tile-cache writer configs. At most ONE writer is ever active: `tiles_writers.select_tile_writer` picks the first AVAILABLE writer in list order (a factory registered for this image's SCOPE, enabled, and its target resolves) — this is what replaces `storage_priority`. An unavailable earlier candidate (e.g. no `StorageProtocol` registered for its scheme) is skipped and logged at INFO, rather than silently caching nothing.

Built-in writers:
- `PgTileWriterConfig` (core) — the per-catalog PostgreSQL `preseeded_tiles` table. Always available, no external dependency.
- `GcsTileWriterConfig` (`gcp` module) — any registered `gs://` `StorageProtocol` backend. `bucket`/`prefix` fields target an operator-supplied bucket; omitted, it uses the catalog's own managed bucket.
- A local-disk equivalent is available in `modules/local` (`LocalTileWriterConfig` + `LocalStorageOps`, `file://` scheme) for on-prem deployments — not wired into any module's lifespan by default.

`writers` unset/empty (default) auto-injects `[GcsTileWriterConfig(), PgTileWriterConfig()]` when the `gcp` module registered a `StorageProtocol` provider, else just `[PgTileWriterConfig()]` — reproducing the old `storage_priority=["bucket", "pg"]` default exactly, zero migration required.

### 🚀 How to Execute

There are two primary ways to trigger the pre-seeding process:

#### 1. Via OGC API - Processes (Web API)
If the `processes` and `tiles` extensions are both enabled, you can trigger the execution via a POST request:

**Endpoint:** `POST /processes/tiles-preseed/execution`

**Example Payload:**
```json
{
  "inputs": {
    "catalog_id": "my_dataset",
    "collection_id": "my_layer",
    "update_bbox": [10.0, 40.0, 15.0, 45.0]
  }
}
```

#### 2. Via Command Line (Task Runner)
You can run the pre-seeding task directly using the DynaStore task runner. This is useful for scheduled jobs or manual interventions.

**Usage:**
```bash
python -m dynastore.main_task tiles-preseed '{"catalog_id": "my_dataset", "collection_id": "my_layer"}'
```

The task will:
1. Load the `tiles_preseed` configuration for the specified `catalog_id`.
2. Intersect the requested `update_bbox` with configured bounds.
3. Generate tiles for all Zoom levels (between `min_zoom` and `max_zoom`).
4. Save the generated tiles to the preferred storage (e.g., Google Cloud Storage bucket).
