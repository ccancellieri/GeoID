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
| `bboxes` | `List[BBox]` | Optional spatial subsets to limit seeding. |
| `storage_priority` | `List[str]` | Priority of storage providers (e.g., `["bucket", "pg"]`). |
| `collections_to_preseed` | `List[str]` | Specific collections to seed (optional). |

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
