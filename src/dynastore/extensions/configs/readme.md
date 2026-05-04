# DynaStore Configurations Extension

The Configurations Extension provides a unified, hierarchical system for managing settings across the entire DynaStore application. It allows administrators to define configuration defaults at a global (platform) level, override them for specific catalogs, and further specialize them for individual collections.

This system is pluggable, meaning any other extension or module can register its own configuration model, making its settings automatically manageable through this central API. The extension is designed to be extensible, allowing any module or other extension to register its own settings and make them centrally manageable.

## Core Architecture

The configuration system is built on two key modules: `db_config` and `catalog`.

### The Foundation: `db_config` Module

The `db_config` module provides the fundamental building blocks for the entire system:

*   **`PluginConfig`**: A Pydantic `BaseModel` that all configuration models must inherit from.
*   **`ConfigRegistry`**: A central, in-memory registry that maps a unique string ID (the `plugin_id`) to its corresponding `PluginConfig` model. This allows the system to validate and instantiate configuration objects dynamically.
*   **`@register_config(plugin_id)`**: A decorator that automatically registers a `PluginConfig` model in the `ConfigRegistry`.
*   **`PlatformConfigManager`**: A manager responsible for storing and retrieving global, platform-wide configuration defaults in the `platform_configs` database table.
*   **`Immutable` Annotation**: A marker used with `typing.Annotated` to flag certain configuration fields as immutable. The framework will prevent changes to these fields once a configuration has been saved, which is critical for settings that define physical infrastructure (e.g., a table's partitioning scheme).

### The Hierarchy: `catalog` Module Extension

The `catalog` module builds upon this foundation by introducing a hierarchical lookup mechanism. It defines its own `ConfigManager` that orchestrates configuration resolution in the following order of precedence:

1.  **Collection Level**: Settings specific to a single collection (e.g., `my_catalog:my_collection`), stored in `collection_configs`.
2.  **Catalog Level**: Settings that apply to all collections within a catalog (e.g., `my_catalog`), stored in `catalog_configs`.
3.  **Platform Level**: Global default settings managed by `PlatformConfigManager`.
4.  **Code Default**: The default values defined in the Pydantic model for the plugin's configuration.

When a configuration is requested for a specific collection, the `ConfigManager` traverses this hierarchy, merging settings to produce the final, effective configuration object.

## Use Cases

### Extending the System with a Custom Configuration

Any extension can add its own manageable settings by defining a Pydantic model and registering it.

For example, imagine a new "caching" extension that needs to control TTL values. It would define a model like this:

```python
# in my_caching_extension/caching_config.py
from pydantic import Field
from dynastore.modules.db_config.platform_config_manager import PluginConfig, register_config

# Define a unique ID for this configuration plugin
CACHING_PLUGIN_ID = "caching"

@register_config(CACHING_PLUGIN_ID)
class CachingConfig(PluginConfig):
    """Configuration for the Caching Extension."""
    ttl_seconds: int = Field(default=3600, description="Default cache Time-To-Live in seconds.")
    max_size_mb: int = Field(default=1024, description="Maximum cache size in megabytes.")

# Now, other parts of the application can get this configuration:
# from dynastore.modules.catalog import catalog_module
#
# async def some_caching_logic(catalog_id: str, collection_id: str):
#     config_service = catalog_module.get_config_service()
#     # This will resolve the config through the Collection > Catalog > Platform hierarchy
#     caching_config = await config_service.get_config(CACHING_PLUGIN_ID, catalog_id, collection_id)
#     # caching_config will be an instance of CachingConfig
#     print(f"Using cache TTL: {caching_config.ttl_seconds}")
```

### Discovering Available Configurations via API

The `configs` extension exposes a registry that lists every config class with its JSON Schema, description, and scope. This is invaluable for administration and for building dynamic user interfaces.

**1. List all registered config classes:**

```bash
curl http://localhost:8000/configs/registry
```
**Response (snippet):**
```json
{
  "caching_config": {
    "json_schema": {
      "title": "CachingConfig",
      "description": "Configuration for the Caching Extension.",
      "type": "object",
      "properties": {
        "enabled": {"default": true, "type": "boolean"},
        "ttl_seconds": {
          "description": "Default cache Time-To-Live in seconds.",
          "default": 3600,
          "type": "integer"
        }
      }
    },
    "description": "Configuration for the Caching Extension.",
    "scope": "platform_waterfall"
  }
}
```

The top-level keys are `class_key`s (snake_case derived from the Pydantic class name; see PR #140's snake_case identity cutover). `scope` is one of `platform_waterfall`, `collection_intrinsic`, or `deployment_env`.

**2. Get JSON Schema + description for one class:**

```bash
curl http://localhost:8000/configs/registry/caching_config
```

**3. List runtime driver instances grouped by Protocol:**

```bash
curl http://localhost:8000/configs/storage/drivers
```
Returns `{ProtocolQualname: {ClassName: DriverInfo}}` for `CollectionItemsStore`, `AssetStore`, and `CollectionStore` — capabilities, supported operations, hints, availability — for the operator's driver picker.

## Quick Start — PostgreSQL Defaults

The most common setup is a catalog with PostgreSQL for all storage.
Use a single `PATCH` to configure many plugins at once. Body shape is `{plugin_id: payload | null}` per RFC 7396 merge-patch semantics — a `null` value deletes the override at that scope. The same endpoint shape works at platform (`PATCH /configs/`), catalog (`PATCH /configs/catalogs/{id}`), and collection (`PATCH /configs/catalogs/{id}/collections/{id}`) scope.

### 1. Configure a new catalog

```bash
curl -X PATCH http://localhost:8000/configs/catalogs/my_catalog \
  -H "Content-Type: application/json" \
  -d '{
  "routing": {
    "enabled": true,
    "operations": {
      "WRITE": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
      "READ":  [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}]
    }
  },
  "routing_assets": {
    "enabled": true,
    "operations": {
      "WRITE": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
      "READ":  [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}]
    }
  },
  "stac": {
    "enabled": true,
    "enabled_extensions": [],
    "asset_tracking": {"enabled": true, "access_mode": "DIRECT"}
  },
  "tiles":    {"enabled": true, "min_zoom": 0, "max_zoom": 12},
  "features": {"enabled": true},
  "tasks":    {"enabled": true, "queue_poll_interval": 30.0}
}'
```

> Use the registry (`GET /configs/registry`) to confirm the exact `plugin_id` keys accepted by your build — they are the snake_case `class_key`s of the registered Pydantic configs.

### 2. Configure a new collection with PG driver

```bash
curl -X PATCH http://localhost:8000/configs/catalogs/my_catalog/collections/my_collection \
  -H "Content-Type: application/json" \
  -d '{
  "driver:postgresql": {
    "enabled": true,
    "collection_type": "VECTOR",
    "sidecars": [
      {
        "sidecar_type": "geometries",
        "enabled": true,
        "target_srid": 4326,
        "target_dimension": "force_2d",
        "geom_column": "geom",
        "bbox_column": "bbox_geom",
        "invalid_geom_policy": "attempt_fix",
        "srid_mismatch_policy": "transform"
      },
      {
        "sidecar_type": "attributes",
        "enabled": true,
        "storage_mode": "automatic",
        "enable_external_id": true,
        "enable_asset_id": true,
        "versioning_behavior": "UPDATE_EXISTING_VERSION"
      }
    ],
    "partitioning": {"enabled": false, "partition_keys": []}
  },
  "routing": {
    "enabled": true,
    "operations": {
      "WRITE": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
      "READ":  [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}]
    }
  },
  "stac": {
    "enabled": true,
    "enabled_extensions": [],
    "asset_tracking": {"enabled": true, "access_mode": "DIRECT"}
  }
}'
```

To remove an override at this scope (and fall back to the catalog/platform value), send `null`:

```bash
curl -X PATCH http://localhost:8000/configs/catalogs/my_catalog/collections/my_collection \
  -H "Content-Type: application/json" \
  -d '{"stac": null}'
```

## API Endpoints

This extension provides a RESTful API for managing configurations at all three levels of the hierarchy (Platform, Catalog, and Collection):

*   **CRUD** — Per-plugin `GET` / `PUT` / `DELETE` at `/configs/plugins/{plugin_id}` (platform), `/configs/catalogs/{id}/plugins/{plugin_id}` (catalog), `/configs/catalogs/{id}/collections/{id}/plugins/{plugin_id}` (collection).
*   **Composed views** — `GET /configs/`, `GET /configs/catalogs/{id}`, `GET /configs/catalogs/{id}/collections/{id}` return waterfall-resolved configs as a tree.
*   **Multi-plugin partial write** — `PATCH /configs/`, `PATCH /configs/catalogs/{id}`, `PATCH /configs/catalogs/{id}/collections/{id}` apply RFC 7396 merge-patch semantics; a `null` value deletes the override.
*   **Discovery** — `GET /configs/registry` lists all registered config classes; `GET /configs/registry/{class_key}` returns JSON Schema + description for one; `GET /configs/storage/drivers` lists runtime driver instances grouped by Protocol qualname.

For the full interactive documentation, see `/docs`.