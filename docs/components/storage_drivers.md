# Multi-Driver Storage Abstraction

Entity-level storage abstraction that routes catalog/collection/items/asset data to pluggable backends.
Each driver is a self-contained module with its own connection lifecycle, location config, and
capabilities. Drivers are discovered via entry points and selected at runtime through an
**operation-based routing system** (post-PR-#261) — one routing config per tier (items / collection /
asset / catalog), each mapping `Operation` (WRITE / READ / SEARCH / INDEX / BACKUP / UPLOAD) to an
ordered list of drivers.

## Architecture

```
REST API
    │
    ▼
get_driver(operation, catalog_id, collection_id, hint=...)
    │
    ├── ItemsRoutingConfig         — items-tier dispatch (per Operation)
    ├── CollectionRoutingConfig    — collection-envelope dispatch
    ├── AssetRoutingConfig         — asset-tier dispatch
    └── CatalogRoutingConfig       — catalog-tier dispatch
            │
            └── operations: { Operation: [OperationDriverEntry, ...] }
                              ▲
                              │ first-match wins per (operation, hint)
    ▼
Drivers (instances discovered via `dynastore.modules` entry points)
    ├── ItemsPostgresqlDriver                   driver_ref="items_postgresql_driver"
    ├── ItemsElasticsearchDriver                driver_ref="items_elasticsearch_driver"             (public, per-tenant index)
    ├── ItemsElasticsearchPrivateDriver         driver_ref="items_elasticsearch_private_driver"     (DENY-policied)
    ├── CollectionPostgresqlDriver              driver_ref="collection_postgresql_driver"
    ├── CollectionElasticsearchDriver           driver_ref="collection_elasticsearch_driver"
    ├── CollectionElasticsearchPrivateDriver    driver_ref="collection_elasticsearch_private_driver"
    ├── CatalogPostgresqlDriver                 driver_ref="catalog_postgresql_driver"
    ├── CatalogElasticsearchDriver              driver_ref="catalog_elasticsearch_driver"
    ├── AssetPostgresqlDriver                   driver_ref="asset_postgresql_driver"
    ├── AssetElasticsearchDriver                driver_ref="asset_elasticsearch_driver"
    ├── ItemsIcebergDriver                 driver_ref="items_iceberg_driver"              (OTF: snapshots, time-travel)
    └── ItemsDuckdbDriver                  driver_ref="items_duckdb_driver"               (analytical reads)
```

`driver_ref` is always `_to_snake(cls.__name__)` — the snake_case class key (post-PR-1e).

### Key Design Decisions

- **Operation-based routing** (post-PR-#261): each tier has its own routing config; each operation
  carries an ordered list of `OperationDriverEntry`. The dispatcher picks the first entry whose hints
  match (or the first entry overall when no hint is supplied).
- **Streaming-first**: read paths return `AsyncIterator[Feature]` — O(1) memory regardless of result size.
- **Entity-level abstraction**: drivers exchange typed Pydantic models (`Feature`, `FeatureCollection`),
  not raw SQL or engine-specific queries.
- **Capability declaration**: drivers declare what they support via `Capability` enum. The router can
  validate capability before dispatching.
- **Lazy initialization**: connections (DuckDB pool, Iceberg catalog) are created on first use.
- **Async fan-out via outbox** (post-PR-#261): non-fatal `INDEX` writes land in the per-tenant
  `storage_outbox` table and the `OutboxDrainTask` consumer dispatches them asynchronously.

## Quick Start

```python
from dynastore.modules.storage.router import get_driver
from dynastore.modules.storage.routing_config import Operation
from dynastore.modules.storage.hints import Hint

# Read — Operation + Hint selects the right backend
driver = await get_driver(Operation.READ, catalog_id, collection_id, hint=Hint.GEOMETRY_SIMPLIFIED)
async for feature in driver.read_entities(catalog_id, collection_id, request=query):
    process(feature)

# Write — first WRITE entry from ItemsRoutingConfig.operations[WRITE] (durability primary)
driver = await get_driver(Operation.WRITE, catalog_id, collection_id)
written = await driver.write_entities(catalog_id, collection_id, feature_collection)

# Capability gate (OTF-specific methods)
from dynastore.models.protocols.storage_driver import Capability
if Capability.TIME_TRAVEL in driver.capabilities:
    async for feature in driver.read_at_snapshot(catalog_id, collection_id, snapshot_id):
        ...
```

## CollectionStorageDriverProtocol

Defined in `dynastore.models.protocols.storage_driver`. All input/output types are Pydantic models.

### Core Methods

| Method | Input Types | Output Type | Description |
|--------|------------|-------------|-------------|
| `write_entities` | `Feature \| FeatureCollection \| Dict \| List[Dict]` | `List[Feature]` | Insert or upsert entities |
| `read_entities` | `QueryRequest`, entity_ids, limit, offset | `AsyncIterator[Feature]` | Stream entities with optional filters |
| `delete_entities` | `List[str]` entity IDs, `soft: bool` | `int` (count) | Hard or soft delete by ID |
| `ensure_storage` | catalog_id, collection_id | `None` | Create backing storage if it doesn't exist |
| `drop_storage` | catalog_id, collection_id, `soft: bool` | `None` | Remove or tag storage as deleted |
| `export_entities` | format, target_path | `str` (path) | Export collection to file |

### OTF Methods (Iceberg-specific, capability-gated)

| Method | Description | Required Capability |
|--------|-------------|---------------------|
| `list_snapshots` | List snapshot history | `SNAPSHOTS` |
| `create_snapshot` | Create named snapshot/branch | `SNAPSHOTS` |
| `rollback_to_snapshot` | Rollback to a previous snapshot | `SNAPSHOTS` |
| `read_at_snapshot` | Time-travel read at specific snapshot | `TIME_TRAVEL` |
| `read_at_timestamp` | Time-travel read at specific datetime | `TIME_TRAVEL` |
| `evolve_schema` | Add/rename/drop/promote columns | `SCHEMA_EVOLUTION` |
| `get_schema_history` | Schema version history | `SCHEMA_EVOLUTION` |

### Capabilities

```python
from dynastore.models.protocols.storage_driver import Capability

class Capability(StrEnum):
    READ_ONLY       = "read_only"
    STREAMING       = "streaming"
    SPATIAL_FILTER  = "spatial_filter"
    FULLTEXT        = "fulltext"
    EXPORT          = "export"
    TIME_TRAVEL     = "time_travel"
    SOFT_DELETE     = "soft_delete"
    VERSIONING      = "versioning"
    SCHEMA_EVOLUTION = "schema_evolution"
    SNAPSHOTS       = "snapshots"
```

## Routing Configs (per tier)

Each tier resolves its routing via the 4-level `ConfigsProtocol` waterfall (collection > catalog >
platform > code defaults). The class key is the snake_case form (e.g. `items_routing_config`).

```jsonc
// items_routing_config — entity-row dispatch
// Defaults: PG fatal WRITE, PG READ, ES public WRITE async/outbox, ES public INDEX
{
  "operations": {
    "WRITE": [
      {"driver_ref": "items_postgresql_driver", "on_failure": "fatal"},
      {"driver_ref": "items_elasticsearch_driver",
       "write_mode": "async", "on_failure": "outbox", "source": "auto"}
    ],
    "READ":  [
      {"driver_ref": "items_elasticsearch_driver", "hints": ["geometry_simplified"]},
      {"driver_ref": "items_postgresql_driver",     "hints": ["geometry_exact"]}
    ],
    "INDEX": [
      {"driver_ref": "items_elasticsearch_driver",
       "write_mode": "async", "on_failure": "outbox", "source": "auto"}
    ]
  }
}

// collection_routing_config — collection-envelope dispatch (CollectionStore drivers)
{
  "operations": {
    "WRITE": [{"driver_ref": "collection_postgresql_driver", "on_failure": "fatal"}],
    "READ":  [{"driver_ref": "collection_postgresql_driver"}],
    "INDEX": [{"driver_ref": "collection_elasticsearch_driver",
               "write_mode": "async", "on_failure": "outbox", "source": "auto"}]
  }
}

// Privacy-pinned items routing — collection.is_private=True (Cycle E.2)
// items_elasticsearch_private_driver substitutes for the public driver in INDEX/SEARCH.
// PG remains the durable WRITE target.
{
  "operations": {
    "WRITE": [
      {"driver_ref": "items_postgresql_driver", "on_failure": "fatal"},
      {"driver_ref": "items_elasticsearch_private_driver",
       "write_mode": "async", "on_failure": "outbox"}
    ],
    "READ":  [{"driver_ref": "items_postgresql_driver"}]
  }
}
```

### `OperationDriverEntry` fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `driver_ref` | `str` (snake_case) | required | Snake_case `_to_snake(cls.__name__)` of a registered driver |
| `hints` | `List[Hint]` | `[]` | Selectivity tags — `Hint` is a closed `StrEnum` (see below) |
| `on_failure` | `FailurePolicy` | `"warn"` | `"fatal"` (raise), `"warn"` (log), `"outbox"` (defer to drain task) |
| `write_mode` | `WriteMode` | `"sync"` | `"sync"` or `"async"`; async writes go through the outbox |
| `source` | `Literal["operator", "auto"]` | `"operator"` | `"auto"` marks self-registered entries from the apply handler |

### Apply-time auto-registration

Each routing config has an apply handler (`_on_apply_items_routing_config` etc.) that:

1. Validates every `driver_ref` against the discovery registry for the tier's protocol
   (`CollectionItemsStore`, `CollectionStore`, `AssetStore`, `CatalogStore`).
2. Auto-registers `*Indexer` drivers under `operations[INDEX]` and `*Store` drivers under
   `operations[SEARCH]` when discoverable but missing from the persisted payload — with
   `source="auto"` so operators can distinguish self-registered defaults from explicit pins.
3. Calls `ensure_storage(catalog_id, collection_id)` on every referenced driver (idempotent).
4. Invalidates the per-tier router cache.

### `Hint` (closed StrEnum)

```python
from dynastore.modules.storage.hints import Hint

# Selectivity tags. Live values include:
Hint.GEOMETRY_EXACT       # PG / Iceberg — full-precision geometry
Hint.GEOMETRY_SIMPLIFIED  # ES — pre-simplified geometry
Hint.AGGREGATION          # SEARCH-side aggregation pipeline
Hint.FEATURES             # OGC Features
Hint.OBFUSCATED           # private/obfuscated index
# ... see src/dynastore/modules/storage/hints.py for the full catalog.
```

Hints are not registerable at runtime (they're a closed enum, post PR #255). To add a hint, extend
`Hint` in `hints.py` and the driver advertises it via `supported_hints: ClassVar[FrozenSet[Hint]]`.

## Router Performance

Resolution is cached at 300s TTL (aligned with config cache) via `@cached(maxsize=4096, ttl=300)`.
The driver index (`driver_ref -> instance`) is rebuilt from the discovery registry on each lookup.
Cache invalidation is wired into every routing apply handler so config writes are visible immediately
to subsequent reads.

---

## Driver Reference

### PostgreSQL Driver (items)

| Property | Value |
|----------|-------|
| **class** | `ItemsPostgresqlDriver` |
| **driver_ref** | `items_postgresql_driver` |
| **capabilities** | `STREAMING`, `SPATIAL_FILTER`, `SOFT_DELETE`, `EXPORT`, `REQUIRED_ENFORCEMENT`, `UNIQUE_ENFORCEMENT` |
| **driver config** | `ItemsPostgresqlDriverConfig` (class_key `items_postgresql_driver_config`) |
| **dependencies** | Core (always available) |

Owns SQL for the per-tenant items table (and its sidecars). The `ItemsPostgresqlDriver`
implements `CollectionItemsStore` directly and is the source-of-truth for entity-row
WRITE operations (durability primary). PostGIS, streaming, and per-collection sidecars
(geometry, attributes, item_metadata, stac_metadata) are owned by this driver.

```
write_entities  → upsert into per-tenant items table + sidecars (single TX)
read_entities   → streaming SELECT with bounded fetchmany() (PostGIS for spatial)
delete_entities → DELETE (hard) or soft-delete sidecar flag
ensure_storage  → CREATE TABLE / partition + register sidecar columns
drop_storage    → DROP partition or set deleted flag
```

Routing entries reference the snake_case `driver_ref`; locations are derived per-collection
via the catalog tenant schema (no explicit location config required).

### Iceberg Driver (collection-tier OTF)

| Property | Value |
|----------|-------|
| **class** | `ItemsIcebergDriver` |
| **driver_ref** | `items_iceberg_driver` |
| **capabilities** | `STREAMING`, `SPATIAL_FILTER`, `EXPORT`, `TIME_TRAVEL`, `VERSIONING`, `SNAPSHOTS`, `SCHEMA_EVOLUTION`, `SOFT_DELETE` |
| **driver config** | `ItemsIcebergDriverConfig` |
| **dependencies** | `pyiceberg[sql-postgres]>=0.9.0`, `pyarrow>=14.0.0` |

Full Open Table Format support via PyIceberg: ACID transactions, snapshots, time-travel reads,
and schema evolution — all backed by a real Iceberg catalog.

**Catalog strategy:**
- **Default**: PostgreSQL-backed `SqlCatalog` using the platform's `DATABASE_URL` (zero config)
- Uses `pyiceberg.catalog.CatalogType` enum and PyIceberg constants (`TYPE`, `URI`, `WAREHOUSE_LOCATION`) — no string literals
- PyIceberg creates lightweight metadata tables (`iceberg_tables`, `iceberg_namespace_properties`) — no conflict with DynaStore schema
- Other catalog types supported via `CatalogType`: `REST`, `GLUE`, `HIVE`, `DYNAMODB`, `BIGQUERY`, `IN_MEMORY`

**Warehouse auto-resolution** (via `_resolve_warehouse()` → `_ensure_catalog()`):

1. **Explicit** — `warehouse_uri` field in `ItemsIcebergDriverConfig` (manual override)
2. **Auto-detected** — from platform `StorageProtocol` (e.g., GCS bucket). When a collection already has a GCP bucket, the driver derives `gs://bucket/.../iceberg/` automatically
3. **Fallback** — local temp dir (`file:///tmp/iceberg_warehouse`)

When the warehouse URI starts with `gs://`, the driver injects `GCS_PROJECT_ID` from environment for PyArrowFileIO. The same pattern applies for future `s3://` support.

```
write_entities       → PyArrow append to Iceberg table
read_entities        → PyIceberg scan with expression filters
delete_entities      → Positional deletes (soft) or delete+compact (hard)
ensure_storage       → Create namespace + table with schema
drop_storage         → Drop table or tag with dynastore.deleted property
export_entities      → Scan → write to parquet/csv/json
list_snapshots       → table.history() → SnapshotInfo list
read_at_snapshot     → scan(snapshot_id=...) → time-travel read
read_at_timestamp    → Find snapshot at timestamp → delegate to read_at_snapshot
evolve_schema        → table.update_schema() with add/rename/drop/type-promote
```

**DynaStore hierarchy mapping:**
- DynaStore catalog → Iceberg namespace
- DynaStore collection → Iceberg table
- DynaStore entity → Iceberg row

**Location config (minimal — warehouse auto-resolved from GCS bucket):**
```json
{
  "driver": "iceberg",
  "catalog_type": "sql",
  "namespace": "analytics"
}
```

**Location config (explicit warehouse override):**
```json
{
  "driver": "iceberg",
  "catalog_name": "production",
  "catalog_type": "sql",
  "catalog_uri": "postgresql+psycopg2://user:pass@host:5432/db",
  "warehouse_uri": "gs://my-bucket/iceberg/",
  "namespace": "analytics",
  "table_name": "observations"
}
```

When `catalog_uri` is omitted and `catalog_type` is `"sql"`, the driver auto-resolves from `DBConfig.database_url`.

**Production deployment:**
```
PostgreSQL ─── Iceberg SQL Catalog (metadata: table locations, snapshots, schemas)
    │
    └── Warehouse Storage (data files: Parquet)
         ├── Local filesystem (dev/test)
         ├── S3 (AWS production)
         └── GCS (GCP production)
```

**Install:**
```bash
pip install dynastore[module_storage_iceberg]
```

### DuckDB Driver (collection-tier analytical)

| Property | Value |
|----------|-------|
| **class** | `ItemsDuckdbDriver` |
| **driver_ref** | `items_duckdb_driver` |
| **capabilities** | `READ_ONLY`, `STREAMING`, `SPATIAL_FILTER`, `EXPORT` |
| **driver config** | `ItemsDuckdbDriverConfig` |
| **dependencies** | `duckdb>=1.0.0` |

File-based analytical reads via DuckDB's built-in readers. Reads from parquet, CSV, JSON, etc.
Optionally writes to SQLite when `write_path` is configured.

**Connection model:** Process-wide singleton DuckDB connection (in-memory, thread-safe).
Extensions (`spatial`, `sqlite`) are loaded once at connection creation.

```
write_entities  → SQLite writes via DuckDB's sqlite extension (if write_path set)
read_entities   → DuckDB reader functions (read_parquet, read_csv_auto, etc.)
delete_entities → DELETE from SQLite (hard only, no soft delete)
export_entities → COPY ... TO (format: parquet, csv, json)
```

**Location config:**
```json
{
  "driver": "duckdb",
  "path": "/data/observations/*.parquet",
  "format": "parquet",
  "write_path": "/data/cache.sqlite",
  "write_format": "sqlite"
}
```

**Install:**
```bash
pip install dynastore[module_storage_duckdb]
```

### Elasticsearch Drivers (public)

Three classes, one per tier. All write to per-tenant indexes:

| Class | driver_ref | Per-tenant index |
|-------|-----------|-------------------|
| `ItemsElasticsearchDriver` | `items_elasticsearch_driver` | `{prefix}-items-{catalog_id}` |
| `CollectionElasticsearchDriver` | `collection_elasticsearch_driver` | `{prefix}-collections` (shared) |
| `CatalogElasticsearchDriver` | `catalog_elasticsearch_driver` | `{prefix}-catalogs` (shared) |
| `AssetElasticsearchDriver` | `asset_elasticsearch_driver` | `{prefix}-assets-{catalog_id}` |

The items driver writes directly with `_routing=collection_id` and is enrolled in the platform alias
`{prefix}-items-public` so OGC discovery routes can target one alias regardless of tenant.

**Capabilities:** `STREAMING`, `SPATIAL_FILTER`, `FULLTEXT`, `SOFT_DELETE`.

**Dispatch:** Driven by the corresponding routing config's `operations[INDEX]`. The
`ReindexWorker` / `OutboxDrainTask` dispatches non-fatal entries asynchronously via the per-tenant
`storage_outbox` table; `on_failure="outbox"` is the standard policy for the public ES INDEX entry.

**Direct programmatic indexing:** `index_item()` / `delete_item()` (and per-tier equivalents)
remain available for explicit ops calls.

### Elasticsearch Private Drivers (per-tenant, DENY-policied)

Two classes:

| Class | driver_ref | Per-tenant index |
|-------|-----------|-------------------|
| `ItemsElasticsearchPrivateDriver` | `items_elasticsearch_private_driver` | `{prefix}-{catalog_id}-private-items` |
| `CollectionElasticsearchPrivateDriver` | `collection_elasticsearch_private_driver` | `{prefix}-{catalog_id}-collections-private` |

**Privacy contract** (Cycle E.2):
- `auto_register_for_routing: ClassVar[FrozenSet[Operation]] = frozenset()` — opt-in only.
  Operators pin them in routing OR set `CollectionPrivacy.is_private=True` (plus the
  catalog policy default) which triggers the seed at collection-create.
- The items-private driver applies a catalog-wide DENY policy (`private_deny_{cat}`) on
  `ensure_storage` blocking public read access at `/.../catalogs/{cat}/...`. The collection-private
  driver does NOT manage its own DENY (the cascade rule guarantees items-private whenever
  collection-private is pinned).
- Write paths shrink oversized geometries via `simplify_to_fit` for the items-private index.

Stores `{geoid, catalog_id, collection_id}` in a custom index with `dynamic: false` mapping.
No geometry, no attributes, no spatial search — geoid lookup only.

```
write_entities  → bulk index to private index
read_entities   → es.get() by geoid → Feature with null geometry
ensure_storage  → create private index + apply DENY policy
drop_storage    → delete index + revoke DENY policy
```

**DENY policy management:** On `ensure_storage`, applies a DENY policy blocking public access.
On `drop_storage`, revokes it. On startup (`lifespan`), restores DENY policies.

---

## Driver Config System

Each driver has its own typed config class in `driver_config.py` (subclass of `PluginConfig` via
`TypedDriver[ConfigCls]`). The class_key is auto-derived as `_to_snake(cls.__name__)` (post-PR-1e —
no `_plugin_id` strings). Fetch via the standard `ConfigsProtocol` waterfall:

```python
from dynastore.modules.storage.driver_config import (
    ItemsPostgresqlDriverConfig,
    ItemsDuckdbDriverConfig,
    ItemsIcebergDriverConfig,
)
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols.configs import ConfigsProtocol

configs = get_protocol(ConfigsProtocol)
config = await configs.get_config(
    ItemsIcebergDriverConfig,
    catalog_id=catalog_id,
    collection_id=collection_id,
)
```

### Driver Config Types

| Driver | Config Class | class_key | Key Fields |
|--------|-------------|-----------|------------|
| `items_postgresql_driver` | `ItemsPostgresqlDriverConfig` | `items_postgresql_driver_config` | `collection_type`, `sidecars`, `partitioning` |
| `items_duckdb_driver` | `ItemsDuckdbDriverConfig` | `items_duckdb_driver_config` | `path`, `format`, `write_path`, `write_format` |
| `items_iceberg_driver` | `ItemsIcebergDriverConfig` | `items_iceberg_driver_config` | `catalog_name`, `catalog_type`, `catalog_uri`, `catalog_properties`, `warehouse_uri`, `warehouse_scheme`, `namespace`, `table_name` |
| `items_elasticsearch_driver` | `ItemsElasticsearchDriverConfig` | `items_elasticsearch_driver_config` | `index_prefix` (resolved at runtime via `get_index_prefix()`) |
| `asset_elasticsearch_driver` | `AssetElasticsearchDriverConfig` | `asset_elasticsearch_driver_config` | `index_prefix` |

### Engine Binding (Cycle F.1 / F.2)

Every driver config inherits two engine-binding attributes from
`_PluginDriverConfig`:

- `required_engine_class: ClassVar[str]` — the platform engine kind this
  driver class consumes (e.g. `"postgresql_engine"`,
  `"elasticsearch_engine"`, `"duckdb_engine"`, `"iceberg_engine"`).
  Concrete subclasses declare it; the validator skips compatibility
  checks when empty (graceful for un-migrated drivers + non-pooled
  drivers like BigQuery).
- `engine_ref: Optional[str]` field — name of the platform engine this
  driver instance binds to.  Defaults to `required_engine_class` for
  single-instance-per-kind deployments (F.1).  F.4 enables operator-
  chosen ref names for multi-instance.

Engines themselves live at `configs.platform.engines.*` and are
sysadmin-only via the existing `configs_access` policy — see
`src/dynastore/modules/db_config/engine_config.py` for the four
concrete engine classes.

---

## Creating a New Driver

### Step 1: Create the Driver Module

Create `src/dynastore/modules/storage/drivers/<name>.py`:

```python
"""
MyDatabase Storage Driver — brief description of what it does.
"""

import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, FrozenSet, List, Optional, Union

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.protocols import ModuleProtocol

logger = logging.getLogger(__name__)


class MyDatabaseStorageDriver(ModuleProtocol):
    """MyDatabase storage driver — what it does and when to use it."""

    # --- Required class attributes ---
    driver_ref: str = "mydatabase"       # Unique ID used in routing config
    priority: int = 40                   # Lower = higher priority

    # Declare what this driver supports
    capabilities: FrozenSet[str] = frozenset({
        Capability.STREAMING,
        Capability.EXPORT,
    })

    def is_available(self) -> bool:
        """Return True if the driver's dependencies are installed."""
        try:
            import mydatabase_client  # noqa: F401
            return True
        except ImportError:
            return False

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        """Startup/shutdown lifecycle hook."""
        logger.info("MyDatabaseStorageDriver: started")
        # Initialize connections, pools, etc.
        yield
        # Cleanup connections
        logger.info("MyDatabaseStorageDriver: stopped")

    # --- Core CRUD ---

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        *,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        # 1. Normalize input to flat dicts
        from dynastore.modules.storage.drivers._duckdb_helpers import (
            normalize_to_dicts, dicts_to_features,
        )
        rows = normalize_to_dicts(entities)
        if not rows:
            return []

        # 2. Write to your backend
        # ...

        # 3. Return Feature models
        return dicts_to_features(rows)

    async def read_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        entity_ids: Optional[List[str]] = None,
        request: Optional[QueryRequest] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Feature]:
        # Stream results — yield Feature objects one at a time
        # for row in backend.query(...):
        #     yield Feature(type="Feature", id=row["id"], ...)
        return
        yield  # Make this an async generator

    async def delete_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> int:
        if soft:
            from dynastore.modules.storage.errors import SoftDeleteNotSupportedError
            raise SoftDeleteNotSupportedError(
                "MyDatabaseStorageDriver does not support soft delete."
            )
        # Delete rows and return count
        return len(entity_ids)

    # --- Storage lifecycle ---

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
    ) -> None:
        """Create tables/indices/namespaces if they don't exist."""
        pass

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        """Remove or tag storage as deleted."""
        pass

    async def export_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        format: str = "parquet",
        target_path: str = "",
        db_resource: Optional[Any] = None,
    ) -> str:
        """Export collection data to a file."""
        raise NotImplementedError("Export not supported")
```

### Step 2: Create a Driver Config (Optional)

If your driver needs custom configuration, add a subclass of `CollectionDriverConfig` in `driver_config.py`:

```python
# In src/dynastore/modules/storage/driver_config.py

class MyDatabaseCollectionDriverConfig(CollectionDriverConfig):
    """Config for MyDatabase driver."""

    _plugin_id: ClassVar[Optional[str]] = "driver:mydatabase"
    connection_pool_size: int = Field(10, description="Connection pool size")
    read_preference: str = Field("primary", description="Read preference")
```

Store and retrieve via the config service using `_plugin_id` as the key.

### Step 3: Register the Entry Point

Add to `pyproject.toml`:

```toml
[project.entry-points."dynastore.modules"]
storage_mydatabase = "dynastore.modules.storage.drivers.mydatabase:MyDatabaseStorageDriver"
```

### Step 4: Add Optional Dependencies

If your driver has external dependencies, add an optional dependency group:

```toml
[project.optional-dependencies]
module_storage_mydatabase = ["mydatabase-client>=1.0.0"]
```

### Step 5: Configure Routing

Pin the driver in the appropriate tier's routing config. For an items-tier driver:

```json
// PUT /configs/catalogs/{cat}/collections/{col}/plugins/items_routing_config
{
  "operations": {
    "WRITE": [
      {"driver_ref": "items_postgresql_driver", "on_failure": "fatal"}
    ],
    "READ":  [
      {"driver_ref": "my_database_driver", "hints": ["analytics"]},
      {"driver_ref": "items_postgresql_driver"}
    ]
  }
}
```

The apply handler validates every `driver_ref` against the discovery registry and rejects unknown
entries with a clear error. New `Hint` values must first be added to `Hint` in `hints.py`.

### Step 6: Write Tests

Create `tests/dynastore/modules/storage/unit/test_mydatabase_driver.py`. Use real connections
where possible (following the Iceberg test pattern with `SqlCatalog` backed by PG):

```python
import pytest
from dynastore.modules.storage.drivers.mydatabase import MyDatabaseStorageDriver


class TestMyDatabaseDriverMeta:
    def test_driver_ref(self):
        assert MyDatabaseStorageDriver().driver_ref == "mydatabase"

    def test_capabilities(self):
        caps = MyDatabaseStorageDriver().capabilities
        assert "streaming" in caps


class TestMyDatabaseWriteEntities:
    @pytest.mark.asyncio
    async def test_write_and_read_back(self, driver, test_loc):
        # Write → read → verify
        ...
```

### Checklist

- [ ] `driver_ref` is unique and matches config strings
- [ ] `is_available()` returns `False` when dependencies are missing (graceful degradation)
- [ ] `capabilities` accurately declares supported features
- [ ] `lifespan()` properly initializes and cleans up resources
- [ ] `read_entities` is an async generator (streaming, not buffered)
- [ ] `write_entities` handles all input types: `Feature`, `FeatureCollection`, `Dict`, `List[Dict]`
- [ ] `delete_entities` raises `SoftDeleteNotSupportedError` if `soft=True` and unsupported
- [ ] Entry point registered in `pyproject.toml`
- [ ] Tests use real connections where feasible (no MagicMock for the backend)

---

## Utility Functions

Shared helpers in `dynastore.modules.storage.drivers._duckdb_helpers`:

```python
from dynastore.modules.storage.drivers._duckdb_helpers import (
    normalize_to_dicts,   # Feature/FeatureCollection/Dict → List[Dict]
    dicts_to_features,    # List[Dict] → List[Feature] (extracts id, geometry, properties)
)
```

These are backend-agnostic and used by DuckDB, Iceberg, and any driver that works with tabular data.

## Error Types

```python
from dynastore.modules.storage.errors import (
    ReadOnlyDriverError,          # Write attempted on a read-only driver
    SoftDeleteNotSupportedError,  # soft=True on a driver without SOFT_DELETE capability
)
```

`ReadOnlyDriverMixin` is available for drivers that only support reads — it raises
`ReadOnlyDriverError` on all write/delete/drop calls automatically.

## File Layout

```
src/dynastore/
├── models/
│   ├── protocols/
│   │   ├── storage_driver.py            # CollectionItemsStore Protocol + Capability enum
│   │   ├── entity_store.py              # CollectionStore / CatalogStore Protocols
│   │   └── asset_driver.py              # AssetStore Protocol
│   ├── ogc.py                           # Feature, FeatureCollection
│   ├── otf.py                           # SnapshotInfo, SchemaVersion, SchemaEvolution
│   └── query_builder.py                 # QueryRequest
├── modules/storage/
│   ├── __init__.py                      # Public API exports
│   ├── protocol.py                      # Re-export convenience
│   ├── routing_config.py                # ItemsRoutingConfig / CollectionRoutingConfig /
│   │                                    # AssetRoutingConfig / CatalogRoutingConfig +
│   │                                    # Operation, OperationDriverEntry, FailurePolicy, WriteMode
│   ├── hints.py                         # Hint StrEnum (closed catalog)
│   ├── driver_config.py                 # ItemsWritePolicy, ItemsSchema, *DriverConfig, ...
│   ├── router.py                        # get_driver() with cached operation-based resolution
│   ├── outbox_ddl.py                    # storage_outbox + index_failure_log DDL (post-PR-#261)
│   ├── errors.py                        # ReadOnlyDriverError, SoftDeleteNotSupportedError, ConflictError
│   └── drivers/
│       ├── __init__.py
│       ├── postgresql.py                # ItemsPostgresqlDriver
│       ├── core_postgresql.py           # CollectionPostgresqlDriver / CatalogPostgresqlDriver
│       ├── collection_postgresql.py     # collection-envelope PG driver
│       ├── catalog_postgresql.py        # catalog-tier PG driver
│       ├── iceberg.py                   # ItemsIcebergDriver
│       ├── duckdb.py                    # ItemsDuckdbDriver
│       ├── elasticsearch.py             # ItemsElasticsearchDriver + AssetElasticsearchDriver
│       └── elasticsearch_private/       # ItemsElasticsearchPrivateDriver +
│                                        # CollectionElasticsearchPrivateDriver (DENY-policied)
└── docs/components/
    ├── storage_drivers.md               # This file
    ├── platform_engines.md              # Engines layer (Cycle F): connection pools,
    │                                    # lifecycle policy, engine ↔ driver compatibility
    └── sidecar_configs.md               # Sidecar configurations (geometries,
                                         # attributes, item_metadata, stac_metadata)
                                         # with full default field surface
```

## See also

- `docs/components/platform_engines.md` — platform-tier engine configs,
  the four engine kinds, lifecycle policy, the `EngineInstanceCache`
  contract, and the operator workflows for provisioning + maintenance.
- `docs/components/sidecar_configs.md` — sidecars composed onto
  PG-backed items drivers; full default field surface for every
  shipped sidecar.
- `docs/components/configs_api.md` — operator surface for runtime
  configuration: GET/PATCH endpoints, query params (`resolved`,
  `meta`, `include`, `strict`), HATEOAS link catalog, scope
  strictness rules, live-shape examples at platform / catalog /
  collection scope.
