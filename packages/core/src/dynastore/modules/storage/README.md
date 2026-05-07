# Storage Module — Multi-Driver Routing

Entity-level storage abstraction that routes catalog/collection/items/asset data to pluggable backends.

## Role

The storage module sits between the REST API layer and the storage backends. It resolves which
driver to dispatch for a given `(operation, catalog_id, collection_id, hint)` tuple via the
appropriate per-tier routing config (`ItemsRoutingConfig` / `CollectionRoutingConfig` /
`AssetRoutingConfig` / `CatalogRoutingConfig`), then delegates the operation to that driver.

## Components

| File | Purpose |
|------|---------|
| `routing_config.py` | `ItemsRoutingConfig` / `CollectionRoutingConfig` / `AssetRoutingConfig` / `CatalogRoutingConfig` — operation-based routing (post-PR-#261); `Operation`, `OperationDriverEntry`, `FailurePolicy`, `WriteMode` |
| `hints.py` | `Hint` (closed `StrEnum`) — selectivity tags advertised by drivers and consumed by the router |
| `driver_config.py` | `ItemsWritePolicy`, `ItemsSchema`, per-driver `*DriverConfig` classes (PluginConfig waterfall) |
| `router.py` | `get_driver(operation, catalog_id, collection_id, hint=...)` — cached operation-based resolution |
| `outbox_ddl.py` | Per-tenant `storage_outbox` DDL for async fan-out. Indexing failures are emitted as structured log events through `LogService`. |
| `protocol.py` | Re-export convenience for `CollectionItemsStore` and friends |
| `drivers/postgresql.py` | `ItemsPostgresqlDriver` — items-tier durability primary |
| `drivers/elasticsearch.py` | `ItemsElasticsearchDriver` + `AssetElasticsearchDriver` — public per-tenant indexes |
| `drivers/elasticsearch_private/` | `ItemsElasticsearchPrivateDriver` + `CollectionElasticsearchPrivateDriver` — DENY-policied per-tenant private indexes |
| `drivers/iceberg.py` | `ItemsIcebergDriver` — OTF (snapshots, time-travel, schema evolution) |
| `drivers/duckdb.py` | `ItemsDuckdbDriver` — file-based analytical reads |
| `drivers/core_postgresql.py` / `collection_postgresql.py` / `catalog_postgresql.py` | per-tier PG drivers |

## Public API

```python
from dynastore.modules.storage.router import get_driver
from dynastore.modules.storage.routing_config import Operation
from dynastore.modules.storage.hints import Hint

# Read — Operation + Hint selects the right backend (first-match wins on hints)
driver = await get_driver(Operation.READ, catalog_id, collection_id, hint=Hint.GEOMETRY_SIMPLIFIED)
async for feature in driver.read_entities(catalog_id, collection_id, request=query):
    process(feature)

# Write — first WRITE entry from ItemsRoutingConfig.operations[WRITE] (durability primary).
driver = await get_driver(Operation.WRITE, catalog_id, collection_id)
written = await driver.write_entities(catalog_id, collection_id, feature_collection)
```

## Configuration

Routing is set via `ConfigsProtocol` at platform / catalog / collection level and resolved via the
4-tier waterfall. **Operation-based** post-PR-#261: `operations: Dict[Operation, List[OperationDriverEntry]]`
where each `Operation` (`WRITE` / `READ` / `SEARCH` / `INDEX` / `BACKUP` / `UPLOAD`) maps to an
ordered list of drivers with per-entry `on_failure` / `write_mode` / `hints` / `source`.

```json
{"operations": {
  "WRITE": [
    {"driver_ref": "items_postgresql_driver", "on_failure": "fatal"},
    {"driver_ref": "items_elasticsearch_driver", "write_mode": "async", "on_failure": "outbox"}
  ],
  "READ": [
    {"driver_ref": "items_elasticsearch_driver", "hints": ["geometry_simplified"]},
    {"driver_ref": "items_postgresql_driver", "hints": ["geometry_exact"]}
  ]
}}
```

`driver_ref` is always `_to_snake(cls.__name__)` (post-PR-1e). Operator API is at
`/configs/.../plugins/{plugin_id}` where `plugin_id` is the snake_case `class_key`.

## Drivers (summary)

### `items_postgresql_driver` (`ItemsPostgresqlDriver`)

Source-of-truth for entity-row WRITE operations (durability primary). Owns SQL for the per-tenant
items table and its sidecars (geometry, attributes, item_metadata, stac_metadata). All sidecar
logic, query optimization, PostGIS, and streaming stay in this driver's service layer.

### `items_elasticsearch_driver` (`ItemsElasticsearchDriver`)

Items-tier ES driver. Writes to per-tenant index `{prefix}-items-{catalog_id}` with
`_routing=collection_id`, enrolled in the platform alias `{prefix}-items-public`. Driven by
`ItemsRoutingConfig.operations[INDEX]` and dispatched async via the outbox.

### `items_elasticsearch_private_driver` (`ItemsElasticsearchPrivateDriver`) — opt-in only

Stores the full feature (geometry simplified to fit when oversized) in a per-tenant private
index `{prefix}-{catalog_id}-private-items` with `TENANT_FEATURE_MAPPING` (root `dynamic: false`).
On `ensure_storage`, applies a catalog-wide DENY policy (`private_deny_{cat}`) blocking public
read access. `auto_register_for_routing = frozenset()` — operators pin it explicitly OR set
`CollectionPrivacy.is_private = True` (Cycle E.2/F.0d) which triggers the seed at create-time.

### `collection_elasticsearch_driver` / `collection_elasticsearch_private_driver`

Public + private collection-envelope drivers. Mirror the items pair but write collection envelopes
to `{prefix}-collections` (public, shared) or `{prefix}-{catalog_id}-collections-private` (private,
per-tenant).

### `items_iceberg_driver` (`ItemsIcebergDriver`)

OTF driver. ACID transactions, snapshots, time-travel reads, schema evolution. Default catalog is
PostgreSQL-backed `SqlCatalog`; warehouse auto-resolves from the platform's `StorageProtocol` (e.g.,
GCS bucket) or falls back to local temp.

### `items_duckdb_driver` (`ItemsDuckdbDriver`)

File-based analytical reads via DuckDB's `read_parquet` / `read_csv_auto` etc. Optionally writes
to SQLite when `write_path` is configured.

## Adding a Driver

1. Create `drivers/<name>.py`, subclass `ModuleProtocol` (and the relevant tier protocol —
   `CollectionItemsStore` / `CollectionStore` / `AssetStore` / `CatalogStore`).
2. Give it a class name that yields the desired `driver_ref` via `_to_snake(cls.__name__)`.
3. Implement the protocol methods.
4. Add entry point in `pyproject.toml` under `[project.entry-points."dynastore.modules"]`.
5. Pin in the relevant routing config (e.g. `PUT /configs/.../plugins/items_routing_config`),
   or rely on auto-registration via `auto_register_for_routing: ClassVar[FrozenSet[Operation]]`.

Full step-by-step in [`docs/components/storage_drivers.md`](../../../../docs/components/storage_drivers.md).

## Dependencies

- Core: `pydantic`, `cachetools` (always available)
- PostgreSQL driver: `dynastore[module_catalog]` (wraps existing services)
- Elasticsearch drivers: `stac-fastapi-elasticsearch` (optional)
- Iceberg driver: `pyiceberg[sql-postgres]>=0.9.0`, `pyarrow>=14.0.0`
- DuckDB driver: `duckdb>=1.0.0`
