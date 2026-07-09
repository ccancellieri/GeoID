# Storage Module — Multi-Driver Routing

Entity-level storage abstraction that routes catalog/collection/items/asset data to pluggable backends.

## Role

The storage module sits between the REST API layer and the storage backends. It resolves which
driver to dispatch for a given `(operation, catalog_id, collection_id, hints)` tuple via the
appropriate per-tier routing config (`ItemsRoutingConfig` / `CollectionRoutingConfig` /
`AssetRoutingConfig` / `CatalogRoutingConfig`), then delegates the operation to that driver.

## Components

| File | Purpose |
|------|---------|
| `routing_config.py` | `ItemsRoutingConfig` / `CollectionRoutingConfig` / `AssetRoutingConfig` / `CatalogRoutingConfig` — operation-based routing (post-PR-#261); `Operation`, `OperationDriverEntry`, `FailurePolicy`, `WriteMode` |
| `hints.py` | `Hint` (closed `StrEnum`) — selectivity tags advertised by drivers and consumed by the router |
| `driver_config.py` | `ItemsWritePolicy`, `ItemsSchema`, per-driver `*DriverConfig` classes (PluginConfig waterfall) |
| `router.py` | `get_driver(operation, catalog_id, collection_id, hints=...)` — cached operation-based resolution |
| `storage_emit.py` | Single-path write into the global `tasks.storage` table with co-transactional drain trigger. |
| `protocol.py` | Re-export convenience for `CollectionItemsStore` and friends |
| `drivers/postgresql.py` | `ItemsPostgresqlDriver` — items-tier durability primary |
| `drivers/elasticsearch.py` | `ItemsElasticsearchDriver` + `AssetElasticsearchDriver` — public per-tenant indexes |
| `drivers/elasticsearch_private/` | `ItemsElasticsearchPrivateDriver` — DENY-policied per-tenant private items index (items tier only; catalog/collection envelopes are PG-only for private catalogs) |
| `drivers/iceberg.py` | `ItemsIcebergDriver` — OTF (snapshots, time-travel, schema evolution) |
| `drivers/duckdb.py` | `ItemsDuckdbDriver` — file-based analytical reads |
| `drivers/core_postgresql.py` / `collection_postgresql.py` / `catalog_postgresql.py` | per-tier PG drivers |

## Public API

```python
from dynastore.modules.storage.router import get_driver
from dynastore.modules.storage.routing_config import Operation
from dynastore.modules.storage.hints import Hint

# Read — Operation + Hint selects the right backend (first-match wins on hints)
driver = await get_driver(Operation.READ, catalog_id, collection_id, hints=frozenset({Hint.GEOMETRY_SIMPLIFIED}))
async for feature in driver.read_entities(catalog_id, collection_id, request=query):
    process(feature)

# Write — first WRITE entry from ItemsRoutingConfig.operations[WRITE] (durability primary).
driver = await get_driver(Operation.WRITE, catalog_id, collection_id)
written = await driver.write_entities(catalog_id, collection_id, feature_collection)
```

## Configuration

Routing is set via `ConfigsProtocol` at platform / catalog / collection level and resolved via the
4-tier waterfall. **Operation-based** post-PR-#261: `operations: Dict[Operation, List[OperationDriverEntry]]`
where each `Operation` (`WRITE` / `READ` / `SEARCH` / `UPLOAD`) maps to an
ordered list of drivers with per-entry `on_failure` / `write_mode` / `hints` / `source`.
Secondary indexes are not a distinct operation: a secondary index is a `WRITE` entry flagged
`secondary_index=true` (typically `write_mode: async`, `on_failure: outbox`), propagated by the
ReindexWorker — see the async ES driver in the `WRITE` list below.

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

## Storage-Plane Outbox (`tasks.storage`)

An `ASYNC` secondary-index `WRITE` entry persists its obligation into the
global `tasks.storage` table — co-transactionally with the upstream write —
via `storage_emit.py`'s `enqueue_storage_op_write_id` / `enqueue_storage_op_id_only`,
then drained by `StorageDrainTask`. The ledger stores identifiers only; there
is no payload column.

- `enqueue_storage_op_write_id` writes one row per `(write_id, driver,
  collection, op)` representing a whole primary write batch; the drain
  hydrates it from the primary WRITE driver's hub by `write_id`
  (keyset-paged chunk reads).
- `enqueue_storage_op_id_only` writes one row per entity id; the drain
  re-reads canonical PG state for that id at replay time instead of
  indexing a payload frozen at enqueue time.

`IndexDispatcher` only groups ops by `write_id` when the collection's primary
WRITE driver exposes the write-id chunk-read capability —
`read_indexable_write_batch`, or the `read_active_rows_by_write_id` /
`read_tombstoned_ids_by_write_id` pair (`driver_supports_write_id_reads` in
`storage_emit.py`, checked per collection via `_primary_supports_write_id_reads`
in `index_dispatcher.py`, #3116). A driver that doesn't expose the capability
falls back to id-only rows instead.

Full row-shape/classification contract and the drain side are documented in
[`../tasks/README.md`](../tasks/README.md).

## Drivers (summary)

### `items_postgresql_driver` (`ItemsPostgresqlDriver`)

Source-of-truth for entity-row WRITE operations (durability primary). Owns SQL for the per-tenant
items table and its sidecars (geometry, attributes, item_metadata, stac_metadata). All sidecar
logic, query optimization, PostGIS, and streaming stay in this driver's service layer.

### `items_elasticsearch_driver` (`ItemsElasticsearchDriver`)

Items-tier ES driver. Writes to per-tenant index `{prefix}-items-{catalog_id}` with
`_routing=collection_id`, enrolled in the platform alias `{prefix}-items-public`. Driven by
the secondary-index `WRITE` entries (`secondary_index=True`) in `ItemsRoutingConfig.operations[WRITE]` and dispatched async via the outbox.

### `items_elasticsearch_private_driver` (`ItemsElasticsearchPrivateDriver`) — opt-in only

Stores the full feature (geometry simplified to fit when oversized) in a per-tenant private
index `{prefix}-{catalog_id}-private-items` with `TENANT_FEATURE_MAPPING` (root `dynamic: false`).
On `ensure_storage`, applies a catalog-wide DENY policy (`private_deny_{cat}`) blocking public
read access. `auto_register_for_routing = frozenset()` — pinning this driver in a routing config
is itself the privacy switch (#733 retired the standalone `CollectionPrivacy.is_private` flag).

### `collection_elasticsearch_driver`

Public collection-envelope driver. Writes collection envelopes to `{prefix}-collections` (shared
global index). Private catalogs do not use ES for collection envelopes — their collections are
PG-only (see #1047).

### `items_iceberg_driver` (`ItemsIcebergDriver`)

OTF driver. ACID transactions, snapshots, time-travel reads, schema evolution. Default catalog is
PostgreSQL-backed `SqlCatalog`; warehouse auto-resolves from the platform's `StorageProtocol` (e.g.,
GCS bucket) or falls back to local temp.

### `items_duckdb_driver` (`ItemsDuckdbDriver`)

File-based analytical reads via DuckDB's `read_parquet` / `read_csv_auto` etc. Optionally writes
to SQLite when `write_path` is configured.

## Routing Presets (#847, #972)

Named, cascade-consistent bundles of routing configs + audience opt-ins that
operators apply with a single admin call. A preset is a thin factory that
emits a `PresetBundle`; the admin endpoint walks the bundle through the
standard `ConfigsProtocol.set_config` lifecycle (no validation bypass).

The registry is a single flat namespace. Each preset declares a `tier`
(`PresetTier`) that decides which admin URL family it is reachable from; the
URL encodes the apply scope. Items/assets-tier presets can attach at the
collection family always, and at the catalog family when
`catalog_scopable=True`.

Built-in presets (in `presets/`):

| Name | Tier | Composition |
|------|------|-------------|
| `public_catalog` | catalog | PG-first storage + public ES indexers on catalog/collection/items. No audience opt-ins; anonymous traffic is gated by the platform's default `public_access` policy. |
| `private_catalog` | catalog | PG-only catalog/collection envelopes + per-tenant private ES indexer on the items tier. No audience opt-ins; the `private_deny_{catalog_id}` policy blocks anonymous reads on item URL patterns. |
| `defaults_postgres` | platform | Platform-wide PG-first routing defaults for catalog/collection/items. No indexers, no audience opt-ins — a safe baseline new catalogs inherit before any override. |
| `private_collection` | collection | Per-collection private items routing override (pins `items_elasticsearch_private_driver` on one collection). |
| `geoid` (extension) | catalog | Composes `private_catalog` and adds `CatalogLookupAudience.is_public=True` — flagship FAO GeoID profile: private storage, anonymous lookup-only (resolve by geoid / external_id, no enumeration, no anonymous insert). Lookup-only mode and anonymous create cannot coexist (deny-precedence), so this profile carries no write audience; intake catalogs use `is_public=False` + per-collection `allow_anonymous_create`. Registered by `dynastore.extensions.geoid` on import. |

Operator API:

```
GET    /admin/presets                                                        # list all presets (name, tier, catalog_scopable)
GET    /admin/presets?tier=collection                                        # filter by tier

POST   /admin/presets/{name}                                                 # platform tier
DELETE /admin/presets/{name}

POST   /admin/catalogs/{catalog_id}/presets/{name}                           # catalog tier
DELETE /admin/catalogs/{catalog_id}/presets/{name}

POST   /admin/catalogs/{catalog_id}/collections/{collection_id}/presets/{name}   # collection tier
DELETE /admin/catalogs/{catalog_id}/collections/{collection_id}/presets/{name}
```

Each `POST` walks the bundle's entries through `set_config` at the URL-derived
scope; `DELETE` rolls them back leaf-first and returns **409** if a persisted
row diverges from the preset bundle. Applying a preset at a URL family that
does not match its tier returns **409** (the preset exists but is invalid at
that scope).

Adding a preset: subclass `BundlePreset` (set `name`, `description`, `tier`,
`catalog_scopable` and implement `build(**scope) -> PresetBundle`) and call
`register_preset(MyPreset())` from your extension or module bootstrap. The
base class supplies the `apply` / `revoke` / `dry_run` lifecycle on top of
`build`; override the optional `on_applied` / `on_revoked` hooks for any
side effects. `build` receives the scope its tier needs (`()` platform,
`catalog_id` catalog, `catalog_id` + `collection_id` collection). Core
presets live under `presets/` and auto-register on import; extension presets
register from their package `__init__.py` (see `extensions/geoid/presets.py`).

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
- Elasticsearch drivers: `elasticsearch[async]` via `dynastore[module_elasticsearch]`
- Iceberg driver: `pyiceberg[sql-postgres]>=0.9.0`, `pyarrow>=14.0.0`
- DuckDB driver: `duckdb>=1.0.0`
