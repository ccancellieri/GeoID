# Collection Lifecycle — Lazy Activation

A Collection in DynaStore has two observable states:

| State | `physical_table` | Metadata visible on `GET /collections`? | Items readable? | Items writable? |
|-------|------------------|------------------------------------------|-----------------|-----------------|
| **Pending** | `None` | ✅ yes | ✅ yes (empty FeatureCollection) | **first write triggers activation** |
| **Active** | `"<table-name>"` | ✅ yes | ✅ yes (queries the provisioned storage) | ✅ yes (writes hit the pinned driver) |

A collection is created in the *pending* state. It transitions to *active*
on its **first item write** — transparently, inside the same transaction
that inserts the first item.

## Why

`CollectionRoutingConfig.operations` is declared `Immutable` to prevent
silent re-routing after data has been written: once storage is
provisioned against a driver, the routing that chose that driver is
frozen. The only safe way to change routing is to create a new collection.

Previously, routing was pinned at **create time** (eager activation).
This made per-collection routing overrides impossible without racing a
catalog-scope mutation across up to 50 Cloud Run pods (5-minute cache
TTL). See `router.py:invalidate_router_cache` and the
`project_geoid_cloudrun_topology` memory note.

Lazy activation moves the pin from create-time to first-insert-time. The
interval between the two is a **configuration window** in which any
collection-scope config (routing, write policy, schema, driver config)
can be freely set. The immutability guard short-circuits while
`current=None`, so first-writes always succeed.

## The three-call workflow

### 1. Create — OGC / STAC standard

```http
POST /stac/catalogs/{catalog_id}/collections
Content-Type: application/json

{
  "id": "anon-locations",
  "title": "Anonymous Locations",
  "extent": { ... }
}
```

Response: `201 Created` with the Collection JSON. Collection is now
**pending**. No physical storage has been provisioned; no driver
configuration has been pinned.

This request carries **only** a standard STAC Collection JSON body. No
extension fields, no `configs` map. The endpoint is fully
OGC-spec-shaped.

### 2. Configure — *optional*, internal API

```http
PUT /configs/catalogs/{catalog_id}/collections/{collection_id}/configs/CollectionRoutingConfig
Content-Type: application/json

{
  "operations": {
    "WRITE":    [{"driver_id": "ItemsElasticsearchPrivateDriver"}],
    "READ":     [{"driver_id": "ItemsElasticsearchPrivateDriver"}],
    "SEARCH":   [{"driver_id": "ItemsElasticsearchPrivateDriver"}],
    "METADATA": [{"driver_id": "ItemsPostgresqlDriver"}]
  }
}
```

This step is **optional**. Skip it and catalog defaults apply on
activation. Call it (zero, one, or many times, for any pinnable
collection-scope config) to override defaults before the first insert.

Call as many different configs as you need — `CollectionRoutingConfig`,
`ItemsWritePolicy`, `CollectionSchema`, `ItemsPostgresqlDriverConfig`,
etc. All accept a first-write while the collection is pending.

This endpoint is **not** OGC-standard — it is an internal API. Clients
that only speak OGC/STAC never touch it and still get a fully functional
collection using catalog defaults.

### 3. Insert — OGC / STAC Transactions standard

```http
POST /stac/catalogs/{catalog_id}/collections/{collection_id}/items
Content-Type: application/json

{ "type": "FeatureCollection", "features": [ ... ] }
```

On the **first** such request, the server transparently:

1. Resolves the effective driver configuration (pinned overrides, then
   catalog defaults, then platform defaults — the normal waterfall).
2. Calls `write_driver.ensure_storage(...)` — creates the PostgreSQL
   hub + sidecar tables, provisions the Elasticsearch index, allocates
   the GCS prefix, etc.
3. Pins `CollectionRoutingConfig` at collection scope. The `Immutable`
   invariant takes hold from this point forward.
4. Runs the actual insert.

All four steps happen in the same database transaction as the insert,
so either everything commits together or nothing does. Concurrent
first-insert callers are serialised by a row lock on the config row;
`ensure_storage` is idempotent by design.

Subsequent inserts short-circuit past activation and go straight to the
driver.

## OGC / STAC compliance

- `POST /collections` body is a plain STAC Collection JSON object with
  no DynaStore extensions. All STAC / OGC Features / Records / Coverages
  conformance classes that require `POST /collections` are satisfied
  verbatim.
- `GET /collections/{col}/items` on a pending collection returns HTTP
  200 with an empty `FeatureCollection` (`features: []`,
  `numberReturned: 0`). This satisfies **OGC API Features Part 1,
  Requirement 26** (successful items request returns 200 with a
  FeatureCollection; empty is valid).
- `GET /collections/{col}` returns the Collection JSON unchanged —
  metadata exists from step 1 onward.
- `GET /collections/{col}/coverage` on a pending collection returns
  404 `problem+json` (OGC Coverages 19-087 is silent on pre-activation
  state; 404 is the conventional HTTP fallback).
- `POST /collections/{col}/items` on a pending collection is
  standard STAC Transactions — the activation is entirely transparent
  to the client.
- **No non-standard endpoints were added.** There is no `/activate`
  endpoint and no `/bootstrap` endpoint. Every surface a spec-conformant
  client sees is spec-shaped.

## Concurrency

| Scenario | Outcome |
|----------|---------|
| Two concurrent `PUT /configs/...` on the same pending collection | `FOR UPDATE` row lock serialises them. While current value is still `None`, both are accepted in sequence; the second sees the first's value and either equals it (no-op) or the `Immutable` guard rejects it with 409. Standard REST semantics. |
| Two concurrent first-item inserts | Both call `_activate_collection` in their own transactions. `ensure_storage` is idempotent; routing pin is serialised by the same row lock; the loser sees a pinned value equal to its own and short-circuits. Both inserts commit. |
| `PUT` config on pod 1 + first-insert on pod 50 at the same time | Activation reads configs **from the database, inside the insert's transaction** — not from any cache. Whichever commits first wins for its own semantics: if PUT commits first, activation pins the new value; if insert commits first, the collection becomes active with the old value and the PUT fails 409. No cross-pod cache coherence is required — the per-process `invalidate_router_cache` becomes irrelevant for this flow. |
| First-insert fails after activation | Transaction rolls back; the collection stays pending; any pinned configs stay pinned (they were written to `current=None` rows, which is allowed). Retry succeeds or surfaces the same failure deterministically. |

## Ops notes

- **To pre-provision a collection before real data arrives**, POST a
  synthetic feature and then soft-delete it. Standard OGC surface; no
  special endpoint needed.
- **To check activation state**, call
  `CollectionService.is_active(catalog_id, collection_id)` — derived
  from `ItemsPostgresqlDriverConfig.physical_table`. No extra table,
  no new column on `collections`.
- **Driver swap on an already-active collection is still forbidden** —
  `CollectionRoutingConfig.operations` remains `Immutable` once pinned.
  The supported migration path stays "create new collection + reingest".

## References

- Implementation: [`modules/catalog/collection_service.py`](../../src/dynastore/modules/catalog/collection_service.py) — `_activate_collection`, `is_active`.
- Lazy hook: [`modules/catalog/item_service.py`](../../src/dynastore/modules/catalog/item_service.py) — Branch B of `upsert` calls `is_active` then `_activate_collection` before the first insert.
- Pending read guard: [`modules/catalog/item_query.py`](../../src/dynastore/modules/catalog/item_query.py) — `stream_items` returns empty `QueryResponse` when `physical_table is None`.
- OGC Features Part 1, Requirement 26 — [`17-069r3`](https://docs.ogc.org/is/17-069r3/17-069r3.html).
- Immutable config invariant: [`modules/db_config/platform_config_service.py`](../../src/dynastore/modules/db_config/platform_config_service.py) — `enforce_config_immutability` short-circuits when current row is `None`.
