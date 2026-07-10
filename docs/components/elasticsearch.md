# The Elasticsearch Module & Search Extension

The `elasticsearch` module and its companion `search` extension provide full-text, spatial, and temporal search over DynaStore entities backed by Elasticsearch. Together they form a complete indexing pipeline with runtime-configurable per-catalog behaviours — including the **GeoID private mode** for privacy-sensitive catalogs.

This component follows the "Three Pillars" architecture: a silent `module` (platform lifecycle + `Indexer`-driver implementations wired through routing-config `INDEX`-lane entries), a stateless API `extension` (search + admin endpoints), and asynchronous `tasks` (durable drain/bulk-reindex workers and jobs).

> **Implementation details:** see the [Elasticsearch module README](../../packages/core/src/dynastore/modules/elasticsearch/README.md).

---

## Protocol-Based Decoupling

Search and indexing are decoupled via protocols defined in `models/protocols/`:

| Protocol | Contract | Current implementor(s) | Discovery |
|---|---|---|---|
| `SearchProtocol` | Query execution (items, catalogs, collections) + reindex triggers | `SearchService` (ES-backed) | `get_protocol(SearchProtocol)` |
| `Indexer` | Slim `index` / `index_bulk` surface every INDEX-lane driver implements | `ItemsElasticsearchDriver`, `ItemsElasticsearchPrivateDriver`, `CollectionElasticsearchDriver`, `CatalogElasticsearchDriver`, `AssetElasticsearchDriver` | `IndexDispatcher` walks `RoutingConfig.operations[INDEX]` and calls this surface uniformly |
| `IndexTierDriver` | Discovery/seeding marker — `index_tiers: ClassVar[FrozenSet[str]]`, checked by value | same ES driver classes | `get_index_drivers()` / routing-config self-registration |

`IndexerProtocol` (the older per-entity fat surface) still exists for backward compatibility but has no current implementor — new indexing drivers target `Indexer` + `IndexTierDriver` and are wired through routing-config `INDEX`-lane entries instead.

**Why this matters:**
- The **router** (`extensions/search/router.py`) has zero imports from `modules/elasticsearch` — it discovers `SearchProtocol` at runtime.
- ES-backed drivers expose `Indexer` so `IndexDispatcher` can dispatch indexing without knowing the backend.
- To **swap backends** (Solr, Meilisearch, etc.), implement `Indexer` (+ `IndexTierDriver` for auto-registration) in a new module and pin the new `driver_ref` under the relevant `*RoutingConfig.operations[INDEX]`. No changes to the router, dispatcher, or consumers.

```
Router  ──discovers──>  SearchProtocol  ──implemented by──>  SearchService (ES)

RoutingConfig.operations[INDEX]  ──walked by──>  IndexDispatcher  ──calls──>  Indexer (ES driver)
```

---

## Module Core (`modules/elasticsearch`)

### Indexing Pipeline (routing-config driven)

Catalog / collection / item / asset `INDEX`-lane propagation flows through routing-config rails,
not through lifecycle-event listeners on `ElasticsearchModule` (that listener path — and the
`elasticsearch_index` / `elasticsearch_delete` task types it enqueued — was retired: it ran in
parallel to the canonical rails and produced misleading log lines on every routing-config write).

1. **Items.** `item_service` calls `IndexDispatcher.fan_out_bulk` directly; the dispatcher reads
   the `INDEX`-lane entries in `ItemsRoutingConfig.operations[INDEX]` and dispatches to whichever
   `Indexer` drivers are pinned there — typically `items_elasticsearch_driver` (public) or
   `items_elasticsearch_private_driver` (private). Item-tier obligations are always durably
   enqueued into the global `tasks.storage` table (id-only rows) and drained asynchronously by
   `StorageDrainTask`. Soft-delete fan-out uses the same dispatcher.
2. **Collections.** `collection_router._dispatch_collection_index` calls
   `IndexDispatcher.fan_out_bulk` with `entity_type='collection'` directly from the write path
   (the same trigger shape as items) against `CollectionRoutingConfig.operations[INDEX]`. Both
   upsert and hard-delete trigger the dispatch.
3. **Catalogs.** `catalog_router` emits `CATALOG_METADATA_CHANGED` inside the WRITE transaction;
   `ReindexWorker` consumes the durable event and fans out to `CatalogRoutingConfig.operations[INDEX]`
   — the trigger is event-driven rather than a direct call from the write path.
4. **Assets.** `AssetEntitySyncSubscriber` listens for `CatalogEventType.ASSET_*` events and fans
   out to `AssetRoutingConfig.operations[INDEX]` — mirrors the catalog-tier event-driven trigger.

All four tiers end at the same `Indexer` surface through durable plumbing; the asymmetry is in how
the hop is *triggered* (direct dispatch for items/collections, event-plane for catalogs/assets),
not in what runs once triggered.

### Index Design

| Index pattern | Entity | Mapping highlights |
|---|---|---|
| `{prefix}-catalogs` | Catalog | keyword + multilingual free-text (`title.*`, `description.*`) |
| `{prefix}-collections` | Collection | `geo_shape` for spatial extent, date range for temporal extent |
| `{prefix}-items` | Item | `geo_shape` for geometry, STAC `datetime`, dynamic template for all properties |
| `{prefix}-assets` | Asset | `item_id`, `asset_key`, `roles`, `href` |
| `{prefix}-geoid-{catalog_id}` | Private item | `dynamic: false`, only `geoid`, `catalog_id`, `collection_id` |

Dynamic templates are applied in order (first match wins) to handle multilingual text fields, projection metadata, and generic catch-all mappings — preventing mapping explosions while preserving aggregation capability.

### Canonical item envelope

Item `_source` is assembled by `build_canonical_index_doc` (`modules/elasticsearch/canonical_doc.py`) into a fixed set of lanes, one modular shape for every entity level. The lanes are the contract; what fills them is the per-level concern:

| Lane | Content | Indexing |
|---|---|---|
| flat root (`id`, `catalog_id`, `collection_id`, `collection`, `external_id`, `asset_id`, `validity`, `geometry`, `bbox`) | identity axes + reserved GeoJSON/STAC members | typed (`COMMON_PROPERTIES`, `geo_shape`) |
| `system` | identity + lifecycle fields (see table below) | typed when seeded as known fields — see caveat |
| `stats` | geometry-derived computed values (`area`, `centroid`, `s2_*`/`h3_*`/`geohash_*` cells) | typed when seeded as known fields — see caveat |
| `properties` | per-collection user attributes | known → flat typed; unknown → `properties.extras` (`flattened`, exact-match only) + analyzed catch-all on root `_search_text` |
| `metadata` | multilingual `title`/`description`/`keywords` from the `ItemMetadataSidecar` | typed `dynamic:false`, per-language analyzed sub-fields |

**System lane.** Populated from the bounded `SYSTEM_FIELD_KEYS` vocabulary (`modules/storage/computed_fields.py`) — every key whose value is present on the PG row lands; the three identity axes (`geoid`/`external_id`/`asset_id`) are *also* mirrored flat at the document root:

| `system.*` key | Source | Present when | ES type |
|---|---|---|---|
| `geoid` | identity | always (also flat root `id`) | `keyword` |
| `external_id` | identity | set at ingest (e.g. a `CODE` column) | `keyword` |
| `asset_id` | identity | asset-derived items only | `keyword` |
| `geometry_hash` | computed | always (geometry digest) | `keyword` |
| `attributes_hash` | computed | always (attribute digest) | `keyword` |
| `validity` | lifecycle | item has a validity window | `date_range` |
| `transaction_time` | lifecycle | always (write timestamp) | `date` |
| `deleted_at` | lifecycle | tombstones only (omitted on live items) | `date` |

The full set is emitted on every normal write — not just `geoid`/`external_id`. When the secondary write path resolves the geoid from the canonical `id`, it routes back through `build_canonical_index_doc`, so the whole row — all system columns and all stats — is rebuilt.

**Caveat — `_source` presence vs. typed searchability.** `build_canonical_index_doc` *always* writes the `system`/`stats` lanes into `_source` (so they are visible in the index and round-trip on read). Whether they are **typed and searchable** depends on `build_item_mapping` declaring the `system`/`stats` containers, which it does *only* when the catalog's `known_fields` carries entries tagged `container="system"`/`"stats"`. The platform baseline `TIER_1_FIELDS` seeds only properties-lane STAC/EO fields — it does **not** yet seed the bounded system/stats vocabulary — and the index root is `dynamic:false`, so on a catalog without those known fields the lanes are stored but not queryable. Baking the bounded system/stats vocabulary into the base mapping is planned.

### Configuration

**Environment variables** (connection-level, set at deploy time):

| Variable | Default | Description |
|---|---|---|
| `ELASTICSEARCH_URL` | `http://localhost:9200` | ES cluster URL |
| `ELASTICSEARCH_USERNAME` | _(empty)_ | Basic-auth username |
| `ELASTICSEARCH_PASSWORD` | _(empty)_ | Basic-auth password |
| `ELASTICSEARCH_API_KEY` | _(empty)_ | API key (alternative to basic auth) |
| `ELASTICSEARCH_VERIFY_CERTS` | `true` | TLS certificate verification |
| `ELASTICSEARCH_INDEX_PREFIX` | `dynastore` | Prefix for all index names |

---

## Per-Collection Privacy

Privacy is expressed by **routing-pin presence** of the private items driver in a collection's routing configs. The private ES branch is **items-only** — there is no catalog/collection private ES driver; catalog and collection envelopes for private catalogs stay PG-only. The private items driver is opt-in only via explicit routing pin (`auto_register_for_routing = frozenset()`):

| Driver | Tier | Per-tenant index | Provided by |
|---|---|---|---|
| `items_elasticsearch_private_driver` | items | `{prefix}-{cat}-private-items` (geoid-only docs) | `modules/storage/drivers/elasticsearch_private/driver.py` |

A collection is "private" iff one of its routing configs pins the private items driver. Per-catalog privacy is configured via routing presets — `POST /admin/catalogs/{catalog_id}/presets/private_catalog`. No separate config plugin or flag is consulted.

### Cascade rule

Mixing public + private driver pins in the same routing config is rejected: it would leak item geometry through `/search` despite the catalog-wide DENY. Items-private + collection-public is allowed (public envelope, private item geometry). The cascade is enforced by apply handlers on `ItemsRoutingConfig` and `CollectionRoutingConfig` (`modules/storage/routing_config.py:_enforce_items_routing_privacy_cascade`).

### DENY policy

Catalog-wide DENY (`private_deny_{catalog_id}`) is owned by the items-private driver and blocks all `GET` requests under `/(catalog|stac|features|tiles|wfs|maps)/catalogs/{cat}/...`.

The items-private driver's `_restore_deny_policies` lifespan hook scans all catalogs at startup and re-registers DENY policies for any catalog with at least one collection whose routing configs pin a private driver.

### Operational pinning

To opt a collection into per-tenant privacy, pin a private driver in the routing config(s) — that is the privacy switch:

```
PUT /configs/catalogs/{cat}/collections/{col}/plugins/items_routing_config
{ "operations": { "INDEX": [{ "driver_ref": "items_elasticsearch_private_driver", ... }] } }
```

Per-catalog privacy can also be applied in one call via the routing preset: `POST /admin/catalogs/{cat}/presets/private_catalog`.

There is no follow-up "set private" step. The cascade validator rejects mixed public/private pins in the same routing config with a clear error message.

---

## Search Extension (`extensions/search`)

### STAC Item Search endpoints

| Method | Path | Description |
|---|---|---|
| `GET/POST` | `/search` | Unscoped item search over the public alias |
| `GET/POST` | `/search/catalogs/{catalog_id}` | Item search scoped to a single catalog |
| `POST` | `/search/catalogs/{catalog_id}/reindex` | Trigger full catalog reindex (admin) |
| `POST` | `/search/catalogs/{catalog_id}/collections/{collection_id}/reindex` | Trigger single-collection reindex (admin) |

Filters: `q`, `bbox`, `intersects`, `datetime`, `ids`, `geoid`, `external_id`, `collections`, `sortby`, `limit`, `token`, `driver`. Free-text query (`q`) searches across `id`, `title.*`, `description.*`, `keywords.*`, and all `properties.*` using ES `multi_match` with `fuzziness: AUTO`. Multilingual fields are searched transparently across all language variants. The extension is **item-only** — catalog/collection keyword search was retired; collection metadata search lives behind the STAC extension's `/stac/collections-search`.

Pagination uses ES `search_after` cursors exposed via STAC `next` links.

### GeoID item-resolve endpoint

| Method | Path | Description |
|---|---|---|
| `POST` | `/search/catalogs/{catalog_id}/items-search` | Resolve an item by exactly one of `geoid` or `external_id` |

Body carries **exactly one** of `geoid` or `external_id` (supplying both, or neither, is a 400).
A `geoid` is resolved catalog-wide (it is unique within a catalog). An `external_id` is not
globally unique, so it **requires a `collection_id`** and is resolved within that single collection
only — a bare `external_id` is a 400 (the public lookup is a targeted
resolve, never a cross-collection scan). Resolution is routing-aware: a `geoid` is served from the
catalog's private ES index when one is pinned (id fetch), otherwise — and for `external_id`, which
is not a document id — over PostgreSQL. The route is hosted by the geoid extension's
`lookup_router.py`.

Response:
```json
{
  "type": "GeoidCollection",
  "results": [
    { "geoid": "abc123", "catalog_id": "my_catalog", "collection_id": "my_collection" }
  ],
  "numberReturned": 1
}
```

### Admin Reindex endpoints

| Method | Path | Status | Description |
|---|---|---|---|
| `POST` | `/search/reindex/catalogs/{catalog_id}` | 202 | Trigger full catalog reindex |
| `POST` | `/search/reindex/catalogs/{catalog_id}/collections/{collection_id}` | 202 | Trigger single collection reindex |

Both endpoints accept an optional `driver` query parameter to restrict the reindex to a single secondary driver (e.g. `?driver=elasticsearch`).  Bulk reindex always targets the per-tenant public items index `{prefix}-{catalog_id}-items`.  Private items are dispatched per-item via the `IndexDispatcher` to `{prefix}-{catalog_id}-private-items` by `items_elasticsearch_private_driver` when the collection's `ItemsRoutingConfig` pins it.

Response:
```json
{
  "task_id": "uuid",
  "catalog_id": "my_catalog",
  "status": "queued"
}
```

**Access control**: restricted to `sysadmin` and `admin` roles via the `search_reindex_admin` ALLOW policy registered at `SearchService.lifespan`.

---

## Tasks

### Per-item private tasks (worker, incremental)

Public per-item indexing is not a standalone task type: item-tier `INDEX`-lane obligations are
durably enqueued as id-only rows into the global `tasks.storage` table and dispatched by
`StorageDrainTask`, which re-reads canonical PG state and calls the resolved `Indexer` driver
directly. The private driver retains discrete task types:

| Task type | Input | Description |
|---|---|---|
| `elasticsearch_private_index` | `geoid`, `catalog_id`, `collection_id` | Index one geoid-only doc |
| `elasticsearch_private_delete` | `geoid`, `catalog_id` | Delete one geoid doc (safe on NotFoundError) |

### Bulk tasks (Cloud Run Job or worker)

| Task type | Input | Description |
|---|---|---|
| `elasticsearch_bulk_reindex_catalog` | `catalog_id`, `driver` (optional), `page_size` (optional) | Wipe + stream all collections/items into the routing-resolved `INDEX`-lane writer |
| `elasticsearch_bulk_reindex_collection` | `catalog_id`, `collection_id`, `driver` (optional), `page_size` (optional) | Same for one collection |

Both tasks read from the routing-resolved source-of-truth reader (PG primary via the
`GEOMETRY_EXACT` hint) and write to the routing-resolved `INDEX`-lane driver (the items ES driver
by default); `driver` pins a specific `driver_ref` explicitly. Each run wipes stale documents for
its scope (`delete_by_query`, catalog-wide or collection-scoped) before reindexing. The private
driver does not ship a bulk reindex — the fresh-start cutover protocol (drop PG + delete ES
indexes pre-deploy) makes one unnecessary.

### Bulk reindex job

A dedicated bulk-reindex job handles large catalogs that would exceed the worker's timeout. It runs with a 2-hour timeout, 2 GiB RAM, and up to 2 retries.

Triggered by the admin endpoint `POST /search/reindex/catalogs/{id}`.

---

## Dependencies

```bash
pip install dynastore[elasticsearch]
# or:
poetry add elasticsearch[async]
```

## File Layout

```
models/protocols/
  search.py                # SearchProtocol — backend-agnostic search contract
  indexer.py               # Indexer / IndexTierDriver (current) + IndexerProtocol (legacy)

modules/elasticsearch/
  __init__.py              # Exports ElasticsearchModule
  module.py                # Platform-level lifecycle: shared client, index templates
                           #   (no lifecycle-event listeners — that path is retired)
  client_config.py         # EnvVar-based ES connection config
  mappings.py              # Index mappings + helpers
  collection_es_driver.py  # Public CollectionStore driver (shared
                           #   {prefix}-collections singleton)

modules/storage/drivers/elasticsearch_private/
  driver.py                # ItemsElasticsearchPrivateDriver — per-tenant
                           #   geoid-only index + DENY policy management
  tasks.py                 # elasticsearch_private_index / _delete task types
  mappings.py              # Tenant-feature mapping for items private index

extensions/search/
  __init__.py              # SearchExtension entry point
  router.py                # FastAPI router — discovers SearchProtocol, zero ES imports
  search_service.py        # SearchProtocol impl (ES-backed) + reindex dispatch
  search_models.py         # Pydantic models (SearchBody, ItemCollection, etc.)
  policies.py              # Admin-only policy for reindex endpoints

tasks/elasticsearch_indexer/
  __init__.py              # Exports bulk reindex + envelope-backfill task classes
  tasks.py                 # BulkCatalogReindexTask / BulkCollectionReindexTask
```

## Implementing an Alternative Backend

To replace Elasticsearch with another search engine (e.g. Meilisearch):

1. **Create a new driver** (e.g. `modules/meilisearch/`) implementing `Indexer` (`index`,
   `index_bulk`) and `IndexTierDriver` (`index_tiers: ClassVar[FrozenSet[str]]`) for the tiers it
   materializes.
   - Pin the new `driver_ref` under the relevant `*RoutingConfig.operations[INDEX]` (or let
     `IndexTierDriver` self-registration add it automatically).
   - No lifecycle-event listeners to register — `IndexDispatcher` calls `Indexer` directly.

2. **Create a new search service** implementing `SearchProtocol`:
   - `search_items()`, `search_catalogs()`, `search_collections()`, `search_by_geoid()`, `reindex_catalog()`, `reindex_collection()`
   - The existing router will discover it automatically via `get_protocol(SearchProtocol)`.

3. **Load the new module** via `SCOPE` or `DYNASTORE_MODULE_MODULES` instead of the ES ones.

No changes to the router, policies, or tasks infrastructure are needed — protocol discovery handles the wiring.
