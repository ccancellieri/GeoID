# The Elasticsearch Module & Search Extension

The `elasticsearch` module and its companion `search` extension provide full-text, spatial, and temporal search over DynaStore entities backed by Elasticsearch. Together they form a complete indexing pipeline with runtime-configurable per-catalog behaviours — including the **GeoID private mode** for privacy-sensitive catalogs.

This component follows the "Three Pillars" architecture: a silent `module` (event-driven indexing), a stateless API `extension` (search + admin endpoints), and asynchronous `tasks` (durable workers and Cloud Run Jobs).

---

## Protocol-Based Decoupling

The search and indexing layers are decoupled via two protocols defined in `models/protocols/`:

| Protocol | Contract | Current implementor | Discovery |
|---|---|---|---|
| `SearchProtocol` | Query execution (items, catalogs, collections) + reindex triggers | `SearchService` (ES-backed) | `get_protocol(SearchProtocol)` |
| `IndexerProtocol` | Document lifecycle (index, delete, bulk reindex, ensure index) | `ElasticsearchModule` | `get_protocol(IndexerProtocol)` |

**Why this matters:**
- The **router** (`extensions/search/router.py`) has zero imports from `modules/elasticsearch` — it discovers `SearchProtocol` at runtime.
- The **module** (`modules/elasticsearch/module.py`) exposes `IndexerProtocol` methods so other components can dispatch indexing without knowing the backend.
- To **swap backends** (Solr, Meilisearch, etc.), implement the same protocols in a new module/extension and load it instead. No changes to the router or consumers.

```
Router  ──discovers──>  SearchProtocol  ──implemented by──>  SearchService (ES)
                                                                   |
Other modules  ──discover──>  IndexerProtocol  ──implemented by──>  ElasticsearchModule
                                                                         |
                                                                    Task Queue  ──>  ES Cluster
```

---

## Module Core (`modules/elasticsearch`)

### Event-Driven Indexing Pipeline

```
Domain Event  ──>  ElasticsearchModule listener  ──>  Task Queue  ──>  Worker/Job  ──>  ES Cluster
```

1. **Event emission**: core services emit events (`ITEM_CREATION`, `CATALOG_DELETION`, `BULK_ITEM_CREATION`, etc.) during database transactions.
2. **Event listening**: `ElasticsearchModule.lifespan` registers async listeners for all CRUD events on catalogs, collections, and items. Listeners receive event kwargs directly (`catalog_id`, `collection_id`, `item_id`, `payload`).
3. **Task enqueuing**: each listener enqueues a durable background task (`elasticsearch_index`, `elasticsearch_delete`, `elasticsearch_private_index`, or `elasticsearch_private_delete`) and returns immediately — the HTTP request is never blocked by ES I/O.
4. **Execution & retries**: the worker picks up the task with heartbeat and retry guarantees.

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

Item `_source` is assembled by `build_canonical_index_doc` (`modules/elasticsearch/canonical_doc.py`) into a fixed set of lanes, one modular shape for every entity level (refs #1285/#1800/#1828). The lanes are the contract; what fills them is the per-level concern:

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

The full set is emitted on every normal write — not just `geoid`/`external_id`. A regression where the pg+es **secondary** write dropped `system`/`stats` (the secondary path failed to resolve the geoid from the canonical `id` and fell back to a `{geoid}`-only row) was fixed in #2199: resolving the geoid from `id` routes the secondary write back through `build_canonical_index_doc`, so the whole row — all system columns and all stats — is rebuilt.

**Caveat — `_source` presence vs. typed searchability.** `build_canonical_index_doc` *always* writes the `system`/`stats` lanes into `_source` (so they are visible in the index and round-trip on read). Whether they are **typed and searchable** depends on `build_item_mapping` declaring the `system`/`stats` containers, which it does *only* when the catalog's `known_fields` carries entries tagged `container="system"`/`"stats"`. The platform baseline `TIER_1_FIELDS` seeds only properties-lane STAC/EO fields — it does **not** yet seed the bounded system/stats vocabulary — and the index root is `dynamic:false`, so on a catalog without those known fields the lanes are stored but not queryable. Baking the bounded system/stats vocabulary into the base mapping is tracked in #1285.

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

Privacy is expressed by **routing-pin presence** of the private items driver in a collection's routing configs (#733 retired the standalone `CollectionPrivacy.is_private` flag). The private ES branch is **items-only** — there is no catalog/collection private ES driver; catalog and collection envelopes for private catalogs stay PG-only. The private items driver is opt-in only via explicit routing pin (`auto_register_for_routing = frozenset()`):

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

Filters: `q`, `bbox`, `intersects`, `datetime`, `ids`, `geoid`, `external_id`, `collections`, `sortby`, `limit`, `token`, `driver`. Free-text query (`q`) searches across `id`, `title.*`, `description.*`, `keywords.*`, and all `properties.*` using ES `multi_match` with `fuzziness: AUTO`. Multilingual fields are searched transparently across all language variants. The extension is **item-only** (#819) — catalog/collection keyword search was retired; collection metadata search lives behind the STAC extension's `/stac/collections-search`.

Pagination uses ES `search_after` cursors exposed via STAC `next` links.

### GeoID item-resolve endpoint

| Method | Path | Description |
|---|---|---|
| `POST` | `/search/catalogs/{catalog_id}/items-search` | Resolve an item by exactly one of `geoid` or `external_id` (#1210) |

Body carries **exactly one** of `geoid` or `external_id` (supplying both, or neither, is a 400).
A `geoid` is resolved catalog-wide (it is unique within a catalog). An `external_id` is not
globally unique, so it **requires a `collection_id`** and is resolved within that single collection
only — a bare `external_id` is a 400 (un-fao/GeoID#1204 R2: the public lookup is a targeted
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

### Per-item tasks (worker, incremental)

| Task type | Input | Description |
|---|---|---|
| `elasticsearch_index` | `entity_type`, `entity_id`, `payload` | Index a full STAC document |
| `elasticsearch_delete` | `entity_type`, `entity_id` | Delete a document (safe on NotFoundError) |
| `elasticsearch_private_index` | `geoid`, `catalog_id`, `collection_id` | Index one geoid-only doc |
| `elasticsearch_private_delete` | `geoid`, `catalog_id` | Delete one geoid doc (safe on NotFoundError) |

### Bulk tasks (Cloud Run Job or worker)

| Task type | Input | Description |
|---|---|---|
| `elasticsearch_bulk_reindex_catalog` | `catalog_id`, `mode` | Stream all collections/items, bulk-index in 500-doc batches |
| `elasticsearch_bulk_reindex_collection` | `catalog_id`, `collection_id`, `mode` | Same for one collection |

Bulk tasks clean the complementary index before reindexing:
- `mode="private"` removes stale STAC items for the catalog.
- `mode="catalog"` removes stale private docs for the catalog.
- `mode="catalog"` skips collections with `search_index=False`.

### Cloud Run Job

The `geospatial-elasticsearch-indexer` Cloud Run Job (`apps.base.yml`) handles bulk reindex for large catalogs that would exceed the worker's timeout:

```yaml
geospatial-elasticsearch-indexer:
  type: "job"
  env:
    SCOPE: "worker_task_elasticsearch_indexer"
    TASK_TIMEOUT: 7200    # 2 hours
    RAM: "2Gi"
    MAX_RETRIES: 2
```

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
  indexer.py               # IndexerProtocol — backend-agnostic indexing contract

modules/elasticsearch/
  __init__.py              # Exports ElasticsearchModule
  module.py                # Event listeners, IndexerProtocol impl
  client_config.py         # EnvVar-based ES connection config
  mappings.py              # Index mappings + helpers
  collection_es_driver.py  # Public CollectionStore driver (shared
                           #   {prefix}-collections singleton)

modules/storage/drivers/elasticsearch_private/
  driver.py                # ItemsElasticsearchPrivateDriver — per-tenant
                           #   geoid-only index + DENY policy management
  mappings.py              # Tenant-feature mapping for items private index

extensions/search/
  __init__.py              # SearchExtension entry point
  router.py                # FastAPI router — discovers SearchProtocol, zero ES imports
  search_service.py        # SearchProtocol impl (ES-backed) + reindex dispatch
  search_models.py         # Pydantic models (SearchBody, ItemCollection, etc.)
  policies.py              # Admin-only policy for reindex endpoints

tasks/elasticsearch_indexer/
  __init__.py              # Exports bulk + private task classes
  tasks.py                 # Bulk reindex + private index/delete tasks
```

## Implementing an Alternative Backend

To replace Elasticsearch with another search engine (e.g. Meilisearch):

1. **Create a new module** (e.g. `modules/meilisearch/`) implementing `IndexerProtocol`:
   - `index_document()`, `delete_document()`, `bulk_reindex()`, `ensure_index()`
   - Register event listeners in `lifespan` (same pattern as `ElasticsearchModule`).

2. **Create a new search service** implementing `SearchProtocol`:
   - `search_items()`, `search_catalogs()`, `search_collections()`, `search_by_geoid()`, `reindex_catalog()`, `reindex_collection()`
   - The existing router will discover it automatically via `get_protocol(SearchProtocol)`.

3. **Load the new module** via `SCOPE` or `DYNASTORE_MODULE_MODULES` instead of the ES ones.

No changes to the router, policies, or tasks infrastructure are needed — protocol discovery handles the wiring.
