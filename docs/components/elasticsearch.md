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

**Per-catalog config** (runtime-mutable, stored in AlloyDB):

| Field | Type | Default | Description |
|---|---|---|---|
| `private` | `bool` | `false` | Enable GeoID private indexing mode for this catalog |

Managed via the standard configuration API:

```
PUT /configs/catalogs/{catalog_id}/elasticsearch
{ "private": true }
```

When `private` changes, the `on_apply` callback fires automatically — no restart required.

---

## Per-Collection Privacy (Cycle E)

Privacy is a per-collection concept, governed by `CollectionPluginConfig.is_private` (Cycle E.2.a). Two specialized drivers — both opt-in only via explicit routing pin (`auto_register_for_routing = frozenset()`) — write privacy-sensitive data into per-tenant indexes:

| Driver | Tier | Per-tenant index | Provided by |
|---|---|---|---|
| `items_elasticsearch_private_driver` | items | `{prefix}-{cat}-private-items` (geoid-only docs) | `modules/storage/drivers/elasticsearch_private/driver.py` |
| `collection_elasticsearch_private_driver` | collection envelopes | `{prefix}-{cat}-collections-private` (full collection envelope) | `modules/storage/drivers/elasticsearch_private/collection_driver.py` (Cycle E.2.b) |

### Cascade rule

`CollectionPluginConfig.is_private == True` REQUIRES that `ItemsRoutingConfig` pins `items_elasticsearch_private_driver` in some operation (typically `INDEX`). Reverse direction is allowed (items-private + collection-public is a real shape — public envelope, private item geometry). Items-public + collection-private is rejected: it would leak item geometry through `/search` despite the per-collection DENY.

The cascade is enforced by apply handlers on both `CollectionPluginConfig` and `ItemsRoutingConfig` (`modules/storage/routing_config.py:_enforce_collection_privacy_cascade` / `_enforce_items_routing_privacy_cascade`).

### DENY policy

Catalog-wide DENY (`private_deny_{catalog_id}`) is owned by the items-private driver and blocks all `GET` requests under `/(catalog|stac|features|tiles|wfs|maps)/catalogs/{cat}/...`. The cascade rule guarantees the items-private driver is in scope whenever any collection is private, so the catalog-wide DENY covers the collection-envelope index access paths too. The collection-private driver does NOT manage its own DENY policies.

The items-private driver's `_restore_deny_policies` lifespan hook scans all catalogs at startup and re-registers DENY policies for any catalog with at least one `is_private=True` collection.

### Catalog-level default

`CatalogPolicyConfig.default_collection_privacy: Literal["public","private"]` (Cycle E.1) is consulted at collection-create time as a seed for new collections' `is_private` flag. Pure data — flipping the default does not retroactively re-flag existing collections.

### Operational pinning

To opt a collection into per-tenant privacy:

```
PUT /configs/catalogs/{cat}/collections/{col}/plugins/items_routing_config
{ "operations": { "INDEX": [{ "driver_id": "items_elasticsearch_private_driver", ... }] } }

PUT /configs/catalogs/{cat}/collections/{col}/plugins/collection_routing_config
{ "operations": { "INDEX": [{ "driver_id": "collection_elasticsearch_private_driver", ... }] } }

PUT /configs/catalogs/{cat}/collections/{col}/plugins/collection_plugin_config
{ "is_private": true }
```

Order matters: pin the routing first, then set `is_private`. The cascade validator rejects attempts that violate the rule with a clear error message guiding the operator to the missing pin.

---

## Search Extension (`extensions/search`)

### STAC Item Search endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/search` | Simple filtering (bbox, datetime, q, ids, collections, sortby, limit) |
| `POST` | `/search` | Full-featured body-based filtering with cursor pagination |
| `GET/POST` | `/search/catalogs` | Keyword search over the catalog index |
| `GET/POST` | `/search/collections` | Keyword search over the collection index |

Free-text query (`q`) searches across `id`, `title.*`, `description.*`, `keywords.*`, and all `properties.*` using ES `multi_match` with `fuzziness: AUTO`. Multilingual fields are searched transparently across all language variants.

Pagination uses ES `search_after` cursors exposed via STAC `next` links.

### GeoID Lookup endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/search/geoid/{geoid}` | Look up a single geoid — returns `{geoid, catalog_id, collection_id}` |
| `POST` | `/search/geoid` | Batch lookup — accepts `{geoids: [...], catalog_id?, limit?}` |

These endpoints query the private index (`{prefix}-geoid-*` or `{prefix}-geoid-{catalog_id}`).
When `catalog_id` is provided (query param on GET, body field on POST), the lookup is restricted
to that catalog's private index. Otherwise all private indexes are searched.

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

Both endpoints accept an optional `mode` query parameter (`"catalog"` or `"private"`). When omitted, the mode is resolved from the catalog's ES config (`private=True` -> `"private"`, otherwise `"catalog"`).

Response:
```json
{
  "task_id": "uuid",
  "catalog_id": "my_catalog",
  "mode": "private",
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
  config.py                # EnvVar-based ES connection config
  mappings.py              # Index mappings + helpers (incl.
                           #   get_tenant_collections_private_index — Cycle E.2.b)
  collection_es_driver.py  # Public CollectionStore driver (shared
                           #   {prefix}-collections singleton)

modules/storage/drivers/elasticsearch_private/
  driver.py                # ItemsElasticsearchPrivateDriver — per-tenant
                           #   geoid-only index + DENY policy management
  collection_driver.py     # CollectionElasticsearchPrivateDriver — per-tenant
                           #   collection envelope index (Cycle E.2.b)
  mappings.py              # Tenant-feature mapping for items private index

extensions/search/
  __init__.py              # SearchExtension entry point
  router.py                # FastAPI router — discovers SearchProtocol, zero ES imports
  search_service.py        # SearchProtocol impl (ES-backed) + reindex dispatch
  search_models.py         # Pydantic models (SearchBody, ItemCollection, etc.)
  policies.py              # Admin-only policy for reindex endpoints

tasks/elasticsearch/
  tasks.py                 # Per-item ElasticsearchIndexTask, ElasticsearchDeleteTask

tasks/elasticsearch_indexer/
  __init__.py              # Exports bulk + private task classes
  tasks.py                 # Bulk reindex + private index/delete tasks
```

## Implementing an Alternative Backend

To replace Elasticsearch with another search engine (e.g. Solr, Meilisearch):

1. **Create a new module** (`modules/solr/`) implementing `IndexerProtocol`:
   - `index_document()`, `delete_document()`, `bulk_reindex()`, `ensure_index()`
   - Register event listeners in `lifespan` (same pattern as `ElasticsearchModule`).

2. **Create a new search service** implementing `SearchProtocol`:
   - `search_items()`, `search_catalogs()`, `search_collections()`, `search_by_geoid()`, `reindex_catalog()`, `reindex_collection()`
   - The existing router will discover it automatically via `get_protocol(SearchProtocol)`.

3. **Load the new module** via `SCOPE` or `DYNASTORE_MODULE_MODULES` instead of the ES ones.

No changes to the router, policies, or tasks infrastructure are needed — protocol discovery handles the wiring.
