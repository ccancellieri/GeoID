# Elasticsearch Integration Module

This module automatically indexes DynaStore entities (Catalogs, Collections, Items, Assets) into an Elasticsearch cluster, allowing for highly optimized free-text, spatial, and temporal searches.

## Protocols

The module ships ES-backed driver implementations of the routing-config rails:
- `catalog_elasticsearch_driver` (`CatalogStore`) — read/write for the platform-wide `{prefix}-catalogs` and `{prefix}-collections` indices.
- `items_elasticsearch_driver` / `items_elasticsearch_private_driver` (`Indexer` + `ItemsSearchProtocol`) — per-tenant `{prefix}-{cat}-items` indices; the private variant is what "private" collections pin under `ItemsRoutingConfig.operations[INDEX]` (#733 made the private-driver pin the canonical expression of privacy — there is no separate `is_private` flag).

The companion search extension implements `SearchProtocol` (`models/protocols/search.py`), discovered at runtime.

To swap to a different backend, implement the same protocols in a new module/extension and pin the new driver_ref under the relevant `*RoutingConfig.operations[...]`.

## Architecture

DynaStore routes catalog / collection / item INDEX hops through routing-config rails, not through this module's lifecycle-event listeners (the listener path that owned these dispatches before #825 was retired — it ran in parallel to the canonical rails and emitted misleading "Indexing collection …" log lines on every routing-config PUT).

1. **Items.** `IndexDispatcher.fan_out_bulk(ctx, ops)` reads `ItemsRoutingConfig.operations[INDEX]` via the entity-aware resolver (#820) and dispatches to whichever Indexer drivers are pinned there — typically `items_elasticsearch_driver` (public) or `items_elasticsearch_private_driver` (private). Soft-delete fan-out uses the same dispatcher.
2. **Collections.** `collection_router._dispatch_collection_index` calls `IndexDispatcher.fan_out_bulk` with `entity_type='collection'`; resolver returns `CollectionRoutingConfig.operations[INDEX]`. Both upsert and hard-delete paths trigger the dispatch.
3. **Catalogs.** `catalog_router.upsert_catalog_metadata` and `catalog_service.delete_catalog` (soft + hard) emit `CATALOG_METADATA_CHANGED`; `ReindexWorker` consumes the event and fans out to `CatalogRoutingConfig.operations[INDEX]` (defaults pin `catalog_elasticsearch_driver` with `OUTBOX`-durable async semantics).

This module's lifespan still creates the shared `{prefix}-catalogs` / `{prefix}-collections` indices and the `{prefix}-items` public alias at startup, and `bulk_reindex(catalog_id, collection_id=…)` remains the entry point for full-index rebuilds (Cloud Run Job task types `elasticsearch_bulk_reindex_{catalog,collection}`).

## Configuration

The Elasticsearch module is installed as part of the `elasticsearch` extra. Enable it
by installing the correct extras for your deployment:

```bash
pip install dynastore[elasticsearch]
# or via SCOPE in your deployment config:
# SCOPE=elasticsearch
```

### Connection (environment variables, deploy-time)

| Variable | Default | Description |
|---|---|---|
| `ES_HOST` | `localhost` | ES/OpenSearch hostname or IP |
| `ES_PORT` | `9200` | REST API port |
| `ES_USE_SSL` | `false` | `true` to use HTTPS (Elastic Cloud, on-prem TLS) |
| `ES_VERIFY_CERTS` | `true` | `false` to skip TLS certificate verification (self-signed) |
| `ES_API_KEY` | — | API key authentication (preferred over basic auth) |
| `ES_USERNAME` | — | Basic auth username |
| `ES_PASSWORD` | — | Basic auth password |
| `ES_INDEX_PREFIX` | `dynastore` | Prefix applied to all index names |

Transport tuning (timeout, pool size, retries) lives in PluginConfig
`platform/protocols/storage/elasticsearch/client` (`ElasticsearchClientConfig`) — edit via
`PUT /configs/plugins/elasticsearch_client_config` (effective at next module lifespan).

#### On-Premise / OpenSearch Example

```env
ES_HOST=my-opensearch-node.internal
ES_PORT=9200
ES_USE_SSL=true
ES_VERIFY_CERTS=false   # self-signed cert
ES_API_KEY=abc123==
ES_INDEX_PREFIX=geoid_prod
```

#### Startup Connectivity Check

On startup, `client.init()` attempts a ping (`GET /`) against the configured host.
On success the cluster name and version are logged at `INFO`. On failure a `WARNING` is
emitted and the service continues — ES is optional. Indexing tasks will fail (and retry)
until the connection is restored.

#### License Note

This module is compatible with **OpenSearch 2.x** (Apache 2.0) and
**Elasticsearch ≤7.10.2** (Apache 2.0). Elasticsearch ≥7.11 uses SSPL/ELv2,
which is incompatible with this project's Apache 2.0 license.
The Docker Compose files in this repository use
`opensearchproject/opensearch:2.17.0` for this reason.

### Per-Catalog Config (runtime-mutable, stored in AlloyDB)

Catalog-tier routing-template defaults are governed by `CatalogRoutingTemplates`
(`modules/catalog/catalog_config.py`) via the standard configuration API.
The embedded `CatalogRoutingDefaults` (`modules/storage/routing_config.py`)
carries optional `items_routing` / `collection_routing` templates seeded
onto each newly-created collection in the catalog. Setting them to
private-driver template auto-seeds new collections as private (items tier only):

```
PUT /configs/catalogs/{catalog_id}/plugins/catalog_routing_templates
{
  "collection_defaults": {
    "items_routing": { "operations": { "index": [ { "driver_ref": "items_elasticsearch_private_driver" } ] } }
  }
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `collection_defaults.items_routing` | `ItemsRoutingConfig?` | `null` | Template seeded as each new collection's `ItemsRoutingConfig`. Pure data — flipping it does not retroactively re-write existing collections. |
| `collection_defaults.collection_routing` | `CollectionRoutingConfig?` | `null` | Template seeded as each new collection's `CollectionRoutingConfig`. Collection envelopes are PG-only for private catalogs — no ES collection-private index (#1047). |

Per-collection privacy is expressed by the presence of `items_elasticsearch_private_driver` in the collection's items routing config (#733 retired the `CollectionPrivacy.is_private` flag; #1047 retired the collection-private ES driver).

## Index Design & Mappings

The module automatically configures and manages the following Elasticsearch indices:

| Index | Entity | Key mappings |
|---|---|---|
| `{prefix}-catalogs` | Catalog | keyword + multilingual free-text |
| `{prefix}-collections` | Collection | `geo_shape` (spatial extent), date range (temporal) |
| `{prefix}-items` | Item | `geo_shape` (geometry), STAC datetime, dynamic template |
| `{prefix}-assets` | Asset | `item_id`, `asset_key`, `roles`, `href` |
| `{prefix}-geoid-{catalog_id}` | Private item (per-tenant) | `dynamic: false`, `geoid`, `catalog_id`, `collection_id` only |

## Per-Tenant Privacy

Privacy is owned by `items_elasticsearch_private_driver`
(`modules/storage/drivers/elasticsearch_private/`), which:

1. Writes items as `{geoid, catalog_id, collection_id}` to `{prefix}-geoid-{catalog_id}` — no geometry, no STAC metadata.
2. Manages its own DENY policies on its lifecycle (apply on `ensure_storage`, revoke on `drop_storage`, restore on lifespan-startup).
3. Opts out of items-tier auto-default routing (`auto_register_for_routing = frozenset()`); collections turn on the private driver via an explicit `ItemsRoutingConfig` pin (#733 — pinning `items_elasticsearch_private_driver` IS the privacy signal).

Geoid lookups remain available via `GET /search/geoid/{geoid}` and `POST /search/geoid` (batch).

See `docs/components/elasticsearch.md` for the full lifecycle description.

## Tasks

### Per-item (worker, incremental, dispatched via `IndexDispatcher`)
- `elasticsearch_index` — index a full STAC document
- `elasticsearch_delete` — delete a document (safe on NotFoundError)
- `elasticsearch_private_index` — index one geoid-only document into the per-tenant private index
- `elasticsearch_private_delete` — delete one geoid document from the per-tenant private index

### Bulk (Cloud Run Job or worker)
- `elasticsearch_bulk_reindex_catalog` — reindex all items in a catalog (500-doc batches)
- `elasticsearch_bulk_reindex_collection` — reindex one collection

Bulk tasks are triggered by the admin endpoint `POST /search/reindex/catalogs/{id}`.

## Dependencies

The module requires the official Elasticsearch async Python client:
```bash
poetry add elasticsearch[async]
```
