# Elasticsearch Indexer Tasks

Bulk reindex and private index tasks for the Elasticsearch integration.

## Task Types

### Canonical OGC Process entry: `elasticsearch_indexer`

Single OGC Process exposed at:
```
POST /processes/catalogs/{catalog_id}/processes/elasticsearch_indexer/execution
```
Defined in [`definition.py`](definition.py); implemented by
[`indexer_task.py:ElasticsearchIndexerTask`](indexer_task.py).

Inputs ([`indexer_models.py:ElasticsearchIndexerRequest`](indexer_models.py)):

| Field | Required | Description |
|---|---|---|
| `catalog_id` | yes | Catalog to reindex into the per-tenant items index |
| `collection_id` | no  | If set, reindex only this collection; otherwise the whole catalog |
| `driver` | no | Restrict reindex to a single secondary driver |

Both runners can claim it:
- **`BackgroundRunner`** — runs in-process via the FastAPI background tasks pool. Use `Prefer: respond-sync` for inline execution or `Prefer: respond-async` for fire-and-poll.
- **`GcpJobRunner`** — spawns the deployed `dynastore-elasticsearch-indexer` Cloud Run Job (which advertises `TASK_TYPE=elasticsearch_indexer` in its env).

`ExecutionEngine.execute()` selects between them by mode and runner availability. The dispatcher class is a thin adapter; the actual bulk-index logic lives in [`modules/elasticsearch/bulk_reindex.py`](../../modules/elasticsearch/bulk_reindex.py) so extensions can call it directly without going through the task layer.

### Underlying bulk reindex tasks (worker / Cloud Run Job)

| Task type | Inputs | Description |
|---|---|---|
| `elasticsearch_bulk_reindex_catalog` | `catalog_id`, `driver?` | Stream all collections/items, bulk-index in 500-doc batches |
| `elasticsearch_bulk_reindex_collection` | `catalog_id`, `collection_id`, `driver?` | Same for a single collection |

Triggered by `POST /search/reindex/catalogs/{id}` (admin endpoint) or by the canonical `elasticsearch_indexer` OGC Process above.

### Per-item Private (worker, incremental)

| Task type | Inputs | Description |
|---|---|---|
| `elasticsearch_private_index` | `geoid`, `catalog_id`, `collection_id` | Index one `{geoid, catalog_id, collection_id}` doc |
| `elasticsearch_private_delete` | `geoid`, `catalog_id` | Delete one geoid doc (safe on NotFoundError) |

Dispatched via the index dispatcher (`modules/storage/index_dispatcher.py`) for collections whose routing pins the `items_elasticsearch_private_driver` (per-collection privacy).

## One-shot job

Bulk reindex for large catalogs runs as a dedicated one-shot worker job rather
than inline, so it can use a longer timeout and more memory. A deployment defines
it with the `worker_task_elasticsearch_indexer` scope, for example:

```yaml
elasticsearch-indexer:
  type: "job"
  env:
    SCOPE: "worker_task_elasticsearch_indexer"
    TASK_TIMEOUT: 7200    # 2 hours
    RAM: "2Gi"
    MAX_RETRIES: 2
```

## Files

```
__init__.py        # Exports task classes
definition.py      # OGC Process definition for `elasticsearch_indexer`
indexer_models.py  # ElasticsearchIndexerRequest
indexer_task.py    # ElasticsearchIndexerTask (canonical entry point)
tasks.py           # BulkCatalogReindexTask, BulkCollectionReindexTask
                   # (driver helpers extracted to modules/elasticsearch/bulk_reindex.py)
```
