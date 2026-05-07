# Elasticsearch Per-Item Tasks

Incremental indexing and deletion tasks for individual STAC documents.

## Task Types

| Task type | Inputs | Description |
|---|---|---|
| `elasticsearch_index` | `entity_type`, `entity_id`, `catalog_id`, `collection_id?`, `item_id?`, `payload` | Index/update a full STAC document |
| `elasticsearch_delete` | `entity_type`, `entity_id` | Delete a document by ID (safe on NotFoundError) |

These tasks are dispatched by `ElasticsearchModule` event handlers on every
catalog/collection/item create, update, or delete event. They run in the
background worker with automatic retry and exponential backoff.

## Event Flow

```
Item created  -->  ITEM_CREATION event
              -->  ElasticsearchModule._on_item_upsert()
              -->  enqueue elasticsearch_index task
              -->  worker picks up task
              -->  ElasticsearchIndexTask.run() indexes doc into ES
```

For private catalogs, the module dispatches `elasticsearch_private_index`
(in `tasks/elasticsearch_indexer/`) instead — see that package's README.

## Files

```
tasks.py    # ElasticsearchIndexTask, ElasticsearchDeleteTask,
            # ElasticsearchIndexInputs, ElasticsearchDeleteInputs
```
