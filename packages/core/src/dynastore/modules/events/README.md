# DynaStore Events Module

The Events Module is a generic, decoupled system that allows DynaStore to publish domain events (e.g., item creation, catalog deletion) and enables external applications or other internal modules to react to these events.

This is extremely powerful for building downstream integrations, such as:
- **Search Indices**: Synchronizing data to Elasticsearch, Solr, or Algolia.
- **External Notifications**: Sending Slack/Email alerts when new collections are published.
- **Data Warehousing**: Replicating specific entities to a data lake or separate database.
- **Asynchronous Processing**: Chaining workflows like thumbnail generation or data validation.

## Architecture Overview

The Events module provides:
1. **Internal Event Bus**: A high-performance, in-memory pub-sub mechanism via `EventsProtocol` for synchronous or lightweight asynchronous reactions from *other modules within the same application instances*.
2. **Durable Webhook Subscriptions**: Table-backed configurations (`event_subscriptions`) for delivering events to external HTTP endpoints.
3. **Background Dispatch**: Integrates with the DynaStore task queue to decouple HTTP request latency from event delivery reliability. Tasks are persisted in PostgreSQL and executed by background workers with automatic retries.


## Event Types

Events follow a standard naming convention: `after_{entity}_{action}`.

Current catalog events include:
- `after_catalog_creation`
- `after_catalog_update`
- `after_catalog_deletion`
- `after_catalog_hard_deletion`
- `after_collection_creation`
- `after_collection_update`
- `after_collection_deletion`
- `after_collection_hard_deletion`

## Building External Integrations

If you are building a separate microservice (like a dedicated Elasticsearch sync service) that connects to the same PostgreSQL database, you can utilize the Events module to stay perfectly in sync.

### Option 1: Webhooks (Recommended for isolated services)

You can register an external service using the Events HTTP API. DynaStore will automatically push JSON payloads to your webhook URL.

**To subscribe a webhook via API:**

```bash
curl -X POST http://localhost:80/api/v1/events/subscriptions \\
  -H "Authorization: Bearer YOUR_PLATFORM_API_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{
    "subscriber_name": "my_es_sync_app",
    "event_type": "after_item_creation",
    "webhook_url": "http://my-es-service/api/webhooks/dynastore",
    "auth_config": {"type": "api_key", "api_key": "YOUR_SECRET_KEY"}
  }'
```

**Handling the Webhook (External App):**
Your application receives an HTTP POST. It must validate the `Authorization` header against the configured API key and return a `200 OK` status quickly.

### Option 2: Building an External Proximity Application

If you have an external application that runs closer to the DynaStore database, you can connect it directly to the PostgreSQL database and listen to the `dynastore_events_channel` using standard Postgres `LISTEN`/`NOTIFY` mechanisms.

**Docker Compose Configuration Example (External App):**
```yaml
services:
  es_sync_worker:
    image: my-es-sync-worker:latest
    build:
      args:
        # Install only the extras required for this worker.
        # This mirrors the SCOPE build arg pattern used in Dynastore Dockerfiles.
        SCOPE: "db-async,elasticsearch,task-base"
    environment:
      # Connect to the DynaStore database
      - DYNASTORE_DB_URL=postgresql://postgres:postgres@db:5432/gis_dev
    depends_on:
      - db
      - elasticsearch
```

## Building Internal Integrations (DynaStore Modules)

If you are writing a new Python module inside the DynaStore ecosystem, you can natively listen to events using the `EventsProtocol` inside your module's `lifespan`. To avoid typos, always use the defined constants from `CatalogEventType`.

```python
from dynastore.modules import ModuleProtocol
from dynastore.models.protocols.events import EventsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.tasks.dispatcher import TaskDispatcher
from dynastore.modules.catalog.event_service import CatalogEventType

class MyCustomModule(ModuleProtocol):
    priority: int = 60

    async def lifespan(self, app_state: object):
        events = get_protocol(EventsProtocol)
        if events:
            # Register an async handler using constants
            events.async_event_listener(CatalogEventType.ITEM_CREATION)(self._on_item_created)
            events.async_event_listener(CatalogEventType.BULK_ITEM_CREATION)(self._on_item_bulk_created)

        yield

    async def _on_item_created(self, event: dict, **kwargs):
        catalog_id = event.get("catalog_id")
        collection_id = event.get("collection_id")
        item_id = event.get("item_id")
        
        # NOTE: Do heavy lifting via TaskDispatcher, not directly in this handler!
        dispatcher = get_protocol(TaskDispatcher)
        if dispatcher:
            from dynastore.modules.elasticsearch.tasks import ElasticsearchIndexInputs
            await dispatcher.dispatch_task(
                task_type="elasticsearch_index",
                payload_data=ElasticsearchIndexInputs(
                    entity_type="item",
                    entity_id=item_id,
                    catalog_id=catalog_id,
                    payload=event.get("payload", {})
                ).model_dump()
            )
```

### Defining Custom Events

If your new module introduces its own domain events that others should listen to, you can register them globally using `define_event`:

```python
from dynastore.modules.catalog.event_service import define_event, EventScope

# Define and register the event
MY_CUSTOM_EVENT = define_event("my_module_completed", EventScope.PLATFORM)

# Later, emit the event using the EventsProtocol
events = get_protocol(EventsProtocol)
if events:
    await events.emit(
        event_type=MY_CUSTOM_EVENT,
        catalog_id="_system_",  # Or a specific catalog ID if scoped
        payload={"status": "success", "processed_records": 100}
    )
```

## Running Sync vs Async Tasks

When reacting to an event, carefully consider performance constraints:

- **Synchronous Logic**: Never do blocking I/O (like an external API call) synchronously inside an event listener. Doing so will block the HTTP thread or the main worker loop.
- **Asynchronous Handlers**: Event handlers (like `_on_item_created`) are non-blocking async functions, but they run in the event loop. They must be extremely fast. Use them ONLY to parse the event and enqueue a background task.
- **Durable Tasks**: The actual work (syncing to Elasticsearch, moving files) should **always** be executed by the `TaskDispatcher`, which persists the task to the database (`dynastore_tasks` table) and lets DynaStore background workers execute it robustly with retry logic.


## Error Reporting and Recovery

Because downstream synchronization uses DynaStore's durable Task queuing:
1. **Automatic Retries**: If your background task (e.g., Elasticsearch indexing) fails due to network issues or service downtime, it will automatically be retried.
2. **Dead Letter Queue**: Failed tasks that exhaust all retries remain in the database with an error/failed status.
3. **Restoring from Issues**: 
   - Administrators can review failed jobs via the core database tables (usually `tasks_dead_letter` or similar, managed by the Tasks module).
   - You can manually resubmit failed tasks using the internal tools.
   - Idempotency is crucial: Ensure that your Tasks can be run safely multiple times. (e.g., an `UPSERT` operation in Elasticsearch handles duplicates automatically).

