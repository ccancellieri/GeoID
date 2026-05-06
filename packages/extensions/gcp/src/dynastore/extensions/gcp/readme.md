# DynaStore GCP Extension

The GCP Extension is a foundational component for integrating DynaStore with Google Cloud Platform. It provides robust, on-demand management of Google Cloud Storage (GCS) buckets and Pub/Sub eventing channels, enabling powerful, event-driven data workflows.

---

## Index

1. [Key Concepts](#key-concepts)
2. [Configuration](#configuration)
    - [GcpCatalogBucketConfig](#gcpcatalogbucketconfig)
    - [GcpCollectionBucketConfig](#gcpcollectionbucketconfig)
3. [Pub/Sub Eventing](#pubsub-eventing)
    - [Managed Eventing](#managed-eventing)
    - [Custom Subscriptions (External Buckets)](#custom-subscriptions-external-buckets)
4. [Event-Driven Actions](#event-driven-actions)
    - [Template Interpolation](#template-interpolation)
    - [Asset Tasks (Simplified Actions)](#asset-tasks-simplified-actions)
5. [Reactive Asset Synchronization](#reactive-asset-synchronization)
6. [Examples & Use Cases](#examples--use-cases)
    - [Simple Auto-Created Bucket](#use-case-1-simple-auto-created-bucket)
    - [Pre-configured for Data Sovereignty](#use-case-2-pre-configured-for-data-sovereignty--cost)
    - [Automated Data Archival](#automated-data-archival)
    - [Shapefile Ingestion](#example-shapefile-ingestion)
    - [CSV Ingestion](#example-csv-ingestion)
    - [GDAL Metadata Extraction (gdalinfo)](#example-gdal-metadata-extraction-on-upload)
    - [Listening to External Data Drops](#use-case-external-data-drop-unmanaged-bucket)
7. [Full Endpoint Reference](#full-endpoint-reference)

---

## Key Concepts

- **Just-in-Time (JIT) Resource Creation**: GCP resources like buckets and topics are not pre-provisioned. They are created automatically and idempotently when first needed (e.g., on the first file upload to a catalog).
- **Declarative Configuration**: You define the *desired state* of your resources (like storage class or eventing rules) via API endpoints. The GCP module handles the creation and configuration to match that state.
- **Managed vs. Unmanaged Resources**: The extension distinguishes between resources it creates and owns (a **managed** bucket for a catalog) and external resources it simply interacts with (an **unmanaged** or external bucket that you subscribe to).

---

## Configuration

DynaStore uses a hierarchical configuration system for GCP. You can set defaults at the catalog level and override them for specific collections.

### `GcpCatalogBucketConfig`
This configuration applies to the entire catalog's bucket.

**Immutable Properties** (Can only be set *before* the bucket is created):
- `location` (string): The GCP region where the bucket will be created (e.g., `europe-west1`).
- `storage_class` (Enum): `STANDARD`, `NEARLINE`, `COLDLINE`, `ARCHIVE`. Default: `STANDARD`.

**Mutable Properties**:
- `cdn_enabled` (boolean): Enables Cloud CDN. Default: `false`.
- `lifecycle_rules` (List[LifecycleRule]): Rules for object lifecycle management.
- `listen_catalog_events` (boolean): If `true`, resources are cleaned up when the catalog is deleted. Default: `true`.

### `GcpCollectionBucketConfig`
Applies to objects within a specific collection.

- `custom_metadata_defaults` (Dict[str, str]): Metadata applied to all uploads in this collection.
- `asset_tasks` (Dict[str, TriggeredAction]): A registry of named task configurations (simplified actions).
- `event_actions` (Dict): Mapping of GCS event types to actions. Actions can be full objects, template IDs (from `GcpEventingConfig`), or asset task IDs (from `asset_tasks`).

---

## Pub/Sub Eventing

DynaStore monitors GCS events via Pub/Sub to trigger automated workflows.

### Managed Eventing
DynaStore automatically creates:
1. A **Pub/Sub Topic** for the catalog.
2. A **GCS Notification** on the bucket.
3. A **Push Subscription** to deliver messages to DynaStore.

### Custom Subscriptions (External Buckets)
Allows subscribing to **existing** Pub/Sub topics for external buckets. You must configure the bucket-to-topic notification manually in GCP; DynaStore will handle the subscription creation.

---

## Event-Driven Actions

Automated actions trigger when GCS events (like `OBJECT_FINALIZE`) occur. This is configured via `event_actions` in `GcpCollectionBucketConfig`.

### Template Interpolation
The `execute_request_template` supports dynamic placeholders:
- `{catalog_id}`, `{collection_id}`: Targeted catalog and collection.
- `{bucket}`, `{name}`: GCS bucket and object path.
- `{event_type}`: The triggering event (e.g., `OBJECT_FINALIZE`).
- `{asset_code}`: Extracted from object metadata.

### Asset Tasks (Simplified Actions)
You can define complex tasks once in `asset_tasks` and reference them by name in `event_actions`. This makes your configuration cleaner and more maintainable.

---

## Reactive Asset Synchronization

This is a core feature of the GCP extension. By listening to GCS events, DynaStore ensures that the **Asset Registry** stays in sync with the physical files in your buckets.

- **OBJECT_FINALIZE**: Automatically creates/updates an asset when a file is uploaded.
- **OBJECT_DELETE / OBJECT_ARCHIVE**: Automatically removes or soft-deletes the asset from the catalog when the file is removed from GCS.

This enables a "Storage-First" workflow where you can simply drop files into a bucket and have them automatically appear in your Geospatial Catalog.

---

---

## Examples & Use Cases

### Use Case 1: Simple Auto-Created Bucket
Simply initiate an upload; defaults will be used.
```bash
curl -X POST "http://localhost:80/gcp/buckets/init-upload" \
  -H "Content-Type: application/json" \
  -d '{ "filename": "data.csv", "catalog_id": "my-catalog", "asset": {"code": "A1", "asset_type": "ASSET"} }'
```

### Use Case 2: Pre-configured for Data Sovereignty & Cost
Set configuration before the first upload.
```bash
curl -X POST "http://localhost:80/gcp/catalogs/my-catalog/config/bucket" \
  -H "Content-Type: application/json" \
  -d '{ "location": "europe-west3", "storage_class": "NEARLINE" }'
```

### Use Case 3: Automated Data Archival
Leveraging Google Cloud Storage's **Lifecycle Management**, you can configure rules to automatically transition objects to cheaper storage classes (like `COLDLINE` or `ARCHIVE`) or delete them after a certain period.

DynaStore allows you to define these `lifecycle_rules` at the catalog level, which are then applied to the managed bucket.

### Use Case: External Data Drop (Unmanaged Bucket)
Listen to events from a bucket you don't own. 
**Assumption**: You configured the external bucket in GCP to send events to `projects/my-proj/topics/external-topic`.
```bash
curl -X POST "http://localhost:80/gcp/catalogs/partner-data/config/eventing" \
  -H "Content-Type: application/json" \
  -d '{
    "custom_subscriptions": [
      {
        "id": "external-listener",
        "enabled": true,
        "topic_path": "projects/my-proj/topics/external-topic",
        "subscription": {
          "subscription_id": "dynastore-external-sub"
        }
      }
    ]
  }'
```

### Example: Shapefile Ingestion
Automatically ingest a shapefile when it's uploaded to a specific collection.
```json
{
  "asset_tasks": {
    "shp_ingestion": {
      "process_id": "ingestion",
      "execute_request_template": {
        "source_file_path": "gs://{bucket}/{name}",
        "column_mapping": {
          "external_id": "id_field",
          "geometry": "geom"
        }
      }
    }
  },
  "event_actions": {
    "OBJECT_FINALIZE": ["shp_ingestion"]
  }
}
```

### Example: CSV Ingestion
```json
{
  "asset_tasks": {
    "csv_ingestion": {
      "process_id": "ingestion",
      "execute_request_template": {
        "source_file_path": "gs://{bucket}/{name}",
        "column_mapping": {
          "external_id": "station_name",
          "csv_lat_column": "lat",
          "csv_lon_column": "lon"
        }
      }
    }
  },
  "event_actions": {
    "OBJECT_FINALIZE": ["csv_ingestion"]
  }
}
```

### Example: GDAL Metadata Extraction on Upload
Extract raster/vector metadata using standard GDAL tools automatically.
```bash
curl -X PUT "http://localhost:80/gcp/catalogs/sat-data/collections/imagery/config/bucket" \
  -H "Content-Type: application/json" \
  -d '{
    "asset_tasks": {
      "analyze": {
        "process_id": "gdalinfo",
        "execute_request_template": {
          "asset_uri": "gs://{bucket}/{name}"
        }
      }
    },
    "event_actions": {
      "OBJECT_FINALIZE": ["analyze"]
    }
  }'
```

---

## Full Endpoint Reference

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| POST | `/gcp/buckets/init-upload` | Initiate resumable upload session |
| POST | `/gcp/catalogs/{id}/config/bucket` | Set catalog bucket config |
| PUT | `/gcp/catalogs/{id}/collections/{cid}/config/bucket` | Set collection bucket config |
| POST | `/gcp/catalogs/{id}/config/eventing` | Set eventing state |
| GET | `/gcp/catalogs/{id}/files.json` | List files in bucket root |
| DELETE | `/gcp/catalogs/{id}/files` | Delete file/folder |