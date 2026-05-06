# Dynastore Asset Service

The Dynastore Asset Service is a comprehensive management layer designed to organize, discover, and link external files—referred to as Assets—to geospatial data. While the platform manages structured data like maps and coordinates, the Asset Service acts as the bridge to the raw physical files that provide deeper context, such as high-resolution satellite imagery, PDF analysis reports, or complex machine learning outputs.

## Functional Overview

The service ensures that every physical file, regardless of where it is stored (cloud buckets, local servers, or external repositories), is treated as a first-class citizen within the geospatial ecosystem. It provides a unified way to track what these files are, where they belong, and how they relate to specific geographical locations.

## Hierarchical Organization

Assets are organized following a natural hierarchy that mirrors how projects are structured:

- **Catalog-Level Assets:** Broad resources applicable to an entire project or region, such as general methodologies, summary reports, or region-wide base maps.

- **Collection-Level Assets:** Files specifically tied to a logical dataset, such as a "User Guide" for a specific sensor's data or a quality control report for a year-long time series.

- **Feature-Linked Assets:** Highly specific associations where a file is tied directly to an individual geographical record—for example, linking a specific aerial photo to the exact GPS coordinate where it was captured.

## Intelligent Discovery and Search

The service provides a powerful interface for finding information within vast data repositories:

- **Case-Insensitive Searching:** Find files using partial names or unique codes without worrying about exact formatting.

- **Contextual Filtering:** Narrow down results by project (Catalog) or dataset (Collection) to find the right file in seconds.

- **Standardized Metadata:** By following international standards for geospatial assets, the service ensures that your data remains compatible with modern third-party mapping tools and browsers.

## Automated Lifecycle Management

Efficiency is achieved through automated background processes that handle the "heavy lifting" of file management:

- **Automatic Indexing:** When files are added to supported cloud storage, the system automatically detects them and registers their metadata, making them immediately searchable without manual entry.

- **Smart Synchronization:** The service keeps the database in sync with the physical storage, ensuring that if a file is moved or updated, the platform reflects those changes instantly.

- **Seamless Cleanup:** When a project is retired, the service coordinates the cleanup of associated resources to ensure no "orphan" files are left behind, optimizing storage costs and organization.

## High-Performance Data Access

Designed for professional workflows, the service supports rapid access to metadata:

- **Bulk Retrieval:** Specialized access points allow for the retrieval of thousands of asset records at once, optimized for applications that need to synchronize large amounts of information quickly.

- **STAC Compliance:** The service uses the SpatioTemporal Asset Catalog (STAC) framework, ensuring that assets are described in a way that is understood by the global geospatial community.

## Integration with the Cloud Ecosystem

The Asset Service is built to excel in cloud-native environments. It monitors your storage buckets and automatically creates the necessary infrastructure whenever a new project begins. This "zero-touch" approach means that as soon as a file is uploaded to the cloud, it becomes part of the searchable geospatial catalog, complete with its technical properties and geographical context.

## Bucket↔DB Invariant and Reconciliation

The Asset Service enforces a 1:1 invariant between objects in the storage backend (e.g. a GCS bucket) and rows in the `assets` table:

- Every blob in the bucket should have a corresponding `assets` row.
- Every `assets` row with `kind='physical' AND status='active'` should have a corresponding blob.
- Every `status='pending'` row older than a configurable TTL (default 60 minutes) is treated as a stalled upload.

When the upload finalize event arrives without a matching PENDING row (a rare event drop or out-of-band write), the blob remains in the bucket and the activator records an `orphan_finalize` entry. Reconciliation is the recovery surface that resolves it.

Two endpoints surface the invariant:

- `GET /assets/catalogs/{catalog_id}/assets:drift` (and the collection-scoped variant) — read-only diff. Returns a `BucketReconcileReport` with counts and a per-row `drift_details` list. Inspect this before running reconcile.
- `POST /assets/catalogs/{catalog_id}/tasks/bucket-reconcile/execute` (and the collection-scoped variant) — apply mode. Imports orphan blobs as ACTIVE rows (`owned_by='gcs:reconcile_orphan'`), marks ghost rows as `status='failed'` (reason `missing_blob`), and fails stuck PENDING rows (reason `upload_abandoned`).

## Reliability and Data Safety

To prevent accidental data loss, the service employs a "Safety First" approach to deletions:

- **Soft-Deletion:** When an asset is removed, it is hidden from view but preserved in the background. This allows for easy recovery if a file was deleted in error and maintains a complete audit trail of the project's history.

- **Controlled Removal:** Permanent "hard" deletions are restricted to administrative tasks, ensuring that the core data remains secure and recoverable during standard operations.

## Use Case Examples

- **Environmental Monitoring:** Automatically link high-resolution drone imagery to specific monitoring sites as soon as the pilot uploads the files.

- **Scientific Research:** Associate peer-reviewed papers or technical documentation with the specific datasets they describe.

- **Infrastructure Management:** Link site inspection photos and maintenance logs to individual bridge or road segments on a map.

## Task Execution

The Asset Service integrates with the internal Processing API to run tasks on assets. This allows you to perform operations like metadata extraction (e.g., `gdalinfo`), format conversion, or any custom processing logic defined by the available tasks.

### Execution Modes

When triggering a task, you can control the execution behavior using the `mode` query parameter:

- **`async-execute` (Default)**: The task is queued for background execution. The API returns immediately with a `201 Created` status and a Job ID. This is recommended for long-running processes.
- **`sync-execute`**: The API waits for the task to complete before returning. This is useful for short tasks or debugging but may time out for heavy operations.
- **`dismiss`**: Signals a request to cancel or stop a running job (if supported by the runner).

### Overriding Metadata

Some tasks, such as `gdalinfo`, allow you to update or override the asset's metadata during execution. This is done by passing an `asset_metadata` dictionary within the `inputs` of the execution request.

**Example Request:**

```http
POST /assets/catalogs/{catalog_id}/assets/{asset_id}/tasks/{task_id}/execute?mode=sync-execute
Content-Type: application/json

{
  "inputs": {
    "asset_metadata": {
      "description": "Updated description from task execution",
      "properties": {
        "custom_field": "value"
      }
    },
    "other_task_param": "some_value"
  },
  "response": "document"
}
```

In this example, the task will receive the `asset_metadata` and can use it to update the asset record in the database, merging it with the results of the task execution (e.g., merging `gdalinfo` output with the custom description).
### Specialized Tasks: Ingestion

The **Ingestion Task** (task ID: `ingestion`) is context-aware. When triggered via the Asset SPI on a specific asset, it can automatically infer required parameters if they are omitted from the request:

*   **`catalog_id`**: Defaults to the asset's catalog.
*   **`collection_id`**: Defaults to the asset's collection.
*   **`ingestion_request.asset.code`**: Defaults to the asset's code.

**Minimal Example Request:**

If you are executing ingestion on a collection-level asset, you can omit the redundant IDs:

```http
POST /assets/catalogs/{catalog_id}/collections/{collection_id}/assets/{asset_id}/tasks/ingestion/execute
{
  "inputs": {
    "ingestion_request": {
      "column_mapping": { ... }
    }
  }
}
```

The system will automatically inject the `catalog_id`, `collection_id`, and linked asset code before execution.
