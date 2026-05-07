# Processes Module

The `Processes` module implements the core logic for the OGC API - Processes standard within DynaStore. It managing job execution, status reporting, and result retrieval.

## Cellular Architecture Alignment

The module is fully aligned with the Cellular Architecture:
- **Context-Aware Execution**: Process execution (`execute_process`) now accepts `catalog_id` and `collection_id` to resolve the correct physical schema for task storage.
- **Specialized Endpoints**: The module supports specialized API routes for jobs and results based on the catalog and collection context:
    - `/jobs/{job_id}` (Global)
    - `/catalogs/{catalog_id}/jobs/{job_id}` (Catalog Context)
    - `/catalogs/{catalog_id}/collections/{collection_id}/jobs/{job_id}` (Collection Context)
- **Automatic Schema Resolution**: Physical schemas are dynamically resolved from the provide catalog IDs before tasks are created or retrieved.

## Key Components

- **`processes_module.py`**: The central entry point for process execution. It selects the appropriate runner, constructs the `RunnerContext`, and initiates task creation.
- **`models.py`**: OGC-compliant models for processes, execution requests, and status information.
- **`schema_gen.py`**: Internal utility for generating simplified schemas for process inputs/outputs.

## Job Management

Jobs are implemented as DynaStore Tasks. The process module uses the `Tasks` module for:
- **Async Execution**: Leveraging the background task system.
- **Status Persistence**: Storing job progress and error messages in tenant-isolated `tasks` tables.
- **Attribution**: Ensuring jobs are attributed to the correct `caller_id`.
