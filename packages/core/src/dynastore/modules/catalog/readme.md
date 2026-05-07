# The Catalog Module: DynaStore's Data Management Core

## 1. Introduction

The **Catalog Module** (`dynastore.modules.catalog`) is the foundational pillar of the DynaStore architecture. It serves as the authoritative source of truth for all geospatial data, metadata, and schema management within the system.

It is designed around a **Protocol-Based Sidecar Architecture**, enabling extreme scalability, performance isolation, and infinite extensibility without modifying the core codebase.

### Key Responsibilities
*   **Data Sovereignty:** Owns the `catalogs` and `collections` tables and their lifecycles.
*   **Schema Management:** Automatically manages physical tables, partitions, and indexes via the `lifespan` process.
*   **Query Optimization:** Intelligently constructs SQL queries to fetch *only* the data requested by the user.
*   **Time Travel:** Supports full bi-temporal versioning (transaction time + valid time).
*   **Multi-Tenancy:** Enforces strict data isolation through PostgreSQL LIST partitioning.

---

## 2. Architecture: The Hub & Sidecar Pattern

The core innovation of the Catalog Module is the separation of a Feature (a row in a database) into a lightweight **Hub** and multiple specialized **Sidecars**.

### 2.1. Conceptual Diagram

```text
       [ API Layer (Extensions) ]
                  |
                  v
       [ Catalog Module (Core) ]
                  |
      +-----------+-----------+
      |           |           |
 [ Hub Table ] [ Sidecar ] [ Sidecar ]
      |           |           |
      |           v           v
      |      [ Geometry ] [ Attributes ]
      |           |           |
      +-----------+-----------+
                  |
          [ Physical Storage ]
```

### 2.2. The Hub Table (The "Spine")
The Hub is the minimal representation of a feature's existence. It contains only what is strictly necessary to identify a record and manage its lifecycle.

*   **`geoid` (UUID):** The stable, internal unique identifier for a specific version of a feature.
*   **`transaction_time`:** When the record was written.
*   **`deleted_at`:** Soft-delete marker.
*   **`validity` (TSTZRANGE):** (Optional) The valid time range for the feature.

### 2.3. The Sidecars (The "Flesh")
Data is stored in specialized sidecar tables that 1:1 join with the Hub on `geoid`. This allows the system to evolve storage strategies independently.

#### Supported Sidecars
1.  **Geometry Sidecar (`geometry`):**
    *   Stores `geom` (PostGIS Geometry), `bbox_geom`, and spatial indexes (H3, S2).
    *   Handles projection (SRID) transformations.
    *   Calculates and stores geometry statistics (Area, Length, Vertices).
2.  **Attributes Sidecar (`attributes`):**
    *   **Hybrid Storage:** Can store attributes as a flexible `JSONB` document OR as strict `COLUMNAR` fields based on configuration.
    *   Manages Identity columns (`external_id`, `asset_id`).
    *   Supports high-performance GIN and B-Tree indexing.

---

## 3. The Query Optimizer

The `QueryOptimizer` is the brain of the module. It ensures that queries are performant by constructing SQL that *only* joins the necessary sidecars.

### Workflow
1.  **Analysis:** The optimizer analyzes the incoming `QueryRequest` (Selects, Filters, Sorts).
2.  **Resolution:** It calls `determine_required_sidecars()` to find which components are implicated.
3.  **Construction:** It dynamically builds the `SELECT` and `JOIN` clauses.

### Example: "Pay-for-what-you-use"

**User Request:** "Give me the ID and Name of all schools."
*   **Query:** `SELECT external_id, name FROM schools`
*   **Optimizer Action:**
    *   Identifies `external_id` and `name` belong to `attributes` sidecar.
    *   *Result:* Joins `Hub` + `Attributes`. **Geometry table is NOT accessed.** (Huge I/O saving).

**User Request:** "Find schools within this polygon."
*   **Query:** `SELECT * FROM schools WHERE ST_Intersects(geom, ...)`
*   **Optimizer Action:**
    *   Identifies `geom` filter requires `geometry` sidecar.
    *   Identifies `*` requires `attributes` sidecar.
    *   *Result:* Joins `Hub` + `Geometry` + `Attributes`.

---

## 4. Configuration & Extensibility

The module is configured via the `CollectionPluginConfig`, which defines the behavior of a collection's data layer.

### 4.1. Collection Configuration Model

```python
class CollectionPluginConfig(BaseModel):
    # Core settings
    partitioning: PartitioningConfig
    versioning: VersioningConfig
    
    # Sidecar definitions
    sidecars: List[SidecarConfig] = [
        GeometrySidecarConfig(...),
        FeatureAttributeSidecarConfig(...)
    ]
```

### 4.2. Sidecar Configuration Examples

#### Geometry Sidecar
```json
{
  "sidecar_type": "geometry",
  "target_srid": 3857,
  "h3_resolutions": [6, 8, 10],
  "statistics": {
    "enabled": true,
    "area": {"enabled": true}
  }
}
```

#### Attributes Sidecar (Hybrid Mode)
```json
{
  "sidecar_type": "attributes",
  "storage_mode": "AUTOMATIC", 
  "attribute_schema": [
    {"name": "population", "type": "INTEGER", "index": "BTREE"},
    {"name": "category", "type": "TEXT"}
  ],
  "enable_external_id": true
}
```
*   *Note:* If `storage_mode` is `AUTOMATIC` and `attribute_schema` is present, it uses highly efficient **Columnar** storage. Otherwise, it defaults to flexible **JSONB**.

---

## 5. Partitioning & Scalability

The module uses a "Lazy" On-Demand Partitioning strategy to handle massive scale.

*   **Strategy:** `PARTITION BY LIST (catalog_id)` for collections.
*   **Mechanism:** Database Triggers (`create_partition_if_not_exists`).
*   **Benefit:** Zero-maintenance. Partitions are created automatically when the first record is inserted.
*   **Isolation:** Each tenant (Catalog) gets their own physical table, ensuring that a heavy query on one tenant does not affect others.

---

## 6. Time Travel (Versioning)

The Catalog Module supports full history tracking.

*   **Transaction Time:** When the database "knew" about the data.
*   **Valid Time:** The real-world time the data is valid for.

### Versioning Strategies
1.  **`ALWAYS_ADD_NEW`:** Every insert creates a new version.
2.  **`CREATE_NEW_VERSION`:** Updates expire the old record (set `valid_to = NOW`) and insert a new one.
3.  **`UPDATE_EXISTING`:** Overwrites the current record in place (no history).
4.  **`REJECT`:** Rejects duplicates.

---

## 7. Adding a New Sidecar (Extensibility)

To add new capabilities (e.g., Vector Embeddings for AI), you do not need to fork the core.

1.  **Define Protocol:** Implement `SidecarProtocol` in `src/dynastore/modules/catalog/sidecars/`.
2.  **Define Config:** Create a `MySidecarConfig` model.
3.  **Implement Logic:**
    *   `get_ddl()`: Define your table structure.
    *   `prepare_upsert_payload()`: Define how to process data.
4.  **Register:** Add to `SidecarRegistry`.

The `QueryOptimizer` will automatically discover and use your new sidecar.
