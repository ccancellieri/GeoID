# DynaStore OGC API - Features Extension

Expose OGC API - Features 1.0 capabilities on top of the DynaStore catalog module. The extension handles catalogs (schemas), collections (layers), and feature CRUD, wiring FastAPI endpoints to the catalog manager and DB utilities.

**Router prefix:** `/features`

**Conformance classes:** core, GeoJSON, CRS, create/replace/delete
- `http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/{core,geojson,crs}`
- `http://www.opengis.net/spec/ogcapi-features-4/1.0/conf/create-replace-delete`

**Optional CRS module:** If `dynastore.modules.crs` is installed, custom CRS URIs are resolved; otherwise only standard EPSG/opengis URIs are accepted.

**Lazy physical tables:** A collection's table is created on first item insert; listing an empty collection returns an empty `FeatureCollection`.

---

## Overview

The Features API is the primary interface for interacting with vector geospatial data in DynaStore. It implements the OGC API - Features standard, providing modern, RESTful access to feature data for a wide range of business, analytical, and integration use cases. This document serves as a comprehensive guide covering both business capabilities and technical implementation details.

---

## Feature Attributes & DynaStore Columns

### Data Model Overview
Each feature in DynaStore is stored as a row in a PostgreSQL/PostGIS table, with a set of reserved columns managed by the system and a flexible `properties` object for user-defined attributes. The API exposes these features in GeoJSON format, mapping database columns to GeoJSON fields as follows:

#### Reserved Columns (System-Managed)
| Column Name      | Type         | Description                                                      |
|------------------|--------------|------------------------------------------------------------------|
| `id`             | UUID/String  | Unique identifier for the feature (GeoJSON `id` field)           |
| `geometry`       | Geometry     | Spatial geometry (GeoJSON `geometry` field)                      |
| `catalog_id`     | String       | Catalog/tenant identifier (not exposed in GeoJSON)               |
| `collection_id`  | String       | Collection/layer identifier (not exposed in GeoJSON)             |
| `created_at`     | Timestamp    | Creation time (may be exposed as a property)                     |
| `updated_at`     | Timestamp    | Last update time (may be exposed as a property)                  |

#### User-Defined Attributes
- All additional attributes are stored in a JSONB `properties` column in the database.
- These are exposed as the `properties` object in the GeoJSON Feature.
- Users can define arbitrary key-value pairs (e.g., `name`, `type`, `status`, etc.).

#### Input (Creating/Updating Features)
- **Required:**
  - `geometry`: Must be a valid GeoJSON geometry object.
  - `properties`: Dictionary of user-defined attributes (can be empty).
- **Optional:**
  - `id`: If not provided, the system will generate a unique ID.
  - Reserved columns like `created_at`/`updated_at` are ignored if provided in input; they are managed by the system.

#### Output (Querying Features)
- Each feature is returned as a GeoJSON Feature object:
  - `id`: The feature's unique identifier.
  - `geometry`: The spatial geometry.
  - `properties`: All user-defined attributes.
  - System-managed fields like `created_at` and `updated_at` may be included in `properties` for traceability.
- Internal columns such as `catalog_id` and `collection_id` are not exposed in the API response.

#### Example: Feature Input
```json
{
  "geometry": {
    "type": "Point",
    "coordinates": [12.4924, 41.8902]
  },
  "properties": {
    "name": "Colosseum",
    "type": "Landmark",
    "status": "active"
  }
}
```

#### Example: Feature Output
```json
{
  "type": "Feature",
  "id": "b1a2c3d4-5678-90ab-cdef-1234567890ab",
  "geometry": {
    "type": "Point",
    "coordinates": [12.4924, 41.8902]
  },
  "properties": {
    "name": "Colosseum",
    "type": "Landmark",
    "status": "active",
    "created_at": "2025-12-01T10:00:00Z",
    "updated_at": "2025-12-01T10:05:00Z"
  }
}
```

### Notes
- **Do not use reserved column names** (e.g., `id`, `geometry`, `catalog_id`, `collection_id`, `created_at`, `updated_at`) as user-defined property keys to avoid conflicts.
- The API enforces schema validation and will reject features with invalid geometry or reserved property names.
- Additional metadata or system fields may be added to `properties` in future versions for auditing or traceability.

---

## Key Capabilities

### 1. Standards Compliance
- **OGC API - Features**: Full support for the OGC API - Features standard, ensuring interoperability with GIS clients, web applications, and enterprise systems.
- **GeoJSON**: All feature data is returned in GeoJSON format, the industry standard for web-based geospatial data exchange.

### 2. Core Functionalities
- **List Feature Collections**: Discover all available feature collections (layers) within a catalog.
- **Query Features**: Retrieve features from a collection with powerful filtering, spatial, and attribute query capabilities.
- **Get Single Feature**: Fetch a specific feature by its unique identifier.
- **Create Features**: Add new features to a collection (if enabled by permissions and collection configuration).
- **Update Features**: Modify existing features (if enabled).
- **Delete Features**: Remove features from a collection (if enabled).

### 3. Advanced Querying
- **Spatial Filtering**: Query features by bounding box (bbox), geometry, or spatial relationship.
- **Attribute Filtering**: Filter features using property-based expressions (e.g., `property=value`).
- **Pagination**: Efficiently page through large result sets with `limit` and `offset` parameters.
- **Sorting**: Order results by one or more properties.
- **Strict Filtering**: Query parameters and CQL2 filters are strictly validated against the physical schema and `attribute_schema`. Unknown properties return a `400 Bad Request` to prevent SQL errors.
- **CRS Support**: Request results in different Coordinate Reference Systems (CRS), as supported by the collection.

### 4. Extensibility & Integration
- **Modular Design**: The Features API is implemented as an Extension, consuming business logic from foundational Modules. This ensures clean separation of concerns and easy extensibility.
- **Stateless Operation**: All API endpoints are stateless, supporting horizontal scaling and high availability.
- **Automated Documentation**: A `/docs` endpoint serves business documentation in HTML, and the `/openapi` endpoint provides machine-readable API definitions.

---

## Typical Business Use Cases

- **Web Mapping**: Powering interactive web maps with real-time feature queries and updates.
- **Data Integration**: Enabling ETL pipelines and data synchronization with external systems.
- **Analytics**: Supporting spatial analysis, reporting, and business intelligence workflows.
- **Mobile Applications**: Providing lightweight, standards-based access for field data collection and inspection apps.
- **Interoperability**: Integrating with third-party GIS tools and platforms via OGC-compliant endpoints.

---

## Internationalization (i18n)

Like the STAC extension, the Features API fully supports the STAC Language Extension model for metadata.

### Language Negotiation

- **Query Parameter:** `?lang=fr`
- **Header:** `Accept-Language: fr-FR`

The server resolves the best matching language. If no match is found, it falls back to the default (English) or the first available language.

### Multi-Language Inputs

When creating or updating Catalogs or Collections, you can provide:

- **Simple Strings:** `"title": "My Data"` (saved as the requested language)
- **Dictionaries:** `"title": {"en": "My Data", "de": "Meine Daten"}` (saves multiple languages at once)

### Extra Fields

The `extra_metadata` field allows storing arbitrary custom properties at the Collection or Catalog level:

- These fields are merged into the response properties
- They support localization just like standard fields
- Use this for domain-specific metadata that doesn't fit into the standard OGC/STAC fields

---

## Security & Access Control
- **API Key & Auth**: Access is controlled via API keys and/or OAuth2, as configured in the deployment.
- **Fine-Grained Permissions**: Read, write, and admin permissions can be configured per collection and per user/role.
- **Audit Logging**: All write operations are logged for traceability and compliance.

---

## Performance & Scalability
- **Partitioned Storage**: Features are stored in partitioned PostgreSQL/PostGIS tables for high performance and scalability.
- **Connection Pooling**: Efficient use of database connections for high concurrency.
- **Horizontal Scaling**: Stateless API design enables scaling out under load.

---

## Error Handling & Support
- **Standardized Errors**: All errors are returned in a consistent, machine-readable format.
- **Validation**: All input is validated for schema and business rules before processing.
- **Support**: For business support, contact your DynaStore administrator or support team.

---

## Change Log & Versioning
- **API Versioning**: The Features API is versioned to ensure backward compatibility.
- **Changelog**: Major changes are documented in the project release notes.

---

# Technical Reference

## Quick Start

### 1) Create a catalog
```bash
curl -X POST http://localhost:8000/features/catalogs \
  -H "Content-Type: application/json" \
  -d '{"id":"public","title":"Public data","description":"Shared catalog"}'
```

### 2) Create a collection (OGC/STAC-aligned metadata)
```bash
curl -X POST http://localhost:8000/features/catalogs/public/collections \
  -H "Content-Type: application/json" \
  -d '{
        "id": "roads",
        "title": "Road network",
        "description": "Primary roads",
        "extent": {
          "spatial": { "bbox": [[-180,-90,180,90]] },
          "temporal": { "interval": [["2020-01-01T00:00:00Z", null]] }
        }
      }'
```

### 3) (Optional but recommended) Configure storage
```bash
curl -X PUT http://localhost:8000/features/catalogs/public/collections/roads/config \
  -H "Content-Type: application/json" \
  -d '{
        "versioning_behavior": "create_new_version",
        "geometry_storage": {
          "target_srid": 4326,
          "allowed_geometry_types": ["LineString","MultiLineString"],
          "invalid_geom_policy": "attempt_fix",
          "target_dimension": "force_2d",
          "write_bbox": true
        },
        "h3_resolutions": [5,8],
        "s2_resolutions": [10,15]
      }'
```

### 4) Add a feature
```bash
curl -X POST http://localhost:8000/features/catalogs/public/collections/roads/items \
  -H "Content-Type: application/json" \
  -d '{
        "type": "Feature",
        "id": "road-1",
        "geometry": {"type":"LineString","coordinates":[[12.48,41.89],[12.50,41.90]]},
        "properties": {"name":"Via Example","valid_from":"2024-01-01T00:00:00Z"}
      }'
```

### 5) Bulk create features
```bash
curl -X POST http://localhost:8000/features/catalogs/public/collections/roads/items \
  -H "Content-Type: application/json" \
  -d '{
        "type": "FeatureCollection",
        "features": [
          {
            "type": "Feature",
            "id": "road-2",
            "geometry": {"type":"LineString","coordinates":[[12.50,41.90],[12.52,41.91]]},
            "properties": {"name":"Via Bulk 1"}
          },
          {
            "type": "Feature",
            "id": "road-3",
            "geometry": {"type":"LineString","coordinates":[[12.52,41.91],[12.54,41.92]]},
            "properties": {"name":"Via Bulk 2"}
          }
        ]
      }'
```

### 6) Read features
```bash
# Paginated listing with optional CRS reprojection
curl "http://localhost:8000/features/catalogs/public/collections/roads/items?limit=20&offset=0&crs=EPSG:3857"

# Single feature
curl "http://localhost:8000/features/catalogs/public/collections/roads/items/road-1"
```

---

## Complete API Reference

### Landing
- `GET /features/` - Landing page with service/doc/conformance/data links.

### Catalogs
- `GET /features/catalogs?limit=10&offset=0` - List catalogs.
- `POST /features/catalogs` - Create catalog (`CatalogDefinition`).
- `GET /features/catalogs/{catalog_id}` - Catalog detail with links.
- `PUT /features/catalogs/{catalog_id}` - Update metadata.
- `DELETE /features/catalogs/{catalog_id}?force=false` - Delete catalog (204 or 404).

### Collections
- `GET /features/catalogs/{catalog_id}/collections?limit=10&offset=0` - List collections (OGC shape, with self/items links).
- `POST /features/catalogs/{catalog_id}/collections` - Create collection (`CollectionDefinition`).
- `GET /features/catalogs/{catalog_id}/collections/{collection_id}` - Collection detail (OGC enriched).
- `PUT /features/catalogs/{catalog_id}/collections/{collection_id}` - Update metadata.
- `DELETE /features/catalogs/{catalog_id}/collections/{collection_id}?force=false` - Delete collection.

### Collection Storage Configuration
- `PUT /features/catalogs/{catalog_id}/collections/{collection_id}/config` - Set immutable `LayerConfig` (409 if already set).
- `GET /features/catalogs/{catalog_id}/collections/{collection_id}/config` - Read `LayerConfig`.

### Items (Features)
- `GET /features/catalogs/{catalog_id}/collections/{collection_id}/items?limit=10&offset=0&crs=` - Paginated features; returns empty `FeatureCollection` if table not yet created. `crs` may be `EPSG:xxxx` or custom URI when CRS module is available.
- `GET /features/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}` - Fetch one feature.
- `POST /features/catalogs/{catalog_id}/collections/{collection_id}/items` - Insert one feature (`Feature`) or multiple features (`FeatureCollection`) in an atomic transaction.
- `PUT /features/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}` - Replace; path ID must match payload ID.
- `DELETE /features/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}` - Delete (404 if missing).

---

## Data Models

### CatalogDefinition
**Module:** `ogc_models.CatalogDefinition`

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Validated SQL identifier for the catalog (schema name) |
| `title` | string | Human-readable title |
| `description` | string | Detailed description |

### CollectionDefinition
**Module:** `ogc_models.CollectionDefinition`

Extends the core OGC Collection model:

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Validated SQL identifier (table name) |
| `title` | string | Human-readable title |
| `description` | string | Detailed description |
| `license` | string | License identifier (default: `proprietary`) |
| `keywords` | array | Optional array of keywords |
| `providers` | array | Optional array of provider objects |
| `summaries` | object | Optional statistics/metadata |
| `extent` | object | **Required.** Spatial and temporal extent |
| `extent.spatial.bbox` | array | Bounding box as `[minX, minY, maxX, maxY]` |
| `extent.temporal.interval` | array | Temporal range as `[[start, end], ...]` |

### Feature (GeoJSON Feature)

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Always `"Feature"` |
| `id` | string \| null | Feature identifier (generated if not provided) |
| `geometry` | object | Valid GeoJSON geometry object |
| `properties` | object | User-defined attributes (key-value pairs) |

#### System Properties (in `properties`)
- `created_at` (timestamp): Creation timestamp
- `updated_at` (timestamp): Last update timestamp

### LayerConfig
**Module:** `dynastore.modules.db_config.models`

Controls storage, partitioning, and geometry handling:

| Field | Type | Description |
|-------|------|-------------|
| `versioning_behavior` | string | e.g., `"create_new_version"` |
| `geometry_storage` | object | Geometry handling rules |
| `geometry_storage.target_srid` | integer | EPSG SRID (e.g., 4326) |
| `geometry_storage.allowed_geometry_types` | array | e.g., `["LineString", "MultiLineString"]` |
| `geometry_storage.invalid_geom_policy` | string | e.g., `"attempt_fix"` or `"reject"` |
| `geometry_storage.target_dimension` | string | e.g., `"force_2d"` |
| `geometry_storage.write_bbox` | boolean | Compute and store bbox |
| `h3_resolutions` | array | H3 spatial index resolutions |
| `s2_resolutions` | array | S2 spatial index resolutions |
| `attribute_schema` | object | Optional schema validation for properties |

---

## Geometry, CRS, and Validation

- **Geometry Processing:** Incoming geometry is parsed via Shapely and processed by `process_geometry` using `geometry_storage` rules. Invalid or disallowed geometries return `400 Bad Request`.
- **Spatial Indices:** H3/S2 indices are computed from the centroid when the layer config requests them.
- **CRS Reprojection:** The `crs` query parameter triggers on-the-fly reprojection when a custom CRS definition is available; unsupported custom URIs return `400 Bad Request`.
- **Strict Validation:** Query parameters and CQL2 filters are validated against the physical schema and `attribute_schema`. Unknown properties return `400 Bad Request` to prevent SQL errors.

---

## Error Codes & Patterns

| Code | Scenario |
|------|----------|
| `400` | Invalid geometry, mismatched item IDs, or unknown query parameters |
| `404` | Missing catalog, collection, item, or physical table |
| `409` | Attempting to set `LayerConfig` after it is already fixed |
| `500` | Unexpected errors from catalog operations |

---

## Navigation & Links

All responses include OGC-compliant navigation links:
- `self` - Link to the current resource
- `parent` - Link to the parent resource (e.g., collection for items)
- `items` - Link to child items (e.g., from catalog to collections)
- `collection` - Link to the parent collection (for items)

The root URL is derived from the incoming request (`get_root_url`, `get_url`).

---

## API Endpoint Quick Recipes

### Landing
**Endpoint:** `GET /features/`

Returns service metadata and navigation links.

**Example:**
```bash
curl http://localhost:8000/features/
```

### List Catalogs
**Endpoint:** `GET /features/catalogs?limit=10&offset=0`

Paginated listing of all catalogs.

**Example:**
```bash
curl "http://localhost:8000/features/catalogs?limit=5&offset=0"
```

### Create Catalog
**Endpoint:** `POST /features/catalogs`

**Body:** `CatalogDefinition` (`id`, `title`, `description`)

**Example:**
```bash
curl -X POST http://localhost:8000/features/catalogs \
  -H "Content-Type: application/json" \
  -d '{"id":"public","title":"Public data","description":"Shared catalog"}'
```

### Get Catalog
**Endpoint:** `GET /features/catalogs/{catalog_id}`

**Example:**
```bash
curl http://localhost:8000/features/catalogs/public
```

### Update Catalog
**Endpoint:** `PUT /features/catalogs/{catalog_id}`

**Body:** `CatalogDefinition`

**Example:**
```bash
curl -X PUT http://localhost:8000/features/catalogs/public \
  -H "Content-Type: application/json" \
  -d '{"id":"public","title":"Public data","description":"Updated description"}'
```

### Delete Catalog
**Endpoint:** `DELETE /features/catalogs/{catalog_id}?force=false`

**Example:**
```bash
curl -X DELETE "http://localhost:8000/features/catalogs/public?force=false"
```

### List Collections
**Endpoint:** `GET /features/catalogs/{catalog_id}/collections?limit=10&offset=0`

**Example:**
```bash
curl "http://localhost:8000/features/catalogs/public/collections?limit=10&offset=0"
```

### Create Collection
**Endpoint:** `POST /features/catalogs/{catalog_id}/collections`

**Body:** `CollectionDefinition` (must include `extent`)

**Example:** See Quick Start step 2 above.

### Get Collection
**Endpoint:** `GET /features/catalogs/{catalog_id}/collections/{collection_id}`

**Example:**
```bash
curl http://localhost:8000/features/catalogs/public/collections/roads
```

### Update Collection
**Endpoint:** `PUT /features/catalogs/{catalog_id}/collections/{collection_id}`

**Body:** `CollectionDefinition`

**Example:**
```bash
curl -X PUT http://localhost:8000/features/catalogs/public/collections/roads \
  -H "Content-Type: application/json" \
  -d '{"id":"roads","title":"Road network","description":"Updated",...}'
```

### Delete Collection
**Endpoint:** `DELETE /features/catalogs/{catalog_id}/collections/{collection_id}?force=false`

**Example:**
```bash
curl -X DELETE "http://localhost:8000/features/catalogs/public/collections/roads?force=false"
```

### Set Collection Storage Config
**Endpoint:** `PUT /features/catalogs/{catalog_id}/collections/{collection_id}/config`

**Body:** `LayerConfig`

**Example:** See Quick Start step 3 above.

### Get Collection Storage Config
**Endpoint:** `GET /features/catalogs/{catalog_id}/collections/{collection_id}/config`

**Example:**
```bash
curl http://localhost:8000/features/catalogs/public/collections/roads/config
```

### List Items (Features)
**Endpoint:** `GET /features/catalogs/{catalog_id}/collections/{collection_id}/items?limit=10&offset=0&crs=`

Optional `crs` parameter to reproject output.

**Example:**
```bash
curl "http://localhost:8000/features/catalogs/public/collections/roads/items?limit=20&offset=0&crs=EPSG:3857"
```

### Get Single Item
**Endpoint:** `GET /features/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}`

**Example:**
```bash
curl http://localhost:8000/features/catalogs/public/collections/roads/items/road-1
```

### Create Item (Single)
**Endpoint:** `POST /features/catalogs/{catalog_id}/collections/{collection_id}/items`

**Body:** GeoJSON `Feature`

**Example:** See Quick Start step 4 above.

### Create Items (Bulk)
**Endpoint:** `POST /features/catalogs/{catalog_id}/collections/{collection_id}/items`

**Body:** GeoJSON `FeatureCollection`

**Example:** See Quick Start step 5 above.

### Update (Replace) Item
**Endpoint:** `PUT /features/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}`

**Note:** Path ID must match `feature.id` in the payload.

**Example:**
```bash
curl -X PUT http://localhost:8000/features/catalogs/public/collections/roads/items/road-1 \
  -H "Content-Type: application/json" \
  -d '{
        "type":"Feature",
        "id":"road-1",
        "geometry":{"type":"LineString","coordinates":[[12.48,41.89],[12.51,41.91]]},
        "properties":{"name":"Via Example Updated"}
      }'
```

### Delete Item
**Endpoint:** `DELETE /features/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}`

**Example:**
```bash
curl -X DELETE http://localhost:8000/features/catalogs/public/collections/roads/items/road-1
```

---

## Naming Conventions and Reserved Words

To ensure database stability and prevent conflicts with system components, DynaStore enforces naming conventions for Catalogs and Collections, as their IDs map directly to database schema and table names, respectively.

### Reserved Catalog Names

The following names (and the names of any enabled modules, extensions, or tasks) are reserved and cannot be used for catalogs:

#### System & Application Schemas
```
admin
api
apikey
auth
catalog
cron
information_schema
pg_catalog
pg_toast
permissions
public
proxy
postgres
postgresql
raster
root
system
stats
styles
tasks
tiger
tiger_data
topology
vector
```

#### Additional Rules
- Catalog IDs must be valid SQL identifiers (max 63 characters)
- Must start with a letter or underscore
- Can contain only lowercase letters, numbers, and underscores
- Cannot be a reserved SQL keyword

### Reserved Collection Names

A Collection ID maps to a database table. In addition to following the same rules as catalog names, collection names also cannot use the following generic terms to avoid ambiguity:

```
layer / layers
feature / features
item / items
collection / collections
table
data
metadata
geom / geometry
wkb / wkt
vector
raster
```

---

## Further Reading

- [OGC API - Features Standard](https://ogcapi.ogc.org/features/)
- [GeoJSON Format Specification](https://tools.ietf.org/html/rfc7946)
- [DynaStore Detailed Architecture](../../../../README_detailed.md)

---

*This documentation is auto-served at `/features/docs` for business, integration, and product management audiences. For technical API details, see the Swagger/OpenAPI documentation at `/features/openapi`.*