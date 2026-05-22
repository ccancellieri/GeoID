# Search Extension

STAC-compliant search API over indexed DynaStore entities. Discovers its backend
via `SearchProtocol` — the router has zero imports from any search implementation.

## Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/search` | Simple item filtering (bbox, datetime, q, ids, collections, sortby, limit) |
| `POST` | `/search` | Full-featured body-based item search with cursor pagination |
| `GET/POST` | `/search/catalogs` | Keyword search over the catalog index |
| `GET/POST` | `/search/collections` | Keyword search over the collection index |
| `POST` | `/search/reindex/catalogs/{catalog_id}` | Trigger full catalog reindex (admin, 202) |
| `POST` | `/search/reindex/catalogs/{catalog_id}/collections/{collection_id}` | Trigger single collection reindex (admin, 202) |

## Protocol Decoupling

The router discovers `SearchProtocol` at runtime via `get_protocol(SearchProtocol)`.
The current implementation is `SearchService` (ES-backed), but any class satisfying
the protocol contract will work transparently.

`SearchProtocol` methods:
- `search_items(body, base_url)` -> `ItemCollection`
- `search_catalogs(body, base_url)` -> `GenericCollection`
- `search_collections(body, base_url)` -> `GenericCollection`
- `reindex_catalog(catalog_id, mode?)` -> `Dict`
- `reindex_collection(catalog_id, collection_id, mode?)` -> `Dict`

> **Note:** the `POST /search/catalogs/{cat}/items-search` item-resolve endpoint
> is served by the geoid extension's `lookup_router.py` (PG-backed). It is not
> part of this extension's surface and does not depend on `SearchProtocol`.

## Models

Defined in `search_models.py`:
- `SearchBody` — STAC Item Search request (q, bbox, datetime, intersects, ids, collections, sortby, limit, token)
- `CatalogSearchBody` — Catalog/collection keyword search (q, ids, limit, token)
- `ItemCollection` — STAC FeatureCollection response
- `GenericCollection` — Entity collection response (catalogs/collections)

## Access Control

- Search endpoints: open to all authenticated users.
- Reindex endpoints: restricted to `sysadmin` and `admin` roles via the `search_reindex_admin` ALLOW policy (registered in `policies.py`).

## Files

```
__init__.py          # Extension entry point
router.py            # FastAPI router — protocol discovery, zero backend imports
search_service.py    # SearchProtocol impl (ES-backed)
search_models.py     # Pydantic request/response models
policies.py          # Admin-only policy for reindex endpoints
```
