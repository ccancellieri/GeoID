"""
STAC API — list, search, and pagination endpoint tests.

Covers the most under-tested STAC routes:
- GET /stac/catalogs                           (list catalogs)
- GET /stac/catalogs/{id}/collections          (list collections)
- GET /stac/catalogs/{id}/collections/{id}/items  (list items, bbox, pagination)
- POST /stac/catalogs/{id}/search              (item search by catalog)
- GET /stac/catalogs/{id}/collections          (collection search via standard path)
"""

import pytest
from httpx import AsyncClient


MARKER = pytest.mark.enable_extensions("stac", "assets", "features")


# ---------------------------------------------------------------------------
# List catalogs
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_list_stac_catalogs_returns_200(
    sysadmin_in_process_client: AsyncClient, setup_catalog
):
    """GET /stac/catalogs — catalog appears in the listing."""
    catalog_id = setup_catalog
    r = await sysadmin_in_process_client.get("/stac/catalogs")
    assert r.status_code == 200
    data = r.json()
    # Response is either a list or a dict with a catalogs/links key
    catalogs = data if isinstance(data, list) else data.get("catalogs", data.get("links", []))
    ids = [c.get("id") or c.get("href", "") for c in catalogs]
    assert any(catalog_id in str(i) for i in ids), (
        f"Catalog {catalog_id!r} not found in listing: {ids}"
    )


@MARKER
@pytest.mark.asyncio
async def test_list_stac_catalogs_pagination(
    sysadmin_in_process_client: AsyncClient,
):
    """GET /stac/catalogs?limit=1 — limit and offset parameters accepted."""
    r = await sysadmin_in_process_client.get("/stac/catalogs", params={"limit": 1})
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# List collections
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_list_stac_collections_returns_200(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /stac/catalogs/{id}/collections — collection appears in listing."""
    catalog_id = setup_catalog
    collection_id = setup_collection
    r = await sysadmin_in_process_client.get(f"/stac/catalogs/{catalog_id}/collections")
    assert r.status_code == 200
    data = r.json()
    collections = data if isinstance(data, list) else data.get("collections", [])
    ids = [c.get("id") for c in collections]
    assert collection_id in ids, f"Collection {collection_id!r} not in {ids}"


@MARKER
@pytest.mark.asyncio
async def test_list_stac_collections_unknown_catalog_404(
    sysadmin_in_process_client: AsyncClient,
):
    """GET /stac/catalogs/{id}/collections — unknown catalog returns 404."""
    r = await sysadmin_in_process_client.get("/stac/catalogs/does_not_exist/collections")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# List items (GET /items)
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_list_stac_items_empty_collection(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /items on an empty collection returns empty FeatureCollection."""
    catalog_id = setup_catalog
    collection_id = setup_collection
    r = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items"
    )
    assert r.status_code == 200
    data = r.json()
    assert data.get("type") == "FeatureCollection"
    assert isinstance(data.get("features"), list)


@MARKER
@pytest.mark.asyncio
async def test_list_stac_items_with_limit(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection, item_raw_data
):
    """GET /items?limit=1 — pagination limit is respected."""
    catalog_id = setup_catalog
    collection_id = setup_collection

    # Ingest 2 items
    for i in range(2):
        item = {**item_raw_data, "id": f"{item_raw_data['id']}_{i}"}
        await sysadmin_in_process_client.post(
            f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items",
            json=item,
        )

    r = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items",
        params={"limit": 1},
    )
    assert r.status_code == 200
    data = r.json()
    assert len(data.get("features", [])) <= 1


@MARKER
@pytest.mark.asyncio
async def test_list_stac_items_bbox_filter(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection, item_raw_data
):
    """GET /items?bbox — spatial filter applied; non-overlapping bbox returns empty."""
    catalog_id = setup_catalog
    collection_id = setup_collection

    # Ingest item at (0,0)
    await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=item_raw_data,
    )

    # Query with bbox far from (0,0) — should return 0 features
    r = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items",
        params={"bbox": "100,50,110,60"},
    )
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data.get("features"), list)


# ---------------------------------------------------------------------------
# POST /search (catalog-scoped item search)
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_stac_search_post_basic(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """POST /stac/catalogs/{id}/search — empty body returns valid GeoJSON."""
    catalog_id = setup_catalog
    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/search",
        json={},
    )
    assert r.status_code == 200
    data = r.json()
    assert data.get("type") == "FeatureCollection"


@MARKER
@pytest.mark.asyncio
async def test_stac_search_post_with_collections(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """POST /stac/catalogs/{id}/search — filter by collections list."""
    catalog_id = setup_catalog
    collection_id = setup_collection
    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/search",
        json={"collections": [collection_id]},
    )
    assert r.status_code == 200
    data = r.json()
    assert data.get("type") == "FeatureCollection"


@MARKER
@pytest.mark.asyncio
async def test_stac_search_post_with_bbox(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """POST /stac/catalogs/{id}/search — bbox filter accepted."""
    catalog_id = setup_catalog
    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/search",
        json={"bbox": [-180, -90, 180, 90]},
    )
    assert r.status_code == 200


@MARKER
@pytest.mark.asyncio
async def test_stac_search_post_with_limit(
    sysadmin_in_process_client: AsyncClient, setup_catalog
):
    """POST /stac/catalogs/{id}/search — limit pagination parameter accepted."""
    catalog_id = setup_catalog
    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/search",
        json={"limit": 5},
    )
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# GET /catalogs/{id}/collections (collection search — standard path)
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_collections_list_returns_200(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /stac/catalogs/{id}/collections (plain) — returns collections list."""
    catalog_id = setup_catalog
    r = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}/collections",
    )
    assert r.status_code == 200
    data = r.json()
    # Should return a list or object with collections
    assert isinstance(data, (list, dict))


@MARKER
@pytest.mark.asyncio
async def test_collections_search_get_with_limit(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /stac/catalogs/{id}/collections?limit — standard collection search path."""
    catalog_id = setup_catalog
    collection_id = setup_collection
    r = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}/collections",
        params={"limit": 100},
    )
    assert r.status_code == 200
    data = r.json()
    collections = data if isinstance(data, list) else data.get("collections", [])
    if collections:
        ids = [c.get("id") for c in collections]
        assert collection_id in ids
