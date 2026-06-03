"""
STAC API standards-compliance tests.

Covers:
- GET /stac/catalogs/{id}/search  (item search, standard GET method)
- sortby on POST and GET item search
- GET /stac/catalogs/{id}/collections with Collection Search params (bbox, q, sortby)
- Removed non-standard routes return 404 or are absent:
    /stac/collections-search
    /stac/collections/search
    /stac/search  (bare deprecated path)
"""

import pytest
from httpx import AsyncClient


MARKER = pytest.mark.enable_extensions("stac", "assets", "features")

_ITEM_1 = {
    "id": "stac_std_item_1",
    "stac_version": "1.0.0",
    "type": "Feature",
    "geometry": {"type": "Point", "coordinates": [10.0, 10.0]},
    "bbox": [10.0, 10.0, 10.0, 10.0],
    "properties": {"datetime": "2024-01-01T10:00:00Z"},
    "links": [{"rel": "self", "href": "http://example.com/item1"}],
    "assets": {},
}
_ITEM_2 = {
    "id": "stac_std_item_2",
    "stac_version": "1.0.0",
    "type": "Feature",
    "geometry": {"type": "Point", "coordinates": [20.0, 20.0]},
    "bbox": [20.0, 20.0, 20.0, 20.0],
    "properties": {"datetime": "2024-06-01T10:00:00Z"},
    "links": [{"rel": "self", "href": "http://example.com/item2"}],
    "assets": {},
}


# ---------------------------------------------------------------------------
# GET /catalogs/{cat}/search — standard item search GET
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_item_search_get_empty_returns_feature_collection(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /stac/catalogs/{id}/search with no params returns a GeoJSON FeatureCollection."""
    catalog_id = setup_catalog
    r = await sysadmin_in_process_client.get(f"/stac/catalogs/{catalog_id}/search")
    assert r.status_code == 200, r.text
    data = r.json()
    assert data.get("type") == "FeatureCollection"
    assert isinstance(data.get("features"), list)


@MARKER
@pytest.mark.asyncio
async def test_item_search_get_same_shape_as_post(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET and POST /stac/catalogs/{id}/search return the same shape for equivalent params."""
    catalog_id = setup_catalog
    collection_id = setup_collection

    # Ingest one item
    r_put = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=_ITEM_1,
    )
    assert r_put.status_code in (200, 201), r_put.text

    # POST search
    r_post = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/search",
        json={"collections": [collection_id], "limit": 5},
    )
    assert r_post.status_code == 200, r_post.text
    post_data = r_post.json()

    # GET search — equivalent params
    r_get = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}/search",
        params={"collections": collection_id, "limit": 5},
    )
    assert r_get.status_code == 200, r_get.text
    get_data = r_get.json()

    # Both must be FeatureCollections
    assert post_data.get("type") == "FeatureCollection"
    assert get_data.get("type") == "FeatureCollection"
    # Both return same feature count
    assert len(post_data.get("features", [])) == len(get_data.get("features", []))


@MARKER
@pytest.mark.asyncio
async def test_item_search_get_bbox_filter(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /stac/catalogs/{id}/search?bbox — spatial filter respected."""
    catalog_id = setup_catalog
    collection_id = setup_collection

    await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=_ITEM_1,
    )

    # Bbox that covers item1 (10,10)
    r = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}/search",
        params={"bbox": "9,9,11,11", "collections": collection_id},
    )
    assert r.status_code == 200, r.text
    assert r.json().get("type") == "FeatureCollection"


@MARKER
@pytest.mark.asyncio
async def test_item_search_get_ids_filter(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /stac/catalogs/{id}/search?ids — ids filter accepted."""
    catalog_id = setup_catalog
    collection_id = setup_collection

    r_ins = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=_ITEM_1,
    )
    assert r_ins.status_code in (200, 201)
    item_id = r_ins.json()["id"]

    r = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}/search",
        params={"ids": item_id},
    )
    assert r.status_code == 200, r.text
    feats = r.json().get("features", [])
    # At least one result with the expected id
    assert any(f.get("id") == item_id for f in feats)


@MARKER
@pytest.mark.asyncio
async def test_item_search_get_limit_param(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /stac/catalogs/{id}/search?limit — pagination parameter accepted."""
    catalog_id = setup_catalog

    r = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}/search",
        params={"limit": 1},
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert len(data.get("features", [])) <= 1


@MARKER
@pytest.mark.asyncio
async def test_item_search_get_invalid_bbox_returns_400(
    sysadmin_in_process_client: AsyncClient, setup_catalog
):
    """GET /stac/catalogs/{id}/search with malformed bbox returns 400."""
    catalog_id = setup_catalog
    r = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}/search",
        params={"bbox": "not-valid"},
    )
    assert r.status_code == 400, r.text


# ---------------------------------------------------------------------------
# sortby — item search GET and POST
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_item_search_post_sortby_asc(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """POST /stac/catalogs/{id}/search sortby=['+datetime'] orders by datetime ascending."""
    catalog_id = setup_catalog
    collection_id = setup_collection

    for item in (_ITEM_1, _ITEM_2):
        await sysadmin_in_process_client.post(
            f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items",
            json=item,
        )

    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/search",
        json={
            "collections": [collection_id],
            "sortby": ["+datetime"],
            "limit": 10,
        },
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data.get("type") == "FeatureCollection"
    # Ascending: earlier date should appear first (when two items)
    feats = data.get("features", [])
    if len(feats) >= 2:
        datetimes = [f.get("properties", {}).get("datetime", "") for f in feats]
        assert datetimes == sorted(datetimes), f"Expected ascending order, got {datetimes}"


@MARKER
@pytest.mark.asyncio
async def test_item_search_post_sortby_desc(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """POST /stac/catalogs/{id}/search sortby=['-datetime'] orders by datetime descending."""
    catalog_id = setup_catalog
    collection_id = setup_collection

    for item in (_ITEM_1, _ITEM_2):
        await sysadmin_in_process_client.post(
            f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items",
            json=item,
        )

    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/search",
        json={
            "collections": [collection_id],
            "sortby": ["-datetime"],
            "limit": 10,
        },
    )
    assert r.status_code == 200, r.text
    data = r.json()
    feats = data.get("features", [])
    if len(feats) >= 2:
        datetimes = [f.get("properties", {}).get("datetime", "") for f in feats]
        assert datetimes == sorted(datetimes, reverse=True), (
            f"Expected descending order, got {datetimes}"
        )


@MARKER
@pytest.mark.asyncio
async def test_item_search_get_sortby_param(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /stac/catalogs/{id}/search?sortby=+datetime returns 200 (sortby wired on GET)."""
    catalog_id = setup_catalog
    collection_id = setup_collection

    for item in (_ITEM_1, _ITEM_2):
        await sysadmin_in_process_client.post(
            f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items",
            json=item,
        )

    r = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}/search",
        params={"collections": collection_id, "sortby": "+datetime"},
    )
    assert r.status_code == 200, r.text
    assert r.json().get("type") == "FeatureCollection"


@MARKER
@pytest.mark.asyncio
async def test_item_search_post_invalid_sortby_returns_400(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """POST /stac/catalogs/{id}/search with an unsupported sortby field returns 400."""
    catalog_id = setup_catalog
    r = await sysadmin_in_process_client.post(
        f"/stac/catalogs/{catalog_id}/search",
        json={"sortby": ["+nonexistent_xyz_field"]},
    )
    assert r.status_code == 400, r.text


# ---------------------------------------------------------------------------
# Collection Search — GET /catalogs/{cat}/collections with search params
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_collection_search_get_with_q(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /catalogs/{cat}/collections?q — free-text filter accepted, returns 200."""
    catalog_id = setup_catalog
    r = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}/collections",
        params={"q": "nonexistent_xyz_keyword_abc"},
    )
    # May return empty list but must be 200
    assert r.status_code == 200, r.text
    data = r.json()
    # Response must be a collection-search shape or plain list
    assert isinstance(data, (list, dict))


@MARKER
@pytest.mark.asyncio
async def test_collection_search_get_q_matches_collection(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection, collection_data
):
    """GET /catalogs/{cat}/collections?q — term matching collection id returns the collection."""
    catalog_id = setup_catalog
    collection_id = setup_collection

    # Search by a substring of the collection id
    term = collection_id[:4]  # first 4 chars, likely unique enough
    r = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}/collections",
        params={"q": term, "limit": 100},
    )
    assert r.status_code == 200, r.text
    data = r.json()
    collections = data if isinstance(data, list) else data.get("collections", [])
    ids = [c.get("id") for c in collections]
    # The collection must be present (substring match on id)
    assert collection_id in ids, (
        f"Collection {collection_id!r} not found when querying q={term!r}: {ids}"
    )


@MARKER
@pytest.mark.asyncio
async def test_collection_search_get_bbox(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /catalogs/{cat}/collections?bbox — bbox filter accepted, returns 200."""
    catalog_id = setup_catalog
    r = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}/collections",
        params={"bbox": "-180,-90,180,90"},
    )
    assert r.status_code == 200, r.text


@MARKER
@pytest.mark.asyncio
async def test_collection_search_get_sortby(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /catalogs/{cat}/collections?sortby — sortby param routed through search, returns 200."""
    catalog_id = setup_catalog
    r = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}/collections",
        params={"sortby": "+code"},
    )
    assert r.status_code == 200, r.text


@MARKER
@pytest.mark.asyncio
async def test_collection_search_get_limit_and_offset(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /catalogs/{cat}/collections?limit&offset — pagination routed through search."""
    catalog_id = setup_catalog
    r = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}/collections",
        params={"limit": 1, "offset": 0},
    )
    assert r.status_code == 200, r.text
    data = r.json()
    collections = data if isinstance(data, list) else data.get("collections", [])
    assert len(collections) <= 1


@MARKER
@pytest.mark.asyncio
async def test_collection_search_get_context_block(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /catalogs/{cat}/collections with search params includes a context block."""
    catalog_id = setup_catalog
    r = await sysadmin_in_process_client.get(
        f"/stac/catalogs/{catalog_id}/collections",
        params={"limit": 5, "q": "a"},
    )
    assert r.status_code == 200, r.text
    data = r.json()
    # When routed through search, response must include context block
    if isinstance(data, dict):
        ctx = data.get("context", {})
        assert "limit" in ctx
        assert "matched" in ctx
        assert "returned" in ctx


# ---------------------------------------------------------------------------
# Removed non-standard routes must be absent / 404
# ---------------------------------------------------------------------------


@MARKER
@pytest.mark.asyncio
async def test_collections_search_post_route_removed(
    sysadmin_in_process_client: AsyncClient,
):
    """POST /stac/collections-search must no longer be registered (404)."""
    r = await sysadmin_in_process_client.post(
        "/stac/collections-search",
        json={},
    )
    assert r.status_code == 404, (
        f"Non-standard /stac/collections-search should be gone but got {r.status_code}"
    )


@MARKER
@pytest.mark.asyncio
async def test_collections_slash_search_post_route_removed(
    sysadmin_in_process_client: AsyncClient,
):
    """POST /stac/collections/search (deprecated) must no longer be registered (404)."""
    r = await sysadmin_in_process_client.post(
        "/stac/collections/search",
        json={},
    )
    assert r.status_code == 404, (
        f"Non-standard /stac/collections/search should be gone but got {r.status_code}"
    )


@MARKER
@pytest.mark.asyncio
async def test_bare_search_post_route_removed(
    sysadmin_in_process_client: AsyncClient,
):
    """POST /stac/search (deprecated bare path) must no longer be registered (404)."""
    r = await sysadmin_in_process_client.post(
        "/stac/search",
        json={},
    )
    assert r.status_code == 404, (
        f"Deprecated /stac/search should be gone but got {r.status_code}"
    )
