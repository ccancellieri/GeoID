"""Integration tests for the Search extension — item-only API (#819)."""

import pytest
from httpx import AsyncClient


MARKER = pytest.mark.enable_extensions("stac", "assets", "features", "search")
# The search extension reaches into Elasticsearch via the items driver
# (`SearchService._resolve_items_driver().es_client`). Without
# ElasticsearchModule registered in `enable_modules`, the in-process app
# loads the search extension but the ES client is never instantiated —
# every search call would surface a registration failure.
ES_MODULE_MARKER = pytest.mark.enable_modules("elasticsearch")
ES_MARKER = pytest.mark.elasticsearch


# ---------------------------------------------------------------------------
# GET /search — simple item search
# ---------------------------------------------------------------------------


@MARKER
@ES_MODULE_MARKER
@ES_MARKER
@pytest.mark.asyncio
async def test_search_get_empty_returns_feature_collection(
    sysadmin_in_process_client: AsyncClient,
):
    """GET /search with no params returns a valid empty FeatureCollection."""
    r = await sysadmin_in_process_client.get("/search")
    assert r.status_code == 200
    data = r.json()
    assert data.get("type") == "FeatureCollection"
    assert isinstance(data.get("features"), list)


@MARKER
@ES_MODULE_MARKER
@ES_MARKER
@pytest.mark.asyncio
async def test_search_get_with_q(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /search?q= — free-text query accepted, returns valid response."""
    r = await sysadmin_in_process_client.get("/search", params={"q": "nonexistent_xyz_123"})
    assert r.status_code == 200
    data = r.json()
    assert data.get("type") == "FeatureCollection"
    assert data.get("features") == []


@MARKER
@ES_MODULE_MARKER
@ES_MARKER
@pytest.mark.asyncio
async def test_search_get_with_bbox(
    sysadmin_in_process_client: AsyncClient,
):
    """GET /search?bbox= — spatial filter accepted."""
    r = await sysadmin_in_process_client.get(
        "/search", params={"bbox": "-180,-90,180,90"}
    )
    assert r.status_code == 200
    data = r.json()
    assert data.get("type") == "FeatureCollection"


@MARKER
@ES_MODULE_MARKER
@ES_MARKER
@pytest.mark.asyncio
async def test_search_get_with_limit(
    sysadmin_in_process_client: AsyncClient,
):
    """GET /search?limit= — limit parameter accepted."""
    r = await sysadmin_in_process_client.get("/search", params={"limit": 5})
    assert r.status_code == 200


@MARKER
@ES_MODULE_MARKER
@ES_MARKER
@pytest.mark.asyncio
async def test_search_get_with_collections_filter(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """GET /search?collections= — collection scoping accepted."""
    collection_id = setup_collection
    r = await sysadmin_in_process_client.get(
        "/search", params={"collections": collection_id}
    )
    assert r.status_code == 200
    data = r.json()
    assert data.get("type") == "FeatureCollection"


# ---------------------------------------------------------------------------
# POST /search — full-featured item search
# ---------------------------------------------------------------------------


@MARKER
@ES_MODULE_MARKER
@ES_MARKER
@pytest.mark.asyncio
async def test_search_post_empty_body(
    sysadmin_in_process_client: AsyncClient,
):
    """POST /search with empty body returns a valid FeatureCollection."""
    r = await sysadmin_in_process_client.post("/search", json={})
    assert r.status_code == 200
    data = r.json()
    assert data.get("type") == "FeatureCollection"


@MARKER
@ES_MODULE_MARKER
@ES_MARKER
@pytest.mark.asyncio
async def test_search_post_with_collections(
    sysadmin_in_process_client: AsyncClient, setup_catalog, setup_collection
):
    """POST /search filtering by collections list."""
    collection_id = setup_collection
    r = await sysadmin_in_process_client.post(
        "/search", json={"collections": [collection_id]}
    )
    assert r.status_code == 200
    data = r.json()
    assert data.get("type") == "FeatureCollection"


@MARKER
@ES_MODULE_MARKER
@ES_MARKER
@pytest.mark.asyncio
async def test_search_post_with_bbox(
    sysadmin_in_process_client: AsyncClient,
):
    """POST /search with bbox spatial filter."""
    r = await sysadmin_in_process_client.post(
        "/search", json={"bbox": [-180, -90, 180, 90]}
    )
    assert r.status_code == 200


@MARKER
@ES_MODULE_MARKER
@ES_MARKER
@pytest.mark.asyncio
async def test_search_post_with_ids(
    sysadmin_in_process_client: AsyncClient,
):
    """POST /search filtering by item IDs returns valid response."""
    r = await sysadmin_in_process_client.post(
        "/search", json={"ids": ["nonexistent_id_xyz"]}
    )
    assert r.status_code == 200
    data = r.json()
    assert data.get("features") == []


# ---------------------------------------------------------------------------
# GET/POST /search/catalogs/{catalog_id} — catalog-scoped item search
# ---------------------------------------------------------------------------


@MARKER
@ES_MODULE_MARKER
@ES_MARKER
@pytest.mark.asyncio
async def test_search_catalog_scoped_get(
    sysadmin_in_process_client: AsyncClient, setup_catalog
):
    """GET /search/catalogs/{catalog_id} — catalog-scoped item search."""
    catalog_id = setup_catalog
    r = await sysadmin_in_process_client.get(f"/search/catalogs/{catalog_id}")
    assert r.status_code == 200
    data = r.json()
    assert data.get("type") == "FeatureCollection"


@MARKER
@ES_MODULE_MARKER
@ES_MARKER
@pytest.mark.asyncio
async def test_search_catalog_scoped_post(
    sysadmin_in_process_client: AsyncClient, setup_catalog
):
    """POST /search/catalogs/{catalog_id} — catalog-scoped item search body."""
    catalog_id = setup_catalog
    r = await sysadmin_in_process_client.post(
        f"/search/catalogs/{catalog_id}", json={"limit": 5}
    )
    assert r.status_code == 200
    data = r.json()
    assert data.get("type") == "FeatureCollection"


@MARKER
@ES_MODULE_MARKER
@ES_MARKER
@pytest.mark.asyncio
async def test_search_catalogs_index_removed(
    sysadmin_in_process_client: AsyncClient, setup_catalog
):
    """Per #819 catalog/collection keyword search routes are gone — 404."""
    r1 = await sysadmin_in_process_client.get("/search/catalogs")
    r2 = await sysadmin_in_process_client.get("/search/collections")
    assert r1.status_code == 404
    assert r2.status_code == 404
