"""Integration tests — public Elasticsearch: item write → index → search.

Verifies the end-to-end flow:
  POST /stac/.../items  →  item lands in per-catalog ES index
                        →  GET /search returns it (by id, bbox, fulltext)
  DELETE .../items/{id} →  item removed from ES index

All tests require a live ES instance and are skipped otherwise.
"""
from __future__ import annotations

import asyncio

import pytest
from httpx import AsyncClient

from tests.dynastore.modules.elasticsearch.integration.conftest import (
    make_item,
    refresh_items_index,
)

pytestmark = [
    pytest.mark.enable_extensions("stac", "features", "search"),
    # Full default stack + elasticsearch: the STAC POST/DELETE endpoints require
    # db_config + db + catalog + iam + collection_postgresql + catalog_postgresql.
    # enable_modules() replaces the default list entirely, so we must repeat it.
    pytest.mark.enable_modules(
        "db_config", "db", "catalog", "iam", "stac",
        "collection_postgresql", "catalog_postgresql", "elasticsearch",
    ),
    pytest.mark.elasticsearch,
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _post_item(client: AsyncClient, catalog_id: str, collection_id: str, item: dict) -> None:
    r = await client.post(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items",
        json=item,
    )
    assert r.status_code in (200, 201), f"POST item failed: {r.status_code} {r.text}"


async def _delete_item(client: AsyncClient, catalog_id: str, collection_id: str, item_id: str) -> None:
    await client.delete(
        f"/stac/catalogs/{catalog_id}/collections/{collection_id}/items/{item_id}"
    )


async def _yield_to_async_writer(catalog_id: str) -> None:
    """Let any pending asyncio ES write tasks run, then flush the index."""
    await asyncio.sleep(0)
    await refresh_items_index(catalog_id)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_item_post_then_found_by_id(
    sysadmin_in_process_client: AsyncClient,
    setup_catalog: str,
    setup_collection: str,
):
    """POST a STAC item → it appears in /search?ids=."""
    cat, col = setup_catalog, setup_collection
    item = make_item("es-rt-by-id")
    await _post_item(sysadmin_in_process_client, cat, col, item)
    await _yield_to_async_writer(cat)

    r = await sysadmin_in_process_client.get("/search", params={"ids": "es-rt-by-id"})
    assert r.status_code == 200
    data = r.json()
    ids = [f["id"] for f in data.get("features", [])]
    assert "es-rt-by-id" in ids


@pytest.mark.asyncio
async def test_item_found_by_bbox(
    sysadmin_in_process_client: AsyncClient,
    setup_catalog: str,
    setup_collection: str,
):
    """POST item at 10°E/40°N → bbox enclosing it returns it; disjoint bbox misses."""
    cat, col = setup_catalog, setup_collection
    item = make_item("es-rt-bbox", lon=10.0, lat=40.0)
    await _post_item(sysadmin_in_process_client, cat, col, item)
    await _yield_to_async_writer(cat)

    # Enclosing bbox
    r = await sysadmin_in_process_client.get("/search", params={"bbox": "5,35,15,45"})
    assert r.status_code == 200
    ids = [f["id"] for f in r.json().get("features", [])]
    assert "es-rt-bbox" in ids

    # Disjoint bbox — item must NOT be returned
    r2 = await sysadmin_in_process_client.get("/search", params={"bbox": "20,50,30,60"})
    assert r2.status_code == 200
    ids2 = [f["id"] for f in r2.json().get("features", [])]
    assert "es-rt-bbox" not in ids2


@pytest.mark.asyncio
async def test_item_found_by_fulltext(
    sysadmin_in_process_client: AsyncClient,
    setup_catalog: str,
    setup_collection: str,
):
    """POST item with unique title → GET /search?q=<title> returns it."""
    cat, col = setup_catalog, setup_collection
    unique_token = "ZZQrtPlanTest7331"
    item = make_item("es-rt-fulltext")
    item["properties"]["title"] = {"en": f"Unique {unique_token} Title"}
    await _post_item(sysadmin_in_process_client, cat, col, item)
    await _yield_to_async_writer(cat)

    r = await sysadmin_in_process_client.get("/search", params={"q": unique_token})
    assert r.status_code == 200
    ids = [f["id"] for f in r.json().get("features", [])]
    assert "es-rt-fulltext" in ids


@pytest.mark.asyncio
async def test_item_not_found_after_delete(
    sysadmin_in_process_client: AsyncClient,
    setup_catalog: str,
    setup_collection: str,
):
    """POST item, DELETE it, verify it is no longer in /search results."""
    cat, col = setup_catalog, setup_collection
    item = make_item("es-rt-delete")
    await _post_item(sysadmin_in_process_client, cat, col, item)
    await _yield_to_async_writer(cat)

    # Verify it's there first
    r = await sysadmin_in_process_client.get("/search", params={"ids": "es-rt-delete"})
    assert r.status_code == 200
    assert any(f["id"] == "es-rt-delete" for f in r.json().get("features", []))

    # Delete
    await _delete_item(sysadmin_in_process_client, cat, col, "es-rt-delete")
    await _yield_to_async_writer(cat)

    # Should be gone
    r2 = await sysadmin_in_process_client.get("/search", params={"ids": "es-rt-delete"})
    assert r2.status_code == 200
    assert not any(f["id"] == "es-rt-delete" for f in r2.json().get("features", []))
