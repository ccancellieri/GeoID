"""Shared fixtures for ES integration tests.

All tests in this directory require:
  - a live Elasticsearch instance (``@pytest.mark.elasticsearch``)
  - the ElasticsearchModule loaded (``@pytest.mark.enable_modules("elasticsearch")``)
"""
from __future__ import annotations

import asyncio

import pytest


# ---------------------------------------------------------------------------
# Catalog / collection helpers (parallel to search/integration/conftest.py)
# ---------------------------------------------------------------------------

@pytest.fixture
async def setup_catalog(sysadmin_in_process_client, catalog_data, catalog_id):
    await sysadmin_in_process_client.delete(f"/features/catalogs/{catalog_id}?force=true")
    await sysadmin_in_process_client.post("/features/catalogs", json=catalog_data)
    yield catalog_id
    await sysadmin_in_process_client.delete(f"/features/catalogs/{catalog_id}?force=true")


@pytest.fixture
async def setup_collection(sysadmin_in_process_client, setup_catalog, collection_data, collection_id):
    catalog_id = setup_catalog
    await sysadmin_in_process_client.delete(
        f"/features/catalogs/{catalog_id}/collections/{collection_id}?force=true"
    )
    await sysadmin_in_process_client.post(
        f"/features/catalogs/{catalog_id}/collections", json=collection_data
    )
    yield collection_id


# ---------------------------------------------------------------------------
# ES refresh helpers
# ---------------------------------------------------------------------------

async def refresh_items_index(catalog_id: str) -> None:
    """Force ES to make all recently written item docs searchable."""
    from dynastore.modules.elasticsearch.client import get_client, get_index_prefix
    from dynastore.modules.elasticsearch.mappings import get_tenant_items_index

    es = get_client()
    index = get_tenant_items_index(get_index_prefix(), catalog_id)
    try:
        await es.indices.refresh(index=index)
    except Exception:
        pass  # index may not exist yet — that's fine for negative tests


async def refresh_private_items_index(catalog_id: str) -> None:
    """Force ES refresh on the per-tenant private items index."""
    from dynastore.modules.elasticsearch.client import get_client, get_index_prefix
    from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
        get_private_index_name,
    )

    es = get_client()
    index = get_private_index_name(get_index_prefix(), catalog_id)
    try:
        await es.indices.refresh(index=index)
    except Exception:
        pass


async def refresh_private_collection_index(catalog_id: str) -> None:
    """Force ES refresh on the per-tenant private collection index."""
    from dynastore.modules.elasticsearch.client import get_client, get_index_prefix
    from dynastore.modules.elasticsearch.mappings import (
        get_tenant_collections_private_index,
    )

    es = get_client()
    index = get_tenant_collections_private_index(get_index_prefix(), catalog_id)
    try:
        await es.indices.refresh(index=index)
    except Exception:
        pass


async def doc_exists(index: str, doc_id: str) -> bool:
    """Return True if a document with the given id exists in the index."""
    from dynastore.modules.elasticsearch.client import get_client

    es = get_client()
    try:
        result = await es.exists(index=index, id=doc_id)
        # opensearch-py wraps the bool in an ObjectApiResponse
        return bool(result)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Minimal STAC item factory
# ---------------------------------------------------------------------------

def make_item(item_id: str, lon: float = 10.0, lat: float = 40.0) -> dict:
    return {
        "id": item_id,
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "bbox": [lon, lat, lon, lat],
        "properties": {
            "datetime": "2024-01-15T00:00:00Z",
            "title": {"en": f"ES Integration Test {item_id}"},
        },
        "links": [],
        "assets": {},
    }
