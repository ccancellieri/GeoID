"""Shared fixtures for ES integration tests.

All tests in this directory require:
  - a live Elasticsearch instance (``@pytest.mark.elasticsearch``)
  - the ElasticsearchModule loaded (``@pytest.mark.enable_modules("elasticsearch")``)
"""
from __future__ import annotations

import asyncio
import os

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

def _resolve_asyncpg_url() -> str:
    """Test DB URL in asyncpg-native form (strip SQLAlchemy driver prefix)."""
    url = os.getenv(
        "DATABASE_URL",
        "postgresql://testuser:testpassword@localhost:54320/gis_dev",
    )
    return url.replace("postgresql+asyncpg://", "postgresql://")


async def drain_es_items_outbox(catalog_id: str) -> int:
    """Drive the ES items OUTBOX drain in-process until the queue is empty.

    Production runs the drain inside a separate Cloud Run Job whose
    ``CapabilityMap`` advertises ``outbox_drain``; the in-process test
    harness never claims that task, so a STAC POST's atomically-enqueued
    ``OutboxDrainTask`` sits forever and a follow-up ``refresh + search``
    hits an empty index (#614). We open a dedicated asyncpg connection,
    pin ``search_path`` to the catalog schema, and call ``drain_once()``
    until it reports zero.

    Returns the total rows drained (informational; tests rely on the
    side effect of items appearing in ES).
    """
    import asyncpg

    from dynastore.tasks.outbox_drain.es_entrypoint import build_es_drain_task

    conn = await asyncpg.connect(_resolve_asyncpg_url())
    try:
        await conn.execute(f'SET search_path TO "{catalog_id}"')
        task = await build_es_drain_task(conn=conn, catalog_id=catalog_id)
        total = 0
        # Bounded loop: a healthy claim batch is finite; the cap guards
        # against an indexer that keeps marking rows transient.
        for _ in range(50):
            n = await task.drain_once()
            total += n
            if n == 0:
                break
        return total
    finally:
        await conn.close()


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
