#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Shared fixtures for ES integration tests.

All tests in this directory require:
  - a live Elasticsearch instance (``@pytest.mark.elasticsearch``)
  - the ElasticsearchModule loaded (``@pytest.mark.enable_modules("elasticsearch")``)
"""
from __future__ import annotations

import os

from tests.dynastore.extensions.conftest import build_setup_fixtures


# ---------------------------------------------------------------------------
# Catalog / collection helpers (parallel to search/integration/conftest.py)
# ---------------------------------------------------------------------------
#
# Delete-before-create, no response assertion, catalog delete on teardown
# (no explicit collection-delete on teardown, matching the previous local
# fixture, which relied on the catalog teardown to cascade).

setup_catalog, setup_collection = build_setup_fixtures(
    "/features",
    delete_before=True,
    catalog_teardown=True,
)


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
    """Drive the ES items storage drain in-process until the queue is empty.

    Production runs the drain inside the ``catalog`` tier (a tier-agnostic
    ``storage_drain`` task routed there by the default matrix); the in-process
    test harness never claims that task, so a STAC POST's atomically-enqueued
    ``storage_drain`` row sits forever and a follow-up ``refresh + search``
    hits an empty index (#614). We construct a :class:`StorageDrainTask`,
    open a dedicated SQLAlchemy engine, and call ``drain_once()`` until it
    reports zero.

    The storage outbox is now the global ``tasks.storage`` table keyed by
    ``catalog_id`` (#1807 P4), not a per-schema ``storage_outbox`` — so a
    single drain empties every pending row regardless of physical schema.
    ``catalog_id`` is retained for signature compatibility and diagnostics.

    Returns the total rows drained (informational; tests rely on the
    side effect of items appearing in ES).
    """
    from uuid import uuid4

    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import NullPool

    from dynastore.tasks.workclass_drain.storage_drain_task import StorageDrainTask

    sa_url = _resolve_asyncpg_url().replace(
        "postgresql://", "postgresql+asyncpg://", 1
    )
    engine = create_async_engine(sa_url, poolclass=NullPool)
    task = StorageDrainTask()
    owner_id = f"test_drain:{uuid4()}"
    try:
        total = 0
        # Bounded loop: a healthy claim batch is finite; the cap guards
        # against an indexer that keeps marking rows transient. If we hit
        # the cap with the queue still non-empty, fail loudly — silently
        # returning would let a test green up with a partially-indexed ES
        # state, which is exactly the failure mode #614 was filed to fix.
        cap = 50
        drained = False
        for _ in range(cap):
            n = await task.drain_once(engine=engine, owner_id=owner_id)
            total += n
            if n == 0:
                drained = True
                break
        if not drained:
            raise AssertionError(
                f"storage drain for catalog={catalog_id!r} did not empty after {cap} iterations "
                f"({total} rows drained); a downstream indexer is likely marking rows transient. "
                "Inspect tasks.storage for pending rows before retrying."
            )
        return total
    finally:
        await engine.dispose()


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
