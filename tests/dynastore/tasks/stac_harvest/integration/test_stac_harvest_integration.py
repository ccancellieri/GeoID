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

"""Live-DB integration test: stac_harvest with storage_backend='es' writes N items
that are immediately readable and searchable.

Regression guard for the 'harvest completes, items=0/invisible' bug:
  (a) Default PG-primary + async ES drain routing left items invisible because
      writes went to PG primary + async ES drain that never ran in-process.
  (b) Dynamic ES mapping made 'collection' an analyzed text field so a term
      filter on collection id never matched.

The fix (already in this branch):
  - harvest applies ES-only ItemsRoutingConfig so WRITE+READ+SEARCH go directly
    to Elasticsearch; no PG table, no async drain.
  - ES index is created with the correct keyword mapping for 'collection' before
    the first write.

This test MUST FAIL on the old behavior (items_written=N but read back=0 or
search returns 0 due to analyzed 'collection' field) and PASS now.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List
from unittest.mock import patch

import pytest

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level markers
# ---------------------------------------------------------------------------

pytestmark = [
    pytest.mark.enable_modules(
        "db_config", "db", "catalog", "iam", "stac",
        "collection_postgresql", "catalog_postgresql", "elasticsearch",
    ),
    pytest.mark.elasticsearch,
    pytest.mark.asyncio,
]

# ---------------------------------------------------------------------------
# Fake STAC source data — no network is hit
# ---------------------------------------------------------------------------

_FAKE_CATALOG_URL = "https://fake-stac-source.test/stac"
_FAKE_COLLECTION_ID = "fake-harvest-col"
_N_ITEMS = 3

_FAKE_COLLECTION: Dict[str, Any] = {
    "id": _FAKE_COLLECTION_ID,
    "type": "Collection",
    "title": "Fake collection for harvest integration test",
    "description": "Minimal STAC collection for harvest regression guard.",
    "extent": {
        "spatial": {"bbox": [[-10.0, 35.0, 40.0, 70.0]]},
        "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
    },
    "links": [],
}

_FAKE_ITEMS: List[Dict[str, Any]] = [
    {
        "id": f"harvest-itest-item-{i:03d}",
        "type": "Feature",
        "collection": _FAKE_COLLECTION_ID,
        "geometry": {
            "type": "Point",
            "coordinates": [10.0 + i, 40.0 + i],
        },
        "bbox": [10.0 + i, 40.0 + i, 10.0 + i, 40.0 + i],
        "properties": {
            "datetime": f"2024-0{1 + i}-15T00:00:00Z",
        },
        "links": [],
        "assets": {},
    }
    for i in range(_N_ITEMS)
]


def _fake_http_get_json(url: str, **_kw: Any) -> Any:
    """Serve a minimal fake STAC catalog without hitting the network.

    URL routing:
      .../collections          -> single-page collections response (no 'next')
      .../collections/{id}/items -> single-page items response (no 'next')
    """
    base = _FAKE_CATALOG_URL
    if url == f"{base}/collections":
        return {"collections": [_FAKE_COLLECTION], "links": []}
    if url.startswith(f"{base}/collections/{_FAKE_COLLECTION_ID}/items"):
        return {"features": list(_FAKE_ITEMS), "links": []}
    # Unknown URL — return empty so iteration terminates cleanly
    return {"collections": [], "features": [], "links": []}


# ---------------------------------------------------------------------------
# ES refresh helper
# ---------------------------------------------------------------------------

async def _refresh_items_index(catalog_id: str) -> None:
    """Force ES index refresh so all written docs are visible to _search.

    ES-only routing uses refresh=wait_for on write_entities, which blocks
    until the doc is visible to _search.  An extra refresh call is
    belt-and-braces for the ES search-idle edge case (auto-refresh pause
    after 30s with no searches).
    """
    from dynastore.modules.elasticsearch.client import get_client, get_index_prefix
    from dynastore.modules.elasticsearch.mappings import get_tenant_items_index

    es = get_client()
    if es is None:
        return
    index = get_tenant_items_index(get_index_prefix(), catalog_id)
    try:
        await es.indices.refresh(index=index)
    except Exception:
        pass  # index may not exist if harvest failed


# ---------------------------------------------------------------------------
# Catalog cleanup helper
# ---------------------------------------------------------------------------

async def _delete_catalog_safe(catalog_id: str) -> None:
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.tools.discovery import get_protocol

    catalogs = get_protocol(CatalogsProtocol)
    if catalogs:
        try:
            await asyncio.wait_for(
                catalogs.delete_catalog(catalog_id, force=True),
                timeout=10.0,
            )
        except Exception as exc:
            logger.warning(
                "harvest itest: teardown delete_catalog(%s) failed: %s", catalog_id, exc
            )


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_harvest_es_only_items_readable_and_searchable(
    task_app_state: Any,
) -> None:
    """Harvest N=3 items into a fresh ES-only catalog; assert all 3 are readable
    and searchable via the internal protocol layer.

    Regression guard for:
      1. items_written == N (harvest wrote them, not 0)
      2. search_items on the catalog returns all N items (readable via ES routing)
      3. A direct ES term query on 'collection' keyword returns all N items
         (searchable — proves the 'collection' field is not analyzed text)
      4. ItemsRoutingConfig for the catalog has ES-only drivers persisted
    """
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.models.protocols.configs import ConfigsProtocol
    from dynastore.models.query_builder import QueryRequest
    from dynastore.modules.catalog.models import Catalog
    from dynastore.modules.processes.models import ExecuteRequest
    from dynastore.modules.storage.routing_config import ItemsRoutingConfig
    from dynastore.modules.tasks.models import TaskPayload
    from dynastore.tasks.stac_harvest.task import StacHarvestTask
    from dynastore.tools.discovery import get_protocol
    from dynastore.tools.identifiers import generate_id_hex, generate_task_id

    # --- create a fresh catalog -------------------------------------------
    catalog_id = f"hvitest{generate_id_hex()}"
    catalogs = get_protocol(CatalogsProtocol)
    assert catalogs is not None, "CatalogsProtocol not available"

    await catalogs.create_catalog(Catalog(id=catalog_id, title=catalog_id))

    try:
        # --- run harvest with mocked HTTP source --------------------------
        payload = TaskPayload(
            task_id=generate_task_id(),
            caller_id="harvest-itest",
            inputs=ExecuteRequest(
                inputs={
                    "catalog_url": _FAKE_CATALOG_URL,
                    "target_catalog": catalog_id,
                    "storage_backend": "es",
                    "max_collections": 0,
                    "max_items": 0,
                    "with_assets": False,
                }
            ),
        )

        with patch(
            "dynastore.tasks.stac_harvest.task._http_get_json",
            side_effect=_fake_http_get_json,
        ):
            result = await StacHarvestTask(None).run(payload)

        assert result is not None, "StacHarvestTask.run() returned None"

        # --- assertion 1: harvest reported N items written ----------------
        items_written: int = result.get("items_written", 0)
        assert items_written == _N_ITEMS, (
            f"harvest reported items_written={items_written}, expected {_N_ITEMS}. "
            f"errors={result.get('errors')}. "
            "Regression: items were not written to ES."
        )
        assert result.get("items_failed", 0) == 0, (
            f"harvest reported items_failed={result.get('items_failed')}; "
            f"errors={result.get('errors')}"
        )
        assert result.get("collections_written", 0) == 1, (
            f"harvest reported collections_written={result.get('collections_written')}, "
            "expected 1"
        )

        # Belt-and-braces refresh: write_entities already uses refresh=wait_for
        # but this ensures the ES refresh interval hasn't paused the index.
        await _refresh_items_index(catalog_id)

        # map_collection lowercases the id
        collection_id = _FAKE_COLLECTION_ID.lower()

        # --- assertion 2: items READABLE via protocol search_items --------
        qr = QueryRequest(limit=100)
        features = await catalogs.search_items(catalog_id, collection_id, qr)

        returned_ids = [getattr(f, "id", None) or (f.get("id") if isinstance(f, dict) else None)
                        for f in features]
        assert len(returned_ids) == _N_ITEMS, (
            f"search_items returned {len(returned_ids)} items, expected {_N_ITEMS}. "
            f"ids={returned_ids}. "
            "Regression: items are not readable via ES-only routing (they may "
            "have been written to PG instead, but the PG table does not exist "
            "for an ES-only catalog)."
        )
        expected_ids = {item["id"] for item in _FAKE_ITEMS}
        assert set(returned_ids) == expected_ids, (
            f"search_items returned unexpected ids: {set(returned_ids)} vs {expected_ids}"
        )

        # --- assertion 3: items SEARCHABLE via ES term filter on 'collection'
        # This tests the keyword mapping fix. An analyzed text mapping makes
        # term filters miss; keyword mapping makes them match.
        from dynastore.modules.elasticsearch.client import get_client, get_index_prefix
        from dynastore.modules.elasticsearch.mappings import get_tenant_items_index

        es = get_client()
        assert es is not None, "ES client not available"

        index = get_tenant_items_index(get_index_prefix(), catalog_id)
        # Explicit index refresh before searching
        try:
            await es.indices.refresh(index=index)
        except Exception:
            pass

        search_resp = await es.search(
            index=index,
            body={
                "query": {"term": {"collection": collection_id}},
                "size": 10,
            },
        )
        hits = search_resp.get("hits", {}).get("hits", [])
        search_count = len(hits)
        assert search_count == _N_ITEMS, (
            f"ES term query on 'collection'={collection_id!r} returned {search_count} "
            f"hits, expected {_N_ITEMS}. "
            "Regression: 'collection' field has analyzed text mapping (term filter "
            "never matches lowercase token of a mixed-case id), OR items were not "
            "written to the ES index at all."
        )
        # Ids in ES are geoids (internal), not the external item id — just check count.
        # The source should contain the collection field we filtered on.
        for hit in hits:
            source_collection = hit.get("_source", {}).get("collection")
            assert source_collection == collection_id, (
                f"ES hit _source.collection={source_collection!r} != {collection_id!r}"
            )

        # --- assertion 4: ItemsRoutingConfig persisted as ES-only ---------
        configs = get_protocol(ConfigsProtocol)
        assert configs is not None, "ConfigsProtocol not available"
        routing = await configs.get_config(ItemsRoutingConfig, catalog_id=catalog_id)
        assert routing is not None, (
            f"ItemsRoutingConfig not persisted for catalog={catalog_id}. "
            "Regression: _apply_stac_presets did not write the routing config."
        )
        ops = routing.operations or {}
        write_drivers = [e.driver_ref for e in ops.get("WRITE", [])]
        read_drivers = [e.driver_ref for e in ops.get("READ", [])]
        assert any("elasticsearch" in d.lower() for d in write_drivers), (
            f"ES-only ItemsRoutingConfig has no elasticsearch WRITE driver: {write_drivers}. "
            "Regression: harvest did not apply the ES-only routing preset."
        )
        assert any("elasticsearch" in d.lower() for d in read_drivers), (
            f"ES-only ItemsRoutingConfig has no elasticsearch READ driver: {read_drivers}. "
            "Regression: harvest did not apply the ES-only routing preset."
        )

    finally:
        await _delete_catalog_safe(catalog_id)
