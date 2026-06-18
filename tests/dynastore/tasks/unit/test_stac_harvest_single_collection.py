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

"""Unit tests for single-collection harvest source detection + scope routing.

No live DB or network — the source HTTP and the preset apply are mocked.
"""
from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.tasks.stac_harvest import task as harvest_task
from dynastore.tasks.stac_harvest.models import StacHarvestRequest


# ---------------------------------------------------------------------------
# _probe_single_collection
# ---------------------------------------------------------------------------


def test_probe_detects_collection_with_items_link():
    doc = {
        "type": "Collection",
        "id": "sentinel-2",
        "links": [{"rel": "items", "href": "https://src/c/sentinel-2/items"}],
    }
    with patch.object(harvest_task, "_http_get_json", return_value=doc):
        result = harvest_task._probe_single_collection("https://src/c/sentinel-2")
    assert result is not None
    coll, items_url = result
    assert coll["id"] == "sentinel-2"
    assert items_url == "https://src/c/sentinel-2/items"


def test_probe_collection_without_items_link_falls_back():
    doc = {"type": "Collection", "id": "x", "links": []}
    with patch.object(harvest_task, "_http_get_json", return_value=doc):
        result = harvest_task._probe_single_collection("https://src/c/x")
    assert result is not None
    _, items_url = result
    assert items_url == "https://src/c/x/items"


def test_probe_returns_none_for_catalog():
    doc = {"type": "Catalog", "id": "root", "links": []}
    with patch.object(harvest_task, "_http_get_json", return_value=doc):
        assert harvest_task._probe_single_collection("https://src") is None


def test_probe_returns_none_on_fetch_error():
    with patch.object(harvest_task, "_http_get_json", side_effect=RuntimeError("boom")):
        assert harvest_task._probe_single_collection("https://src") is None


# ---------------------------------------------------------------------------
# run_harvest — single-collection mode
# ---------------------------------------------------------------------------


def _mock_catalogs() -> AsyncMock:
    catalogs = AsyncMock()
    catalogs.get_collection = AsyncMock(return_value=None)  # not present → create
    catalogs.create_collection = AsyncMock(return_value=None)
    catalogs.update_collection = AsyncMock(return_value=None)
    catalogs.upsert = AsyncMock(return_value=None)
    return catalogs


async def _aiter(items: List[Dict[str, Any]]):
    for it in items:
        yield it


@pytest.mark.asyncio
async def test_single_collection_harvest_default_target_is_source_id():
    """A single-collection source harvests into the source id at collection scope."""
    source_coll = {"type": "Collection", "id": "MyColl", "description": "d"}
    items = [{"type": "Feature", "id": "i1", "geometry": None, "properties": {}}]

    applied: list = []

    async def fake_apply(ctx, scope, catalog_id, drivers):
        applied.append((scope, drivers))

    request = StacHarvestRequest(
        catalog_url="https://src/c/MyColl", target_catalog="cat-7", drivers="es",
    )
    catalogs = _mock_catalogs()

    with (
        patch.object(harvest_task, "_probe_single_collection",
                     return_value=(source_coll, "https://src/c/MyColl/items")),
        patch.object(harvest_task, "_iter_items_from", return_value=_aiter(items)),
        patch.object(harvest_task, "_apply_harvest_presets", side_effect=fake_apply),
    ):
        stats = await harvest_task.run_harvest(
            request, catalogs, preset_ctx=object(), base_scope="catalog:cat-7"
        )

    # Routing pinned at CATALOG scope (so the target collection's create routes
    # correctly); items land in the lowercased source id.
    assert applied == [("catalog:cat-7", request.drivers)]
    assert stats.collections_seen == 1
    assert stats.collections_written == 1
    assert stats.items_written == 1
    # The collection was created with id = target collection (lowercased source id).
    created = catalogs.create_collection.await_args.args
    assert created[1]["id"] == "mycoll"
    # Items upserted into the same target collection.
    up = catalogs.upsert.await_args.args
    assert up[1] == "mycoll"


@pytest.mark.asyncio
async def test_single_collection_harvest_explicit_target_collection():
    """An explicit target_collection overrides the source id."""
    source_coll = {"type": "Collection", "id": "src-id", "description": "d"}
    items = [{"type": "Feature", "id": "i1", "geometry": None, "properties": {}}]
    applied: list = []

    async def fake_apply(ctx, scope, catalog_id, drivers):
        applied.append(scope)

    request = StacHarvestRequest(
        catalog_url="https://src/c/src-id", target_catalog="cat-7",
        target_collection="dest", drivers="pg_es",
    )
    catalogs = _mock_catalogs()

    with (
        patch.object(harvest_task, "_probe_single_collection",
                     return_value=(source_coll, "https://src/c/src-id/items")),
        patch.object(harvest_task, "_iter_items_from", return_value=_aiter(items)),
        patch.object(harvest_task, "_apply_harvest_presets", side_effect=fake_apply),
    ):
        stats = await harvest_task.run_harvest(
            request, catalogs, preset_ctx=object(), base_scope="catalog:cat-7"
        )

    # Routing at catalog scope; items land in the explicit target collection.
    assert applied == ["catalog:cat-7"]
    assert catalogs.create_collection.await_args.args[1]["id"] == "dest"
    assert stats.items_written == 1


@pytest.mark.asyncio
async def test_catalog_harvest_applies_at_catalog_scope():
    """A catalog source pins routing at catalog scope and walks /collections."""
    applied: list = []

    async def fake_apply(ctx, scope, catalog_id, drivers):
        applied.append(scope)

    coll = {"type": "Collection", "id": "C1", "description": "d"}
    request = StacHarvestRequest(
        catalog_url="https://src", target_catalog="cat-7", drivers="es",
    )
    catalogs = _mock_catalogs()

    with (
        patch.object(harvest_task, "_probe_single_collection", return_value=None),
        patch.object(harvest_task, "iter_collections", return_value=_aiter([coll])),
        patch.object(harvest_task, "iter_items", return_value=_aiter([])),
        patch.object(harvest_task, "_apply_harvest_presets", side_effect=fake_apply),
    ):
        stats = await harvest_task.run_harvest(
            request, catalogs, preset_ctx=object(), base_scope="catalog:cat-7"
        )

    assert applied == ["catalog:cat-7"]
    assert stats.collections_seen == 1
    assert stats.collections_written == 1
