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

from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.tasks.stac_harvest import task as harvest_task
from dynastore.tasks.stac_harvest.models import StacHarvestRequest


@pytest.fixture(autouse=True)
def _mock_pg_collection_metadata_upsert():
    with patch.object(
        harvest_task,
        "_upsert_collection_metadata_pg",
        AsyncMock(),
    ):
        yield


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
async def test_single_collection_harvest_preserves_source_collection_metadata():
    """Harvest collection creation carries source STAC metadata and extent."""
    source_extent = {
        "spatial": {"bbox": [[10.0, 20.0, 30.0, 40.0]]},
        "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
    }
    source_coll = {
        "type": "Collection",
        "id": "AGERA5-RH12",
        "stac_version": "1.1.0",
        "stac_extensions": [
            "https://stac-extensions.github.io/datacube/v2.3.0/schema.json"
        ],
        "description": "Relative humidity",
        "license": "CC-BY-SA-4.0",
        "extent": source_extent,
        "assets": {"thumbnail": {"href": "https://example.test/thumb.png"}},
        "providers": [{"name": "ECMWF", "roles": ["producer"]}],
        "summaries": {"datetime": {"min": "2020-01-01"}},
        "cube:dimensions": {"time": {"type": "temporal", "extent": ["2020", None]}},
        "cube:variables": {"rh": {"type": "data", "unit": "%"}},
        "links": [{"rel": "self", "href": "https://example.test"}],
    }
    items = [{"type": "Feature", "id": "i1", "geometry": None, "properties": {}}]
    request = StacHarvestRequest(
        catalog_url="https://src/c/AGERA5-RH12", target_catalog="cat-7", drivers="es",
    )
    catalogs = _mock_catalogs()

    with (
        patch.object(harvest_task, "_probe_single_collection",
                     return_value=(source_coll, "https://src/c/AGERA5-RH12/items")),
        patch.object(harvest_task, "_iter_items_from", return_value=_aiter(items)),
        patch.object(harvest_task, "_apply_harvest_presets", return_value=None),
    ):
        stats = await harvest_task.run_harvest(
            request, catalogs, preset_ctx=object(), base_scope="catalog:cat-7"
        )

    assert stats.collections_written == 1
    payload = catalogs.create_collection.await_args.args[1]
    assert payload["id"] == "agera5-rh12"
    assert payload["extent"] == source_extent
    assert payload["assets"] == source_coll["assets"]
    assert payload["providers"] == source_coll["providers"]
    assert payload["summaries"] == source_coll["summaries"]
    extra = payload["extra_metadata"]
    assert extra["extent"] == source_extent
    assert extra["stac_extensions"] == source_coll["stac_extensions"]
    assert extra["assets"] == source_coll["assets"]
    assert extra["cube:dimensions"] == source_coll["cube:dimensions"]
    assert extra["cube:variables"] == source_coll["cube:variables"]
    assert "links" not in payload
    assert "links" not in extra


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


@pytest.mark.asyncio
async def test_catalog_harvest_can_skip_empty_source_collections():
    """skip_empty_collections avoids creating local collection metadata when
    the source collection's item walk drains without yielding any item."""
    empty = {"type": "Collection", "id": "empty-source", "description": "d"}
    full = {"type": "Collection", "id": "full-source", "description": "d"}
    item = {"type": "Feature", "id": "i1", "geometry": None, "properties": {}}

    def fake_iter_items(_catalog_url: str, collection_id: str, *, cursor=None):
        if collection_id == "empty-source":
            return _aiter([])
        return _aiter([item])

    request = StacHarvestRequest(
        catalog_url="https://src",
        target_catalog="cat-7",
        drivers="es",
        skip_empty_collections=True,
    )
    catalogs = _mock_catalogs()

    with (
        patch.object(harvest_task, "_probe_single_collection", return_value=None),
        patch.object(harvest_task, "iter_collections", return_value=_aiter([empty, full])),
        patch.object(harvest_task, "iter_items", side_effect=fake_iter_items),
        patch.object(harvest_task, "_apply_harvest_presets", return_value=None),
    ):
        stats = await harvest_task.run_harvest(
            request, catalogs, preset_ctx=object(), base_scope="catalog:cat-7"
        )

    assert stats.collections_seen == 2
    assert stats.collections_written == 1
    assert stats.collections_skipped_empty == 1
    catalogs.create_collection.assert_awaited_once()
    assert catalogs.create_collection.await_args.args[1]["id"] == "full-source"


@pytest.mark.asyncio
async def test_single_collection_harvest_can_skip_empty_source_collection():
    """The skip option also applies when catalog_url points at one Collection."""
    source_coll = {"type": "Collection", "id": "MyColl", "description": "d"}
    request = StacHarvestRequest(
        catalog_url="https://src/c/MyColl",
        target_catalog="cat-7",
        drivers="es",
        skip_empty_collections=True,
    )
    catalogs = _mock_catalogs()

    with (
        patch.object(harvest_task, "_probe_single_collection",
                     return_value=(source_coll, "https://src/c/MyColl/items")),
        patch.object(harvest_task, "_iter_items_from", return_value=_aiter([])),
        patch.object(harvest_task, "_apply_harvest_presets", return_value=None),
    ):
        stats = await harvest_task.run_harvest(
            request, catalogs, preset_ctx=object(), base_scope="catalog:cat-7"
        )

    assert stats.collections_seen == 1
    assert stats.collections_written == 0
    assert stats.collections_skipped_empty == 1
    catalogs.create_collection.assert_not_awaited()
    catalogs.upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_skip_empty_does_not_skip_truncated_first_page():
    """A fetch failure can drain the iterator with PageCursor.truncated=True;
    that is not evidence that the source collection is empty."""
    coll = {"type": "Collection", "id": "C1", "description": "d"}
    request = StacHarvestRequest(
        catalog_url="https://src",
        target_catalog="cat-7",
        drivers="es",
        skip_empty_collections=True,
    )
    catalogs = _mock_catalogs()

    def fake_iter_items(_catalog_url: str, _collection_id: str, *, cursor=None):
        cursor.truncated = True
        return _aiter([])

    with (
        patch.object(harvest_task, "_probe_single_collection", return_value=None),
        patch.object(harvest_task, "iter_collections", return_value=_aiter([coll])),
        patch.object(harvest_task, "iter_items", side_effect=fake_iter_items),
        patch.object(harvest_task, "_apply_harvest_presets", return_value=None),
    ):
        stats = await harvest_task.run_harvest(
            request, catalogs, preset_ctx=object(), base_scope="catalog:cat-7"
        )

    assert stats.collections_seen == 1
    assert stats.collections_written == 1
    assert stats.collections_skipped_empty == 0
    catalogs.create_collection.assert_awaited_once()


# ---------------------------------------------------------------------------
# run_harvest — per-collection read-policy pin (#3070)
# ---------------------------------------------------------------------------


async def _run_single_collection_harvest(request: StacHarvestRequest):
    """Drive a single-collection harvest with a config writer attached to the
    preset context, returning ``(stats, config_writer)`` so a test can assert on
    the pinned ItemsReadPolicy."""
    source_coll = {"type": "Collection", "id": "MyColl", "description": "d"}
    items = [{"type": "Feature", "id": "i1", "geometry": None, "properties": {}}]

    async def fake_apply(ctx, scope, catalog_id, drivers):
        return None

    catalogs = _mock_catalogs()
    config_writer = AsyncMock()
    config_writer.set_config = AsyncMock(return_value=None)
    preset_ctx = SimpleNamespace(config=config_writer)

    with (
        patch.object(harvest_task, "_probe_single_collection",
                     return_value=(source_coll, "https://src/c/MyColl/items")),
        patch.object(harvest_task, "_iter_items_from", return_value=_aiter(items)),
        patch.object(harvest_task, "_apply_harvest_presets", side_effect=fake_apply),
    ):
        stats = await harvest_task.run_harvest(
            request, catalogs, preset_ctx=preset_ctx, base_scope="catalog:cat-7"
        )
    return stats, config_writer


@pytest.mark.asyncio
async def test_harvest_pins_external_id_read_policy_by_default():
    """#3070 — a harvest pins each collection's ItemsReadPolicy with
    ``external_id_as_feature_id=True`` (the default) at collection scope, so the
    harvested item id round-trips the source STAC id across STAC and Features."""
    from dynastore.modules.storage.read_policy import ItemsReadPolicy

    request = StacHarvestRequest(
        catalog_url="https://src/c/MyColl", target_catalog="cat-7", drivers="es",
    )
    stats, config_writer = await _run_single_collection_harvest(request)

    assert not stats.errors
    config_writer.set_config.assert_awaited_once()
    call = config_writer.set_config.await_args
    # Positional: (config_cls, config, catalog_id, collection_id).
    assert call.args[0] is ItemsReadPolicy
    policy = call.args[1]
    assert isinstance(policy, ItemsReadPolicy)
    assert policy.feature_type.external_id_as_feature_id is True
    assert call.args[2] == "cat-7"
    assert call.args[3] == "mycoll"


@pytest.mark.asyncio
async def test_harvest_read_policy_opt_out_pins_false():
    """``external_id_as_feature_id=False`` on the request pins the collection
    policy to expose the internal geoid instead of the source id."""
    request = StacHarvestRequest(
        catalog_url="https://src/c/MyColl", target_catalog="cat-7", drivers="es",
        external_id_as_feature_id=False,
    )
    stats, config_writer = await _run_single_collection_harvest(request)

    assert not stats.errors
    policy = config_writer.set_config.await_args.args[1]
    assert policy.feature_type.external_id_as_feature_id is False


@pytest.mark.asyncio
async def test_harvest_read_policy_pin_failure_is_soft_error():
    """A read-policy write failure is recorded as a soft error and never aborts
    the item walk (best-effort, mirroring the routing/storage presets)."""
    request = StacHarvestRequest(
        catalog_url="https://src/c/MyColl", target_catalog="cat-7", drivers="es",
    )
    source_coll = {"type": "Collection", "id": "MyColl", "description": "d"}
    items = [{"type": "Feature", "id": "i1", "geometry": None, "properties": {}}]

    async def fake_apply(ctx, scope, catalog_id, drivers):
        return None

    catalogs = _mock_catalogs()
    config_writer = AsyncMock()
    config_writer.set_config = AsyncMock(side_effect=RuntimeError("boom"))
    preset_ctx = SimpleNamespace(config=config_writer)

    with (
        patch.object(harvest_task, "_probe_single_collection",
                     return_value=(source_coll, "https://src/c/MyColl/items")),
        patch.object(harvest_task, "_iter_items_from", return_value=_aiter(items)),
        patch.object(harvest_task, "_apply_harvest_presets", side_effect=fake_apply),
    ):
        stats = await harvest_task.run_harvest(
            request, catalogs, preset_ctx=preset_ctx, base_scope="catalog:cat-7"
        )

    # Items still ingested; the failure surfaced as a soft error, not a raise.
    assert stats.items_written == 1
    assert any(e.startswith("read_policy:mycoll:RuntimeError") for e in stats.errors)


# ---------------------------------------------------------------------------
# StacHarvestTask.run — total-failure guard
# ---------------------------------------------------------------------------


class _Payload:
    """Minimal stand-in for TaskPayload — ``run`` reads ``.inputs``/``.task_id``."""

    def __init__(self, inputs: Dict[str, Any]) -> None:
        self.inputs = {"inputs": inputs}
        self.task_id = "11111111-1111-1111-1111-111111111111"


def _run_task_with_stats(stats: harvest_task.HarvestStats):
    """Build a StacHarvestTask with everything external mocked and return
    the coroutine for ``run`` with ``run_harvest`` pinned to ``stats``."""
    task = object.__new__(harvest_task.StacHarvestTask)
    task.app_state = None
    task.engine = object()

    payload = _Payload(
        {"catalog_url": "https://src", "target_catalog": "cat-7", "drivers": "es"}
    )

    return patch.multiple(
        harvest_task,
        run_harvest=AsyncMock(return_value=stats),
    ), patch(
        "dynastore.tools.discovery.get_protocol",
        side_effect=lambda proto: AsyncMock(),
    ), task, payload


@pytest.mark.asyncio
async def test_run_raises_when_every_collection_failed_and_nothing_written():
    """collections_seen > 0 with zero writes and per-collection errors is a
    total failure — the task must fail, not report success with empty stats."""
    stats = harvest_task.HarvestStats(
        collections_seen=1970,
        errors=[f"collection:c{i}" for i in range(3)],
    )
    run_patch, proto_patch, task, payload = _run_task_with_stats(stats)

    with run_patch, proto_patch:
        with pytest.raises(RuntimeError, match="nothing harvested"):
            await task.run(payload)


@pytest.mark.asyncio
async def test_run_partial_failure_stays_successful_with_error_count():
    """Some collections written + some errors is a partial failure — the run
    succeeds and the summary carries the error count."""
    stats = harvest_task.HarvestStats(
        collections_seen=10,
        collections_written=7,
        items_written=100,
        errors=["collection:bad1", "collection:bad2"],
    )
    run_patch, proto_patch, task, payload = _run_task_with_stats(stats)

    with run_patch, proto_patch:
        result = await task.run(payload)

    assert result is not None
    assert "errors=2" in result["message"]
    assert result["collections_written"] == 7


@pytest.mark.asyncio
async def test_run_empty_source_is_not_a_failure():
    """Zero collections seen (empty source) has nothing to fail on."""
    stats = harvest_task.HarvestStats()
    run_patch, proto_patch, task, payload = _run_task_with_stats(stats)

    with run_patch, proto_patch:
        result = await task.run(payload)

    assert result is not None
    assert result["collections_seen"] == 0
