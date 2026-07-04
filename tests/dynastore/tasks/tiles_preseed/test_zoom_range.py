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

"""Tests for the request-level zoom-range bound on ``tiles_preseed`` (#2953).

Without a bound, the preseed job iterates the full configured zoom range
over the entire requested bbox in one Cloud Run job invocation, which grows
combinatorially with the zoom span and always exhausts the job's 3600s
timeout on any non-trivial bbox. These tests cover: (1) the input schema
rejects an invalid range, and (2) the zoom-resolution logic that feeds the
preseed loop actually honors a request-level override in isolation, without
needing a live DB or Cloud Run job.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("morecantile")  # optional dep — skip when SCOPE excludes it

from pydantic import ValidationError

from dynastore.tasks.tiles_preseed.models import MAX_TMS_ZOOM, TilePreseedRequest
from dynastore.tasks.tiles_preseed.task import TilePreseedTask, _resolve_effective_zoom_range
from dynastore.modules.tasks.models import TaskPayload
from dynastore.modules.processes.models import ExecuteRequest


# --- Schema validation -------------------------------------------------

def test_rejects_min_zoom_greater_than_max_zoom():
    with pytest.raises(ValidationError, match="min_zoom must be <= max_zoom"):
        TilePreseedRequest(catalog_id="cat", collection_id="col", min_zoom=10, max_zoom=5)


def test_rejects_max_zoom_beyond_tms_range():
    with pytest.raises(ValidationError):
        TilePreseedRequest(catalog_id="cat", collection_id="col", max_zoom=MAX_TMS_ZOOM + 1)


def test_rejects_negative_min_zoom():
    with pytest.raises(ValidationError):
        TilePreseedRequest(catalog_id="cat", collection_id="col", min_zoom=-1)


def test_accepts_valid_zoom_range():
    req = TilePreseedRequest(catalog_id="cat", collection_id="col", min_zoom=2, max_zoom=10)
    assert req.min_zoom == 2
    assert req.max_zoom == 10


def test_zoom_bounds_default_to_none():
    req = TilePreseedRequest(catalog_id="cat", collection_id="col")
    assert req.min_zoom is None
    assert req.max_zoom is None


def test_accepts_equal_min_and_max_zoom():
    req = TilePreseedRequest(catalog_id="cat", collection_id="col", min_zoom=5, max_zoom=5)
    assert req.min_zoom == req.max_zoom == 5


# --- Zoom-resolution logic (feeds the preseed loop) ---------------------

def test_resolution_falls_back_to_defaults_when_request_omits_zoom():
    request = TilePreseedRequest(catalog_id="cat", collection_id="col")
    min_zoom, max_zoom = _resolve_effective_zoom_range(request, runtime_min_zoom=0, default_max_zoom=12)
    assert (min_zoom, max_zoom) == (0, 12)


def test_resolution_honors_explicit_request_override():
    """The bug fix: a caller can narrow the zoom range so a single preseed
    job stays within the Cloud Run timeout, instead of always attempting
    the collection's full configured range."""
    request = TilePreseedRequest(catalog_id="cat", collection_id="col", min_zoom=8, max_zoom=10)
    min_zoom, max_zoom = _resolve_effective_zoom_range(request, runtime_min_zoom=0, default_max_zoom=12)
    assert (min_zoom, max_zoom) == (8, 10)


def test_resolution_honors_partial_override_min_only():
    request = TilePreseedRequest(catalog_id="cat", collection_id="col", min_zoom=6)
    min_zoom, max_zoom = _resolve_effective_zoom_range(request, runtime_min_zoom=0, default_max_zoom=12)
    assert (min_zoom, max_zoom) == (6, 12)


def test_resolution_honors_partial_override_max_only():
    request = TilePreseedRequest(catalog_id="cat", collection_id="col", max_zoom=4)
    min_zoom, max_zoom = _resolve_effective_zoom_range(request, runtime_min_zoom=0, default_max_zoom=12)
    assert (min_zoom, max_zoom) == (0, 4)


# --- Wiring: the preseed loop actually honors the bound ------------------

def _make_task() -> TilePreseedTask:
    task = TilePreseedTask.__new__(TilePreseedTask)
    task.app_state = None
    task.engine = MagicMock()
    return task


def _fake_mc_tms(tiles_per_zoom):
    tms = MagicMock()

    def _tiles(*bbox, zooms):
        z = zooms[0]
        return [MagicMock(x=i, y=i) for i in range(tiles_per_zoom.get(z, 0))]

    tms.tiles = MagicMock(side_effect=_tiles)
    return tms


@pytest.mark.asyncio
async def test_preseed_mvt_only_iterates_requested_zoom_range(monkeypatch):
    """A request narrowing the zoom range to [2, 3] must never touch zoom
    0 or 1, even though the collection's runtime config allows [0, 5] —
    this is the actual bound that keeps a single preseed job inside the
    Cloud Run job timeout."""
    import dynastore.tasks.tiles_preseed.task as task_mod

    @asynccontextmanager
    async def fake_managed_transaction(engine):
        yield MagicMock()

    monkeypatch.setattr(task_mod, "managed_transaction", fake_managed_transaction)
    monkeypatch.setattr(task_mod.DQLQuery, "execute", AsyncMock(return_value=None))
    monkeypatch.setattr(task_mod.tiles_engine, "render_tile", AsyncMock(return_value=None))
    monkeypatch.setattr(task_mod.tasks_module, "update_task", AsyncMock())

    task = _make_task()
    ctx = MagicMock()
    runtime_config = MagicMock(min_zoom=0, max_zoom=5, simplification_algorithm=None)
    preseed_config = MagicMock(simplification_by_zoom_override=None)
    storage = MagicMock()
    storage.save_tile = AsyncMock()

    mc_tms = _fake_mc_tms({0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 1})
    payload = TaskPayload(
        task_id="00000000-0000-0000-0000-000000000001",
        inputs=ExecuteRequest(inputs={}), caller_id="test",
    )
    results = {"generated": 0, "skipped": 0, "errors": 0}

    total = await task._preseed_mvt(
        engine=task.engine, payload=payload, ctx=ctx, storage=storage,
        catalog_id="cat1", col_id="coll1", tms_id="WebMercatorQuad",
        mc_tms=mc_tms,
        effective_bboxes=[(-10.0, -10.0, 10.0, 10.0)],
        effective_min_zoom=2, effective_max_zoom=3,
        runtime_config=runtime_config, preseed_config=preseed_config,
        formats=["mvt"], statement_timeout_ms=30000,
        schema="test_schema", results=results, total_processed_so_far=0,
    )

    requested_zooms = {call.kwargs["zooms"][0] for call in mc_tms.tiles.call_args_list}
    assert requested_zooms == {2, 3}
    assert total == 2
