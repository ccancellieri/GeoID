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

"""Unit tests for the stac_harvest wall-clock stall circuit breaker (geoid#2890).

No live DB or network — the source items iterator and the catalogs backend are
mocked; ``time.monotonic`` is patched to deterministically drive the streak
clock without real sleeps. ``_BATCH_SIZE`` is patched down to 1 so each source
item maps to exactly one flush, keeping the fixtures small.
"""
from __future__ import annotations

import inspect
import time
from typing import Any, Dict, List
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.tasks.stac_harvest import task as harvest_task
from dynastore.tasks.stac_harvest.models import StacHarvestRequest


def _task_scoped_monotonic(*values: float):
    """Build a ``time.monotonic`` replacement that only hands out ``values``
    to calls made from inside ``dynastore.tasks.stac_harvest.task`` (the
    module's single ``time.monotonic()`` call site, in ``_flush``).

    Patching ``harvest_task.time.monotonic`` patches the process-wide ``time``
    module, which asyncio's own event loop also reads for scheduling. Falling
    through to the real clock for every other caller keeps the loop's timing
    intact while still deterministically driving the stall clock.

    ``patch.object(..., side_effect=...)`` invokes this function through
    ``unittest.mock``'s own call machinery, so the immediate caller frame(s)
    belong to ``unittest.mock`` rather than the real caller — walk past those
    before checking ``__name__``.
    """
    it = iter(values)
    real_monotonic = time.monotonic

    def _fn() -> float:
        frame = inspect.currentframe().f_back
        while frame is not None and frame.f_globals.get("__name__") == "unittest.mock":
            frame = frame.f_back
        if frame is not None and frame.f_globals.get("__name__") == harvest_task.__name__:
            try:
                return next(it)
            except StopIteration:
                pass
        return real_monotonic()

    return _fn


def _mock_catalogs() -> AsyncMock:
    catalogs = AsyncMock()
    catalogs.get_collection = AsyncMock(return_value=None)  # not present → create
    catalogs.create_collection = AsyncMock(return_value=None)
    catalogs.update_collection = AsyncMock(return_value=None)
    return catalogs


async def _aiter(items: List[Dict[str, Any]]):
    for it in items:
        yield it


def _request(**overrides: Any) -> StacHarvestRequest:
    params: Dict[str, Any] = {
        "catalog_url": "https://src",
        "target_catalog": "cat-7",
        "drivers": "es",
        "with_assets": False,
    }
    params.update(overrides)
    return StacHarvestRequest(**params)


def _valid_items(n: int) -> List[Dict[str, Any]]:
    return [
        {"type": "Feature", "id": f"i{i}", "geometry": None, "properties": {}}
        for i in range(n)
    ]


def _invalid_items(n: int) -> List[Dict[str, Any]]:
    # No ``type``/``geometry``/``properties`` — fails Feature.model_validate.
    return [{"not_a_feature": i} for i in range(n)]


@pytest.mark.asyncio
async def test_flush_resets_stall_clock_on_successful_write():
    """Every batch writes successfully — the stall clock stays unset throughout."""
    catalogs = _mock_catalogs()
    catalogs.upsert = AsyncMock(return_value=None)
    request = _request()
    stats = harvest_task.HarvestStats()
    source_coll = {"type": "Collection", "id": "c1", "description": "d"}

    with patch.object(harvest_task, "_BATCH_SIZE", 1):
        await harvest_task._harvest_collection(
            catalogs, request, source_coll, _aiter(_valid_items(3)), "c1", stats,
        )

    assert stats.items_written == 3
    assert stats._stall_since is None


@pytest.mark.asyncio
async def test_flush_raises_after_sustained_zero_progress():
    """Consecutive failing flushes spanning >= _STALL_ABORT_SECONDS abort the harvest.

    Also proven separately below (test_flush_raises_across_collections_in_run_harvest)
    that moving to a new collection does NOT reset the streak.
    """
    catalogs = _mock_catalogs()
    catalogs.upsert = AsyncMock(side_effect=RuntimeError("boom"))
    request = _request()
    stats = harvest_task.HarvestStats()
    source_coll = {"type": "Collection", "id": "c1", "description": "d"}

    with (
        patch.object(harvest_task, "_BATCH_SIZE", 1),
        patch.object(
            harvest_task.time,
            "monotonic",
            side_effect=_task_scoped_monotonic(
                1000.0, 1000.0 + harvest_task._STALL_ABORT_SECONDS
            ),
        ),
    ):
        with pytest.raises(RuntimeError, match="aborting"):
            await harvest_task._harvest_collection(
                catalogs, request, source_coll, _aiter(_valid_items(2)), "c1", stats,
            )


@pytest.mark.asyncio
async def test_flush_raises_across_collections_in_run_harvest():
    """The zero-progress streak spans collections — a fresh collection does not
    reset the clock, so a two-collection harvest that never writes still aborts."""
    catalogs = _mock_catalogs()
    catalogs.upsert = AsyncMock(side_effect=RuntimeError("boom"))

    coll1 = {"type": "Collection", "id": "c1", "description": "d"}
    coll2 = {"type": "Collection", "id": "c2", "description": "d"}
    request = _request()

    with (
        patch.object(harvest_task, "_BATCH_SIZE", 1),
        patch.object(
            harvest_task.time,
            "monotonic",
            side_effect=_task_scoped_monotonic(
                2000.0, 2000.0 + harvest_task._STALL_ABORT_SECONDS
            ),
        ),
        patch.object(harvest_task, "_probe_single_collection", return_value=None),
        patch.object(harvest_task, "iter_collections", return_value=_aiter([coll1, coll2])),
        patch.object(
            harvest_task, "iter_items",
            side_effect=[_aiter(_valid_items(1)), _aiter(_valid_items(1))],
        ),
        patch.object(harvest_task, "_apply_harvest_presets", AsyncMock(return_value=None)),
    ):
        with pytest.raises(RuntimeError, match="aborting"):
            await harvest_task.run_harvest(
                request, catalogs, preset_ctx=object(), base_scope="catalog:cat-7",
            )


@pytest.mark.asyncio
async def test_flush_does_not_trip_on_transient_failure_then_recovery():
    """A success in the middle of a failing streak resets the clock — the
    harvest is never killed for a merely-transient error."""
    catalogs = _mock_catalogs()
    catalogs.upsert = AsyncMock(
        side_effect=[RuntimeError("boom"), RuntimeError("boom"), None,
                     RuntimeError("boom"), RuntimeError("boom")]
    )
    request = _request()
    stats = harvest_task.HarvestStats()
    source_coll = {"type": "Collection", "id": "c1", "description": "d"}

    # monotonic is only sampled on failing flushes (4 of the 5 batches);
    # each segment advances by well under _STALL_ABORT_SECONDS.
    with (
        patch.object(harvest_task, "_BATCH_SIZE", 1),
        patch.object(
            harvest_task.time,
            "monotonic",
            side_effect=_task_scoped_monotonic(0.0, 100.0, 250.0, 550.0),
        ),
    ):
        await harvest_task._harvest_collection(
            catalogs, request, source_coll, _aiter(_valid_items(5)), "c1", stats,
        )

    assert stats.items_written == 1
    assert stats.items_failed == 4


@pytest.mark.asyncio
async def test_empty_collection_never_calls_flush():
    """An empty source collection never accumulates a batch, so _flush (and the
    stall clock) is never exercised at all."""
    catalogs = _mock_catalogs()
    request = _request()
    stats = harvest_task.HarvestStats()
    source_coll = {"type": "Collection", "id": "c1", "description": "d"}

    with patch.object(
        harvest_task, "_upsert_items_batch", AsyncMock(return_value=(0, None))
    ) as mock_upsert_batch:
        await harvest_task._harvest_collection(
            catalogs, request, source_coll, _aiter([]), "c1", stats,
        )

    mock_upsert_batch.assert_not_called()
    assert stats._stall_since is None


@pytest.mark.asyncio
async def test_all_items_invalid_still_trips_after_threshold():
    """A source collection whose items all fail Feature validation writes zero
    docs (catalogs.upsert is never even called) — a genuine zero-progress case
    that must still trip after the threshold."""
    catalogs = _mock_catalogs()
    catalogs.upsert = AsyncMock(return_value=None)  # never reached
    request = _request()
    stats = harvest_task.HarvestStats()
    source_coll = {"type": "Collection", "id": "c1", "description": "d"}

    with (
        patch.object(harvest_task, "_BATCH_SIZE", 1),
        patch.object(
            harvest_task.time,
            "monotonic",
            side_effect=_task_scoped_monotonic(
                3000.0, 3000.0 + harvest_task._STALL_ABORT_SECONDS
            ),
        ),
    ):
        with pytest.raises(RuntimeError, match="aborting"):
            await harvest_task._harvest_collection(
                catalogs, request, source_coll, _aiter(_invalid_items(2)), "c1", stats,
            )

    catalogs.upsert.assert_not_called()
