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

"""Raster/vector render-admission bulkhead independence (geoid#3209).

Follow-up to #3161's single shared per-worker render-admission gate: raster
(rio-tiler/COG) and vector (PostGIS MVT + vector-rendered map PNG) renders
now draw from SEPARATE ``RenderAdmissionGate`` budgets on ``TilesService``,
so a burst of one class can no longer exhaust the other's capacity. These
tests exercise the actual wired attributes (``TilesService._raster_render_gate``
/ ``_vector_render_gate``), not just the ``RenderAdmissionGate`` primitive in
isolation, so a regression that re-aliases both names to one shared instance
would fail here.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

pytest.importorskip("morecantile", reason="morecantile not installed — tiles imports it")

from dynastore.extensions.tiles.tiles_service import TilesService
from dynastore.tools.render_admission import (
    DEFAULT_RASTER_RENDER_SHARE,
    DEFAULT_RENDER_SHARE,
    DEFAULT_VECTOR_RENDER_SHARE,
    RenderAdmissionGate,
    RenderAdmissionRejected,
)


def _make_service_with_bulkheads(
    *, raster_max: int, vector_max: int, queue_wait_seconds: float = 0.05
) -> TilesService:
    """A TilesService with small, deterministic per-class caps — bypasses
    ``__init__`` (same ``object.__new__`` pattern already used elsewhere in
    this test package, e.g. ``test_map_tile_render_dispatch.py``) so no
    memory-budget/env resolution is involved."""
    svc = object.__new__(TilesService)
    svc._raster_render_gate = RenderAdmissionGate(  # type: ignore[attr-defined]
        max_concurrent=raster_max,
        queue_wait_seconds=queue_wait_seconds,
        get_rss_bytes=lambda: None,
        get_budget_bytes=lambda: None,
    )
    svc._vector_render_gate = RenderAdmissionGate(  # type: ignore[attr-defined]
        max_concurrent=vector_max,
        queue_wait_seconds=queue_wait_seconds,
        get_rss_bytes=lambda: None,
        get_budget_bytes=lambda: None,
    )
    return svc


# ---------------------------------------------------------------------------
# Independence: exhausting one class's budget never blocks the other's.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_exhausting_raster_budget_does_not_block_vector_render():
    svc = _make_service_with_bulkheads(raster_max=1, vector_max=1)

    # Exhaust the ONLY raster slot.
    await svc._raster_render_gate.acquire()

    # A vector render must still be admitted immediately — it draws from a
    # separate budget. Bounded wait proves it is not queueing behind raster.
    await asyncio.wait_for(svc._vector_render_gate.acquire(), timeout=0.5)
    svc._vector_render_gate.release()

    # Sanity: the raster gate really is exhausted (not a false negative
    # where both gates happened to have free capacity for another reason).
    with pytest.raises(RenderAdmissionRejected) as exc_info:
        await svc._raster_render_gate.acquire()
    assert exc_info.value.reason == "queue_timeout"

    svc._raster_render_gate.release()


@pytest.mark.asyncio
async def test_exhausting_vector_budget_does_not_block_raster_render():
    svc = _make_service_with_bulkheads(raster_max=1, vector_max=1)

    # Exhaust the ONLY vector slot.
    await svc._vector_render_gate.acquire()

    # A raster render must still be admitted immediately.
    await asyncio.wait_for(svc._raster_render_gate.acquire(), timeout=0.5)
    svc._raster_render_gate.release()

    # Sanity: the vector gate really is exhausted.
    with pytest.raises(RenderAdmissionRejected) as exc_info:
        await svc._vector_render_gate.acquire()
    assert exc_info.value.reason == "queue_timeout"

    svc._vector_render_gate.release()


# ---------------------------------------------------------------------------
# Wiring: the two gates are genuinely distinct objects, not two names for
# the same one (the exact regression the split above would otherwise miss
# if a future edit re-aliased them).
# ---------------------------------------------------------------------------


def test_class_level_default_gates_are_distinct_instances():
    """Covers the ``object.__new__`` bypass path other tests in this
    package rely on (e.g. test_map_tile_render_dispatch.py) — the
    class-level defaults must already be two separate gates, not one
    shared object under two names."""
    assert (
        TilesService._raster_render_gate is not TilesService._vector_render_gate
    )


def test_constructed_service_gets_fresh_distinct_gate_instances():
    svc = TilesService()
    assert svc._raster_render_gate is not svc._vector_render_gate
    # __init__ must also not reuse the class-level default objects — every
    # instance gets its own gates (mirrors the pre-split _render_gate
    # comment/contract).
    assert svc._raster_render_gate is not TilesService._raster_render_gate
    assert svc._vector_render_gate is not TilesService._vector_render_gate


# ---------------------------------------------------------------------------
# Startup observability (geoid#3209 part 1): the computed per-class caps
# must be logged once at construction so they are visible without needing a
# shed to occur first.
# ---------------------------------------------------------------------------


def test_startup_logs_computed_raster_and_vector_caps(caplog):
    with caplog.at_level(logging.INFO, logger="dynastore.extensions.tiles.tiles_service"):
        svc = TilesService()

    messages = [r.getMessage() for r in caplog.records]
    matches = [m for m in messages if "render admission bulkheads" in m]
    assert matches, f"no render-admission startup log line found in: {messages!r}"

    logged = matches[0]
    assert f"raster max_concurrent={svc._raster_render_gate.max_concurrent}" in logged
    assert f"vector max_concurrent={svc._vector_render_gate.max_concurrent}" in logged


# ---------------------------------------------------------------------------
# Conservative defaults: the split must not, by default, raise the combined
# worst case above what the single pre-split gate allowed.
# ---------------------------------------------------------------------------


def test_split_shares_are_conservative_relative_to_the_pre_split_share():
    assert DEFAULT_RASTER_RENDER_SHARE + DEFAULT_VECTOR_RENDER_SHARE <= DEFAULT_RENDER_SHARE
