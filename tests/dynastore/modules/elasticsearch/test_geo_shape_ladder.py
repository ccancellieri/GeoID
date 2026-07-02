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

"""Tests for `dynastore.modules.elasticsearch.geo_shape_ladder` (#2769)."""
from __future__ import annotations

import pytest

from dynastore.modules.elasticsearch.geo_shape_ladder import (
    RUNG_AGGRESSIVE_SIMPLIFY,
    RUNG_ANTIMERIDIAN_SPLIT,
    RUNG_BBOX_ENVELOPE,
    RUNG_ORIENT_VALIDATE,
    degrade_rungs,
    retry_doc_with_ladder,
)


_POLE_GEOMETRY = {
    "type": "MultiPolygon",
    "coordinates": [[[
        [170.0, -85.0], [90.0, -87.0], [0.0, -90.0], [-90.0, -87.0],
        [-170.0, -85.0], [-170.0, -60.0], [170.0, -60.0], [170.0, -85.0],
    ]]],
}

_SIMPLE_GEOMETRY = {
    "type": "Polygon",
    "coordinates": [[[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [1.0, 0.0], [0.0, 0.0]]],
}


# ---------------------------------------------------------------------------
# degrade_rungs
# ---------------------------------------------------------------------------


def test_degrade_rungs_always_reaches_bbox_envelope():
    """The bbox rung always produces a candidate — a well-formed geometry
    input therefore always yields at least that final rung."""
    rungs = list(degrade_rungs(_SIMPLE_GEOMETRY))
    names = [name for name, _ in rungs]
    assert names[-1] == RUNG_BBOX_ENVELOPE


def test_degrade_rungs_yields_progressively_from_pole_touching_fixture():
    rungs = list(degrade_rungs(_POLE_GEOMETRY))
    names = [name for name, _ in rungs]
    # Rungs run in fixed declared order; not every rung necessarily produces
    # a candidate (e.g. antimeridian split is a no-op below the threshold),
    # but the order among whichever DO fire must be preserved.
    expected_order = [
        RUNG_ORIENT_VALIDATE, RUNG_ANTIMERIDIAN_SPLIT,
        RUNG_AGGRESSIVE_SIMPLIFY, RUNG_BBOX_ENVELOPE,
    ]
    assert names == [n for n in expected_order if n in names]
    assert RUNG_BBOX_ENVELOPE in names


def test_degrade_rungs_empty_for_falsy_geometry():
    assert list(degrade_rungs(None)) == []
    assert list(degrade_rungs({})) == []


# ---------------------------------------------------------------------------
# retry_doc_with_ladder
# ---------------------------------------------------------------------------


class _AlwaysFailEs:
    def __init__(self) -> None:
        self.calls: list = []

    async def index(self, *, index, id, body, params=None):
        self.calls.append((index, id, body, params))
        raise RuntimeError("document_parsing_exception: still rejected")


class _AcceptsOnRungEs:
    """Fake ES client that only accepts a document once its geometry
    matches the candidate produced by *accept_rung*."""

    def __init__(self, accept_after: int) -> None:
        self.calls: list = []
        self._accept_after = accept_after

    async def index(self, *, index, id, body, params=None):
        self.calls.append((index, id, body, params))
        if len(self.calls) < self._accept_after:
            raise RuntimeError("document_parsing_exception: still rejected")
        return {"result": "created"}


@pytest.mark.asyncio
async def test_retry_doc_with_ladder_no_geometry_short_circuits():
    es = _AlwaysFailEs()
    doc = {"id": "x", "properties": {}}
    recovered, rung = await retry_doc_with_ladder(
        es, index_name="idx", doc_id="x", doc=doc, reason="some error",
    )
    assert recovered is False
    assert rung is None
    assert es.calls == []


@pytest.mark.asyncio
async def test_retry_doc_with_ladder_exhausts_all_rungs():
    es = _AlwaysFailEs()
    doc = {"id": "x", "geometry": _POLE_GEOMETRY, "properties": {}}
    recovered, rung = await retry_doc_with_ladder(
        es, index_name="idx", doc_id="x", doc=doc,
        reason="document_parsing_exception: failed to parse field [geometry] of type [geo_shape]",
    )
    assert recovered is False
    assert rung is None
    # Every produced rung was attempted.
    assert len(es.calls) == len(list(degrade_rungs(_POLE_GEOMETRY)))


@pytest.mark.asyncio
async def test_retry_doc_with_ladder_recovers_on_second_rung():
    es = _AcceptsOnRungEs(accept_after=2)
    doc = {"id": "x", "geometry": _POLE_GEOMETRY, "properties": {}}
    recovered, rung = await retry_doc_with_ladder(
        es, index_name="idx", doc_id="x", doc=doc, reason="bad geo_shape",
        routing="col1",
    )
    assert recovered is True
    assert rung == RUNG_ANTIMERIDIAN_SPLIT
    assert len(es.calls) == 2
    # The accepting call carried the routing param through.
    _, _, _, params = es.calls[-1]
    assert params == {"routing": "col1"}


@pytest.mark.asyncio
async def test_retry_doc_with_ladder_recovers_on_bbox_rung_worst_case():
    """bbox_envelope always produces a candidate, so a client that only
    accepts the last rung still recovers rather than exhausting silently."""
    rung_count = len(list(degrade_rungs(_POLE_GEOMETRY)))
    es = _AcceptsOnRungEs(accept_after=rung_count)
    doc = {"id": "x", "geometry": _POLE_GEOMETRY, "properties": {}}
    recovered, rung = await retry_doc_with_ladder(
        es, index_name="idx", doc_id="x", doc=doc, reason="bad geo_shape",
    )
    assert recovered is True
    assert rung == RUNG_BBOX_ENVELOPE
