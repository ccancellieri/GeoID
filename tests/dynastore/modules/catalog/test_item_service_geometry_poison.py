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

"""Pre-write poison-geometry guard in ``ItemService`` (#2112).

Scope
-----
* ES-backed catalogs: a geometry that collapses to empty after make_valid +
  polygonal coercion is rejected 400 (ValueError) *before* the item reaches
  any driver.
* ES-backed catalogs: a repairable geometry is accepted and has its geometry
  mutated in place so the ES driver receives an indexable shape.
* PG-only catalogs: no rejection regardless of geometry shape.
* ES-backed catalogs with ``simplify_geometry=True``: no rejection (the
  driver handles geometry transformations itself).

These are pure-unit tests: they call the guard helper directly with stub
resolved-driver objects.  No DB or app graph is constructed.
"""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Geometry fixtures
# ---------------------------------------------------------------------------


def _valid_polygon() -> dict:
    """A simple valid GeoJSON Polygon."""
    return {
        "type": "Polygon",
        "coordinates": [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]],
    }


def _valid_point() -> dict:
    return {"type": "Point", "coordinates": [0.0, 0.0]}


def _valid_linestring() -> dict:
    return {"type": "LineString", "coordinates": [[0.0, 0.0], [1.0, 1.0]]}


def _bowtie_polygon() -> dict:
    """A self-intersecting (bow-tie) polygon.

    ES rejects this with ``invalid_shape_exception``.  Shapely's ``make_valid``
    turns it into two triangles (a valid MultiPolygon) — so it IS repairable.
    """
    # Classic figure-eight / bow-tie: two triangles sharing a vertex
    return {
        "type": "Polygon",
        "coordinates": [
            [[0.0, 0.0], [2.0, 2.0], [2.0, 0.0], [0.0, 2.0], [0.0, 0.0]]
        ],
    }


def _duplicate_consecutive_coords_polygon() -> dict:
    """A polygon with duplicate consecutive coordinates.

    ES rejects this with ``invalid_shape_exception``.  After ``make_valid`` the
    duplicates are removed and a valid polygon survives.
    """
    return {
        "type": "Polygon",
        "coordinates": [
            [[0.0, 0.0], [0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]
        ],
    }


def _degenerate_ring_polygon() -> dict:
    """A polygon whose ring degenerates to a line (all co-linear points).

    ``make_valid`` returns a ``LineString`` (zero-area) which coerce_to_polygonal
    discards entirely → empty MultiPolygon → poison.
    """
    # All points on the x-axis → zero-area polygon → collapses to a LineString
    return {
        "type": "Polygon",
        "coordinates": [[[0.0, 0.0], [1.0, 0.0], [2.0, 0.0], [0.0, 0.0]]],
    }


def _geometry_collection_with_polygon() -> dict:
    """A GeometryCollection carrying a real polygon plus a LineString.

    ES ``geo_shape`` rejects ``GeometryCollection`` with
    ``mapper_parsing_exception`` when the field is typed for polygons.
    After coercion the polygon area survives → repairable (not poison).
    """
    return {
        "type": "GeometryCollection",
        "geometries": [
            {
                "type": "Polygon",
                "coordinates": [
                    [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]
                ],
            },
            {
                "type": "LineString",
                "coordinates": [[0.0, 0.0], [1.0, 1.0]],
            },
        ],
    }


def _geometry_collection_lines_only() -> dict:
    """A GeometryCollection with no polygon area — poison."""
    return {
        "type": "GeometryCollection",
        "geometries": [
            {"type": "LineString", "coordinates": [[0.0, 0.0], [1.0, 1.0]]},
            {"type": "Point", "coordinates": [0.5, 0.5]},
        ],
    }


# ---------------------------------------------------------------------------
# Stub drivers — mirror the pattern in test_item_service_geometry_guard.py
# ---------------------------------------------------------------------------


class _StubResolved:
    def __init__(self, driver):
        self.driver = driver


class _StubPgDriver:
    pass


_StubPgDriver.__name__ = "ItemsPostgresqlDriver"


class _StubEsDriver:
    is_es_items_driver = True

    def __init__(self, simplify_geometry: bool = False):
        self._simplify = simplify_geometry

    async def get_driver_config(self, catalog_id, collection_id, *, db_resource=None):
        class _Cfg:
            simplify_geometry: bool
        cfg = _Cfg()
        cfg.simplify_geometry = self._simplify
        return cfg


def _es_resolved(simplify_geometry: bool = False) -> _StubResolved:
    return _StubResolved(_StubEsDriver(simplify_geometry))


def _pg_resolved() -> _StubResolved:
    return _StubResolved(_StubPgDriver())


class _StubCtx:
    def __init__(self, seed_rejections: bool = True):
        self.extensions = {"_rejections": []} if seed_rejections else {}
        self.db_resource = None


# ---------------------------------------------------------------------------
# repair_geometry_for_es — pure-function unit tests
# ---------------------------------------------------------------------------


def test_valid_polygon_unchanged():
    from dynastore.tools.geometry_repair import repair_geometry_for_es

    geom = _valid_polygon()
    repaired, was_repaired, is_poison = repair_geometry_for_es(geom)
    assert not is_poison
    assert not was_repaired
    assert repaired == geom


def test_valid_point_unchanged():
    from dynastore.tools.geometry_repair import repair_geometry_for_es

    geom = _valid_point()
    repaired, was_repaired, is_poison = repair_geometry_for_es(geom)
    assert not is_poison
    assert not was_repaired
    assert repaired == geom


def test_valid_linestring_unchanged():
    """Non-polygon geometry types pass through without repair."""
    from dynastore.tools.geometry_repair import repair_geometry_for_es

    geom = _valid_linestring()
    repaired, was_repaired, is_poison = repair_geometry_for_es(geom)
    assert not is_poison
    assert not was_repaired


def test_bowtie_polygon_is_repaired_not_poison():
    """A bow-tie polygon is invalid but repairable (make_valid splits it)."""
    from dynastore.tools.geometry_repair import repair_geometry_for_es

    geom = _bowtie_polygon()
    repaired, was_repaired, is_poison = repair_geometry_for_es(geom)
    assert not is_poison, "bow-tie should be repairable, not poison"
    assert was_repaired, "bow-tie should be marked as repaired"
    assert repaired is not None
    assert repaired["type"] in ("Polygon", "MultiPolygon")


def test_duplicate_consecutive_coords_is_repaired_not_poison():
    """Duplicate consecutive coordinates: make_valid removes dupes — repairable."""
    from dynastore.tools.geometry_repair import repair_geometry_for_es

    geom = _duplicate_consecutive_coords_polygon()
    repaired, was_repaired, is_poison = repair_geometry_for_es(geom)
    assert not is_poison
    # Repaired or already-valid depending on shapely version; either way not poison.
    assert repaired is not None


def test_degenerate_ring_is_poison():
    """A zero-area polygon (all co-linear) collapses to empty → poison."""
    from dynastore.tools.geometry_repair import repair_geometry_for_es

    geom = _degenerate_ring_polygon()
    _, _, is_poison = repair_geometry_for_es(geom)
    assert is_poison, "zero-area degenerate polygon should be classified poison"


def test_geometry_collection_with_polygon_is_repaired():
    """GeometryCollection + polygon area → coercion succeeds (repairable)."""
    from dynastore.tools.geometry_repair import repair_geometry_for_es

    geom = _geometry_collection_with_polygon()
    repaired, was_repaired, is_poison = repair_geometry_for_es(geom)
    assert not is_poison
    assert was_repaired
    assert repaired is not None
    assert repaired["type"] in ("Polygon", "MultiPolygon")


def test_geometry_collection_lines_only_is_poison():
    """GeometryCollection with no polygonal area → poison."""
    from dynastore.tools.geometry_repair import repair_geometry_for_es

    geom = _geometry_collection_lines_only()
    _, _, is_poison = repair_geometry_for_es(geom)
    assert is_poison


def test_none_geometry_returns_none():
    from dynastore.tools.geometry_repair import repair_geometry_for_es

    repaired, was_repaired, is_poison = repair_geometry_for_es(None)
    assert repaired is None
    assert not was_repaired
    assert not is_poison


# ---------------------------------------------------------------------------
# ItemService._enforce_es_geometry_poison_check — integration with guard logic
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_valid_polygon_passes_through_es_guard():
    """A valid polygon is accepted unchanged by the guard."""
    from dynastore.modules.catalog.item_service import ItemService

    svc = ItemService()
    items = [{"id": "ok", "type": "Feature", "geometry": _valid_polygon(), "properties": {}}]
    result = await svc._enforce_es_geometry_poison_check(
        "cat", "col", items, [_pg_resolved(), _es_resolved()],
    )
    assert len(result) == 1


@pytest.mark.asyncio
async def test_bowtie_repaired_in_place_es_guard():
    """A repairable bow-tie polygon is accepted; its geometry is fixed in place."""
    from dynastore.modules.catalog.item_service import ItemService

    svc = ItemService()
    item = {"id": "bowtie", "type": "Feature", "geometry": _bowtie_polygon(), "properties": {}}
    result = await svc._enforce_es_geometry_poison_check(
        "cat", "col", [item], [_pg_resolved(), _es_resolved()],
    )
    assert len(result) == 1
    # The geometry on the item dict must have been updated to a valid polygon type.
    assert result[0]["geometry"]["type"] in ("Polygon", "MultiPolygon")


@pytest.mark.asyncio
async def test_geometry_collection_with_polygon_repaired():
    """A GeometryCollection carrying polygon area is accepted after coercion."""
    from dynastore.modules.catalog.item_service import ItemService

    svc = ItemService()
    item = {
        "id": "gc",
        "type": "Feature",
        "geometry": _geometry_collection_with_polygon(),
        "properties": {},
    }
    result = await svc._enforce_es_geometry_poison_check(
        "cat", "col", [item], [_pg_resolved(), _es_resolved()],
    )
    assert len(result) == 1
    # Must be coerced from GeometryCollection to a proper polygon type.
    assert result[0]["geometry"]["type"] in ("Polygon", "MultiPolygon")


@pytest.mark.asyncio
async def test_degenerate_polygon_rejected_single_item():
    """A degenerate (zero-area) polygon raises ValueError on single-item ingest."""
    from dynastore.modules.catalog.item_service import ItemService

    svc = ItemService()
    item = {
        "id": "degen",
        "type": "Feature",
        "geometry": _degenerate_ring_polygon(),
        "properties": {},
    }
    with pytest.raises(ValueError) as exc:
        await svc._enforce_es_geometry_poison_check(
            "cat", "col", [item], [_pg_resolved(), _es_resolved()],
        )
    msg = str(exc.value)
    # Message must identify the item and mention poison/invalid_shape.
    assert "degen" in msg
    assert "empty" in msg.lower() or "poison" in msg.lower() or "collapses" in msg.lower()


@pytest.mark.asyncio
async def test_geometry_collection_lines_only_rejected_single():
    """GeometryCollection with no polygon area → ValueError (→ HTTP 400)."""
    from dynastore.modules.catalog.item_service import ItemService

    svc = ItemService()
    item = {
        "id": "gc-lines",
        "type": "Feature",
        "geometry": _geometry_collection_lines_only(),
        "properties": {},
    }
    with pytest.raises(ValueError):
        await svc._enforce_es_geometry_poison_check(
            "cat", "col", [item], [_pg_resolved(), _es_resolved()],
        )


@pytest.mark.asyncio
async def test_poison_geometry_on_pg_only_catalog_is_not_rejected():
    """Requirement: PG-only catalog must NOT reject any geometry shape."""
    from dynastore.modules.catalog.item_service import ItemService

    svc = ItemService()
    items = [
        {"id": "x", "type": "Feature", "geometry": _degenerate_ring_polygon(), "properties": {}},
        {"id": "y", "type": "Feature", "geometry": _geometry_collection_lines_only(), "properties": {}},
    ]
    result = await svc._enforce_es_geometry_poison_check(
        "cat", "col", items, [_pg_resolved()],
    )
    # PG-only: guard is inactive, all items pass through.
    assert len(result) == 2


@pytest.mark.asyncio
async def test_poison_geometry_with_es_simplify_enabled_not_rejected():
    """When ES simplify_geometry=True the poison guard is inactive."""
    from dynastore.modules.catalog.item_service import ItemService

    svc = ItemService()
    item = {
        "id": "degen",
        "type": "Feature",
        "geometry": _degenerate_ring_polygon(),
        "properties": {},
    }
    # simplify_geometry=True → guard does not activate.
    result = await svc._enforce_es_geometry_poison_check(
        "cat", "col", [item], [_pg_resolved(), _es_resolved(simplify_geometry=True)],
    )
    assert len(result) == 1


@pytest.mark.asyncio
async def test_bulk_poison_drained_to_rejections():
    """In a bulk ingest with a rejection channel, a poison geometry is reported
    per-item via ctx.extensions['_rejections'] (→ HTTP 207), not raised."""
    from dynastore.modules.catalog.item_service import ItemService

    svc = ItemService()
    ctx = _StubCtx()
    items = [
        {"id": "ok", "type": "Feature", "geometry": _valid_polygon(), "properties": {}},
        {"id": "bad", "type": "Feature", "geometry": _degenerate_ring_polygon(), "properties": {}},
    ]
    result = await svc._enforce_es_geometry_poison_check(
        "cat", "col", items, [_pg_resolved(), _es_resolved()],
        ctx=ctx, is_single=False,
    )
    # Only the good item survives.
    assert [it["id"] for it in result] == ["ok"]
    # The poison item is in the rejection channel.
    sink = ctx.extensions["_rejections"]
    assert len(sink) == 1
    rej = sink[0]
    assert rej["external_id"] == "bad"
    assert rej["reason"] == "geometry_poison"
    assert "collapses" in rej["message"] or "empty" in rej["message"].lower()


@pytest.mark.asyncio
async def test_bulk_poison_without_sink_raises():
    """A bulk call with no rejection channel raises ValueError (fail the batch)."""
    from dynastore.modules.catalog.item_service import ItemService

    svc = ItemService()
    ctx = _StubCtx(seed_rejections=False)
    items = [
        {"id": "ok", "type": "Feature", "geometry": _valid_polygon(), "properties": {}},
        {"id": "bad", "type": "Feature", "geometry": _degenerate_ring_polygon(), "properties": {}},
    ]
    with pytest.raises(ValueError):
        await svc._enforce_es_geometry_poison_check(
            "cat", "col", items, [_pg_resolved(), _es_resolved()],
            ctx=ctx, is_single=False,
        )


@pytest.mark.asyncio
async def test_all_items_poison_bulk_returns_empty():
    """When all items in a bulk are poison, the write set is emptied."""
    from dynastore.modules.catalog.item_service import ItemService

    svc = ItemService()
    ctx = _StubCtx()
    items = [
        {"id": "a", "type": "Feature", "geometry": _degenerate_ring_polygon(), "properties": {}},
        {"id": "b", "type": "Feature", "geometry": _geometry_collection_lines_only(), "properties": {}},
    ]
    result = await svc._enforce_es_geometry_poison_check(
        "cat", "col", items, [_pg_resolved(), _es_resolved()],
        ctx=ctx, is_single=False,
    )
    assert result == []
    assert {r["external_id"] for r in ctx.extensions["_rejections"]} == {"a", "b"}


@pytest.mark.asyncio
async def test_mixed_batch_repair_and_reject():
    """Mixed batch: valid + repairable + poison → valid accepted, repaired
    accepted with fixed geometry, poison drained to rejections."""
    from dynastore.modules.catalog.item_service import ItemService

    svc = ItemService()
    ctx = _StubCtx()
    items = [
        {"id": "valid", "type": "Feature", "geometry": _valid_polygon(), "properties": {}},
        {"id": "bowtie", "type": "Feature", "geometry": _bowtie_polygon(), "properties": {}},
        {"id": "degen", "type": "Feature", "geometry": _degenerate_ring_polygon(), "properties": {}},
    ]
    result = await svc._enforce_es_geometry_poison_check(
        "cat", "col", items, [_pg_resolved(), _es_resolved()],
        ctx=ctx, is_single=False,
    )
    result_ids = [it["id"] for it in result]
    assert "valid" in result_ids
    assert "bowtie" in result_ids
    assert "degen" not in result_ids

    # The bow-tie geometry must have been repaired in place.
    bowtie_item = next(it for it in result if it["id"] == "bowtie")
    assert bowtie_item["geometry"]["type"] in ("Polygon", "MultiPolygon")

    # The degenerate item is in the rejection channel.
    sink = ctx.extensions["_rejections"]
    assert len(sink) == 1
    assert sink[0]["external_id"] == "degen"
