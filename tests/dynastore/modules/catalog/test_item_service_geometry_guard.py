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

"""Pre-write 10 MB geometry guard in ``ItemService`` (#1248).

ES is an ASYNC secondary (the PG primary commits before ES ever runs), so a
true "don't keep the primary row" reject for an oversized geometry MUST happen
BEFORE any driver write. When the resolved WRITE routing includes an ES items
secondary whose ``simplify_geometry`` is disabled, an item whose geometry
serializes above 10 MB rejects the whole ingest with HTTP 422 (ValueError)
before anything is written. A PG-only catalog never rejects — PG handles large
geometries fine; the limit is an ES constraint.

These are pure-unit tests: they exercise the guard helper directly with stub
resolved drivers and never build the app graph or touch a DB.
"""
from __future__ import annotations

import math

import pytest
from shapely.geometry import Polygon, mapping

from dynastore.models.driver_context import DriverContext
from dynastore.modules.catalog.item_service import ItemService


def _ring(n: int):
    return [
        [math.cos(2 * math.pi * i / n), math.sin(2 * math.pi * i / n)]
        for i in range(n)
    ] + [[1.0, 0.0]]


def _big_geometry():
    """A Polygon whose GeoJSON serialization exceeds 10 MB."""
    return mapping(Polygon(_ring(300_000)))


def _small_geometry():
    return {"type": "Point", "coordinates": [0.0, 0.0]}


class _StubResolved:
    """Mimics ``ResolvedDriver`` enough for the guard: a ``.driver`` whose
    class name and ``simplify_geometry`` flag drive detection."""

    def __init__(self, driver):
        self.driver = driver


class _StubPgDriver:
    pass


_StubPgDriver.__name__ = "ItemsPostgresqlDriver"


class _StubEsDriver:
    """ES items driver stub exposing a resolvable ``simplify_geometry`` flag.

    Carries the ``is_es_items_driver`` structural marker — the same class attr
    set on ``_ItemsElasticsearchBase`` — so the guard's
    ``_resolved_driver_is_es_items`` detects it without a class-name match.
    """

    is_es_items_driver = True

    def __init__(self, simplify_geometry: bool):
        self._simplify = simplify_geometry

    async def get_driver_config(self, catalog_id, collection_id, *, db_resource=None):
        class _Cfg:
            simplify_geometry = self._simplify
        return _Cfg()


def _es_resolved(simplify_geometry: bool):
    drv = _StubEsDriver(simplify_geometry)
    return _StubResolved(drv)


def _pg_resolved():
    return _StubResolved(_StubPgDriver())


@pytest.mark.asyncio
async def test_rejects_big_geometry_when_es_secondary_simplify_disabled():
    svc = ItemService()
    items = [{"id": "big", "type": "Feature", "geometry": _big_geometry(), "properties": {}}]
    with pytest.raises(ValueError) as exc:
        await svc._enforce_es_geometry_size_limit(
            "cat1", "col1", items, [_pg_resolved(), _es_resolved(simplify_geometry=False)],
        )
    # 422-shaped message — must NOT contain "not found" (would map to 404).
    msg = str(exc.value)
    assert "not found" not in msg.lower()
    assert "10" in msg  # references the 10 MB limit


@pytest.mark.asyncio
async def test_allows_big_geometry_when_simplify_enabled():
    svc = ItemService()
    items = [{"id": "big", "type": "Feature", "geometry": _big_geometry(), "properties": {}}]
    # simplify enabled → ES will shrink, so no pre-write reject.
    await svc._enforce_es_geometry_size_limit(
        "cat1", "col1", items, [_pg_resolved(), _es_resolved(simplify_geometry=True)],
    )


@pytest.mark.asyncio
async def test_no_reject_when_pg_only():
    svc = ItemService()
    items = [{"id": "big", "type": "Feature", "geometry": _big_geometry(), "properties": {}}]
    # No ES secondary routed → PG handles large geometry fine.
    await svc._enforce_es_geometry_size_limit(
        "cat1", "col1", items, [_pg_resolved()],
    )


@pytest.mark.asyncio
async def test_no_reject_for_small_geometry_with_es_secondary():
    svc = ItemService()
    items = [{"id": "ok", "type": "Feature", "geometry": _small_geometry(), "properties": {}}]
    await svc._enforce_es_geometry_size_limit(
        "cat1", "col1", items, [_pg_resolved(), _es_resolved(simplify_geometry=False)],
    )


@pytest.mark.asyncio
async def test_reject_identifies_offending_item_id():
    svc = ItemService()
    items = [
        {"id": "ok", "type": "Feature", "geometry": _small_geometry(), "properties": {}},
        {"id": "toobig", "type": "Feature", "geometry": _big_geometry(), "properties": {}},
    ]
    with pytest.raises(ValueError) as exc:
        await svc._enforce_es_geometry_size_limit(
            "cat1", "col1", items, [_pg_resolved(), _es_resolved(simplify_geometry=False)],
        )
    assert "toobig" in str(exc.value)


class _StubCtx(DriverContext):
    """DriverContext subclass: pre-seeds the ``extensions`` dict with the
    rejection channel the guard drains per-item errors into (the same channel
    ``_ingest_items`` seeds as ``ctx.extensions["_rejections"]``)."""

    def __init__(self, seed_rejections: bool = True):
        super().__init__(
            extensions={"_rejections": []} if seed_rejections else {},
        )


@pytest.mark.asyncio
async def test_bulk_oversized_drained_to_rejections_not_raised():
    """#1260: in a BULK ingest with a rejection channel, an oversized geometry
    is reported per-item via the ``_rejections`` out-list (→ 207) rather than
    raising and killing the whole batch. The good items survive."""
    svc = ItemService()
    ctx = _StubCtx()
    items = [
        {"id": "ok", "type": "Feature", "geometry": _small_geometry(), "properties": {}},
        {"id": "toobig", "type": "Feature", "geometry": _big_geometry(), "properties": {}},
    ]
    remaining = await svc._enforce_es_geometry_size_limit(
        "cat1", "col1", items, [_pg_resolved(), _es_resolved(simplify_geometry=False)],
        ctx=ctx, is_single=False,
    )
    # The good item survives; the oversized one is dropped from the write set.
    remaining_ids = [it["id"] for it in remaining]
    assert remaining_ids == ["ok"]
    # The oversized item is recorded as a per-item rejection.
    sink = ctx.extensions["_rejections"]
    assert len(sink) == 1
    rej = sink[0]
    assert rej["external_id"] == "toobig"
    assert "10" in rej["message"]


@pytest.mark.asyncio
async def test_single_oversized_still_raises_even_with_sink():
    """A single-item ingest must keep its hard 4xx reject (ValueError), even
    when a rejection channel is present."""
    svc = ItemService()
    ctx = _StubCtx()
    items = [{"id": "big", "type": "Feature", "geometry": _big_geometry(), "properties": {}}]
    with pytest.raises(ValueError):
        await svc._enforce_es_geometry_size_limit(
            "cat1", "col1", items, [_es_resolved(simplify_geometry=False)],
            ctx=ctx, is_single=True,
        )
    assert ctx.extensions["_rejections"] == []


@pytest.mark.asyncio
async def test_bulk_oversized_without_sink_still_raises():
    """A bulk call with no rejection channel (direct service use) keeps the
    fail-the-batch behavior — there is nowhere to deliver a per-item report."""
    svc = ItemService()
    ctx = _StubCtx(seed_rejections=False)
    items = [
        {"id": "ok", "type": "Feature", "geometry": _small_geometry(), "properties": {}},
        {"id": "toobig", "type": "Feature", "geometry": _big_geometry(), "properties": {}},
    ]
    with pytest.raises(ValueError):
        await svc._enforce_es_geometry_size_limit(
            "cat1", "col1", items, [_es_resolved(simplify_geometry=False)],
            ctx=ctx, is_single=False,
        )


@pytest.mark.asyncio
async def test_bulk_all_oversized_drains_all_and_returns_empty():
    """When every item in a bulk is oversized, all are reported and the write
    set is emptied (the caller then returns a 207 with no accepted items)."""
    svc = ItemService()
    ctx = _StubCtx()
    items = [
        {"id": "a", "type": "Feature", "geometry": _big_geometry(), "properties": {}},
        {"id": "b", "type": "Feature", "geometry": _big_geometry(), "properties": {}},
    ]
    remaining = await svc._enforce_es_geometry_size_limit(
        "cat1", "col1", items, [_es_resolved(simplify_geometry=False)],
        ctx=ctx, is_single=False,
    )
    assert remaining == []
    assert {r["external_id"] for r in ctx.extensions["_rejections"]} == {"a", "b"}


@pytest.mark.asyncio
async def test_works_with_feature_model_geometry():
    """The guard reads geometry off Pydantic Feature models too."""
    from dynastore.models.ogc import Feature

    svc = ItemService()
    feature = Feature.model_validate({
        "id": "big",
        "type": "Feature",
        "geometry": _big_geometry(),
        "properties": {},
    })
    with pytest.raises(ValueError):
        await svc._enforce_es_geometry_size_limit(
            "cat1", "col1", [feature], [_es_resolved(simplify_geometry=False)],
        )


# ---------------------------------------------------------------------------
# Fail-open tests: missing config / resolver error must NOT reject items
# ---------------------------------------------------------------------------

class _StubESDriver:
    is_es_items_driver = True

    def __init__(self, *, simplify=None, raise_on_resolve=False):
        self._simplify = simplify
        self._raise = raise_on_resolve

    async def _resolve_simplify_geometry(self, catalog_id, collection_id, *, db_resource=None):
        if self._raise:
            raise RuntimeError("configs protocol unavailable")
        return self._simplify


class _Resolved:
    def __init__(self, driver):
        self.driver = driver


@pytest.mark.asyncio
async def test_guard_accepts_when_simplify_explicitly_enabled():
    svc = ItemService()
    item = {"id": "1582", "geometry": _big_geometry()}
    kept = await svc._enforce_es_geometry_size_limit(
        "cat", "coll", [item], [_Resolved(_StubESDriver(simplify=True))],
        is_single=True,
    )
    assert kept == [item]


@pytest.mark.asyncio
async def test_guard_accepts_when_simplify_not_configured():
    """Resolver returns None (not configured) → fail open: guard must accept
    the oversized geometry rather than reject it."""
    svc = ItemService()
    item = {"id": "1582", "geometry": _big_geometry()}
    kept = await svc._enforce_es_geometry_size_limit(
        "cat", "coll", [item], [_Resolved(_StubESDriver(simplify=None))],
        is_single=True,
    )
    assert kept == [item]


@pytest.mark.asyncio
async def test_guard_fails_open_when_resolution_errors():
    svc = ItemService()
    item = {"id": "1582", "geometry": _big_geometry()}
    kept = await svc._enforce_es_geometry_size_limit(
        "cat", "coll", [item], [_Resolved(_StubESDriver(raise_on_resolve=True))],
        is_single=True,
    )
    assert kept == [item]


@pytest.mark.asyncio
async def test_guard_rejects_when_simplify_explicitly_disabled():
    svc = ItemService()
    item = {"id": "1582", "geometry": _big_geometry()}
    with pytest.raises(ValueError) as exc:
        await svc._enforce_es_geometry_size_limit(
            "cat", "coll", [item], [_Resolved(_StubESDriver(simplify=False))],
            is_single=True,
        )
    assert "simplify_geometry" in str(exc.value)
