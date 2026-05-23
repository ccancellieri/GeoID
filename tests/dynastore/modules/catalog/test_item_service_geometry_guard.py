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
