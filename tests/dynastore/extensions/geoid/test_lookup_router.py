"""Unit tests for the geoid extension's geoid-search router (#1210).

The route resolves one item by exactly one of geoid or external_id; the
service layer is mocked here and exercised directly in test_lookup_service.py.
"""
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.fixture
def app(monkeypatch):
    from dynastore.extensions.geoid.lookup_router import router

    fake_lookup_by_geoids = AsyncMock(return_value=[
        {
            "geoid": "g1", "catalog_id": "cat", "collection_id": "col",
            "external_id": "e1", "geometry": None, "bbox": None, "properties": None,
        },
    ])
    fake_lookup_by_external_id = AsyncMock(return_value=[
        {
            "geoid": "g2", "catalog_id": "cat", "collection_id": "col",
            "external_id": "ext-1", "geometry": None, "bbox": None, "properties": None,
        },
    ])
    monkeypatch.setattr(
        "dynastore.extensions.geoid.lookup_router.lookup_by_geoids",
        fake_lookup_by_geoids,
    )
    monkeypatch.setattr(
        "dynastore.extensions.geoid.lookup_router.lookup_by_external_id",
        fake_lookup_by_external_id,
    )
    a = FastAPI()
    a.include_router(router)
    a.state._fake_geoids = fake_lookup_by_geoids
    a.state._fake_external_id = fake_lookup_by_external_id
    return a


def test_items_search_by_geoid(app):
    with TestClient(app) as c:
        r = c.post("/search/catalogs/cat/geoid-search", json={"geoid": "g1"})
    assert r.status_code == 200
    body = r.json()
    assert len(body["results"]) == 1
    assert body["results"][0]["geoid"] == "g1"
    # external_id resolver must not be touched on the geoid branch.
    app.state._fake_external_id.assert_not_awaited()


def test_items_search_external_id_without_collection_id_rejected(app):
    # external_id is not globally unique → a bare external_id (no collection_id)
    # would be a cross-collection scan, disallowed by the public lookup contract
    # (un-fao/GeoID#1204 R2). It must be a 400, and the resolver is never reached.
    with TestClient(app) as c:
        r = c.post("/search/catalogs/cat/geoid-search", json={"external_id": "ext-1"})
    assert r.status_code == 400
    app.state._fake_external_id.assert_not_awaited()


def test_items_search_by_external_id_with_collection(app):
    with TestClient(app) as c:
        r = c.post(
            "/search/catalogs/cat/geoid-search",
            json={"external_id": "ext-1", "collection_id": "col"},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["results"][0]["external_id"] == "ext-1"
    # Resolved within the named collection only (positional call).
    call = app.state._fake_external_id.call_args
    assert call.args == ("cat", "col", "ext-1")


def test_items_search_rejects_both_geoid_and_external_id(app):
    with TestClient(app) as c:
        r = c.post(
            "/search/catalogs/cat/geoid-search",
            json={"geoid": "g1", "external_id": "ext-1"},
        )
    assert r.status_code == 400


def test_items_search_rejects_empty_body(app):
    with TestClient(app) as c:
        r = c.post("/search/catalogs/cat/geoid-search", json={})
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# #975 — deterministic shape for malformed geoid input: 200 + empty collection
# (operator visibility via the WARN log in lookup_service, not the HTTP status).
# Exercises the real lookup_service short-circuit (no mock).
# ---------------------------------------------------------------------------


def test_items_search_with_non_uuid_geoid_returns_200_empty():
    from dynastore.extensions.geoid.lookup_router import router

    a = FastAPI()
    a.include_router(router)
    with TestClient(a) as c:
        r = c.post("/search/catalogs/cat/geoid-search", json={"geoid": "not-a-uuid"})
    assert r.status_code == 200
    body = r.json()
    assert body["results"] == []
    assert body["numberReturned"] == 0
