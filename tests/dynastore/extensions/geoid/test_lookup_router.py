"""Unit tests for the geoid extension's lookup router (mocks lookup_service)."""
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
    return a


def test_get_geoid_scoped(app):
    with TestClient(app) as c:
        r = c.get("/search/catalogs/cat/geoid/g1")
    assert r.status_code == 200
    body = r.json()
    assert len(body["results"]) == 1
    assert body["results"][0]["geoid"] == "g1"


def test_post_geoid_scoped_with_geoids_list(app):
    with TestClient(app) as c:
        r = c.post(
            "/search/catalogs/cat/geoid",
            json={"geoids": ["g1"], "limit": 10},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["results"][0]["geoid"] == "g1"


def test_post_geoid_scoped_with_external_id_pair(app):
    with TestClient(app) as c:
        r = c.post(
            "/search/catalogs/cat/geoid",
            json={"external_id": "ext-1", "collection_id": "col", "limit": 1},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["results"][0]["external_id"] == "ext-1"


def test_post_geoid_scoped_rejects_external_id_without_collection(app):
    with TestClient(app) as c:
        r = c.post(
            "/search/catalogs/cat/geoid",
            json={"external_id": "ext-1"},
        )
    assert r.status_code == 400


def test_post_geoid_scoped_rejects_empty_body(app):
    with TestClient(app) as c:
        r = c.post("/search/catalogs/cat/geoid", json={})
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# #975 — route-level pin: deterministic shape for malformed UUID inputs.
# The contract chosen is 200 + empty GeoidCollection (symmetric with the batch
# endpoint where a 400 would punish the whole batch for one bad element).
# Operator visibility comes from the WARN log in lookup_service, not the HTTP
# response. Exercises the real lookup_service short-circuit (no mock).
# ---------------------------------------------------------------------------


def test_get_geoid_scoped_with_non_uuid_returns_200_empty():
    """GET /search/catalogs/{cat}/geoid/{not-a-uuid} → deterministic 200 + empty."""
    from dynastore.extensions.geoid.lookup_router import router

    a = FastAPI()
    a.include_router(router)
    with TestClient(a) as c:
        r = c.get("/search/catalogs/cat/geoid/not-a-uuid")
    assert r.status_code == 200
    body = r.json()
    assert body["results"] == []
    assert body["numberReturned"] == 0


def test_post_geoid_scoped_with_all_non_uuid_geoids_returns_200_empty():
    """POST with body.geoids that are all malformed → 200 + empty (not 400)."""
    from dynastore.extensions.geoid.lookup_router import router

    a = FastAPI()
    a.include_router(router)
    with TestClient(a) as c:
        r = c.post(
            "/search/catalogs/cat/geoid",
            json={"geoids": ["not-a-uuid", "still-bogus"], "limit": 10},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["results"] == []
    assert body["numberReturned"] == 0
