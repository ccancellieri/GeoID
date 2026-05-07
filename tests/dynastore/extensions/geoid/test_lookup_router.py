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
