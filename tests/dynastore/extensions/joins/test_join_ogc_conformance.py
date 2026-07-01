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

"""End-to-end (TestClient) coverage for the OGC conformance polish on
``POST /join`` (issue #2588): the route mounts, its OpenAPI schema documents
`limit`/`offset` and a response schema, and `Accept`-based content negotiation
actually changes the wire `Content-Type` through the full FastAPI response
pipeline (not just the in-process dict the unit tests in
``test_endpoints.py`` exercise).
"""

from __future__ import annotations

from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from dynastore.extensions.joins.joins_service import JoinsService
from dynastore.models.ogc import Feature
from dynastore.modules.joins.models import BigQuerySecondarySpec, JoinRequest, JoinSpec
from dynastore.modules.storage.drivers.bigquery_models import BigQueryTarget


def _build_app(monkeypatch) -> FastAPI:
    import dynastore.extensions.joins.joins_service as svc_mod

    async def fake_bq_stream(spec, *, secondary_column, **kwargs):
        yield Feature(type="Feature", id="bq1", geometry=None,
                      properties={"user_id": "alice", "score": 42})

    class _FakePrimaryDriver:
        async def read_entities(self, *args, **kwargs):
            yield Feature(type="Feature", id="p1", geometry=None,
                          properties={"uid": "alice", "name": "Alice"})

    fake_resolved = type("R", (), {"driver": _FakePrimaryDriver()})()
    monkeypatch.setattr(svc_mod, "stream_bigquery_secondary", fake_bq_stream)
    monkeypatch.setattr(svc_mod, "resolve_drivers", AsyncMock(return_value=[fake_resolved]))

    app = FastAPI()
    svc = JoinsService(app)
    app.include_router(svc.router)
    return app


def _body() -> dict:
    req = JoinRequest(
        secondary=BigQuerySecondarySpec(
            target=BigQueryTarget(project_id="p", dataset_id="d", table_name="t"),
        ),
        join=JoinSpec(primary_column="uid", secondary_column="user_id"),
    )
    return req.model_dump(mode="json")


def test_openapi_documents_limit_offset_and_response_schema(monkeypatch):
    app = _build_app(monkeypatch)
    schema = app.openapi()
    post_op = schema["paths"]["/join/catalogs/{catalog_id}/collections/{collection_id}/join"]["post"]

    param_names = {p["name"] for p in post_op.get("parameters", [])}
    assert "limit" in param_names
    assert "offset" in param_names

    responses = post_op["responses"]["200"]
    assert "application/geo+json" in responses["content"]
    assert "application/json" in responses["content"]


def test_execute_join_default_serves_geojson_content_type(monkeypatch):
    app = _build_app(monkeypatch)
    client = TestClient(app)

    resp = client.post(
        "/join/catalogs/c/collections/l/join", json=_body(),
    )
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/geo+json")
    payload = resp.json()
    assert payload["type"] == "FeatureCollection"
    assert len(payload["features"]) == 1


def test_execute_join_accept_json_serves_plain_json_content_type(monkeypatch):
    app = _build_app(monkeypatch)
    client = TestClient(app)

    resp = client.post(
        "/join/catalogs/c/collections/l/join",
        json=_body(),
        headers={"Accept": "application/json"},
    )
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/json")
    assert "geo+json" not in resp.headers["content-type"]
    payload = resp.json()
    assert payload["type"] == "FeatureCollection"
