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

"""Unit tests for GET /admin/presets/{name}?meta= and GET /admin/presets/{name}/describe.

These tests mount the AdminService router without a DB. They validate:
  - GET /admin/presets/{name}?meta=field injects _meta into the response.
  - GET /admin/presets/{name}?meta=none (default) returns the existing shape.
  - GET /admin/presets/{name}/describe?format=json returns JSON with describe_preset keys.
  - GET /admin/presets/{name}/describe?format=md returns text/markdown.
  - GET /admin/presets/{name}/describe?format=html returns text/html.
  - GET /admin/presets/{name}/describe for an unknown preset returns 404.
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from dynastore.extensions.admin.admin_service import AdminService


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(AdminService.router)
    return app


# Use "public_catalog" as a known registered preset (always registered by import).


class TestGetPresetDetailWithMeta:
    def test_default_meta_none_has_no_meta_key(self) -> None:
        """Default ?meta=none returns the existing shape without _meta."""
        client = TestClient(_app())
        resp = client.get("/admin/presets/public_catalog")
        assert resp.status_code == 200
        body = resp.json()
        assert "_meta" not in body

    def test_meta_field_injects_meta_key(self) -> None:
        """?meta=field adds _meta with docs key (preset with real params)."""
        client = TestClient(_app())
        resp = client.get("/admin/presets/file_backed", params={"meta": "field"})
        assert resp.status_code == 200
        body = resp.json()
        assert "_meta" in body
        assert "docs" in body["_meta"]

    def test_meta_schema_injects_meta_key(self) -> None:
        """?meta=schema adds _meta with json_schema key (preset with real params)."""
        client = TestClient(_app())
        resp = client.get("/admin/presets/file_backed", params={"meta": "schema"})
        assert resp.status_code == 200
        body = resp.json()
        assert "_meta" in body
        assert "json_schema" in body["_meta"]

    def test_meta_field_omitted_for_no_params_preset(self) -> None:
        """A no-params preset gets no _meta envelope even under ?meta=field."""
        client = TestClient(_app())
        resp = client.get("/admin/presets/public_catalog", params={"meta": "field"})
        assert resp.status_code == 200
        assert "_meta" not in resp.json()

    def test_unknown_preset_returns_404(self) -> None:
        client = TestClient(_app())
        resp = client.get("/admin/presets/does_not_exist_xyz", params={"meta": "field"})
        assert resp.status_code == 404

    def test_existing_fields_still_present_with_meta(self) -> None:
        """Adding ?meta= must not remove existing fields."""
        client = TestClient(_app())
        resp = client.get("/admin/presets/public_catalog", params={"meta": "field"})
        body = resp.json()
        assert "name" in body
        assert "description" in body
        assert "tier" in body


class TestPresetDescribeEndpoint:
    def test_describe_json_format_returns_200(self) -> None:
        client = TestClient(_app())
        resp = client.get("/admin/presets/public_catalog/describe", params={"format": "json"})
        assert resp.status_code == 200
        assert "application/json" in resp.headers["content-type"]

    def test_describe_json_has_required_keys(self) -> None:
        client = TestClient(_app())
        resp = client.get("/admin/presets/public_catalog/describe", params={"format": "json"})
        body = resp.json()
        assert "name" in body
        assert "description" in body
        assert "examples" in body
        assert "tier" in body

    def test_describe_md_format_returns_text_markdown(self) -> None:
        client = TestClient(_app())
        resp = client.get("/admin/presets/public_catalog/describe", params={"format": "md"})
        assert resp.status_code == 200
        assert "text/markdown" in resp.headers["content-type"]
        assert "public_catalog" in resp.text

    def test_describe_html_format_returns_text_html(self) -> None:
        client = TestClient(_app())
        resp = client.get("/admin/presets/public_catalog/describe", params={"format": "html"})
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_describe_unknown_preset_returns_404(self) -> None:
        client = TestClient(_app())
        resp = client.get("/admin/presets/no_such_preset_xyz/describe")
        assert resp.status_code == 404
        assert "no_such_preset_xyz" in resp.json()["detail"]

    def test_describe_default_format_is_json(self) -> None:
        """Omitting format= defaults to JSON."""
        client = TestClient(_app())
        resp = client.get("/admin/presets/public_catalog/describe")
        assert resp.status_code == 200
        assert "application/json" in resp.headers["content-type"]

    def test_stac_preset_describe_has_params_schema(self) -> None:
        """stac preset has params — params_schema must be non-null in the descriptor."""
        client = TestClient(_app())
        resp = client.get("/admin/presets/stac/describe")
        assert resp.status_code == 200
        body = resp.json()
        assert body["params_schema"] is not None
