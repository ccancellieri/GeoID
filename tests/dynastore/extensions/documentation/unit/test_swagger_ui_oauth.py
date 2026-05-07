#    Copyright 2025 FAO
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

"""Unit tests for ``documentation.configure_swagger_ui()``.

These tests exercise the env-driven OAuth init_oauth dict, the OAuth2
redirect URL attribute, and registration of the ``/docs/oauth2-redirect``
callback route. No DB, no external HTTP — pure FastAPI app construction.
"""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from dynastore.extensions.documentation.service import configure_swagger_ui


def _fresh_app() -> FastAPI:
    return FastAPI()


def test_swagger_init_oauth_clientId_from_env(monkeypatch):
    monkeypatch.setenv("IDP_CLIENT_ID", "geoid-fe")
    monkeypatch.delenv("IDP_AUDIENCE", raising=False)

    app = _fresh_app()
    configure_swagger_ui(app)

    assert app.swagger_ui_init_oauth is not None
    assert app.swagger_ui_init_oauth.get("clientId") == "geoid-fe"


def test_swagger_init_oauth_no_clientId_when_unset(monkeypatch):
    monkeypatch.delenv("IDP_CLIENT_ID", raising=False)
    monkeypatch.delenv("IDP_AUDIENCE", raising=False)

    app = _fresh_app()
    configure_swagger_ui(app)

    # clientId omitted entirely — the Authorize popup will render an empty
    # client_id field, signalling the missing env var to the operator.
    assert app.swagger_ui_init_oauth is not None
    assert "clientId" not in app.swagger_ui_init_oauth


def test_swagger_init_oauth_audience_from_env(monkeypatch):
    monkeypatch.setenv("IDP_CLIENT_ID", "geoid-fe")
    monkeypatch.setenv("IDP_AUDIENCE", "geoid-be")

    app = _fresh_app()
    configure_swagger_ui(app)

    assert app.swagger_ui_init_oauth is not None
    qs = app.swagger_ui_init_oauth.get("additionalQueryStringParams")
    assert qs == {"audience": "geoid-be"}


def test_swagger_init_oauth_no_audience_when_unset(monkeypatch):
    monkeypatch.setenv("IDP_CLIENT_ID", "geoid-fe")
    monkeypatch.delenv("IDP_AUDIENCE", raising=False)

    app = _fresh_app()
    configure_swagger_ui(app)

    assert app.swagger_ui_init_oauth is not None
    assert "additionalQueryStringParams" not in app.swagger_ui_init_oauth


def test_swagger_oauth2_redirect_url_set(monkeypatch):
    monkeypatch.delenv("IDP_CLIENT_ID", raising=False)
    monkeypatch.delenv("IDP_AUDIENCE", raising=False)

    app = _fresh_app()
    configure_swagger_ui(app)

    assert app.swagger_ui_oauth2_redirect_url == "/docs/oauth2-redirect"


def test_oauth2_redirect_route_registered(monkeypatch):
    monkeypatch.delenv("IDP_CLIENT_ID", raising=False)
    monkeypatch.delenv("IDP_AUDIENCE", raising=False)

    app = _fresh_app()
    configure_swagger_ui(app)

    paths = {getattr(r, "path", None) for r in app.router.routes}
    assert "/docs/oauth2-redirect" in paths

    client = TestClient(app)
    resp = client.get("/docs/oauth2-redirect")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")
