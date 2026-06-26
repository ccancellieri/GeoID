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

"""Unit tests for extensions/moving_features/mf_service.py.

Tests service instantiation, route wiring, OGCServiceMixin, conformance URIs,
and landing page generation — no database required.
"""

import pytest
from unittest.mock import MagicMock

from fastapi import Request

from dynastore.extensions.moving_features.mf_service import (
    OGC_API_MOVING_FEATURES_URIS,
    MovingFeaturesService,
)
from dynastore.models.protocols import MovingFeaturesProtocol


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _build_service() -> MovingFeaturesService:
    return MovingFeaturesService()


def _make_request(url: str = "http://example.com/movingfeatures/") -> Request:
    req = MagicMock(spec=Request)
    req.url = url
    req.headers = MagicMock()
    req.headers.get = MagicMock(return_value="")
    return req


# ---------------------------------------------------------------------------
# Instantiation & route wiring
# ---------------------------------------------------------------------------

def test_service_instantiation():
    svc = _build_service()
    assert svc.prefix == "/movingfeatures"
    assert svc.router is not None


def test_router_prefix():
    svc = _build_service()
    assert svc.router.prefix == "/movingfeatures"


def test_self_app_stored():
    app_mock = MagicMock()
    svc = MovingFeaturesService(app=app_mock)
    assert svc.app is app_mock


def test_conformance_uris_present():
    svc = _build_service()
    assert "http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/core" in svc.conformance_uris
    assert "http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/mf-collection" in svc.conformance_uris
    assert "http://www.opengis.net/spec/ogcapi-movingfeatures-1/1.0/conf/tgsequence" in svc.conformance_uris


def test_conformance_uris_match_module_constant():
    assert MovingFeaturesService.conformance_uris == OGC_API_MOVING_FEATURES_URIS


def test_routes_registered():
    svc = _build_service()
    paths = {r.path for r in svc.router.routes}
    col = "/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}"

    assert "/movingfeatures/" in paths
    assert "/movingfeatures/conformance" in paths
    assert "/movingfeatures/catalogs/{catalog_id}/collections" in paths
    assert col in paths
    assert col + "/items" in paths
    assert col + "/items/{mf_id}" in paths
    assert col + "/items/{mf_id}/tgsequence" in paths
    assert col + "/items/{mf_id}/tgsequence/{tg_id}" in paths


def test_update_routes_methods():
    """Verify PUT and PATCH methods are registered for update operations."""
    svc = _build_service()
    route_methods = {}
    for route in svc.router.routes:
        if hasattr(route, 'methods'):
            route_methods[route.path] = route.methods
    
    col = "/movingfeatures/catalogs/{catalog_id}/collections/{collection_id}"
    
    # PUT on items/{mf_id}
    assert "PUT" in route_methods.get(col + "/items/{mf_id}", set())
    
    # PATCH on items/{mf_id}/tgsequence/{tg_id}
    assert "PATCH" in route_methods.get(col + "/items/{mf_id}/tgsequence/{tg_id}", set())


# ---------------------------------------------------------------------------
# OGCServiceMixin — landing page
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_landing_page_contains_self_link(monkeypatch):
    import json as _json
    monkeypatch.setattr(
        "dynastore.extensions.ogc_base.get_root_url",
        lambda req: "http://example.com",
    )
    svc = _build_service()
    req = _make_request()
    resp = await svc.get_landing_page(req, language="en")
    body = _json.loads(resp.body)
    hrefs = [lnk["href"] for lnk in body["links"]]
    assert any("self" in lnk["rel"] for lnk in body["links"])
    assert any("/movingfeatures/" in h for h in hrefs)


@pytest.mark.asyncio
async def test_landing_page_contains_conformance_link(monkeypatch):
    import json as _json
    monkeypatch.setattr(
        "dynastore.extensions.ogc_base.get_root_url",
        lambda req: "http://example.com",
    )
    svc = _build_service()
    req = _make_request()
    resp = await svc.get_landing_page(req, language="en")
    body = _json.loads(resp.body)
    assert any("conformance" in lnk["rel"] for lnk in body["links"])


@pytest.mark.asyncio
async def test_conformance_returns_uris():
    svc = _build_service()
    req = _make_request()
    conf = await svc.get_conformance(req)
    assert set(OGC_API_MOVING_FEATURES_URIS).issubset(set(conf.conformsTo))


# ---------------------------------------------------------------------------
# Protocol compliance
# ---------------------------------------------------------------------------

def test_service_is_moving_features_protocol_instance():
    svc = _build_service()
    assert isinstance(svc, MovingFeaturesProtocol)
