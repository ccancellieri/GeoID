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
from unittest.mock import AsyncMock, MagicMock

from fastapi import Request

from dynastore.extensions.moving_features.config import MovingFeaturesPluginConfig
from dynastore.extensions.moving_features.mf_service import (
    OGC_API_MOVING_FEATURES_URIS,
    MovingFeaturesService,
)
from dynastore.models.localization import LocalizedText
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
    resp = await svc.ogc_landing_page_handler(req, language="en")
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
    resp = await svc.ogc_landing_page_handler(req, language="en")
    body = _json.loads(resp.body)
    assert any("conformance" in lnk["rel"] for lnk in body["links"])


@pytest.mark.asyncio
async def test_conformance_returns_uris():
    svc = _build_service()
    req = _make_request()
    conf = await svc.ogc_conformance_handler(req)
    assert set(OGC_API_MOVING_FEATURES_URIS).issubset(set(conf.conformsTo))


# ---------------------------------------------------------------------------
# Protocol compliance
# ---------------------------------------------------------------------------

def test_service_is_moving_features_protocol_instance():
    svc = _build_service()
    assert isinstance(svc, MovingFeaturesProtocol)


# ---------------------------------------------------------------------------
# list_catalogs / list_collections — consolidated onto the shared
# OGCServiceMixin._ogc_list_catalogs/_ogc_list_collections helpers (#2692).
# Pins: the internal id is never exposed (external_id wins), and a ?lang=
# query param is now honoured (previously absent entirely).
# ---------------------------------------------------------------------------

def _wire_mf_service(svc, catalogs_mock):
    async def _get_catalogs_service():
        return catalogs_mock

    async def _get_plugin_config(config_cls, catalog_id=None, collection_id=None):
        return MovingFeaturesPluginConfig()

    svc._get_catalogs_service = _get_catalogs_service
    svc._get_plugin_config = _get_plugin_config


class _Catalog:
    def __init__(self, id, external_id=None, title=None):
        self.id = id
        self.external_id = external_id
        self.title = title


class _Collection:
    def __init__(self, id, external_id=None, title=None):
        self.id = id
        self.external_id = external_id
        self.title = title


@pytest.mark.asyncio
async def test_list_catalogs_normalizes_internal_id_to_external_id():
    svc = _build_service()
    catalogs = AsyncMock()
    catalogs.list_catalogs = AsyncMock(
        return_value=[_Catalog(id="internal-1", external_id="public-cat")]
    )
    _wire_mf_service(svc, catalogs)

    result = await svc.list_catalogs(limit=None, offset=0, language="en")

    assert result["catalogs"] == [{"id": "public-cat", "title": None}]


@pytest.mark.asyncio
async def test_list_catalogs_resolves_lang_param():
    svc = _build_service()
    title = LocalizedText(en="Catalog", fr="Catalogue")
    catalogs = AsyncMock()
    catalogs.list_catalogs = AsyncMock(
        return_value=[_Catalog(id="c1", external_id="c1", title=title)]
    )
    _wire_mf_service(svc, catalogs)

    result = await svc.list_catalogs(limit=None, offset=0, language="fr")

    assert result["catalogs"][0]["title"] == "Catalogue"


@pytest.mark.asyncio
async def test_list_collections_normalizes_internal_id_to_external_id():
    svc = _build_service()
    catalogs = AsyncMock()
    catalogs.list_collections = AsyncMock(
        return_value=[_Collection(id="internal-coll", external_id="public-coll")]
    )
    _wire_mf_service(svc, catalogs)

    result = await svc.list_collections("cat", limit=None, offset=0, language="en")

    assert result["collections"] == [{"id": "public-coll"}]
