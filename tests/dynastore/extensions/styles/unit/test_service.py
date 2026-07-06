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

"""Unit tests for extensions/styles/styles_service.py.

Tests the StylesService class-level behaviour that does not require a running
database: route registration, OGCServiceMixin wiring, conformance URIs,
landing page generation, and the internal stylesheet helpers.
"""

import json
import pytest
from unittest.mock import MagicMock

from fastapi import Request

from dynastore.extensions.styles.styles_service import (
    OGC_API_STYLES_URIS,
    StylesService,
    _pick_stylesheet_by_media_type,
    _stylesheet_to_bytes,
)
from dynastore.modules.styles.encodings import MEDIA_TYPE_MAPBOX_GL, MEDIA_TYPE_SLD_11
from dynastore.modules.styles.models import (
    Link,
    MapboxContent,
    SLDContent,
    StyleFormatEnum,
    StyleSheet,
    Style,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _build_service() -> StylesService:
    return StylesService()


def _make_request(url: str = "http://example.com/styles/") -> Request:
    req = MagicMock(spec=Request)
    req.url = url
    req.headers = MagicMock()
    req.headers.get = MagicMock(return_value="")
    return req


def _mapbox_sheet() -> StyleSheet:
    content = MapboxContent(
        format=StyleFormatEnum.MAPBOX_GL,
        version=8,
        sources={"s": {"type": "geojson", "data": {}}},
        layers=[],
    )
    return StyleSheet(content=content, link=Link(href="/stylesheet", rel="stylesheet"))


def _sld_sheet() -> StyleSheet:
    sld_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<StyledLayerDescriptor version="1.1.0" xmlns="http://www.opengis.net/sld"/>'
    )
    content = SLDContent(sld_body=sld_xml)
    return StyleSheet(content=content, link=Link(href="/stylesheet", rel="stylesheet"))


# ---------------------------------------------------------------------------
# Service instantiation and route wiring
# ---------------------------------------------------------------------------


def test_service_instantiation():
    svc = _build_service()
    assert svc.prefix == "/styles"
    assert svc.router is not None


def test_router_prefix():
    svc = _build_service()
    assert svc.router.prefix == "/styles"


def test_self_app_stored():
    app_mock = MagicMock()
    svc = StylesService(app=app_mock)
    assert svc.app is app_mock


def test_conformance_uris_present():
    svc = _build_service()
    assert "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/core" in svc.conformance_uris
    assert (
        "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/manage-styles"
        in svc.conformance_uris
    )


def test_conformance_uris_match_module_constant():
    assert StylesService.conformance_uris == OGC_API_STYLES_URIS


def test_routes_registered():
    svc = _build_service()
    # With instance router (Pattern B), paths include the /styles prefix.
    paths = {r.path for r in svc.router.routes}
    col = "/styles/catalogs/{catalog_id}/collections/{collection_id}/styles"
    # Core CRUD
    assert col in paths
    assert col + "/{style_id}" in paths
    # Sub-resources
    assert col + "/{style_id}/stylesheet" in paths
    assert col + "/{style_id}/metadata" in paths
    assert col + "/{style_id}/legend" in paths
    # OGC landing page + conformance
    assert "/styles/" in paths
    assert "/styles/conformance" in paths
    # Cross-catalog discovery
    assert "/styles/all" in paths


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
    assert any("/styles/" in h for h in hrefs)


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
async def test_conformance_returns_uris(monkeypatch):
    svc = _build_service()
    req = _make_request()
    conf = await svc.ogc_conformance_handler(req)
    assert set(OGC_API_STYLES_URIS).issubset(set(conf.conformsTo))


# ---------------------------------------------------------------------------
# _pick_stylesheet_by_media_type
# ---------------------------------------------------------------------------


def test_pick_mapbox_sheet():
    mapbox = _mapbox_sheet()
    sld = _sld_sheet()
    style = MagicMock(spec=Style)
    style.stylesheets = [mapbox, sld]
    result = _pick_stylesheet_by_media_type(style, MEDIA_TYPE_MAPBOX_GL)
    assert result is mapbox


def test_pick_sld_sheet():
    mapbox = _mapbox_sheet()
    sld = _sld_sheet()
    style = MagicMock(spec=Style)
    style.stylesheets = [mapbox, sld]
    result = _pick_stylesheet_by_media_type(style, MEDIA_TYPE_SLD_11)
    assert result is sld


def test_pick_missing_encoding_returns_none():
    style = MagicMock(spec=Style)
    style.stylesheets = [_mapbox_sheet()]
    result = _pick_stylesheet_by_media_type(style, MEDIA_TYPE_SLD_11)
    assert result is None


# ---------------------------------------------------------------------------
# _stylesheet_to_bytes
# ---------------------------------------------------------------------------


def test_stylesheet_to_bytes_mapbox():
    sheet = _mapbox_sheet()
    raw = _stylesheet_to_bytes(sheet)
    parsed = json.loads(raw)
    assert parsed["version"] == 8
    assert "format" in parsed or "sources" in parsed


def test_stylesheet_to_bytes_sld():
    sheet = _sld_sheet()
    raw = _stylesheet_to_bytes(sheet)
    assert b"StyledLayerDescriptor" in raw
    assert isinstance(raw, bytes)


# ---------------------------------------------------------------------------
# StylesProtocol structural compliance
# ---------------------------------------------------------------------------


def test_service_is_styles_protocol_instance():
    from dynastore.models.protocols import StylesProtocol

    svc = _build_service()
    assert isinstance(svc, StylesProtocol)


# ---------------------------------------------------------------------------
# get_style_metadata — Link serialization (regression for #2191)
# ---------------------------------------------------------------------------


def _make_style_with_links(style_id: str = "dark") -> "Style":
    """Build a minimal Style with a self Link and one Mapbox stylesheet."""
    from dynastore.modules.styles.models import Style, StyleSheet, MapboxContent, StyleFormatEnum

    return Style(
        id="00000000-0000-0000-0000-000000000099",
        catalog_id="cat1",
        collection_id="col1",
        style_id=style_id,
        title="Dark style",
        description=None,
        keywords=None,
        created_at="2024-01-01T00:00:00Z",
        updated_at="2024-01-01T00:00:00Z",
        stylesheets=[
            StyleSheet(
                content=MapboxContent(
                    format=StyleFormatEnum.MAPBOX_GL,
                    version=8,
                    sources={},
                    layers=[],
                ),
                link=Link(href="http://example.com/styles/catalogs/cat1/collections/col1/styles/dark/stylesheet", rel="stylesheet"),
            )
        ],
        links=[Link(href="http://example.com/styles/catalogs/cat1/collections/col1/styles/dark", rel="self", type="application/json")],
    )


@pytest.mark.asyncio
async def test_get_style_metadata_returns_200_with_serialized_links(monkeypatch):
    """Regression: get_style_metadata must not raise TypeError on Link serialization.

    Before the fix, style.links contained Link model instances that JSONResponse
    could not serialize with stdlib json.dumps, causing a 500 for every call.
    """
    import json as _json
    from unittest.mock import AsyncMock
    import dynastore.modules.styles.db as _styles_db
    import dynastore.extensions.styles.styles_service as _svc_mod

    style = _make_style_with_links("dark")

    monkeypatch.setattr(_styles_db, "get_style_by_id_and_collection", AsyncMock(return_value=style))
    # Patch in the service module's namespace (imported with `from ... import get_root_url`)
    monkeypatch.setattr(_svc_mod, "get_root_url", lambda req: "http://example.com")

    svc = _build_service()
    # Inject a stub catalogs protocol so _resolve_internal_catalog_id and
    # _resolve_internal_collection_id can resolve without a live registry.
    # "c_cat1"/"c_col1" are arbitrary internal ids.
    mock_catalogs = MagicMock()
    mock_catalogs.resolve_catalog_id = AsyncMock(return_value="c_cat1")
    mock_catalogs.collections.resolve_collection_id = AsyncMock(return_value="c_col1")
    svc._ogc_catalogs_protocol = mock_catalogs

    req = _make_request("http://example.com/styles/catalogs/cat1/collections/col1/styles/dark/metadata")
    conn = AsyncMock()

    resp = await svc.get_style_metadata(
        request=req,
        catalog_id="cat1",
        collection_id="col1",
        style_id="dark",
        conn=conn,
    )

    assert resp.status_code == 200

    body = _json.loads(resp.body)

    assert body["id"] == "dark"
    assert body["scope"] == "style"
    assert isinstance(body["links"], list), "links must be a JSON-serializable list"
    assert len(body["links"]) >= 1, "at least the self link must be present"

    for lnk in body["links"]:
        assert isinstance(lnk, dict), f"each link must be a plain dict, got {type(lnk)}"
        assert "href" in lnk, "each link dict must have an href key"
        assert "rel" in lnk, "each link dict must have a rel key"

    self_links = [lnk for lnk in body["links"] if lnk.get("rel") == "self"]
    assert self_links, "at least one self link must be present"
    assert self_links[0]["href"] == (
        "http://example.com/styles/catalogs/cat1/collections/col1/styles/dark"
    )


@pytest.mark.asyncio
async def test_get_style_metadata_includes_stylesheet_links(monkeypatch):
    """Stylesheet links (plain dicts) must appear alongside serialized self link."""
    import json as _json
    from unittest.mock import AsyncMock
    import dynastore.modules.styles.db as _styles_db
    import dynastore.extensions.styles.styles_service as _svc_mod

    style = _make_style_with_links("blue")

    monkeypatch.setattr(_styles_db, "get_style_by_id_and_collection", AsyncMock(return_value=style))
    monkeypatch.setattr(_svc_mod, "get_root_url", lambda req: "http://example.com")

    svc = _build_service()
    mock_catalogs = MagicMock()
    mock_catalogs.resolve_catalog_id = AsyncMock(return_value="c_cat1")
    mock_catalogs.collections.resolve_collection_id = AsyncMock(return_value="c_col1")
    svc._ogc_catalogs_protocol = mock_catalogs

    req = _make_request("http://example.com/styles/catalogs/cat1/collections/col1/styles/blue/metadata")
    conn = AsyncMock()

    resp = await svc.get_style_metadata(
        request=req,
        catalog_id="cat1",
        collection_id="col1",
        style_id="blue",
        conn=conn,
    )

    body = _json.loads(resp.body)
    stylesheet_links = [lnk for lnk in body["links"] if lnk.get("rel") == "stylesheet"]
    assert stylesheet_links, "at least one stylesheet link must appear in metadata"
    assert all(isinstance(lnk, dict) for lnk in stylesheet_links)


# ---------------------------------------------------------------------------
# Regression: styles keyed by internal collection id (#2952)
# ---------------------------------------------------------------------------


def _make_style_create(style_id: str = "demo") -> "object":
    from dynastore.modules.styles.models import StyleSheetCreate, StyleCreate

    content = MapboxContent(
        format=StyleFormatEnum.MAPBOX_GL,
        version=8,
        sources={"s": {"type": "geojson", "data": {}}},
        layers=[],
    )
    return StyleCreate(style_id=style_id, stylesheets=[StyleSheetCreate(content=content)])


def _stub_catalogs(internal_catalog_id: str, internal_collection_id: str):
    from unittest.mock import AsyncMock

    catalogs = MagicMock()
    catalogs.resolve_catalog_id = AsyncMock(return_value=internal_catalog_id)
    catalogs.collections.resolve_collection_id = AsyncMock(return_value=internal_collection_id)
    return catalogs


@pytest.mark.asyncio
async def test_create_style_keys_storage_by_internal_collection_id(monkeypatch):
    """Regression for #2952: create_style_for_collection must resolve the
    external collection id to its internal id before calling styles_db —
    the same internal id the styled-map render path resolves to. Before the
    fix this call site passed the raw external collection_id straight
    through, so a style created via the external id was never found by the
    render route (which looks up by internal id).
    """
    from unittest.mock import AsyncMock
    import dynastore.extensions.styles.styles_service as _svc_mod
    import dynastore.modules.styles.db as _styles_db

    svc = _build_service()
    svc._ogc_catalogs_protocol = _stub_catalogs("c_cat1", "c_col1")

    monkeypatch.setattr(
        _svc_mod.catalog_module, "get_collection", AsyncMock(return_value={"id": "col1"})
    )
    monkeypatch.setattr(
        "dynastore.modules.db_config.partition_tools.ensure_partitions_off_request_connection",
        AsyncMock(return_value=None),
    )

    captured = {}

    async def fake_create_style(
        conn, catalog_id, collection_id, style, external_catalog_id=None, external_collection_id=None
    ):
        captured["catalog_id"] = catalog_id
        captured["collection_id"] = collection_id
        return _make_style_with_links(style.style_id)

    monkeypatch.setattr(_styles_db, "create_style", fake_create_style)

    conn = AsyncMock()
    engine = MagicMock()

    await svc.create_style_for_collection(
        catalog_id="cat1",
        collection_id="col1",
        style=_make_style_create("demo"),
        conn=conn,
        engine=engine,
    )

    assert captured["catalog_id"] == "c_cat1"
    assert captured["collection_id"] == "c_col1", (
        "create_style must be called with the internal collection id, "
        "not the raw external path parameter"
    )


@pytest.mark.asyncio
async def test_get_style_looks_up_by_internal_collection_id(monkeypatch):
    """The read side (get_style) must resolve to the same internal
    collection id used at write time (see
    test_create_style_keys_storage_by_internal_collection_id), matching the
    styled-map render route's own resolution.
    """
    from unittest.mock import AsyncMock
    import dynastore.modules.styles.db as _styles_db

    svc = _build_service()
    svc._ogc_catalogs_protocol = _stub_catalogs("c_cat1", "c_col1")

    captured = {}

    async def fake_get_style(
        conn, catalog_id, collection_id, style_id, external_catalog_id=None, external_collection_id=None
    ):
        captured["catalog_id"] = catalog_id
        captured["collection_id"] = collection_id
        return _make_style_with_links(style_id)

    monkeypatch.setattr(_styles_db, "get_style_by_id_and_collection", fake_get_style)

    conn = AsyncMock()
    style = await svc.get_style(
        catalog_id="cat1", collection_id="col1", style_id="demo", conn=conn
    )

    assert style is not None
    assert captured["catalog_id"] == "c_cat1"
    assert captured["collection_id"] == "c_col1"
