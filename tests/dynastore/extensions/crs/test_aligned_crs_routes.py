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

"""Unit tests for the aligned CRS routes added to CRSExtension.

Covers:
- list_crs_endpoint: happy path, unknown catalog → 404.
- create_crs_endpoint: happy path, unknown catalog → 404.
- get_crs_by_name_endpoint: found vs not-found → 404.
- delete_crs_endpoint: found vs not-found → 404.
- Route registration: aligned catalog paths only (no deprecated bare-catalog paths),
  plus a platform-scope resolver registered LAST so its greedy path converter does
  not shadow the catalog routes.
- Platform resolver: _resolve_global_crs (pyproj-backed) + content negotiation.

All DB and CatalogsProtocol calls are stubbed; no real I/O.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI, HTTPException

# ---------------------------------------------------------------------------
# pyproj stub (SCOPE gate in crs_extension.py pulls pyproj at import time)
# ---------------------------------------------------------------------------
import sys
import types

if "pyproj" not in sys.modules:
    _fake_pyproj = types.ModuleType("pyproj")
    sys.modules["pyproj"] = _fake_pyproj


from sqlalchemy.ext.asyncio import AsyncConnection  # noqa: E402

from dynastore.extensions.crs.crs_extension import CRSExtension  # noqa: E402
import dynastore.extensions.crs.crs_extension as _crs_mod  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ext() -> CRSExtension:
    app = MagicMock(spec=FastAPI)
    return CRSExtension(app)


def _conn_stub() -> MagicMock:
    """Return a MagicMock that satisfies DriverContext's AsyncConnection type check."""
    return MagicMock(spec=AsyncConnection)


def _mock_catalogs_proto(catalog_found: bool = True) -> MagicMock:
    svc = MagicMock()
    svc.get_catalog = AsyncMock(return_value=MagicMock() if catalog_found else None)
    return svc


def _mock_crs_proto(crs_obj=None) -> MagicMock:
    svc = MagicMock()
    svc.list_crs = AsyncMock(return_value=[])
    svc.search_crs = AsyncMock(return_value=[])
    svc.create_crs = AsyncMock(return_value=crs_obj or MagicMock())
    svc.update_crs = AsyncMock(return_value=crs_obj or MagicMock())
    svc.get_crs_by_name = AsyncMock(return_value=crs_obj)
    svc.get_crs_by_uri = AsyncMock(return_value=crs_obj)
    svc.delete_crs = AsyncMock(return_value=True)
    return svc


def _patch_protocols(monkeypatch, *, catalog_found: bool = True, crs_obj=None) -> None:
    """Patch get_protocol in the crs_extension module for both protocols."""
    from dynastore.models.protocols import CatalogsProtocol
    from dynastore.models.protocols.crs import CRSProtocol

    catalogs_svc = _mock_catalogs_proto(catalog_found=catalog_found)
    crs_svc = _mock_crs_proto(crs_obj=crs_obj)

    def _get_protocol(proto):
        if proto is CatalogsProtocol:
            return catalogs_svc
        if proto is CRSProtocol:
            return crs_svc
        return None

    monkeypatch.setattr(_crs_mod, "get_protocol", _get_protocol)


# ---------------------------------------------------------------------------
# Route registration — aligned paths must be in the router
# ---------------------------------------------------------------------------


class TestRouteRegistration:
    # The router has prefix="/crs", so all paths include that prefix.

    def test_aligned_list_route_registered(self):
        ext = _make_ext()
        paths = [r.path for r in ext.router.routes]
        assert "/crs/catalogs/{catalog_id}" in paths

    def test_aligned_search_route_registered(self):
        ext = _make_ext()
        paths = [r.path for r in ext.router.routes]
        assert "/crs/catalogs/{catalog_id}/search" in paths

    def test_aligned_by_name_route_registered(self):
        ext = _make_ext()
        paths = [r.path for r in ext.router.routes]
        assert "/crs/catalogs/{catalog_id}/by-name/{crs_name}" in paths

    def test_aligned_uri_route_registered(self):
        ext = _make_ext()
        paths = [r.path for r in ext.router.routes]
        # The path_regex captures crs_uri as a path parameter
        assert "/crs/catalogs/{catalog_id}/{crs_uri}" in paths or any(
            "/crs/catalogs/{catalog_id}/" in p for p in paths
        )

    def test_no_deprecated_bare_catalog_route(self):
        """Clean replacement: the old bare /crs/{catalog_id} route must be gone."""
        ext = _make_ext()
        paths = [r.path for r in ext.router.routes]
        assert "/crs/{catalog_id}" not in paths

    def test_platform_resolver_route_registered(self):
        """Platform scope: /crs/{crs_uri} resolves global authority CRS."""
        ext = _make_ext()
        paths = [r.path for r in ext.router.routes]
        assert "/crs/{crs_uri:path}" in paths

    def test_platform_route_registered_after_catalog_routes(self):
        """The greedy {crs_uri:path} converter must be registered LAST so the
        catalog-scoped routes are matched first by registration order."""
        ext = _make_ext()
        paths = [r.path for r in ext.router.routes if hasattr(r, "path")]
        platform_idx = paths.index("/crs/{crs_uri:path}")
        catalog_idxs = [i for i, p in enumerate(paths) if p.startswith("/crs/catalogs/")]
        assert catalog_idxs, "expected catalog-scoped routes to be registered"
        assert platform_idx > max(catalog_idxs), (
            "platform resolver must be registered after all catalog routes"
        )


# ---------------------------------------------------------------------------
# list_crs_endpoint
# ---------------------------------------------------------------------------


class TestListCrsEndpoint:
    @pytest.mark.asyncio
    async def test_happy_path_returns_list(self, monkeypatch):
        _patch_protocols(monkeypatch, catalog_found=True)
        ext = _make_ext()

        result = await ext.list_crs_endpoint(
            catalog_id="my-cat",
            conn=_conn_stub(),
            limit=20,
            offset=0,
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_unknown_catalog_raises_404(self, monkeypatch):
        _patch_protocols(monkeypatch, catalog_found=False)
        ext = _make_ext()

        with pytest.raises(HTTPException) as exc_info:
            await ext.list_crs_endpoint(
                catalog_id="unknown-cat",
                conn=_conn_stub(),
                limit=20,
                offset=0,
            )

        assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# create_crs_endpoint
# ---------------------------------------------------------------------------


class TestCreateCrsEndpoint:
    @pytest.mark.asyncio
    async def test_happy_path_returns_crs(self, monkeypatch):
        fake_crs = MagicMock(name="crs")
        _patch_protocols(monkeypatch, catalog_found=True, crs_obj=fake_crs)
        ext = _make_ext()

        from dynastore.modules.crs.models import CRSCreate
        crs_data = MagicMock(spec=CRSCreate)
        crs_data.crs_uri = "urn:ogc:def:crs:EPSG::32632"

        result = await ext.create_crs_endpoint(
            catalog_id="my-cat",
            crs_data=crs_data,
            conn=_conn_stub(),
        )
        assert result is fake_crs

    @pytest.mark.asyncio
    async def test_unknown_catalog_raises_404(self, monkeypatch):
        _patch_protocols(monkeypatch, catalog_found=False)
        ext = _make_ext()

        from dynastore.modules.crs.models import CRSCreate
        crs_data = MagicMock(spec=CRSCreate)
        crs_data.crs_uri = "urn:ogc:def:crs:EPSG::4326"

        with pytest.raises(HTTPException) as exc_info:
            await ext.create_crs_endpoint(
                catalog_id="unknown-cat",
                crs_data=crs_data,
                conn=_conn_stub(),
            )

        assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# get_crs_by_name_endpoint
# ---------------------------------------------------------------------------


class TestGetCrsByNameEndpoint:
    @pytest.mark.asyncio
    async def test_found_returns_crs(self, monkeypatch):
        fake_crs = MagicMock(name="crs")
        _patch_protocols(monkeypatch, crs_obj=fake_crs)
        ext = _make_ext()

        result = await ext.get_crs_by_name_endpoint(
            catalog_id="my-cat",
            crs_name="WGS84",
            conn=_conn_stub(),
        )
        assert result is fake_crs

    @pytest.mark.asyncio
    async def test_not_found_raises_404(self, monkeypatch):
        _patch_protocols(monkeypatch, crs_obj=None)
        ext = _make_ext()

        with pytest.raises(HTTPException) as exc_info:
            await ext.get_crs_by_name_endpoint(
                catalog_id="my-cat",
                crs_name="UnknownCRS",
                conn=_conn_stub(),
            )

        assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# delete_crs_endpoint
# ---------------------------------------------------------------------------


class TestDeleteCrsEndpoint:
    @pytest.mark.asyncio
    async def test_happy_path_returns_204(self, monkeypatch):
        _patch_protocols(monkeypatch, catalog_found=True)
        ext = _make_ext()

        from fastapi import Response
        result = await ext.delete_crs_endpoint(
            catalog_id="my-cat",
            crs_uri="urn:ogc:def:crs:EPSG::4326",
            conn=_conn_stub(),
        )
        # Returns a 204 Response
        assert isinstance(result, Response)
        assert result.status_code == 204

    @pytest.mark.asyncio
    async def test_not_found_raises_404(self, monkeypatch):
        from dynastore.models.protocols.crs import CRSProtocol
        from dynastore.models.protocols import CatalogsProtocol

        catalogs_svc = _mock_catalogs_proto(catalog_found=True)
        crs_svc = _mock_crs_proto(crs_obj=None)
        crs_svc.delete_crs = AsyncMock(return_value=False)

        def _get_protocol(proto):
            if proto is CatalogsProtocol:
                return catalogs_svc
            if proto is CRSProtocol:
                return crs_svc
            return None

        monkeypatch.setattr(_crs_mod, "get_protocol", _get_protocol)
        ext = _make_ext()

        with pytest.raises(HTTPException) as exc_info:
            await ext.delete_crs_endpoint(
                catalog_id="my-cat",
                crs_uri="urn:ogc:def:crs:EPSG::99999",
                conn=_conn_stub(),
            )

        assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# Platform resolver — _resolve_global_crs + resolve_global_crs_endpoint
# ---------------------------------------------------------------------------


def _install_fake_pyproj(monkeypatch, *, from_user_input):
    """Install a controllable fake pyproj so the in-function imports resolve."""
    fake_exc = types.ModuleType("pyproj.exceptions")

    class _CRSError(Exception):
        pass

    fake_exc.CRSError = _CRSError  # type: ignore[attr-defined]

    fake_pyproj = types.ModuleType("pyproj")
    fake_crs_cls = MagicMock()
    fake_crs_cls.from_user_input = from_user_input
    fake_pyproj.CRS = fake_crs_cls  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "pyproj", fake_pyproj)
    monkeypatch.setitem(sys.modules, "pyproj.exceptions", fake_exc)
    return _CRSError


class TestResolveGlobalCrs:
    def test_resolves_authority_code_to_wkt2(self, monkeypatch):
        fake_obj = MagicMock()
        fake_obj.to_wkt.return_value = 'GEOGCRS["WGS 84", ...]'
        _install_fake_pyproj(monkeypatch, from_user_input=MagicMock(return_value=fake_obj))

        result = _crs_mod._resolve_global_crs("EPSG:4326")

        assert result.definition_type.value == "WKT2"
        assert "WGS 84" in result.definition
        fake_obj.to_wkt.assert_called_once_with(version="WKT2_2019")

    def test_unknown_crs_raises_404(self, monkeypatch):
        err_cls = _install_fake_pyproj(monkeypatch, from_user_input=MagicMock())
        sys.modules["pyproj"].CRS.from_user_input.side_effect = err_cls("nope")

        with pytest.raises(HTTPException) as exc_info:
            _crs_mod._resolve_global_crs("NOT-A-CRS")

        assert exc_info.value.status_code == 404

    def test_missing_pyproj_raises_503(self, monkeypatch):
        # None in sys.modules makes `from pyproj import CRS` raise ImportError.
        monkeypatch.setitem(sys.modules, "pyproj", None)

        with pytest.raises(HTTPException) as exc_info:
            _crs_mod._resolve_global_crs("EPSG:4326")

        assert exc_info.value.status_code == 503


class TestResolveGlobalCrsEndpoint:
    @pytest.mark.asyncio
    async def test_text_plain_returns_wkt(self, monkeypatch):
        ext = _make_ext()
        fake_def = MagicMock()
        fake_def.definition = "WKT_STRING"
        monkeypatch.setattr(_crs_mod, "_resolve_global_crs", lambda uri: fake_def)

        from fastapi import Response
        req = MagicMock()
        req.headers = {"Accept": "text/plain"}

        result = await ext.resolve_global_crs_endpoint(request=req, crs_uri="EPSG:4326")

        assert isinstance(result, Response)
        assert result.body == b"WKT_STRING"
        assert result.media_type == "text/plain"

    @pytest.mark.asyncio
    async def test_json_returns_definition(self, monkeypatch):
        ext = _make_ext()
        fake_def = MagicMock(name="crsdef")
        monkeypatch.setattr(_crs_mod, "_resolve_global_crs", lambda uri: fake_def)

        req = MagicMock()
        req.headers = {"Accept": "application/json"}

        result = await ext.resolve_global_crs_endpoint(request=req, crs_uri="EPSG:4326")

        assert result is fake_def


# ---------------------------------------------------------------------------
# Platform list — _list_global_crs + list_global_crs_endpoint (paginated)
# ---------------------------------------------------------------------------

from collections import namedtuple  # noqa: E402

_CrsInfo = namedtuple("_CrsInfo", ["auth_name", "code"])


def _install_fake_pyproj_db(monkeypatch, infos):
    fake_db = types.ModuleType("pyproj.database")
    fake_db.query_crs_info = MagicMock(return_value=infos)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pyproj.database", fake_db)
    return fake_db


class TestListGlobalCrs:
    def test_list_route_registered_before_resolver(self):
        ext = _make_ext()
        paths = [r.path for r in ext.router.routes if hasattr(r, "path")]
        assert "/crs/" in paths
        assert paths.index("/crs/") < paths.index("/crs/{crs_uri:path}")

    def test_paginates_and_builds_register_uris(self, monkeypatch):
        infos = [_CrsInfo("EPSG", str(1000 + i)) for i in range(5)]
        fake_db = _install_fake_pyproj_db(monkeypatch, infos)

        uris, total = _crs_mod._list_global_crs("EPSG", limit=2, offset=1)

        assert total == 5
        assert uris == [
            "http://www.opengis.net/def/crs/EPSG/0/1001",
            "http://www.opengis.net/def/crs/EPSG/0/1002",
        ]
        fake_db.query_crs_info.assert_called_once_with(
            auth_name="EPSG", allow_deprecated=False
        )

    def test_empty_authority_enumerates_all(self, monkeypatch):
        fake_db = _install_fake_pyproj_db(monkeypatch, [_CrsInfo("OGC", "CRS84")])

        uris, total = _crs_mod._list_global_crs("", limit=10, offset=0)

        assert total == 1
        assert uris == ["http://www.opengis.net/def/crs/OGC/0/CRS84"]
        fake_db.query_crs_info.assert_called_once_with(
            auth_name=None, allow_deprecated=False
        )

    def test_missing_pyproj_raises_503(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "pyproj.database", None)

        with pytest.raises(HTTPException) as exc_info:
            _crs_mod._list_global_crs("EPSG", 10, 0)

        assert exc_info.value.status_code == 503


class TestListGlobalCrsEndpoint:
    @pytest.mark.asyncio
    async def test_next_link_when_more_pages(self, monkeypatch):
        ext = _make_ext()
        monkeypatch.setattr(_crs_mod, "_list_global_crs", lambda a, lim, off: (["u1", "u2"], 10))
        req = MagicMock()
        req.url = "http://testserver/crs/?limit=2&offset=0&authority=EPSG"

        result = await ext.list_global_crs_endpoint(
            request=req, limit=2, offset=0, authority="EPSG"
        )

        assert result.numberMatched == 10
        assert result.numberReturned == 2
        rels = [link.rel for link in result.links]
        assert "self" in rels and "next" in rels
        next_link = next(link for link in result.links if link.rel == "next")
        assert "offset=2" in next_link.href

    @pytest.mark.asyncio
    async def test_no_next_link_on_last_page(self, monkeypatch):
        ext = _make_ext()
        monkeypatch.setattr(_crs_mod, "_list_global_crs", lambda a, lim, off: (["u1"], 1))
        req = MagicMock()
        req.url = "http://testserver/crs/?limit=20&offset=0&authority=EPSG"

        result = await ext.list_global_crs_endpoint(
            request=req, limit=20, offset=0, authority="EPSG"
        )

        rels = [link.rel for link in result.links]
        assert "next" not in rels
