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

"""Unit tests for the aligned map routes added to MapsService.

Covers:
- get_collection_map: happy path, hidden collection → 404, unknown catalog → 404.
- get_collection_map_styled: styled variant delegates correct style_id.
- get_collection_maps: metadata endpoint returns DatasetMaps.
- get_catalog_maps: catalog-level metadata lists visible collections (public ids).
- get_catalog_map: dataset-level render resolves collections + delegates to impl.
- _get_map_impl: shared helper called by the aligned routes.

All external collaborators (DB engine, CatalogsProtocol, visibility) are
stubbed — no real I/O or database is touched.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException


# ---------------------------------------------------------------------------
# Stub osgeo BEFORE importing maps_service (renderer.py uses osgeo at module
# level, so the stub must be in sys.modules first).
# ---------------------------------------------------------------------------

def _install_osgeo_stubs() -> bool:
    """Install bare osgeo stubs; returns True if this call installed them
    (``osgeo`` was absent), i.e. it is safe for the caller to remove them
    again once the module import that needed them has completed — see
    ``_uninstall_osgeo_stubs``.
    """
    if "osgeo" in sys.modules:
        return False
    osgeo = types.ModuleType("osgeo")
    osgeo.__version__ = "3.x.stub"  # type: ignore[attr-defined]

    gdal = types.ModuleType("osgeo.gdal")
    gdal.Dataset = object  # type: ignore[attr-defined]
    gdal.GetDriverByName = lambda n: None  # type: ignore[attr-defined]
    gdal.VSIFOpenL = lambda *a: None  # type: ignore[attr-defined]
    gdal.VSIFSeekL = lambda *a: None  # type: ignore[attr-defined]
    gdal.VSIFTellL = lambda *a: 0  # type: ignore[attr-defined]
    gdal.VSIFReadL = lambda *a: b""  # type: ignore[attr-defined]
    gdal.VSIFCloseL = lambda *a: None  # type: ignore[attr-defined]
    gdal.Unlink = lambda *a: None  # type: ignore[attr-defined]
    gdal.RasterizeLayer = lambda *a, **kw: None  # type: ignore[attr-defined]

    ogr = types.ModuleType("osgeo.ogr")
    ogr.Geometry = object  # type: ignore[attr-defined]
    ogr.Layer = object  # type: ignore[attr-defined]
    ogr.Feature = lambda *a: None  # type: ignore[attr-defined]
    ogr.GetDriverByName = lambda n: None  # type: ignore[attr-defined]
    ogr.wkbLineString = 2  # type: ignore[attr-defined]
    ogr.wkbMultiPolygon = 6  # type: ignore[attr-defined]

    osr = types.ModuleType("osgeo.osr")
    osr.SpatialReference = object  # type: ignore[attr-defined]

    sys.modules.update({
        "osgeo": osgeo,
        "osgeo.gdal": gdal,
        "osgeo.ogr": ogr,
        "osgeo.osr": osr,
    })
    return True


def _uninstall_osgeo_stubs() -> None:
    """Remove the stub entries installed by ``_install_osgeo_stubs``.

    ``maps_service`` binds ``gdal``/``ogr``/``osr`` into its own module
    globals at import time, so these ``sys.modules`` entries aren't needed
    afterwards. Leaving them registered shadows the real ``osgeo`` package
    for every test module collected later in the same process — a later
    ``from osgeo import gdal; gdal.UseExceptions()`` (e.g.
    ``dynastore.modules.gdal.service``) would resolve to this bare stub and
    raise ``AttributeError: module 'osgeo.gdal' has no attribute
    'UseExceptions'`` instead of importing the real bindings.
    """
    for name in ("osgeo.osr", "osgeo.ogr", "osgeo.gdal", "osgeo"):
        sys.modules.pop(name, None)


_we_installed_osgeo_stubs = _install_osgeo_stubs()

from dynastore.extensions.maps import maps_service as ms  # noqa: E402

if _we_installed_osgeo_stubs:
    _uninstall_osgeo_stubs()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAKE_PNG = b"\x89PNG\r\n\x1a\n"


def _mock_request() -> MagicMock:
    req = MagicMock(name="request")
    req.url = MagicMock()
    req.url.__str__ = lambda self: "http://testserver/maps/catalogs/cat/collections/coll"
    req.url_for = MagicMock(return_value="http://testserver/maps/")
    return req


def _mock_catalogs_svc(
    catalog_result: str = "internal-cat",
    collection_result: str = "internal-coll",
    catalog_raises: Exception | None = None,
    collection_raises: Exception | None = None,
    collection_obj: object | None = None,
) -> MagicMock:
    svc = MagicMock()
    if catalog_raises:
        svc.resolve_catalog_id = AsyncMock(side_effect=catalog_raises)
    else:
        svc.resolve_catalog_id = AsyncMock(return_value=catalog_result)
    if collection_raises:
        svc.collections.resolve_collection_id = AsyncMock(side_effect=collection_raises)
    else:
        svc.collections.resolve_collection_id = AsyncMock(return_value=collection_result)

    coll = collection_obj or MagicMock()
    if not collection_obj:
        coll.id = "internal-coll"
        coll.title = None
    svc.get_collection = AsyncMock(return_value=coll)
    svc.get_catalog_model = AsyncMock(return_value=MagicMock())
    return svc


def _patch_visibility(monkeypatch, visible: bool = True) -> None:
    import dynastore.models.protocols.visibility as vis_mod
    visible_ids = None if visible else set()
    monkeypatch.setattr(
        vis_mod,
        "resolve_collection_listing_ids",
        AsyncMock(return_value=visible_ids),
    )


# ---------------------------------------------------------------------------
# get_collection_maps — metadata endpoint
# ---------------------------------------------------------------------------


class TestGetCollectionMaps:
    @pytest.mark.asyncio
    async def test_happy_path_returns_dataset_maps(self, monkeypatch):
        svc = _mock_catalogs_svc()
        monkeypatch.setattr(ms, "get_protocol", lambda _proto: svc)
        _patch_visibility(monkeypatch, visible=True)

        result = await ms.MapsService.get_collection_maps(
            catalog_id="my-cat",
            collection_id="my-coll",
            request=_mock_request(),
            language="en",
        )

        assert result.title is not None
        assert "my-cat" in result.title

    @pytest.mark.asyncio
    async def test_hidden_collection_returns_404(self, monkeypatch):
        svc = _mock_catalogs_svc()
        monkeypatch.setattr(ms, "get_protocol", lambda _proto: svc)
        _patch_visibility(monkeypatch, visible=False)

        with pytest.raises(HTTPException) as exc_info:
            await ms.MapsService.get_collection_maps(
                catalog_id="cat",
                collection_id="secret-coll",
                request=_mock_request(),
                language="en",
            )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_unknown_catalog_raises_404(self, monkeypatch):
        svc = _mock_catalogs_svc(catalog_raises=ValueError("not found"))
        monkeypatch.setattr(ms, "get_protocol", lambda _proto: svc)
        _patch_visibility(monkeypatch, visible=True)

        with pytest.raises(HTTPException) as exc_info:
            await ms.MapsService.get_collection_maps(
                catalog_id="unknown-cat",
                collection_id="coll",
                request=_mock_request(),
                language="en",
            )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_unknown_collection_raises_404(self, monkeypatch):
        svc = _mock_catalogs_svc(collection_raises=ValueError("no such collection"))
        monkeypatch.setattr(ms, "get_protocol", lambda _proto: svc)
        _patch_visibility(monkeypatch, visible=True)

        with pytest.raises(HTTPException) as exc_info:
            await ms.MapsService.get_collection_maps(
                catalog_id="cat",
                collection_id="missing-coll",
                request=_mock_request(),
                language="en",
            )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_map_render_link_uses_ogc_map_rel(self, monkeypatch):
        """The link to the /map render endpoint must use the OGC map relation type."""
        svc = _mock_catalogs_svc()
        monkeypatch.setattr(ms, "get_protocol", lambda _proto: svc)
        _patch_visibility(monkeypatch, visible=True)

        result = await ms.MapsService.get_collection_maps(
            catalog_id="my-cat",
            collection_id="my-coll",
            request=_mock_request(),
            language="en",
        )

        assert len(result.maps) == 1
        map_links = result.maps[0].links
        assert len(map_links) == 1
        assert map_links[0].rel == "http://www.opengis.net/def/rel/ogc/1.0/map"
        assert map_links[0].type == "image/png"


# ---------------------------------------------------------------------------
# get_collection_map — default style raster branch (happy path)
# ---------------------------------------------------------------------------


def _patch_raster_map_for_aligned(monkeypatch, *, visible: bool = True) -> None:
    """Wire up the raster branch for get_collection_map tests."""
    svc = _mock_catalogs_svc()
    svc.search_items = AsyncMock(
        return_value=[{"assets": {"data": {"href": "gs://bucket/data.tif"}}}]
    )
    monkeypatch.setattr(ms, "get_protocol", lambda _proto: svc)
    _patch_visibility(monkeypatch, visible=visible)

    monkeypatch.setattr(ms, "_RENDER_COG_MAP", lambda href, *, bbox, width, height, colormap, output_format: _FAKE_PNG)

    async def _fake_run(fn, *a, **kw):
        return fn(*a, **kw)

    monkeypatch.setattr(ms, "run_in_thread", _fake_run)
    monkeypatch.setattr(ms, "_convert_png_to_format", lambda b, fmt, **_: b)

    from dynastore.modules.catalog.catalog_config import CollectionInfo, CollectionKind
    info = CollectionInfo(kind=CollectionKind.RASTER)
    configs_mock = AsyncMock()
    configs_mock.get_config = AsyncMock(return_value=info)

    # _is_raster_collection calls get_protocol(ConfigsProtocol) — patch at module level
    def _get_protocol_selective(proto):
        from dynastore.models.protocols import ConfigsProtocol
        if proto is ConfigsProtocol:
            return configs_mock
        return svc

    monkeypatch.setattr(ms, "get_protocol", _get_protocol_selective)


class TestGetCollectionMap:
    @pytest.mark.asyncio
    async def test_raster_collection_returns_200(self, monkeypatch):
        _patch_raster_map_for_aligned(monkeypatch, visible=True)

        response = await ms.MapsService.get_collection_map(
            catalog_id="my-cat",
            collection_id="my-coll",
            request=_mock_request(),
            bbox="-180,-90,180,90",
            bbox_crs=None,
            crs="EPSG:4326",
            width=256,
            height=256,
            bgcolor=None,
            transparent=True,
            datetime=None,
            subset=None,
            f="png",
        )

        assert response.status_code == 200
        assert response.media_type == "image/png"

    @pytest.mark.asyncio
    async def test_hidden_collection_returns_404(self, monkeypatch):
        _patch_raster_map_for_aligned(monkeypatch, visible=False)

        with pytest.raises(HTTPException) as exc_info:
            await ms.MapsService.get_collection_map(
                catalog_id="cat",
                collection_id="hidden-coll",
                request=_mock_request(),
                bbox="-1,-1,1,1",
                bbox_crs=None,
                crs="EPSG:4326",
                width=64,
                height=64,
                bgcolor=None,
                transparent=True,
                datetime=None,
                subset=None,
                f="png",
            )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_unknown_catalog_raises_404(self, monkeypatch):
        svc = _mock_catalogs_svc(catalog_raises=ValueError("not found"))
        monkeypatch.setattr(ms, "get_protocol", lambda _proto: svc)
        _patch_visibility(monkeypatch, visible=True)

        with pytest.raises(HTTPException) as exc_info:
            await ms.MapsService.get_collection_map(
                catalog_id="bad-cat",
                collection_id="coll",
                request=_mock_request(),
                bbox="-180,-90,180,90",
                bbox_crs=None,
                crs="EPSG:4326",
                width=256,
                height=256,
                bgcolor=None,
                transparent=True,
                datetime=None,
                subset=None,
                f="png",
            )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_unsupported_format_raises_415(self, monkeypatch):
        _patch_raster_map_for_aligned(monkeypatch, visible=True)

        with pytest.raises(HTTPException) as exc_info:
            await ms.MapsService.get_collection_map(
                catalog_id="cat",
                collection_id="coll",
                request=_mock_request(),
                bbox="-1,-1,1,1",
                bbox_crs=None,
                crs="EPSG:4326",
                width=64,
                height=64,
                bgcolor=None,
                transparent=True,
                datetime=None,
                subset=None,
                f="bmp",
            )

        assert exc_info.value.status_code == 415


# ---------------------------------------------------------------------------
# get_collection_map_styled — explicit style_id
# ---------------------------------------------------------------------------


class TestGetCollectionMapStyled:
    @pytest.mark.asyncio
    async def test_styled_calls_impl_with_style_id(self, monkeypatch):
        """get_collection_map_styled must pass style_id as style= to _get_map_impl."""
        svc = _mock_catalogs_svc()
        monkeypatch.setattr(ms, "get_protocol", lambda _proto: svc)
        _patch_visibility(monkeypatch, visible=True)

        impl_calls: list[str | None] = []
        fake_response = MagicMock(status_code=200)

        async def _fake_impl(**kwargs):
            impl_calls.append(kwargs.get("style"))
            return fake_response

        with patch.object(ms.MapsService, "_get_map_impl", staticmethod(_fake_impl)):
            await ms.MapsService.get_collection_map_styled(
                catalog_id="cat",
                collection_id="coll",
                style_id="ndvi",
                request=_mock_request(),
                bbox="-180,-90,180,90",
                bbox_crs=None,
                crs="EPSG:4326",
                width=256,
                height=256,
                bgcolor=None,
                transparent=True,
                datetime=None,
                subset=None,
                f="png",
            )

        assert impl_calls == ["ndvi"], f"Expected style='ndvi', got {impl_calls}"


# ---------------------------------------------------------------------------
# get_catalog_maps — catalog-level metadata listing
# ---------------------------------------------------------------------------


def _make_coll(cid: str, ext: str | None = None, title: object | None = None) -> MagicMock:
    c = MagicMock()
    c.id = cid
    c.external_id = ext
    c.title = title
    return c


class TestGetCatalogMaps:
    @pytest.mark.asyncio
    async def test_lists_collections_with_public_ids(self, monkeypatch):
        """Links use the public external_id when set, else the internal id."""
        svc = _mock_catalogs_svc()
        svc.list_collections = AsyncMock(
            return_value=[_make_coll("internal-a", ext="public-a"), _make_coll("internal-b")]
        )
        monkeypatch.setattr(ms, "get_protocol", lambda _proto: svc)
        _patch_visibility(monkeypatch, visible=True)  # None → no filter

        result = await ms.MapsService.get_catalog_maps(
            catalog_id="my-cat", request=_mock_request(), language="en"
        )

        assert result.title is not None and "my-cat" in result.title
        hrefs = [m.links[0].href for m in result.maps]
        assert any(h.endswith("/collections/public-a") for h in hrefs)
        assert any(h.endswith("/collections/internal-b") for h in hrefs)

    @pytest.mark.asyncio
    async def test_visibility_filters_hidden_collections(self, monkeypatch):
        svc = _mock_catalogs_svc()
        svc.list_collections = AsyncMock(
            return_value=[_make_coll("internal-a", ext="public-a"), _make_coll("internal-secret")]
        )
        monkeypatch.setattr(ms, "get_protocol", lambda _proto: svc)
        import dynastore.models.protocols.visibility as vis_mod
        monkeypatch.setattr(
            vis_mod, "resolve_collection_listing_ids", AsyncMock(return_value={"internal-a"})
        )

        result = await ms.MapsService.get_catalog_maps(
            catalog_id="cat", request=_mock_request(), language="en"
        )

        assert len(result.maps) == 1
        assert result.maps[0].links[0].href.endswith("/collections/public-a")

    @pytest.mark.asyncio
    async def test_unknown_catalog_raises_404(self, monkeypatch):
        svc = _mock_catalogs_svc(catalog_raises=ValueError("not found"))
        monkeypatch.setattr(ms, "get_protocol", lambda _proto: svc)
        _patch_visibility(monkeypatch, visible=True)

        with pytest.raises(HTTPException) as exc_info:
            await ms.MapsService.get_catalog_maps(
                catalog_id="x", request=_mock_request(), language="en"
            )

        assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# get_catalog_map — dataset-level render (OGC Maps /conf/dataset-map)
# ---------------------------------------------------------------------------


class TestGetCatalogMap:
    @pytest.mark.asyncio
    async def test_resolves_and_delegates_joined_internal_collections(self, monkeypatch):
        svc = _mock_catalogs_svc()
        svc.collections.resolve_collection_id = AsyncMock(side_effect=["int-a", "int-b"])
        monkeypatch.setattr(ms, "get_protocol", lambda _proto: svc)
        _patch_visibility(monkeypatch, visible=True)

        captured: dict = {}

        async def _fake_impl(**kwargs):
            captured.update(kwargs)
            return MagicMock(status_code=200)

        with patch.object(ms.MapsService, "_get_map_impl", staticmethod(_fake_impl)):
            resp = await ms.MapsService.get_catalog_map(
                catalog_id="cat",
                request=_mock_request(),
                collections="a,b",
                bbox="-180,-90,180,90",
                bbox_crs=None,
                crs="EPSG:4326",
                width=256,
                height=256,
                style=None,
                bgcolor=None,
                transparent=True,
                datetime=None,
                subset=None,
                f="png",
            )

        assert resp.status_code == 200
        assert captured["dataset"] == "internal-cat"
        assert captured["collections"] == "int-a,int-b"

    @pytest.mark.asyncio
    async def test_hidden_collection_returns_404(self, monkeypatch):
        svc = _mock_catalogs_svc()
        svc.collections.resolve_collection_id = AsyncMock(return_value="int-secret")
        monkeypatch.setattr(ms, "get_protocol", lambda _proto: svc)
        import dynastore.models.protocols.visibility as vis_mod
        monkeypatch.setattr(
            vis_mod, "resolve_collection_listing_ids", AsyncMock(return_value=set())
        )

        with pytest.raises(HTTPException) as exc_info:
            await ms.MapsService.get_catalog_map(
                catalog_id="cat",
                request=_mock_request(),
                collections="secret",
                bbox="-180,-90,180,90",
                bbox_crs=None,
                crs="EPSG:4326",
                width=64,
                height=64,
                style=None,
                bgcolor=None,
                transparent=True,
                datetime=None,
                subset=None,
                f="png",
            )

        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_empty_collections_raises_400(self, monkeypatch):
        svc = _mock_catalogs_svc()
        monkeypatch.setattr(ms, "get_protocol", lambda _proto: svc)
        _patch_visibility(monkeypatch, visible=True)

        with pytest.raises(HTTPException) as exc_info:
            await ms.MapsService.get_catalog_map(
                catalog_id="cat",
                request=_mock_request(),
                collections="  ,  ",
                bbox="-180,-90,180,90",
                bbox_crs=None,
                crs="EPSG:4326",
                width=64,
                height=64,
                style=None,
                bgcolor=None,
                transparent=True,
                datetime=None,
                subset=None,
                f="png",
            )

        assert exc_info.value.status_code == 400
