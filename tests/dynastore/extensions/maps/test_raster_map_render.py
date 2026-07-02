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

"""Unit tests for the raster branch added to MapsService (Slice 2).

Covers:
- ``render_cog_map`` engine function signature and behaviour.
- ``OGC_API_MAPS_URIS`` conformance URI set (no over-claim).
- Raster map handler: external→internal ID resolution, visibility guard.
- Raster tile handler: WebMercatorQuad uses render_cog_tile; other TMS
  falls back to render_cog_map via bbox.
- Hidden collection returns 404 on both routes.
- Format negotiation passes bbox+crs through to convert_png_to_format.

All collaborators (osgeo, rio_tiler, DB, protocol registry) are stubbed;
no real I/O or database is touched.
"""

from __future__ import annotations

import sys
import types
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest


# ---------------------------------------------------------------------------
# Stub heavy C-extension dependencies that are absent in the local unit env.
# The maps service does a bare  ``from osgeo import ...`` at module level;
# we need those entries in sys.modules BEFORE importing maps_service.
# ---------------------------------------------------------------------------

def _install_stubs() -> bool:
    """Idempotently insert lightweight stubs for osgeo into sys.modules.

    ``renderer.py`` uses osgeo type annotations at module level
    (``ogr.Geometry``, ``ogr.Layer``, ``osr.SpatialReference``, …) so the
    stubs must expose those names as bare ``object`` stand-ins to avoid
    ``AttributeError`` during collection.

    Returns ``True`` if this call installed the stubs (``osgeo`` was absent),
    ``False`` if a real (or already-stubbed) ``osgeo`` was already resident.
    Callers use the return value to know whether it is safe to remove the
    entries again afterwards — see ``_uninstall_stubs`` below.
    """
    if "osgeo" not in sys.modules:
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

        sys.modules["osgeo"] = osgeo
        sys.modules["osgeo.gdal"] = gdal
        sys.modules["osgeo.ogr"] = ogr
        sys.modules["osgeo.osr"] = osr
        return True
    return False


def _uninstall_stubs() -> None:
    """Remove the stub entries installed by ``_install_stubs``.

    ``maps_service`` (imported right below) binds ``gdal``/``ogr``/``osr``
    into its own module globals at import time, so it no longer needs these
    ``sys.modules`` entries once the import completes. Leaving the bare
    ``types.ModuleType`` stand-ins registered under ``osgeo``/``osgeo.gdal``/
    etc. shadows the real package for every test module collected afterwards
    in the same process — any later ``from osgeo import gdal; gdal.UseExceptions()``
    (e.g. ``dynastore.modules.gdal.service``) would resolve to this stub and
    raise ``AttributeError: module 'osgeo.gdal' has no attribute
    'UseExceptions'`` instead of importing the real bindings.
    """
    for name in ("osgeo.osr", "osgeo.ogr", "osgeo.gdal", "osgeo"):
        sys.modules.pop(name, None)


_we_installed_stubs = _install_stubs()

# Now it is safe to import maps_service.
from dynastore.extensions.maps import maps_service as ms  # noqa: E402

if _we_installed_stubs:
    _uninstall_stubs()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAKE_PNG = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"


def _mock_request() -> MagicMock:
    return MagicMock(name="request")


# ---------------------------------------------------------------------------
# 1. Conformance URIs — no over-claim
# ---------------------------------------------------------------------------


class TestConformanceUris:
    """OGC_API_MAPS_URIS must contain every class we implement and nothing we don't."""

    BASE = "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/"

    def test_contains_core(self):
        assert f"{self.BASE}core" in ms.OGC_API_MAPS_URIS

    def test_contains_dataset_map(self):
        # /conf/dataset-map: /map at dataset level (Req 10).
        assert f"{self.BASE}dataset-map" in ms.OGC_API_MAPS_URIS

    def test_contains_collection_map(self):
        # /conf/collection-map: /collections/{cid}/map at collection level (Req 11).
        assert f"{self.BASE}collection-map" in ms.OGC_API_MAPS_URIS

    def test_contains_png(self):
        assert f"{self.BASE}png" in ms.OGC_API_MAPS_URIS

    def test_contains_jpeg(self):
        assert f"{self.BASE}jpeg" in ms.OGC_API_MAPS_URIS

    def test_contains_tiff_not_geotiff(self):
        # The registered Maps encoding slug is `tiff`, not `geotiff`.
        # `geotiff` is a Coverages slug; claiming it here would be over-claiming.
        assert f"{self.BASE}tiff" in ms.OGC_API_MAPS_URIS
        assert f"{self.BASE}geotiff" not in ms.OGC_API_MAPS_URIS, (
            "Over-claim: geotiff is a Coverages slug; Maps uses tiff"
        )

    def test_does_not_claim_tilesets(self):
        # Map-tile generation (the Maps /conf/tilesets class, Req 22-24) moved to the
        # Tiles extension, served as map-tiles (dataType=map) under
        # /tiles/catalogs/{cat}/collections/{coll}/map/tiles/... — Maps must not claim it.
        assert f"{self.BASE}tilesets" not in ms.OGC_API_MAPS_URIS, (
            "Stale claim: map-tile generation moved to the Tiles extension"
        )
        assert f"{self.BASE}tilesets-map" not in ms.OGC_API_MAPS_URIS, (
            "Over-claim: tilesets-map is not a registered Maps conformance class"
        )

    def test_contains_scaling(self):
        # /conf/scaling: width/height resample output via COGReader.part() (Req 15).
        assert f"{self.BASE}scaling" in ms.OGC_API_MAPS_URIS

    def test_contains_background_not_display(self):
        # /conf/background: bgcolor and transparent params accepted on /map (Req 16).
        # NOT /conf/display-resolution (mm-per-pixel class, not implemented).
        assert f"{self.BASE}background" in ms.OGC_API_MAPS_URIS
        assert f"{self.BASE}display" not in ms.OGC_API_MAPS_URIS, (
            "Over-claim: display is not the registered slug for bgcolor/transparent"
        )
        assert f"{self.BASE}display-resolution" not in ms.OGC_API_MAPS_URIS, (
            "Over-claim: display-resolution (mm-per-pixel) is not implemented"
        )

    def test_contains_spatial_subsetting(self):
        # /conf/spatial-subsetting: bbox/bbox-crs params accepted on /map (Req 17/18).
        assert f"{self.BASE}spatial-subsetting" in ms.OGC_API_MAPS_URIS

    def test_no_overclaim(self):
        """Classes we do NOT implement must not appear in the list."""
        not_implemented = [
            f"{self.BASE}temporal",
            f"{self.BASE}general-subsetting",
            f"{self.BASE}display-resolution",
            f"{self.BASE}geotiff",
            f"{self.BASE}tilesets",
            f"{self.BASE}tilesets-map",
            f"{self.BASE}display",
        ]
        for uri in not_implemented:
            assert uri not in ms.OGC_API_MAPS_URIS, f"Over-claimed: {uri}"


# ---------------------------------------------------------------------------
# 2. render_cog_map engine function
# ---------------------------------------------------------------------------


class TestRenderCogMapEngine:
    """``render_cog_map`` in engine.py must call COGReader.part() with correct args."""

    def test_calls_cog_reader_part(self, monkeypatch):
        from dynastore.modules.renders import engine

        captured: dict[str, Any] = {}

        class _FakePart:
            def render(self, *, img_format, colormap, add_mask):
                return _FAKE_PNG

        class _FakeCOGReader:
            def __init__(self, *, input):
                captured["href"] = input

            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

            def part(self, *, bbox, indexes, width, height, expression=None):
                captured["bbox"] = bbox
                captured["width"] = width
                captured["height"] = height
                captured["indexes"] = indexes
                captured["expression"] = expression
                return _FakePart()

        fake_rio = types.ModuleType("rio_tiler")
        fake_rio_io = types.ModuleType("rio_tiler.io")
        fake_rio_io.COGReader = _FakeCOGReader  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "rio_tiler", fake_rio)
        monkeypatch.setitem(sys.modules, "rio_tiler.io", fake_rio_io)

        result = engine.render_cog_map(
            "gs://bucket/data.tif",
            bbox=[-180.0, -90.0, 180.0, 90.0],
            width=256,
            height=256,
        )

        assert result == _FAKE_PNG
        assert captured["href"] == "gs://bucket/data.tif"
        assert captured["bbox"] == (-180.0, -90.0, 180.0, 90.0)
        assert captured["width"] == 256
        assert captured["height"] == 256
        assert captured["indexes"] == (1,)
        assert captured["expression"] is None

    def test_passes_colormap(self, monkeypatch):
        from dynastore.modules.renders import engine

        received: dict[str, Any] = {}

        class _FakePart:
            def render(self, *, img_format, colormap, add_mask):
                received["colormap"] = colormap
                return _FAKE_PNG

        class _FakeCOGReader:
            def __init__(self, *, input):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

            def part(self, **kwargs):
                return _FakePart()

        monkeypatch.setitem(sys.modules, "rio_tiler", types.ModuleType("rio_tiler"))
        fake_io = types.ModuleType("rio_tiler.io")
        fake_io.COGReader = _FakeCOGReader  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "rio_tiler.io", fake_io)

        cmap = {1: (255, 0, 0, 255)}
        engine.render_cog_map(
            "gs://b/f.tif",
            bbox=[0.0, 0.0, 1.0, 1.0],
            width=64,
            height=64,
            colormap=cmap,
        )

        assert received["colormap"] == cmap

    def test_raises_import_error_without_rio_tiler(self, monkeypatch):
        from dynastore.modules.renders import engine

        monkeypatch.setitem(sys.modules, "rio_tiler", None)  # type: ignore[call-overload]
        monkeypatch.setitem(sys.modules, "rio_tiler.io", None)  # type: ignore[call-overload]

        with pytest.raises(ImportError, match="rio-tiler"):
            engine.render_cog_map(
                "gs://b/f.tif",
                bbox=[0.0, 0.0, 1.0, 1.0],
                width=32,
                height=32,
            )


# ---------------------------------------------------------------------------
# 3. _is_raster_collection helper
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_is_raster_collection_true(monkeypatch):
    from dynastore.modules.catalog.catalog_config import CollectionInfo, CollectionKind

    info = CollectionInfo(kind=CollectionKind.RASTER)
    configs_mock = AsyncMock()
    configs_mock.get_config = AsyncMock(return_value=info)

    monkeypatch.setattr(ms, "get_protocol", lambda _proto: configs_mock)

    result = await ms._is_raster_collection("cat", "coll")
    assert result is True


@pytest.mark.asyncio
async def test_is_raster_collection_false_for_vector(monkeypatch):
    from dynastore.modules.catalog.catalog_config import CollectionInfo, CollectionKind

    info = CollectionInfo(kind=CollectionKind.VECTOR)
    configs_mock = AsyncMock()
    configs_mock.get_config = AsyncMock(return_value=info)

    monkeypatch.setattr(ms, "get_protocol", lambda _proto: configs_mock)

    result = await ms._is_raster_collection("cat", "coll")
    assert result is False


@pytest.mark.asyncio
async def test_is_raster_collection_false_when_configs_unavailable(monkeypatch):
    monkeypatch.setattr(ms, "get_protocol", lambda _proto: None)

    result = await ms._is_raster_collection("cat", "coll")
    assert result is False


# ---------------------------------------------------------------------------
# 4. _resolve_raster_cog_href helper
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_raster_cog_href_data_key(monkeypatch):
    item = {"assets": {"data": {"href": "gs://b/file.tif"}}}

    class _FakeItem:
        def model_dump(self, **kwargs):
            return item

    catalogs_mock = AsyncMock()
    catalogs_mock.search_items = AsyncMock(return_value=[_FakeItem()])
    monkeypatch.setattr(ms, "get_protocol", lambda _proto: catalogs_mock)

    result = await ms._resolve_raster_cog_href("cat", "coll")
    assert result == "gs://b/file.tif"


@pytest.mark.asyncio
async def test_resolve_raster_cog_href_fallback_to_any(monkeypatch):
    item = {"assets": {"thumbnail": {"href": "https://host/img.tif"}}}

    catalogs_mock = AsyncMock()
    catalogs_mock.search_items = AsyncMock(return_value=[item])
    monkeypatch.setattr(ms, "get_protocol", lambda _proto: catalogs_mock)

    result = await ms._resolve_raster_cog_href("cat", "coll")
    assert result == "https://host/img.tif"


@pytest.mark.asyncio
async def test_resolve_raster_cog_href_none_on_empty(monkeypatch):
    catalogs_mock = AsyncMock()
    catalogs_mock.search_items = AsyncMock(return_value=[])
    monkeypatch.setattr(ms, "get_protocol", lambda _proto: catalogs_mock)

    result = await ms._resolve_raster_cog_href("cat", "coll")
    assert result is None


@pytest.mark.asyncio
async def test_resolve_raster_cog_href_none_when_no_protocol(monkeypatch):
    monkeypatch.setattr(ms, "get_protocol", lambda _proto: None)
    result = await ms._resolve_raster_cog_href("cat", "coll")
    assert result is None


# ---------------------------------------------------------------------------
# 5. _resolve_internal_collection_id helper
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_internal_collection_id_resolves(monkeypatch):
    catalogs_mock = MagicMock()
    catalogs_mock.collections.resolve_collection_id = AsyncMock(return_value="c_internal_001")
    monkeypatch.setattr(ms, "get_protocol", lambda _proto: catalogs_mock)

    result = await ms._resolve_internal_collection_id("cat", "public-name")
    assert result == "c_internal_001"


@pytest.mark.asyncio
async def test_resolve_internal_collection_id_passthrough_on_error(monkeypatch):
    catalogs_mock = MagicMock()
    catalogs_mock.collections.resolve_collection_id = AsyncMock(
        side_effect=RuntimeError("no resolution")
    )
    monkeypatch.setattr(ms, "get_protocol", lambda _proto: catalogs_mock)

    result = await ms._resolve_internal_collection_id("cat", "my-collection")
    assert result == "my-collection"


@pytest.mark.asyncio
async def test_resolve_internal_collection_id_passthrough_when_no_protocol(monkeypatch):
    monkeypatch.setattr(ms, "get_protocol", lambda _proto: None)
    result = await ms._resolve_internal_collection_id("cat", "ext-id")
    assert result == "ext-id"


# ---------------------------------------------------------------------------
# 6. _render_raster_map — happy path and visibility guard
# ---------------------------------------------------------------------------


def _patch_raster_map_dependencies(monkeypatch, *, visible: bool = True) -> None:
    """Wire up all dependencies for ``_render_raster_map`` tests."""
    # Protocol registry: return a CatalogsProtocol stub for EVERY get_protocol call.
    catalogs_mock = MagicMock(name="catalogs")
    catalogs_mock.resolve_catalog_id = AsyncMock(return_value="cat_internal")
    catalogs_mock.collections.resolve_collection_id = AsyncMock(return_value="coll_internal")
    catalogs_mock.search_items = AsyncMock(
        return_value=[{"assets": {"data": {"href": "gs://bucket/data.tif"}}}]
    )
    monkeypatch.setattr(ms, "get_protocol", lambda _proto: catalogs_mock)

    # Visibility: resolve_collection_listing_ids returns None (all visible) or a set.
    visible_ids = None if visible else set()

    import dynastore.models.protocols.visibility as vis_mod  # type: ignore[import]
    monkeypatch.setattr(
        vis_mod, "resolve_collection_listing_ids", AsyncMock(return_value=visible_ids)
    )

    # Engine: replace render_cog_map with a simple sync stub.
    monkeypatch.setattr(ms, "_RENDER_COG_MAP", lambda href, *, bbox, width, height, colormap, output_format: _FAKE_PNG)

    # run_in_thread: call the callable synchronously in tests.
    async def _fake_run_in_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(ms, "run_in_thread", _fake_run_in_thread)

    # Format converter: passthrough for PNG.
    monkeypatch.setattr(ms, "_convert_png_to_format", lambda b, fmt, **_: b)


@pytest.mark.asyncio
async def test_render_raster_map_happy_path(monkeypatch):
    _patch_raster_map_dependencies(monkeypatch)

    response = await ms._render_raster_map(
        catalog_id="my-catalog",
        collection_id="my-collection",
        bbox=[-180.0, -90.0, 180.0, 90.0],
        width=256,
        height=256,
        style_name=None,
        fmt="png",
        request=_mock_request(),
    )

    assert response.status_code == 200
    assert response.media_type == "image/png"
    assert response.body == _FAKE_PNG


@pytest.mark.asyncio
async def test_render_raster_map_hidden_collection_returns_404(monkeypatch):
    _patch_raster_map_dependencies(monkeypatch, visible=False)

    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc:
        await ms._render_raster_map(
            catalog_id="my-catalog",
            collection_id="hidden-coll",
            bbox=[-1.0, -1.0, 1.0, 1.0],
            width=64,
            height=64,
            style_name=None,
            fmt="png",
            request=_mock_request(),
        )

    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_render_raster_map_no_items_returns_404(monkeypatch):
    catalogs_mock = MagicMock(name="catalogs")
    catalogs_mock.resolve_catalog_id = AsyncMock(return_value="cat_internal")
    catalogs_mock.collections.resolve_collection_id = AsyncMock(return_value="coll_internal")
    catalogs_mock.search_items = AsyncMock(return_value=[])
    monkeypatch.setattr(ms, "get_protocol", lambda _proto: catalogs_mock)

    import dynastore.models.protocols.visibility as vis_mod  # type: ignore[import]
    monkeypatch.setattr(
        vis_mod, "resolve_collection_listing_ids", AsyncMock(return_value=None)
    )

    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc:
        await ms._render_raster_map(
            catalog_id="cat",
            collection_id="empty-coll",
            bbox=[0.0, 0.0, 1.0, 1.0],
            width=32,
            height=32,
            style_name=None,
            fmt="png",
            request=_mock_request(),
        )

    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_render_raster_map_rio_tiler_unavailable_returns_422(monkeypatch):
    """When the renders extension is not installed _RENDER_COG_MAP is None → 422."""
    monkeypatch.setattr(ms, "_RENDER_COG_MAP", None)

    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc:
        await ms._render_raster_map(
            catalog_id="cat",
            collection_id="coll",
            bbox=[0.0, 0.0, 1.0, 1.0],
            width=32,
            height=32,
            style_name=None,
            fmt="png",
            request=_mock_request(),
        )

    assert exc.value.status_code == 422
    assert "rio-tiler" in exc.value.detail


# ---------------------------------------------------------------------------
# 8. Format negotiation: bbox+crs forwarded to convert_png_to_format
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_render_raster_map_format_jpeg(monkeypatch):
    """JPEG format: _convert_png_to_format receives fmt='jpeg' and bbox."""
    _patch_raster_map_dependencies(monkeypatch)

    convert_calls: list[dict[str, Any]] = []

    def _fake_convert(png_bytes, fmt, *, bbox=None, crs=None):
        convert_calls.append({"fmt": fmt, "bbox": bbox})
        return b"fake_jpeg"

    monkeypatch.setattr(ms, "_convert_png_to_format", _fake_convert)
    monkeypatch.setattr(ms, "_FORMAT_MEDIA_TYPES", {"png": "image/png", "jpeg": "image/jpeg", "geotiff": "image/tiff;application=geotiff"})

    response = await ms._render_raster_map(
        catalog_id="cat",
        collection_id="coll",
        bbox=[10.0, 20.0, 11.0, 21.0],
        width=128,
        height=128,
        style_name=None,
        fmt="jpeg",
        request=_mock_request(),
    )

    assert response.media_type == "image/jpeg"
    assert len(convert_calls) == 1
    assert convert_calls[0]["fmt"] == "jpeg"
    assert convert_calls[0]["bbox"] == [10.0, 20.0, 11.0, 21.0]


# ---------------------------------------------------------------------------
# 9. External→internal ID resolution in _render_raster_map
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_render_raster_map_uses_internal_id_for_item_lookup(monkeypatch):
    """The internal (resolved) catalog and collection IDs must be used for
    item lookup, not the external public IDs supplied in the request path."""

    resolved_ids: dict[str, str] = {}
    item_lookup_catalog: list[str] = []

    catalogs_mock = MagicMock(name="catalogs")
    catalogs_mock.resolve_catalog_id = AsyncMock(return_value="cat_internal")
    catalogs_mock.collections.resolve_collection_id = AsyncMock(return_value="coll_internal")

    async def _fake_search_items(catalog_id, collection_id, *args, **kwargs):
        item_lookup_catalog.append(catalog_id)
        resolved_ids["catalog"] = catalog_id
        resolved_ids["collection"] = collection_id
        return [{"assets": {"data": {"href": "gs://b/f.tif"}}}]

    catalogs_mock.search_items = _fake_search_items
    monkeypatch.setattr(ms, "get_protocol", lambda _proto: catalogs_mock)

    import dynastore.models.protocols.visibility as vis_mod  # type: ignore[import]
    monkeypatch.setattr(
        vis_mod, "resolve_collection_listing_ids", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(ms, "_RENDER_COG_MAP", lambda href, *, bbox, width, height, colormap, output_format: _FAKE_PNG)

    async def _fake_run_in_thread(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    monkeypatch.setattr(ms, "run_in_thread", _fake_run_in_thread)
    monkeypatch.setattr(ms, "_convert_png_to_format", lambda b, fmt, **_: b)

    await ms._render_raster_map(
        catalog_id="ext-catalog",
        collection_id="ext-collection",
        bbox=[0.0, 0.0, 1.0, 1.0],
        width=64,
        height=64,
        style_name=None,
        fmt="png",
        request=_mock_request(),
    )

    # The item lookup must use the INTERNAL ids, never the external ones.
    assert resolved_ids["catalog"] == "cat_internal", (
        "Item lookup used external catalog id instead of internal id"
    )
    assert resolved_ids["collection"] == "coll_internal", (
        "Item lookup used external collection id instead of internal id"
    )
