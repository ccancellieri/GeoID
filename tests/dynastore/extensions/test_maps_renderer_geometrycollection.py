"""Renderer regression: GEOMETRYCOLLECTION features must render, not 500.

GEOS refuses boundary/overlay operations on GeometryCollection inputs
(``IllegalArgumentException: Operation not supported by GeometryCollection``),
so a single stored GeometryCollection feature crashed the stroke pass
(``geom.GetBoundary()``) and turned every tile containing it into an HTTP 500
— a blank map for the whole layer. These tests pin that collection geometries
are exploded into their concrete parts and paint under both the default-style
path and the styled-layer path.
"""

import pytest

osgeo = pytest.importorskip("osgeo")
from osgeo import gdal, ogr  # noqa: E402

from dynastore.extensions.maps.renderer import render_map_image  # noqa: E402


def _polygon() -> ogr.Geometry:
    ring = ogr.Geometry(ogr.wkbLinearRing)
    for x, y in [(10, 5), (40, 5), (40, 30), (10, 30), (10, 5)]:
        ring.AddPoint_2D(float(x), float(y))
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    return poly


def _linestring() -> ogr.Geometry:
    line = ogr.Geometry(ogr.wkbLineString)
    for x, y in [(5, 5), (20, 20), (35, 15), (50, 35)]:
        line.AddPoint_2D(float(x), float(y))
    return line


def _wkb_geometrycollection_4326() -> bytes:
    """A polygon + line collection (the shape that crashed GetBoundary)."""
    gc = ogr.Geometry(ogr.wkbGeometryCollection)
    gc.AddGeometry(_polygon())
    gc.AddGeometry(_linestring())
    return gc.ExportToWkb()


def _wkb_nested_geometrycollection_4326() -> bytes:
    """A collection nested inside a collection (legal WKB) — pins recursion."""
    inner = ogr.Geometry(ogr.wkbGeometryCollection)
    inner.AddGeometry(_polygon())
    outer = ogr.Geometry(ogr.wkbGeometryCollection)
    outer.AddGeometry(inner)
    outer.AddGeometry(_linestring())
    return outer.ExportToWkb()


def _max_alpha(png_bytes: bytes) -> float:
    """Return the maximum alpha-band value of a rendered PNG (0 == fully blank)."""
    mem_path = "/vsimem/_maps_gc_test.png"
    gdal.FileFromMemBuffer(mem_path, png_bytes)
    try:
        ds = gdal.Open(mem_path)
        assert ds is not None
        _, mx = ds.GetRasterBand(4).ComputeRasterMinMax(False)
        return mx
    finally:
        gdal.Unlink(mem_path)


_BBOX_DEGREES = [0.0, 0.0, 55.0, 40.0]


def _render(wkb: bytes, style_record=None) -> bytes:
    return render_map_image(
        256,
        256,
        list(_BBOX_DEGREES),
        "EPSG:3857",
        4326,  # source_srid
        [{"geom": wkb, "layer": "gc_layer", "attributes": {"name": "a"}}],
        style_record,
        True,  # transparent background
        None,  # bgcolor
        4326,  # bbox_srid
    )


@pytest.mark.parametrize(
    "wkb_factory",
    [_wkb_geometrycollection_4326, _wkb_nested_geometrycollection_4326],
    ids=["flat", "nested"],
)
def test_geometrycollection_paints_under_default_style(wkb_factory):
    """A GeometryCollection feature must paint. Pre-fix this raised
    ``IllegalArgumentException: Operation not supported by GeometryCollection``
    from GetBoundary() and 500'd the whole tile."""
    png = _render(wkb_factory())
    assert _max_alpha(png) > 0, "GeometryCollection rendered blank"


def test_geometrycollection_paints_under_styled_layer_path():
    """The styled-layer loading loop must also explode collections: an
    unsupported style format falls back to the default renderer over the OGR
    layer's features, which hits the same GetBoundary() stroke pass."""

    class _Format:
        value = "UNSUPPORTED_TEST_FORMAT"

    class _Content:
        format = _Format()

        @staticmethod
        def model_dump():
            return {}

    class _StyleRecord:
        content = _Content()

    png = _render(_wkb_geometrycollection_4326(), style_record=_StyleRecord())
    assert _max_alpha(png) > 0, "GeometryCollection rendered blank via styled path"


def test_empty_geometrycollection_renders_blank_without_error():
    """GEOMETRYCOLLECTION EMPTY contributes nothing but must not raise."""
    gc = ogr.Geometry(ogr.wkbGeometryCollection)
    png = _render(gc.ExportToWkb())
    assert _max_alpha(png) == 0
