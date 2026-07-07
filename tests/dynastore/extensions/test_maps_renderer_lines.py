"""Renderer regression: line/network geometries must paint, not render blank.

A (multi)line collection (roads, rivers, network graphs) rendered with the
default or SLD style previously produced a fully transparent PNG: the stroke
pass was built from ``geom.GetBoundary()``, which on a LineString returns only
its endpoints (a MultiPoint), and the fill pass draws nothing for a
zero-area line. The result was blank/white tiles for the whole layer. These
tests pin that a line geometry now paints under the default style (and that a
polygon still paints, guarding against a regression in the boundary branch).
"""

import pytest

osgeo = pytest.importorskip("osgeo")
from osgeo import gdal, ogr  # noqa: E402

from dynastore.extensions.maps.renderer import render_map_image  # noqa: E402


def _wkb_linestring_4326() -> bytes:
    """A diagonal line across the test bbox, in EPSG:4326."""
    line = ogr.Geometry(ogr.wkbLineString)
    for x, y in [(5, 5), (20, 20), (35, 15), (50, 35)]:
        line.AddPoint_2D(float(x), float(y))
    return line.ExportToWkb()


def _wkb_multilinestring_4326() -> bytes:
    """A two-part multiline in EPSG:4326 (mirrors demo8m/network geometry)."""
    mls = ogr.Geometry(ogr.wkbMultiLineString)
    for pts in ([(5, 30), (25, 32)], [(30, 8), (48, 12)]):
        seg = ogr.Geometry(ogr.wkbLineString)
        for x, y in pts:
            seg.AddPoint_2D(float(x), float(y))
        mls.AddGeometry(seg)
    return mls.ExportToWkb()


def _wkb_polygon_4326() -> bytes:
    ring = ogr.Geometry(ogr.wkbLinearRing)
    for x, y in [(10, 5), (40, 5), (40, 30), (10, 30), (10, 5)]:
        ring.AddPoint_2D(float(x), float(y))
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    return poly.ExportToWkb()


def _max_alpha(png_bytes: bytes) -> float:
    """Return the maximum alpha-band value of a rendered PNG (0 == fully blank)."""
    mem_path = "/vsimem/_maps_lines_test.png"
    gdal.FileFromMemBuffer(mem_path, png_bytes)
    try:
        ds = gdal.Open(mem_path)
        assert ds is not None
        _, mx = ds.GetRasterBand(4).ComputeRasterMinMax(False)
        return mx
    finally:
        gdal.Unlink(mem_path)


_BBOX_DEGREES = [0.0, 0.0, 55.0, 40.0]


def _render(wkb: bytes) -> bytes:
    return render_map_image(
        256,
        256,
        list(_BBOX_DEGREES),
        "EPSG:3857",
        4326,  # source_srid
        [{"geom": wkb}],
        None,  # default style (random fill + black stroke)
        True,  # transparent background
        None,  # bgcolor
        4326,  # bbox_srid
    )


@pytest.mark.parametrize(
    "wkb_factory",
    [_wkb_linestring_4326, _wkb_multilinestring_4326],
    ids=["linestring", "multilinestring"],
)
def test_line_geometry_paints_under_default_style(wkb_factory):
    """Line/network geometries must paint. Pre-fix these rendered blank because
    the stroke pass used GetBoundary() (endpoints only) on a zero-area line."""
    png = _render(wkb_factory())
    assert _max_alpha(png) > 0, "line geometry rendered blank (GetBoundary regression)"


def test_polygon_still_paints_under_default_style():
    """Guard: the polygon ring-boundary stroke path must still paint."""
    png = _render(_wkb_polygon_4326())
    assert _max_alpha(png) > 0, "polygon rendered blank"
