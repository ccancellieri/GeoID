"""Renderer regression: the request bbox must be reprojected into the render CRS.

A collection map requested with a CRS84/degree bbox but the default EPSG:3857
(metre) render CRS previously produced a fully transparent PNG: the geotransform
window was built from the raw degree numbers interpreted as metres (a tiny box
near 0,0), so every reprojected feature fell off-canvas. These tests pin that the
window now tracks the geometry regardless of the bbox-vs-render CRS combination.
"""

import pytest

osgeo = pytest.importorskip("osgeo")
from osgeo import gdal, ogr  # noqa: E402

from dynastore.extensions.maps.renderer import render_map_image  # noqa: E402


def _wkb_polygon_4326() -> bytes:
    """A polygon well inside the Africa test bbox, in EPSG:4326.

    Kept clear of (0, 0) on purpose: without the bbox reprojection the render
    window collapses to a metres-sized box at Null Island, and a polygon
    touching the origin would still paint a sliver there — masking the bug. An
    off-origin polygon renders blank pre-fix and painted post-fix.
    """
    ring = ogr.Geometry(ogr.wkbLinearRing)
    for x, y in [(10, 5), (40, 5), (40, 30), (10, 30), (10, 5)]:
        ring.AddPoint_2D(float(x), float(y))
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    return poly.ExportToWkb()


def _max_alpha(png_bytes: bytes) -> float:
    """Return the maximum alpha-band value of a rendered PNG (0 == fully blank)."""
    mem_path = "/vsimem/_maps_bbox_crs_test.png"
    gdal.FileFromMemBuffer(mem_path, png_bytes)
    try:
        ds = gdal.Open(mem_path)
        assert ds is not None
        # Band 4 is alpha (RGBA raster created by render_map_image).
        _, mx = ds.GetRasterBand(4).ComputeRasterMinMax(False)
        return mx
    finally:
        gdal.Unlink(mem_path)


# Africa-ish window in CRS84 degrees, mirroring the reported demo URL.
_BBOX_DEGREES = [-20.0, -35.0, 55.0, 40.0]


@pytest.mark.parametrize("render_crs", ["EPSG:3857", "EPSG:4326"])
def test_degree_bbox_renders_features_in_any_render_crs(render_crs):
    """A degree (CRS84) bbox must paint features whether the render CRS is
    metres (3857) or degrees (4326). The 3857 case is the regression: before the
    bbox was reprojected, it produced a transparent image."""
    png = render_map_image(
        256,
        256,
        list(_BBOX_DEGREES),
        render_crs,
        4326,  # source_srid: geometry stored in EPSG:4326
        [{"geom": _wkb_polygon_4326()}],
        None,  # default style (random fill + black stroke)
        True,  # transparent background
        None,  # bgcolor
        4326,  # bbox_srid: bbox given in CRS84 degrees
    )
    assert _max_alpha(png) > 0, f"blank render for render CRS {render_crs}"
