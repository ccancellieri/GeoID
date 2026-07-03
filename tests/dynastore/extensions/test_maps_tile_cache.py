"""Unit tests for the optional MVT-cache fast path behind vector ``/map``.

The full cache-hit → decode → render chain is exercised end-to-end elsewhere;
here we pin the two pieces that are self-contained and load-bearing:

- ``_decode_tiles_to_wkb`` must yield geometries in EPSG:3857 *world*
  coordinates (via the GDAL OGR MVT driver's z/x/y georeferencing), not
  tile-local 0..extent coordinates — that is what lets the renderer reproject
  them like any PostGIS geometry.
- ``try_mvt_cache_layers`` must fall back (return None) whenever the tiles
  module / cache provider is not available, so OGC behaviour is preserved.
"""

import pytest

osgeo = pytest.importorskip("osgeo")
from osgeo import gdal, ogr, osr  # noqa: E402

from dynastore.extensions.maps import maps_tile_cache as mtc  # noqa: E402
from dynastore.extensions.maps.renderer import reproject_bbox_epsg  # noqa: E402

# EPSG:3857 world half-extent (WebMercatorQuad).
_FULL = 20037508.342789244


def _write_single_mvt_tile(z: int, x: int, y: int) -> bytes:
    """Write one MVT tile whose single polygon sits at the tile centre, return its bytes."""
    span = 2 * _FULL / (2 ** z)
    left = -_FULL + x * span
    top = _FULL - y * span
    cx, cy = left + span / 2, top - span / 2
    d = span / 4

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(3857)
    mem = gdal.GetDriverByName("MEM").Create("", 0, 0, 0, gdal.GDT_Unknown)
    lyr = mem.CreateLayer("testlayer", srs=srs, geom_type=ogr.wkbPolygon)
    feat = ogr.Feature(lyr.GetLayerDefn())
    feat.SetGeometry(
        ogr.CreateGeometryFromWkt(
            f"POLYGON(({cx-d} {cy-d},{cx+d} {cy-d},{cx+d} {cy+d},{cx-d} {cy+d},{cx-d} {cy-d}))"
        )
    )
    lyr.CreateFeature(feat)

    outdir = f"/vsimem/mvt_{z}_{x}_{y}"
    gdal.VectorTranslate(
        outdir, mem, format="MVT",
        datasetCreationOptions=[f"MINZOOM={z}", f"MAXZOOM={z}", "COMPRESS=NO"],
    )
    tile_path = f"{outdir}/{z}/{x}/{y}.pbf"
    f = gdal.VSIFOpenL(tile_path, "rb")
    gdal.VSIFSeekL(f, 0, 2)
    size = gdal.VSIFTellL(f)
    gdal.VSIFSeekL(f, 0, 0)
    data = gdal.VSIFReadL(1, size, f)
    gdal.VSIFCloseL(f)
    gdal.RmdirRecursive(outdir)
    return bytes(data)


def test_decode_tiles_to_wkb_yields_world_coordinates():
    z, x, y = 4, 8, 8
    blob = _write_single_mvt_tile(z, x, y)

    layers = mtc._decode_tiles_to_wkb([(z, x, y, blob)])

    assert layers, "decode produced no features"
    assert isinstance(layers[0]["geom"], (bytes, bytearray))
    geom = ogr.CreateGeometryFromWkb(layers[0]["geom"])
    c = geom.Centroid()
    # Tile z4/x8/y8 centre in EPSG:3857 metres.
    span = 2 * _FULL / (2 ** z)
    exp_x = -_FULL + x * span + span / 2
    exp_y = _FULL - y * span - span / 2
    assert abs(c.GetX() - exp_x) < span, "X not georeferenced to 3857 world coords"
    assert abs(c.GetY() - exp_y) < span, "Y not georeferenced to 3857 world coords"


def test_reproject_bbox_epsg_identity_and_reprojection():
    same = reproject_bbox_epsg([-20.0, -35.0, 55.0, 40.0], 4326, 4326)
    assert same == [-20.0, -35.0, 55.0, 40.0]

    merc = reproject_bbox_epsg([0.0, 0.0, 90.0, 0.0], 4326, 3857)
    assert merc is not None
    # lon 0..90 degrees -> 0..~10018754 metres in Web Mercator.
    assert abs(merc[0]) < 1.0
    assert abs(merc[2] - 10018754.0) < 5000.0


def test_reproject_bbox_epsg_unresolvable_crs_fails_closed():
    """An unresolvable EPSG code must yield None, not raise."""
    assert reproject_bbox_epsg([-20.0, -35.0, 55.0, 40.0], 4326, 999999) is None


@pytest.mark.asyncio
async def test_try_mvt_cache_layers_falls_back_without_provider(monkeypatch):
    """No registered tile-storage provider ⇒ None (caller renders from source)."""
    monkeypatch.setattr(mtc, "_TILES_IMPORTS_OK", True)
    monkeypatch.setattr(mtc, "get_protocol", lambda _proto: None)

    result = await mtc.try_mvt_cache_layers(
        catalog_id="cat",
        collection_id="coll",
        bbox=[-20.0, -35.0, 55.0, 40.0],
        bbox_srid=4326,
        width=256,
        height=256,
    )
    assert result is None
