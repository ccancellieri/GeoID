"""Regression tests for WFS GML geometry serialization (#1646).

The unified streaming read path yields geometry as GeoJSON and no longer
populates the legacy ``geom_gml`` (``ST_AsGML``) column, so the WFS GML generator
must build GML from the GeoJSON geometry itself. Before the fix, the geometry
fell through the generic attribute loop and rendered as a Python ``dict`` repr
(``{'type': 'Point', 'coordinates': (...)}``), which no WFS/QGIS client can parse.

These tests need no DB harness — they exercise the pure converter and the
generator's per-feature rendering directly.
"""

from typing import Any, Optional
from xml.etree.ElementTree import tostring

from pydantic import BaseModel

from dynastore.extensions.wfs import wfs_generator
from dynastore.extensions.wfs.wfs_generator import geojson_geometry_to_gml

GML_NS = "http://www.opengis.net/gml/3.2"


def _gml_text(geom: dict) -> str:
    el = geojson_geometry_to_gml(geom)
    assert el is not None
    return tostring(el, encoding="unicode")


def test_point_uses_pos_and_short_srs_with_xy_order():
    xml = _gml_text({"type": "Point", "coordinates": [12.4964, 41.9028]})
    assert f'<gml:Point xmlns:gml="{GML_NS}" srsName="EPSG:4326">' in xml
    # X Y (lon lat) order, matching ST_AsGML(3, geom, 15, 6) short-CRS output.
    assert "<gml:pos>12.4964 41.9028</gml:pos>" in xml


def test_linestring_uses_poslist():
    xml = _gml_text({"type": "LineString", "coordinates": [[0, 0], [1, 1], [2, 3]]})
    assert "<gml:LineString" in xml
    assert "<gml:posList>0 0 1 1 2 3</gml:posList>" in xml


def test_polygon_exterior_and_interior_rings():
    xml = _gml_text(
        {
            "type": "Polygon",
            "coordinates": [
                [[0, 0], [4, 0], [4, 4], [0, 4], [0, 0]],
                [[1, 1], [2, 1], [2, 2], [1, 1]],
            ],
        }
    )
    assert "<gml:exterior><gml:LinearRing><gml:posList>0 0 4 0 4 4 0 4 0 0" in xml
    assert "<gml:interior><gml:LinearRing><gml:posList>1 1 2 1 2 2 1 1" in xml


def test_multipoint_multicurve_multisurface_tags():
    assert "<gml:MultiPoint" in _gml_text(
        {"type": "MultiPoint", "coordinates": [[1, 2], [3, 4]]}
    )
    # GeoJSON MultiLineString -> GML 3.2 MultiCurve; MultiPolygon -> MultiSurface.
    assert "<gml:MultiCurve" in _gml_text(
        {"type": "MultiLineString", "coordinates": [[[0, 0], [1, 1]], [[2, 2], [3, 3]]]}
    )
    assert "<gml:MultiSurface" in _gml_text(
        {"type": "MultiPolygon", "coordinates": [[[[0, 0], [1, 0], [1, 1], [0, 0]]]]}
    )


def test_geometrycollection_uses_multigeometry():
    xml = _gml_text(
        {
            "type": "GeometryCollection",
            "geometries": [{"type": "Point", "coordinates": [5, 6]}],
        }
    )
    assert "<gml:MultiGeometry" in xml
    assert "<gml:geometryMember>" in xml


def test_unsupported_or_malformed_returns_none():
    assert geojson_geometry_to_gml({"type": "Weird", "coordinates": [1, 2]}) is None
    assert geojson_geometry_to_gml({"type": "Point"}) is None  # no coordinates
    assert geojson_geometry_to_gml(None) is None
    assert geojson_geometry_to_gml("not-a-geom") is None


class _GeoModel(BaseModel):
    """A pydantic geometry model, mirroring how the streaming path exposes it."""

    type: str
    coordinates: Any


class _StreamedFeature(BaseModel):
    """Minimal stand-in for the streamed feature object the GML generator sees."""

    geoid: Optional[str] = None
    id: Optional[str] = None
    type: str = "Feature"
    collection: Optional[str] = None
    geometry: Any = None
    properties: dict = {}


def _render(feature: _StreamedFeature) -> str:
    return wfs_generator.create_feature_collection_response(
        [feature],
        "cat",
        "cities",
        "http://example.org/wfs/cat",
        number_matched=1,
        number_returned=1,
    )


def test_generator_emits_gml_geometry_not_dict_repr_for_dict_geometry():
    feat = _StreamedFeature(
        geoid="abc",
        id="abc",
        collection="cities",
        geometry={"type": "Point", "coordinates": [12.4964, 41.9028]},
        properties={"name": "Rome", "population": 2873000},
    )
    xml = _render(feat)
    # Geometry rendered as real GML inside the namespaced <...:geom> wrapper.
    assert "<gml:Point" in xml
    assert "12.4964 41.9028" in xml
    # The dict-repr leak (#1646) must be gone: no raw <...:geometry> text element.
    assert ":geometry>" not in xml
    assert "'coordinates'" not in xml
    # User properties still render.
    assert ">Rome<" in xml


def test_generator_handles_pydantic_geometry_model():
    feat = _StreamedFeature(
        geoid="def",
        id="def",
        collection="cities",
        geometry=_GeoModel(type="Point", coordinates=[9.19, 45.46]),
        properties={"name": "Milan"},
    )
    xml = _render(feat)
    assert "<gml:Point" in xml
    assert "9.19 45.46" in xml
    assert "'coordinates'" not in xml
