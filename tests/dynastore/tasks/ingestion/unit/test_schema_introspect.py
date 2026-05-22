"""Tests for ``extract_ogr_schema`` — derives a ItemsSchema from a vector source.

Covers the OGR-type → canonical-data_type mapping (incl. OGR subtypes), layer
selection, the always-present geometry field, and error handling.
Mocks ``gdal.OpenEx`` + the OGR layer/field-defn objects so the tests
don't need actual GDAL files.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


pytest.importorskip("osgeo")  # whole module is gated on libgdal availability

from osgeo import ogr  # noqa: E402

from dynastore.tasks.ingestion.schema_introspect import (  # noqa: E402
    _map_ogr_type,
    extract_ogr_schema,
)


def _mock_field_defn(
    name: str, ogr_type_name: str, subtype: int = ogr.OFSTNone,
) -> MagicMock:
    fd = MagicMock()
    fd.GetName.return_value = name
    # Carry the type name on GetType() so the GetFieldTypeName patch below
    # can be a stable identity lambda — avoids brittle call-count math when
    # extract_ogr_schema invokes GetFieldTypeName more than once per field.
    fd.GetType.return_value = ogr_type_name
    fd.GetSubType.return_value = subtype
    return fd


def _mock_layer_defn(field_specs: list[tuple[str, str]]) -> MagicMock:
    """Build a mock layer-defn with ``[(name, ogr_type_name), ...]``."""
    layer_defn = MagicMock()
    field_defns = [_mock_field_defn(n, t) for n, t in field_specs]
    layer_defn.GetFieldCount.return_value = len(field_defns)
    layer_defn.GetFieldDefn.side_effect = lambda i: field_defns[i]
    return layer_defn


def _mock_layer(field_specs: list[tuple[str, str]], name: str = "roads") -> MagicMock:
    layer = MagicMock()
    layer.GetName.return_value = name
    # A real geometry-type int so the (unpatched) ogr.GeometryTypeToName call
    # in extract_ogr_schema resolves to a real label.
    layer.GetGeomType.return_value = ogr.wkbUnknown
    layer.GetLayerDefn.return_value = _mock_layer_defn(field_specs)
    return layer


def _mock_dataset(layers: list[MagicMock]) -> MagicMock:
    ds = MagicMock()
    ds.GetLayerCount.return_value = len(layers)
    ds.GetLayer.side_effect = lambda i: layers[i]
    ds.GetLayerByName.side_effect = lambda n: next(
        (lyr for lyr in layers if lyr.GetName() == n), None,
    )
    return ds


# ---------------------------------------------------------------------------
# _map_ogr_type — covers every base OGR type (canonical, no subtype)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "ogr_type_name, expected",
    [
        ("Integer",       ("integer", None)),
        ("Integer64",     ("bigint", None)),
        ("Real",          ("double", None)),
        ("String",        ("string", None)),
        ("Date",          ("date", None)),
        ("Time",          ("time", None)),
        ("DateTime",      ("timestamp", None)),
        ("Binary",        ("binary", None)),
        ("IntegerList",   ("jsonb", None)),
        ("Integer64List", ("jsonb", None)),
        ("RealList",      ("jsonb", None)),
        ("StringList",    ("jsonb", None)),
    ],
)
def test_ogr_type_mapping(ogr_type_name: str, expected: tuple) -> None:
    fd = MagicMock()
    fd.GetSubType.return_value = ogr.OFSTNone
    with patch(
        "dynastore.tasks.ingestion.schema_introspect.ogr.GetFieldTypeName",
        return_value=ogr_type_name,
    ):
        assert _map_ogr_type(fd) == expected


# ---------------------------------------------------------------------------
# _map_ogr_type — OGR subtypes (Boolean/JSON/UUID promote the base type;
# Int16/Float32 keep their base but record the subtype)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "ogr_type_name, ogr_subtype, expected",
    [
        ("Integer", ogr.OFSTBoolean, ("boolean", "boolean")),
        ("Integer", ogr.OFSTInt16,   ("integer", "int16")),
        ("Real",    ogr.OFSTFloat32, ("double", "float32")),
        ("String",  ogr.OFSTJSON,    ("jsonb", "json")),
        ("String",  ogr.OFSTUUID,    ("uuid", "uuid")),
    ],
)
def test_ogr_subtype_mapping(ogr_type_name, ogr_subtype, expected) -> None:
    fd = MagicMock()
    fd.GetSubType.return_value = ogr_subtype
    with patch(
        "dynastore.tasks.ingestion.schema_introspect.ogr.GetFieldTypeName",
        return_value=ogr_type_name,
    ):
        # uses the real ogr.GetFieldSubTypeName for the subtype label
        assert _map_ogr_type(fd) == expected


def test_unknown_ogr_type_falls_back_to_string() -> None:
    """An OGR type the table doesn't know defaults to ``string``
    (safe choice — every backend can store text)."""
    fd = MagicMock()
    fd.GetSubType.return_value = ogr.OFSTNone
    with patch(
        "dynastore.tasks.ingestion.schema_introspect.ogr.GetFieldTypeName",
        return_value="SomeNewOGRType",
    ):
        assert _map_ogr_type(fd) == ("string", None)


# ---------------------------------------------------------------------------
# extract_ogr_schema — happy path + edge cases
# ---------------------------------------------------------------------------

def test_extract_schema_yields_geometry_first_then_fields() -> None:
    layer = _mock_layer([
        ("road_id", "String"),
        ("lanes",   "Integer"),
    ])
    ds = _mock_dataset([layer])

    with patch(
        "dynastore.tasks.ingestion.schema_introspect.gdal.OpenEx",
        return_value=ds,
    ), patch(
        "dynastore.tasks.ingestion.schema_introspect.ogr.GetFieldTypeName",
        side_effect=lambda t: t,  # GetType() returns the type name verbatim
    ):
        out = extract_ogr_schema("/tmp/roads.shp")

    assert list(out.keys()) == ["geometry", "road_id", "lanes"]
    assert out["geometry"].data_type == "geometry"
    assert out["road_id"].data_type == "string"
    assert out["lanes"].data_type == "integer"


def test_extract_schema_records_subtype() -> None:
    layer = _mock_layer([("is_paved", "Integer")])
    # promote the single field to an OGR Boolean subtype
    layer.GetLayerDefn().GetFieldDefn(0).GetSubType.return_value = ogr.OFSTBoolean
    ds = _mock_dataset([layer])

    with patch(
        "dynastore.tasks.ingestion.schema_introspect.gdal.OpenEx",
        return_value=ds,
    ), patch(
        "dynastore.tasks.ingestion.schema_introspect.ogr.GetFieldTypeName",
        side_effect=lambda t: t,
    ):
        out = extract_ogr_schema("/tmp/roads.shp")

    assert out["is_paved"].data_type == "boolean"
    assert out["is_paved"].subtype == "boolean"


def test_extract_schema_picks_first_layer_when_layer_name_none() -> None:
    layer1 = _mock_layer([("a", "String")], name="layer_a")
    layer2 = _mock_layer([("b", "Integer")], name="layer_b")
    ds = _mock_dataset([layer1, layer2])

    with patch(
        "dynastore.tasks.ingestion.schema_introspect.gdal.OpenEx",
        return_value=ds,
    ), patch(
        "dynastore.tasks.ingestion.schema_introspect.ogr.GetFieldTypeName",
        return_value="String",
    ):
        out = extract_ogr_schema("/tmp/multi.gpkg")

    # Picked layer_a, so only field "a" appears
    assert "a" in out
    assert "b" not in out


def test_extract_schema_selects_named_layer() -> None:
    layer1 = _mock_layer([("a", "String")], name="layer_a")
    layer2 = _mock_layer([("b", "Integer")], name="layer_b")
    ds = _mock_dataset([layer1, layer2])

    with patch(
        "dynastore.tasks.ingestion.schema_introspect.gdal.OpenEx",
        return_value=ds,
    ), patch(
        "dynastore.tasks.ingestion.schema_introspect.ogr.GetFieldTypeName",
        return_value="Integer",
    ):
        out = extract_ogr_schema("/tmp/multi.gpkg", layer_name="layer_b")

    assert "b" in out
    assert "a" not in out


def test_extract_schema_geometry_only_layer() -> None:
    """Layer with no fields beyond geometry still produces a schema
    (just the geometry entry)."""
    layer = _mock_layer([], name="just_geom")
    ds = _mock_dataset([layer])

    with patch(
        "dynastore.tasks.ingestion.schema_introspect.gdal.OpenEx",
        return_value=ds,
    ):
        out = extract_ogr_schema("/tmp/blank.shp")

    assert list(out.keys()) == ["geometry"]


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

def test_open_failure_raises() -> None:
    with patch(
        "dynastore.tasks.ingestion.schema_introspect.gdal.OpenEx",
        return_value=None,
    ):
        with pytest.raises(RuntimeError, match="OGR could not open"):
            extract_ogr_schema("/tmp/nonexistent.shp")


def test_named_layer_not_found_raises() -> None:
    layer = _mock_layer([("a", "String")], name="real_layer")
    ds = _mock_dataset([layer])

    with patch(
        "dynastore.tasks.ingestion.schema_introspect.gdal.OpenEx",
        return_value=ds,
    ):
        with pytest.raises(RuntimeError, match="Layer 'missing_layer' not found"):
            extract_ogr_schema("/tmp/x.gpkg", layer_name="missing_layer")


def test_zero_layer_dataset_raises() -> None:
    ds = _mock_dataset([])
    with patch(
        "dynastore.tasks.ingestion.schema_introspect.gdal.OpenEx",
        return_value=ds,
    ):
        with pytest.raises(RuntimeError, match="has no layers"):
            extract_ogr_schema("/tmp/empty.gpkg")


# ---------------------------------------------------------------------------
# FieldDefinition shape — derived entries match the model contract
# ---------------------------------------------------------------------------

def test_derived_field_definitions_carry_descriptions() -> None:
    layer = _mock_layer([("name", "String")])
    ds = _mock_dataset([layer])

    with patch(
        "dynastore.tasks.ingestion.schema_introspect.gdal.OpenEx",
        return_value=ds,
    ), patch(
        "dynastore.tasks.ingestion.schema_introspect.ogr.GetFieldTypeName",
        return_value="String",
    ):
        out = extract_ogr_schema("/tmp/x.shp")

    assert "name" in out["name"].description
    assert "OGR type: String" in out["name"].description
    # Geometry field also has a description
    assert "geometry" in out["geometry"].description.lower()
