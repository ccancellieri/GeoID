"""Tests for ``schema_from_gdalinfo`` — derive an items_schema from a stored
``gdalinfo`` blob, with no OGR re-open (pure dict processing, no libgdal)."""

from __future__ import annotations

import pytest

from dynastore.models.protocols.field_definition import FieldCapability, FieldDefinition
from dynastore.tasks.ingestion.schema_from_gdalinfo import (
    derive_schema_from_gdalinfo,
    field_definition_from_ogr_names,
    geometry_field_definition,
    merge_derived_fields,
)


def _blob(fields, *, geometry_type="Point", layer_name="roads", extra_layers=()):
    layers = [{"name": layer_name, "geometryType": geometry_type, "fields": fields}]
    layers.extend(extra_layers)
    return {"driverShortName": "GPKG", "layers": layers}


# ---------------------------------------------------------------------------
# derive_schema_from_gdalinfo — happy path
# ---------------------------------------------------------------------------

def test_geometry_first_then_canonical_fields() -> None:
    out = derive_schema_from_gdalinfo(_blob([
        {"name": "road_id", "type": "String"},
        {"name": "lanes", "type": "Integer"},
        {"name": "length_m", "type": "Real"},
        {"name": "osm_id", "type": "Integer64"},
    ]))
    assert list(out.keys()) == ["geometry", "road_id", "lanes", "length_m", "osm_id"]
    assert out["geometry"].data_type == "geometry"
    assert out["road_id"].data_type == "string"
    assert out["lanes"].data_type == "integer"
    assert out["length_m"].data_type == "double"
    # 64-bit int stays bigint (no narrowing) — the headline #1216 fix.
    assert out["osm_id"].data_type == "bigint"


def test_dates_and_times_stay_distinct() -> None:
    out = derive_schema_from_gdalinfo(_blob([
        {"name": "d", "type": "Date"},
        {"name": "t", "type": "Time"},
        {"name": "ts", "type": "DateTime"},
    ]))
    assert out["d"].data_type == "date"
    assert out["t"].data_type == "time"
    assert out["ts"].data_type == "timestamp"


def test_subtype_promotes_base_type() -> None:
    out = derive_schema_from_gdalinfo(_blob([
        {"name": "is_paved", "type": "Integer", "subtype": "Boolean"},
        {"name": "props", "type": "String", "subtype": "JSON"},
        {"name": "rec_id", "type": "String", "subtype": "UUID"},
    ]))
    assert (out["is_paved"].data_type, out["is_paved"].subtype) == ("boolean", "boolean")
    assert (out["props"].data_type, out["props"].subtype) == ("jsonb", "json")
    assert (out["rec_id"].data_type, out["rec_id"].subtype) == ("uuid", "uuid")


def test_narrowing_subtype_keeps_base_records_subtype() -> None:
    out = derive_schema_from_gdalinfo(_blob([
        {"name": "small", "type": "Integer", "subtype": "Int16"},
        {"name": "single", "type": "Real", "subtype": "Float32"},
    ]))
    assert (out["small"].data_type, out["small"].subtype) == ("integer", "int16")
    assert (out["single"].data_type, out["single"].subtype) == ("double", "float32")


def test_list_types_collapse_to_jsonb() -> None:
    out = derive_schema_from_gdalinfo(_blob([
        {"name": "tags", "type": "StringList"},
        {"name": "codes", "type": "IntegerList"},
    ]))
    assert out["tags"].data_type == "jsonb"
    assert out["codes"].data_type == "jsonb"


def test_unknown_type_defaults_to_string() -> None:
    out = derive_schema_from_gdalinfo(_blob([{"name": "weird", "type": "Frobnicate"}]))
    assert out["weird"].data_type == "string"


def test_field_missing_name_is_skipped() -> None:
    out = derive_schema_from_gdalinfo(_blob([
        {"type": "String"},                 # no name → skipped
        {"name": "keep", "type": "String"},
    ]))
    assert "keep" in out
    assert list(out.keys()) == ["geometry", "keep"]


def test_geometry_type_recorded_in_description() -> None:
    out = derive_schema_from_gdalinfo(_blob([], geometry_type="3D Multi Polygon"))
    assert list(out.keys()) == ["geometry"]
    assert "3D Multi Polygon" in out["geometry"].description


# ---------------------------------------------------------------------------
# Layer selection
# ---------------------------------------------------------------------------

def test_first_layer_wins_when_layer_name_none() -> None:
    blob = _blob(
        [{"name": "a", "type": "String"}],
        layer_name="layer_a",
        extra_layers=[{"name": "layer_b", "geometryType": "Point",
                       "fields": [{"name": "b", "type": "Integer"}]}],
    )
    out = derive_schema_from_gdalinfo(blob)
    assert "a" in out and "b" not in out


def test_named_layer_is_selected() -> None:
    blob = _blob(
        [{"name": "a", "type": "String"}],
        layer_name="layer_a",
        extra_layers=[{"name": "layer_b", "geometryType": "Point",
                       "fields": [{"name": "b", "type": "Integer"}]}],
    )
    out = derive_schema_from_gdalinfo(blob, layer_name="layer_b")
    assert "b" in out and "a" not in out


def test_named_layer_not_found_raises() -> None:
    with pytest.raises(RuntimeError, match="Layer 'missing' not found"):
        derive_schema_from_gdalinfo(_blob([{"name": "a", "type": "String"}]),
                                    layer_name="missing")


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("blob", [None, {}, {"layers": []}, {"layers": None}])
def test_no_layers_raises(blob) -> None:
    with pytest.raises(RuntimeError, match="no layers"):
        derive_schema_from_gdalinfo(blob)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def test_field_definition_from_ogr_names_carries_description() -> None:
    fd = field_definition_from_ogr_names("count", "Integer64")
    assert fd.name == "count"
    assert fd.data_type == "bigint"
    assert fd.subtype is None
    assert "OGR type: Integer64" in fd.description


def test_field_definition_from_ogr_names_notes_subtype() -> None:
    fd = field_definition_from_ogr_names("flag", "Integer", "Boolean")
    assert (fd.data_type, fd.subtype) == ("boolean", "boolean")
    assert "subtype: Boolean" in fd.description


def test_geometry_field_definition_without_type() -> None:
    fd = geometry_field_definition()
    assert fd.name == "geometry"
    assert fd.data_type == "geometry"
    assert "geometry" in fd.description.lower()


# ---------------------------------------------------------------------------
# merge_derived_fields — preserve admin tuning, never silently drop
# ---------------------------------------------------------------------------

def test_merge_into_empty_schema_adds_everything() -> None:
    derived = derive_schema_from_gdalinfo(_blob([{"name": "a", "type": "String"}]))
    merged, summary = merge_derived_fields({}, derived)
    assert set(merged) == {"geometry", "a"}
    assert summary["added"] == ["a", "geometry"]
    assert summary["updated"] == [] and summary["preserved"] == []


def test_merge_preserves_admin_tuning_and_updates_type() -> None:
    # Admin tuned "lanes": forced a column, FILTERABLE, required, custom title.
    current = {
        "lanes": FieldDefinition(
            name="lanes", data_type="string", materialize=True, required=True,
            capabilities=[FieldCapability.FILTERABLE], title="Lane count",
            description="hand-written",
        ),
    }
    # The asset says lanes is really an integer.
    derived = {"lanes": field_definition_from_ogr_names("lanes", "Integer")}
    merged, summary = merge_derived_fields(current, derived)

    out = merged["lanes"]
    assert out.data_type == "integer"          # taken from derivation
    assert out.materialize is True             # admin tuning preserved
    assert out.required is True
    assert out.capabilities == [FieldCapability.FILTERABLE]
    assert out.title == "Lane count"
    assert out.description == "hand-written"   # not overwritten
    assert summary["updated"] == ["lanes"]


def test_merge_same_type_is_unchanged() -> None:
    current = {"name": FieldDefinition(name="name", data_type="string", materialize=True)}
    derived = {"name": field_definition_from_ogr_names("name", "String")}
    merged, summary = merge_derived_fields(current, derived)
    assert summary["unchanged"] == ["name"]
    assert summary["updated"] == []
    assert merged["name"].materialize is True


def test_merge_keeps_fields_absent_from_asset() -> None:
    current = {
        "legacy": FieldDefinition(name="legacy", data_type="string", materialize=True),
    }
    derived = {"new_col": field_definition_from_ogr_names("new_col", "Integer")}
    merged, summary = merge_derived_fields(current, derived)
    assert "legacy" in merged                 # never auto-dropped
    assert merged["legacy"].materialize is True
    assert summary["preserved"] == ["legacy"]
    assert summary["added"] == ["new_col"]


def test_merge_subtype_change_counts_as_update() -> None:
    current = {"flag": FieldDefinition(name="flag", data_type="integer")}
    derived = {"flag": field_definition_from_ogr_names("flag", "Integer", "Boolean")}
    merged, summary = merge_derived_fields(current, derived)
    assert (merged["flag"].data_type, merged["flag"].subtype) == ("boolean", "boolean")
    assert summary["updated"] == ["flag"]
