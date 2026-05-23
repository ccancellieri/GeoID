"""Unit tests for the canonical field-type vocabulary (``dynastore.models.field_types``).

Pins: the strict canonical set, the (no-legacy) rejection of unknown tokens,
parametrized-geometry passthrough, OGR (type, subtype) mapping incl. subtype
promotion, and the ``FieldDefinition`` validator wiring.
"""

from __future__ import annotations

import pytest

from dynastore.models.field_types import (
    CANONICAL_DATA_TYPES,
    CANONICAL_SUBTYPES,
    CANONICAL_TO_JSON_SCHEMA,
    CANONICAL_TO_PG_DDL,
    DataSubtype,
    DataType,
    canonical_data_type,
    normalize_subtype,
    ogr_to_canonical,
)
from dynastore.models.protocols.field_definition import FieldDefinition


def test_canonical_set_is_exactly_the_enum() -> None:
    assert CANONICAL_DATA_TYPES == {t.value for t in DataType}
    # The headline temporal trio stays distinct (date != time != timestamp).
    assert {"date", "time", "timestamp"} <= CANONICAL_DATA_TYPES


@pytest.mark.parametrize("value", sorted(CANONICAL_DATA_TYPES))
def test_canonical_tokens_pass_through(value: str) -> None:
    assert canonical_data_type(value) == value
    assert canonical_data_type(value.upper()) == value  # case-insensitive


def test_none_and_empty_default_to_string() -> None:
    assert canonical_data_type(None) == "string"
    assert canonical_data_type("   ") == "string"


def test_parametrized_geometry_preserved_lowercased() -> None:
    assert canonical_data_type("geometry(Point,4326)") == "geometry(point,4326)"
    # the SRID survives so query_optimizer's substring checks keep working
    assert "4326" in canonical_data_type("Geometry(MultiPolygon,4326)")
    assert canonical_data_type("geometry").startswith("geometry")


@pytest.mark.parametrize("bad", ["text", "float", "int", "datetime", "json", "number", "tstzrange", "box2d", "varchar"])
def test_legacy_aliases_are_rejected(bad: str) -> None:
    # No backward-compatibility layer: legacy spellings raise, not silently map.
    with pytest.raises(ValueError):
        canonical_data_type(bad)


def test_normalize_subtype() -> None:
    assert normalize_subtype(None) is None
    assert normalize_subtype("none") is None
    assert normalize_subtype("Boolean") == "boolean"
    assert normalize_subtype("FLOAT32") == "float32"
    assert normalize_subtype("not_a_subtype") is None
    assert CANONICAL_SUBTYPES == {s.value for s in DataSubtype}


@pytest.mark.parametrize(
    "ogr_type, expected",
    [
        ("Integer", ("integer", None)),
        ("Integer64", ("bigint", None)),
        ("Real", ("double", None)),
        ("String", ("string", None)),
        ("Date", ("date", None)),
        ("Time", ("time", None)),
        ("DateTime", ("timestamp", None)),
        ("Binary", ("binary", None)),
        ("StringList", ("jsonb", None)),
        ("RealList", ("jsonb", None)),
        ("Nonsense", ("string", None)),  # unknown -> string
    ],
)
def test_ogr_base_types(ogr_type: str, expected: tuple) -> None:
    assert ogr_to_canonical(ogr_type) == expected


@pytest.mark.parametrize(
    "ogr_type, ogr_subtype, expected",
    [
        ("Integer", "Boolean", ("boolean", "boolean")),   # promote
        ("String", "JSON", ("jsonb", "json")),            # promote
        ("String", "UUID", ("uuid", "uuid")),             # promote
        ("Integer", "Int16", ("integer", "int16")),       # keep base, record
        ("Real", "Float32", ("double", "float32")),       # keep base, record
    ],
)
def test_ogr_subtype_promotion(ogr_type, ogr_subtype, expected) -> None:
    assert ogr_to_canonical(ogr_type, ogr_subtype) == expected


def test_field_definition_validator_strict() -> None:
    fd = FieldDefinition(name="x", data_type="DOUBLE")
    assert fd.data_type == "double"
    assert fd.subtype is None
    fd2 = FieldDefinition(name="b", data_type="boolean", subtype="Boolean")
    assert (fd2.data_type, fd2.subtype) == ("boolean", "boolean")
    with pytest.raises(ValueError):
        FieldDefinition(name="bad", data_type="text")


def test_field_definition_geometry_param_preserved() -> None:
    fd = FieldDefinition(name="g", data_type="geometry(Point,4326)")
    assert fd.data_type == "geometry(point,4326)"


def test_canonical_to_pg_ddl_representative_mappings() -> None:
    # Pins the canonical -> PostgreSQL DDL table now co-located in the SSOT.
    assert CANONICAL_TO_PG_DDL["string"] == "TEXT"
    assert CANONICAL_TO_PG_DDL["integer"] == "INTEGER"
    assert CANONICAL_TO_PG_DDL["timestamp"] == "TIMESTAMPTZ"
    assert CANONICAL_TO_PG_DDL["double"] == "FLOAT"
    # No "geometry" key — geometry columns are not materialized via this table.
    assert "geometry" not in CANONICAL_TO_PG_DDL


def test_canonical_to_json_schema_representative_mappings() -> None:
    # Pins the canonical -> JSON-Schema fragment table now co-located in the SSOT.
    assert CANONICAL_TO_JSON_SCHEMA["string"] == {"type": "string"}
    assert CANONICAL_TO_JSON_SCHEMA["integer"] == {"type": "integer"}
    assert CANONICAL_TO_JSON_SCHEMA["timestamp"] == {
        "type": "string",
        "format": "date-time",
    }
    assert CANONICAL_TO_JSON_SCHEMA["geometry"] == {"type": "object"}


def test_co_located_tables_are_the_objects_the_use_sites_consume() -> None:
    # The storage modules must alias the SSOT objects, not redefine them.
    from dynastore.modules.storage.field_constraints import _DATA_TYPE_TO_PG_NAME
    from dynastore.modules.storage.schema_derive import _TYPE_MAP

    assert _DATA_TYPE_TO_PG_NAME is CANONICAL_TO_PG_DDL
    assert _TYPE_MAP is CANONICAL_TO_JSON_SCHEMA
