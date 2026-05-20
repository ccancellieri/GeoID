from dynastore.models.protocols.field_definition import FieldDefinition
from dynastore.modules.storage.schema_derive import derive_wire_schema

DRAFT = "https://json-schema.org/draft/2020-12/schema"


def test_empty_fields_returns_none():
    assert derive_wire_schema({}) is None
    assert derive_wire_schema(None) is None  # type: ignore[arg-type]


def test_non_strict_omits_additional_properties():
    schema = derive_wire_schema({"a": FieldDefinition(name="a", data_type="text")})
    assert schema is not None
    assert schema["$schema"] == DRAFT
    assert schema["type"] == "object"
    assert "additionalProperties" not in schema


def test_strict_adds_additional_properties_false():
    schema = derive_wire_schema(
        {"a": FieldDefinition(name="a", data_type="text")}, strict=True
    )
    assert schema is not None
    assert schema["additionalProperties"] is False


def test_required_list_uses_dict_keys_and_is_sorted():
    fields = {
        "zeta": FieldDefinition(name="zeta_field", data_type="text", required=True),
        "alpha": FieldDefinition(name="alpha_field", data_type="text", required=True),
        "beta": FieldDefinition(name="beta_field", data_type="text", required=False),
    }
    schema = derive_wire_schema(fields)
    assert schema is not None
    assert schema["required"] == ["alpha", "zeta"]
    # property names also come from the dict key, not fd.name
    assert set(schema["properties"]) == {"zeta", "alpha", "beta"}


def test_data_type_mapping():
    fields = {
        "s": FieldDefinition(name="s", data_type="text"),
        "i": FieldDefinition(name="i", data_type="integer"),
        "f": FieldDefinition(name="f", data_type="float"),
        "b": FieldDefinition(name="b", data_type="boolean"),
        "t": FieldDefinition(name="t", data_type="timestamp"),
        "g": FieldDefinition(name="g", data_type="geometry"),
    }
    props = derive_wire_schema(fields)["properties"]  # type: ignore[index]
    assert props["s"] == {"type": "string"}
    assert props["i"] == {"type": "integer"}
    assert props["f"] == {"type": "number"}
    assert props["b"] == {"type": "boolean"}
    assert props["t"] == {"type": "string", "format": "date-time"}
    assert props["g"] == {"type": "object"}


def test_unknown_and_case_insensitive_default_to_string():
    fields = {
        "u": FieldDefinition(name="u", data_type="something_weird"),
        "c": FieldDefinition(name="c", data_type="TEXT"),
    }
    props = derive_wire_schema(fields)["properties"]  # type: ignore[index]
    assert props["u"] == {"type": "string"}
    assert props["c"] == {"type": "string"}


def test_validator_pass_through():
    fields = {
        "name": FieldDefinition(name="name", data_type="text", max_length=50, pattern="^[a-z]+$"),
        "score": FieldDefinition(name="score", data_type="float", minimum=0.0, maximum=100.0),
        "kind": FieldDefinition(name="kind", data_type="text", enum=["a", "b", "c"]),
    }
    props = derive_wire_schema(fields)["properties"]  # type: ignore[index]
    assert props["name"]["maxLength"] == 50
    assert props["name"]["pattern"] == "^[a-z]+$"
    assert props["score"]["minimum"] == 0.0
    assert props["score"]["maximum"] == 100.0
    assert props["kind"]["enum"] == ["a", "b", "c"]


def test_field_format_overrides_timestamp_default():
    fields = {
        "ts": FieldDefinition(name="ts", data_type="timestamp", format="date"),
        "raw": FieldDefinition(name="raw", data_type="text", format="email"),
    }
    props = derive_wire_schema(fields)["properties"]  # type: ignore[index]
    assert props["ts"]["format"] == "date"
    assert props["raw"]["format"] == "email"


# ── purpose="write" variant ─────────────────────────────────────────────
# The write variant feeds the write-path JSON-Schema validator. It must
# carry value constraints (type/enum/min/max/maxLength/pattern/format) but
# NOT structural constraints already owned by other write-path checks:
# - ``additionalProperties`` is owned by the strict-unknown-fields check
# - ``required`` is owned by NOT NULL DDL (PG) / the ``check_required``
#   app-level fallback (ES)


def test_write_purpose_omits_additional_properties_even_when_strict():
    fields = {"a": FieldDefinition(name="a", data_type="text")}
    schema = derive_wire_schema(fields, strict=True, purpose="write")
    assert schema is not None
    # strict would set additionalProperties=False on the read variant, but the
    # write variant must never do so (would double-422 with the unknown-fields
    # check).
    assert "additionalProperties" not in schema


def test_write_purpose_omits_required():
    fields = {
        "name": FieldDefinition(name="name", data_type="text", required=True),
        "score": FieldDefinition(name="score", data_type="float", required=True),
    }
    schema = derive_wire_schema(fields, purpose="write")
    assert schema is not None
    # required is enforced by NOT NULL DDL / app-level fallback, not here.
    assert "required" not in schema


def test_write_purpose_keeps_value_constraints():
    fields = {
        "name": FieldDefinition(
            name="name", data_type="text", max_length=50, pattern="^[a-z]+$"
        ),
        "score": FieldDefinition(
            name="score", data_type="float", minimum=0.0, maximum=100.0
        ),
        "kind": FieldDefinition(name="kind", data_type="text", enum=["a", "b", "c"]),
    }
    schema = derive_wire_schema(fields, purpose="write")
    assert schema is not None
    props = schema["properties"]
    assert props["name"]["type"] == "string"
    assert props["name"]["maxLength"] == 50
    assert props["name"]["pattern"] == "^[a-z]+$"
    assert props["score"]["minimum"] == 0.0
    assert props["score"]["maximum"] == 100.0
    assert props["kind"]["enum"] == ["a", "b", "c"]


def test_write_purpose_returns_none_for_empty_fields():
    assert derive_wire_schema({}, purpose="write") is None


def test_read_purpose_is_default_and_unchanged():
    fields = {"a": FieldDefinition(name="a", data_type="text", required=True)}
    default = derive_wire_schema(fields, strict=True)
    explicit_read = derive_wire_schema(fields, strict=True, purpose="read")
    assert default == explicit_read
    # the read variant keeps both required and additionalProperties
    assert default["required"] == ["a"]  # type: ignore[index]
    assert default["additionalProperties"] is False  # type: ignore[index]
