"""Tests for ``ItemsSchema.materialize_fields_as_columns``.

Default (False): the bridge lifts only fields carrying required/unique
constraints — plain fields stay in JSONB properties.

When True: ALL declared fields get an AttributeSchemaEntry → native PG
column, enabling per-field indexes, ANALYZE statistics, and column-store
query plans.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from dynastore.modules.storage.field_constraints import (
    bridge_schema_to_attribute_sidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeSchemaEntry,
    FeatureAttributeSidecarConfig,
    PostgresType,
)


def _field(data_type: str = "string", required: bool = False, unique: bool = False, default=None):
    fd = MagicMock()
    fd.data_type = data_type
    fd.required = required
    fd.unique = unique
    fd.default = default
    fd.description = None
    return fd


def _empty_sidecar() -> FeatureAttributeSidecarConfig:
    """Build a fresh attributes-sidecar config with an empty schema."""
    return FeatureAttributeSidecarConfig()


# ---------------------------------------------------------------------------
# Default behaviour — only constrained fields are lifted
# ---------------------------------------------------------------------------

def test_default_only_constrained_fields_lifted() -> None:
    schema = MagicMock()
    schema.materialize_fields_as_columns = False
    schema.fields = {
        "constrained": _field(required=True),
        "plain":       _field(),  # no required, no unique → stays in JSONB
    }

    bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
    names = {e.name for e in bridged.attribute_schema}
    assert names == {"constrained"}


def test_default_no_change_when_all_plain() -> None:
    """Schema with only plain fields produces no sidecar entries."""
    schema = MagicMock()
    schema.materialize_fields_as_columns = False
    schema.fields = {
        "a": _field(),
        "b": _field(),
    }
    sidecar = _empty_sidecar()
    bridged = bridge_schema_to_attribute_sidecar(schema, sidecar)
    # Identity returned — nothing changed
    assert bridged is sidecar


# ---------------------------------------------------------------------------
# materialize_fields_as_columns=True — every declared field lifts
# ---------------------------------------------------------------------------

def test_materialize_all_lifts_every_field() -> None:
    schema = MagicMock()
    schema.materialize_fields_as_columns = True
    schema.fields = {
        "road_id":  _field(data_type="string"),
        "lanes":    _field(data_type="integer"),
        "highway":  _field(data_type="string"),
    }

    bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
    names = {e.name for e in bridged.attribute_schema}
    assert names == {"road_id", "lanes", "highway"}


def test_materialize_all_preserves_pg_types() -> None:
    schema = MagicMock()
    schema.materialize_fields_as_columns = True
    schema.fields = {
        "lanes":    _field(data_type="integer"),
        "name":     _field(data_type="string"),
        "active":   _field(data_type="boolean"),
    }

    bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
    by_name = {e.name: e for e in bridged.attribute_schema}
    assert by_name["lanes"].type == PostgresType.INTEGER
    assert by_name["name"].type == PostgresType.TEXT
    assert by_name["active"].type == PostgresType.BOOLEAN


def test_materialize_all_combines_with_constraints() -> None:
    """Constrained + plain fields ALL get lifted; constraints carry through."""
    schema = MagicMock()
    schema.materialize_fields_as_columns = True
    schema.fields = {
        "id":       _field(required=True, unique=True),
        "label":    _field(data_type="string"),  # plain
    }

    bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
    by_name = {e.name: e for e in bridged.attribute_schema}
    assert set(by_name.keys()) == {"id", "label"}
    # Constrained: NOT NULL + UNIQUE
    assert by_name["id"].nullable is False
    assert by_name["id"].unique is True
    # Plain: nullable, not unique
    assert by_name["label"].nullable is True
    assert by_name["label"].unique is False


def test_materialize_all_existing_entries_kept() -> None:
    """Pre-existing AttributeSchemaEntry stays; new fields are added on top."""
    schema = MagicMock()
    schema.materialize_fields_as_columns = True
    schema.fields = {
        "existing":  _field(data_type="string"),
        "new_field": _field(data_type="integer"),
    }
    sidecar = FeatureAttributeSidecarConfig(
        attribute_schema=[AttributeSchemaEntry(
            name="existing", type=PostgresType.TEXT, nullable=True, unique=False,
        )],
    )

    bridged = bridge_schema_to_attribute_sidecar(schema, sidecar)
    names = {e.name for e in bridged.attribute_schema}
    assert names == {"existing", "new_field"}


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_materialize_all_with_empty_fields_no_op() -> None:
    schema = MagicMock()
    schema.materialize_fields_as_columns = True
    schema.fields = {}
    sidecar = _empty_sidecar()
    bridged = bridge_schema_to_attribute_sidecar(schema, sidecar)
    assert bridged is sidecar


def test_materialize_all_unknown_data_type_falls_back_to_text() -> None:
    schema = MagicMock()
    schema.materialize_fields_as_columns = True
    schema.fields = {"weird": _field(data_type="some_alien_type")}
    bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
    by_name = {e.name: e for e in bridged.attribute_schema}
    assert by_name["weird"].type == PostgresType.TEXT


# ---------------------------------------------------------------------------
# Per-field default (P6) — FieldDefinition.default threads into the sidecar
# ---------------------------------------------------------------------------

def test_default_threads_into_new_entry() -> None:
    """A field's ``default`` reaches the synthesised AttributeSchemaEntry."""
    schema = MagicMock()
    schema.materialize_fields_as_columns = True
    schema.fields = {"status": _field(data_type="string", default="active")}
    bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
    by_name = {e.name: e for e in bridged.attribute_schema}
    assert by_name["status"].default == "active"
    # And it produces a SQL DEFAULT literal.
    assert by_name["status"].get_sql_default() == "'active'"


def test_default_overlays_existing_entry_when_field_declares_one() -> None:
    """SSOT field default wins over an existing entry's silence."""
    schema = MagicMock()
    schema.materialize_fields_as_columns = True
    schema.fields = {"n": _field(data_type="integer", default=7)}
    sidecar = FeatureAttributeSidecarConfig(
        attribute_schema=[AttributeSchemaEntry(name="n", type=PostgresType.INTEGER)]
    )
    bridged = bridge_schema_to_attribute_sidecar(schema, sidecar)
    by_name = {e.name: e for e in bridged.attribute_schema}
    assert by_name["n"].default == 7


def test_existing_entry_default_preserved_when_field_silent() -> None:
    """A field that declares no default leaves the entry's own default intact."""
    schema = MagicMock()
    schema.materialize_fields_as_columns = True
    schema.fields = {"n": _field(data_type="integer", default=None)}
    sidecar = FeatureAttributeSidecarConfig(
        attribute_schema=[
            AttributeSchemaEntry(name="n", type=PostgresType.INTEGER, default=42)
        ]
    )
    bridged = bridge_schema_to_attribute_sidecar(schema, sidecar)
    by_name = {e.name: e for e in bridged.attribute_schema}
    assert by_name["n"].default == 42
