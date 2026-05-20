"""Unit tests for per-field ``materialize`` + capability-driven sidecar columns.

Covers the five-rule precedence in ``bridge_schema_to_attribute_sidecar``:

  Rule 1 — hard constraint (unique + required): COLUMN regardless of ``materialize``
  Rule 2 — ``materialize=True``:  COLUMN
  Rule 3 — ``materialize=False``: JSONB (do NOT lift), even when capabilities present
  Rule 4 — ``materialize=None``, no capabilities → JSONB (unless materialize_all)
  Rule 5 — ``materialize=None``, column-implying capability → COLUMN

The existing ``AttributeSchemaEntry`` overlay path is checked separately to
confirm the constraint-update logic remains intact when per-field materialize
is in play.
"""

from __future__ import annotations

import pytest

from dynastore.models.protocols.field_definition import FieldCapability, FieldDefinition
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeSchemaEntry,
    FeatureAttributeSidecarConfig,
    PostgresType,
)
from dynastore.modules.storage.field_constraints import bridge_schema_to_attribute_sidecar


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fd(
    *,
    data_type: str = "text",
    required: bool = False,
    unique: bool = False,
    materialize: bool | None = None,
    capabilities: list[FieldCapability] | None = None,
) -> FieldDefinition:
    return FieldDefinition(
        name="x",          # name is overridden by the dict key in schema.fields
        data_type=data_type,
        required=required,
        unique=unique,
        materialize=materialize,
        capabilities=capabilities or [],
    )


def _schema(fields: dict, *, materialize_all: bool = False):
    """Lightweight stand-in for ItemsSchema with only the attrs we need."""
    from unittest.mock import MagicMock
    s = MagicMock()
    s.fields = fields
    s.materialize_fields_as_columns = materialize_all
    return s


def _empty_sidecar() -> FeatureAttributeSidecarConfig:
    return FeatureAttributeSidecarConfig()


def _names(sidecar: FeatureAttributeSidecarConfig) -> set[str]:
    return {e.name for e in (sidecar.attribute_schema or [])}


# ---------------------------------------------------------------------------
# Rule 1 — hard constraint always wins (unique=True, required=True → COLUMN)
# ---------------------------------------------------------------------------

class TestRule1HardConstraint:
    def test_unique_and_required_lifts_even_when_materialize_false(self) -> None:
        """unique+required = hard DB constraint → must be a column."""
        schema = _schema({"col": _fd(required=True, unique=True, materialize=False)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged), (
            "Rule 1: unique+required field must become a column even with materialize=False"
        )

    def test_unique_required_overrides_no_capabilities(self) -> None:
        schema = _schema({"col": _fd(required=True, unique=True)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged)

    def test_required_only_lifts(self) -> None:
        """required alone is a constraint → COLUMN."""
        schema = _schema({"col": _fd(required=True)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged)

    def test_unique_only_lifts(self) -> None:
        """unique alone is a constraint → COLUMN."""
        schema = _schema({"col": _fd(unique=True)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged)


# ---------------------------------------------------------------------------
# Rule 2 — materialize=True forces a COLUMN (no constraint needed)
# ---------------------------------------------------------------------------

class TestRule2MaterializeTrue:
    def test_plain_field_with_materialize_true_lifts(self) -> None:
        schema = _schema({"col": _fd(materialize=True)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged)

    def test_materialize_true_no_capabilities_still_lifts(self) -> None:
        schema = _schema({"col": _fd(materialize=True, capabilities=[])})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged)

    def test_materialize_true_with_constraint_lifts(self) -> None:
        """Redundant but harmless: constraint + materialize=True → COLUMN."""
        schema = _schema({"col": _fd(required=True, materialize=True)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged)


# ---------------------------------------------------------------------------
# Rule 3 — materialize=False suppresses capability-driven lifting
# ---------------------------------------------------------------------------

class TestRule3MaterializeFalse:
    def test_materialize_false_no_constraint_stays_jsonb(self) -> None:
        schema = _schema({"col": _fd(materialize=False)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" not in _names(bridged), (
            "Rule 3: materialize=False must suppress column synthesis"
        )

    def test_materialize_false_with_filterable_still_stays_jsonb(self) -> None:
        """Explicit false beats capability-driven lifting."""
        schema = _schema({
            "col": _fd(materialize=False, capabilities=[FieldCapability.FILTERABLE])
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" not in _names(bridged)

    def test_materialize_false_with_sortable_still_stays_jsonb(self) -> None:
        schema = _schema({
            "col": _fd(materialize=False, capabilities=[FieldCapability.SORTABLE])
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" not in _names(bridged)

    def test_materialize_false_multiple_caps_still_stays_jsonb(self) -> None:
        schema = _schema({
            "col": _fd(
                materialize=False,
                capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE,
                               FieldCapability.INDEXED],
            )
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" not in _names(bridged)

    def test_materialize_false_with_materialize_all_still_stays_jsonb(self) -> None:
        """Per-field False beats schema-level materialize_all."""
        schema = _schema({"col": _fd(materialize=False)}, materialize_all=True)
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" not in _names(bridged)


# ---------------------------------------------------------------------------
# Rule 4 — materialize=None, no column-implying capability → JSONB
# ---------------------------------------------------------------------------

class TestRule4NoneNoCapability:
    def test_plain_field_materialize_none_stays_jsonb(self) -> None:
        schema = _schema({"col": _fd(materialize=None)})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" not in _names(bridged)

    def test_none_with_groupable_stays_jsonb(self) -> None:
        """GROUPABLE alone does not imply a native column."""
        schema = _schema({
            "col": _fd(materialize=None, capabilities=[FieldCapability.GROUPABLE])
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" not in _names(bridged)

    def test_none_materialize_all_true_lifts(self) -> None:
        """materialize_all=True + None → schema-level opt-in lifts the field."""
        schema = _schema({"col": _fd(materialize=None)}, materialize_all=True)
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged)


# ---------------------------------------------------------------------------
# Rule 5 — materialize=None + column-implying capability → COLUMN
# ---------------------------------------------------------------------------

class TestRule5NoneWithCapability:
    @pytest.mark.parametrize("cap", [
        FieldCapability.FILTERABLE,
        FieldCapability.SORTABLE,
        FieldCapability.INDEXED,
    ])
    def test_none_column_implying_capability_lifts(self, cap: FieldCapability) -> None:
        """Each column-implying capability independently triggers synthesis."""
        schema = _schema({"col": _fd(materialize=None, capabilities=[cap])})
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged), (
            f"Rule 5: capability {cap!r} should trigger column synthesis when materialize=None"
        )

    def test_none_multiple_column_caps_lifts(self) -> None:
        schema = _schema({
            "col": _fd(
                materialize=None,
                capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
            )
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        assert "col" in _names(bridged)


# ---------------------------------------------------------------------------
# Mixed schema — multiple fields with different rules in one call
# ---------------------------------------------------------------------------

class TestMixedSchema:
    def test_mixed_precedence_rules(self) -> None:
        """Validate all rules apply correctly within a single schema pass."""
        schema = _schema({
            # Rule 1: constraint → COLUMN
            "constrained": _fd(required=True, unique=True, materialize=False),
            # Rule 2: explicit True → COLUMN
            "forced":       _fd(materialize=True),
            # Rule 3: explicit False → JSONB even with cap
            "suppressed":   _fd(materialize=False, capabilities=[FieldCapability.FILTERABLE]),
            # Rule 4: None + no cap → JSONB
            "plain_none":   _fd(materialize=None),
            # Rule 5: None + FILTERABLE → COLUMN
            "cap_none":     _fd(materialize=None, capabilities=[FieldCapability.FILTERABLE]),
        })
        bridged = bridge_schema_to_attribute_sidecar(schema, _empty_sidecar())
        names = _names(bridged)
        assert "constrained" in names, "Rule 1 failed"
        assert "forced"      in names, "Rule 2 failed"
        assert "suppressed" not in names, "Rule 3 failed"
        assert "plain_none" not in names, "Rule 4 failed"
        assert "cap_none"    in names, "Rule 5 failed"


# ---------------------------------------------------------------------------
# Overlay path — existing AttributeSchemaEntry updates remain intact
# ---------------------------------------------------------------------------

class TestExistingEntryOverlay:
    def test_existing_entry_overlay_respects_required(self) -> None:
        """Pre-existing entry gets nullable updated by fd.required."""
        sidecar = FeatureAttributeSidecarConfig(
            attribute_schema=[
                AttributeSchemaEntry(name="col", type=PostgresType.TEXT, nullable=True)
            ]
        )
        schema = _schema({"col": _fd(required=True, materialize=False)})
        bridged = bridge_schema_to_attribute_sidecar(schema, sidecar)
        by_name = {e.name: e for e in bridged.attribute_schema}
        # Overlay must set nullable=False even though materialize=False
        assert by_name["col"].nullable is False

    def test_existing_entry_overlay_respects_unique(self) -> None:
        sidecar = FeatureAttributeSidecarConfig(
            attribute_schema=[
                AttributeSchemaEntry(name="col", type=PostgresType.TEXT, unique=False)
            ]
        )
        schema = _schema({"col": _fd(unique=True, materialize=None)})
        bridged = bridge_schema_to_attribute_sidecar(schema, sidecar)
        by_name = {e.name: e for e in bridged.attribute_schema}
        assert by_name["col"].unique is True
