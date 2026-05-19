"""Unit tests for schema_types.py — M8.

Covers:
- 2 FieldConstraint types (construct, constraint_type, frozen)
- SchemaViolation model
- SchemaExtension Protocol structural check
- StacSchemaExtension.validate_schema
- OgcFeaturesSchemaExtension.validate_schema
- ConfigScopeMixin
- ItemsSchema with constraints field
"""
import pytest
from pydantic import ValidationError

from dynastore.modules.storage.schema_types import (
    ConfigScopeMixin,
    FieldConstraint,
    OgcFeaturesSchemaExtension,
    RequiredConstraint,
    SchemaExtension,
    SchemaViolation,
    StacSchemaExtension,
    UniqueConstraint,
)


# ---------------------------------------------------------------------------
# FieldConstraint types
# ---------------------------------------------------------------------------


class TestRequiredConstraint:
    def test_constraint_type(self):
        assert RequiredConstraint.constraint_type == "required"

    def test_construction(self):
        c = RequiredConstraint()
        assert c.constraint_type == "required"

    def test_frozen(self):
        c = RequiredConstraint()
        with pytest.raises(Exception):
            c.constraint_type = "other"  # type: ignore


class TestUniqueConstraint:
    def test_constraint_type(self):
        assert UniqueConstraint.constraint_type == "unique"

    def test_construction(self):
        c = UniqueConstraint()
        assert c.constraint_type == "unique"


class TestAllConstraintTypesDistinct:
    def test_constraint_types_distinct(self):
        types = {
            RequiredConstraint.constraint_type,
            UniqueConstraint.constraint_type,
        }
        assert len(types) == 2


# ---------------------------------------------------------------------------
# SchemaViolation
# ---------------------------------------------------------------------------


class TestSchemaViolation:
    def test_construction(self):
        v = SchemaViolation(extension="StacSchemaExtension", message="missing field")
        assert v.level == "error"
        assert v.field is None

    def test_with_field(self):
        v = SchemaViolation(extension="ext", field="geometry", message="bad type", level="warning")
        assert v.field == "geometry"
        assert v.level == "warning"


# ---------------------------------------------------------------------------
# SchemaExtension protocol
# ---------------------------------------------------------------------------


class TestSchemaExtensionProtocol:
    def test_stac_extension_satisfies_protocol(self):
        ext = StacSchemaExtension()
        assert isinstance(ext, SchemaExtension)

    def test_ogc_extension_satisfies_protocol(self):
        ext = OgcFeaturesSchemaExtension()
        assert isinstance(ext, SchemaExtension)

    def test_custom_extension_satisfies_protocol(self):
        class MyExtension:
            def validate_schema(self, schema) -> list:
                return []

        assert isinstance(MyExtension(), SchemaExtension)

    def test_missing_method_does_not_satisfy_protocol(self):
        class NotAnExtension:
            pass

        assert not isinstance(NotAnExtension(), SchemaExtension)


# ---------------------------------------------------------------------------
# StacSchemaExtension
# ---------------------------------------------------------------------------


class _MockSchema:
    """Minimal ItemsSchema-like object for testing."""
    def __init__(self, fields):
        self.fields = fields


class TestStacSchemaExtension:
    def test_no_violations_when_stac_fields_present(self):
        schema = _MockSchema({"geometry": None, "bbox": None, "assets": None})
        ext = StacSchemaExtension()
        violations = ext.validate_schema(schema)
        assert violations == []

    def test_violations_when_mandatory_fields_missing(self):
        schema = _MockSchema({})
        ext = StacSchemaExtension()
        violations = ext.validate_schema(schema)
        assert len(violations) == 3  # geometry, bbox, assets

    def test_partial_fields(self):
        schema = _MockSchema({"geometry": None})
        ext = StacSchemaExtension()
        violations = ext.validate_schema(schema)
        missing = {v.field for v in violations}
        assert "bbox" in missing
        assert "assets" in missing
        assert "geometry" not in missing

    def test_violation_level_is_warning(self):
        schema = _MockSchema({})
        ext = StacSchemaExtension()
        for v in ext.validate_schema(schema):
            assert v.level == "warning"


# ---------------------------------------------------------------------------
# OgcFeaturesSchemaExtension
# ---------------------------------------------------------------------------


class _MockFieldDef:
    def __init__(self, data_type):
        self.data_type = data_type


class TestOgcFeaturesSchemaExtension:
    def test_no_violations_when_no_geometry_fields(self):
        schema = _MockSchema({"name": _MockFieldDef("text")})
        ext = OgcFeaturesSchemaExtension()
        assert ext.validate_schema(schema) == []

    def test_no_violations_when_geometry_field_type_correct(self):
        schema = _MockSchema({"geom": _MockFieldDef("geometry")})
        ext = OgcFeaturesSchemaExtension()
        assert ext.validate_schema(schema) == []

    def test_violation_when_geometry_field_type_wrong(self):
        schema = _MockSchema({"geom": _MockFieldDef("text")})
        ext = OgcFeaturesSchemaExtension()
        violations = ext.validate_schema(schema)
        assert len(violations) == 1
        assert violations[0].field == "geom"
        assert violations[0].level == "warning"


# ---------------------------------------------------------------------------
# ConfigScopeMixin
# ---------------------------------------------------------------------------


class TestConfigScopeMixin:
    def test_default_scope(self):
        assert ConfigScopeMixin.config_scope == "platform_waterfall"

    def test_subclass_can_override_scope(self):
        class MyConfig(ConfigScopeMixin):
            config_scope = "collection_intrinsic"

        assert MyConfig.config_scope == "collection_intrinsic"

    def test_scope_literal_values(self):
        valid_scopes = {"platform_waterfall", "collection_intrinsic", "deployment_env"}
        assert ConfigScopeMixin.config_scope in valid_scopes


# ---------------------------------------------------------------------------
# ItemsSchema with constraints field
# ---------------------------------------------------------------------------


class TestItemsSchemaConstraints:
    def test_empty_constraints_default(self):
        from dynastore.modules.storage.driver_config import ItemsSchema
        cfg = ItemsSchema()
        assert cfg.constraints == []

    def test_constraints_field_accepts_field_constraints(self):
        from dynastore.modules.storage.driver_config import ItemsSchema
        cfg = ItemsSchema(constraints=[
            RequiredConstraint(),
            UniqueConstraint(),
        ])
        assert len(cfg.constraints) == 2

    def test_class_key(self):
        from dynastore.modules.storage.driver_config import ItemsSchema
        assert ItemsSchema.class_key() == "items_schema"
