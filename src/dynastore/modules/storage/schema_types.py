#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

"""M8 — Schema type system: FieldConstraint, SchemaExtension, ConfigScopeMixin.

FieldConstraint
===============
Declarative, composable constraints on individual fields in a ``CollectionSchema``.
Each constraint kind is a frozen Pydantic model implementing the
``FieldConstraint`` protocol (``constraint_type: str`` class attribute).

Five built-in constraint types:

    RequiredConstraint        — field must be present on every write
    UniqueConstraint          — values must be unique across the collection
    IdentityKeyConstraint     — deduplication identity key with optional geohash precision
    ValidityConstraint        — field carries temporal validity range
    GeometryHashConstraint    — skip write when geometry hash is unchanged

Additional constraint types can be registered by any module.

SchemaExtension
===============
A ``@runtime_checkable`` Protocol for plug-in schema validators.  Two built-in
implementations:

    StacSchemaExtension       — checks mandatory STAC fields are declared
    OgcFeaturesSchemaExtension — checks geometry and CRS constraints

ConfigScopeMixin
================
Mixin that adds a ``config_scope`` annotation to any ``PluginConfig`` subclass,
classifying it as ``"platform_waterfall"`` | ``"collection_intrinsic"`` |
``"deployment_env"``.  Used by the discovery API (M9) for schema grouping.

WritePolicyDefaults
===================
Posture-only write policy — no field name references.  Sits at
platform / catalog waterfall scope.  Field-level constraints (identity key,
validity field, geohash precision) live in ``CollectionSchema.constraints``.
"""

from __future__ import annotations

from typing import ClassVar, List, Literal, Optional, Protocol, TYPE_CHECKING, runtime_checkable

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from dynastore.modules.storage.driver_config import CollectionSchema


# ---------------------------------------------------------------------------
# FieldConstraint protocol + concrete types
# ---------------------------------------------------------------------------


class FieldConstraint(BaseModel):
    """Base class for all field constraints.

    Sub-classes MUST declare ``constraint_type: ClassVar[str]`` to identify
    the constraint kind in serialised form.
    """

    constraint_type: ClassVar[str]

    model_config = {"frozen": True}


class RequiredConstraint(FieldConstraint):
    """Field must be present (non-null) on every write."""

    constraint_type: ClassVar[str] = "required"


class UniqueConstraint(FieldConstraint):
    """Values must be unique across the collection.

    Enforcement is native (DDL-level) when the primary driver advertises
    ``Capability.UNIQUE_ENFORCEMENT``; otherwise falls through to service-layer
    enforcement when ``CollectionSchema.allow_app_level_enforcement=True``.
    """

    constraint_type: ClassVar[str] = "unique"


class IdentityKeyConstraint(FieldConstraint):
    """This field acts as the deduplication identity key.

    Replaces ``ItemsWritePolicy.external_id_field`` +
    ``ItemsWritePolicy.geohash_precision``.

    geohash_precision: geohash grid precision when the GEOHASH matcher is used.
        Ignored when ``external_id_field`` mode is active.
    """

    constraint_type: ClassVar[str] = "identity_key"
    geohash_precision: int = Field(default=9, ge=1, le=12)


class ValidityConstraint(FieldConstraint):
    """This field carries the temporal validity range for a feature.

    Replaces ``ItemsWritePolicy.validity_field``.

    field: name of the temporal validity field (e.g. ``"valid_time"``).
    """

    constraint_type: ClassVar[str] = "validity"
    field: str = Field(description="Name of the temporal validity field.")


class GeometryHashConstraint(FieldConstraint):
    """Skip write when the incoming geometry hash equals the stored one.

    Replaces ``ItemsWritePolicy.skip_if_unchanged_geometry_hash``.
    NEW_VERSION degrades to no-op; UPDATE degrades to REFUSE_RETURN.
    """

    constraint_type: ClassVar[str] = "geometry_hash"


# ---------------------------------------------------------------------------
# SchemaViolation — returned by SchemaExtension.validate()
# ---------------------------------------------------------------------------


class SchemaViolation(BaseModel):
    """A single validation failure from a ``SchemaExtension``."""

    extension: str = Field(description="Extension class name.")
    field: Optional[str] = Field(default=None, description="Affected field name, if any.")
    message: str = Field(description="Human-readable description of the violation.")
    level: Literal["error", "warning"] = Field(default="error")


# ---------------------------------------------------------------------------
# SchemaExtension protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class SchemaExtension(Protocol):
    """Plug-in validator for ``CollectionSchema``.

    Each registered extension receives the full ``CollectionSchema`` and
    returns a (possibly empty) list of :class:`SchemaViolation` items.
    All violations from all extensions are aggregated at bootstrap time.
    """

    def validate_schema(self, schema: "CollectionSchema") -> List[SchemaViolation]:
        """Validate *schema* and return any violations found."""
        ...


# ---------------------------------------------------------------------------
# Built-in SchemaExtension implementations
# ---------------------------------------------------------------------------

# Mandatory STAC fields that a collection schema should declare.
_STAC_REQUIRED_FIELDS = frozenset({"geometry", "bbox", "assets"})
_STAC_RECOMMENDED_FIELDS = frozenset({"datetime"})


class StacSchemaExtension:
    """Validates that mandatory STAC fields are present in the schema."""

    def validate_schema(self, schema: "CollectionSchema") -> List[SchemaViolation]:
        violations: List[SchemaViolation] = []
        declared = set(schema.fields.keys())
        for fname in _STAC_REQUIRED_FIELDS:
            if fname not in declared:
                violations.append(
                    SchemaViolation(
                        extension="StacSchemaExtension",
                        field=fname,
                        message=f"Mandatory STAC field '{fname}' is not declared in schema.fields.",
                        level="warning",
                    )
                )
        return violations


class OgcFeaturesSchemaExtension:
    """Validates that geometry and CRS constraints conform to OGC API - Features rules.

    Currently checks:
    - If a geometry field is declared, it must have ``data_type`` set to
      ``"geometry"`` or ``"geojson"``.
    """

    def validate_schema(self, schema: "CollectionSchema") -> List[SchemaViolation]:
        violations: List[SchemaViolation] = []
        for fname, fdef in schema.fields.items():
            dt = getattr(fdef, "data_type", None)
            if "geom" in fname.lower() and dt and dt.lower() not in {"geometry", "geojson", "point", "polygon", "linestring"}:
                violations.append(
                    SchemaViolation(
                        extension="OgcFeaturesSchemaExtension",
                        field=fname,
                        message=(
                            f"Field '{fname}' appears to be a geometry field but has "
                            f"data_type='{dt}'. Expected 'geometry' or 'geojson'."
                        ),
                        level="warning",
                    )
                )
        return violations


# ---------------------------------------------------------------------------
# ConfigScopeMixin
# ---------------------------------------------------------------------------

ConfigScopeType = Literal["platform_waterfall", "collection_intrinsic", "deployment_env"]


class ConfigScopeMixin:
    """Mixin that annotates a ``PluginConfig`` with its waterfall scope.

    Add to any ``PluginConfig`` subclass::

        class ItemsRoutingConfig(ConfigScopeMixin, PluginConfig):
            config_scope: ClassVar[ConfigScopeType] = "platform_waterfall"

    Used by the config discovery API (M9) to group configs by scope.
    Defaults to ``"platform_waterfall"`` when not explicitly declared.
    """

    config_scope: ClassVar[ConfigScopeType] = "platform_waterfall"


# WritePolicyDefaults is defined in driver_config.py to avoid circular imports.
# It is exported from this module for convenience:
# from dynastore.modules.storage.schema_types import WritePolicyDefaults  (after import from driver_config)
