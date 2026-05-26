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
Declarative, composable constraints on individual fields in a ``ItemsSchema``.
Each constraint kind is a frozen Pydantic model implementing the
``FieldConstraint`` protocol (``constraint_type: str`` class attribute).

Two built-in constraint types:

    RequiredConstraint        — field must be present on every write
    UniqueConstraint          — values must be unique across the collection

Identity / validity / geometry-hash semantics now live on
:class:`~dynastore.modules.storage.driver_config.ItemsWritePolicy`
(``compute``/``identity``/``schema``) — geometry-hash skip is expressed
as an ``IdentityRule(match_on=["geometry_hash"], ...)`` entry rather
than a sub-config flag.

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
"""

from __future__ import annotations

from typing import ClassVar, List, Literal, Optional, Protocol, TYPE_CHECKING, runtime_checkable

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from dynastore.modules.storage.driver_config import ItemsSchema


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
    enforcement when ``ItemsSchema.allow_app_level_enforcement=True``.
    """

    constraint_type: ClassVar[str] = "unique"


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
    """Plug-in validator for ``ItemsSchema``.

    Each registered extension receives the full ``ItemsSchema`` and
    returns a (possibly empty) list of :class:`SchemaViolation` items.
    All violations from all extensions are aggregated at bootstrap time.
    """

    def validate_schema(self, schema: "ItemsSchema") -> List[SchemaViolation]:
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

    def validate_schema(self, schema: "ItemsSchema") -> List[SchemaViolation]:
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

    def validate_schema(self, schema: "ItemsSchema") -> List[SchemaViolation]:
        violations: List[SchemaViolation] = []
        for fname, fdef in schema.fields.items():
            dt = getattr(fdef, "data_type", None)
            # Canonical geometry is ``geometry`` or parametrized ``geometry(...)``.
            if "geom" in fname.lower() and dt and not dt.lower().startswith("geometry"):
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


