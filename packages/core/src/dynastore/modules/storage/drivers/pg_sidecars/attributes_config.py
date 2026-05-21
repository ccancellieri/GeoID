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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""
Feature Attribute Sidecar Configuration Models.

Provides enhanced configuration for attribute storage, including storage modes
(Relational vs JSONB), per-attribute indexing, and partitioning control.
"""

from typing import List, Optional, Dict, Any, Union, Literal
from enum import Enum
from pydantic import BaseModel, Field, field_validator, model_validator
from dynastore.modules.storage.drivers.pg_sidecars.base import SidecarConfig, SidecarConfigRegistry
from dynastore.modules.storage.computed_fields import ComputedField
from dynastore.tools.db import validate_column_identifier


class AttributePartitionStrategyPreset(str, Enum):
    """Partition strategies supported by the Attribute Sidecar."""

    BY_ASSET_ID = "BY_ASSET_ID"
    BY_ATTRIBUTE_VALUE = "BY_ATTRIBUTE_VALUE"
    BY_TIME = "BY_TIME"


class AttributeStorageMode(str, Enum):
    """Storage mode for feature attributes."""

    COLUMNAR = "columnar"  # Mode A: Physical columns
    JSONB = "jsonb"  # Mode B: JSONB column
    AUTOMATIC = "automatic"  # Automatic selection based on attribute schema (fallback to JSONB if schema is not specified)


class PostgresType(str, Enum):
    """Common PostgreSQL types for attributes."""

    TEXT = "TEXT"
    VARCHAR_255 = "VARCHAR(255)"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    NUMERIC = "NUMERIC"
    FLOAT = "FLOAT"  # Added FLOAT
    BOOLEAN = "BOOLEAN"
    TIMESTAMPTZ = "TIMESTAMPTZ"
    DATE = "DATE"
    JSONB = "JSONB"
    UUID = "UUID"


class AttributeIndexType(str, Enum):
    """Index types for attributes."""

    NONE = "none"
    BTREE = "btree"
    GIN = "gin"  # For JSONB/array columns
    GIST = "gist"  # For range types
    HASH = "hash"  # For equality only


class AttributeSchemaEntry(BaseModel):
    """Enhanced schema definition for a single attribute."""

    name: str = Field(..., description="Attribute name (column name or JSON key)")
    type: PostgresType = Field(default=PostgresType.TEXT, description="PostgreSQL data type")

    # Indexing control (B-Tree recommended for numeric/text at scale)
    index: AttributeIndexType = Field(
        default=AttributeIndexType.NONE, description="Index strategy for this attribute"
    )

    # Partitioning control
    can_partition: bool = Field(
        default=False, description="Whether this attribute can be used in a partition key"
    )

    # Constraints
    nullable: bool = Field(default=True, description="Allow NULL values")
    
    # TODO add default value validation and SQL code to ddl
    # TODO add check constraint for valid values
    default: Optional[Any] = Field(default=None, description="Default value for the attribute")

    unique: bool = Field(
        default=False, description="Enforce uniqueness (at table/partition level)"
    )

    # Metadata
    description: Optional[str] = Field(default=None, description="Human-readable description")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Reject column names that are not plain SQL identifiers.

        Names flow (quoted) into CREATE TABLE / INSERT and are reused as
        bind-parameter names, so anything beyond ``[A-Za-z_][A-Za-z0-9_]*`` —
        spaces, dots, symbols, reserved words — is rejected at config-parse
        time rather than failing later at ingest. Case is preserved: the
        physical column is created quoted, so ``Area`` stays ``Area``.
        """
        return validate_column_identifier(v)

    @model_validator(mode="after")
    def validate_default_type(self) -> "AttributeSchemaEntry":
        """Check if the default value matches the defined type."""
        if self.default is None:
            return self

        # Basic type checking
        if self.type in [PostgresType.INTEGER, PostgresType.BIGINT]:
            if not isinstance(self.default, int):
                raise ValueError(f"Default value for {self.name} must be an integer, got {type(self.default)}")
        elif self.type in [PostgresType.FLOAT, PostgresType.NUMERIC]:
            if not isinstance(self.default, (int, float)):
                raise ValueError(f"Default value for {self.name} must be a number, got {type(self.default)}")
        elif self.type == PostgresType.BOOLEAN:
            if not isinstance(self.default, bool):
                raise ValueError(f"Default value for {self.name} must be a boolean, got {type(self.default)}")
        elif self.type in [PostgresType.TEXT, PostgresType.VARCHAR_255, PostgresType.UUID]:
            if not isinstance(self.default, str):
                raise ValueError(f"Default value for {self.name} must be a string, got {type(self.default)}")
        
        return self

    def get_sql_default(self) -> Optional[str]:
        """Format the default value for SQL DDL."""
        if self.default is None:
            return None
            
        if isinstance(self.default, str):
            # Safe quoting for strings (simple for now)
            # Use dollar quoting or double single quotes if needed, 
            # but for simple defaults single quotes are enough.
            escaped = self.default.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(self.default, bool):
            return "TRUE" if self.default else "FALSE"
        else:
            return str(self.default)


class FeatureAttributeSidecarConfig(SidecarConfig):
    """PG sidecar table that stores feature attributes + identity off-hub.

    Owns the ``{schema}.{table}_attributes`` table — one row per item
    (FK to hub on ``geoid``).  Always-on for every collection_type; the
    only sidecar that's never optional.

    Two storage modes (auto-resolved by ``storage_mode=AUTOMATIC``
    based on whether ``attribute_schema`` is supplied):

    * **JSONB (Mode B, default when no schema)** — single ``attributes
      JSONB`` column + STORED GENERATED ``attributes_hash CHAR(64)``
      (SHA256 of canonicalised JSONB) powering
      ``ComputedKind.ATTRIBUTES_HASH``.  Schema-flexible; one column
      per item regardless of property count.

    * **Columnar (Mode A, when schema is supplied)** — one PG column
      per declared attribute with type/nullability/index controls.
      Stronger query plans, requires schema declaration up-front.

    Identity columns use a null-object pattern: the field name present
    means "enabled, use this storage column name"; ``None`` means disabled.

    * ``external_id_field`` — ``None`` disables the column entirely;
      any string value (default ``"external_id"``) names the PG column.
      SSOT: ``ItemsWritePolicy.identity`` (presence of a
      ``ComputedField(kind=EXTERNAL_ID)`` rule).  The PG driver sets this
      to ``"external_id"`` or ``None`` at ``ensure_storage`` time.

    * ``asset_id_field`` — ``None`` disables the column; any string value
      (default ``"asset_id"``) names the PG column.
      SSOT: ``ItemsWritePolicy.track_asset_id``.  The PG driver sets this
      to ``"asset_id"`` or ``None`` at ``ensure_storage`` time.

    Operators MUST set the policy fields; values here are overwritten on
    the next ``ensure_storage``.
    """

    sidecar_type: Literal["attributes"] = "attributes"

    # --- Storage Mode ---
    storage_mode: AttributeStorageMode = Field(
        default=AttributeStorageMode.AUTOMATIC,
        description="Storage mode: columnar, jsonb, or automatic (based on schema presence)",
    )


    # --- Protocol-Driven Architecture ---
    storage_only_fields: List[str] = Field(
        default_factory=list,
        description="Fields that are stored and queryable but NOT included in Feature output. "
                    "These fields have expose=False in get_queryable_fields().",
    )

    # ``feature_type_schema`` was retired in #976: the wire shape of Feature
    # ``properties`` is now the SSOT on ``ItemsWritePolicy.resolved_schema`` (see PR
    # #961 phase 2). Sidecars derive their schema fragments from their own
    # storage columns and the policy overlays the user-data ``properties``.

    # Identity Columns — null-object pattern
    #
    # ``require_external_id`` (behavior) lives exclusively on
    # ``ItemsWritePolicy`` (config key ``items_write_policy``).  The sidecar
    # reads it at write time via ``processing_context["_items_write_policy"]``.
    #
    # ``external_id_field`` and ``asset_id_field`` are storage-shape overlay
    # fields using a null-object pattern: a non-None string value names the PG
    # column (enabling it); ``None`` disables the column entirely.
    # The PG driver overlays the SSOT values from ``ItemsWritePolicy`` at
    # ``ensure_storage`` time so persisted config stays policy-aligned.
    # SSOT: ``ItemsWritePolicy.identity`` (EXTERNAL_ID rule) → external_id_field
    # SSOT: ``ItemsWritePolicy.track_asset_id``               → asset_id_field
    # Operators MUST set those policy fields; values here are overwritten on the
    # next ``ensure_storage``.
    external_id_field: Optional[str] = Field(
        default="external_id",
        description=(
            "Storage column name for the external identifier.  None disables the "
            "column.  Null-object mirror of ItemsWritePolicy.identity (EXTERNAL_ID "
            "rule presence).  Overwritten by the PG driver at DDL time."
        ),
    )
    index_external_id: bool = Field(
        default=True, description="Create unique index on external_id"
    )
    # ``external_id_as_feature_id`` lives on ``ItemsReadPolicy.feature_type``
    # (wire-shape decision, not a storage knob). The sidecar's
    # ``provides_feature_id`` property advertises capability only — the
    # policy decides whether to surface ``external_id`` as ``feature.id``.
    expose_geoid: bool = Field(
        default=False,
        description="Add geoid to feature properties if external_id is not unique",
    )

    asset_id_field: Optional[str] = Field(
        default="asset_id",
        description=(
            "Storage column name for the asset identifier.  None disables the "
            "column.  Null-object mirror of ItemsWritePolicy.track_asset_id.  "
            "Overwritten by the PG driver at DDL time."
        ),
    )
    index_asset_id: bool = Field(default=True, description="Create index on asset_id")

    @property
    def enable_external_id(self) -> bool:
        """True when ``external_id_field`` is set (null-object pattern)."""
        return self.external_id_field is not None

    @property
    def enable_asset_id(self) -> bool:
        """True when ``asset_id_field`` is set (null-object pattern)."""
        return self.asset_id_field is not None

    # Validity Configuration
    #
    # ``validity_column`` is a null-object storage-shape mirror of the SSOT
    # ``ItemsWritePolicy.validity`` (:class:`ValiditySpec`). The PG driver
    # overlays ``policy.validity.column`` onto this field at ``ensure_storage``
    # time. Its presence IS the toggle: a non-None value names/enables the
    # validity column, ``None`` disables it. The value is a COLUMN NAME, NOT a
    # source path for extraction. The PG driver persists the result, so every
    # read path that loads the collection's sidecar config sees the
    # policy-aligned value (#957/#974/#1126).
    # Operators MUST set ``validity`` on ``ItemsWritePolicy``;
    # values set here are overwritten on the next ``ensure_storage``.
    validity_column: Optional[str] = Field(
        default=None,
        description=(
            "Null-object storage-shape mirror of ItemsWritePolicy.validity.column "
            "(SSOT). Non-None names/enables the validity column; None → disabled. "
            "Column name only, not a source path. Overwritten by the driver at "
            "DDL time."
        ),
    )

    # Validity VALUE sources — storage-shape mirror of
    # ``ItemsWritePolicy.validity.start_from`` / ``end_from``. The PG driver
    # overlays these at ``ensure_storage`` time so the ingestion path resolves
    # the validity-range start/end without threading the policy itself.
    # ``"context"`` (default for start) reads ``write_context.valid_from`` /
    # ``valid_to``; any other string is a dotted source path walked into the
    # feature; ``validity_end_from=None`` means open-ended.
    validity_start_from: str = Field(
        default="context",
        description=(
            "Storage-shape mirror of ItemsWritePolicy.validity.start_from. "
            "'context' reads write_context.valid_from; otherwise a dotted source "
            "path walked into the feature. Overwritten by the driver at DDL time."
        ),
    )
    validity_end_from: Optional[str] = Field(
        default=None,
        description=(
            "Storage-shape mirror of ItemsWritePolicy.validity.end_from. None → "
            "open-ended; 'context' reads write_context.valid_to; otherwise a "
            "dotted source path walked into the feature. Overwritten by the "
            "driver at DDL time."
        ),
    )

    @property
    def enable_validity(self) -> bool:
        """True when ``validity_column`` is set (null-object pattern).

        Bool shim over the null-object field so the ~13 DDL/SQL callsites in
        ``attributes.py`` keep reading a boolean. The field name is the single
        toggle, so the bool can never diverge from the configured column.
        """
        return self.validity_column is not None

    # Mode A: Relational schema
    attribute_schema: Optional[List[AttributeSchemaEntry]] = Field(default=None, description="List of attribute definitions for relational mode"
    )

    # Mode B: JSONB settings
    jsonb_column_name: str = Field(default="attributes", description="Name of JSONB column")
    use_hot_updates: bool = Field(
        default=True, description="Enable HOT updates with FILLFACTOR=80"
    )

    # JSONB functional indexes (Strategic B-Tree indexes on specific JSON paths)
    # Map of JSON path -> Cast Type (e.g., {"population": "bigint", "area": "numeric"})
    jsonb_indexed_paths: Dict[str, PostgresType] = Field(
        default_factory=dict,
        description="Functional B-Tree indexes on specific JSONB paths for performance at scale",
    )

    # Attribute Statistics — storage-shape mirror of ``ItemsWritePolicy.compute``
    #
    # The PG driver populates this list at ``ensure_storage`` time with the
    # subset of ``ItemsWritePolicy.compute`` whose entries target the attributes
    # sidecar (``ComputedKind.ATTRIBUTE_STAT``) and declare a ``storage_mode``.
    # DDL emission, ``get_select_fields``, ``get_field_definitions``,
    # ``resolve_computed_value`` and ``prepare_upsert_payload`` all read this
    # single list. COLUMNAR fields get their own ``DOUBLE PRECISION`` column;
    # JSONB fields share an ``attribute_stats`` JSONB column. An empty list means
    # no attribute-derived statistics are materialised. Mirrors the geometries
    # sidecar's overlay (#1074).
    compute_fields_overlay: List[ComputedField] = Field(
        default_factory=list,
        description=(
            "Storage-shape snapshot of the attribute-derived (ATTRIBUTE_STAT) "
            "entries from ``ItemsWritePolicy.compute`` for this collection. "
            "Overwritten by the PG driver at DDL time."
        ),
    )

    # Partitioning
    partition_strategy: Optional[AttributePartitionStrategyPreset] = Field(default=None,
        description="Strategy to use for contributing to the global partition key.",
    )
    partition_attribute: Optional[str] = Field(default=None,
        description="Attribute name to partition by (must be in identity or have can_partition=True). Required if strategy is BY_ATTRIBUTE_VALUE.",
    )

    @property
    def has_validity(self) -> bool:
        """Whether this sidecar is configured to manage validity.

        Mirrors :attr:`ItemsWritePolicy.validity`; the driver overlays
        ``policy.validity.column`` onto :attr:`validity_column` at DDL time
        and persists the result so every read sees the same value.
        """
        return self.enable_validity

    @property
    def partition_key_contributions(self) -> Dict[str, str]:
        """Determine partition key contribution based on the requested partition_strategy."""
        if not self.partition_strategy:
            return {}

        # 1. BY_TIME -> validity
        if self.partition_strategy == AttributePartitionStrategyPreset.BY_TIME:
            if not self.enable_validity:
                raise ValueError(
                    "BY_TIME partitioning requires validity to be set on the policy"
                )
            return {"validity": "TSTZRANGE"}

        # 2. BY_ASSET_ID -> asset_id
        if self.partition_strategy == AttributePartitionStrategyPreset.BY_ASSET_ID:
            if self.asset_id_field is None:
                raise ValueError(
                    "BY_ASSET_ID partitioning requires asset_id_field to be set"
                )
            return {self.asset_id_field: "VARCHAR(255)"}

        # 3. BY_ATTRIBUTE_VALUE -> custom attribute
        if (
            self.partition_strategy
            == AttributePartitionStrategyPreset.BY_ATTRIBUTE_VALUE
        ):
            if not self.partition_attribute:
                raise ValueError(
                    "partition_attribute must be specified for BY_ATTRIBUTE_VALUE strategy"
                )

            # Check Schema Attributes
            if self.attribute_schema:
                for attr in self.attribute_schema:
                    if attr.name == self.partition_attribute:
                        if not attr.can_partition:
                            raise ValueError(
                                f"Attribute '{attr.name}' is not marked as can_partition=True"
                            )
                        return {attr.name: attr.type.value}

            raise ValueError(
                f"Partition attribute '{self.partition_attribute}' not found in configuration or not columnar."
            )

        return {}

    @property
    def feature_id_field_name(self) -> Optional[str]:
        """Column holding the external feature id in this sidecar's table.

        Returns ``external_id_field`` when set, ``None`` when the column is
        disabled.  Independent of whether the read policy elects to surface
        the column as ``feature.id`` — that decision lives on
        ``ItemsReadPolicy.feature_type.external_id_as_feature_id``.
        """
        return self.external_id_field

    model_config = {"extra": "allow"}

# Register for polymorphic resolution
SidecarConfigRegistry.register("attributes", FeatureAttributeSidecarConfig)
