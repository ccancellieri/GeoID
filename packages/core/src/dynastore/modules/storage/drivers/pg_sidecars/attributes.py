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
Feature Attribute Sidecar Implementation.

This module provides the FeatureAttributeSidecar which manages:
- Feature attributes (Relational or JSONB Mode)
- Identity columns (external_id, asset_id)
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple, Union, cast
from enum import Enum
from pydantic import Field, BaseModel
from geojson_pydantic import Feature
from dynastore.models.query_builder import QueryRequest
from sqlalchemy import text
from dynastore.modules.db_config.query_executor import DbResource, DDLQuery, DQLQuery, ResultHandler
from dynastore.models.protocols import ConfigsProtocol, AssetsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.tools import map_pg_to_json_type
from dynastore.models.field_types import CANONICAL_TO_PG_DDL
from dynastore.modules.storage.field_constraints import pg_native_to_canonical
from dynastore.tools.json import CustomJSONEncoder
from dynastore.modules.storage.drivers.pg_sidecars.base import (
    SidecarProtocol,
    SidecarConfig,
    FeaturePipelineContext,
    FieldDefinition,
    FieldCapability,
)
from dynastore.modules.catalog.models import LocalizedText
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    FeatureAttributeSidecarConfig,
    AttributeStorageMode,
    PostgresType,
    AttributeIndexType,
    AttributeSchemaEntry,
)
from dynastore.modules.storage.computed_fields import (
    ComputedField,
    StatisticStorageMode,
)
from dynastore.modules.db_config.locking_tools import (
    acquire_lock_if_needed,
    check_trigger_exists,
)

logger = logging.getLogger(__name__)


from pydantic_core import PydanticUndefined


def _make_tstzrange(start: Any, end: Any, *, lower_inc: bool = True, upper_inc: bool = False) -> Any:
    """Build a tstzrange-compatible value usable by both async (asyncpg) and
    sync (psycopg2) PG drivers.  Sync workers (DatastoreModule + worker
    SCOPEs) don't ship asyncpg, so the historical ``from asyncpg import
    Range`` blew up at ingestion time with ``ModuleNotFoundError``.
    """
    try:
        from asyncpg import Range
        return Range(start, end, lower_inc=lower_inc, upper_inc=upper_inc)
    except ImportError:
        pass
    try:
        from psycopg2.extras import DateTimeTZRange
        bounds = ("[" if lower_inc else "(") + ("]" if upper_inc else ")")
        return DateTimeTZRange(start, end, bounds=bounds)
    except ImportError:
        pass
    # Last resort: PG literal string.  PG casts ``'[lo,hi)'`` to tstzrange
    # at INSERT time — works with any driver, costs a server-side parse.
    lo = ("[" if lower_inc else "(")
    hi = ("]" if upper_inc else ")")
    s_iso = start.isoformat() if hasattr(start, "isoformat") else (start or "")
    e_iso = end.isoformat() if hasattr(end, "isoformat") else (end or "")
    return f"{lo}{s_iso},{e_iso}{hi}"


def _resolve_external_id_field(context: Dict[str, Any]) -> Optional[str]:
    """Resolve the ``external_id`` extraction path from ``ItemsWritePolicy``.

    The policy is the single source of truth for the field path. Reads the
    ``name`` override on the ``ComputedField(kind=EXTERNAL_ID)`` entry.
    Returns ``None`` when no policy is on the context or no EXTERNAL_ID
    ComputedField is configured — callers MUST treat ``None`` as "skip
    extraction; conflict resolution falls back to geoid".
    """
    policy = context.get("_items_write_policy") if context else None
    if policy is None:
        return None
    getter = getattr(policy, "external_id_path", None)
    return cast(Optional[str], getter()) if callable(getter) else None


class FeatureAttributeSidecar(SidecarProtocol):
    """
    Sidecar for feature attributes and identity.

    Manages:
    - Identity: external_id, asset_id (optional)
    - Attributes: Relational columns or JSONB document

    Identity field-name binding lives on ``ItemsWritePolicy`` and reaches the
    sidecar via ``processing_context["_items_write_policy"]`` — see the
    module-level ``_resolve_external_id_field`` helper. A missing external_id
    that is a declared required field is rejected by the normal required-field
    check plus NOT NULL columns, not by a sidecar-level guard.
    """

    def __init__(
        self,
        config: FeatureAttributeSidecarConfig,
        **_kwargs: Any,
    ):
        # ``**_kwargs`` absorbs forward-compatible factory inputs
        # (e.g. ``policy=...``) reserved for future SSOT threading.
        # ``config.validity_column`` is already policy-aligned because the PG
        # driver sets its own fixed ``validity`` column on the sidecar config at
        # ``ensure_storage`` time when ``ItemsWritePolicy.validity`` is enabled
        # (#957/#974/#1126/#1168 — validity is a driver-abstracted concept; the
        # policy never names a physical column).
        self.config = config

    @property
    def resolved_storage_mode(self) -> AttributeStorageMode:
        """Resolve AUTOMATIC storage mode based on schema presence."""
        if self.config.storage_mode != AttributeStorageMode.AUTOMATIC:
            return self.config.storage_mode
        if self.config.attribute_schema:
            return AttributeStorageMode.COLUMNAR
        return AttributeStorageMode.JSONB

    def get_property_field_names(self) -> List[str]:
        """Property field names, storage-agnostic (JSONB blob vs COLUMNAR columns).

        - JSONB: the single blob column (``jsonb_column_name``); the optimizer
          resolves it to the whole document, preserving the long-standing tile
          property shape.
        - COLUMNAR: one name per declared ``attribute_schema`` column.

        Identity columns and computed statistics are intentionally excluded —
        they are not feature properties. See ``get_property_field_names`` on the
        base protocol for the contract.
        """
        if self.resolved_storage_mode == AttributeStorageMode.JSONB:
            return [self.config.jsonb_column_name]
        return [attr.name for attr in (self.config.attribute_schema or [])]

    # ------------------------------------------------------------------
    # Attribute-statistics storage-shape helpers (consume the
    # ``compute_fields_overlay``; mirror the geometries sidecar). #1074
    # ------------------------------------------------------------------

    # COLUMNAR attribute statistics materialise as a numeric column. A property
    # whose value is non-numeric should declare ``storage_mode=JSONB`` instead —
    # the shared ``attribute_stats`` blob preserves any JSON type.
    _STAT_COLUMNAR_SQL_TYPE: str = "DOUBLE PRECISION"

    def _stat_fields(self) -> List[ComputedField]:
        """Storage-bearing attribute-derived computed fields for this sidecar."""
        return [
            f for f in self.config.compute_fields_overlay
            if f.storage_mode is not None
        ]

    def _has_jsonb_stats(self) -> bool:
        """Any storage-bearing attribute stat with ``storage_mode == JSONB``?"""
        return any(
            f.storage_mode == StatisticStorageMode.JSONB for f in self._stat_fields()
        )

    def _columnar_stat_fields(self) -> List[ComputedField]:
        return [
            f for f in self._stat_fields()
            if f.storage_mode == StatisticStorageMode.COLUMNAR
        ]

    @property
    def sidecar_id(self) -> str:
        return "attributes"

    @property
    def sidecar_type_id(self) -> str:
        """Type ID for protocol-based discovery and config matching."""
        return "attributes"

    @classmethod
    def get_default_config(
        cls, context: Dict[str, Any]
    ) -> Optional[FeatureAttributeSidecarConfig]:
        """Auto-inject attributes sidecar by default."""
        return FeatureAttributeSidecarConfig()

    @property
    def provides_feature_id(self) -> bool:
        """Capability flag — attributes sidecar can map ``external_id`` to
        ``feature.id``. Whether to do so on a given read is decided by
        ``ItemsReadPolicy.feature_type.external_id_as_feature_id`` (the
        policy intent), not by this property (the sidecar capability).
        """
        return True

    @property
    def feature_id_field_name(self) -> Optional[str]:
        return self.config.feature_id_field_name

    @property
    def partition_key_contributions(self) -> Dict[str, str]:
        """Expose keys available for partitioning."""
        return self.config.partition_key_contributions

    def get_queryable_fields(self) -> Dict[str, FieldDefinition]:
        """
        Returns all queryable fields including auto-included asset_id.

        Fields with expose=False are queryable but not in Feature output.
        """
        # Alias must match builder convention
        alias = f"sc_{self.sidecar_id}"

        fields = {}

        # ALWAYS include asset_id if enabled (even if not in attribute_schema)
        if self.config.asset_id_field is not None:
            asset_col = self.config.asset_id_field
            fields[asset_col] = FieldDefinition(
                name=asset_col,
                sql_expression=f"{alias}.{asset_col}",
                capabilities=[
                    FieldCapability.FILTERABLE,
                    FieldCapability.SORTABLE,
                    FieldCapability.GROUPABLE,
                ],
                data_type="uuid",
                expose=False,  # Never in Feature output unless explicitly configured
                title="Asset ID",
                description="Reference to parent asset",
            )

        # external_id - always query-only, mapped to Feature.id
        if self.config.external_id_field is not None:
            ext_col = self.config.external_id_field
            fields[ext_col] = FieldDefinition(
                name=ext_col,
                alias="id",
                sql_expression=f"{alias}.{ext_col}",
                capabilities=[
                    FieldCapability.FILTERABLE,
                    FieldCapability.SORTABLE,
                    FieldCapability.GROUPABLE,
                ],
                data_type="string",
                expose=False,  # Mapped to Feature.id, not exposed as property
                title="External ID",
                description="External identifier",
            )

        # Attributes from schema
        if self.config.attribute_schema:
            for attr in self.config.attribute_schema:
                # Check if this is a storage-only field
                is_storage_only = attr.name in (
                    getattr(self.config, "storage_only_fields", None) or []
                )

                # Determine capabilities based on type
                caps = [
                    FieldCapability.FILTERABLE,
                    FieldCapability.SORTABLE,
                    FieldCapability.GROUPABLE,
                ]
                if attr.type in [
                    PostgresType.INTEGER,
                    PostgresType.BIGINT,
                    PostgresType.NUMERIC,
                    PostgresType.FLOAT,
                ]:
                    caps.append(FieldCapability.AGGREGATABLE)

                fields[attr.name] = FieldDefinition(
                    name=attr.name,
                    # Quote the identifier: columnar columns are created with
                    # case-preserving DDL (``"CODE" TEXT``). An unquoted
                    # reference is folded to lowercase by PostgreSQL and fails
                    # for any non-lowercase column name (the MVT tile 500).
                    sql_expression=f'{alias}."{attr.name}"',
                    capabilities=caps,
                    data_type=pg_native_to_canonical(attr.type.value),
                    expose=not is_storage_only,  # Storage-only fields not in Feature output
                    title=getattr(attr, "title", attr.name),
                    description=getattr(attr, "description", None),
                )

        # Merge with other definitions (validity, transaction_time) from get_field_definitions
        # Pass the correct alias!
        other_fields = self.get_field_definitions(sidecar_alias=alias)
        # We only add fields that aren't already covered or if we prefer the detailed definition
        # get_field_definitions includes external_id/asset_id/attributes too.
        # It seems there's duplication between get_queryable_fields and get_field_definitions?
        # Yes. We should probably rely on get_field_definitions more.

        for k, v in other_fields.items():
            if k not in fields:
                fields[k] = v

        return fields

    def get_feature_type_schema(self) -> Dict[str, Any]:
        """
        Returns JSON Schema for Feature properties contribution.

        Auto-derived from this sidecar's ``attribute_schema``; the user-data
        wire shape is derived from ``ItemsSchema`` (the SSOT) and overlays
        this fragment at the service layer (#976).
        """
        schema: Dict[str, Any] = {}

        if self.config.attribute_schema:
            storage_only = getattr(self.config, "storage_only_fields", None) or []
            for attr in self.config.attribute_schema:
                if attr.name not in storage_only:
                    schema[attr.name] = {
                        "type": self._map_pg_to_json_type(attr.type),
                        "description": str(attr.description)
                        if attr.description
                        else f"{attr.name} attribute",
                    }
                    if not attr.nullable:
                        schema[attr.name]["required"] = True

        return schema

    def _map_pg_to_json_type(self, pg_type: PostgresType) -> str:
        """Map PostgreSQL type to JSON Schema type."""
        return map_pg_to_json_type(pg_type)

    # --- Partitioning & Capabilities ---

    def get_partition_keys(self) -> List[str]:
        """Returns list of columns this sidecar contributes to composite partitioning."""
        return self.config.partition_keys

    def get_partition_key_types(self) -> Dict[str, str]:
        """Returns mapping of partition keys to their SQL types."""
        return self.config.partition_key_types

    def has_validity(self) -> bool:
        """Returns True if this sidecar manages temporal validity.

        SSOT: ``ItemsWritePolicy.validity`` (null-object ValiditySpec) — its
        PRESENCE enables validity; the PG driver sets its own fixed
        ``validity`` column on ``self.config.validity_column`` at DDL time
        (#1168); ``config.enable_validity`` is the derived bool over that field.
        """
        return self.config.has_validity

    def get_ddl(
        self,
        physical_table: str,
        partition_keys: List[str] = [],
        partition_key_types: Dict[str, str] = {},
        has_validity: bool = False,
    ) -> str:
        """Generate DDL for attribute sidecar table."""
        table_name = f"{physical_table}_{self.sidecar_id}"
        columns = ["geoid UUID NOT NULL"]
        known_columns = {"geoid"}

        # Add Identity Columns (null-object: field name present → column enabled)
        if self.config.external_id_field is not None:
            columns.append(f"{self.config.external_id_field} VARCHAR(255)")
            known_columns.add(self.config.external_id_field)

        if self.config.asset_id_field is not None:
            columns.append(f"{self.config.asset_id_field} VARCHAR(255)")
            known_columns.add(self.config.asset_id_field)

        # Add Attribute Storage
        indexes = []
        if self.resolved_storage_mode == AttributeStorageMode.COLUMNAR:
            # Mode A: Add physical columns from schema
            if self.config.attribute_schema:
                for attr in self.config.attribute_schema:
                    null_constraint = "" if attr.nullable else " NOT NULL"
                    sql_default = attr.get_sql_default()
                    default_clause = f" DEFAULT {sql_default}" if sql_default else ""

                    unique_constraint = " UNIQUE" if attr.unique else ""
                    # Quote the column name so its case is preserved: DML
                    # (item_distributed._upsert_sidecar_table_raw) and SELECT
                    # both quote it, so an unquoted DDL here would fold e.g.
                    # "Area" to "area" and break the round-trip (geoid #719).
                    columns.append(
                        f'"{attr.name}" {attr.type.value}{null_constraint}{default_clause}{unique_constraint}'
                    )
                    known_columns.add(attr.name)

                    # Create index if specified
                    if attr.index != AttributeIndexType.NONE:
                        idx_type = attr.index.value.upper()
                        idx_clause = f"USING {idx_type}" if idx_type != "BTREE" else ""
                        indexes.append(
                            f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_{attr.name}" ON {{schema}}."{table_name}" '
                            f'{idx_clause} ("{attr.name}")'
                        )
        else:
            # Mode B: JSONB column
            columns.append(f"{self.config.jsonb_column_name} JSONB")
            known_columns.add(self.config.jsonb_column_name)

            # attributes_hash STORED GENERATED column — SHA256 of the
            # canonicalised JSONB (PG keeps jsonb internally normalised so
            # ``jsonb::text`` is deterministic for a given value).  Powers
            # ``ComputedKind.ATTRIBUTES_HASH`` for "same attribute
            # combination, regardless of geometry" deduplication. Requires
            # pgcrypto, which ``ensure_init_db`` enables at boot.
            columns.append(
                "attributes_hash CHAR(64) GENERATED ALWAYS AS "
                f"(encode(digest({self.config.jsonb_column_name}::text, 'sha256'), 'hex')) STORED"
            )
            known_columns.add("attributes_hash")
            indexes.append(
                f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_attributes_hash" '
                f'ON {{schema}}."{table_name}" (attributes_hash)'
            )

            # GIN index for full document containment (if not overridden by functional indexes)
            # Standard GIN
            indexes.append(
                f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_attrs_gin" ON {{schema}}."{table_name}" USING GIN({self.config.jsonb_column_name})'
            )

            # Functional B-Tree indexes on specific JSON paths for trillion-row scale
            for path, cast_type in self.config.jsonb_indexed_paths.items():
                indexes.append(
                    f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_{path}" ON {{schema}}."{table_name}" '
                    f"(({self.config.jsonb_column_name}->>'{path}')::{cast_type.value})"
                )

        # Attribute Statistics (ATTRIBUTE_STAT overlay) — independent of the
        # COLUMNAR/JSONB *attribute* mode above. JSONB stats share an
        # ``attribute_stats`` blob; COLUMNAR stats each get a numeric column
        # (+ optional B-tree). Mirrors the geometries sidecar's geom_stats. #1074
        if self._has_jsonb_stats() and "attribute_stats" not in known_columns:
            columns.append("attribute_stats JSONB")
            known_columns.add("attribute_stats")
        for stat in self._columnar_stat_fields():
            stat_col = stat.resolved_name
            if stat_col in known_columns:
                continue
            columns.append(f'"{stat_col}" {self._STAT_COLUMNAR_SQL_TYPE}')
            known_columns.add(stat_col)
            if stat.indexed:
                indexes.append(
                    f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_{stat_col}" '
                    f'ON {{schema}}."{table_name}" ("{stat_col}")'
                )

        # Prepare validity column
        validity_col = ""
        if has_validity or "validity" in partition_keys:
            if "validity" not in known_columns:
                # validity_col = '"validity" TSTZRANGE NOT NULL,' # WRONG: we need to add it to columns list
                columns.append(
                    "\"validity\" TSTZRANGE NOT NULL DEFAULT tstzrange(CURRENT_TIMESTAMP, NULL, '[)')"
                )
                known_columns.add("validity")

        # Composite PK Construction
        # Rule: Partition keys FIRST, then Hub identity (geoid, validity)
        pk_columns = []
        partition_clause = ""

        if partition_keys:
            # 1. Add any non-sidecar partition keys (e.g. catalog_id from Hub)
            for key in partition_keys:
                if key not in known_columns:
                    col_type = partition_key_types.get(key, "TEXT")
                    columns.insert(0, f'"{key}" {col_type} NOT NULL')
                    known_columns.add(key)

            # 2. Partitions must be part of the PK
            pk_set = set()
            for key in partition_keys:
                pk_columns.append(f'"{key}"')
                pk_set.add(key)

            # 3. Complete the PK with identity if not already present
            if "geoid" not in pk_set:
                pk_columns.append('"geoid"')
                pk_set.add("geoid")
            if (
                has_validity or "validity" in partition_keys
            ) and "validity" not in pk_set:
                pk_columns.append('"validity"')
                pk_set.add("validity")

            partition_clause = (
                f" PARTITION BY LIST ({', '.join([f'"{k}"' for k in partition_keys])})"
            )
        else:
            partition_clause = ""
            # Non-partitioned table: PK is geoid + validity (if has_validity)
            pk_columns = ['"geoid"']
            if has_validity or "validity" in partition_keys:
                pk_columns.append('"validity"')

        # Build DDL
        storage_params = ""
        if (
            self.resolved_storage_mode == AttributeStorageMode.JSONB
            and self.config.use_hot_updates
        ):
            storage_params = " WITH (FILLFACTOR=80)"

        ddl = f"""
CREATE TABLE IF NOT EXISTS {{schema}}."{table_name}" (
    {", ".join(columns)},
    PRIMARY KEY ({", ".join(pk_columns)})
){partition_clause}{storage_params};
"""

        # Foreign Key to Hub - only if validity matches or only geoid
        ref_cols = ["geoid"]

        # Determine if validity should be in FK
        # Only if validity is a partition key (shared with Hub)
        if "validity" in partition_keys:
            ref_cols.append("validity")

        ddl += f"""
ALTER TABLE {{schema}}."{table_name}"
ADD CONSTRAINT "fk_{table_name}_geoid"
FOREIGN KEY ({", ".join([f'"{c}"' for c in ref_cols])}) REFERENCES {{schema}}."{physical_table}" ON DELETE CASCADE;
"""

        # Add Indices
        # Identity Indices
        if self.config.external_id_field is not None and self.config.index_external_id:
            # external_id is unique WITHIN a PK scope (e.g. per validity window
            # for versioned tables), not globally per geoid. Including the full
            # PK in the unique index keeps it compatible with the upsert's
            # ON CONFLICT target and allows re-materialization with a fresh
            # validity range to INSERT instead of colliding.
            ext_col = self.config.external_id_field
            ext_id_cols = pk_columns + [ext_col] if pk_columns else [ext_col]
            ddl += f'\nCREATE UNIQUE INDEX IF NOT EXISTS "idx_{table_name}_ext_id" ON {{schema}}."{table_name}" ({", ".join(ext_id_cols)});'

        if self.config.asset_id_field is not None and self.config.index_asset_id:
            asset_col = self.config.asset_id_field
            ddl += f'\nCREATE INDEX IF NOT EXISTS "idx_{table_name}_asset_id" ON {{schema}}."{table_name}" ({asset_col});'
            # Note: We cannot add a simple FK to assets(asset_id) because assets table is partitioned by collection_id
            # and thus PK is (collection_id, asset_id).
            # Integrity is enforced via trigger (trg_asset_cleanup).

        # Add Computed Indices from schema/paths
        for idx_stmt in indexes:
            ddl += f"\n{idx_stmt};"

        return ddl

    async def setup_lifecycle_hooks(
        self, conn: DbResource, schema: str, table_name: str
    ) -> None:
        """Setup maintenance for attribute table."""
        if self.resolved_storage_mode == AttributeStorageMode.JSONB:
            logger.info(
                f"JSONB table {schema}.{table_name} configured with FILLFACTOR=80"
            )

        # Setup Asset Cleanup Trigger if asset_id column is enabled
        if self.config.asset_id_field is not None:
            logger.info(f"Setting up asset cleanup trigger for {schema}.{table_name}")
            am = get_protocol(AssetsProtocol)
            if am:
                from dynastore.models.driver_context import DriverContext
                await am.ensure_asset_cleanup_trigger(
                    schema, table_name, ctx=DriverContext(db_resource=conn) if conn is not None else None,
                )
            else:
                logger.warning(
                    f"AssetsProtocol not available. Skipping asset cleanup trigger for {schema}.{table_name}"
                )

    async def on_partition_create(
        self,
        conn: DbResource,
        schema: str,
        parent_table: str,
        partition_table: str,
        partition_value: Any,
    ) -> None:
        """Partition-specific setup."""
        pass

    def resolve_query_path(self, attr_name: str) -> Optional[Tuple[str, str]]:
        """Resolve attribute access."""
        # QueryOptimizer alias convention: sc_{sidecar_id} -> sc_attributes
        alias = f"sc_{self.sidecar_id}"

        # Hub Columns (Special handling: we return them with 'h' alias but associate with this sidecar?)
        # Or we return None and let ItemService handle it?
        # If ItemService falls through to sidecars, we must handle it if we want to support it via this path.
        if attr_name == "transaction_time":
            return ("h.transaction_time", "h")

        # Identity and Versioning
        if attr_name == "validity" and self.config.enable_validity:
            return (f"{alias}.validity", alias)

        # Identity Columns (null-object: field name present → column enabled)
        if self.config.external_id_field is not None and attr_name == self.config.external_id_field:
            return (f"{alias}.{self.config.external_id_field}", alias)
        if self.config.asset_id_field is not None and attr_name == self.config.asset_id_field:
            return (f"{alias}.{self.config.asset_id_field}", alias)

        # Columnar Mode
        if self.resolved_storage_mode == AttributeStorageMode.COLUMNAR:
            if self.config.attribute_schema:
                for attr in self.config.attribute_schema:
                    if attr.name == attr_name:
                        # Quoted: the column is created quoted (case-preserved),
                        # so filter/sort references must quote it too (#719).
                        return (f'{alias}."{attr_name}"', alias)
        # JSONB Mode
        else:
            if attr_name == self.config.jsonb_column_name:
                return (f"{alias}.{self.config.jsonb_column_name}", alias)

            # Check if it's a known functional path
            if attr_name in self.config.jsonb_indexed_paths:
                cast_type = self.config.jsonb_indexed_paths[attr_name].value
                return (
                    f"({alias}.{self.config.jsonb_column_name}->>'{attr_name}')::{cast_type}",
                    alias,
                )

            # Nested JSON path fallback
            if attr_name.startswith("properties."):
                json_key = attr_name.split(".", 1)[1]
                return (
                    f"{alias}.{self.config.jsonb_column_name}->>'{json_key}'",
                    alias,
                )
            else:
                return (
                    f"{alias}.{self.config.jsonb_column_name}->>'{attr_name}'",
                    alias,
                )

        return None

    def get_select_fields(
        self,
        request: Optional[QueryRequest] = None,
        hub_alias: str = "h",
        sidecar_alias: Optional[str] = None,
        include_all: bool = False,
    ) -> List[str]:
        """Returns SELECT field expressions for attributes sidecar."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        fields = []

        if request is not None and not include_all:
            # Selective mode: only return what's needed
            requested = {sel.field for sel in request.select} if hasattr(request, "select") and request.select else set()
            filter_fields = {f.field for f in request.filters} if hasattr(request, "filters") and request.filters else set()
            sort_fields = {s.field for s in (request.sort or [])} if hasattr(request, "sort") and request.sort else set()
            all_needed = requested | filter_fields | sort_fields

            # 1. Identity Columns (null-object: field name present → column enabled)
            if self.config.external_id_field is not None and (
                self.config.external_id_field in all_needed
                or "id" in all_needed
                or "*" in requested
                or not requested
            ):
                fields.append(f"{alias}.{self.config.external_id_field}")

            if self.config.asset_id_field is not None and (
                self.config.asset_id_field in all_needed or "*" in requested
            ):
                fields.append(f"{alias}.{self.config.asset_id_field}")

            # 2. Attribute Columns
            storage_mode = self.resolved_storage_mode
            if storage_mode == AttributeStorageMode.JSONB:
                fields.append(f"{alias}.{self.config.jsonb_column_name}")
            else:
                # Relational mode: return selectively
                if self.config.attribute_schema:
                    for attr in self.config.attribute_schema:
                        if attr.name in all_needed or "*" in requested or not requested:
                            fields.append(f'{alias}."{attr.name}"')

            # 3. Attribute Statistics (ATTRIBUTE_STAT overlay) — COLUMNAR per
            # resolved name, JSONB via the shared ``attribute_stats`` column.
            # Gated like the geometries sidecar's geom_stats projection. #1074
            for stat in self._columnar_stat_fields():
                stat_key = stat.resolved_name
                if stat_key not in all_needed and "*" not in requested:
                    continue
                fields.append(f'{alias}."{stat_key}"')
            if self._has_jsonb_stats() and (
                "attribute_stats" in all_needed or "*" in requested
            ):
                fields.append(f"{alias}.attribute_stats")
        else:
            # Full mode: return all fields (existing behavior)
            if self.config.external_id_field is not None:
                fields.append(f"{alias}.{self.config.external_id_field}")

            if self.config.asset_id_field is not None:
                fields.append(f"{alias}.{self.config.asset_id_field}")

            storage_mode = self.resolved_storage_mode
            if storage_mode == AttributeStorageMode.JSONB:
                fields.append(f"{alias}.{self.config.jsonb_column_name}")
            else:
                if self.config.attribute_schema:
                    for attr in self.config.attribute_schema:
                        fields.append(f'{alias}."{attr.name}"')

            # Attribute Statistics — full mode projects all storage-bearing
            # fields (COLUMNAR columns + the shared ``attribute_stats`` blob). #1074
            for stat in self._columnar_stat_fields():
                fields.append(f'{alias}."{stat.resolved_name}"')
            if self._has_jsonb_stats():
                fields.append(f"{alias}.attribute_stats")

        return fields

    def get_join_clause(
        self,
        schema: str,
        hub_table: str,
        hub_alias: str = "h",
        sidecar_alias: Optional[str] = None,
        join_type: str = "LEFT",
        extra_condition: Optional[str] = None,
    ) -> str:
        """Returns JOIN clause for attributes sidecar (geoid-only join)."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        table = f"{hub_table}_{self.sidecar_id}"
        on_clause = f"{hub_alias}.geoid = {alias}.geoid"
        if self.config.enable_validity:
            on_clause = (
                f"{on_clause} AND {alias}.validity @> {hub_alias}.transaction_time"
            )
        if extra_condition:
            on_clause = f"{on_clause} {extra_condition}"
        return f'{join_type} JOIN "{schema}"."{table}" {alias} ON {on_clause}'

    def get_where_conditions(
        self, sidecar_alias: Optional[str] = None, **filters
    ) -> List[str]:
        """Returns WHERE clause conditions for filtering."""
        conditions = []
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        if "external_id" in filters and filters["external_id"] is not None:
            conditions.append(f"{alias}.external_id = :external_id")
        if "asset_id" in filters and filters["asset_id"] is not None:
            conditions.append(f"{alias}.asset_id = :asset_id")
        return conditions

    def get_feature_id_condition(
        self,
        feature_ids: Union[str, List[str]],
        hub_alias: str = "h",
        sidecar_alias: Optional[str] = None,
        partition_keys: Optional[List[str]] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Returns JOIN and WHERE conditions for feature ID resolution.

        For attributes sidecar:
        - JOIN condition includes partition key matches and temporal validity
        - WHERE condition matches the external_id(s)
        - Using hub_alias.transaction_time ensures we match the sidecar record
          that was active at the time of the hub record creation.
        """
        if not self.config.feature_id_field_name:
            return (None, None)
        if not self.provides_feature_id:
            return (None, None)

        alias = sidecar_alias or f"sc_{self.sidecar_id}"

        # Build JOIN condition components
        join_conditions = []

        # 1. Partition key matches (if collection is partitioned)
        if partition_keys:
            for pk in partition_keys:
                join_conditions.append(f"{alias}.{pk} = {hub_alias}.{pk}")

        # Combine JOIN conditions
        join_clause = " AND ".join(join_conditions) if join_conditions else None
        # Note: We don't add validity check here as it's already in get_join_clause
        if join_clause:
            join_clause = f"AND {join_clause}"

        # WHERE condition: match external_id (singular or array)
        # AND ensure we only get active records if validity is enabled
        # If we query by ID, we generally want the current state.
        validity_filter = ""
        # FIX: The test item has validity=NULL (infinite), but lower/upper are usually timestamps.
        # upper(validity) IS NULL means infinite future -> active.
        # But if the item was inserted without explicit validity, does it default to [now, NULL)?
        # In get_ddl, it's NOT NULL.
        # But verify logic logic.
        if self.config.enable_validity:
            validity_filter = f"AND (upper({alias}.validity) IS NULL OR upper({alias}.validity) > NOW())"

        if isinstance(feature_ids, list):
            # Check both external_id and fallback to hub geoid
            where_clause = f"({alias}.external_id::text = ANY(CAST(:item_ids AS TEXT[])) OR {hub_alias}.geoid::text = ANY(CAST(:item_ids AS TEXT[]))) {validity_filter}"
        else:
            # Check both external_id and fallback to hub geoid
            where_clause = f"({alias}.external_id::text = CAST(:item_id AS TEXT) OR {hub_alias}.geoid::text = CAST(:item_id AS TEXT)) {validity_filter}"

        return (join_clause, where_clause)

    def get_order_by_fields(self, sidecar_alias: Optional[str] = None) -> List[str]:
        """Returns fields that can be used for ordering."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        fields = []

        if self.config.external_id_field is not None:
            fields.append(f"{alias}.{self.config.external_id_field}")

        if self.config.asset_id_field is not None:
            fields.append(f"{alias}.{self.config.asset_id_field}")

        # Columnar mode attributes can be ordered
        if (
            self.resolved_storage_mode == AttributeStorageMode.COLUMNAR
            and self.config.attribute_schema
        ):
            for attr in self.config.attribute_schema:
                fields.append(f"{alias}.{attr.name}")

        return fields

    def get_group_by_fields(self, sidecar_alias: Optional[str] = None) -> List[str]:
        """Returns fields that can be used for grouping."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        fields = []

        if self.config.external_id_field is not None:
            fields.append(f"{alias}.{self.config.external_id_field}")

        if self.config.asset_id_field is not None:
            fields.append(f"{alias}.{self.config.asset_id_field}")

        # Columnar mode attributes can be grouped
        if (
            self.resolved_storage_mode == AttributeStorageMode.COLUMNAR
            and self.config.attribute_schema
        ):
            for attr in self.config.attribute_schema:
                fields.append(f"{alias}.{attr.name}")

        return fields

    def get_field_definitions(
        self, sidecar_alias: Optional[str] = None
    ) -> Dict[str, FieldDefinition]:
        """Returns capabilities of attribute fields and identity mapping."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        fields = {}

        # 1. Identity Columns (null-object: field name present → column enabled)
        if self.config.external_id_field is not None:
            ext_col = self.config.external_id_field
            fields[ext_col] = FieldDefinition(
                name=ext_col,
                alias="id",  # Standard external name for STAC/OGC
                title=LocalizedText(
                    en="External ID",
                    fr="Identifiant Externe",
                    es="Identificador Externo",
                ),
                description=LocalizedText(
                    en="The unique identifier of the feature in the source system.",
                    fr="L'identifiant unique de l'entité dans le système source.",
                    es="El identificador único de la entidad en el sistema de origen.",
                ),
                sql_expression=f"{alias}.{ext_col}",
                capabilities=[
                    FieldCapability.FILTERABLE,
                    FieldCapability.SORTABLE,
                    FieldCapability.GROUPABLE,
                    FieldCapability.INDEXED,
                ],
                data_type="string",
                aggregations=["count", "array_agg"],
                expose=True,
            )

        if self.config.asset_id_field is not None:
            asset_col = self.config.asset_id_field
            fields[asset_col] = FieldDefinition(
                name=asset_col,
                alias=asset_col,
                title=LocalizedText(
                    en="Asset ID",
                    fr="Identifiant de l'Actif",
                    es="Identificador del Activo",
                ),
                description=LocalizedText(
                    en="The identifier of the associated physical asset.",
                    fr="L'identifiant de l'actif physique associé.",
                    es="El identificador del activo físico asociado.",
                ),
                sql_expression=f"{alias}.{asset_col}",
                capabilities=[
                    FieldCapability.FILTERABLE,
                    FieldCapability.SORTABLE,
                    FieldCapability.GROUPABLE,
                    FieldCapability.INDEXED,
                ],
                data_type="string",
                aggregations=["count", "array_agg"],
                expose=True,
            )

        # 3. Validity Mapping (Bi-temporal exposure)
        if self.config.enable_validity:
            # start_datetime mapping
            fields["start_datetime"] = FieldDefinition(
                name="validity_start",
                alias="start_datetime",
                title=LocalizedText(
                    en="Valid From", fr="Valide à partir de", es="Válido desde"
                ),
                description=LocalizedText(
                    en="Start of the temporal validity period.",
                    fr="Début de la période de validité temporelle.",
                    es="Inicio del periodo de validez temporal.",
                ),
                sql_expression=f"lower({alias}.validity)",
                capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
                data_type="timestamp",
                expose=True,
            )
            # end_datetime mapping
            fields["end_datetime"] = FieldDefinition(
                name="validity_end",
                alias="end_datetime",
                title=LocalizedText(
                    en="Valid To", fr="Valide jusqu'à", es="Válido hasta"
                ),
                description=LocalizedText(
                    en="End of the temporal validity period.",
                    fr="Fin de la période de validité temporelle.",
                    es="Fin del periodo de validez temporal.",
                ),
                sql_expression=f"upper({alias}.validity)",
                capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
                data_type="timestamp",
                expose=True,
            )
            # validity range mapping
            fields["validity"] = FieldDefinition(
                name="validity",
                title=LocalizedText(
                    en="Temporal Validity",
                    fr="Validité Temporelle",
                    es="Validez Temporal",
                ),
                description=LocalizedText(
                    en="The temporal validity range of the record.",
                    fr="La période de validité temporelle de l'enregistrement.",
                    es="El rango de validez temporal del registro.",
                ),
                sql_expression=f"{alias}.validity",
                capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
                # No canonical range type; the validity range is exposed as a
                # timestamp field (precise bounds carried by start/end_datetime).
                data_type="timestamp",
                expose=True,
            )

        # 4. Standard Hub Temporal Info
        fields["transaction_time"] = FieldDefinition(
            name="transaction_time",
            title=LocalizedText(
                en="Transaction Time",
                fr="Temps de Transaction",
                es="Tiempo de Transacción",
            ),
            description=LocalizedText(
                en="The time when this record was recorded in the database.",
                fr="Le moment où cet enregistrement a été consigné dans la base de données.",
                es="El momento en que este registro fue grabado en la base de datos.",
            ),
            sql_expression="h.transaction_time",
            capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
            data_type="timestamp",
            expose=True,
        )

        # 5. Columnar Mode Attributes
        if (
            self.resolved_storage_mode == AttributeStorageMode.COLUMNAR
            and self.config.attribute_schema
        ):
            for attr in self.config.attribute_schema:
                caps = [
                    FieldCapability.FILTERABLE,
                    FieldCapability.SORTABLE,
                    FieldCapability.GROUPABLE,
                ]
                if attr.index != AttributeIndexType.NONE:
                    caps.append(FieldCapability.INDEXED)

                aggs = ["count", "array_agg"]
                transforms = []

                if attr.type in [
                    PostgresType.INTEGER,
                    PostgresType.BIGINT,
                    PostgresType.NUMERIC,
                    PostgresType.FLOAT,
                ]:
                    caps.append(FieldCapability.AGGREGATABLE)
                    aggs.extend(["sum", "avg", "min", "max"])
                elif attr.type in [PostgresType.TEXT, PostgresType.VARCHAR_255]:
                    transforms = None

                fields[attr.name] = FieldDefinition(
                    name=attr.name,
                    # Quote the identifier — see note in get_queryable_fields.
                    sql_expression=f'{alias}."{attr.name}"',
                    capabilities=caps,
                    data_type=pg_native_to_canonical(attr.type.value),
                    aggregations=aggs,
                    transformations=transforms,
                    expose=True,  # Standard attributes exposed by default
                )

        # 6. Attribute Statistics (ATTRIBUTE_STAT overlay) — COLUMNAR stats are
        # queryable (filter/sort/aggregate); their values reach Feature output
        # only via ``ItemsReadPolicy.feature_type.expose`` (expose=False here),
        # never as default properties. JSONB stats live inside the shared
        # ``attribute_stats`` blob and are not individually defined. #1074
        for stat in self._columnar_stat_fields():
            stat_key = stat.resolved_name
            stat_caps = [
                FieldCapability.FILTERABLE,
                FieldCapability.SORTABLE,
                FieldCapability.GROUPABLE,
                FieldCapability.AGGREGATABLE,
            ]
            if stat.indexed:
                stat_caps.append(FieldCapability.INDEXED)
            fields[stat_key] = FieldDefinition(
                name=stat_key,
                sql_expression=f'{alias}."{stat_key}"',
                capabilities=stat_caps,
                data_type="numeric",
                aggregations=["count", "sum", "avg", "min", "max"],
                expose=False,
            )

        return fields

    def jsonb_property_field(
        self,
        name: str,
        fd: FieldDefinition,
        *,
        sidecar_alias: Optional[str] = None,
    ) -> FieldDefinition:
        """Build a ``FieldDefinition`` that resolves ``name`` via JSONB extraction.

        Used by :class:`...catalog.QueryOptimizer` to enrich its field index
        from ``ItemsSchema.fields`` — schema is the SSOT for what a collection
        exposes; entries not materialised as a native column live in the JSONB
        attributes blob. The cast token is derived from ``fd.data_type`` via
        the canonical SSOT (:data:`CANONICAL_TO_PG_DDL`), so a typed compare
        (e.g. ``start_date <= now()``) parses correctly instead of producing
        a text-vs-date error at execution time.
        """
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        canonical = (fd.data_type or "string").lower()
        pg_type = CANONICAL_TO_PG_DDL.get(canonical, "TEXT")
        sql_expr = (
            f"({alias}.{self.config.jsonb_column_name}->>'{name}')::{pg_type}"
        )
        caps = [
            FieldCapability.FILTERABLE,
            FieldCapability.SORTABLE,
            FieldCapability.GROUPABLE,
        ]
        return FieldDefinition(
            name=name,
            sql_expression=sql_expr,
            capabilities=caps,
            data_type=canonical,
            expose=getattr(fd, "expose", True),
            title=getattr(fd, "title", None),
            description=getattr(fd, "description", None),
        )

    def get_dynamic_field_definition(
        self, field_name: str, sidecar_alias: Optional[str] = None
    ) -> Optional[FieldDefinition]:
        """
        Dynamically resolves fields for JSONB storage mode.
        """
        if self.resolved_storage_mode != AttributeStorageMode.JSONB:
            return None

        alias = sidecar_alias or f"sc_{self.sidecar_id}"

        # Determine SQL expression
        sql_expr = None
        data_type = "string"  # JSONB extraction defaults to string unless cast
        if field_name == self.config.jsonb_column_name:
            # The blob column itself — return the whole JSONB, not a sub-key.
            # Mirrors ``resolve_query_path``.
            sql_expr = f"{alias}.{self.config.jsonb_column_name}"
            data_type = "jsonb"
        elif field_name in self.config.jsonb_indexed_paths:
            cast_type = self.config.jsonb_indexed_paths[field_name].value
            sql_expr = f"({alias}.{self.config.jsonb_column_name}->>'{field_name}')::{cast_type}"
        elif field_name.startswith("properties."):
            json_key = field_name.split(".", 1)[1]
            sql_expr = f"{alias}.{self.config.jsonb_column_name}->>'{json_key}'"
        else:
            # Assume root level property
            sql_expr = f"{alias}.{self.config.jsonb_column_name}->>'{field_name}'"

        return FieldDefinition(
            name=field_name,
            sql_expression=sql_expr,
            capabilities=[
                FieldCapability.FILTERABLE,
                FieldCapability.SORTABLE,
                FieldCapability.GROUPABLE,
            ],
            data_type=data_type,
            aggregations=["count", "array_agg"],  # Basic aggregations
            expose=True,
        )

    def get_default_sort(self) -> Optional[List[Tuple[str, str]]]:
        if self.config.external_id_field is not None:
            return [(self.config.external_id_field, "ASC")]
        return None

    def prepare_upsert_payload(
        self, feature: Union[Feature, Dict[str, Any]], context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Extract attributes and identity from feature."""
        # 1. Identity Extraction & Mapping
        #
        # Resolution precedence:
        #   1. Real ``geojson_pydantic.Feature`` -> ``feature.id`` (the
        #      top-level GeoJSON id member is the canonical identity).
        #   2. Dict / pydantic shape with no policy override -> fall back
        #      to the top-level ``id`` key. This matches the GeoJSON
        #      contract and preserves the pre-#940 default of treating
        #      ``"id"`` as the implicit identity path when the operator
        #      hasn't configured an ``ItemsWritePolicy.external_id_field``.
        #   3. Policy-bound path -> ``_extract_value`` walks the path.
        if isinstance(feature, Feature):
            ext_id = feature.id
        else:
            field_path = _resolve_external_id_field(context)
            if field_path:
                ext_id = self._extract_value(feature, field_path)
            elif isinstance(feature, dict):
                ext_id = feature.get("id")
            else:
                ext_id = getattr(feature, "id", None)

        # Fallback to context if not in feature
        if ext_id is None:
            ext_id = context.get("external_id")

        if self.config.enable_external_id and ext_id:
            ext_id_str = str(ext_id)
            context["external_id"] = ext_id_str

        # 2. Resolve Geoid from Context
        geoid = context.get("geoid")
        if not geoid:
            raise ValueError("geoid is required in context")

        payload = {"geoid": geoid}

        # 3. Partition Key check
        if context.get("partition_key_name"):
            payload[context["partition_key_name"]] = context["partition_key_value"]

        # 4. Identity Column Storage (null-object: field name present → column enabled)
        if self.config.external_id_field is not None and ext_id:
            payload[self.config.external_id_field] = str(ext_id)

        if self.config.asset_id_field is not None:
            # IMPORTANT: asset_id is ONLY extracted from context, not feature body.
            asset_id = context.get("asset_id")
            if asset_id:
                asset_id_str = str(asset_id)
                payload[self.config.asset_id_field] = asset_id_str
                context["asset_id"] = asset_id_str

        # 5. Temporal Fields extraction (Supports JSON-FG 'time' member)
        feature_time = getattr(feature, "time", None)
        if feature_time:
            # JSON-FG time can be an instant or interval. For simplicity in mapping to validity:
            if isinstance(feature_time, dict):
                valid_from = feature_time.get("date") or feature_time.get("timestamp")
                valid_to = None
            elif isinstance(feature_time, (list, tuple)) and len(feature_time) == 2:
                valid_from = feature_time[0]
                valid_to = feature_time[1]
            else:
                valid_from = feature_time
                valid_to = None
        elif isinstance(feature, Feature):
            props = feature.properties or {}
            valid_from = props.get("valid_from") or props.get("datetime")
            valid_to = props.get("valid_to")
        else:
            time_val = feature.get("time")
            if time_val:
                if isinstance(time_val, dict):
                    valid_from = time_val.get("date") or time_val.get("timestamp")
                    valid_to = None
                elif isinstance(time_val, (list, tuple)) and len(time_val) == 2:
                    valid_from = time_val[0]
                    valid_to = time_val[1]
                else:
                    valid_from = time_val
                    valid_to = None
            else:
                valid_from = (
                    feature.get("valid_from")
                    or feature.get("properties", {}).get("valid_from")
                    or feature.get("properties", {}).get("datetime")
                )
                valid_to = feature.get("valid_to") or feature.get("properties", {}).get(
                    "valid_to"
                )

        # 5b. Explicit value-source override (#1126/#1172 ValiditySpec.start_from
        # / end_from). Each bound is independent:
        #   - "context"  → keep the heuristic resolution above (the write-context
        #                  default is injected downstream by finalize_payload);
        #   - any path   → extract the bound VALUE from the feature at that
        #                  dotted path, overriding the heuristic resolution.
        # ``start_from=None`` is the open-lower-bound case: it has no value
        # source, so the heuristically resolved start is dropped here and
        # ``finalize_upsert_payload`` builds ``tstzrange(NULL, …)`` (#1172).
        # ``end_from=None`` is the historical default and is intentionally NOT
        # forced here — an absent end value already yields an open upper bound,
        # while a feature-provided end is still honoured by the heuristic.
        start_from = getattr(self.config, "validity_start_from", "context")
        end_from = getattr(self.config, "validity_end_from", None)
        needs_path_walk = (start_from not in (None, "context")) or (
            end_from not in (None, "context")
        )
        if needs_path_walk:
            feature_dict_for_paths: Dict[str, Any] = (
                feature.model_dump() if isinstance(feature, Feature) else feature
            )
            if start_from not in (None, "context"):
                valid_from = self._extract_value(feature_dict_for_paths, start_from)
            if end_from not in (None, "context"):
                valid_to = self._extract_value(feature_dict_for_paths, end_from)
        if start_from is None:
            valid_from = None  # open lower bound — no start value source

        if valid_from:
            if isinstance(valid_from, str):
                try:
                    from dateutil.parser import isoparse

                    valid_from = isoparse(valid_from)
                except (ValueError, ImportError):
                    logger.warning(f"Failed to parse valid_from string: {valid_from}")
            context["valid_from"] = valid_from

        if valid_to:
            if isinstance(valid_to, str):
                try:
                    from dateutil.parser import isoparse

                    valid_to = isoparse(valid_to)
                except (ValueError, ImportError):
                    logger.warning(f"Failed to parse valid_to string: {valid_to}")
            context["valid_to"] = valid_to

        # 6. Attributes Extraction
        feature_as_dict: Dict[str, Any] = (
            feature.model_dump() if isinstance(feature, Feature) else feature
        )
        if isinstance(feature, Feature):
            properties = feature.properties
        else:
            properties = feature.get("properties", {})

        if self.resolved_storage_mode == AttributeStorageMode.COLUMNAR:
            if self.config.attribute_schema:
                for attr in self.config.attribute_schema:
                    val = self._extract_value(
                        feature_as_dict, f"properties.{attr.name}"
                    ) or feature_as_dict.get(attr.name)

                    # Apply default if missing
                    if val is None:
                        val = attr.default

                    if val is not None and val is not PydanticUndefined:
                        payload[attr.name] = val
        else:
            # JSONB Mode: Clean duplicates
            props_to_save = dict(properties or {})

            # Apply defaults from schema if present (even in JSONB mode)
            if self.config.attribute_schema:
                for attr in self.config.attribute_schema:
                    if (
                        attr.name not in props_to_save
                        or props_to_save[attr.name] is None
                    ):
                        if attr.default is not None:
                            props_to_save[attr.name] = attr.default

            if self.config.external_id_field is not None:
                ext_col = self.config.external_id_field
                field_path = _resolve_external_id_field(context)
                if field_path and "." not in field_path and field_path in props_to_save:
                    del props_to_save[field_path]

                ext_id = context.get("external_id")
                if not ext_id and field_path:
                    ext_id = self._extract_value(feature_as_dict, field_path)
                if ext_id:
                    payload[ext_col] = ext_id

            if self.config.asset_id_field is not None:
                asset_col = self.config.asset_id_field
                # Input property to extract from feature body uses the storage column name.
                if asset_col in props_to_save:
                    del props_to_save[asset_col]

                asset_id = context.get("asset_id")
                if not asset_id:
                    asset_id = self._extract_value(feature_as_dict, asset_col)
                if asset_id:
                    payload[asset_col] = asset_id
                    context["asset_id"] = asset_id

            import json

            # IMPORTANT: We use json.dumps because some database drivers
            # (like asyncpg without specific JSONB codec registration)
            # expect a string for JSONB columns.
            payload[self.config.jsonb_column_name] = json.dumps(
                props_to_save, cls=CustomJSONEncoder
            )

        # Attribute-derived statistics (ATTRIBUTE_STAT overlay) — promote the
        # configured ``properties.<field>`` values into their COLUMNAR column or
        # the shared ``attribute_stats`` JSONB blob. Mirrors the geometries
        # sidecar's geom_stats assignment. #1074
        stat_fields = self._stat_fields()
        if stat_fields:
            from dynastore.tools.geospatial import compute_attribute_derived_fields

            derived = compute_attribute_derived_fields(properties or {}, stat_fields)
            jsonb_stats: Dict[str, Any] = {}
            for stat in stat_fields:
                key = stat.resolved_name
                if key not in derived:
                    continue
                if stat.storage_mode == StatisticStorageMode.JSONB:
                    jsonb_stats[key] = derived[key]
                else:
                    payload[key] = derived[key]
            if jsonb_stats:
                import json as _json_stats

                payload["attribute_stats"] = _json_stats.dumps(
                    jsonb_stats, cls=CustomJSONEncoder
                )

        return payload

    def get_internal_columns(self) -> set:
        """Columns owned by this sidecar that are never part of Feature properties."""
        cols = {"geoid", "external_id", "validity", "transaction_time", "deleted_at",
                "transaction_period", "catalog_id", "collection_id"}
        if self.config.asset_id_field is not None:
            cols.add(self.config.asset_id_field)
        # attributes_hash is write-policy plumbing for ComputedKind.ATTRIBUTES_HASH;
        # never leak it into Feature.properties.  Only present in Mode B (JSONB).
        if self.resolved_storage_mode == AttributeStorageMode.JSONB:
            cols.add("attributes_hash")
        # Attribute-statistics columns surface via the expose loop, never as raw
        # Feature properties. #1074
        if self._has_jsonb_stats():
            cols.add("attribute_stats")
        for stat in self._columnar_stat_fields():
            cols.add(stat.resolved_name)
        return cols

    def producible_computed_names(self) -> set:
        """Attribute-derived computed names this sidecar surfaces at read.

        Mirrors the geometries sidecar: the storage-bearing ATTRIBUTE_STAT
        fields projected by ``get_select_fields`` (COLUMNAR columns or the shared
        ``attribute_stats`` JSONB blob). The pipeline-level exposure loop
        (``SidecarProtocol.apply_exposed_computed_values``) intersects this with
        ``ItemsReadPolicy.feature_type.expose``. #1074
        """
        return {f.resolved_name for f in self._stat_fields()}

    def resolve_computed_value(
        self, row: Dict[str, Any], resolved_name: str
    ) -> Tuple[bool, Any]:
        """Locate an attribute-derived computed value in a read row.

        Returns ``(found, value)``. Mirrors the storage layout decided by
        ``get_select_fields``: COLUMNAR fields surface as a top-level row key;
        JSONB fields live inside the shared ``attribute_stats`` column. ``found``
        is ``False`` only when the layout has no slot for the value at all. #1074
        """
        field = next(
            (f for f in self._stat_fields() if f.resolved_name == resolved_name),
            None,
        )
        if field is None:
            return (False, None)
        if field.storage_mode == StatisticStorageMode.COLUMNAR:
            if resolved_name in row:
                return (True, row[resolved_name])
            return (False, None)
        # JSONB: value nests inside the shared attribute_stats blob.
        blob = row.get("attribute_stats")
        if isinstance(blob, (str, bytes, bytearray)):
            import json as _json

            try:
                blob = _json.loads(blob)
            except Exception:
                return (False, None)
        if isinstance(blob, dict) and resolved_name in blob:
            return (True, blob[resolved_name])
        return (False, None)

    def map_row_to_feature(
        self,
        row: Dict[str, Any],
        feature: Feature,
        context: FeaturePipelineContext,
    ) -> None:
        """Populate Feature from database row.

        Pipeline responsibilities:
          - **Identity**: When the read policy enables it
            (``ItemsReadPolicy.feature_type.external_id_as_feature_id``,
            default True), override ``feature.id`` with the row's
            ``external_id``. The Hub already set ``geoid`` as the
            default.
          - **Properties**: Merge attribute columns / JSONB blob into
            ``feature.properties``.
          - **Context enrichment**: Write all fetched row values into
            ``context["_sidecar_data"]["attributes"]`` so downstream
            sidecars (e.g. STAC) can access them (e.g. ``asset_id``).
        """
        # Publish all raw row values under this sidecar's key in context so that
        # downstream sidecars (e.g. STAC) can access them via context.get_sidecar().
        context.publish(self.sidecar_id, dict(row))

        # 1. Identity Mapping
        # Hub initialised feature.id = geoid — the default id (#1212). Override
        # with the row's external_id ONLY when a collection explicitly opts in
        # via read policy; absent a policy the stable geoid id is kept.
        read_policy = context.get("_items_read_policy") if context else None
        use_external_id = (
            read_policy.feature_type.external_id_as_feature_id
            if read_policy is not None
            else False
        )
        if use_external_id and self.config.external_id_field is not None:
            ext_id = row.get(self.config.external_id_field)
            if ext_id is not None:
                feature.id = str(ext_id)
            # else: keep Hub's geoid-based id

        # 2. Publish asset_id into shared context for downstream sidecars.
        if self.config.asset_id_field is not None:
            asset_id = row.get(self.config.asset_id_field)
            if asset_id is not None:
                context["asset_id"] = asset_id

        # 2. Attributes Mapping (STRICT MODE)
        storage_mode = self.resolved_storage_mode
        props = feature.properties if feature.properties else {}

        if storage_mode == AttributeStorageMode.COLUMNAR:
            # Use schema to identify attribute columns (STRICT)
            if self.config.attribute_schema:
                for attr in self.config.attribute_schema:
                    if attr.name in row:
                        props[attr.name] = row[attr.name]
        else:
            # JSONB Mode (STRICT)
            jsonb_col = self.config.jsonb_column_name
            if jsonb_col in row:
                val = row[jsonb_col]
                if isinstance(val, str):
                    try:
                        import json

                        val = json.loads(val)
                    except Exception:
                        logger.warning(f"Failed to parse JSONB column {jsonb_col}")
                        val = None

                if isinstance(val, dict):
                    # Merge JSONB content into properties
                    new_props = dict(feature.properties) if feature.properties else {}
                    new_props.update(val)

                    # Remove raw column key to avoid nesting
                    new_props.pop(jsonb_col, None)

                    # Explicit re-assignment to trigger Pydantic validation/update
                    feature.properties = new_props

            # JSONB Mode: merge individual non-internal columns that the Optimizer
            # selected but are not already in properties.
            # Use get_internal_columns() dynamically instead of a hardcoded list.
            internal_cols = self.get_internal_columns()
            jsonb_col = self.config.jsonb_column_name
            props = feature.properties if feature.properties is not None else {}
            
            # Delegate exclusion entirely to the pipeline context, which already
            # merged HUB_INTERNAL_COLUMNS + every other sidecar's get_internal_columns().
            excluded = context.all_internal_columns
            for key, val in row.items():
                if key not in internal_cols and key not in excluded and key != jsonb_col and key not in props:
                    if val is not PydanticUndefined and not isinstance(val, (bytes, bytearray)):
                        props[key] = val
            feature.properties = props

        # 3. Time Standardization
        # Map transaction_time -> created (ISO8601 UTC with 'Z'). Opt-in only:
        # surfaced solely when the read policy sets feature_type.expose_created
        # (#1212). By default the response mirrors the input feature 1:1.
        expose_created = (
            read_policy.feature_type.expose_created
            if read_policy is not None
            else False
        )
        if (
            expose_created
            and "transaction_time" in row
            and row["transaction_time"] is not None
        ):
            from datetime import datetime

            tx_time = row["transaction_time"]
            if isinstance(tx_time, datetime):
                # Ensure UTC and format with 'Z'
                if tx_time.tzinfo is None:
                    from datetime import timezone

                    tx_time = tx_time.replace(tzinfo=timezone.utc)
                props["created"] = tx_time.isoformat().replace("+00:00", "Z")
            elif isinstance(tx_time, str):
                # Already formatted, ensure it ends with 'Z'
                props["created"] = (
                    tx_time if tx_time.endswith("Z") else tx_time.replace("+00:00", "Z")
                )

        # 3b. Optionally surface the internal geoid as a ``geoid`` property —
        # only when external_id is the feature id, so the stable geoid stays
        # accessible alongside it. When geoid is already the id this is a no-op
        # (it would merely duplicate the id). Opt-in via the read policy (#1212).
        if (
            use_external_id
            and read_policy is not None
            and read_policy.feature_type.expose_geoid
        ):
            geoid_val = row.get("geoid")
            if geoid_val is not None:
                fp = dict(feature.properties) if feature.properties else {}
                fp["geoid"] = str(geoid_val)
                feature.properties = fp

        # 4. Validity Mapping (if enabled)
        if self.config.enable_validity and "validity" in row:
            validity = row["validity"]
            if validity is not None:
                from datetime import datetime
                from datetime import timezone as dt_timezone

                # Extract start and end from validity range
                # asyncpg returns Range objects, but they might be strings in some contexts
                if hasattr(validity, "lower") and hasattr(validity, "upper"):
                    start = validity.lower
                    end = validity.upper
                elif isinstance(validity, str):
                    # Parse string representation of range
                    # Example: "[2023-01-01 00:00:00+00,2023-12-31 23:59:59+00)"
                    import re

                    match = re.match(r"\[([^,]+),([^\)]+)\)", validity)
                    if match:
                        from dateutil.parser import isoparse

                        try:
                            start = isoparse(match.group(1).strip())
                            end_str = match.group(2).strip()
                            end = isoparse(end_str) if end_str != "infinity" else None
                        except Exception:
                            start = None
                            end = None
                    else:
                        start = None
                        end = None
                else:
                    start = None
                    end = None

                # Map to STAC datetime fields (ISO8601 UTC with 'Z')
                if start is not None:
                    if isinstance(start, datetime):
                        if start.tzinfo is None:
                            start = start.replace(tzinfo=dt_timezone.utc)
                        props["start_datetime"] = start.isoformat().replace(
                            "+00:00", "Z"
                        )

                if end is not None:
                    if isinstance(end, datetime):
                        if end.tzinfo is None:
                            end = end.replace(tzinfo=dt_timezone.utc)
                        props["end_datetime"] = end.isoformat().replace("+00:00", "Z")

        # 5. Update feature properties
        if props:
            feature.properties = props

    async def on_item_created(
        self,
        conn: DbResource,
        physical_schema: str,
        physical_table: str,
        geoid: str,
        feature: Dict[str, Any],
        context: Dict[str, Any],
    ) -> None:
        """
        Hook to link assets if an asset_id was identified during preparation.
        """
        if not self.config.enable_asset_id:
            return

        asset_id = context.get("asset_id")
        if not asset_id:
            return

        try:
            # Import protocol to avoid circular imports
            from dynastore.tools.discovery import get_protocol
            from dynastore.models.protocols import AssetsProtocol

            assets_protocol = get_protocol(AssetsProtocol)
            if not assets_protocol:
                logger.warning(
                    f"AssetsProtocol not available. Cannot link asset {asset_id} to feature {geoid}."
                )
                return

            await assets_protocol.link_asset_to_feature(  # type: ignore[attr-defined]
                physical_schema,
                physical_table,
                geoid=geoid,
                asset_id=asset_id,
                db_resource=conn,
            )
            logger.info(f"Linked asset {asset_id} to feature {geoid}.")

        except Exception as e:
            logger.error(
                f"Failed to link asset {asset_id} to feature {geoid}: {e}",
                exc_info=True,
            )

    def apply_query_context(
        self,
        request: QueryRequest,
        context: Dict[str, Any],
    ) -> None:
        """
        Allows the sidecar to inspect the query request and contribute to the
        query definition (e.g., by adding JOINs or SELECT fields via context).
        """
        alias = f"sc_{self.sidecar_id}"
        
        # Check if requested explicitly or implicitly via select=* / empty select
        requested_fields = {sel.field for sel in request.select} if hasattr(request, "select") and request.select else {"*"}
        filter_fields = {f.field for f in request.filters} if hasattr(request, "filters") and request.filters else set()
        sort_fields = {s.field for s in (request.sort or [])} if hasattr(request, "sort") and request.sort else set()
        all_needed = requested_fields | filter_fields | sort_fields

        # Use get_select_fields logic to populate context
        fields_to_add = self.get_select_fields(request=request, sidecar_alias=alias)
        
        for expr in fields_to_add:
            if expr not in context.get("select_fields", []):
                context.setdefault("select_fields", []).append(expr)

    def get_identity_columns(self) -> List[str]:
        """Returns identity columns for this sidecar (geoid + validity if versioned)."""
        cols = ["geoid"]
        if self.config.enable_validity:
            cols.append("validity")
        return cols

    def finalize_upsert_payload(
        self,
        sc_payload: Dict[str, Any],
        hub_row: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Finalizes attributes payload by injecting validity from Hub or context."""
        payload = sc_payload.copy()

        # Geoid is already in sc_payload from prepare_upsert_payload

        # Inject validity if managed by this sidecar
        if self.config.enable_validity and "validity" not in payload:
            validity = hub_row.get("validity") or context.get("validity")
            if validity:
                payload["validity"] = validity
            else:
                # Check for explicit valid_from/to in context (extracted from properties)
                v_from = context.get("valid_from")
                v_to = context.get("valid_to")

                from datetime import datetime, timezone

                # ``validity_start_from=None`` means an OPEN lower bound: there is
                # no start value source, so the window stays unbounded below
                # (``tstzrange(NULL, v_to)``). Otherwise, a missing start MUST
                # sync with the Hub transaction_time for join consistency — do
                # NOT rely on the DB default CURRENT_TIMESTAMP, which would land
                # later than transaction_time. The feature keeps its ingestion
                # time regardless; this only affects the validity window. (#1172)
                start_is_open = self.config.validity_start_from is None
                if not v_from and not start_is_open:
                    v_from = hub_row.get("transaction_time") or datetime.now(
                        timezone.utc
                    )

                # Driver-agnostic tstzrange wrapper.  asyncpg services
                # use ``asyncpg.Range``; sync (psycopg2 + DatastoreModule)
                # workers don't have asyncpg installed at all.  The
                # psycopg2 equivalent is ``DateTimeTZRange``; both are
                # bound to a ``tstzrange`` column the same way.  As a
                # last resort emit the PG literal ``[lo,hi)`` string —
                # Postgres casts it to tstzrange on insert.
                payload["validity"] = _make_tstzrange(
                    v_from, v_to, lower_inc=True, upper_inc=False,
                )

        return payload

    # --- Query Generation Methods ---

    async def check_upsert_collision(
        self,
        conn: DbResource,
        physical_schema: str,
        physical_table: str,
        processing_context: Dict[str, Any],
        exclude_geoid: Optional[Any] = None,
    ) -> bool:
        """
        Checks for identity collisions (external_id or asset_id).
        """
        alias = f"sc_{self.sidecar_id}"
        table = f"{physical_table}_{self.sidecar_id}"

        # 1. Check Asset ID Collision (if behavior requires it)
        # Note: ItemService handles the check based on ItemsWritePolicy,
        # but here we provide the implementation for specific fields.
        asset_id = processing_context.get("asset_id")
        if asset_id and self.config.asset_id_field is not None:
            asset_col = self.config.asset_id_field
            query = text(f"""
                SELECT 1 FROM "{physical_schema}"."{table}"
                WHERE {asset_col} = :asset_id
                {f"AND geoid != :exclude_geoid" if exclude_geoid else ""}
                LIMIT 1
            """)
            params = {"asset_id": str(asset_id), "exclude_geoid": exclude_geoid}
            if await conn.scalar(query, params):  # type: ignore[union-attr, misc]
                return True

        # 2. Check External ID Collision (for unique constraints if not versioned)
        # Attributes sidecar usually handles external_id uniquely within active versions.
        # This is mostly handled by the unique index, but can be checked here too.

        return False

    def get_identity_payload(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Returns the subset of the context that identifies the feature for this sidecar.
        """
        identity = {}
        if self.config.external_id_field is not None and "external_id" in context:
            identity[self.config.external_id_field] = context["external_id"]
        if self.config.asset_id_field is not None and "asset_id" in context:
            identity[self.config.asset_id_field] = context["asset_id"]
        return identity

    async def check_collision(
        self,
        conn: DbResource,
        physical_schema: str,
        physical_table: str,
        field_name: str,
        value: Any,
        exclude_geoid: Optional[Any] = None,
    ) -> bool:
        """Check for value existence in external_id or asset_id columns."""
        # Guard: reject lookups on disabled identity columns
        if self.config.external_id_field is None and field_name == "external_id":
            return False
        if self.config.asset_id_field is None and field_name == "asset_id":
            return False

        # We only support checks for identity fields or columnar attributes
        sc_table = f"{physical_table}_{self.sidecar_id}"

        # Check standard identity fields
        if field_name in ["external_id", "asset_id"]:
            sql = f'SELECT 1 FROM "{physical_schema}"."{sc_table}" WHERE {field_name} = :val'
            params: Dict[str, Any] = {"val": str(value)}
            if exclude_geoid:
                sql += " AND geoid <> :exclude"
                params["exclude"] = exclude_geoid

            # Limit 1 for speed
            sql += " LIMIT 1"

            scalar = await DQLQuery(sql, result_handler=ResultHandler.SCALAR).execute(conn, **params)
            return scalar is not None

        # Check columnar attributes
        if (
            self.resolved_storage_mode == AttributeStorageMode.COLUMNAR
            and self.config.attribute_schema
        ):
            attr_names = {a.name for a in self.config.attribute_schema}
            if field_name in attr_names:
                sql = f'SELECT 1 FROM "{physical_schema}"."{sc_table}" WHERE "{field_name}" = :val'
                params = {"val": value}
                if exclude_geoid:
                    sql += " AND geoid <> :exclude"
                    params["exclude"] = exclude_geoid

                sql += " LIMIT 1"
                scalar = await DQLQuery(sql, result_handler=ResultHandler.SCALAR).execute(conn, **params)
                return scalar is not None

        return False

    async def resolve_existing_item(
        self,
        conn: DbResource,
        physical_schema: str,
        physical_table: str,
        processing_context: Dict[str, Any],
        matcher: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Resolve by EXTERNAL_ID (default) or GEOMETRY_HASH matcher.

        ``matcher`` is a :class:`ComputedKind` string value.  Unknown
        matchers and those owned by another sidecar return None.
        """
        if matcher is None:
            matcher = "external_id"

        if matcher == "geometry_hash":
            return await self._resolve_by_geometry_hash(
                conn, physical_schema, physical_table, processing_context
            )
        if matcher == "attributes_hash":
            return await self._resolve_by_attributes_hash(
                conn, physical_schema, physical_table, processing_context
            )
        if matcher != "external_id":
            return None

        if not self.config.enable_external_id:
            return None

        external_id = processing_context.get("external_id")
        if not external_id:
            return None

        # Build query to match external_id and find active record
        sc_table = f"{physical_table}_{self.sidecar_id}"
        geom_sc_table = f"{physical_table}_geometries"

        # Issue #220: geometry_hash now lives on the geometries sidecar.
        # JOIN it so the matcher chain has both the geoid and the
        # current geometry_hash for the skip-if-unchanged gate.
        select_fields = ["h.geoid", "g.geometry_hash"]

        # Check active status:
        # 1. Hub deleted_at IS NULL
        # 2. Sidecar validity is active (if enabled)

        where_conditions = [
            """s.external_id = :ext_id""",
            """h.deleted_at IS NULL""",
            """h.geoid = s.geoid""",
            """g.geoid = h.geoid""",
        ]

        if self.config.enable_validity:
            # Active usually means valid_to is infinite (upper bound is NULL)
            # OR we match the logic "current version".
            # For exact ID lookup (external_id), we typically want the ACTIVE version.
            # If deleted_at is NULL (checked above), we also want validity to be current.
            where_conditions.append("""upper(s.validity) IS NULL""")
            select_fields.append("s.validity")

        sql = f"""
            SELECT {", ".join(select_fields)}
            FROM "{physical_schema}"."{physical_table}" h,
                 "{physical_schema}"."{sc_table}" s,
                 "{physical_schema}"."{geom_sc_table}" g
            WHERE {" AND ".join(where_conditions)}
            ORDER BY h.transaction_time DESC
            LIMIT 1;
        """

        row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn, ext_id=str(external_id)
        )
        return row or None

    async def _resolve_by_geometry_hash(
        self,
        conn: DbResource,
        physical_schema: str,
        physical_table: str,
        processing_context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Match on the geometries sidecar's ``geometry_hash`` (PG-generated).

        Issue #220: ``geometry_hash`` lives on the geometries sidecar as a
        STORED GENERATED column (``encode(digest(ST_AsBinary(geom),
        'sha256'), 'hex')``) — same shape as ``attributes_hash`` on the
        attributes sidecar, same shape as ``geohash``.  Postgres maintains
        it atomically with the geometry, eliminating the skew window the
        previous hub-side application-computed hash had.

        The matcher reads the incoming hash from
        ``processing_context["geometry_hash"]`` (populated by the
        geometries sidecar's pre-write pipeline) and JOINs the sidecar
        to find the active geoid carrying the same hash.
        """
        geometry_hash = processing_context.get("geometry_hash")
        if not geometry_hash:
            return None

        geom_sidecar_table = f"{physical_table}_geometries"
        sql = f"""
            SELECT h.geoid, s.geometry_hash
            FROM "{physical_schema}"."{physical_table}" h
            JOIN "{physical_schema}"."{geom_sidecar_table}" s
              ON s.geoid = h.geoid
            WHERE s.geometry_hash = :ch
              AND h.deleted_at IS NULL
            ORDER BY h.transaction_time DESC
            LIMIT 1;
        """
        row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn, ch=geometry_hash
        )
        return row or None

    async def _resolve_by_attributes_hash(
        self,
        conn: DbResource,
        physical_schema: str,
        physical_table: str,
        processing_context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Match on the attributes sidecar's ``attributes_hash`` (PG-generated).

        The hash is a STORED GENERATED column on the attributes sidecar
        (Mode B / JSONB only) — ``encode(digest(attributes::text, 'sha256'),
        'hex')``. Two items with byte-equal canonicalised JSONB attributes
        produce the same hash regardless of geometry.  Use case: "same
        attribute combination, different geometry" detection — pair with
        ``ComputedKind.GEOMETRY_HASH`` to distinguish "duplicate" from
        "moved" from "renamed".

        Returns ``None`` in Mode A (columnar storage) since the column
        isn't emitted there, and when the incoming feature didn't carry
        any attributes payload.
        """
        if self.resolved_storage_mode != AttributeStorageMode.JSONB:
            return None

        # Compute the hash on the incoming payload using the same
        # canonicalisation PG applies (jsonb internal sort), via the same
        # JSON dump the upsert payload prep uses (json.dumps with the
        # CustomJSONEncoder). PG canonicalises further on insert so the
        # comparison happens against the stored canonical form.
        feature_props = processing_context.get("feature_attributes")
        if feature_props is None:
            # Fall back to the upsert payload prep value if the orchestrator
            # already serialised it.
            feature_props = processing_context.get(
                f"{self.config.jsonb_column_name}_for_hash"
            )
        if feature_props is None:
            return None

        sc_table = f"{physical_table}_{self.sidecar_id}"
        geom_sc_table = f"{physical_table}_geometries"
        sql = f"""
            SELECT h.geoid, g.geometry_hash, s.attributes_hash
            FROM "{physical_schema}"."{physical_table}" h,
                 "{physical_schema}"."{sc_table}" s,
                 "{physical_schema}"."{geom_sc_table}" g
            WHERE h.geoid = s.geoid
              AND g.geoid = h.geoid
              AND h.deleted_at IS NULL
              AND s.attributes_hash = encode(
                  digest(CAST(:attrs AS jsonb)::text, 'sha256'), 'hex')
            ORDER BY h.transaction_time DESC
            LIMIT 1;
        """

        import json as _json
        attrs_json = _json.dumps(feature_props, cls=CustomJSONEncoder)
        row = await DQLQuery(sql, result_handler=ResultHandler.ONE_DICT).execute(
            conn, attrs=attrs_json,
        )
        return row or None

    def _extract_value(self, data: Dict[str, Any], path: str) -> Any:
        """Helper to extract value from dict using dot notation and properties fallback."""
        val = None
        if "." in path:
            parts = path.split(".")
            current = data
            for part in parts:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    current = None
                    break
            val = current
        else:
            val = data.get(path)

        # Fallback to properties if not found at root (standard GIS/STAC behavior).
        # GDAL/shapefile features arrive as {"properties": {...}, "geometry": ...};
        # any configured external_id_field must resolve from the properties bag,
        # not just the legacy id/asset_id paths.
        if val is None and "." not in path:
            props = data.get("properties")
            if isinstance(props, dict):
                val = props.get(path)

        return val

    async def expire_version(
        self,
        conn: DbResource,
        physical_schema: str,
        physical_table: str,
        geoid: str,
        expire_at: datetime,
    ) -> int:
        """Marks the current active version as expired in the attributes sidecar table."""
        if not self.config.enable_validity:
            return 0

        sc_table = f"{physical_table}_{self.sidecar_id}"
        # Standard Postgres temporal range update: set upper bound to expire_at
        sql = f'UPDATE "{physical_schema}"."{sc_table}" SET validity = tstzrange(lower(validity), :expire_at, \'[)\') WHERE geoid = :geoid'

        from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler

        return await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
            conn, geoid=geoid, expire_at=expire_at
        )
