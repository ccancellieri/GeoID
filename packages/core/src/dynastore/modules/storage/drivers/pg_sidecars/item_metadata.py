#    Copyright 2026 FAO
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
Item Metadata Sidecar - Generic per-item multilanguage metadata.

Stores per-item title, description, keywords (internationalized) for
non-STAC consumers (OGC Features, EDR, WFS).  Resolves multilanguage
fields into the requested language at render time.

STAC-specific stored content (``external_extensions``,
``external_assets``, ``extra_fields``) lives in the
:class:`StacItemsSidecar`'s own physical table — keeping
``item_metadata`` free of STAC-only columns so OGC-only deployments
carry no STAC pollution.
"""

import json
import logging
from datetime import datetime
from typing import ClassVar, Dict, Any, List, Optional, Set, Tuple

from geojson_pydantic import Feature

from dynastore.modules.db_config.query_executor import (
    DbResource,
    DQLQuery,
    ResultHandler,
)
from dynastore.modules.storage.drivers.pg_sidecars.registry import SidecarRegistry
from dynastore.modules.storage.drivers.pg_sidecars.base import (
    SidecarProtocol,
    FeaturePipelineContext,
    ValidationResult,
    FieldDefinition,
    FieldCapability,
)
from dynastore.modules.storage.drivers.pg_sidecars.item_metadata_config import (
    ItemMetadataSidecarConfig,
)

logger = logging.getLogger(__name__)

# Raw column aliases produced by the item metadata sidecar's SQL join.
ITEM_METADATA_RAW_COLUMNS: Set[str] = {
    "item_title",
    "item_description",
    "item_keywords",
}


class ItemMetadataSidecar(SidecarProtocol):
    """
    Core sidecar for per-item multilanguage metadata.

    Stores per-item title, description, keywords (internationalized).
    STAC-specific external content lives in :class:`StacItemsSidecar`.
    """

    def __init__(self, config: ItemMetadataSidecarConfig, **_kwargs: Any):
        self.config = config

    @property
    def sidecar_id(self) -> str:
        return "item_metadata"

    @property
    def sidecar_type_id(self) -> str:
        return "item_metadata"

    @classmethod
    def get_default_config(
        cls, context: Dict[str, Any]
    ) -> Optional[ItemMetadataSidecarConfig]:
        """Auto-inject metadata sidecar by default.

        Per the class docstring, this is a core sidecar that runs for all
        consumers (OGC Features, STAC, WFS) — it resolves multilanguage
        title/description/keywords. Skipped only for RECORDS collections
        which have no per-item descriptive metadata layer.

        The previous gate (``if context.get("stac_context")``) relied on a
        flag that the lazy-activation refactor stopped propagating: the
        STAC service set ``stac_context=True`` on ``create_collection``
        but ``_activate_collection`` (where ``ensure_storage`` actually
        runs) had no recovery path for it. Result: the table never got
        created and any item-metadata-touching test failed with
        ``UndefinedTableError``. Always-inject restores the documented
        contract; non-STAC collections that produce no per-item metadata
        rows pay only the empty-table cost.
        """
        if context.get("collection_type") == "RECORDS":
            return None
        return ItemMetadataSidecarConfig()

    def is_mandatory(self) -> bool:
        """Metadata sidecar is not mandatory — items can exist without metadata."""
        return False

    @property
    def provides_feature_id(self) -> bool:
        return False

    @property
    def feature_id_field_name(self) -> Optional[str]:
        return None

    def get_ddl(
        self,
        physical_table: str,
        partition_keys: Optional[List[str]] = None,
        partition_key_types: Optional[Dict[str, str]] = None,
        has_validity: bool = False,
    ) -> str:
        """
        Generate DDL for item metadata sidecar table.

        Follows the standard sidecar pattern:
        - Composite PK from partition keys + geoid
        - Supports dynamic partitioning from other sidecars
        """
        partition_keys = partition_keys or []
        partition_key_types = partition_key_types or {}
        columns = [
            "geoid UUID NOT NULL",
            "title JSONB",
            "description JSONB",
            "keywords JSONB",
        ]

        known_columns = {
            "geoid",
            "title",
            "description",
            "keywords",
        }

        if has_validity or "validity" in partition_keys:
            columns.append('"validity" TSTZRANGE NOT NULL')
            known_columns.add("validity")

        pk_columns = []

        if partition_keys:
            for key in partition_keys:
                if key not in known_columns:
                    col_type = partition_key_types.get(key, "TEXT")
                    columns.insert(0, f'"{key}" {col_type} NOT NULL')
                    known_columns.add(key)

            for key in partition_keys:
                pk_columns.append(f'"{key}"')

            if "geoid" not in set(partition_keys):
                pk_columns.append('"geoid"')

            partition_clause = (
                f" PARTITION BY LIST ({', '.join([f'\"{k}\"' for k in partition_keys])})"
            )
        else:
            partition_clause = ""
            pk_columns = ['"geoid"']

        table_name = f"{physical_table}_{self.sidecar_id}"

        create_sql = (
            f'CREATE TABLE IF NOT EXISTS {{schema}}."{table_name}" '
            f"({', '.join(columns)}, PRIMARY KEY ({', '.join(pk_columns)}))"
            f"{partition_clause};"
        )

        ref_cols = ["geoid"]
        if "validity" in partition_keys:
            ref_cols.append("validity")

        fk_sql = (
            f'\nALTER TABLE {{schema}}."{table_name}" '
            f'ADD CONSTRAINT "fk_{table_name}_hub" '
            f"FOREIGN KEY ({', '.join([f'\"' + c + '\"' for c in ref_cols])}) "
            f'REFERENCES {{schema}}."{physical_table}" '
            f"({', '.join([f'\"' + c + '\"' for c in ref_cols])}) "
            f"ON DELETE CASCADE;"
        )

        ddl = create_sql + fk_sql

        for col in ["title", "description", "keywords"]:
            ddl += (
                f'\nCREATE INDEX IF NOT EXISTS "idx_{table_name}_{col}" '
                f'ON {{schema}}."{table_name}" USING GIN({col});'
            )

        return ddl

    def get_select_fields(
        self,
        request: Optional[Any] = None,
        sidecar_alias: Optional[str] = None,
        include_all: bool = False,
    ) -> List[str]:
        """Return SELECT fields for item metadata."""
        if request is not None and getattr(request, "group_by", None):
            # #2829: none of title/description/keywords is aggregatable, and
            # this sidecar has no per-field selective mode (it always
            # returns all three together) — never contribute them to a
            # GROUP BY request, or an ungrouped column would violate
            # PostgreSQL's GROUP BY rule.
            return []
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        return [
            f"{alias}.title AS item_title",
            f"{alias}.description AS item_description",
            f"{alias}.keywords AS item_keywords",
        ]

    def get_join_clause(
        self,
        schema: str,
        hub_table: str,
        hub_alias: str = "h",
        sidecar_alias: Optional[str] = None,
        join_type: str = "LEFT",
        extra_condition: Optional[str] = None,
    ) -> str:
        """Generate JOIN clause for item metadata sidecar."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        table_name = f"{hub_table}_{self.sidecar_id}"

        on_clause = f"{hub_alias}.geoid = {alias}.geoid"
        if extra_condition:
            on_clause += f" AND {extra_condition}"

        return f'{join_type} JOIN "{schema}"."{table_name}" AS {alias} ON {on_clause}'

    async def validate_upsert(
        self,
        feature: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> ValidationResult:
        """Validate metadata before upsert."""
        errors = []

        if "assets" in feature and not isinstance(feature["assets"], dict):
            errors.append("Assets must be an object")

        if "stac_extensions" in feature and not isinstance(
            feature["stac_extensions"], list
        ):
            errors.append("stac_extensions must be an array")

        return ValidationResult(
            valid=len(errors) == 0,
            error="; ".join(errors) if errors else None,
        )

    async def setup_lifecycle_hooks(
        self, conn: Any, schema: str, table_name: str
    ) -> None:
        pass

    async def on_partition_create(
        self,
        conn: Any,
        schema: str,
        parent_table: str,
        partition_table: str,
        partition_value: Any,
    ) -> None:
        pass

    def resolve_query_path(self, attr_name: str) -> Optional[Tuple[str, str]]:
        """Resolves an attribute reference to SQL and JOIN requirements."""
        alias = f"sc_{self.sidecar_id}"
        if attr_name in ["item_title", "title"]:
            return (f"{alias}.title", alias)
        if attr_name in ["item_description", "description"]:
            return (f"{alias}.description", alias)
        return None

    def apply_query_context(
        self,
        request: Any,
        context: Dict[str, Any],
    ) -> None:
        """Populates query context with metadata selections and joins."""
        is_select_all = not request.select or any(
            s.field == "*" for s in request.select
        )

        has_metadata_request = any(
            s.field.startswith("item_")
            or s.field in ["title", "description", "keywords"]
            for s in request.select
        )

        if is_select_all or has_metadata_request:
            sc_alias = f"sc_{self.sidecar_id}"
            context["select_fields"].extend(
                self.get_select_fields(request, sidecar_alias=sc_alias)
            )

    def get_queryable_fields(self) -> Dict[str, FieldDefinition]:
        """Return queryable field definitions."""
        return {
            "item_title": FieldDefinition(
                name="item_title",
                data_type="jsonb",
                sql_expression="sc_item_metadata.title",
                capabilities=[FieldCapability.FILTERABLE],
                description="Item title (internationalized)",
            ),
            "item_description": FieldDefinition(
                name="item_description",
                data_type="jsonb",
                sql_expression="sc_item_metadata.description",
                capabilities=[FieldCapability.FILTERABLE],
                description="Item description (internationalized)",
            ),
        }

    def get_feature_type_schema(self) -> Dict[str, Any]:
        """Return JSON Schema fragment for Feature output."""
        return {
            "title": {"type": "string"},
            "description": {"type": "string"},
            "keywords": {
                "type": "array",
                "items": {"type": "string"},
            },
        }

    def get_identity_columns(self) -> List[str]:
        return ["geoid"]

    def prepare_upsert_payload(
        self, feature: Any, context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Extract item metadata and prune managed assets.
        Called automatically by ItemService.upsert if registered.
        """
        geoid = context.get("geoid")
        if not geoid:
            raise ValueError("geoid is required in context for item metadata sidecar")

        if hasattr(feature, "dict"):
            data = feature.dict(by_alias=True)
        elif hasattr(feature, "model_dump"):
            data = feature.model_dump(by_alias=True)
        else:
            data = dict(feature)

        try:
            from dynastore.extensions.stac.metadata_helpers import (
                prune_managed_content_sync,
                prune_stac_managed_properties,
            )
        except ImportError as exc:
            from dynastore.modules.db_config.exceptions import ConfigResolutionError
            catalog_id = context.get("catalog_id", "<unknown>")
            collection_id = context.get("collection_id", "<unknown>")
            raise ConfigResolutionError(
                (
                    f"ItemMetadataSidecar.prepare_upsert_payload requires the STAC extension "
                    f"for managed-content pruning, but 'dynastore.extensions.stac' is not "
                    f"importable in this service. Collection '{catalog_id}/{collection_id}' "
                    f"cannot be written from a SCOPE that omits 'dynastore[extension_stac]'. "
                    f"Reinstall with the STAC extra, or write to this collection from a "
                    f"STAC-aware service."
                ),
                missing_key="extension:stac",
                scope_tried=[f"catalog={catalog_id}", f"collection={collection_id}"],
                hint=(
                    "Install 'dynastore[extension_stac]' in this service, or route writes "
                    "for STAC-managed collections to a service whose SCOPE includes it."
                ),
            ) from exc

        pruned = prune_managed_content_sync(data)

        if isinstance(feature, dict) and "properties" in feature:
            prune_stac_managed_properties(feature["properties"])
        else:
            props = getattr(feature, "properties", None)
            if isinstance(props, dict):
                prune_stac_managed_properties(props)

        payload = {
            "geoid": geoid,
            "title": pruned["title"],
            "description": pruned["description"],
            "keywords": pruned["keywords"],
        }

        if context.get("partition_key_name"):
            payload[context["partition_key_name"]] = context["partition_key_value"]

        from dynastore.extensions.tools.fast_api import CustomJSONEncoder

        for field in ["title", "description", "keywords"]:
            if payload[field] is not None:
                payload[field] = json.dumps(payload[field], cls=CustomJSONEncoder)

        return payload

    def finalize_upsert_payload(
        self,
        sc_payload: Dict[str, Any],
        hub_row: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Finalizes payload by injecting validity from Hub or context."""
        payload = sc_payload.copy()

        if "validity" in context or "validity" in hub_row:
            validity = hub_row.get("validity") or context.get("validity")
            if validity:
                payload["validity"] = validity

        return payload

    async def expire_version(
        self,
        conn: DbResource,
        physical_schema: str,
        physical_table: str,
        geoid: str,
        expire_at: datetime,
    ) -> int:
        """Marks the current active version as expired."""
        sc_table = f"{physical_table}_{self.sidecar_id}"

        sql = (
            f'UPDATE "{physical_schema}"."{sc_table}" '
            f"SET validity = tstzrange(lower(validity), :expire_at, '[)') "
            f"WHERE geoid = :geoid AND upper(validity) IS NULL"
        )

        try:
            return await DQLQuery(sql, result_handler=ResultHandler.ROWCOUNT).execute(
                conn, geoid=geoid, expire_at=expire_at
            )
        except Exception as e:
            if 'column "validity" does not exist' in str(e).lower():
                return 0
            raise

    def get_internal_columns(self) -> set:
        """Columns owned by this sidecar that are never part of Feature properties."""
        return ITEM_METADATA_RAW_COLUMNS

    # ── Canonical ES metadata container (refs #1828 Phase 2 / #1838) ─────────

    _METADATA_COLUMN_MAP: ClassVar[Dict[str, str]] = {
        "title": "item_title",
        "description": "item_description",
        "keywords": "item_keywords",
    }

    def producible_metadata_names(self) -> set:
        """Metadata container field names this sidecar produces.

        Returns the three canonical names (``title``, ``description``,
        ``keywords``) that ``build_canonical_index_doc`` routes into the typed
        ``metadata{}`` ES container. The storage columns are the ``item_*``
        aliases produced by ``get_select_fields``; the canonical names are the
        un-prefixed forms the ES mapping expects (refs #1828 / #1838).
        """
        return set(self._METADATA_COLUMN_MAP)

    def resolve_metadata_value(self, row: dict, resolved_name: str):
        """Locate one metadata value from a PG read row.

        Maps canonical names (``title`` / ``description`` / ``keywords``) to
        their ``item_*`` storage aliases. Returns ``(True, value)`` when the
        row carries a non-None value; ``(False, None)`` otherwise.
        """
        col = self._METADATA_COLUMN_MAP.get(resolved_name)
        if col is None:
            return (False, None)
        val = row.get(col)
        if val is None:
            return (False, None)
        return (True, val)

    def map_row_to_feature(
        self,
        row: Dict[str, Any],
        feature: Feature,
        context: FeaturePipelineContext,
    ) -> None:
        """Resolve multilanguage metadata from database row into feature properties.

        This sidecar handles ONLY the generic metadata fields (title,
        description, keywords).  Extension-specific fields (assets,
        stac_extensions, extra_fields) are left in the context for
        downstream sidecars (e.g. the STAC overlay) to consume.
        """
        # Publish raw row values for downstream sidecars.
        context.publish(
            self.sidecar_id,
            {k: row[k] for k in self.get_internal_columns() if k in row},
        )

        def _maybe_parse(v):
            if isinstance(v, str):
                try:
                    return json.loads(v)
                except (ValueError, TypeError):
                    return v
            return v

        if feature.properties is None:
            feature.properties = {}

        from dynastore.tools.language_utils import resolve_localized_field
        from dynastore.models.localization import is_valid_language_key

        # Resolve multilanguage title, description, keywords
        props: Dict[str, Any] = feature.properties if feature.properties is not None else {}
        feature.properties = props
        available_langs: Set[str] = set()
        for field in ["title", "description", "keywords"]:
            val = row.get(f"item_{field}") or row.get(f"stac_{field}") or row.get(field)
            if val is not None:
                parsed = _maybe_parse(val)
                if isinstance(parsed, dict):
                    available_langs.update(
                        k for k in parsed.keys()
                        if isinstance(k, str) and is_valid_language_key(k)
                    )
                    resolved = resolve_localized_field(parsed, context.lang)
                    if resolved is not None:
                        props[field] = resolved
                else:
                    props[field] = parsed

        if available_langs:
            context.publish("item_metadata_available_langs", list(available_langs))


SidecarRegistry.register("item_metadata", ItemMetadataSidecar)
