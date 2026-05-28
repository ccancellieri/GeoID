"""
STAC Items Sidecar — first-class sidecar owning STAC-specific physical state.

Owns a per-items-table ``_stac_metadata`` companion table holding the
externally-supplied STAC content that is persisted once and returned
verbatim on read:

- ``external_extensions``: extension URIs supplied by the producer
- ``external_assets``: assets supplied at ingest (not derived)
- ``extra_fields``: open content under STAC-spec extension points

Generic multilanguage descriptive metadata (title / description /
keywords) remains owned by ``ItemMetadataSidecar`` so OGC-only
deployments that don't mount the STAC extension carry no STAC pollution
in their items rows.

On read, ``map_row_to_feature`` merges the stored content into the
Feature.  Generated content (proj/raster/vector blocks, derived asset
URLs, HATEOAS links) is computed downstream by the STAC generator and
extension-protocol providers — those are not owned by this sidecar.
"""

import json
import logging
from typing import Dict, Any, List, Optional, Set, Tuple, Union
from datetime import datetime

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
    ConsumerType,
    ValidationResult,
    FieldDefinition,
    FieldCapability,
)
from dynastore.extensions.stac.stac_metadata_config import (
    StacItemsSidecarConfig,
)

logger = logging.getLogger(__name__)

# Raw column aliases produced by the stac_metadata sidecar's SQL join.
STAC_METADATA_RAW_COLUMNS: Set[str] = {
    "external_extensions",
    "external_assets",
    "stac_extra_fields",
}


class StacItemsSidecar(SidecarProtocol):
    """First-class STAC items sidecar with its own physical table.

    Owns ``{schema}.{items_table}_stac_metadata`` with three JSONB
    columns persisting externally-supplied STAC content
    (``external_extensions``, ``external_assets``, ``extra_fields``).

    On read, ``map_row_to_feature`` merges the stored content into the
    Feature; gated on ``context.consumer`` to avoid injecting STAC
    fields into OGC Features responses.
    """

    def __init__(self, config: StacItemsSidecarConfig, **_kwargs: Any):
        self.config = config

    @property
    def sidecar_id(self) -> str:
        return "stac_metadata"

    @property
    def sidecar_type_id(self) -> str:
        return "stac_metadata"

    @classmethod
    def get_default_config(
        cls, context: Dict[str, Any]
    ) -> Optional[StacItemsSidecarConfig]:
        """Inject the STAC sidecar only when STAC is enabled for the collection.

        Two gates:

        - ``collection_type == "RECORDS"`` → skipped (RECORDS have no
          per-item descriptive layer).
        - ``stac_enabled is False`` → skipped.  The injection call site
          resolves ``StacPluginConfig.enabled`` from the config waterfall
          and plumbs it into the context as ``stac_enabled`` so a
          collection whose STAC extension is disabled at this scope
          carries no STAC sidecar in its composed/materialised config.

        When ``stac_enabled`` is absent from the context (callers that
        don't resolve it), the sidecar still injects — matching the
        ``StacPluginConfig.enabled`` default of ``True``.
        """
        if context.get("collection_type") == "RECORDS":
            return None
        if context.get("stac_enabled") is False:
            return None
        return StacItemsSidecarConfig()

    def is_mandatory(self) -> bool:
        """Not mandatory — items can exist without STAC content."""
        return False

    @classmethod
    def serves_consumers(cls) -> Optional[set]:
        """STAC payload columns are only useful for STAC responses.

        OGC Features / Records / generic reads don't surface
        ``external_extensions / external_assets / extra_fields``, so the
        optimizer should skip this sidecar's JOIN+SELECT for those
        consumers (unless the caller explicitly selects/filters one of
        its columns).
        """
        from dynastore.modules.storage.drivers.pg_sidecars.base import (
            ConsumerType,
        )
        return {ConsumerType.STAC}

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
        """Generate DDL for the STAC metadata sidecar table."""
        partition_keys = partition_keys or []
        partition_key_types = partition_key_types or {}
        columns = [
            "geoid UUID NOT NULL",
            "external_extensions JSONB",
            "external_assets JSONB",
            "extra_fields JSONB",
        ]

        known_columns = {
            "geoid",
            "external_extensions",
            "external_assets",
            "extra_fields",
        }

        if has_validity or "validity" in partition_keys:
            columns.append('"validity" TSTZRANGE NOT NULL')
            known_columns.add("validity")

        pk_columns: List[str] = []

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

        for col in ["external_extensions", "external_assets", "extra_fields"]:
            ddl += (
                f'\nCREATE INDEX IF NOT EXISTS "idx_{table_name}_{col}" '
                f'ON {{schema}}."{table_name}" USING GIN({col});'
            )

        return ddl

    def get_select_fields(
        self,
        request: Optional[Any] = None,
        hub_alias: str = "h",
        sidecar_alias: Optional[str] = None,
        include_all: bool = False,
    ) -> List[str]:
        """Return SELECT fields for STAC metadata."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        return [
            f"{alias}.external_extensions",
            f"{alias}.external_assets",
            f"{alias}.extra_fields AS stac_extra_fields",
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
        """Generate JOIN clause for the STAC metadata sidecar."""
        alias = sidecar_alias or f"sc_{self.sidecar_id}"
        table_name = f"{hub_table}_{self.sidecar_id}"

        on_clause = f"{hub_alias}.geoid = {alias}.geoid"
        if extra_condition:
            on_clause += f" AND {extra_condition}"

        return f'{join_type} JOIN "{schema}"."{table_name}" AS {alias} ON {on_clause}'

    # ── Validation ───────────────────────────────────────────────────────

    async def validate_upsert(
        self,
        feature: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> ValidationResult:
        errors = []
        if "assets" in feature and not isinstance(feature["assets"], dict):
            errors.append("STAC assets must be an object")
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

    def apply_query_context(
        self,
        request: Any,
        context: Dict[str, Any],
    ) -> None:
        pass

    def get_queryable_fields(self) -> Dict[str, FieldDefinition]:
        return {
            "external_extensions": FieldDefinition(
                name="external_extensions",
                data_type="jsonb",
                sql_expression=f"sc_{self.sidecar_id}.external_extensions",
                capabilities=[FieldCapability.FILTERABLE],
                description="Externally-supplied STAC extension URIs",
            ),
        }

    def get_feature_type_schema(self) -> Dict[str, Any]:
        return {
            "stac_extensions": {"type": "array", "items": {"type": "string"}},
            "assets": {"type": "object"},
        }

    def get_identity_columns(self) -> List[str]:
        return ["geoid"]

    def resolve_query_path(self, attr_name: str) -> Optional[Tuple[str, str]]:
        """Resolves an attribute reference to SQL and JOIN requirements."""
        alias = f"sc_{self.sidecar_id}"
        if attr_name == "external_extensions":
            return (f"{alias}.external_extensions", alias)
        if attr_name == "external_assets":
            return (f"{alias}.external_assets", alias)
        return None

    def prepare_upsert_payload(
        self, feature: Any, context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Extract STAC-specific content for the stac_metadata table.

        Reuses ``prune_managed_content_sync`` (the same helper that
        ``ItemMetadataSidecar`` uses for title/description/keywords) and
        cherry-picks the three STAC keys (``external_assets``,
        ``external_extensions``, ``extra_fields``) for this sidecar's
        physical row.
        """
        geoid = context.get("geoid")
        if not geoid:
            raise ValueError(
                "geoid is required in context for stac_metadata sidecar"
            )

        # Prefer the pristine pre-prune snapshot: a prune-first sidecar
        # (item_metadata) strips ``:``-namespaced extension keys from the
        # shared ``properties`` before this sidecar runs, so reading the live
        # feature here would silently drop ``extra_fields``.
        source = context.get("_pristine_item")
        if source is None:
            source = feature

        if hasattr(source, "dict"):
            data = source.dict(by_alias=True)
        elif hasattr(source, "model_dump"):
            data = source.model_dump(by_alias=True)
        else:
            data = dict(source)

        from dynastore.tools.discovery import get_protocols
        from dynastore.extensions.stac.stac_extension_protocol import (
            StacExtensionProtocol,
        )
        from dynastore.extensions.stac.metadata_helpers import (
            prune_managed_content_sync,
        )

        providers = get_protocols(StacExtensionProtocol)
        pruned = prune_managed_content_sync(data, providers)

        payload: Dict[str, Any] = {
            "geoid": geoid,
            "external_extensions": pruned["external_extensions"],
            "external_assets": pruned["external_assets"],
            "extra_fields": pruned["extra_fields"],
        }

        if context.get("partition_key_name"):
            payload[context["partition_key_name"]] = context["partition_key_value"]

        from dynastore.extensions.tools.fast_api import CustomJSONEncoder

        for field in ["external_extensions", "external_assets", "extra_fields"]:
            if payload[field] is not None:
                payload[field] = json.dumps(payload[field], cls=CustomJSONEncoder)

        return payload

    def finalize_upsert_payload(
        self,
        sc_payload: Dict[str, Any],
        hub_row: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Finalize payload by injecting validity from Hub or context."""
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
        """Mark the current active version as expired."""
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
        return STAC_METADATA_RAW_COLUMNS

    # ── The core method: inject STAC fields from context ─────────────────

    def map_row_to_feature(
        self,
        row: Dict[str, Any],
        feature: Feature,
        context: FeaturePipelineContext,
    ) -> None:
        """Inject STAC-specific fields into the Feature.

        Reads stored content directly from the row (columns surfaced by
        this sidecar's SELECT/JOIN).  Skips injection entirely for OGC
        Features consumers.
        """
        # For OGC Features consumers, do nothing — generic metadata
        # (title/desc/keywords) was already resolved by ItemMetadataSidecar.
        if context.consumer == ConsumerType.OGC_FEATURES:
            return

        def _get(key: str):
            return row.get(key)

        def _maybe_parse(v):
            if isinstance(v, str):
                try:
                    return json.loads(v)
                except (ValueError, TypeError):
                    return v
            return v

        props: Dict[str, Any] = feature.properties if feature.properties is not None else {}
        feature.properties = props

        from dynastore.tools.language_utils import resolve_localized_field
        from dynastore.models.localization import is_valid_language_key

        # 1. Extensions Merging
        raw_exts = _get("external_extensions")
        exts = _maybe_parse(raw_exts) if raw_exts else []
        if exts:
            existing: List[str] = getattr(feature, "stac_extensions", []) or []
            if not existing:
                existing = props.get("stac_extensions", []) or []
            exts_list: List[str] = exts if isinstance(exts, list) else ([exts] if isinstance(exts, str) else [])
            merged_exts = list(set([*existing, *exts_list]))
            try:
                setattr(feature, "stac_extensions", merged_exts)
            except Exception:
                props["stac_extensions"] = merged_exts

        # 2. Assets Merging
        assets = _maybe_parse(_get("external_assets"))
        if isinstance(assets, dict):
            try:
                for asset_key, asset_val in assets.items():
                    if isinstance(asset_val, dict):
                        for sub_field in ["title", "description"]:
                            if sub_field in asset_val and isinstance(
                                asset_val[sub_field], dict
                            ):
                                asset_val[sub_field] = resolve_localized_field(
                                    asset_val[sub_field], context.lang
                                )

                curr_assets = getattr(feature, "assets", None)
                if curr_assets is None:
                    if "assets" in props and isinstance(props["assets"], dict):
                        curr_assets = props["assets"]
                        try:
                            setattr(feature, "assets", curr_assets)
                            del props["assets"]
                        except Exception:
                            pass
                    else:
                        try:
                            setattr(feature, "assets", {})
                            curr_assets = getattr(feature, "assets")
                        except Exception:
                            pass

                if curr_assets is not None and isinstance(curr_assets, dict):
                    curr_assets.update(assets)
                elif curr_assets is not None:
                    for k, v in assets.items():
                        try:
                            setattr(curr_assets, k, v)
                        except Exception:
                            pass
                else:
                    if "assets" not in props:
                        props["assets"] = {}
                    props["assets"].update(assets)
            except Exception as e:
                logger.warning(f"Failed to merge STAC assets: {e}")
                if "assets" not in props:
                    props["assets"] = {}
                props["assets"].update(assets)

        # 3. Extra Fields (column aliased as ``stac_extra_fields`` in SELECT)
        extra = _maybe_parse(
            _get("stac_extra_fields")
            or _get("extra_fields")
        )
        if isinstance(extra, dict):
            top_level_stac = {
                "stac_version",
                "stac_extensions",
                "collection",
                "bbox",
                "links",
            }
            for k, v in extra.items():
                if isinstance(v, dict):
                    if all(
                        isinstance(k2, str) and is_valid_language_key(k2)
                        for k2 in v.keys()
                        if k2
                    ):
                        v = resolve_localized_field(v, context.lang)

                if k in top_level_stac:
                    try:
                        setattr(feature, k, v)
                    except Exception:
                        props[k] = v
                else:
                    props[k] = v

        # Publish to context for downstream consumers (e.g. STAC generator)
        context.publish(self.sidecar_id, {
            "stac_extensions": getattr(feature, "stac_extensions", None)
            or props.get("stac_extensions"),
            "assets": getattr(feature, "assets", None)
            or props.get("assets"),
        })


# Register for polymorphic resolution
# (Config is registered in stac_metadata_config.py at import time.)
SidecarRegistry.register("stac_metadata", StacItemsSidecar)
