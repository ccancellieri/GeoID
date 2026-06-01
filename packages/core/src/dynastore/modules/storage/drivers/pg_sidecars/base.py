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
Sidecar Base Protocol and Configuration.

This module defines the contract for implementing specialized storage sidecars
including methods for DDL generation, data transformation, and operation validation.
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import (
    Annotated,
    ClassVar,
    Dict,
    Any,
    Optional,
    List,
    Set,
    Union,
    Tuple,
    Type,
)
from pydantic import BaseModel, BeforeValidator, Field, SerializeAsAny, model_validator
from enum import Enum
from dynastore.models.ogc import Feature
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.db_config.query_executor import DbResource



logger = logging.getLogger(__name__)


_SIDECAR_DATA_KEY = "_sidecar_data"


class ConsumerType(str, Enum):
    """Consumer of the sidecar pipeline output.

    Passed via ``FeaturePipelineContext.consumer`` so that sidecars can
    gate extension-specific field injection (e.g. STAC assets) while
    still performing shared work (e.g. multilanguage resolution).

    New protocols can pass custom string values without modifying this
    enum — ``_missing_`` will accept any string as a valid member.
    """

    GENERIC = "generic"
    OGC_FEATURES = "ogc_features"
    STAC = "stac"
    OGC_RECORDS = "ogc_records"

    @classmethod
    def _missing_(cls, value):
        obj = str.__new__(cls, value)
        obj._value_ = value
        return obj

# Columns that belong to the Hub table and must never appear in Feature.properties,
# regardless of which sidecar is currently running. geometry_hash is plumbing for
# the GEOMETRY_HASH identity matcher (see ItemsWritePolicy.identity); it is
# queryable at the SQL layer for write policies but is not surfaced to API
# consumers by default.
HUB_INTERNAL_COLUMNS: frozenset = frozenset({
    "geoid",
    "transaction_time",
    "deleted_at",
    "catalog_id",
    "collection_id",
    "geometry_hash",
})


class SidecarDataEntry:
    """
    Holds raw row values published by a single sidecar during
    ``map_row_to_feature``.  Each sidecar publishes under its own
    ``sidecar_id`` to avoid column-name collisions between sidecars.

    Usage example inside a downstream sidecar::

        geom = context.get_sidecar("geometry")
        if geom:
            bbox = geom.get_context_values().get("bbox_geom")
    """

    __slots__ = ("_data",)

    def __init__(self, data: Dict[str, Any]) -> None:
        self._data: Dict[str, Any] = data

    def get_context_values(self) -> Dict[str, Any]:
        """Return all raw row values published by this sidecar."""
        return dict(self._data)

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __contains__(self, key: object) -> bool:
        return key in self._data

    def __repr__(self) -> str:
        return f"SidecarDataEntry(keys={list(self._data.keys())})"


# ── ContextContract ─────────────────────────────────────────────────────────
# Well-known top-level context keys and their ownership.
# Use the typed properties on FeaturePipelineContext (e.g. context.asset_id)
# rather than accessing these keys directly.
#
# Key                  Written by                    Read by
# ─────────────────    ─────────────────────────     ──────────────────────────
# "asset_id"           FeatureAttributeSidecar        StacItemsSidecar, STAC gen
# "valid_from"         FeatureAttributeSidecar        StacItemsSidecar
# "valid_to"           FeatureAttributeSidecar        StacItemsSidecar
# "lang"               ItemService.map_row_to_feature All sidecars
# "include_internal"   Callers                        All sidecars
# "_all_internal_cols" ItemService.map_row_to_feature All sidecars (via property)
# "_sidecar_data"      context.publish()              context.get_sidecar()
# ────────────────────────────────────────────────────────────────────────────


class FeaturePipelineContext:
    """
    Typed pipeline context passed through ``map_row_to_feature``.

    Does **not** inherit from ``dict``.  All state is held in typed slots:

    - ``lang`` / ``include_internal`` — construction-time configuration.
    - ``_all_internal_cols`` — populated by ``ItemService`` before the
      pipeline starts; use the ``all_internal_columns`` property to read.
    - ``_sidecar_store`` — namespace-safe sidecar data published via
      :meth:`publish` and read back via :meth:`get_sidecar`.
    - ``_store`` — backward-compat flat key storage for sidecars that
      still write ``context["asset_id"] = …`` style entries.

    **Writing** (inside a sidecar's ``map_row_to_feature``)::

        context.publish(self.sidecar_id, dict(row))

    **Reading** (inside a downstream sidecar)::

        attributes = context.get_sidecar("attributes")
        if attributes:
            asset_id = attributes.get("asset_id")

    **Typed shortcuts** (preferred over raw ``_store`` access)::

        context.lang        # requested language, default "en"
        context.asset_id    # asset_id from attributes sidecar (or None)
        context.geoid       # Hub geoid for the current row (or None)
        context.valid_from  # validity start from attributes sidecar (or None)
        context.valid_to    # validity end from attributes sidecar (or None)
    """

    __slots__ = ("lang", "include_internal", "_all_internal_cols", "_sidecar_store", "_store", "consumer")

    def __init__(
        self,
        lang: str = "en",
        include_internal: bool = False,
        consumer: ConsumerType = ConsumerType.GENERIC,
    ) -> None:
        self.lang: str = lang
        self.include_internal: bool = include_internal
        self.consumer: ConsumerType = consumer
        self._all_internal_cols: Set[str] = set()
        self._sidecar_store: Dict[str, Dict[str, Any]] = {}
        self._store: Dict[str, Any] = {}

    # ── Sidecar data access ─────────────────────────────────────────────────

    def publish(self, sidecar_id: str, data: Any) -> None:
        """
        Store *data* under *sidecar_id* so downstream sidecars can read it.

        Call this **first thing** inside ``map_row_to_feature`` with the raw
        row values for this sidecar (whether or not they end up in the Feature).
        """
        self._sidecar_store[sidecar_id] = data

    def get_sidecar(self, sidecar_id: str) -> Optional["SidecarDataEntry"]:
        """
        Return the data entry published by *sidecar_id*, or ``None`` if that
        sidecar was not included in the current query.
        """
        data = self._sidecar_store.get(sidecar_id)
        return SidecarDataEntry(data) if data is not None else None

    def all_sidecars(self) -> Dict[str, "SidecarDataEntry"]:
        """Return all published sidecar entries keyed by sidecar_id."""
        return {sid: SidecarDataEntry(d) for sid, d in self._sidecar_store.items()}

    # ── Backward-compat flat key access for sidecars that write context["x"] ─

    def __setitem__(self, key: str, value: Any) -> None:
        self._store[key] = value

    def __getitem__(self, key: str) -> Any:
        return self._store[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self._store.get(key, default)

    def setdefault(self, key: str, default: Any = None) -> Any:
        return self._store.setdefault(key, default)

    def __contains__(self, key: object) -> bool:
        return key in self._store

    # ── Typed shortcuts for common pipeline values ───────────────────────────

    @property
    def asset_id(self) -> Optional[str]:
        """
        The asset_id associated with the current feature row.

        Primary source: sidecar data published by FeatureAttributeSidecar
        via ``context.publish("attributes", row_data)``.
        Backward-compat fallback: direct ``context["asset_id"]`` write.
        """
        attrs = self.get_sidecar("attributes")
        if attrs:
            v = attrs.get("asset_id")
            if v is not None:
                return str(v)
        v = self._store.get("asset_id")
        return str(v) if v is not None else None

    @property
    def geoid(self) -> Optional[str]:
        """Hub geoid for the current row."""
        v = self._store.get("geoid")
        return str(v) if v is not None else None

    @property
    def valid_from(self) -> Optional[Any]:
        """Validity start; reads from sidecar data then flat store."""
        attrs = self.get_sidecar("attributes")
        if attrs:
            v = attrs.get("valid_from")
            if v is not None:
                return v
        return self._store.get("valid_from")

    @property
    def valid_to(self) -> Optional[Any]:
        """Validity end; reads from sidecar data then flat store."""
        attrs = self.get_sidecar("attributes")
        if attrs:
            v = attrs.get("valid_to")
            if v is not None:
                return v
        return self._store.get("valid_to")

    @property
    def all_internal_columns(self) -> frozenset:
        """
        Full set of DB column names that must never appear in Feature.properties.

        Combines ``HUB_INTERNAL_COLUMNS`` with the per-pipeline set populated
        by ``ItemService.map_row_to_feature`` from every sidecar's
        ``get_internal_columns()`` before the pipeline starts.
        """
        return HUB_INTERNAL_COLUMNS | self._all_internal_cols


# Re-export protocol-level definitions for backward compatibility
from dynastore.models.protocols.field_definition import (  # noqa: F401
    FieldCapability,
    FieldDefinition as _ProtocolFieldDefinition,
)


class FieldDefinition(_ProtocolFieldDefinition):
    """PG-sidecar field definition — extends protocol FieldDefinition with sql_expression."""

    sql_expression: str = ""  # e.g., "sc_geom.geom", "sc_attr.external_id"


class ValidationResult(BaseModel):
    """Result of an operation validation."""

    valid: bool
    error: Optional[str] = None


class SidecarConfigRegistry:
    """
    Registry for SidecarConfig subclasses to support polymorphic deserialization.
    """

    _registry: ClassVar[Dict[str, Type["SidecarConfig"]]] = {}
    _defaults_loaded: bool = False

    @classmethod
    def register(cls, sidecar_type: str, config_cls: Type["SidecarConfig"]):
        """Register a SidecarConfig subclass for a specific sidecar type."""
        cls._registry[sidecar_type] = config_cls

    @classmethod
    def _ensure_defaults(cls) -> None:
        """Import known extension config modules so their module-level
        ``register(...)`` side effects run.

        Backstop for workers where the extension hasn't been imported yet
        when ``_coerce_pg_sidecar`` first runs against a cached driver
        config dict — see ``stac/__init__.py`` for the primary eager
        import.  Each import is guarded so absent extensions stay silent.
        """
        if cls._defaults_loaded:
            return
        cls._defaults_loaded = True
        try:
            from dynastore.extensions.stac import stac_metadata_config  # noqa: F401
        except ImportError:
            pass

    @classmethod
    def resolve_config_class(cls, sidecar_type: str) -> Type["SidecarConfig"]:
        """Resolve the specialized SidecarConfig subclass for a given type."""
        hit = cls._registry.get(sidecar_type)
        if hit is None:
            cls._ensure_defaults()
            hit = cls._registry.get(sidecar_type)
        return hit if hit is not None else SidecarConfig


class SidecarConfig(BaseModel):
    """
    Base configuration model for sidecars.
    """

    sidecar_type: str  # Discriminator field — subclasses override with Literal[...]
    enabled: bool = True

    # Per-sidecar indexing configuration
    # Note: IndexingConfig will be imported inside methods to avoid circular imports if needed
    indexing: Optional[Dict[str, Any]] = Field(
        default=None, description="Sidecar-specific indexing configuration"
    )

    @model_validator(mode="after")
    def _retain_sidecar_type_on_dump(self) -> "SidecarConfig":
        """Mark ``sidecar_type`` as explicitly set so ``exclude_unset=True`` keeps it.

        Concrete subclasses declare ``sidecar_type: Literal["..."] = "..."``; when
        a caller builds one via ``SomeConfig()`` with no arguments, Pydantic treats
        the default as not-explicitly-set, and ``model_dump(exclude_unset=True)``
        drops the discriminator.  That makes the dump non-roundtrippable via
        ``model_validate`` on the Union — the very point of the discriminator.

        This validator runs once after construction and adds ``"sidecar_type"``
        to ``__pydantic_fields_set__``, making the dump lossless regardless of
        how the instance was built.  See role-based driver plan §M1b.1.

        Rebase note: ``main`` shipped the same fix under the name
        ``_pin_discriminator_as_set`` (commit ``96fbf8c``).  The two
        implementations are semantically identical (same
        ``__pydantic_fields_set__.add("sidecar_type")`` body); we keep the
        branch's more-descriptive name + docstring here.  The failure
        mode ``main``'s commit message mentions — "default-constructed
        ``GeometriesSidecarConfig()`` serialises to ``{}`` under
        ``exclude_unset=True`` → Cloud Run ingestion crash on
        re-validate" — is the exact case this validator closes.
        """
        self.__pydantic_fields_set__.add("sidecar_type")
        return self

    @property
    def partition_key_contributions(self) -> Dict[str, str]:
        """
        Returns a dictionary of {column_name: sql_type} for keys this sidecar
        contributes to the global composite partition key.
        Override in subclasses.
        """
        return {}

    @property
    def has_validity(self) -> bool:
        """
        Whether this sidecar is configured to manage validity.
        """
        return False

    @property
    def partition_keys(self) -> List[str]:
        """
        List of partition keys this sidecar contributes.
        """
        return list(self.partition_key_contributions.keys())

    @property
    def partition_key_types(self) -> Dict[str, str]:
        """
        Mapping of partition keys to their SQL types.
        """
        return self.partition_key_contributions

    @property
    def sidecar_id(self) -> str:
        """
        Returns the standard sidecar_id for this config type.
        """
        mapping = {
            "geometry": "geometry",
            "attributes": "attributes",
            "access_envelope": "access_envelope",
        }
        return mapping.get(self.sidecar_type, self.sidecar_type)

    @property
    def feature_id_field_name(self) -> Optional[str]:
        """
        The field name used for feature ID in this sidecar's table.
        E.g., 'external_id' for AttributesSidecar.

        Returns None when this sidecar has no candidate feature-id column.

        Capability ("can this sidecar map to feature.id") lives on the
        :class:`SidecarProtocol` (``provides_feature_id``). Intent
        ("should it on a given read") lives on
        :class:`ItemsReadPolicy.feature_type.external_id_as_feature_id`.
        """
        return None

    model_config = {"extra": "allow"}


# Canonical sidecar types contributed by *optional* extensions that are not
# installed in every deployment SCOPE.  The slim ``maps``/``tiles`` service,
# for example, ships without the STAC extension, yet it reads the same
# DB-persisted ``ItemsPostgresqlDriverConfig`` rows authored by the
# STAC-enabled catalog service — rows that legitimately carry a
# ``stac_metadata`` sidecar.  When such a type can't be resolved to a concrete
# config class in *this* process, it is retained as an opaque base
# :class:`SidecarConfig` rather than rejected, so the shared config still
# loads; the consuming pipeline (:class:`QueryOptimizer`) already skips
# sidecars it can't instantiate (lenient) and consumer-gates extension-specific
# ones, so an unusable sidecar is a no-op here (a tile read JOINs only the
# geometry sidecar).  A ``sidecar_type`` that is neither registered nor listed
# here is treated as a typo and rejected loudly.
#
# An extension that adds a new optional sidecar type MUST add its canonical
# name here so slim services can round-trip configs that reference it.
OPTIONAL_EXTENSION_SIDECAR_TYPES: frozenset = frozenset({"stac_metadata", "access_envelope"})


def _coerce_pg_sidecar(v: Any) -> Any:
    """Resolve a sidecar config dict → its concrete subclass via
    :class:`SidecarConfigRegistry`, dispatching on ``sidecar_type``.

    Used as a Pydantic ``BeforeValidator`` on
    ``ItemsPostgresqlDriverConfig.sidecars``.  Reading the registry at
    every validation lets extensions register sidecar configs *after*
    class-definition of the items driver — important since the driver
    config is shared code while the extension configs (e.g. STAC) live
    in their own modules.

    Resolution rules for a ``sidecar_type``:

    - Registered → coerce to its concrete config subclass.
    - Unregistered but in :data:`OPTIONAL_EXTENSION_SIDECAR_TYPES` → retain
      as an opaque base :class:`SidecarConfig` (``extra="allow"`` keeps every
      field; :meth:`SidecarConfig._retain_sidecar_type_on_dump` keeps the
      discriminator), so a slim service can load a shared driver config that
      references an extension it doesn't host.
    - Unregistered and unknown (typo) → fail loud so configuration typos
      surface at validation time instead of silently degrading.

    Already-typed instances pass through unchanged so model-instance
    round-tripping (``model_dump(exclude_unset=True)`` → ``model_validate``)
    stays lossless.
    """
    if isinstance(v, SidecarConfig):
        return v
    if isinstance(v, dict):
        sc_type = v.get("sidecar_type")
        if not sc_type:
            registered = sorted(SidecarConfigRegistry._registry.keys())
            raise ValueError(
                f"sidecar_type is required; registered types: {registered}"
            )
        cls = SidecarConfigRegistry.resolve_config_class(sc_type)
        if cls is SidecarConfig:
            if sc_type in OPTIONAL_EXTENSION_SIDECAR_TYPES:
                logger.debug(
                    "sidecar_type %r not registered in this process; "
                    "retaining as opaque config (its extension is not in "
                    "this deployment's SCOPE)",
                    sc_type,
                )
                return SidecarConfig.model_validate(v)
            registered = sorted(SidecarConfigRegistry._registry.keys())
            raise ValueError(
                f"sidecar_type {sc_type!r} not registered; "
                f"registered types: {registered}"
            )
        return cls.model_validate(v)
    # Non-dict, non-SidecarConfig (e.g. a raw string) — let Pydantic
    # surface its native ``model_type`` error, which the upstream
    # error-message assertions still match.
    return v


# Field type for ``ItemsPostgresqlDriverConfig.sidecars``.  Keeping this
# alias inside ``pg_sidecars/`` (not in shared ``storage/driver_config``)
# means non-PG drivers (DuckDB, Iceberg, Elasticsearch) never see PG
# sidecar machinery.
#
# ``SerializeAsAny`` opts out of Pydantic v2's default type-narrowing on
# serialisation: without it, ``List[SidecarConfig]`` dumps every entry as
# the base class and drops every subclass field (spatial_cells_overlay,
# attribute_schema, …).  The runtime instance is the concrete subclass; the
# wrapper tells Pydantic to use that instance's serializer instead of the
# declared base type's, so the operator-facing config API renders the full
# DTO with all defaults.
PgSidecarConfig = Annotated[SerializeAsAny[SidecarConfig], BeforeValidator(_coerce_pg_sidecar)]


class SidecarProtocol(ABC):
    """
    Abstract base class for sidecar storage implementations.

    Each sidecar manages a specific domain of data (geometry, attributes, etc.)
    and must implement methods for DDL generation, data transformation,
    query resolution, and operation validation.
    """

    def __init__(self, config: SidecarConfig):
        """Initialize with configuration."""
        pass

    @classmethod
    def get_default_config(cls, context: Dict[str, Any]) -> Optional[SidecarConfig]:
        """
        Returns a default configuration for this sidecar type if it should be
        automatically injected based on the provided context.
        
        Args:
            context: injection context (e.g., {"stac_context": True})
            
        Returns:
            SidecarConfig instance or None if not applicable.
        """
        return None

    @property
    @abstractmethod
    def sidecar_id(self) -> str:
        """
        Unique identifier for this sidecar (e.g., 'geom', 'attrs').
        Used for table naming: {physical_table}_{sidecar_id}
        """
        pass

    @property
    @abstractmethod
    def sidecar_type_id(self) -> str:
        """
        Type identifier matching config sidecar_type (e.g., 'attributes', 'geometries').
        Used for protocol-based discovery and config matching.
        """
        pass

    def is_mandatory(self) -> bool:
        """
        Whether this sidecar must own a DB table and participate in writes.
        Sidecars that are pure read-overlays (no DDL of their own) return False.
        Default: True.
        """
        return True

    @classmethod
    def serves_consumers(cls) -> Optional[set]:
        """Consumers whose responses need this sidecar's projected columns.

        Returns ``None`` (default) for consumer-agnostic sidecars (geometry,
        attributes, item_metadata) — they always JOIN/SELECT.

        Returns a set of ``ConsumerType`` for consumer-specific sidecars
        whose payload only matters to particular response shapes (e.g.
        ``StacItemsSidecar`` → ``{ConsumerType.STAC}``).  When a query is
        running for a consumer not in that set, ``QueryOptimizer`` skips
        the JOIN+SELECT unless the caller explicitly references one of
        this sidecar's fields in ``query.select / filters / sort``.
        """
        return None

    @property
    def provides_feature_id(self) -> bool:
        """Capability flag — does this sidecar know how to map a feature
        column (e.g. ``external_id``) onto ``feature.id`` on read.

        Whether to *use* the capability on a given read is decided by
        ``ItemsReadPolicy.feature_type.external_id_as_feature_id``; this
        property only reports the sidecar's capability. At most one
        sidecar per collection should return True (validated at config
        time).

        Default: False.
        """
        return False

    @property
    def feature_id_field_name(self) -> Optional[str]:
        """
        The field name used for feature ID in this sidecar's table.
        Default: None.
        """
        return None

    @abstractmethod
    def get_ddl(
        self,
        physical_table: str,
        partition_keys: Optional[List[str]] = None,
        partition_key_types: Optional[Dict[str, str]] = None,
        has_validity: bool = False,
    ) -> str:
        """
        Generates the CREATE TABLE DDL for this sidecar.

        Args:
            physical_table: Physical Hub table name (e.g. 't_abc123')
            partition_keys: List of columns forming the composite partition key
            partition_key_types: Dictionary mapping key names to their SQL types
            has_validity: Flag indicating if the sidecar should include a 'validity' column.

        Returns:
            SQL DDL string for creating the sidecar table

        Note:
            - MUST include FK to Hub: FOREIGN KEY (geoid) REFERENCES hub(geoid) ON DELETE CASCADE
            - MUST use same PARTITION BY clause as Hub if partitioning is enabled
            - MUST use (partition_key, geoid) as composite PK
        """
        pass

    @abstractmethod
    async def setup_lifecycle_hooks(
        self, conn: DbResource, schema: str, table_name: str
    ) -> None:
        """Register maintenance hooks on table creation."""
        pass

    @abstractmethod
    async def on_partition_create(
        self,
        conn: DbResource,
        schema: str,
        parent_table: str,
        partition_table: str,
        partition_value: Any,
    ) -> None:
        """Hook called after JIT partition creation."""
        pass

    @abstractmethod
    def resolve_query_path(self, attr_name: str) -> Optional[Tuple[str, str]]:
        """Resolves an attribute reference to SQL and JOIN requirements."""
        pass

    @abstractmethod
    def apply_query_context(
        self,
        request: QueryRequest,
        context: Dict[str, Any],
    ) -> None:
        """
        Allows the sidecar to inspect the query request and contribute to the
        query definition (JOINs, params, SELECTs, WHEREs).

        The context dictionary must support:
        - "joins": List[str]            # Sidecar appends JOIN clauses here
        - "params": Dict[str, Any]      # Sidecar adds bind parameters here
        - "select_fields": List[str]    # Sidecar adds SELECT expressions here
        - "where_conditions": List[str] # Sidecar adds WHERE conditions here
        """
        pass

    @abstractmethod
    def get_queryable_fields(self) -> Dict[str, FieldDefinition]:
        """
        Returns all fields that can be used in queries.
        
        This includes:
        - Fields that can appear in WHERE clauses (filterable)
        - Fields that can appear in SELECT (if expose=True)
        - Fields that can appear in ORDER BY (sortable)
        - Fields that can appear in GROUP BY (groupable)
        - Storage-only fields (expose=False) that are queryable but not in Feature output
        
        Returns:
            Dict mapping field names to FieldDefinition with capabilities
        """
        pass

    @abstractmethod
    def get_feature_type_schema(self) -> Dict[str, Any]:
        """
        Returns JSON Schema fragment for this sidecar's contribution to Feature output.
        
        Only includes fields where expose=True in get_queryable_fields().
        For geometries sidecar, may include 'geometry' key for main geometry.
        
        Returns:
            Dict with JSON Schema properties for Feature output
        """
        pass

    def get_main_geometry_field(self) -> Optional[str]:
        """
        Returns the name of the main geometry field, if this sidecar provides one.
        
        Used when query requests 'geometry' without specifying which geometry field.
        Default implementation returns None (non-geometry sidecars).
        
        Returns:
            Field name of main geometry, or None
        """
        return None

    def get_property_field_names(self) -> List[str]:
        """
        Return the names of this sidecar's feature *property* fields, in a way
        that is independent of the physical storage layout.

        Property fields are the user-facing attributes that populate
        ``feature.properties`` — distinct from identity columns (external_id,
        asset_id), geometry, and computed statistics. A consumer that needs to
        project a collection's properties without knowing whether they live in a
        JSONB blob or individual COLUMNAR columns selects exactly these field
        names and lets the QueryOptimizer resolve each to its SQL expression.

        Default implementation returns an empty list; only sidecars that own
        user-facing properties (the attributes sidecar) override this.
        """
        return []

    @abstractmethod
    def get_identity_columns(self) -> List[str]:
        """
        Returns the list of columns that form the identity (PK) of the sidecar table.
        Example: ["geoid", "validity"] or just ["geoid"].
        """
        pass

    @abstractmethod
    def finalize_upsert_payload(
        self,
        sc_payload: Dict[str, Any],
        hub_row: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Finalizes the sidecar payload before storage.
        Allows injecting sidecar-specific details (like 'validity') from the Hub row or context.
        """
        pass

    @abstractmethod
    async def expire_version(
        self,
        conn: DbResource,
        physical_schema: str,
        physical_table: str,
        geoid: str,
        expire_at: datetime,
    ) -> int:
        """
        Marks the current active version as expired.
        Returns the number of rows updated.
        """
        pass

    @abstractmethod
    def prepare_upsert_payload(
        self, feature: Union[Feature, Dict[str, Any]], context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Transforms a feature into sidecar-specific insert data."""
        pass

    @abstractmethod
    def map_row_to_feature(
        self,
        row: Dict[str, Any],
        feature: Feature,
        context: FeaturePipelineContext,
    ) -> None:
        """
        Populate a GeoJSON feature from a database row.
        Each sidecar is responsible for mapping its specialized columns/data
        back into the standard GeoJSON structure (geometry, properties).

        Must be stateless (no DB lookups) to support O(1) streaming.
        Sidecars are allowed to override core Feature values (including the feature id)
        if their logic supersedes the default (e.g. mapping external_id to id).

        Sidecars SHOULD also populate ``context["_sidecar_data"][self.sidecar_id]``
        with all raw row values they fetched (even those not exposed in the Feature),
        so that downstream sidecars can access them (e.g. STAC reading bbox from
        the geometry sidecar's context entry).
        """
        pass

    def get_internal_columns(self) -> set:
        """
        Return the set of DB column names this sidecar manages that are
        **never** part of the public Feature output.

        Used by callers to build a dynamic exclusion set without hardcoding
        column names outside the sidecar that owns them.

        Default implementation returns an empty set.  Sidecars that store
        identity, geometry, or other non-property columns must override this.
        """
        return set()

    def producible_computed_names(self) -> Set[str]:
        """Resolved names of computed fields this sidecar can surface at read.

        Read-shape exposure (``ItemsReadPolicy.feature_type.expose``) consults
        this to learn which sidecar owns a given computed value. Storage-layout
        knowledge stays here, in the sidecar; the cross-sidecar exposure loop
        lives once in :meth:`apply_exposed_computed_values`. Default: none — a
        sidecar that materialises computed statistics overrides this.
        """
        return set()

    def resolve_computed_value(
        self, row: Dict[str, Any], resolved_name: str
    ) -> Tuple[bool, Any]:
        """Locate one producible computed value in a read row.

        Returns ``(found, value)``; ``found`` is False when this sidecar's
        storage layout has no slot for the value. Default: not found.
        """
        return (False, None)

    @staticmethod
    def apply_exposed_computed_values(
        sidecars: List["SidecarProtocol"],
        row: Dict[str, Any],
        feature: Feature,
        context: "FeaturePipelineContext",
    ) -> None:
        """Surface ``ItemsReadPolicy.feature_type.expose`` values onto properties.

        Applied ONCE per row, after every sidecar has contributed, so the
        ``failure_mode`` decision sees the complete producible set and no
        sidecar re-implements the exposure loop (the attributes sidecar reuses
        this verbatim for attribute-derived statistics). Each sidecar
        contributes only its storage-layout knowledge via
        :meth:`producible_computed_names` / :meth:`resolve_computed_value`.

        - A name already present on ``properties`` (a declared schema field
          surfaced by its sidecar) is left untouched.
        - A producible computed name is copied from the row.
        - Otherwise ``failure_mode`` decides: ``best_effort`` (default) omits it
          silently; ``strict`` raises so a typo or mid-flight schema change
          surfaces loudly.
        """
        policy = context.get("_items_read_policy") if context is not None else None
        feature_type = getattr(policy, "feature_type", None)
        if feature_type is None:
            return
        expose = list(getattr(feature_type, "expose", None) or [])
        if not expose:
            return
        strict = getattr(feature_type, "failure_mode", "best_effort") == "strict"

        if feature.properties is None:
            feature.properties = {}
        props = feature.properties

        # Map each producible computed name to the sidecar that owns it
        # (first registration wins; sidecars own disjoint name spaces).
        producers: Dict[str, "SidecarProtocol"] = {}
        for sidecar in sidecars:
            for name in sidecar.producible_computed_names():
                producers.setdefault(name, sidecar)

        for name in expose:
            if props.get(name) is not None:
                continue  # declared field already surfaced by its sidecar
            producer = producers.get(name)
            if producer is not None:
                found, value = producer.resolve_computed_value(row, name)
                if found and value is not None:
                    props[name] = value
                    continue
            if strict:
                raise ValueError(
                    "ItemsReadPolicy.feature_type.expose lists "
                    f"'{name}' but the read row carries no value for it."
                )

    # ── Pipeline context ---------------------------------------------------
    # Use FeaturePipelineContext (see module level) as the context type for
    # map_row_to_feature.  All sidecars should call:
    #   context.publish(self.sidecar_id, data)   # to write
    #   context.get_sidecar(other_id)            # to read from another sidecar

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
        Hook called after a new item has been successfully inserted into the database.
        Useful for side effects like linking assets, triggering events, etc.
        """
        pass

    # --- Query Generation Methods ---

    @abstractmethod
    def get_select_fields(
        self,
        request: Optional[QueryRequest] = None,
        hub_alias: str = "h",
        sidecar_alias: Optional[str] = None,
        include_all: bool = False,
    ) -> List[str]:
        """
        Returns list of SELECT field expressions.
        
        This method MUST be stateless and only use the request/config to 
        determine which fields to return.
        
        Args:
            request: The QueryRequest containing selection/filter/sort info.
            hub_alias: Alias of the hub table.
            sidecar_alias: Alias of the sidecar table.
            include_all: If True, returns all available fields regardless of request.
        """
        pass

    @abstractmethod
    def get_join_clause(
        self,
        schema: str,
        hub_table: str,
        hub_alias: str = "h",
        sidecar_alias: Optional[str] = None,
        join_type: str = "LEFT",
        extra_condition: Optional[str] = None,
    ) -> str:
        """
        Returns JOIN clause for this sidecar.

        Args:
            schema: Physical schema name
            hub_table: Physical hub table name
            hub_alias: Alias for hub table
            sidecar_alias: Alias for sidecar (auto-generated if None)
            join_type: JOIN type (LEFT, INNER, etc.)
            extra_condition: Additional condition to append to the ON clause (e.g. 'AND sc.validity @> NOW()')

        Returns:
            Complete JOIN clause (e.g., 'LEFT JOIN "schema"."table_geom" sc_geom ON h.geoid = sc_geom.geoid')
        """
        pass

    def get_where_conditions(
        self, sidecar_alias: Optional[str] = None, **filters
    ) -> List[str]:
        """
        Returns WHERE clause conditions for filtering by this sidecar's columns.

        Args:
            sidecar_alias: Alias for sidecar table
            **filters: Filter criteria (e.g., external_id="abc")

        Returns:
            List of WHERE conditions (e.g., ["sc.external_id = :ext_id"])
        """
        return []

    def get_feature_id_condition(
        self,
        feature_ids: Union[str, List[str]],
        hub_alias: str = "h",
        sidecar_alias: Optional[str] = None,
        partition_keys: Optional[List[str]] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Returns JOIN and WHERE conditions for feature ID resolution.

        This method is called when this sidecar is configured as the feature ID provider
        (provides_feature_id = True). It returns additional JOIN conditions and a WHERE
        clause to uniquely identify a feature by its ID.

        Args:
            feature_id: The feature ID to match
            hub_alias: Alias for hub table
            sidecar_alias: Alias for this sidecar table (auto-generated if None)
            partition_keys: List of partition key columns (if collection is partitioned)

        Returns:
            Tuple of (join_clause, where_clause):
            - join_clause: Additional JOIN conditions beyond geoid match.
              Must include:
              * Partition key matches (if partitioned)
              * Temporal validity conditions (if versioned)
              * Any other conditions needed for unique feature identification
            - where_clause: WHERE condition to match the feature ID

        Example for AttributesSidecar with validity and partitioning:
            join_clause = "AND sc_attr.asset_id = h.asset_id AND sc_attr.validity @> NOW()"
            where_clause = "sc_attr.external_id = %s"

        The complete JOIN becomes:
            LEFT JOIN t_abc_attributes sc_attr
                ON sc_attr.geoid = h.geoid          -- Always present
                AND sc_attr.asset_id = h.asset_id   -- Partition key match
                AND sc_attr.validity @> NOW()       -- Temporal validity
            WHERE sc_attr.external_id = %s          -- Feature ID match

        This ensures a unique join that identifies exactly one feature version.

        Returns (None, None) if provides_feature_id is False.
        """
        return (None, None)

    def get_spatial_condition(
        self,
        bbox: Optional[List[float]] = None,
        geometry: Optional[Dict[str, Any]] = None,
        sidecar_alias: Optional[str] = None,
        srid: int = 4326,
    ) -> Optional[str]:
        """
        Returns a spatial filter condition for this sidecar.
        """
        return None

    def get_geometry_select(
        self, params: Dict[str, Any], sidecar_alias: Optional[str] = None
    ) -> Optional[str]:
        """
        Returns the formatted geometry column expression for SELECT.
        Handles SRID transforms, simplification, and MVT/GeoJSON/GML formatting.
        """
        return None

    def get_group_by_fields(self, sidecar_alias: Optional[str] = None) -> List[str]:
        """
        Returns list of fields that can be used in GROUP BY clauses.

        Args:
            sidecar_alias: Alias for sidecar table

        """
        return []

    # --- Partitioning & Capabilities ---

    def get_partition_keys(self) -> List[str]:
        """
        Returns list of columns this sidecar contributes to composite partitioning.
        """
        return []

    def get_partition_key_types(self) -> Dict[str, str]:
        """
        Returns mapping of partition keys to their SQL types.
        """
        return {}

    def has_validity(self) -> bool:
        """
        Returns True if this sidecar manages temporal validity.
        """
        return False

    async def resolve_existing_item(
        self,
        conn: DbResource,
        physical_schema: str,
        physical_table: str,
        processing_context: Dict[str, Any],
        matcher: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Resolves an existing item using the requested identity matcher.

        Args:
            conn: Database connection.
            physical_schema: Physical schema name.
            physical_table: Physical table name (Hub).
            processing_context: Context containing resolved IDs / geometry.
            matcher: ``ComputedKind`` string (e.g. ``"external_id"``,
                     ``"geohash"``, ``"geometry_hash"``, ``"attributes_hash"``).
                     Sidecars return None for matchers they do not own.

        Returns:
            Dictionary with at least 'geoid' (and 'validity', 'geometry_hash'
            when available) of the matched row, or None if no match.
        """
        return None

    def get_identity_payload(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Returns the subset of the context that identifies the feature for this sidecar.
        Used for collision detection and identity resolution.
        """
        return {}

    async def check_upsert_collision(
        self,
        conn: DbResource,
        physical_schema: str,
        physical_table: str,
        processing_context: Dict[str, Any],
        exclude_geoid: Optional[Any] = None,
    ) -> bool:
        """
        Checks if the incoming feature conflicts with any existing feature
        based on the sidecar's uniqueness constraints.
        """
        return False

    def is_acceptable(self, feature: Dict[str, Any], context: Dict[str, Any]) -> bool:
        """
        Checks if the sidecar can accept this feature for processing.

        Args:
            feature: The incoming GeoJSON feature.
            context: Processing context.

        Returns:
            True if feature is acceptable, False otherwise.
        """
        return True

    def get_field_definitions(
        self, sidecar_alias: Optional[str] = None
    ) -> Dict[str, FieldDefinition]:
        """
        Returns all queryable fields exposed by this sidecar.
        
        DEPRECATED: Use get_queryable_fields() directly instead.
        This method delegates to get_queryable_fields() for backward compatibility.

        Returns:
            Dict mapping field name to FieldDefinition
        """
        return self.get_queryable_fields()

    def get_dynamic_field_definition(
        self, field_name: str, sidecar_alias: Optional[str] = None
    ) -> Optional[FieldDefinition]:
        """
        Returns definition for a field that is not statically defined but supported dynamically.
        Used for schemaless sidecars (e.g. JSONB attributes).
        """
        return None

    def get_default_sort(self) -> Optional[List[Tuple[str, str]]]:
        """
        Returns default sort order for this sidecar.

        Returns:
            List of (field_name, direction) tuples, e.g., [("external_id", "ASC")]
        """
        return None

    def supports_aggregation(self, field_name: str, agg_func: str) -> bool:
        """Check if field supports specific aggregation."""
        fields = self.get_field_definitions()
        field = fields.get(field_name)
        return field is not None and field.supports_aggregation(agg_func)

    def supports_transformation(self, field_name: str, transform_func: str) -> bool:
        """Check if field supports specific transformation."""
        fields = self.get_field_definitions()
        field = fields.get(field_name)
        return field is not None and field.supports_transformation(transform_func)

    # --- Operation Validation Hooks ---

    def validate_insert(
        self, feature: Dict[str, Any], context: Dict[str, Any]
    ) -> ValidationResult:
        """
        Validate if a feature can be inserted into this sidecar.
        Default implementation allows everything.
        """
        return ValidationResult(valid=True)

    def validate_update(
        self, feature: Dict[str, Any], existing: Dict[str, Any], context: Dict[str, Any]
    ) -> ValidationResult:
        """
        Validate if a feature update is allowed.
        Default implementation allows everything.
        """
        return ValidationResult(valid=True)

    def validate_delete(self, geoid: str, context: Dict[str, Any]) -> ValidationResult:
        """
        Validate if a feature deletion is allowed.
        Default implementation allows everything.
        """
        return ValidationResult(valid=True)
