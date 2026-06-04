"""
Driver-agnostic field definitions and feature type declarations.

``FieldDefinition`` and ``FieldCapability`` describe the queryable surface of a
collection — name, type, capabilities (filterable, sortable, etc.).  These are
protocol-level concepts used by all drivers and extensions.

``FeatureTypeDefinition`` acts as a decorator: it declares which fields are
exposed, added, or removed at a given entity level (catalog, collection, item,
asset).  Drivers use it to create storage with the right structure; extensions
use it to build OGC/STAC responses.
"""

from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator

from dynastore.models.field_types import (
    CANONICAL_DATA_TYPES,
    CANONICAL_SUBTYPES,
    canonical_data_type,
    normalize_subtype,
)
from dynastore.models.localization import LocalizedText


class FieldCapability(str, Enum):
    """Capabilities a field can have — driver-agnostic.

    Two kinds of member live here:

    * **Authorable intents** — what a config author *asks for*. ``FILTERABLE``,
      ``SORTABLE``, ``GROUPABLE``, ``AGGREGATABLE``, ``SPATIAL`` and
      ``FULLTEXT`` are declared by the author (or inferred by a reader from the
      native type) to say "this field should support predicate X".
    * **Driver-reported introspection** — what a driver *observes* after it has
      built storage. ``INDEXED`` is the only such member: it means the backing
      store has a physical index on the field. It is **never** an authoring
      knob — an author does not set ``INDEXED`` to request that an index be
      built. The portable way to ask for fast filtering/sorting is
      :attr:`FieldAccess.FAST`; the driver then decides whether to
      build an index and reports ``INDEXED`` back through ``get_entity_fields``.

    ``FILTERABLE`` vs ``FULLTEXT`` is a deliberate split:

    * ``FILTERABLE`` = exact-match / keyword predicate. A CQL2 ``=`` resolves to
      an Elasticsearch ``term`` over the ``.keyword`` sub-field; on PostgreSQL it
      is an equality predicate over a column / JSONB path.
    * ``FULLTEXT`` = analyzed free-text search. The field is offered for an
      Elasticsearch ``match`` over an analyzed ``.text`` (per-locale analyzer)
      field. It is additive: a field can be both ``FILTERABLE`` and ``FULLTEXT``.
    """

    FILTERABLE = "filterable"       # Authorable: exact-match / keyword predicate (WHERE / CQL2 `=`)
    SORTABLE = "sortable"           # Authorable: can be used in ORDER BY / sortby
    GROUPABLE = "groupable"         # Authorable: can be used in GROUP BY
    AGGREGATABLE = "aggregatable"   # Authorable: can be aggregated (SUM, COUNT, etc.)
    SPATIAL = "spatial"             # Authorable: spatial operations available
    FULLTEXT = "fulltext"           # Authorable: analyzed free-text search (ES `match`), distinct from FILTERABLE
    INDEXED = "indexed"             # Driver-REPORTED only: storage has a backing index (NOT an authoring knob — use FieldAccess.FAST)


# Capabilities that let a field appear in a filter predicate: exact/keyword
# (FILTERABLE), spatial (SPATIAL — ST_* / bbox) or analyzed free-text (FULLTEXT
# — ES ``match``). Used by ``FieldDefinition.is_filterable`` to decide, read-side
# only, whether a field with an *explicit* capability set is filterable. A field
# with no explicit capabilities is filterable by default (see is_filterable).
_FILTERING_CAPABILITIES: "frozenset[FieldCapability]" = frozenset({
    FieldCapability.FILTERABLE,
    FieldCapability.SPATIAL,
    FieldCapability.FULLTEXT,
})


class FieldAccess(str, Enum):
    """How aggressively a field should be optimised for query access — driver-agnostic.

    The *intent* is portable; the *mechanism* stays with the driver. ``FAST`` asks the
    driver to optimise the field for filtering/sorting — PostgreSQL lifts it to a native
    sidecar column with an index, Elasticsearch maps a typed/keyword field, a
    Parquet/GeoParquet driver emits column statistics plus a bloom filter, Iceberg adds a
    sort field, a GDAL/GeoPackage driver adds an attribute index. ``COMPACT`` asks the
    driver to minimise storage instead — PostgreSQL keeps the value in the JSONB
    properties blob. ``AUTO`` (the default) lets the driver decide from the field's
    declared :class:`FieldCapability` set.
    """

    AUTO = "auto"
    FAST = "fast"
    COMPACT = "compact"


class FieldDefinition(BaseModel):
    """Definition of a queryable field — driver-agnostic.

    Each driver maps its native type system to ``FieldDefinition``:
    - PG driver: wraps ``QueryOptimizer.get_all_queryable_fields()``
    - ES driver: maps ES field mappings
    - Iceberg driver: maps Iceberg schema types
    - DuckDB driver: maps DuckDB column types
    """

    name: str
    alias: Optional[str] = None
    title: Optional[Union[str, Dict[str, str], LocalizedText]] = None
    description: Optional[Union[str, Dict[str, str], LocalizedText]] = None
    capabilities: List[FieldCapability] = []
    data_type: str = Field(
        description=(
            "Canonical, GDAL-rooted field type. WORKS TODAY: one of "
            + ", ".join(sorted(CANONICAL_DATA_TYPES))
            + ", or a parametrized geometry such as ``geometry(Point,4326)``; "
            "gdalinfo types are accepted via the items-schema/derive endpoint, "
            "which translates OGR names (e.g. Real -> double) to these tokens. "
            "DEPRECATED (still accepted, will be removed): legacy SQL spellings "
            "(e.g. text, int, float, real, datetime, json, bool) are folded to "
            "the canonical token during a temporary migration window. See the "
            "Field Types reference (docs/components/field-types.md) for the full "
            "today / deprecated / planned breakdown."
        ),
        json_schema_extra={
            "examples": ["string", "integer", "timestamp", "geometry(Point,4326)"],
        },
    )
    subtype: Optional[str] = Field(
        default=None,
        description=(
            "Optional OGR-style refinement of ``data_type``, carrying the "
            "gdalinfo subtype so it is not flattened away. One of: "
            + ", ".join(sorted(CANONICAL_SUBTYPES))
            + ". boolean/json/uuid promote the base type today; int16/float32 "
            "keep their base now and are recorded for the planned narrowing to "
            "SMALLINT/REAL (see docs/components/field-types.md)."
        ),
    )
    # Driver-computed read-projection detail (e.g. "h.geom", "a.asset_id"). NOT
    # author-facing: it means nothing to ES/Iceberg/DuckDB/BigQuery/Parquet/GDAL and
    # would be a raw-SQL injection seam if author-settable (same class of footgun as
    # ``physical_table``, #1135). Drivers populate it when they build queryable-field
    # maps; it is ``exclude``d from serialization and marked read-only so it never
    # enters the author config or leaks into API/queryables responses (#1291).
    sql_expression: str = Field(default="", exclude=True, json_schema_extra={"readOnly": True})
    expose: bool = True  # Whether to expose in public APIs (OGC, STAC)
    required: bool = False  # Reject feature if value is null/missing
    unique: bool = False    # Value must be unique within the collection
    aggregations: Optional[List[str]] = None   # None or ["*"] = all, [] = none, ["count","sum"] = specific
    transformations: Optional[List[str]] = None
    max_length: Optional[int] = None
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    enum: Optional[List[Any]] = None
    pattern: Optional[str] = None
    format: Optional[str] = None
    # Optional ingestion-time parse hint for a temporal field (``date`` /
    # ``time`` / ``timestamp``). A ``strptime`` pattern (e.g. ``%d/%m/%Y``) that
    # the ingestion coercion uses to read the source string before re-emitting
    # canonical ISO-8601. Distinct from ``format`` above, which carries the
    # JSON-Schema *output* format (``date`` vs ``date-time``); this is the
    # *input* pattern. When set, it disambiguates numeric formats that
    # ``dateutil``'s auto-detection resolves month-first (so a European
    # ``01/02/2024`` is read as 1 Feb, not 2 Jan). Unset → auto-detection.
    # Ignored for non-temporal fields. See ``apply_temporal_coercion`` (#1350).
    parse_format: Optional[str] = None
    # Driver-agnostic access intent — how aggressively to optimise this field for
    # query access (see :class:`FieldAccess`). The driver picks the mechanism; AUTO
    # defers to the declared ``capabilities``. Replaces the former PG-specific
    # ``materialize`` boolean (#1291).
    access: FieldAccess = FieldAccess.AUTO
    # Optional default value: the value to use when the field is missing on write.
    # The driver decides how to apply it — a native column DEFAULT, inject-on-write
    # into the stored properties, or ignore it if the backend can't express defaults.
    # Independent of ``access`` (#1291). The value's Python type must match
    # ``data_type`` (validated below). SSOT counterpart of ``AttributeSchemaEntry.default``.
    default: Optional[Any] = None
    # Canonical ES envelope container. Drives both the ES mapping builder
    # (``build_item_mapping``) and the field-path resolvers (``resolve_es_field_path``,
    # ``build_es_field_mapping``, ``parse_sort``). Single source of truth — no drift
    # between mapping and resolution. Sidecars that produce computed statistics set
    # this to ``"stats"``; the platform identity / lifecycle fields use ``"system"``
    # or ``"identity"``; multilingual descriptive metadata (title/description/keywords)
    # uses ``"metadata"``; user / STAC attributes default to ``"properties"``. The
    # open-ended lanes (``"extras"``, ``"stac"``, ``"assets"``, ``"access"``) are mapped
    # ``flat_object``/``flattened`` so they never grow the strict mapping (refs #1800/#1828).
    container: Literal[
        "identity",
        "properties",
        "stats",
        "system",
        "metadata",
        "extras",
        "stac",
        "assets",
        "access",
    ] = "properties"

    @field_validator("data_type")
    @classmethod
    def _validate_data_type(cls, v: str) -> str:
        # Canonical vocabulary — every FieldDefinition, regardless of which
        # driver/reader built it, holds the same token for a given logical type.
        # Legacy spellings normalize to canonical (temporary window); truly
        # unknown values raise.
        return canonical_data_type(v)

    @field_validator("subtype")
    @classmethod
    def _normalize_subtype(cls, v: Optional[str]) -> Optional[str]:
        return normalize_subtype(v)

    @model_validator(mode="after")
    def _validate_default_type(self) -> "FieldDefinition":
        """Reject a ``default`` whose Python type can't back ``data_type``.

        Caught at config-parse time rather than deep in DDL generation, over the
        canonical vocabulary. ``date``/``time``/``timestamp`` take an ISO-8601
        string (config is JSON, so there is no native datetime); ``jsonb`` and
        any parametrized/unknown canonical token accept any value; ``geometry``
        has no SQL-literal default form and rejects one. Mirrors
        ``AttributeSchemaEntry.validate_default_type`` but also keeps ``bool``
        out of the numeric types (``bool`` is an ``int`` subclass in Python).
        """
        if self.default is None:
            return self
        dt = (self.data_type or "").lower()
        if dt in ("integer", "bigint"):
            if isinstance(self.default, bool) or not isinstance(self.default, int):
                raise ValueError(
                    f"Default for field '{self.name}' must be an integer, "
                    f"got {type(self.default).__name__}"
                )
        elif dt in ("double", "numeric"):
            if isinstance(self.default, bool) or not isinstance(self.default, (int, float)):
                raise ValueError(
                    f"Default for field '{self.name}' must be a number, "
                    f"got {type(self.default).__name__}"
                )
        elif dt == "boolean":
            if not isinstance(self.default, bool):
                raise ValueError(
                    f"Default for field '{self.name}' must be a boolean, "
                    f"got {type(self.default).__name__}"
                )
        elif dt in ("string", "uuid", "date", "time", "timestamp", "binary"):
            if not isinstance(self.default, str):
                raise ValueError(
                    f"Default for field '{self.name}' must be a string, "
                    f"got {type(self.default).__name__}"
                )
        elif dt.startswith("geometry"):
            raise ValueError(
                f"Default values are not supported on geometry field '{self.name}'"
            )
        # jsonb (and any parametrized/unknown canonical token): accept as-is.
        return self

    def supports_aggregation(self, agg_func: str) -> bool:
        if self.aggregations is None or "*" in (self.aggregations or []):
            return True
        return agg_func in (self.aggregations or [])

    def supports_transformation(self, transform_func: str) -> bool:
        if self.transformations is None or "*" in (self.transformations or []):
            return True
        return transform_func in (self.transformations or [])

    def is_filterable(self) -> bool:
        """Whether this field may be used in a filter predicate — read-side only.

        Filterable by default: a field with an *unspecified* (empty) capability
        set is treated as filterable, so a config author does not have to spell
        out ``FILTERABLE`` on every field. A field is non-filterable only when it
        declares an explicit, non-empty capability set that contains none of the
        filtering capabilities (:data:`_FILTERING_CAPABILITIES`) — the deliberate
        opt-out (e.g. ``capabilities=[SORTABLE]`` to allow sort but block
        filtering).

        Any one filtering capability suffices: ``FILTERABLE`` (exact/keyword),
        ``SPATIAL`` (a spatial predicate such as ``ST_Intersects`` / bbox) or
        ``FULLTEXT`` (an analyzed ``match``). So a geometry field declared
        ``[SPATIAL]`` is still filterable via its spatial predicate.

        This reads ``capabilities`` without mutating it. The stored set is left
        untouched on purpose: the write/DDL access rules treat ``FILTERABLE`` as
        a *column-implying* capability (see ``field_constraints._COLUMN_CAPS``),
        so injecting a default ``FILTERABLE`` into the stored set would flip
        AUTO-access JSONB fields into native columns — an unwanted schema change
        on existing collections. Defaulting lives here, on the read path, where
        it has no storage-layout effect.
        """
        return not self.capabilities or any(
            c in _FILTERING_CAPABILITIES for c in self.capabilities
        )

    def is_sortable(self) -> bool:
        """Whether this field may be used in an ORDER BY — read-side only.

        Same default-capable semantics as :meth:`is_filterable`: an unspecified
        capability set is sortable; an explicit, non-empty set that omits
        ``SORTABLE`` is the opt-out. Read-only over ``capabilities`` for the same
        write/DDL-safety reason described there.
        """
        return not self.capabilities or FieldCapability.SORTABLE in self.capabilities


class EntityLevel(str, Enum):
    """Which entity level a feature type definition applies to."""

    CATALOG = "catalog"
    COLLECTION = "collection"
    ITEM = "item"       # covers features and records
    ASSET = "asset"


class FeatureTypeDefinition(BaseModel):
    """Declared schema for an entity level.

    Acts as a decorator: defines which fields are exposed, added, or removed.
    Drivers use this to create storage with the right structure.
    Extensions use this to build OGC/STAC responses.

    Stored as ``ItemsSchema`` config in the waterfall (identity: class_key = ``"items_schema"``).

    Note: the ``PluginConfig`` subclass used for config registration lives in
    ``modules.storage.driver_config.ItemsSchema`` to avoid
    circular imports from ``models.protocols`` → ``modules.db_config``.
    """

    level: EntityLevel = EntityLevel.ITEM
    fields: Dict[str, FieldDefinition] = {}
    exclude_fields: Optional[List[str]] = None
    metadata_fields: Optional[Dict[str, Any]] = None
