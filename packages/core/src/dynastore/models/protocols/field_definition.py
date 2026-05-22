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
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, field_validator, model_validator

from dynastore.models.field_types import canonical_data_type, normalize_subtype
from dynastore.models.localization import LocalizedText


class FieldCapability(str, Enum):
    """Capabilities a field can have — driver-agnostic."""

    FILTERABLE = "filterable"       # Can be used in WHERE / CQL2 filter
    SORTABLE = "sortable"           # Can be used in ORDER BY / sortby
    GROUPABLE = "groupable"         # Can be used in GROUP BY
    AGGREGATABLE = "aggregatable"   # Can be aggregated (SUM, COUNT, etc.)
    SPATIAL = "spatial"             # Spatial operations available
    INDEXED = "indexed"             # Has a backing index (informational)


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
    # Canonical, GDAL-rooted type vocabulary (see ``dynastore.models.field_types``):
    # string / integer / bigint / double / numeric / boolean / date / time /
    # timestamp / binary / jsonb / uuid / geometry. Strict: a non-canonical value
    # raises on assignment — there is no legacy alias layer (text/int/float/… are
    # rejected, not silently rewritten).
    data_type: str
    # Optional OGR-style refinement of ``data_type`` — boolean / int16 / float32
    # / json / uuid. Carries the gdalinfo subtype so it is not flattened away.
    subtype: Optional[str] = None
    sql_expression: str = ""  # Driver-specific SQL expression for the field (e.g. "h.geom", "a.asset_id").
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
    # None = driver decides from capabilities; True = force a native column; False = force JSONB.
    materialize: Optional[bool] = None
    # Optional default value for the field. When the field is materialised as a
    # native column it becomes the column DEFAULT (and back-fills missing values
    # on write); on JSONB-stored fields it has no effect. The value's Python type
    # must match ``data_type`` (validated below). This is the SSOT counterpart of
    # ``AttributeSchemaEntry.default``.
    default: Optional[Any] = None

    @field_validator("data_type")
    @classmethod
    def _validate_data_type(cls, v: str) -> str:
        # Strict canonical vocabulary — every FieldDefinition, regardless of
        # which driver/reader built it, holds the same token for a given logical
        # type. Non-canonical values raise (no legacy alias fallback).
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
