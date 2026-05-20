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

from pydantic import BaseModel

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
    data_type: str  # "geometry", "text", "integer", "jsonb", "float", "boolean", "timestamp", etc.
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
