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

# dynastore/modules/stac/stac_config.py

from enum import Enum
from typing import Any, ClassVar, Dict, List, Literal, Optional, Tuple, Union
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from dynastore.models.mutability import Mutable
from dynastore.modules.db_config.plugin_config import PluginConfig
from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin
from dynastore.models.localization import LocalizedText, Language

# --- STAC Core & Navigation ---

class StacLink(BaseModel):
    """Represents a standard STAC Link Object."""
    rel: str
    href: str
    type: Optional[str] = "application/json"
    title: Optional[str] = None
    method: Optional[str] = "GET"

# --- Dynamic Hierarchy Configuration ---

class HierarchyStrategy(str, Enum):
    """Defines the strategy to build the hierarchy."""
    FIXED = "FIXED"         # Levels are defined by fixed rules (e.g. L1, L2, L3)
    RECURSIVE = "RECURSIVE" # Levels are defined recursively (Parent -> Child)

class DatacubeDimensionType(str, Enum):
    """Common types for datacube dimensions."""
    SPATIAL = "spatial"
    TEMPORAL = "temporal"
    ORDINAL = "ordinal"
    NOMINAL = "nominal"

class DynamicSourceType(str, Enum):
    """Defines how to dynamically populate datacube dimension values."""
    ATTRIBUTE_SCAN = "attribute_scan"
    SQL_QUERY = "sql_query"
    STATIC = "static"

class DynamicSource(BaseModel):
    type: DynamicSourceType = DynamicSourceType.ATTRIBUTE_SCAN
    target_attribute: Optional[str] = None
    custom_query: Optional[str] = None

class DatacubeDimension(BaseModel):
    """STAC Datacube dimension with OGC Dimensions pagination support.

    Standard STAC Datacube fields (type, extent, values, step, unit) describe
    the dimension.  For large dimensions that cannot be enumerated inline,
    three additional fields enable paginated access:

    - ``size``: total member count (allows cardinality check without download)
    - ``href``: link to a paginated OGC API endpoint returning members
    - ``generator``: algorithmic generation metadata (type, config, invertible)

    Small dimensions (< ~1 000 members) should use ``values`` directly.
    Large dimensions (dekadal over decades, indicator catalogs, admin trees)
    should set ``size`` + ``href`` and optionally ``generator``.
    """
    type: DatacubeDimensionType
    description: Optional[str] = None
    axis: Optional[str] = None
    extent: Optional[List[Any]] = None
    values: Optional[List[Any]] = None
    step: Optional[Union[float, str]] = None
    unit: Optional[str] = None
    reference_system: Optional[Union[str, int, dict]] = 4326
    dynamic_source: Optional[DynamicSource] = None

    # OGC Dimensions pagination properties
    size: Mutable[Optional[int]] = Field(default=None,
        description=(
            "Total number of discrete members in the dimension. "
            "Allows clients to assess cardinality without downloading values."
        ),
    )
    href: Mutable[Optional[str]] = Field(default=None,
        description=(
            "Link to a paginated endpoint returning dimension members "
            "following OGC API - Common Part 2 pagination conventions "
            "(limit/offset query params, rel:next/prev links)."
        ),
    )
    generator: Mutable[Optional[Dict[str, Any]]] = Field(default=None,
        description=(
            "Generator object describing algorithmic rules for producing "
            "dimension members. Contains type (e.g. 'daily-period'), config, "
            "invertible flag, capabilities, and search_protocols. "
            "See ogc-dimensions spec/schema/generator.json for the full schema."
        ),
    )

class HierarchyRule(BaseModel):
    """Defines a specific hierarchy level rule based on row content."""
    strategy: Mutable[HierarchyStrategy] = Field(HierarchyStrategy.FIXED, description="The strategy used for this rule.")
    
    # Identification
    hierarchy_id: Mutable[str] = Field(..., description="Unique identifier for this hierarchy rule (e.g., 'admin_level_0', 'region').")
    parent_hierarchy_id: Mutable[Optional[str]] = Field(default=None, description="The hierarchy_id of the parent level (e.g., 'admin_level_0' for a region level). Used to generate parent/child links between hierarchy levels.")
    
    # Common Fields
    item_code_field: Mutable[str] = Field(..., description="The feature attribute used as the ID (e.g., iso_code, l4).")
    parent_code_field: Mutable[Optional[str]] = Field(default=None, description="The attribute for the parent ID within items (e.g., continent_code). Links items to their parent in the same collection.")
    
    # Fixed Strategy Fields
    level_name: Mutable[Optional[str]] = Field(default=None, description="Name of this level (e.g. 'Region', 'Level 4').")
    condition: Mutable[Optional[str]] = Field(default=None, description="A SQL-like filter string (e.g., type='admin_0', l4 IS NOT NULL).")
    
    # Recursive Strategy Fields
    root_condition: Mutable[Optional[str]] = Field(default=None, description="(For Recursive Strategy) A filter to identify root nodes (e.g., parent_code IS NULL).")
    
    # Virtual Collection Metadata
    collection_title_template: Mutable[Optional[str]] = Field(default=None, description="Template for virtual collection title (e.g., '{level_name}: {value}').")
    collection_description_template: Mutable[Optional[str]] = Field(default=None, description="Template for virtual collection description.")
    
    # Filtering and Links
    link_properties: Mutable[Optional[List[str]]] = Field(default=None, description="Properties to expose in hierarchy links.")
    item_filter_properties: Mutable[Optional[List[str]]] = Field(default=None, description="Additional properties for item filtering.")
    
    # Metadata
    datacube_dim: Mutable[Optional[DatacubeDimension]] = Field(default=None, description="Metadata for OGC Cube dimensions associated with this level.")
    
    # Virtual View Mapping (deprecated in favor of hierarchy_id)
    id_alias: Mutable[Optional[str]] = Field(default=None, description="An alias for the generated collection ID to be used in virtual hierarchy endpoints.")

class HierarchyConfig(BaseModel):
    """Defines how to dynamically extract parent/child relationships."""
    enabled: Mutable[bool] = Field(True, description="Enable or disable hierarchical link generation.")
    rules: Mutable[Dict[str, HierarchyRule]] = Field(default_factory=dict, description="Dictionary of SQL-based rules keyed by hierarchy_id. Legacy data-derived path.")
    providers: Mutable[Dict[str, Any]] = Field(
        default_factory=dict,
        description=(
            "Pluggable hierarchy providers keyed by hierarchy_id. "
            "Each value is a HierarchyProviderConfig (kind=data-derived|dimension-backed|static|external-skos). "
            "When set, takes precedence over `rules` for that hierarchy_id. "
            "Typed as Any to avoid a circular import with extensions.stac.hierarchy.config."
        ),
    )

    @model_validator(mode='after')
    def check_rules_if_enabled(self) -> 'HierarchyConfig':
        """
        Validates that if the hierarchy is enabled, at least one rule is defined.
        """
        if self.enabled and not self.rules:
            # We allow empty rules if just enabled but no specific logic is configured yet
            pass 
        return self


class DatacubeVariable(BaseModel):
    type: str = "data"
    description: Optional[str] = None
    unit: Optional[str] = None
    dimensions: Mutable[List[str]] = Field(default_factory=list)

# --- Aggregation Configuration (OGC STAC Aggregation Extension) ---

class AggregationType(str, Enum):
    """OGC STAC Aggregation types."""
    TERM = "term"              # Frequency counts by unique values
    STATS = "stats"            # Statistical aggregation (min, max, avg, sum, count)
    GEOHASH = "geohash"        # Spatial aggregation using geohash grid
    GEOTILE = "geotile"        # Tile-based spatial aggregation
    DATETIME = "datetime"      # Temporal histogram aggregation
    BBOX = "bbox"              # Bounding box extent aggregation
    TEMPORAL_EXTENT = "temporal_extent"  # Temporal extent aggregation (min/max datetime)

class AggregationRule(BaseModel):
    """Defines a single aggregation rule."""
    name: Mutable[str] = Field(..., description="Unique name for this aggregation")
    type: Mutable[AggregationType] = Field(..., description="Type of aggregation to perform")
    property: Mutable[str] = Field(..., description="Property to aggregate (e.g., 'properties.asset_code', 'properties.country')")
    limit: Mutable[int] = Field(10, ge=1, le=1000, description="Maximum number of buckets to return")
    precision: Mutable[Optional[int]] = Field(default=None, description="Precision for geohash/geotile aggregations (1-12 for geohash)")
    interval: Mutable[Optional[str]] = Field(default=None, description="Interval for datetime histograms (e.g., '1d', '1M', '1y')")

class AggregationConfig(BaseModel):
    """Configuration for STAC aggregations following OGC extension."""
    default_rules: Mutable[List[AggregationRule]] = Field(default_factory=list, description="Pre-configured aggregations available by default")
    allow_custom: Mutable[bool] = Field(True, description="Allow ad-hoc aggregations via API requests")
    max_aggregations_per_request: Mutable[int] = Field(5, ge=1, le=10, description="Maximum number of aggregations per request")

# --- Simplification (Replaces env vars) ---

class SimplificationConfig(BaseModel):
    """
    Dynamic geometry simplification rules applied at query time.
    """
    vertex_thresholds: Mutable[Dict[int, float]] = Field(
        default_factory=lambda: {50000: 0.1, 10000: 0.01, 1000: 0.001}
    )
    default_tolerance: float = 0.0001

class AssetAccessMode(str, Enum):
    """Defines how the asset file is exposed in the STAC Item."""
    PROXY = "proxy"       # Use the internal proxy service (safe, default)
    DIRECT = "direct"     # Expose the raw storage URL (e.g. s3:// or https://storage..., good for public buckets)

class AssetTrackingConfig(BaseModel):
    """Configuration for dynamic asset tracking."""
    enabled: Mutable[bool] = Field(True, description="Enable or disable dynamic tracking for ingestion sources.")
    access_mode: Mutable[AssetAccessMode] = Field(AssetAccessMode.DIRECT, description="Method to expose the source file.")

# --- STAC Summaries ---

class StacSummaryRange(BaseModel):
    """STAC Range Object for summaries: ``{minimum, maximum}`` plus optional derived stats."""
    model_config = ConfigDict(extra="allow")
    minimum: Union[float, int, str]
    maximum: Union[float, int, str]

# A summary value is one of: Range | enum array | JSON Schema dict
StacSummaryValue = Union[StacSummaryRange, List[Any], Dict[str, Any]]

# --- STAC Asset Definition ---

class StacAssetDefinition(BaseModel):
    """
    Open-schema STAC asset definition for collection ``assets`` and ``item_assets``.

    Text fields (``title``, ``description``) use ``LocalizedText`` for multilanguage.
    Extension-specific fields (``eo:bands``, ``raster:bands``, ``proj:shape``,
    ``table:columns``, etc.) are accepted directly via ``extra="allow"`` and
    validated downstream by ``stac-pydantic``'s ``validate_extensions()`` against
    the actual JSON schemas listed in ``stac_extensions``.

    For collection ``assets``, ``href`` is required.
    For ``item_assets``, ``href`` MUST NOT be set (templates only).
    """
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    href: Optional[str] = None
    title: Optional[LocalizedText] = None
    description: Optional[LocalizedText] = None
    type: Mutable[Optional[str]] = Field(default=None, description="IANA media type (e.g. image/tiff; application=geotiff)")
    roles: Mutable[Optional[List[str]]] = Field(default=None, description="Semantic roles: data, overview, thumbnail, metadata, source, …")
    hreflang: Mutable[Optional[str]] = Field(default=None, description="RFC 5646 language tag for the href target (STAC Language Extension)")

    @field_validator("title", "description", mode="before")
    @classmethod
    def wrap_localized(cls, v: Any) -> Any:
        if isinstance(v, str):
            return {Language.EN.value: v}
        return v

# --- THE PLUGIN DEFINITION ---

class StacPluginConfig(ExposableConfigMixin, PluginConfig):
    """
    Mutable STAC metadata and behavior configuration.
    """
    _address: ClassVar[Tuple[str, ...]] = ("platform", "extensions", "stac")

    # Extension schemas — URIs advertised on the collection's top-level
    # ``stac_extensions`` array.  Independent of ``auto_render_extensions``
    # below: this list is *what we say we use*, the next list is *what we
    # auto-generate at item-render time*.
    enabled_extensions: Mutable[List[str]] = Field(default_factory=list)

    # Item-render gate.  Controls which auto-generators contribute content
    # at render time (proj/raster/vector blocks from ``StacExtensionProtocol``
    # providers, plus the collection-level ``proj:epsg``/``proj:wkt2``
    # injection driven by ``storage_config.target_srid``).
    #
    # Default keeps current behavior — all three auto-renderers run.
    # Empty list = passthrough mode: stored content (``external_extensions``
    # / ``external_assets`` / ``extra_fields`` from the ``stac_metadata``
    # sidecar) flows verbatim, no auto-generation; navigation / hierarchy
    # links still get added.
    auto_render_extensions: Mutable[List[Literal["proj", "raster", "vector"]]] = Field(
        default_factory=lambda: ["proj", "raster", "vector"],
        description=(
            "Which extension auto-renderers run at item-render time.  Empty "
            "list = passthrough mode: only externally-supplied content (from "
            "the stac_metadata sidecar) is surfaced; HATEOAS navigation and "
            "hierarchy links still get added.  Default enables all three."
        ),
        examples=[
            ["proj", "raster", "vector"],
            ["proj"],
            [],
        ],
    )
    
    # Metadata summaries (Range Object, enum array, or JSON Schema dict)
    summaries: Mutable[Dict[str, StacSummaryValue]] = Field(default_factory=dict)

    # STAC Providers (spec §providers)
    providers: Mutable[List[Dict[str, Any]]] = Field(
        default_factory=list,
        description="List of provider objects (name, description, roles, url + extras).",
    )

    # Collection-level assets (spec §assets) — keyed by asset ID
    assets: Mutable[Dict[str, StacAssetDefinition]] = Field(
        default_factory=dict,
        description="Static collection-level assets (href required).",
    )

    # Item asset templates (item_assets extension) — keyed by asset ID
    item_assets: Mutable[Dict[str, StacAssetDefinition]] = Field(
        default_factory=dict,
        description="Item asset templates (no href, ≥2 fields each).",
    )
    
    # Datacube definitions
    cube_dimensions: Mutable[Dict[str, DatacubeDimension]] = Field(default_factory=dict)
    cube_variables: Mutable[Dict[str, DatacubeVariable]] = Field(default_factory=dict)
    
    # Navigation & Hierarchy
    navigation_links: Mutable[List[StacLink]] = Field(default_factory=list)
    hierarchy: Mutable[Optional[HierarchyConfig]] = None

    # --- Asset Tracking (Runtime only) ---
    asset_tracking: Mutable[AssetTrackingConfig] = Field(default_factory=AssetTrackingConfig)  # type: ignore[arg-type]

    # --- Aggregations (OGC STAC Extension) ---
    aggregations: Mutable[Optional[AggregationConfig]] = None

    # Geometry Simplification (Query time)
    simplification: Mutable[SimplificationConfig] = Field(default_factory=SimplificationConfig)

    # Write-time STAC schema validation gate.
    #
    # When False (default) the pystac/stac-pydantic schema validation step is
    # skipped on the write path entirely.  The validators fetch remote JSON
    # schemas from stac-extensions.github.io, which is a synchronous network
    # call that blocks the async event loop and starves the DB connection pool
    # under batch ingest.  Validation is already lenient (warnings only), so
    # the only observable effect of the default is that extension-schema
    # warnings are suppressed.  Enable this flag only in low-traffic or
    # offline-capable deployments that have local schema mirrors configured.
    validate_on_write: Mutable[bool] = Field(
        default=False,
        description=(
            "Gate write-time pystac/stac-pydantic schema validation.  "
            "Defaults to False to prevent blocking network I/O on the async "
            "event loop during ingest.  Set to True only when a local schema "
            "cache is in place and the deployment can afford the latency."
        ),
    )
