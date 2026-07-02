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

import logging
from enum import Enum
from datetime import datetime
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
)
from pydantic import (
    BaseModel,
    Field,
    HttpUrl,
    ConfigDict,
    field_validator,
    model_serializer,
)
from dynastore.tools.db import validate_sql_identifier
from dynastore.models.localization import (
    LocalizedText,
    LocalizedLicense,
    LocalizedKeywords,
    LocalizedExtraMetadata,
    Language,
    LocalizableModelMixin,
)

logger = logging.getLogger(__name__)


def _clean_localized_text_schema(
    schema: Dict[str, Any],
    field_names: List[str],
    examples: List[Dict[str, Any]],
) -> None:
    """Override LocalizedText object patterns with plain string type in OpenAPI."""
    props = schema.get("properties", {})
    for field_name in field_names:
        if field_name in props:
            desc = props[field_name].get("description", "")
            default = props[field_name].get("default")
            new_prop: Dict[str, Any] = {"type": "string", "description": desc}
            if default is not None:
                new_prop["default"] = default
            props[field_name] = new_prop
    schema["examples"] = examples


class Link(BaseModel, LocalizableModelMixin):
    """Represents a generic link object, compliant with OGC/STAC standards."""

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra=lambda schema: _clean_localized_text_schema(
            schema,
            ["title"],
            [
                {
                    "rel": "self",
                    "href": "https://example.com/stac/catalogs/cat1",
                    "type": "application/json",
                    "title": "This catalog",
                },
                {
                    "rel": "license",
                    "href": "https://creativecommons.org/licenses/by/4.0/",
                    "type": "text/html",
                    "title": "CC-BY-4.0 License",
                },
            ],
        ),
    )

    href: str
    rel: str
    type: Optional[str] = Field(default=None, description="Media type of the resource.")
    title: Optional[LocalizedText] = Field(
        default=None, description="Human-readable title of the link destination."
    )
    hreflang: Optional[str] = Field(
        default=None,
        description="The language of the resource at the link destination (RFC 5646).",
    )

    @field_validator("title", mode="before")
    @classmethod
    def wrap_localized_text(cls, v: Any) -> Any:
        if isinstance(v, str):
            return {Language.EN.value: v}
        return v


# --- Shared Constants ---
SYSTEM_CATALOG_ID = "_system_"
SYSTEM_CATALOG_TITLE = "System Catalog"
SYSTEM_SCHEMA = "catalog"  # Physical schema for system-level tables
SYSTEM_LOGS_TABLE = "system_logs"

# --- Reusable Internationalization Types ---

# Generic type variable for the content (e.g., str, List[str], etc.)
ContentT = TypeVar("ContentT")

# Type alias for a field that can be either:
# 1. A localized value (ContentT) for the current context's language.
# 2. An internationalized dictionary (Dict[str, ContentT]) mapping language codes to values.
# usage: title: Optional[Internationalized[str]] = None
Internationalized = Union[Dict[str, ContentT], ContentT]


# --- OGC / STAC Aligned Primitives ---





class Provider(BaseModel, LocalizableModelMixin):
    model_config = ConfigDict(
        json_schema_extra=lambda schema: _clean_localized_text_schema(
            schema,
            ["name", "description"],
            [
                {
                    "name": "European Space Agency",
                    "roles": ["producer"],
                    "url": "https://www.esa.int",
                },
                {
                    "name": "FAO",
                    "description": "Food and Agriculture Organization",
                    "roles": ["producer", "host"],
                    "url": "https://www.fao.org",
                },
            ],
        ),
    )

    name: LocalizedText = Field(description="Name of the organization or individual.")
    description: Optional[LocalizedText] = Field(
        default=None, description="Description of the provider."
    )
    roles: Optional[List[str]] = Field(
        default=None, description="Roles of the provider (e.g. producer, host, licensor)."
    )
    url: Optional[HttpUrl] = Field(
        default=None, description="URL of the provider."
    )

    @field_validator("name", "description", mode="before")
    @classmethod
    def wrap_localized_text(cls, v: Any) -> Any:
        if isinstance(v, str):
            return {Language.EN.value: v}
        return v


class TemporalExtent(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    interval: List[List[Optional[datetime]]]

    @field_validator("interval")
    @classmethod
    def validate_interval(cls, v: List) -> List:
        """
        Ensures the temporal interval is never an empty list and that start
        times are before end times.
        """
        if not v:
            return [[None, None]]

        for item in v:
            start, end = item
            if start and end and start > end:
                raise ValueError(
                    f"Invalid temporal interval: start time '{start}' is after end time '{end}'."
                )
        return v


class SpatialExtent(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    bbox: List[List[float]] = Field(default=[[0.0, 0.0, 0.0, 0.0]])

    @field_validator("bbox")
    @classmethod
    def validate_bbox(cls, v: List) -> List:
        """
        Validates that each bounding box is logically and geographically correct.
        """
        for box in v:
            minx, miny, maxx, maxy = box
            if minx > maxx:
                raise ValueError(
                    f"Invalid bounding box: minx ({minx}) cannot be greater than maxx ({maxx})."
                )
            if miny > maxy:
                raise ValueError(
                    f"Invalid bounding box: miny ({miny}) cannot be greater than maxy ({maxy})."
                )
            if not (-180 <= minx <= 180 and -180 <= maxx <= 180):
                raise ValueError(
                    f"Invalid longitude in bounding box: values must be between -180 and 180."
                )
            if not (-90 <= miny <= 90 and -90 <= maxy <= 90):
                raise ValueError(
                    f"Invalid latitude in bounding box: values must be between -90 and 90."
                )
        return v


class Extent(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "examples": [
                {
                    "spatial": {"bbox": [[-180, -90, 180, 90]]},
                    "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
                },
            ]
        },
    )
    spatial: SpatialExtent
    temporal: TemporalExtent

    @field_validator("spatial", "temporal", mode="before")
    @classmethod
    def ensure_model_instances(cls, v: Any, info) -> Any:
        """
        Ensures spatial and temporal are proper model instances.
        Handles cases where they might be dicts from JSON storage.
        """
        field_name = info.field_name
        if isinstance(v, dict):
            if field_name == "spatial":
                return SpatialExtent(**v)
            elif field_name == "temporal":
                return TemporalExtent(**v)
        return v



class LicenseInfo(BaseModel, LocalizableModelMixin):
    license_id: str = Field(..., description="SPDX License Identifier")
    is_osi_compliant: bool = Field(
        False, description="Whether the license is OSI compliant"
    )
    localized_content: Optional[LocalizedLicense] = Field(
        None, description="Localized license details"
    )

    def merge_updates(
        self, updates: Union[str, Dict[str, Any]], lang: str
    ) -> "LicenseInfo":
        """Merges updates into this LicenseInfo, returning a new instance."""
        merged_data = self.model_dump(exclude_unset=True)

        if isinstance(updates, str):
            merged_data["license_id"] = updates
            return LicenseInfo.model_validate(merged_data)

        if isinstance(updates, dict):
            for key in ["license_id", "is_osi_compliant"]:
                if key in updates:
                    merged_data[key] = updates[key]

            if (
                "localized_content" in updates
                and updates["localized_content"] is not None
            ):
                current_localized_dto = self.localized_content or LocalizedLicense()
                updated_localized_dto = current_localized_dto.merge_updates(
                    updates["localized_content"], lang
                )
                merged_data["localized_content"] = updated_localized_dto

            return LicenseInfo.model_validate(merged_data)

        return self.model_copy(deep=True)

    @classmethod
    def delocalize_input(cls, value: Any, lang: str) -> Dict[str, Any]:
        """Wraps a simple SPDX string ID into a valid LicenseInfo structure."""
        if isinstance(value, str):
            return {"license_id": value}
        # If it's a dict or other type, return as is for Pydantic to validate.
        return value


class LocalizedFieldsBase(BaseModel, LocalizableModelMixin):
    """
    Model for handling localized metadata updates.
    """

    title: Optional[LocalizedText] = None
    description: Optional[LocalizedText] = None
    keywords: Optional[LocalizedKeywords] = None
    license: Optional[LicenseInfo] = None
    extra_metadata: Optional[LocalizedExtraMetadata] = None

    @field_validator("title", "description", mode="before")
    @classmethod
    def wrap_localized_text(cls, v: Any) -> Any:
        """Wraps a string in a default 'en' dict if it's not already a dict."""
        if isinstance(v, str):
            return {Language.EN.value: v}
        return v

    @field_validator("keywords", mode="before")
    @classmethod
    def wrap_keywords(cls, v: Any) -> Any:
        """Wraps a list of strings in a default 'en' dict if it's not already a dict."""
        if isinstance(v, list) and v and all(isinstance(i, str) for i in v):
            return {Language.EN.value: v}
        return v

    @field_validator("license", mode="before")
    @classmethod
    def wrap_license(cls, v: Any) -> Any:
        """Wraps a string license in a LicenseInfo object if it's not already a dict/model."""
        if isinstance(v, str):
            return {"license_id": v}
        return v

    @field_validator("extra_metadata", mode="before")
    @classmethod
    def wrap_extra_metadata(cls, v: Any) -> Any:
        """Wraps a dict in a LocalizedExtraMetadata object if it's a flat dict."""
        if isinstance(v, dict):
            from dynastore.models.localization import _LANGUAGE_METADATA

            # If keys match known languages, assume it's already localized
            if any(k in _LANGUAGE_METADATA for k in v):
                return v
            # Otherwise treat as default language content
            return {Language.EN.value: v}
        return v


# --- Core Information Models (Catalog & Collection) ---
class BaseMetadata(LocalizedFieldsBase):
    model_config = ConfigDict(
        extra="allow", from_attributes=True, populate_by_name=True
    )

    id: str = Field(..., description="A unique logical identifier.")
    external_id: Optional[str] = Field(
        default=None,
        description=(
            "The renamable public label for this resource. "
            "Populated by the service layer after create; not user-supplied on the model."
        ),
    )
    # ``physical_schema`` (the per-tenant PostgreSQL schema) is deliberately NOT a
    # field on this model. It is a storage-driver / provisioning detail that must be
    # resolved from the authoritative ``catalog.catalogs`` registry via
    # ``CatalogsProtocol.resolve_physical_schema`` — never carried on the public
    # metadata model. Carrying it here leaked an internal identifier into API output
    # and (because it was ``exclude=True``) was silently dropped whenever the model
    # crossed the distributed cache, breaking schema resolution on slim read-only
    # services. ``get_internal_columns()`` still lists it so any stray copy that
    # arrives as an ``extra`` field is filtered out of external metadata.
    # See docs/architecture/tenant-storage-workspace.md.

    @field_validator("id")
    @classmethod
    def validate_id_format(cls, v: str) -> str:
        """
        Ensures the ID is a valid, unquoted, and non-reserved PostgreSQL identifier.
        """
        # Step 1: Basic SQL identifier validation (format, length, SQL keywords)
        validated_code = validate_sql_identifier(v)

        return validated_code

    @model_serializer(mode="wrap")
    def _serialize_public_id(self, handler: Any) -> Dict[str, Any]:
        """Emit the renamable public label as the ``"id"`` field on the wire.

        Rules:
        - If ``external_id`` is set, replace ``id`` with its value so every
          caller (HTTP JSON response, localize(), model_dump()) sees the public
          label, not the opaque internal token.
        - The raw ``external_id`` key is never emitted (it must not appear in
          API output or intermediate dicts).
        - The in-memory ``.id`` attribute is not modified — all in-process code
          that reads ``.id`` directly continues to get the immutable internal
          token.

        Internal callers (service layer, pipeline stages) that call
        ``model_dump()`` / ``localize()`` receive ``id = external_id`` in the
        resulting dict.  Pipeline stages that round-trip through
        ``Catalog.model_validate(data)`` will produce a model whose ``.id``
        equals the external label — this is acceptable because those models are
        used only for output, not for further DB operations (the service layer
        resolves the internal id independently from the path-param).
        """
        data: Dict[str, Any] = handler(self)
        if self.external_id is not None:
            data["id"] = self.external_id
        data.pop("external_id", None)
        # Strip internal identifiers that ``extra="allow"`` absorbs from a DB row
        # on create/replace/update (the same leak class the ``physical_schema``
        # note above describes). The immutable internal id stays on ``.id`` in
        # memory for in-process callers and the ``/catalog`` management protocol;
        # it must never appear on a standard OGC/STAC surface, where the public
        # ``id`` is the renamable external label only.
        for _internal in ("catalog_id", "collection_id", "physical_schema", "connection_info"):
            data.pop(_internal, None)
        return data

    @classmethod
    def get_internal_columns(cls) -> Set[str]:
        """
        Returns a set of field names that are internal to DynaStore and should 
        not be exposed in external metadata (e.g. STAC extra_fields).
        """
        # Common fields that are implementation details or already top-level in STAC
        return {
            "physical_schema",
            "connection_info",
            "type",
            "provisioning_status",
            "extra_metadata",
        }


class Catalog(BaseMetadata):
    """
    Represents a top-level container for data.
    Maps to a logical concept, backed by a physical schema.
    """

    type: str = "Catalog"
    stac_version: Optional[str] = Field(None, description="The STAC version the Catalog implements.")
    stac_extensions: Optional[List[str]] = Field(None, description="A list of extension identifiers the Catalog implements.")
    conformsTo: Optional[List[str]] = Field(
        None, description="A list of conformance classes implemented by this catalog."
    )
    links: Optional[List[Link]] = Field(default_factory=list)
    assets: Optional[Dict[str, Any]] = Field(None, description="Dictionary of asset objects that can be downloaded.")
    provisioning_status: str = Field(
        "ready", description="Provisioning status: provisioning | ready | failed"
    )
    deleted_at: Optional[datetime] = Field(
        None,
        description=(
            "Soft-deletion timestamp. Non-null when the catalog has been soft-deleted "
            "but not yet hard-deleted or reclaimed. Included in GET responses so callers "
            "can observe the deleted lifecycle state without triggering 404."
        ),
    )


class CatalogUpdate(LocalizedFieldsBase):
    """
    Model for updating a catalog (code/id comes from URL path).
    """

    model_config = ConfigDict(
        extra="allow", from_attributes=True, populate_by_name=True
    )
    stac_extensions: Optional[List[str]] = Field(None)
    conformsTo: Optional[List[str]] = Field(None)
    links: Optional[List[Link]] = Field(default_factory=list)
    assets: Optional[Dict[str, Any]] = Field(None)


class Collection(BaseMetadata):
    """
    Represents a logical grouping of related geospatial items (features).
    """

    type: str = "Collection"
    stac_version: Optional[str] = Field(None, description="The STAC version the Collection implements.")
    stac_extensions: Optional[List[str]] = Field(None, description="A list of extension identifiers the Collection implements.")
    providers: Optional[List[Provider]] = Field(None)
    extent: Optional[Extent] = Field(None)
    summaries: Optional[Dict[str, Any]] = Field(None)
    links: Optional[List[Link]] = Field(default_factory=list)
    assets: Optional[Dict[str, Any]] = Field(None, description="Dictionary of asset objects that can be downloaded.")
    item_assets: Optional[Dict[str, Any]] = Field(None, description="A dictionary of assets that can be found in member Items.")

    # Reserved names validation removed as per new architecture


class CollectionUpdate(LocalizedFieldsBase):
    """
    Model for updating a collection (code/id comes from URL path).
    """

    model_config = ConfigDict(
        extra="allow", from_attributes=True, populate_by_name=True
    )
    stac_extensions: Optional[List[str]] = Field(None)
    providers: Optional[List[Provider]] = Field(None)
    extent: Optional[Extent] = Field(None)
    summaries: Optional[Dict[str, Any]] = Field(None)
    assets: Optional[Dict[str, Any]] = Field(None)
    item_assets: Optional[Dict[str, Any]] = Field(None)


# --- OGC API Common Models (e.g., for Filtering/CQL) ---


class FunctionDescription(BaseModel):
    """Describes a single supported CQL2 function."""

    name: str = Field(..., description="The name of the function.")
    returns: List[str] = Field(..., description="The return type(s) of the function.")
    arguments: List[Dict[str, List[str]]] = Field(
        ..., description="The arguments the function accepts."
    )


class FunctionsResponse(BaseModel):
    """A response model for the /functions endpoint."""

    functions: List[FunctionDescription]
    links: Optional[List[Link]] = None


class OutputFormatEnum(str, Enum):
    """Supported output formats for feature retrieval."""

    GEOJSON = "geojson"
    JSON = "json"
    CSV = "csv"
    GEOPACKAGE = "gpkg"
    SHAPEFILE = "shp"
    PARQUET = "parquet"
    GEOPARQUET = "geoparquet"
    GML = "gml"


class EventType(str, Enum):
    """A base class for creating extensible, strongly-typed event enums."""

    pass


class AssetReferenceType(str, Enum):
    """
    Base extensible enum for asset reference types.

    Each driver module subclasses this to define its own namespaced values.
    Values are stored as ``VARCHAR`` in the ``asset_references`` table, so
    use ``"module:kind"`` namespacing to avoid collisions across modules.

    Extension pattern
    ~~~~~~~~~~~~~~~~~
    ::

        # In dynastore/modules/duckdb/models.py
        class DuckDbReferenceType(AssetReferenceType):
            TABLE = "duckdb:table"     # A DuckDB table backed by this asset

        # In dynastore/modules/iceberg/models.py
        class IcebergReferenceType(AssetReferenceType):
            TABLE = "iceberg:table"    # An Iceberg table backed by this asset

        # In dynastore/modules/http/models.py
        class HttpReferenceType(AssetReferenceType):
            DOWNLOAD = "http:download" # An HTTP-served file backed by this asset

    Usage
    ~~~~~
    ::

        # Register a non-cascading reference (blocks hard-delete):
        await assets.add_asset_reference(
            asset_id="my_parquet_asset",
            catalog_id="my_catalog",
            ref_type=DuckDbReferenceType.TABLE,
            ref_id="collection_table_name",
            cascade_delete=False,
        )

        # Register an informational reference (does NOT block hard-delete):
        await assets.add_asset_reference(
            asset_id="stations_csv",
            catalog_id="my_catalog",
            ref_type=CoreAssetReferenceType.COLLECTION,
            ref_id="weather_collection",
            cascade_delete=True,
        )
    """

    pass


class CoreAssetReferenceType(AssetReferenceType):
    """
    Built-in reference types for standard catalog/collection relationships.

    ``COLLECTION = "collection"``
        An asset is the data source for a collection.  Used by ingestion tasks
        and the SQL catalog module where the PostgreSQL ``trg_asset_cleanup``
        trigger already handles row-level cascade cleanup.

        Always registered with ``cascade_delete=True`` — this reference is
        informational and **does not block** hard-deletion of the asset.

    ``ITEM = "item"``
        An asset is owned by a specific item.  Used by the STAC-harvest
        virtual-asset path: each virtual asset registered for a harvested
        feature receives an ``ITEM`` back-reference keyed by the feature's
        ``id`` field.

        Always registered with ``cascade_delete=True`` — this reference is
        informational and **does not block** hard-deletion of the asset.
        The ``ITEM_DELETION`` forward cascade soft-deletes these virtual
        assets so they track the item lifecycle symmetrically.

    Example::

        from dynastore.modules.catalog.models import CoreAssetReferenceType

        await asset_service.add_asset_reference(
            asset_id=asset.asset_id,
            catalog_id=catalog_id,
            ref_type=CoreAssetReferenceType.COLLECTION,
            ref_id=collection_id,
            cascade_delete=True,
        )
    """

    COLLECTION = "collection"
    ITEM = "item"


class ItemDataForDB(BaseModel):
    """
    A Pydantic model representing the processed item data ready for database insertion.
    It includes core fields expected by shared_queries.insert_or_update_feature
    and allows for arbitrary extra fields (e.g., spatial indices, extension properties).
    """

    model_config = ConfigDict(extra="allow")

    external_id: str
    attributes: Dict[str, Any]
    wkb_hex_processed: str
    geom_type: str
    geometry_hash: str

    # These are intermediate values used to construct PostGIS geometries
    bbox_coords: Optional[List[float]] = None

    # These are used to construct the PostGIS TSTZRANGE 'validity' column
    valid_from: Optional[datetime] = None
    valid_to: Optional[datetime] = None

    # Ingestion-related metadata
    asset_code: str
    asset_id: Optional[str] = None
