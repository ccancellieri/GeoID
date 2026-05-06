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

# dynastore/extensions/features/ogc_models.py

from typing import List, Optional, Dict, Any, Literal, Union
from pydantic import BaseModel, Field, ConfigDict, AliasChoices
from dynastore.models.shared_models import Link, Extent, Provider, Internationalized

# --- OGC API Common / Features DTOs ---

# --- OpenAPI Schema Helpers ---

_I18N_STRING = {"type": "string"}
_I18N_STRING_ARRAY = {"type": "array", "items": {"type": "string"}}
_I18N_OBJECT = {"type": "object"}

_OGC_I18N_FIELDS: Dict[str, Dict[str, Any]] = {
    "title": _I18N_STRING,
    "description": _I18N_STRING,
    "keywords": _I18N_STRING_ARRAY,
    "license": _I18N_STRING,
    "extra_metadata": _I18N_OBJECT,
}


def _clean_ogc_schema(
    schema: Dict[str, Any],
    field_overrides: Dict[str, Dict[str, Any]],
    examples: List[Dict[str, Any]],
) -> None:
    """Override Internationalized[T] anyOf patterns with clean OGC types in OpenAPI."""
    props = schema.get("properties", {})
    for field_name, clean_type in field_overrides.items():
        if field_name in props:
            desc = props[field_name].get("description", "")
            default = props[field_name].get("default")
            new_prop = {**clean_type, "description": desc}
            if default is not None:
                new_prop["default"] = default
            props[field_name] = new_prop
    schema["examples"] = examples


_OGC_CATALOG_EXAMPLES = [
    {
        "id": "my_catalog",
        "title": "My Geospatial Catalog",
        "description": "A catalog of satellite imagery and geospatial data.",
        "links": [],
    },
    {
        "id": "multilingual_catalog",
        "title": {"en": "My Catalog", "fr": "Mon Catalogue"},
        "description": {"en": "English description", "fr": "Description en francais"},
        "links": [],
    },
]

_OGC_COLLECTION_EXAMPLES = [
    {
        "id": "sentinel_2",
        "title": "Sentinel-2 L2A",
        "description": "Sentinel-2 Level 2A surface reflectance data.",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2015-06-23T00:00:00Z", None]]},
        },
        "providers": [{"name": "ESA", "roles": ["producer"], "url": "https://www.esa.int"}],
    },
]


# Canonical Conformance — single source in ogc_common_models
from dynastore.extensions.tools.ogc_common_models import Conformance


class CatalogDefinition(BaseModel):
    """
    Request body for creating/updating a Catalog via OGC API.
    Fields like title/description accept either a string or a {lang: value} dictionary for multilingual input.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
        from_attributes=True,
        json_schema_extra=lambda schema: _clean_ogc_schema(
            schema, _OGC_I18N_FIELDS, _OGC_CATALOG_EXAMPLES
        ),
    )

    id: Optional[str] = Field(default=None, description="Unique identifier for the catalog.")
    title: Optional[Internationalized[str]] = Field(
        default=None, description="Title of the catalog. Also accepts a {lang: value} dictionary."
    )
    description: Optional[Internationalized[str]] = Field(
        default=None, description="Description of the catalog. Also accepts a {lang: value} dictionary."
    )
    keywords: Optional[Internationalized[List[str]]] = Field(
        default=None, description="Keywords. Also accepts a {lang: [values]} dictionary."
    )
    license: Optional[Internationalized[Any]] = Field(
        default=None, description="SPDX License identifier. Also accepts a {lang: value} dictionary."
    )
    links: Optional[List[Link]] = Field(
        default_factory=list,
        description="Custom links. Navigation links are generated server-side.",
    )

    # Extra metadata
    extra_metadata: Optional[Internationalized[Any]] = Field(
        default=None,
        description="Additional metadata fields. Also accepts a {lang: value} dictionary.",
    )


class CollectionDefinition(BaseModel):
    """
    Request body for creating/updating a Collection via OGC API.
    Fields like title/description accept either a string or a {lang: value} dictionary for multilingual input.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
        from_attributes=True,
        json_schema_extra=lambda schema: _clean_ogc_schema(
            schema, _OGC_I18N_FIELDS, _OGC_COLLECTION_EXAMPLES
        ),
    )

    id: Optional[str] = Field(default=None, description="Unique identifier for the collection.")
    title: Optional[Internationalized[str]] = Field(
        default=None, description="Title of the collection. Also accepts a {lang: value} dictionary."
    )
    description: Optional[Internationalized[str]] = Field(
        default=None, description="Description of the collection. Also accepts a {lang: value} dictionary."
    )
    keywords: Optional[Internationalized[List[str]]] = Field(
        default=None, description="Keywords. Also accepts a {lang: [values]} dictionary."
    )
    license: Optional[Internationalized[Any]] = Field(
        default=None, description="SPDX License identifier. Also accepts a {lang: value} dictionary."
    )

    extent: Optional[Extent] = Field(default=None, description="Spatial and temporal extent.")
    providers: Optional[List[Provider]] = Field(default=None, description="Data providers.")
    summaries: Optional[Dict[str, Any]] = Field(default=None, description="Data summaries.")
    extra_metadata: Optional[Internationalized[Any]] = Field(
        default=None, description="Additional metadata fields. Also accepts a {lang: value} dictionary."
    )


# Canonical LandingPage — single source in ogc_common_models
from dynastore.extensions.tools.ogc_common_models import LandingPage


class OGCCollection(BaseModel):
    """Response model for a Collection."""

    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "id": "sentinel_2",
                    "title": "Sentinel-2 L2A",
                    "description": "Sentinel-2 Level 2A surface reflectance data.",
                    "links": [
                        {"rel": "self", "href": "https://example.com/collections/sentinel_2", "type": "application/json"},
                        {"rel": "items", "href": "https://example.com/collections/sentinel_2/items", "type": "application/geo+json"},
                    ],
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [["2015-06-23T00:00:00Z", None]]},
                    },
                    "itemType": "feature",
                    "crs": ["http://www.opengis.net/def/crs/OGC/1.3/CRS84"],
                }
            ]
        },
    )

    id: str = Field(..., description="Collection identifier")
    title: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    links: List[Link] = []
    extent: Optional[Extent] = Field(default=None)
    itemType: str = "feature"
    crs: List[str] = ["http://www.opengis.net/def/crs/OGC/1.3/CRS84"]


class Collections(BaseModel):
    links: List[Link]
    collections: List[OGCCollection]
    crs: Optional[List[str]] = None


class Catalogs(BaseModel):
    """Response container for list of catalogs (not strictly OGC Features core but common extension)."""

    links: List[Link]
    catalogs: List[
        CatalogDefinition
    ]  # Reusing definition for list output might need checking fields


class Queryables(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    type: str = "object"
    title: str
    properties: Dict[str, Any]
    required_fields: Optional[List[str]] = Field(None, alias="required")
    schema_: str = Field("http://json-schema.org/draft/2019-09/schema", alias="$schema")
    link: Optional[str] = Field(None, alias="$id")


from dynastore.models.ogc import Feature, FeatureCollection, GeoJSONGeometry


class FeatureDefinition(Feature):
    """Input model for creating a feature."""

    pass


class FeatureCollectionDefinition(FeatureCollection):
    """Input model for bulk creation."""

    features: List[FeatureDefinition] = Field(default_factory=list)


from pydantic import Discriminator, Tag
from typing import Annotated


def _discriminate_feature_type(v: Any) -> str:
    if isinstance(v, dict):
        return v.get("type", "Feature")
    return getattr(v, "type", "Feature")


FeatureOrFeatureCollection = Annotated[
    Union[
        Annotated[FeatureDefinition, Tag("Feature")],
        Annotated[FeatureCollectionDefinition, Tag("FeatureCollection")],
    ],
    Discriminator(_discriminate_feature_type),
]


# Re-exported from the shared module so existing importers do not break.
from dynastore.extensions.ogc_models_shared import (  # noqa: F401
    BulkCreationResponse,
    IngestionReport,
    SidecarRejection,
)
