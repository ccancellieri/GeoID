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

# dynastore/extensions/stac/all_langls.py

import logging
import os
from typing import Annotated, Any, Dict, List, Optional, Set, Tuple, Union
import pystac
from pydantic import BaseModel, Field, model_validator, ConfigDict, AliasChoices

logger = logging.getLogger(__name__)
from dynastore.models.shared_models import LocalizableModelMixin
from dynastore.models.localization import (
    STAC_LANGUAGE_EXTENSION_URI,
    get_language_object,
    localize_dict,
)

try:
    import jsonschema

    PYSTAC_VALIDATION_AVAILABLE = True
except ImportError:
    PYSTAC_VALIDATION_AVAILABLE = False

from dynastore.models.shared_models import Link, Provider, Extent, Internationalized

# --- STAC API DTOs (Standard Compliance) ---
# These models represent the "Wire Format" exposed in OpenAPI.
# They allow both standard single-value strings (localized input)
# AND dictionary values (multilanguage input) via the generic Internationalized alias.

# --- OpenAPI Schema Helpers ---
# Internationalized[T] = Union[Dict[str, T], T] generates ugly anyOf/additionalProperties
# schemas in OpenAPI. These helpers rewrite the JSON schema to show clean STAC types
# while keeping runtime behavior unchanged.

_I18N_STRING = {"type": "string"}
_I18N_STRING_ARRAY = {"type": "array", "items": {"type": "string"}}
_I18N_OBJECT = {"type": "object"}

_CATALOG_I18N_FIELDS: Dict[str, Dict[str, Any]] = {
    "title": _I18N_STRING,
    "description": _I18N_STRING,
    "keywords": _I18N_STRING_ARRAY,
    "license": _I18N_STRING,
    "extra_metadata": _I18N_OBJECT,
}

_COLLECTION_I18N_FIELDS: Dict[str, Dict[str, Any]] = {
    **_CATALOG_I18N_FIELDS,
}


def _clean_internationalized_schema(
    schema: Dict[str, Any],
    field_overrides: Dict[str, Dict[str, Any]],
    examples: List[Dict[str, Any]],
) -> None:
    """Override Internationalized[T] anyOf patterns with clean STAC types in OpenAPI."""
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


_STAC_CATALOG_EXAMPLES = [
    {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "id": "my_catalog",
        "title": "My Geospatial Catalog",
        "description": "A catalog of satellite imagery and geospatial data.",
        "links": [],
    },
    {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "id": "fao_catalog",
        "title": "FAO Data Catalog",
        "description": "Food and Agriculture Organization geospatial data.",
        "links": [
            {
                "rel": "describedby",
                "href": "https://www.fao.org/data",
                "type": "text/html",
                "title": "FAO Data Portal",
            }
        ],
    },
    {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "id": "multilingual_catalog",
        "title": {"en": "My Catalog", "fr": "Mon Catalogue", "es": "Mi Catalogo"},
        "description": {
            "en": "English description",
            "fr": "Description en francais",
        },
        "keywords": {"en": ["satellite", "imagery"], "fr": ["satellite", "imagerie"]},
        "links": [],
    },
]


class STACCatalogRequest(BaseModel):
    """
    Standard STAC Catalog Definition for Creation/Update.
    Fields like title/description accept either:
    1. A string (treated as content for the request's active language)
    2. A dictionary (treated as full multi-language content)
    """

    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
        json_schema_extra=lambda schema: _clean_internationalized_schema(
            schema, _CATALOG_I18N_FIELDS, _STAC_CATALOG_EXAMPLES
        ),
    )

    type: str = Field("Catalog", description="Must be 'Catalog'.")
    stac_version: str = Field(
        "1.1.0", description="The STAC version the Catalog implements."
    )
    stac_extensions: Optional[List[str]] = Field(
        None, description="A list of STAC extension URIs the Catalog implements."
    )

    id: Annotated[str, Field(description="The ID of the catalog.")]

    # Use generic Internationalized type for i18n support
    title: Optional[Internationalized[str]] = Field(
        None,
        description="A short descriptive one-line title. Also accepts a {lang: value} dictionary for multilingual input.",
    )
    description: Internationalized[str] = Field(
        ...,
        description="Detailed multi-line description. Also accepts a {lang: value} dictionary for multilingual input.",
    )

    keywords: Optional[Internationalized[List[str]]] = Field(
        None,
        description="List of keywords describing the catalog. Also accepts a {lang: [values]} dictionary.",
    )
    license: Internationalized[Any] = Field(
        "proprietary",
        description="SPDX License identifier (e.g. 'CC-BY-4.0'). Also accepts a {lang: value} dictionary.",
    )

    links: Optional[List[Link]] = Field(
        default_factory=list,
        description="Custom links (e.g. rel='license', rel='describedby'). Navigation links (self, root, parent) are generated server-side.",
    )

    # Extra metadata container for custom fields
    extra_metadata: Optional[Internationalized[Any]] = Field(
        None,
        description="Additional metadata fields. Also accepts a {lang: value} dictionary.",
    )


_STAC_COLLECTION_EXAMPLES = [
    {
        "type": "Collection",
        "stac_version": "1.1.0",
        "id": "sentinel_2",
        "title": "Sentinel-2 L2A",
        "description": "Sentinel-2 Level 2A surface reflectance data.",
        "license": "CC-BY-4.0",
        "keywords": ["satellite", "sentinel"],
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2015-06-23T00:00:00Z", None]]},
        },
        "providers": [
            {
                "name": "ESA",
                "roles": ["producer"],
                "url": "https://www.esa.int",
            }
        ],
        "links": [
            {
                "rel": "license",
                "href": "https://creativecommons.org/licenses/by/4.0/",
                "type": "text/html",
                "title": "CC-BY-4.0 License",
            }
        ],
    },
    {
        "type": "Collection",
        "stac_version": "1.1.0",
        "id": "fao_agri",
        "title": {"en": "Agricultural Data", "fr": "Donnees Agricoles"},
        "description": {
            "en": "FAO agricultural statistics.",
            "fr": "Statistiques agricoles de la FAO.",
        },
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2000-01-01T00:00:00Z", None]]},
        },
        "links": [],
    },
]


class STACCollectionRequest(BaseModel):
    """
    Standard STAC Collection Definition for Creation/Update.
    Fields like title/description accept either:
    1. A string (treated as content for the request's active language)
    2. A dictionary (treated as full multi-language content)
    """

    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
        json_schema_extra=lambda schema: _clean_internationalized_schema(
            schema, _COLLECTION_I18N_FIELDS, _STAC_COLLECTION_EXAMPLES
        ),
    )

    type: str = Field("Collection", description="Must be 'Collection'.")
    stac_version: str = Field(
        "1.1.0", description="The STAC version the Collection implements."
    )
    stac_extensions: Optional[List[str]] = Field(
        None, description="A list of STAC extensions the Collection implements."
    )

    id: Annotated[str, Field(description="The ID of the collection.")]

    # Use generic Internationalized type for i18n support
    title: Optional[Internationalized[str]] = Field(
        None,
        description="A short descriptive one-line title. Also accepts a {lang: value} dictionary for multilingual input.",
    )
    description: Internationalized[str] = Field(
        ...,
        description="Detailed multi-line description. Also accepts a {lang: value} dictionary for multilingual input.",
    )
    keywords: Optional[Internationalized[List[str]]] = Field(
        None,
        description="List of keywords describing the collection. Also accepts a {lang: [values]} dictionary.",
    )
    license: Internationalized[Any] = Field(
        "proprietary",
        description="SPDX License identifier (e.g. 'CC-BY-4.0'). Also accepts a {lang: value} dictionary.",
    )

    providers: Optional[List[Provider]] = Field(
        None,
        description="A list of providers, which may include all the organizations that captured or processed the data.",
    )
    extent: Extent = Field(
        ..., description="Spatial and temporal extent of the data in the collection."
    )
    summaries: Optional[Dict[str, Any]] = Field(
        None, description="A dictionary of summaries of the data in the collection."
    )

    assets: Optional[Dict[str, Any]] = Field(
        None,
        description="Collection-level assets keyed by asset ID (STAC spec §assets).",
    )
    item_assets: Optional[Dict[str, Any]] = Field(
        None,
        description="Item asset templates keyed by asset ID (STAC Item Assets extension).",
    )

    links: Optional[List[Link]] = Field(
        default_factory=list,
        description="Custom links (e.g. rel='license', rel='describedby'). Navigation links (self, root, parent) are generated server-side.",
    )

    # Extra metadata container
    extra_metadata: Optional[Internationalized[Any]] = Field(
        None,
        description="Additional metadata fields. Also accepts a {lang: value} dictionary.",
    )


_STAC_CATALOG_UPDATE_EXAMPLES = [
    {
        "title": "Updated Catalog Title",
        "description": "Updated description for the catalog.",
    },
    {
        "title": {"en": "Updated Title", "fr": "Titre mis a jour"},
        "keywords": {"en": ["updated", "geospatial"], "fr": ["mis a jour", "geospatial"]},
    },
]


class STACCatalogUpdate(BaseModel):
    """DTO for updating a Catalog. All fields optional."""

    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
        json_schema_extra=lambda schema: _clean_internationalized_schema(
            schema, _CATALOG_I18N_FIELDS, _STAC_CATALOG_UPDATE_EXAMPLES
        ),
    )

    title: Optional[Internationalized[str]] = Field(default=None)
    description: Optional[Internationalized[str]] = Field(default=None)
    keywords: Optional[Internationalized[List[str]]] = Field(default=None)
    license: Optional[Internationalized[Any]] = Field(default=None)
    extra_metadata: Optional[Internationalized[Any]] = Field(default=None)


_STAC_COLLECTION_UPDATE_EXAMPLES = [
    {
        "title": "Updated Collection Title",
        "description": "Updated description.",
        "keywords": ["updated", "satellite"],
    },
    {
        "title": {"en": "Updated Title", "fr": "Titre mis a jour"},
        "extent": {
            "spatial": {"bbox": [[-10, -10, 10, 10]]},
            "temporal": {"interval": [["2020-01-01T00:00:00Z", "2025-12-31T23:59:59Z"]]},
        },
    },
]


class STACCollectionUpdate(BaseModel):
    """DTO for updating a Collection. All fields optional."""

    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
        json_schema_extra=lambda schema: _clean_internationalized_schema(
            schema, _COLLECTION_I18N_FIELDS, _STAC_COLLECTION_UPDATE_EXAMPLES
        ),
    )

    title: Optional[Internationalized[str]] = Field(default=None)
    description: Optional[Internationalized[str]] = Field(default=None)
    keywords: Optional[Internationalized[List[str]]] = Field(default=None)
    license: Optional[Internationalized[Any]] = Field(default=None)
    providers: Optional[List[Provider]] = Field(default=None)
    extent: Optional[Extent] = Field(default=None)
    summaries: Optional[Dict[str, Any]] = Field(default=None)
    assets: Optional[Dict[str, Any]] = Field(default=None)
    item_assets: Optional[Dict[str, Any]] = Field(default=None)
    extra_metadata: Optional[Internationalized[Any]] = Field(default=None)


# --- Containers ---


class Collections(BaseModel):
    """A container for a list of STAC Collection objects."""

    collections: List[
        Dict[str, Any]
    ]  # We use Dict to allow the flexible STAC output structure
    links: List[Link]


from dynastore.models.ogc import Feature


_STAC_ITEM_EXAMPLES = [
    {
        "type": "Feature",
        "stac_version": "1.1.0",
        "id": "item_001",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
        },
        "bbox": [0, 0, 1, 1],
        "properties": {"datetime": "2024-01-15T00:00:00Z"},
        "links": [],
        "assets": {
            "data": {
                "href": "https://example.com/data.tif",
                "type": "image/tiff; application=geotiff",
                "roles": ["data"],
            }
        },
        "collection": "my_collection",
    },
    {
        "type": "Feature",
        "stac_version": "1.1.0",
        "id": "item_002",
        "geometry": {
            "type": "Point",
            "coordinates": [12.49, 41.89],
        },
        "bbox": [12.49, 41.89, 12.49, 41.89],
        "properties": {
            "datetime": None,
            "start_datetime": "2024-01-01T00:00:00Z",
            "end_datetime": "2024-12-31T23:59:59Z",
        },
        "links": [],
        "assets": {
            "thumbnail": {
                "href": "https://example.com/thumb.png",
                "type": "image/png",
                "roles": ["thumbnail"],
            }
        },
    },
]


class STACItem(Feature):
    """
    A Pydantic model that validates a raw dictionary as a STAC Item.
    Inherits fields (id, type, geometry, properties, links) from Feature.
    """

    model_config = ConfigDict(
        extra="allow",
        arbitrary_types_allowed=True,
        json_schema_extra={"examples": _STAC_ITEM_EXAMPLES},  # type: ignore[arg-type]
    )

    # Overrides/Extensions
    stac_version: str = Field(
        "1.1.0", description="The STAC version the Item implements."
    )
    stac_extensions: Optional[List[str]] = Field(
        None, description="A list of STAC extensions the Item implements."
    )

    # Feature's fields refined for STAC requirements
    id: str = Field(..., description="Provider-specific identifier for the Item.")  # type: ignore[reportGeneralTypeIssues]
    bbox: List[float] = Field(..., description="Bounding box of the asset.")
    collection: Optional[str] = Field(
        None, description="The ID of the collection this Item belongs to."
    )

    # Note: 'properties' and 'geometry' are inherited from Feature but STAC enforces specific semantics.
    # We can override if stricter validation is needed, but Base Feature is usually sufficient.

    @model_validator(mode="before")
    @classmethod
    def validate_as_stac_item(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if not values:
            raise ValueError("Request body cannot be empty.")

        try:
            # 1. Prepare a flattened version for strict STAC validation
            # Standard pystac expects strings for title/description/etc.
            # We resolve these to 'en' (or first available) just for validation.
            # This doesn't affect the actual stored data (values).
            validation_dict = values.copy()

            # Localize properties
            if "properties" in validation_dict and isinstance(
                validation_dict["properties"], dict
            ):
                # Normalization: handle booleans like 'updated' which might be expected as strings
                # This is done before localization to ensure the 'en' version gets the string representation
                normalized_props = {}
                for k, v in validation_dict["properties"].items():
                    if isinstance(v, bool):
                        normalized_props[k] = str(v).lower()
                    else:
                        normalized_props[k] = v

                loc_props, _ = localize_dict(normalized_props, "en")
                validation_dict["properties"] = loc_props

            # Localize assets
            if "assets" in validation_dict and isinstance(
                validation_dict["assets"], dict
            ):
                loc_assets = {}
                for k, v in validation_dict["assets"].items():
                    if isinstance(v, dict):
                        # Some asset fields like 'title' or 'description' might be localized
                        loc_v, _ = localize_dict(v, "en")
                        loc_assets[k] = loc_v
                    else:
                        loc_assets[k] = v
                validation_dict["assets"] = loc_assets

            # STAC 1.1.0 item-spec requires a rel:"collection" link when the
            # `collection` property is set. Clients typically omit this link on
            # create (the server injects it later in stac_generator), so inject
            # a synthetic one into the validation copy only.
            if validation_dict.get("collection"):
                links = list(validation_dict.get("links") or [])
                if not any(
                    isinstance(link, dict) and link.get("rel") == "collection"
                    for link in links
                ):
                    links.append(
                        {
                            "rel": "collection",
                            "href": f"./collections/{validation_dict['collection']}",
                            "type": "application/json",
                        }
                    )
                    validation_dict["links"] = links

            # 2. Parse as pystac Item
            stac_item = pystac.Item.from_dict(validation_dict)

            # Always validate STAC compliance
            if PYSTAC_VALIDATION_AVAILABLE:
                try:
                    stac_item.validate()
                except Exception as e:
                    # Log as warning if it's an extension validation failure that we want to be lenient about.
                    # The datacube extension often fails due to strict type checks in its schema versioning.
                    err_str = str(e)
                    if stac_item.stac_extensions:
                        logger.warning(f"STAC Extension validation warning: {err_str}")
                    else:
                        raise e

                # Log extension validation success
                if stac_item.stac_extensions:
                    logger.debug(
                        f"Validated STAC extensions: {stac_item.stac_extensions}"
                    )
        except Exception as ve:
            # If validation fails, we log it but don't block.
            # This makes the validation lenient as requested.
            logger.warning(
                f"STAC Item validation warning for {values.get('id', 'unknown')}: {str(ve)}"
            )
            # we don't raise here to allow lenient loading of extensions
            # and non-standard fields like boolean 'updated'

        return values

    def to_pystac(self) -> pystac.Item:
        return pystac.Item.from_dict(self.model_dump(by_alias=True, exclude_unset=True))


from pydantic import Discriminator, Tag  # noqa: E402


def _discriminate_stac_type(v: Any) -> str:
    if isinstance(v, dict):
        return v.get("type", "Feature")
    return getattr(v, "type", "Feature")


class STACItemCollection(BaseModel):
    """Input model for bulk creation of STAC Items (ItemCollection / FeatureCollection).

    Mirrors the STAC ItemCollection spec:
    https://github.com/radiantearth/stac-api-spec/blob/main/fragments/itemcollection/README.md
    """

    model_config = ConfigDict(extra="allow")

    type: Annotated[str, Field(...)] = "FeatureCollection"
    features: List[STACItem] = Field(default_factory=list)
    links: Optional[List[Link]] = Field(default=None)
    stac_version: Optional[str] = Field(default=None)
    stac_extensions: Optional[List[str]] = Field(default=None)
    numberReturned: Optional[int] = Field(default=None)
    numberMatched: Optional[int] = Field(default=None)


STACItemOrItemCollection = Annotated[
    Union[
        Annotated[STACItem, Tag("Feature")],
        Annotated[STACItemCollection, Tag("FeatureCollection")],
    ],
    Discriminator(_discriminate_stac_type),
]


class STACItemResponse(BaseModel):
    """Response model for STAC Item (open dict)."""

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "type": "Feature",
                    "stac_version": "1.1.0",
                    "id": "item_001",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                    },
                    "bbox": [0, 0, 1, 1],
                    "properties": {"datetime": "2024-01-15T00:00:00Z"},
                    "links": [
                        {
                            "rel": "self",
                            "href": "https://example.com/stac/catalogs/cat1/collections/col1/items/item_001",
                            "type": "application/geo+json",
                        },
                        {
                            "rel": "collection",
                            "href": "https://example.com/stac/catalogs/cat1/collections/col1",
                            "type": "application/json",
                        },
                    ],
                    "assets": {
                        "data": {
                            "href": "https://example.com/data.tif",
                            "type": "image/tiff; application=geotiff",
                            "roles": ["data"],
                        }
                    },
                    "collection": "col1",
                }
            ]
        },
    )


class STACItemCollectionResponse(BaseModel):
    """Response model for STAC ItemCollection (open dict)."""

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "stac_version": "1.1.0",
                            "id": "item_001",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [12.49, 41.89],
                            },
                            "bbox": [12.49, 41.89, 12.49, 41.89],
                            "properties": {"datetime": "2024-01-15T00:00:00Z"},
                            "links": [],
                            "assets": {},
                            "collection": "col1",
                        }
                    ],
                    "links": [
                        {
                            "rel": "self",
                            "href": "https://example.com/stac/catalogs/cat1/collections/col1/items",
                            "type": "application/geo+json",
                        }
                    ],
                    "numberMatched": 1,
                    "numberReturned": 1,
                }
            ]
        },
    )


def inject_stac_language_fields(
    data: Dict[str, Any], available_languages: Set[str], lang: str
) -> Dict[str, Any]:
    """
    Injects STAC Language Extension fields ('language', 'languages') into a dictionary.
    """
    if not available_languages:
        return data

    if lang != "*":
        # 1. The 'language' field describes the current representation's language
        data["language"] = get_language_object(lang).model_dump(exclude_none=True)

        # 2. The 'languages' field lists OTHER available languages
        other_langs = available_languages - {lang}
        # Fallback handling: if 'en-US' was requested but only 'en' exists, remove 'en' from others
        base_lang = lang.split("-")[0]
        if base_lang in other_langs:
            other_langs.remove(base_lang)

        if other_langs:
            data["languages"] = [
                get_language_object(l).model_dump(exclude_none=True)
                for l in sorted(other_langs)
            ]

    # Also add the extension URI if it's a STAC object with stac_extensions
    if "stac_extensions" not in data:
        data["stac_extensions"] = []

    if isinstance(data["stac_extensions"], list):
        if STAC_LANGUAGE_EXTENSION_URI not in data["stac_extensions"]:
            data["stac_extensions"].append(STAC_LANGUAGE_EXTENSION_URI)

    return data


def stac_localize(
    model: LocalizableModelMixin, lang: str
) -> Tuple[Dict[str, Any], Set[str]]:
    """Localizes a model and injects STAC language fields."""
    # STAC Language Extension treats documents as mono-lingual in the requested language.
    # If lang is '*', we return all available translations as a dictionary.
    # Otherwise, we return plain strings for the requested language.
    include_keys = lang == "*"
    localized_data, available_langs = model.localize(
        lang, include_language_keys=include_keys
    )

    if available_langs:
        localized_data = inject_stac_language_fields(
            localized_data, available_langs, lang
        )
    return localized_data, available_langs
