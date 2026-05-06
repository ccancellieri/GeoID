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

# dynastore/extensions/features/ogc_generator.py

import logging

logger = logging.getLogger(__name__)

import uuid
from typing import Dict, Any, Optional, List, Union, cast
from dynastore.tools.geospatial import process_geometry, GeometryProcessingError
from fastapi import Request, HTTPException, status
from datetime import datetime, timezone
from shapely import wkb
from dynastore.models.protocols import ItemsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.db_config.tools import map_pg_to_json_type
from shapely.geometry import shape, mapping

from . import ogc_models
from dynastore.tools.geospatial import calculate_spatial_indices
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.modules.catalog.models import (
    Collection as CoreCollection,
)  # Import CoreCollection for type hinting
from dynastore.models.localization import LocalizedText, Language
from dynastore.extensions.tools.url import get_root_url
from dynastore.models.shared_models import Link
from dynastore.extensions.tools.conformance import get_active_conformance, Conformance
from dynastore.models.protocols import CatalogsProtocol, ItemsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.extensions.tools.url import get_parent_from_url, get_url


def create_landing_page(
    request: Request, language: str = Language.EN.value
) -> ogc_models.LandingPage:
    """Generates the response for the API's landing page."""
    base_url = get_root_url(request)
    links = [
        Link(
            href=f"{base_url}/features/",
            rel="self",
            type="application/json",
            title=cast(Any, "This document"),
        ),
        Link(
            href=f"{base_url}/features/catalogs",
            rel="catalogs",
            type="application/json",
            title=cast(Any, "List of catalogs"),
        ),
        Link(
            href=f"{base_url}/features/conformance",
            rel="conformance",
            type="application/json",
            title=cast(Any, "Conformance classes"),
        ),
        Link(
            href=f"{base_url}/api",
            rel="service-doc",
            type="application/json",
            title=cast(Any, "API documentation"),
        ),
    ]
    return ogc_models.LandingPage(
        title="DynaStore OGC API Features",
        description="OGC API Features (Parts 1-4) with CQL2 filtering, multi-CRS support, queryables, sorting, and full CRUD transactions.",
        links=links,
    )


def _map_validity_to_ogc(properties: Dict[str, Any]) -> None:
    """Map ``validity`` TSTZRANGE to ``start_datetime`` / ``end_datetime``.

    Mutates *properties* in-place: removes the ``validity`` key and adds
    the OGC temporal extent properties if parseable.
    """
    import re as _re

    v = properties.pop("validity", None)
    if v is None:
        return
    try:
        if hasattr(v, "lower"):
            start, end = v.lower, v.upper
            if start and not getattr(start, "is_infinite", lambda: False)():
                properties["start_datetime"] = (
                    start.isoformat() if hasattr(start, "isoformat") else str(start)
                )
            if end and not getattr(end, "is_infinite", lambda: False)():
                properties["end_datetime"] = (
                    end.isoformat() if hasattr(end, "isoformat") else str(end)
                )
        else:
            match = _re.search(r"[\[\(]([^,]*),\s*([^\]\)]*)", str(v))
            if match:
                start, end = match.groups()
                if start and start.strip() and start.strip() != "-infinity":
                    properties["start_datetime"] = start.strip().strip('"')
                if end and end.strip() and end.strip() != "infinity":
                    properties["end_datetime"] = end.strip().strip('"')
    except Exception as e:
        logger.warning(f"Failed to parse validity range {v}: {e}")


def _db_row_to_ogc_feature(
    item: Union[Dict, Any],
    catalog_id: str,
    collection_id: str,
    root_url: str,
    layer_config: Optional[ItemsPostgresqlDriverConfig] = None,
) -> ogc_models.Feature:
    """
    Converts a database row or an already-mapped Feature into an OGC Feature.

    If *item* is not yet a GeoJSON Feature it is mapped via ``ItemsProtocol``
    (which runs the full sidecar pipeline).  The function then applies
    OGC-specific post-processing:
      - validity TSTZRANGE -> ``start_datetime`` / ``end_datetime``
      - safe ``id`` serialisation (never the string ``'None'``)
      - collection/self links
    """
    from geojson_pydantic import Feature as _GeoJSONFeature

    # -- 1. Map raw DB row -> Feature (if not already mapped) -----------------
    if not isinstance(item, _GeoJSONFeature):
        items_mod = get_protocol(ItemsProtocol)
        if items_mod and layer_config:
            item = items_mod.map_row_to_feature(item, layer_config)
        else:
            logger.warning(
                "Cannot map DB row: ItemsProtocol unavailable or no layer_config. "
                "Returning empty feature."
            )
            item = _GeoJSONFeature(type="Feature", geometry=None, properties={})

    item = cast(_GeoJSONFeature, item)
    # -- 2. Extract id (never serialise None as the string 'None') ------------
    feature_id = item.id if item.id is not None else None

    # -- 3. Convert geometry to ogc_models.GeoJSONGeometry --------------------
    geometry_model = None
    if item.geometry is not None:
        geom_dict = (
            item.geometry.model_dump()
            if hasattr(item.geometry, "model_dump")
            else dict(item.geometry)
        )
        from pydantic import TypeAdapter
        geometry_model = TypeAdapter(ogc_models.GeoJSONGeometry).validate_python(geom_dict)

    # -- 3b. Strip STAC-specific output fields (sidecar ran for multilanguage) -
    from dynastore.extensions.stac.stac_items_sidecar import STAC_FEATURES_STRIP

    for stac_key in STAC_FEATURES_STRIP:
        if hasattr(item, stac_key):
            try:
                delattr(item, stac_key)
            except Exception:
                pass

    # -- 4. Properties + OGC-specific validity mapping ------------------------
    properties = dict(item.properties or {})
    # Remove any STAC keys that leaked into properties
    for stac_key in STAC_FEATURES_STRIP:
        properties.pop(stac_key, None)

    _map_validity_to_ogc(properties)

    properties.pop("_total_count", None)

    # -- 5. Build OGC links ---------------------------------------------------
    collection_url = (
        f"{root_url}/features/catalogs/{catalog_id}/collections/{collection_id}"
    )
    links = [
        Link(
            href=f"{collection_url}/items/{feature_id}",
            rel="self",
            type="application/geo+json",
        ),
        Link(href=collection_url, rel="collection", type="application/json"),
    ]

    return ogc_models.Feature(
        type="Feature",
        id=feature_id,
        geometry=geometry_model,
        properties=properties,
        links=links,
    )


async def create_queryables_response(
    request: Request,
    catalog_id: str,
    collection_id: str,
    columns: list,
    language: str = "en",
    driver_fields: Optional[Dict[str, Any]] = None,
) -> ogc_models.Queryables:
    """Generates the queryables response for a collection using Sidecar FieldDefinitions."""
    catalogs = get_protocol(CatalogsProtocol)
    if catalogs is None:
        raise HTTPException(status_code=503, detail="CatalogsProtocol not registered")
    collection: Optional[CoreCollection] = await catalogs.get_collection(
        catalog_id, collection_id
    )
    if not collection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Collection not found"
        )

    queryables_url = get_url(request)

    properties: Dict[str, Any] = {
        "geometry": {
            "type": "object",
            "$ref": "https://geojson.org/schema/Geometry.json",
            "description": "The geometry of the feature.",
        }
    }

    required_list: List[str] = []

    if driver_fields:
        # Non-PG driver supplied rich FieldDefinition objects via get_entity_fields().
        # Use them directly — bypass ItemsProtocol which is PG-specific.
        for field_def in driver_fields.values():
            if not getattr(field_def, "expose", True):
                continue
            final_name = getattr(field_def, "alias", None) or field_def.name
            if final_name in ("geoid", "geom"):
                continue
            title = getattr(field_def, "title", None)
            description = getattr(field_def, "description", None)
            properties[final_name] = {
                "title": str(title) if title else final_name,
                "description": str(description) if description else None,
                "type": map_pg_to_json_type(field_def.data_type),
            }
            if getattr(field_def, "required", False):
                required_list.append(final_name)
    else:
        # PG path: fetch field definitions via ItemsProtocol (ItemService).
        items_svc = get_protocol(ItemsProtocol)
        if items_svc:
            all_fields = await items_svc.get_collection_fields(catalog_id, collection_id)
            for field_def in all_fields.values():
                if not field_def.expose:
                    continue
                # Use alias as the property key in Queryables (matches Feature output)
                final_name = field_def.alias or field_def.name
                properties[final_name] = {
                    "title": str(field_def.title) if field_def.title else final_name,
                    "description": str(field_def.description) if field_def.description else None,
                    "type": map_pg_to_json_type(field_def.data_type),
                }
                if getattr(field_def, "required", False):
                    required_list.append(final_name)
        else:
            # Final fallback: column name list only (no type info)
            for col_name in columns:
                if col_name in ["geoid", "geom"]:
                    continue
                properties[col_name] = {"title": col_name, "type": "string"}

    localized, _ = collection.localize(language)
    payload: Dict[str, Any] = {
        "$id": queryables_url,
        "title": localized.get("title") or collection_id,
        "properties": properties,
    }
    if required_list:
        payload["required"] = required_list
    return ogc_models.Queryables.model_validate(payload)


def _process_feature_for_db(
    feature: ogc_models.FeatureDefinition, layer_config: ItemsPostgresqlDriverConfig
) -> Dict[str, Any]:
    """
    Validates and prepares a feature for database insertion, raising
    HTTPExceptions on failure.
    """
    if not feature.geometry:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Feature must have a valid geometry.",
        )

    try:
        # Convert Pydantic GeoJSON model to dict
        geom_dict = feature.geometry.model_dump()

        # Defensively look for ID in top level or properties
        props = feature.properties or {}
        external_id = feature.id or props.get("id")
        logger.debug(f"Processing feature for DB: id={external_id}")

        return feature.model_dump(by_alias=True, exclude_unset=True)
    except ValueError as e:
        logger.error(f"Failed to process feature: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except KeyError as e:
        logger.error(f"Failed to process feature due to KeyError: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal config error: Missing key {e}",
        )
