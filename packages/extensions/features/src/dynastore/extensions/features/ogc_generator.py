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

# dynastore/extensions/features/ogc_generator.py

import logging

logger = logging.getLogger(__name__)

from typing import Dict, Any, Optional, List, Union, cast
from fastapi import Request, HTTPException, status
from dynastore.models.protocols import CatalogsProtocol, ItemsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.modules.elasticsearch.items_projection import strip_reserved_members
from dynastore.modules.db_config.tools import map_pg_to_json_type

from . import ogc_models
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.modules.catalog.models import (
    Collection as CoreCollection,
)  # Import CoreCollection for type hinting
from dynastore.models.localization import Language
from dynastore.extensions.tools.url import get_root_url
from dynastore.models.shared_models import Link
from dynastore.extensions.tools.url import get_url


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
    read_policy: Optional[Any] = None,
) -> ogc_models.Feature:
    """
    Converts a database row or an already-mapped Feature into an OGC Feature.

    If *item* is not yet a GeoJSON Feature it is mapped via ``ItemsProtocol``
    (which runs the full sidecar pipeline).  The function then applies
    OGC-specific post-processing:
      - validity TSTZRANGE -> ``start_datetime`` / ``end_datetime``
      - safe ``id`` serialisation (never the string ``'None'``)
      - collection/self links

    ``read_policy`` is the resolved :class:`ItemsReadPolicy` for the
    collection; it is threaded into the sidecar pipeline so the raw-row
    fallback honours ``feature_type.expose`` / ``external_id_as_feature_id``.
    Items arriving already mapped (the canonical ``get_item``/``stream_items``
    path) skip the fallback and are unaffected.
    """
    from geojson_pydantic import Feature as _GeoJSONFeature

    # -- 1. Map raw DB row -> Feature (if not already mapped) -----------------
    if not isinstance(item, _GeoJSONFeature):
        items_mod = get_protocol(ItemsProtocol)
        if items_mod and layer_config:
            item = items_mod.map_row_to_feature(
                item, layer_config, read_policy=read_policy
            )
        else:
            logger.warning(
                "Cannot map DB row: ItemsProtocol unavailable or no layer_config. "
                "Returning empty feature."
            )
            item = _GeoJSONFeature(type="Feature", geometry=None, properties={})

    item = cast(_GeoJSONFeature, item)
    # -- 2. Extract id (never serialise None as the string 'None') ------------
    feature_id = item.id if item.id is not None else None

    # -- 3. Convert geometry to a GeoJSONGeometry -----------------------------
    geometry_model = None
    if item.geometry is not None:
        geom_dict = (
            item.geometry.model_dump()
            if hasattr(item.geometry, "model_dump")
            else dict(item.geometry)
        )
        from pydantic import TypeAdapter
        # Imported here (not via ``ogc_models.GeoJSONGeometry``) so an F401 sweep
        # of ogc_models.py cannot strip a re-export reached only by attribute
        # access — the exact way #1648 regressed.
        from dynastore.models.ogc import GeoJSONGeometry
        geometry_model = TypeAdapter(GeoJSONGeometry).validate_python(geom_dict)

    # -- 4. Properties + OGC-specific validity mapping ------------------------
    # Strip GeoJSON/STAC structural members (id, geometry, …) that can ride into
    # stored properties — the PG ``feature_id_expr AS id`` alias via the
    # attributes sidecar, or the ES write echo returning the in-memory feature
    # as-is. The canonical id already lives at the feature top level; keeping it
    # out of properties aligns the POST/PUT echo with the GET read contract that
    # ``project_item_for_es`` enforces on the index path (#1232).
    properties = strip_reserved_members(item.properties or {})

    _map_validity_to_ogc(properties)

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

    # Advertise the bounded canonical system/stats lanes (refs #2230). They are
    # baked into build_item_mapping unconditionally (refs #2228) and resolved
    # end-to-end by the read path, but were not discoverable via /queryables so
    # a conformant client could not know they are filterable. ``setdefault`` so
    # a per-collection field of the same name keeps precedence over the generic
    # canonical type. Driver-agnostic — sourced from the mapping SSOT.
    from dynastore.modules.elasticsearch.mappings import canonical_queryable_properties

    for _qname, _frag in canonical_queryable_properties().items():
        properties.setdefault(_qname, _frag)

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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    except KeyError as e:
        logger.error(f"Failed to process feature due to KeyError: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal config error: Missing key {e}",
        ) from e
