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

"""Build EDR collection metadata from STAC collection models."""

from __future__ import annotations

from typing import Any, Dict

from dynastore.extensions.tools.response_i18n import resolve_localized


def build_edr_collection(
    catalog_id: str,
    collection: Any,
    *,
    base_url: str,
    language: str = "en",
) -> Dict[str, Any]:
    """Build EDR collection metadata dict from a STAC collection model.

    ``language`` controls how multi-language ``LocalizedText`` values on the
    STAC collection (title, description) are resolved before being written into
    the response dict.  ``language='*'`` leaves them as full multi-language
    objects; any other value collapses to a single string, falling back to
    ``"en"`` if the requested language is absent.
    """
    collection_id = getattr(collection, "id", None) or ""
    raw_title = getattr(collection, "title", None)
    title = resolve_localized(raw_title, language) or collection_id
    raw_description = getattr(collection, "description", None)
    description = resolve_localized(raw_description, language) or ""

    extent: Dict[str, Any] = {}
    raw_extent = getattr(collection, "extent", None)
    if raw_extent is not None:
        extent_dict = (
            raw_extent.model_dump(exclude_none=True)
            if hasattr(raw_extent, "model_dump")
            else {}
        )
        if extent_dict:
            extent = _build_extent(extent_dict)

    prefix = f"{base_url}/edr/catalogs/{catalog_id}/collections/{collection_id}"

    return {
        "id": collection_id,
        "title": title,
        "description": description,
        "extent": extent,
        "data_queries": {
            "position": {
                "link": {
                    "href": f"{prefix}/position",
                    "rel": "data",
                    "type": "application/prs.coverage+json",
                    "title": "Position query",
                }
            },
            "area": {
                "link": {
                    "href": f"{prefix}/area",
                    "rel": "data",
                    "type": "application/prs.coverage+json",
                    "title": "Area query",
                }
            },
            "cube": {
                "link": {
                    "href": f"{prefix}/cube",
                    "rel": "data",
                    "type": "application/prs.coverage+json",
                    "title": "Cube query",
                }
            },
        },
        "crs": ["CRS84"],
        "output_formats": ["CoverageJSON", "GeoJSON"],
        "links": [
            {
                "href": prefix,
                "rel": "self",
                "type": "application/json",
                "title": title,
            },
            {
                "href": f"{base_url}/edr/catalogs/{catalog_id}/collections",
                "rel": "collection",
                "type": "application/json",
                "title": "Collections",
            },
        ],
    }


def _build_extent(extent_dict: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    spatial = extent_dict.get("spatial") or {}
    if spatial.get("bbox"):
        bbox = spatial["bbox"]
        first = bbox[0] if isinstance(bbox[0], list) else bbox
        result["spatial"] = {
            "bbox": [first],
            "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
        }
    temporal = extent_dict.get("temporal") or {}
    if temporal.get("interval"):
        result["temporal"] = {
            "interval": temporal["interval"],
            "trs": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian",
        }
    return result
