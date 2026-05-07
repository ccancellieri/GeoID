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

"""
Build a tenant-feature doc shaped for ``TENANT_FEATURE_MAPPING``.

Driver-private helper used by :class:`PrivateEntityTransformer`,
:class:`ItemsElasticsearchPrivateDriver` (write_entities), and the
``PrivateIndexTask`` runner. Lives in the private subpackage so the
platform never imports it.
"""

from __future__ import annotations

from typing import Any, Dict


def build_tenant_feature_doc(
    item: Any,
    *,
    catalog_id: str,
    collection_id: str,
    external_id: Any = None,
) -> Dict[str, Any]:
    """Build a ``TENANT_FEATURE_MAPPING``-shaped doc from a Feature/dict.

    Accepts a Feature pydantic model, a STAC item dict, or a GeoJSON
    Feature dict. Pulls ``geoid`` from the item's ``id``, ``geometry`` and
    ``bbox`` from the GeoJSON shape, and copies any non-internal
    ``properties`` (keys starting with ``_`` are skipped — those are
    SFEOS-internal tracking fields like ``_external_id``).
    """
    if hasattr(item, "model_dump"):
        src = item.model_dump(by_alias=True, exclude_none=True)
    elif isinstance(item, dict):
        src = item
    else:
        src = dict(item)

    geoid = src.get("id") or src.get("geoid")
    raw_props = src.get("properties") or {}
    props = {k: v for k, v in raw_props.items() if not str(k).startswith("_")}

    doc: Dict[str, Any] = {
        "geoid": geoid,
        "catalog_id": catalog_id,
        "collection_id": collection_id,
    }

    ext = external_id if external_id is not None else src.get("_external_id")
    if ext is not None:
        doc["external_id"] = str(ext)

    geom = src.get("geometry")
    if geom is not None:
        doc["geometry"] = geom

    bbox = src.get("bbox")
    if bbox is not None:
        doc["bbox"] = list(bbox)

    if props:
        doc["properties"] = props

    return doc
