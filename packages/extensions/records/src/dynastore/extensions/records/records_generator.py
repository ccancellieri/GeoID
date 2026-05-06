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

"""Transforms internal Feature / DB rows into OGC Records API responses.

The internal model is ``Feature(geometry=None, properties={...})``; this
module maps those to the ``Record`` wire format defined by OGC API -
Records Part 1 (OGC 20-004).
"""

import logging
from typing import Any, Dict, List, Optional, Union, cast

from geojson_pydantic import Feature as _GeoJSONFeature

from dynastore.models.protocols import ItemsProtocol
from dynastore.models.localization import LocalizedText
from dynastore.models.shared_models import Link
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.tools.discovery import get_protocol

from . import records_models as rm

logger = logging.getLogger(__name__)


def db_row_to_record(
    item: Union[Dict, Any],
    catalog_id: str,
    collection_id: str,
    root_url: str,
    layer_config: Optional[ItemsPostgresqlDriverConfig] = None,
) -> rm.Record:
    """Convert a DB row or mapped Feature into an OGC Record.

    If *item* is not yet a GeoJSON Feature it is mapped via
    ``ItemsProtocol`` (sidecar pipeline).  Then OGC Records-specific
    post-processing is applied:
      - ``validity`` TSTZRANGE -> ``time.interval``
      - self / collection links
    """
    # Map raw DB row via sidecar pipeline if needed
    if not isinstance(item, _GeoJSONFeature):
        items_mod = get_protocol(ItemsProtocol)
        if items_mod and layer_config:
            item = items_mod.map_row_to_feature(item, layer_config)
        else:
            logger.warning(
                "Cannot map DB row: ItemsProtocol unavailable or no layer_config."
            )
            item = _GeoJSONFeature(type="Feature", geometry=None, properties={})

    # Extract properties
    props = dict(item.properties or {}) if hasattr(item, "properties") else {}

    # Map validity range to Records time.interval
    time_obj = _extract_time(props)

    # Build record properties
    record_props = rm.RecordProperties(
        title=props.pop("title", None),
        description=props.pop("description", None),
        keywords=props.pop("keywords", None),
        language=props.pop("language", None),
        languages=props.pop("languages", None),
        type=props.pop("recordType", None),
        time=time_obj,
        created=_to_iso(props.pop("created", props.pop("created_at", None))),
        updated=_to_iso(props.pop("updated", props.pop("updated_at", None))),
        **{k: v for k, v in props.items() if v is not None},
    )

    # Feature ID
    feature_id = str(item.id) if item.id is not None else None

    # Links
    self_url = f"{root_url}/records/catalogs/{catalog_id}/collections/{collection_id}/items/{feature_id}"
    links = [
        Link(href=self_url, rel="self", type="application/geo+json"),
        Link(
            href=f"{root_url}/records/catalogs/{catalog_id}/collections/{collection_id}",
            rel="collection",
            type="application/json",
        ),
    ]

    return rm.Record(
        type="Feature",
        id=feature_id,
        geometry=None,
        properties=record_props,
        links=links,
    )


def collection_to_records_collection(
    collection: Any,
    catalog_id: str,
    root_url: str,
) -> rm.RecordsCatalogCollection:
    """Convert an internal Collection model to a Records collection descriptor.

    For dimension collections (those whose ``extra_metadata`` carries
    ``cube:dimensions``), the generator metadata and dimension-specific
    links (queryables, members, inverse) are surfaced in the response.
    """
    coll_dict = collection if isinstance(collection, dict) else collection.model_dump(exclude_none=True)

    coll_id = coll_dict.get("id", "")
    self_url = f"{root_url}/records/catalogs/{catalog_id}/collections/{coll_id}"

    links = [
        Link(href=self_url, rel="self", type="application/json"),
        Link(
            href=f"{self_url}/items",
            rel="items",
            type="application/geo+json",
            title=LocalizedText(en="Records in this collection"),
        ),
    ]

    # Extract cube:dimensions from extra_metadata (set by DimensionsExtension)
    extra_meta = coll_dict.get("extra_metadata") or {}
    cube_dimensions = extra_meta.get("cube:dimensions")

    # Add dimension-specific links when cube:dimensions is present
    if cube_dimensions:
        dim_base = f"{root_url}/dimensions/{coll_id}"
        links.append(
            Link(
                href=f"{dim_base}/members",
                rel="items",
                type="application/geo+json",
                title=LocalizedText(en="Dimension members (generator API)"),
            ),
        )
        links.append(
            Link(
                href=f"{dim_base}/queryables",
                rel="queryables",
                type="application/schema+json",
                title=LocalizedText(en="Queryable member properties"),
            ),
        )
        # Check generator capabilities for inverse/hierarchical links
        for dim_meta in cube_dimensions.values():
            gen_meta = dim_meta.get("generator", {})
            if gen_meta.get("invertible"):
                links.append(
                    Link(
                        href=f"{dim_base}/inverse",
                        rel="describedby",
                        type="application/json",
                        title=LocalizedText(en="Value-to-member inverse mapping"),
                    ),
                )
            caps = gen_meta.get("capabilities", [])
            if "children" in caps or "hierarchical" in caps:
                links.append(
                    Link(
                        href=f"{dim_base}/children",
                        rel="children",
                        type="application/geo+json",
                        title=LocalizedText(en="Hierarchical children navigation"),
                    ),
                )
            break  # single dimension per collection

    return rm.RecordsCatalogCollection(
        id=coll_id,
        title=coll_dict.get("title"),
        description=coll_dict.get("description"),
        extent=coll_dict.get("extent"),
        keywords=coll_dict.get("keywords"),
        links=links,
        itemType="record",
        cube_dimensions=cube_dimensions,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_time(props: Dict[str, Any]) -> Optional[rm.RecordTime]:
    """Extract temporal information from properties."""
    import re

    # Direct time object
    time_val = props.pop("time", None)
    if time_val and isinstance(time_val, dict):
        return rm.RecordTime(**time_val)

    # From validity range (PG tstzrange)
    validity = props.pop("validity", None)
    if validity is not None:
        try:
            if hasattr(validity, "lower"):
                start = _to_iso(validity.lower) if validity.lower else None
                end = _to_iso(validity.upper) if validity.upper else None
                return rm.RecordTime(interval=[start, end])
            match = re.search(r"[\[\(]([^,]*),\s*([^\]\)]*)", str(validity))
            if match:
                s, e = match.groups()
                start = s.strip().strip('"') if s.strip() and s.strip() != "-infinity" else None
                end = e.strip().strip('"') if e.strip() and e.strip() != "infinity" else None
                return rm.RecordTime(interval=[start, end])
        except Exception as exc:
            logger.warning("Failed to parse validity range %s: %s", validity, exc)

    # From valid_from / valid_to
    vf = props.pop("valid_from", None)
    vt = props.pop("valid_to", None)
    if vf or vt:
        return rm.RecordTime(interval=[_to_iso(vf), _to_iso(vt)])

    # From start_datetime / end_datetime
    sd = props.pop("start_datetime", None)
    ed = props.pop("end_datetime", None)
    if sd or ed:
        return rm.RecordTime(interval=[_to_iso(sd), _to_iso(ed)])

    return None


def _to_iso(val: Any) -> Optional[str]:
    """Convert a datetime-like value to ISO 8601 string."""
    if val is None:
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)
