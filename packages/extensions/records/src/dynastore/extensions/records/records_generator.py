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

"""Transforms internal Feature / DB rows into OGC Records API responses.

The internal model is ``Feature(geometry=None, properties={...})`` by
default; this module maps those to the ``Record`` wire format defined by
OGC API - Records Part 1 (OGC 20-004). Req 55 of that spec allows a
record's ``geometry`` to be a real geometry instead of ``null`` — a
collection opts into that via the ``CollectionInfo.allow_geometry``
capability (RFC #2550), resolved by :func:`collection_has_geometry`.
"""

import logging
from typing import Any, Dict, Optional, Union

from geojson_pydantic import Feature as _GeoJSONFeature

from dynastore.models.protocols import ItemsProtocol
from dynastore.models.localization import LocalizedText
from dynastore.models.shared_models import Link
from dynastore.models.driver_context import DriverContext
from dynastore.modules.storage.driver_config import ItemsPostgresqlDriverConfig
from dynastore.tools.discovery import get_protocol

from . import records_models as rm

logger = logging.getLogger(__name__)


async def collection_has_geometry(
    catalog_id: str,
    collection_id: str,
    db_resource: Optional[Any] = None,
    strict: bool = False,
) -> bool:
    """Resolve whether *collection_id* currently has an active geometry sidecar.

    Consults the same ``_effective_sidecars`` resolution the PG driver uses
    at write/read time — ``CollectionInfo.kind`` plus the ``allow_geometry``
    capability override (RFC #2550) — instead of re-reading
    ``allow_geometry`` in isolation. This keeps the answer correct across a
    ``kind``/``allow_geometry`` reclassification (both are mutable) without
    requiring a matching physical migration: the resolved sidecar list is
    the single source of truth every write/read call site already uses.

    ``strict`` selects the error posture:

    - ``strict=False`` (default, read paths) fails *closed* — a resolution
      error returns ``False`` (the historic geometry-less RECORDS default),
      so a geometry is at worst hidden from a response, never fabricated.
    - ``strict=True`` (write paths) re-raises the resolution error instead
      of returning ``False``. On a write, a false negative would force a
      client-submitted geometry to ``null`` and persist that data loss
      behind a ``201`` — so a transient config-resolution hiccup must fail
      the write loudly and let the client retry, not silently drop data.
    """
    try:
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.catalog.catalog_config import CollectionInfo
        from dynastore.modules.storage.drivers.pg_sidecars import _effective_sidecars
        from dynastore.modules.storage.drivers.pg_sidecars.geometries_config import (
            GeometriesSidecarConfig,
        )

        configs = get_protocol(ConfigsProtocol)
        if configs is None:
            return False
        ct = await configs.get_config(
            CollectionInfo, catalog_id=catalog_id, collection_id=collection_id,
        )
        col_config = await configs.get_config(
            ItemsPostgresqlDriverConfig,
            catalog_id=catalog_id, collection_id=collection_id,
            ctx=DriverContext(db_resource=db_resource),
        )
        resolved = _effective_sidecars(
            col_config,
            catalog_id=catalog_id,
            collection_id=collection_id,
            collection_type=ct.kind.value,
            context={"allow_geometry": ct.allow_geometry},
        )
        return any(isinstance(sc, GeometriesSidecarConfig) for sc in resolved)
    except Exception:
        logger.warning(
            "collection_has_geometry: resolution failed for %s/%s",
            catalog_id, collection_id, exc_info=True,
        )
        if strict:
            raise
        return False


def db_row_to_record(
    item: Union[Dict, Any],
    catalog_id: str,
    collection_id: str,
    root_url: str,
    layer_config: Optional[ItemsPostgresqlDriverConfig] = None,
    read_policy: Optional[Any] = None,
    geometry_enabled: bool = False,
) -> rm.Record:
    """Convert a DB row or mapped Feature into an OGC Record.

    If *item* is not yet a GeoJSON Feature it is mapped via
    ``ItemsProtocol`` (sidecar pipeline).  Then OGC Records-specific
    post-processing is applied:
      - ``validity`` TSTZRANGE -> ``time.interval``
      - self / collection links

    ``read_policy`` is the resolved :class:`ItemsReadPolicy` for the
    collection; it is threaded into the sidecar pipeline so the raw-row
    fallback honours ``feature_type.expose`` / ``external_id_as_feature_id``.
    Items arriving already mapped (the canonical ``stream_items`` path) skip
    the fallback and are unaffected.

    ``geometry_enabled`` (RFC #2550, resolved by the caller via
    :func:`collection_has_geometry`) gates whether the mapped item's
    ``geometry``/``bbox`` are surfaced on the returned ``Record`` or forced
    to ``null`` — the OGC API - Records Part 1 default for a collection with
    no geometry capability. Defaults to ``False`` so existing callers that
    don't pass it keep the byte-identical geometry-less behaviour.
    """
    # Map raw DB row via sidecar pipeline if needed
    if not isinstance(item, _GeoJSONFeature):
        items_mod = get_protocol(ItemsProtocol)
        if items_mod and layer_config:
            item = items_mod.map_row_to_feature(
                item, layer_config, read_policy=read_policy
            )
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

    record_kwargs: Dict[str, Any] = dict(
        type="Feature",
        id=feature_id,
        geometry=getattr(item, "geometry", None) if geometry_enabled else None,
        properties=record_props,
        links=links,
    )
    if geometry_enabled:
        bbox = getattr(item, "bbox", None)
        if bbox is not None:
            record_kwargs["bbox"] = bbox

    return rm.Record(**record_kwargs)


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
