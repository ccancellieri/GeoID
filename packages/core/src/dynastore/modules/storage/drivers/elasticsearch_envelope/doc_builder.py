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
Build an envelope-feature doc shaped for ``ENVELOPE_FEATURE_MAPPING``.

Driver-private helper used by :class:`ItemsElasticsearchEnvelopeDriver`. Lives
in the envelope subpackage so the platform never imports it.

Like the private-driver doc builder, but additionally stamps the canonical
*access envelope* — ``visibility`` / ``owner`` / ``attrs`` — as typed
top-level fields so the row-level access filter can match on them reliably.

The doc shape aligns with the canonical envelope (#1285/#1800/#1828):
- flat identity + access ABAC fields at root (``visibility``/``owner``/``attrs``)
- ``geometry`` / ``bbox`` as GeoJSON reserved members
- ``properties`` sub-tree (dynamic)
- ``system`` container for lifecycle/simplification metadata
  (``geometry_simplification`` lives here, not at root)
- ``metadata`` container for multilingual descriptive metadata (optional)
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def build_envelope_feature_doc(
    item: Any,
    *,
    catalog_id: str,
    collection_id: str,
    external_id: Any = None,
    asset_id: Any = None,
    visibility: Any = None,
    owner: Any = None,
    attrs: Any = None,
    system: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build an ``ENVELOPE_FEATURE_MAPPING``-shaped doc from a Feature/dict.

    Accepts a Feature pydantic model, a STAC item dict, or a GeoJSON Feature
    dict. Pulls ``geoid`` from the item's ``id``, ``geometry`` and ``bbox``
    from the GeoJSON shape, and copies any non-internal ``properties`` (keys
    starting with ``_`` are skipped — those are internal tracking fields like
    ``_external_id``).

    Identity and access fields are projected as typed top-level fields.  Each
    falls back to a dispatcher-stamped ``_``-prefixed source key:

    * ``external_id``    ← arg, else ``src["_external_id"]``
    * ``asset_id``       ← arg, else ``src["_asset_id"]``
    * ``visibility``     ← arg, else ``src["_visibility"]``
    * ``owner``          ← arg, else ``src["_owner"]``
    * ``attrs``          ← arg, else ``src["_attrs"]`` (ABAC attribute dict
      from the per-collection stamping policy)

    Canonical containers:

    * ``system`` — carries ``geometry_simplification`` (factor/mode) when
      simplification was applied (mode != "none"), plus any caller-supplied
      system entries. Populated by the write path after ``maybe_simplify_for_es``
      is called; pass as ``None`` at initial build time.
    * ``metadata`` — multilingual descriptive metadata (title/description/
      keywords as localized dicts). Emitted when non-empty.

    The ABAC root fields (``visibility`` / ``owner`` / ``attrs``) deliberately
    stay at the document root — NOT inside the canonical ``access`` container —
    because the row-level filter predicates match on the root path
    (``{"terms": {"visibility": [...]}}``). Moving them would silently break
    every compiled AccessFilter clause.
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

    aid = asset_id if asset_id is not None else src.get("_asset_id")
    if aid is not None:
        doc["asset_id"] = str(aid)

    vis = visibility if visibility is not None else src.get("_visibility")
    if vis is not None:
        doc["visibility"] = str(vis)

    own = owner if owner is not None else src.get("_owner")
    if own is not None:
        doc["owner"] = str(own)

    # ABAC attribute dict — propagated verbatim from ``_attrs``.
    raw_attrs = attrs if attrs is not None else src.get("_attrs")
    if isinstance(raw_attrs, dict) and raw_attrs:
        doc["attrs"] = raw_attrs

    geom = src.get("geometry")
    if geom is not None:
        doc["geometry"] = geom

    bbox = src.get("bbox")
    if bbox is not None:
        doc["bbox"] = list(bbox)

    if props:
        doc["properties"] = props

    # Canonical ``system`` container — populated by the caller after
    # simplification (geometry_simplification lives here, not at root).
    if system:
        doc["system"] = dict(system)

    # Canonical ``metadata`` container — multilingual descriptive metadata.
    if metadata:
        doc["metadata"] = dict(metadata)

    return doc
