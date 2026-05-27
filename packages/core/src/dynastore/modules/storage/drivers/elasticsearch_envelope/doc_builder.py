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

``grant_subjects`` support has been retired as a write path (#1441): the
``_normalize_grant_subjects`` helper is kept for read-tolerance on documents
written before #1441 (accept-but-ignore on already-stored docs) and will be
removed in a future release.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def _normalize_grant_subjects(value: Any) -> Optional[List[str]]:
    """Coerce a grant-subjects value to a list of strings (or ``None``).

    Retained for read-tolerance on docs written before #1441.  New docs no
    longer carry ``_grant_subjects``; this normaliser is a no-op on them.
    Scheduled for removal after one release.
    """
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        out = [str(v) for v in value if v is not None]
        return out or None
    return [str(value)]


def build_envelope_feature_doc(
    item: Any,
    *,
    catalog_id: str,
    collection_id: str,
    external_id: Any = None,
    asset_id: Any = None,
    visibility: Any = None,
    owner: Any = None,
    grant_subjects: Any = None,
    attrs: Any = None,
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
    * ``grant_subjects`` ← arg, else ``src["_grant_subjects"]`` (read-tolerance
      for pre-#1441 docs; new docs omit this field)
    * ``attrs``          ← arg, else ``src["_attrs"]`` (ABAC attribute dict
      from the per-collection stamping policy, #1441)
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

    # Read-tolerance for pre-#1441 docs that still carry ``grant_subjects``.
    raw_grants = (
        grant_subjects if grant_subjects is not None else src.get("_grant_subjects")
    )
    grants = _normalize_grant_subjects(raw_grants)
    if grants is not None:
        doc["grant_subjects"] = grants

    # ABAC attribute dict (#1441) — propagated verbatim from ``_attrs``.
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

    return doc
