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
- ``properties`` sub-tree (dynamic — only user attrs; SYSTEM_FIELD_KEYS and
  ``_``-prefixed internal keys are excluded so they never leak into properties)
- ``system`` container for lifecycle/simplification metadata
  (``geometry_simplification`` lives here, not at root)
- ``metadata`` container for multilingual descriptive metadata (optional)

``CanonicalIndexInput`` fast path: when the caller already has the PG-fetched
canonical input (e.g. the IndexDispatcher passing a pre-built payload), the
identity / properties / system / stats sections are assembled via
:func:`~dynastore.modules.elasticsearch.canonical_doc.build_canonical_index_doc`
and the access fields are overlaid on top.  Stats/system sections are populated
with real sidecar values on this path; they are empty when the caller provides
only a Feature/dict.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

# SYSTEM_FIELD_KEYS that live in the canonical ``system`` / identity containers
# and must NEVER appear inside ``properties`` on any envelope doc.
# Imported lazily to avoid a circular import risk at module level.
_SYSTEM_KEYS_CACHE: "frozenset[str] | None" = None


def _get_system_keys() -> "frozenset[str]":
    global _SYSTEM_KEYS_CACHE
    if _SYSTEM_KEYS_CACHE is None:
        from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS
        _SYSTEM_KEYS_CACHE = frozenset(SYSTEM_FIELD_KEYS)
    return _SYSTEM_KEYS_CACHE


def _overlay_access_fields(
    doc: Dict[str, Any],
    *,
    src: Dict[str, Any],
    visibility: Any,
    owner: Any,
    attrs: Any,
) -> None:
    """Stamp ABAC root fields onto ``doc`` in-place.

    Each field falls back to a dispatcher-stamped ``_``-prefixed source key.
    The fields are typed root keywords so row-level filter predicates can
    match them reliably — they must NOT be inside the ``access`` container.
    """
    vis = visibility if visibility is not None else src.get("_visibility")
    if vis is not None:
        doc["visibility"] = str(vis)

    own = owner if owner is not None else src.get("_owner")
    if own is not None:
        doc["owner"] = str(own)

    raw_attrs = attrs if attrs is not None else src.get("_attrs")
    if isinstance(raw_attrs, dict) and raw_attrs:
        doc["attrs"] = raw_attrs


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

    Accepts:

    * A :class:`~dynastore.modules.catalog.canonical_index_read.CanonicalIndexInput`
      instance — the preferred path when the caller already has a batched PG
      read result.  Delegates to
      :func:`~dynastore.modules.elasticsearch.canonical_doc.build_canonical_index_doc`
      for the base (identity / properties / system / stats) then overlays the
      ABAC access fields at root.

    * A Feature pydantic model, a STAC item dict, or a GeoJSON Feature dict —
      extracts identity / geometry / properties and assembles the envelope doc
      directly.

    Pulls ``geoid`` from the item's ``id`` / ``geoid``, ``geometry`` and
    ``bbox`` from the GeoJSON shape.  ``properties`` is cleaned: keys starting
    with ``_`` (internal tracking fields like ``_external_id``) and keys in
    :data:`SYSTEM_FIELD_KEYS` (lifecycle fields like ``external_id``,
    ``validity``, ``geometry_hash``) are excluded so they never leak into the
    ``properties`` container (refs #1828).

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
    # --- CanonicalIndexInput fast path -----------------------------------
    # When the caller already has the PG-fetched canonical input, build the
    # canonical base (identity / user-properties / system / stats) via the
    # shared SSOT then overlay the envelope-specific access fields at root.
    #
    # ``build_canonical_index_doc`` calls ``project_item_for_es(doc, {})``
    # which routes all user properties to ``properties.extras`` when
    # ``known_fields`` is empty (the no-known-fields = extras-everything
    # contract).  The envelope mapping uses ``dynamic:True`` on
    # ``properties``, so the read path (``_envelope_source_to_feature``)
    # reads ``source["properties"]`` flat — it does not hoist from
    # ``extras``.  We therefore hoist ``properties.extras`` back to flat
    # ``properties`` after the canonical build, matching the envelope's
    # dynamic-properties semantics while still using the shared SSOT for
    # identity / system / stats assembly.
    try:
        from dynastore.modules.catalog.canonical_index_read import CanonicalIndexInput
        if isinstance(item, CanonicalIndexInput):
            from dynastore.modules.elasticsearch.canonical_doc import build_canonical_index_doc
            doc = build_canonical_index_doc(
                item.row,
                resolved_sidecars=item.resolved_sidecars,
                known_fields={},  # envelope uses dynamic properties; extras hoisted below
                catalog_id=catalog_id,
                collection_id=collection_id,
                geometry=item.geometry,
                bbox=item.bbox,
                user_properties=item.user_properties,
                # ABAC fields are overlaid below as flat root keys (the ONLY
                # shape ``ENVELOPE_FEATURE_MAPPING`` declares for them — see
                # its ``visibility``/``owner``/``attrs`` root properties); the
                # canonical ``access`` container is never passed through here
                # even when ``item.access`` is populated (#2687).
                access=None,
            )
            # ``access=None`` above keeps ``doc["access"]`` from ever being
            # set here — ``ENVELOPE_FEATURE_MAPPING`` (root ``dynamic:
            # false``) declares no ``access`` field, and the ABAC values
            # live at the flat root instead (overlaid below). ``stats`` /
            # ``system`` stay: the mapping stores-but-does-not-index
            # unmapped ``dynamic: false`` fields, and letting them ride
            # along unindexed for observability is existing, tested
            # behaviour (see ``test_canonical_input_fast_path_stats_populated``).
            # Canonical sets ``id``=geoid; envelope read path uses ``geoid`` at root.
            geoid = doc.get("id")
            if geoid and "geoid" not in doc:
                doc["geoid"] = geoid
            # Hoist ``properties.extras`` back to flat ``properties`` so the
            # envelope read path finds user attrs at ``properties.<key>``
            # (the read does not hoist from extras).  A flat key already
            # present wins over the extras value (more specific wins).
            inner_props = doc.get("properties")
            if isinstance(inner_props, dict):
                extras = inner_props.pop("extras", None)
                if isinstance(extras, dict):
                    for k, v in extras.items():
                        inner_props.setdefault(k, v)
                # Remove _search_text injected by project_item_for_es for the
                # extras we just un-nested; the envelope mapping does not need
                # it (properties is dynamic:True, full-text is not used there).
                doc.pop("_search_text", None)
            # Overlay access fields: an explicit ``visibility``/``owner``/
            # ``attrs`` arg wins (caller opts in to a specific value), else
            # fall back to ``item.access`` (#2687 — the drain-recomputed
            # envelope) via the same ``_owner``/``_visibility``/``_attrs``
            # source keys a dispatcher-stamped row would carry.
            raw_src: Dict[str, Any] = dict(item.row or {})
            if item.access:
                raw_src.setdefault("_visibility", item.access.get("_visibility"))
                raw_src.setdefault("_owner", item.access.get("_owner"))
                raw_src.setdefault("_attrs", item.access.get("_attrs"))
            _overlay_access_fields(doc, src=raw_src, visibility=visibility, owner=owner, attrs=attrs)
            if system:
                doc.setdefault("system", {}).update(system)
            if metadata:
                doc["metadata"] = dict(metadata)
            return doc
    except ImportError:
        pass

    # --- Feature / dict path -------------------------------------------
    _item: Any = item  # rebind to Any so pyright does not narrow further
    if hasattr(_item, "model_dump"):
        src: Dict[str, Any] = _item.model_dump(by_alias=True, exclude_none=True)
    elif isinstance(_item, dict):
        src = _item
    else:
        src = dict(_item)  # type: ignore[arg-type]

    geoid = src.get("id") or src.get("geoid")
    raw_props = src.get("properties") or {}
    _sys_keys = _get_system_keys()
    # Exclude both ``_``-prefixed internal tracking keys and SYSTEM_FIELD_KEYS
    # (e.g. ``external_id``, ``validity``, ``geometry_hash``) so they never
    # pollute the ``properties`` container (refs #1828).
    props = {
        k: v for k, v in raw_props.items()
        if not str(k).startswith("_") and k not in _sys_keys
    }

    doc = {
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

    _overlay_access_fields(doc, src=src, visibility=visibility, owner=owner, attrs=attrs)

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
