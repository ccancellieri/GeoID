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

"""Canonical, policy-independent Elasticsearch _source builders.

The canonical-index-envelope initiative (#1800) shipped the item builder; the
generalization (#1285) factors its section assembly into a level-agnostic core
(:func:`build_canonical_envelope`) reused by every entity level (catalog /
collection / item / asset). The shape is the contract — ``system`` (core
identity/lifecycle), ``properties`` (attributes), ``stats`` (derived),
``access`` (IAM), plus reserved protocol-structural members the read-time
projector surfaces verbatim. ES-only for now; other drivers keep their own
internal storage concern.
"""
from typing import Any, Dict, List, Optional

from dynastore.modules.elasticsearch.items_projection import project_item_for_es
from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS

_SYSTEM_KEYS: frozenset = frozenset(SYSTEM_FIELD_KEYS)


def build_canonical_envelope(
    *,
    identity: Dict[str, Any],
    properties: Dict[str, Any],
    known_fields: Dict[str, Any],
    reserved_members: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    system: Optional[Dict[str, Any]] = None,
    stats: Optional[Dict[str, Any]] = None,
    access: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Assemble the level-agnostic canonical ES ``_source`` (refs #1285/#1800/#1828).

    One modular, pluggable shape for every entity level. The sections are the
    contract; what fills them is the caller's (per-level) concern:

    * **flat identity** — ``identity`` keys sit at the document top level
      (``id``, ``catalog_id``, ``collection_id``, ``external_id``, ``asset_id``,
      ``validity``, the ``_external_id`` read tracker …). ``id`` is whatever
      stable identifier the level uses (``geoid`` for items, ``collection_id``
      for collections, ``catalog_id`` for catalogs).
    * **reserved members** — protocol-structural keys surfaced verbatim by the
      read-time projector (``collection``/``geometry``/``bbox`` for items;
      ``extent``/``summaries``/``providers``/``links``/``assets``/
      ``stac_extensions`` for collections; …). ``None`` values are skipped.
    * **properties** — the attribute bag. Reshaped through
      :func:`project_item_for_es` so unknown keys move under
      ``properties.extras`` (the ``flattened`` long-tail lane) and the analyzed
      ``_search_text`` catch-all is populated — identical handling at every
      level, so the strict ``dynamic: false`` mapping never grows per-key.
    * **metadata** — multilingual descriptive metadata (``title``,
      ``description``, ``keywords``), sourced from the ``ItemMetadataSidecar``
      (refs #1828). Typed ``dynamic: false`` mapping with per-language analyzed
      sub-fields. Emitted when non-empty.
    * **system** — the core identity/lifecycle container. Emitted when non-empty.
    * **stats** — derived values. Emitted when non-empty.
    * **access** — the IAM authorization sidecar, plugged in by the ABAC layer.
      Emitted when non-empty.

    Pure function — returns a new dict; inputs are not mutated. Reserved
    GeoJSON/STAC members never leak into ``properties`` (enforced by
    :func:`project_item_for_es`).
    """
    doc: Dict[str, Any] = dict(identity)
    if reserved_members:
        for key, value in reserved_members.items():
            if value is not None:
                doc[key] = value

    # properties: attribute bag; unknown keys move to properties.extras by
    # reusing the existing projection so behavior matches the rest of the ES
    # projection path at every level.
    doc["properties"] = dict(properties or {})
    doc = project_item_for_es(doc, known_fields)

    if metadata:
        doc["metadata"] = dict(metadata)
    if system:
        doc["system"] = dict(system)
    if stats:
        doc["stats"] = dict(stats)
    if access:
        doc["access"] = dict(access)
    return doc


def build_canonical_index_doc(
    row: Dict[str, Any],
    *,
    resolved_sidecars: List[Any],
    known_fields: Dict[str, Any],
    catalog_id: str,
    collection_id: str,
    geometry: Optional[dict] = None,
    bbox: Optional[list] = None,
    user_properties: Optional[Dict[str, Any]] = None,
    access: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Assemble the policy-independent canonical ES _source.

    Sections:
      - flat identity: id(=geoid), catalog_id, collection_id, external_id,
        asset_id, validity
      - geometry, bbox
      - properties: user attrs, typed-known kept flat, unknown moved to
        properties.extras (via project_item_for_es — same reshape as the
        rest of the ES projection path)
      - stats: every producible computed value from resolved_sidecars NOT in
        SYSTEM_FIELD_KEYS (system wins the overlap)
      - system: SYSTEM_FIELD_KEYS values present on the row (content hashes
        belong here)
      - access: pass-through when non-empty
      - _external_id: transition tracker mirrored at top level (read path
        depends on it)

    ``id`` is ALWAYS ``row["geoid"]``, regardless of any policy.
    Reserved STAC/GeoJSON members never leak into properties.

    Implemented as a thin item-level adapter over
    :func:`build_canonical_envelope`: it maps the PG row + sidecars onto the
    generic sections (identity / reserved members / properties / system /
    stats / access) and delegates the assembly.
    """
    identity: Dict[str, Any] = {
        "id": row.get("geoid"),
        "catalog_id": catalog_id,
        "collection_id": collection_id,
        # ``collection`` is the STAC/GeoJSON wire member (a reserved key the
        # read reconstruction surfaces verbatim); ``collection_id`` is the
        # internal queryable/filter field. Both are carried until the
        # read-time projector derives the wire ``collection`` from
        # ``collection_id`` (#1285). Dropping it here would null the
        # ``collection`` member on every ES-served STAC hit and break the
        # ``collection``-term existence check used by the REFUSE write policy.
        "collection": collection_id,
    }

    external_id = row.get("external_id")
    if external_id is not None:
        identity["external_id"] = str(external_id)
        identity["_external_id"] = str(external_id)   # transition tracker (read path)

    if row.get("asset_id") is not None:
        identity["asset_id"] = str(row["asset_id"])

    if row.get("validity") is not None:
        identity["validity"] = row["validity"]

    # system: SYSTEM_FIELD_KEYS values present on the row (content hashes live
    # here, not in stats).
    system = {k: row[k] for k in SYSTEM_FIELD_KEYS if row.get(k) is not None}

    # stats: producible computed values from sidecars NOT claimed by system.
    # System wins all overlaps: if a sidecar also produces geometry_hash it
    # still lives in system only.
    stats: Dict[str, Any] = {}
    for sidecar in resolved_sidecars:
        for name in sidecar.producible_computed_names():
            if name in _SYSTEM_KEYS or name in stats:
                continue
            found, value = sidecar.resolve_computed_value(row, name)
            if found and value is not None:
                stats[name] = value

    # metadata: multilingual descriptive metadata from the ItemMetadataSidecar
    # (item_title / item_description / item_keywords JSONB columns). The sidecar
    # wiring into this assembly is deferred to #1828 Phase 2; for now we read
    # the three columns directly from the row if present so callers that already
    # populate them (e.g. future PG-level joins) get the typed metadata container
    # without any driver change. The preferred canonical names are the un-prefixed
    # forms (title/description/keywords); the item_* aliases are also accepted.
    # TODO (#1828 Phase 2): wire ItemMetadataSidecar here so sidecars can produce
    # metadata fields and this reads from resolved_sidecars instead of the row.
    metadata: Dict[str, Any] = {}
    for col, key in (
        ("item_title", "title"),
        ("item_description", "description"),
        ("item_keywords", "keywords"),
    ):
        val = row.get(col)
        if val is not None:
            metadata[key] = val

    return build_canonical_envelope(
        identity=identity,
        properties=user_properties or {},
        known_fields=known_fields,
        reserved_members={"geometry": geometry, "bbox": bbox},
        metadata=metadata or None,
        system=system or None,
        stats=stats or None,
        access=access or None,
    )


__all__ = ["build_canonical_envelope", "build_canonical_index_doc"]
