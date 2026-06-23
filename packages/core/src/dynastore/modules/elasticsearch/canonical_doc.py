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
from dynastore.modules.storage.computed_fields import (
    SYSTEM_FIELD_KEYS,
    _SYSTEM_ONLY_FIELD_NAMES,
)

_SYSTEM_KEYS: frozenset = frozenset(SYSTEM_FIELD_KEYS)


def _iso(value: Any) -> Any:
    """Render a temporal bound as an ES-parseable string.

    ``datetime``/``date`` objects become ISO-8601 (the ``date_range`` field's
    ``strict_date_optional_time`` accepts the offset form); anything else is
    passed through unchanged (already a string, or an epoch number).
    """
    iso = getattr(value, "isoformat", None)
    return iso() if callable(iso) else value


def _validity_to_es_range(value: Any) -> Optional[Dict[str, Any]]:
    """Convert a PG ``tstzrange``-like validity value into an ES ``date_range``.

    A PostgreSQL ``tstzrange`` surfaces (via asyncpg/psycopg2) as a Range-like
    object exposing ``lower``/``upper`` bounds and ``lower_inc``/``upper_inc``
    inclusivity flags. ES ``date_range`` accepts an object with ``gte``/``gt``
    (lower) and ``lte``/``lt`` (upper) bounds, so the inclusivity maps directly:
    an inclusive bound uses ``gte``/``lte``, an exclusive one ``gt``/``lt``. An
    open bound (``None``) is omitted — ES treats a missing bound as unbounded.

    Returns ``None`` when the value carries no bounds (fully-open window) or is
    otherwise unusable, so callers can drop the field entirely. An input that is
    already a mapping (pre-converted / idempotent re-index) is returned as-is.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return value or None
    # Range-like duck-type: a genuine tstzrange value (asyncpg.Range, etc.)
    # exposes the inclusivity flag ``lower_inc``. The flag is the discriminator
    # — a bare ``str`` carries ``.lower`` / ``.upper`` *methods* but no
    # ``lower_inc``, so this guard keeps a stray string from being mistaken for
    # a range. Anything that is neither a dict nor Range-like cannot be a valid
    # ``date_range`` body, so it is dropped rather than risk an ingest rejection.
    if not hasattr(value, "lower_inc"):
        return None
    lower = getattr(value, "lower", None)
    upper = getattr(value, "upper", None)
    if lower is None and upper is None:
        # Fully-open window: nothing to index.
        return None
    lower_inc = bool(getattr(value, "lower_inc", True))
    upper_inc = bool(getattr(value, "upper_inc", False))
    out: Dict[str, Any] = {}
    if lower is not None:
        out["gte" if lower_inc else "gt"] = _iso(lower)
    if upper is not None:
        out["lte" if upper_inc else "lt"] = _iso(upper)
    return out or None


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
      (``id``, ``catalog_id``, ``collection_id``, ``external_id``,
      ``asset_physical_id``, ``validity`` …). ``id`` is whatever stable
      identifier the level uses (``geoid`` for items, ``collection_id`` for
      collections, ``catalog_id`` for catalogs).  The mutable logical
      ``asset_id`` is NOT stored here — only the immutable physical UUID.
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
    stac_reserved_members: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Assemble the policy-independent canonical ES _source.

    Sections:
      - flat identity: id(=geoid), catalog_id, collection_id, external_id,
        asset_physical_id (immutable), validity
      - geometry, bbox
      - properties: user attrs, typed-known kept flat, unknown moved to
        properties.extras (via project_item_for_es — same reshape as the
        rest of the ES projection path)
      - stats: every producible computed value from resolved_sidecars NOT in
        SYSTEM_FIELD_KEYS (system wins the overlap)
      - system: the lifecycle SYSTEM_FIELD_KEYS that are NOT identity axes
        (content hashes / validity / timestamps belong here). The identity
        axes (geoid / external_id / asset_physical_id) live flat at the
        root, never duplicated into ``system`` (#1285 identity convergence).
      - access: pass-through when non-empty

    ``id`` is ALWAYS ``row["geoid"]``, regardless of any policy.
    Reserved STAC/GeoJSON members never leak into properties.

    ``stac_reserved_members`` carries per-item STAC content that lives only in
    the inbound feature and has no PG sidecar (ES-only storage).  Typical keys:
    ``assets``, ``stac_extensions``, ``extra_fields``.  These are merged into
    ``reserved_members`` so they survive the canonical write and are surfaced
    verbatim by ``unproject_item_from_es`` on read (``assets`` /
    ``stac_extensions`` are already in ``_RESERVED_MEMBER_KEYS``).

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
        # Single canonical home: the flat root ``external_id`` keyword. The
        # legacy ``_external_id`` mirror is no longer written — queries and the
        # read inverse address the root field directly (#1285 identity
        # convergence). The resolved-identity transport still rides on the
        # in-flight payload as ``_external_id`` (item_service stamps it from the
        # write policy's path), but it is not persisted into ``_source``.
        identity["external_id"] = str(external_id)

    if row.get("asset_id") is not None:
        # Stamp only the immutable join key under ``asset_physical_id``.
        # The logical asset_id (mutable, renamed freely) is intentionally
        # NOT written to ``_source``: visibility filters and the virtual-asset
        # view key on asset_physical_id so asset renames require no ES reindex.
        # ``row["asset_physical_id"]`` is the canonical key (column-renamed
        # from ``physical_id`` in #2296).  ``row["physical_id"]`` is kept as a
        # backward-compat fallback for callers that pre-date the rename;
        # otherwise ``row["asset_id"]`` holds the physical UUID because the PG
        # attributes sidecar already resolved and stored it there at write time.
        _raw_phys = row.get("asset_physical_id") or row.get("physical_id") or row["asset_id"]
        identity["asset_physical_id"] = str(_raw_phys)

    # validity is a PG tstzrange (Range-like object) — convert it once to the ES
    # date_range shape ({gte|gt, lte|lt}) so it is JSON-serializable and lands in
    # the typed ``system.validity`` date_range field (refs #1828). The same
    # converted value is mirrored at the document root for read-path parity.
    validity_range = _validity_to_es_range(row.get("validity"))
    if validity_range is not None:
        identity["validity"] = validity_range

    # system: the lifecycle SYSTEM_FIELD_KEYS that are NOT identity axes
    # (content hashes / validity / timestamps live here). The identity axes
    # (geoid / external_id / asset_physical_id) live flat at the document root —
    # ``classify_container`` routes them to "identity", and the strict ``system``
    # mapping never declared them, so the old ``system.{external_id,geoid}``
    # copies were unindexed ``_source`` duplicates. Building from
    # ``_SYSTEM_ONLY_FIELD_NAMES`` keeps ``_source`` aligned with the mapping
    # and the classifier (#1285 identity convergence). The expose_all read
    # rebuilds the wire ``system`` section from the PG row, not from this
    # container, so dropping the dups does not change any read response.
    system = {k: row[k] for k in _SYSTEM_ONLY_FIELD_NAMES if row.get(k) is not None}
    # validity is typed as ES date_range — store the converted range object
    # (or drop it when the window is fully open / unusable). Overriding the raw
    # Range here is what makes the date_range mapping ingestible (#1828).
    if "validity" in system:
        if validity_range is not None:
            system["validity"] = validity_range
        else:
            system.pop("validity")

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

    # stats.centroid is typed ``geo_point`` ([lon, lat]); a POINTZ-configured
    # centroid column yields a 3-element [x, y, z] array that ES geo_point
    # rejects (silently dropped by ``ignore_malformed``), so 3D items would
    # lose their indexed centroid. Project to 2D for the geo_point lane and
    # keep Z as the separate typed scalar ``stats.centroid_z`` (refs #2232).
    _centroid = stats.get("centroid")
    if isinstance(_centroid, (list, tuple)) and len(_centroid) >= 3:
        stats["centroid"] = [_centroid[0], _centroid[1]]
        if _centroid[2] is not None:
            stats["centroid_z"] = float(_centroid[2])

    # metadata: multilingual descriptive metadata from the ItemMetadataSidecar
    # (item_title / item_description / item_keywords JSONB columns). The sidecar
    # declares which canonical names it produces via producible_metadata_names()
    # and maps storage columns to values via resolve_metadata_value(). First
    # sidecar that claims a name wins (same rule as stats). This removes the
    # direct row-column read that bypassed the sidecar abstraction (refs #1838).
    metadata: Dict[str, Any] = {}
    for sidecar in resolved_sidecars:
        for name in sidecar.producible_metadata_names():
            if name in metadata:
                continue
            found, value = sidecar.resolve_metadata_value(row, name)
            if found and value is not None:
                metadata[name] = value

    reserved_members: Dict[str, Any] = {"geometry": geometry, "bbox": bbox}
    if stac_reserved_members:
        for k, v in stac_reserved_members.items():
            if v is not None:
                reserved_members[k] = v

    return build_canonical_envelope(
        identity=identity,
        properties=user_properties or {},
        known_fields=known_fields,
        reserved_members=reserved_members,
        metadata=metadata or None,
        system=system or None,
        stats=stats or None,
        access=access or None,
    )


__all__ = [
    "build_canonical_envelope",
    "build_canonical_index_doc",
    "_validity_to_es_range",
]
