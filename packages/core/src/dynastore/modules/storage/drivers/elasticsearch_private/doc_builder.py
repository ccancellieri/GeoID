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
Build a tenant-feature doc shaped for the canonical ES envelope.

Driver-private helper used by :class:`PrivateEntityTransformer`,
:class:`ItemsElasticsearchPrivateDriver` (write_entities, index, index_bulk),
and the ``PrivateIndexTask`` runner. Lives in the private subpackage so the
platform never imports it.

Post-#1800: ``build_tenant_feature_doc`` now produces the same canonical
envelope as the public items driver (``id``=geoid, ``properties`` user-only,
``stats`` / ``system`` containers). Private differs from public ONLY by
target index/alias. The old flat shape (``geoid`` at root, no stats/system)
is no longer emitted.

Post-#1839: the Feature/dict path carries through already-computed stat values
(s2/h3/geohash spatial-cell keys, area, centroid, etc.) that were produced on
the ingest path and ride flat on the source dict after ``map_row_to_feature``.
These are classified into ``stats{}`` via :func:`classify_container` (the SSOT
for container routing, refs #1800/#1828) instead of being silently dropped.
No recomputation is performed — only values already present are carried through.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def _collect_flat_stats(src: Dict[str, Any], exclude: "frozenset[str]") -> Dict[str, Any]:
    """Extract stat-classified keys from a flat source dict (carry-through only).

    Scans ``src`` top-level keys and any keys inside ``src["properties"]`` using
    :func:`~dynastore.modules.storage.computed_fields.classify_container`.
    A key classifies to ``"stats"`` when it matches a geometry-derived statistic
    base name (``area``, ``centroid``, …) or a spatial-cell resolved-name pattern
    (``s2_*``, ``h3_*``, ``geohash_*``).  Identity and system keys are excluded
    via ``exclude`` so ``system`` always wins the overlap (same rule as the PG
    sidecar path in ``build_canonical_index_doc``).

    Returns a dict of ``{name: value}`` suitable for merging into the canonical
    ``stats`` section.  Values that are ``None`` are skipped.

    This function performs **no recomputation** — it only carries values that
    already exist on the source.
    """
    from dynastore.modules.storage.computed_fields import classify_container

    stats: Dict[str, Any] = {}
    # Check top-level keys first (model_extra / map_row_to_feature flat output).
    for k, v in src.items():
        if k in exclude or v is None:
            continue
        if classify_container(k, None) == "stats":
            stats[k] = v
    # Also check properties bag — some callers serialise stats into properties.
    for k, v in (src.get("properties") or {}).items():
        if k in exclude or k in stats or v is None:
            continue
        if classify_container(k, None) == "stats":
            stats[k] = v
    return stats


def build_tenant_feature_doc(
    item: Any,
    *,
    catalog_id: str,
    collection_id: str,
    external_id: Any = None,
    asset_id: Any = None,
    known_fields: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a canonical-envelope doc from a Feature/dict or
    :class:`~dynastore.modules.catalog.canonical_index_read.CanonicalIndexInput`.

    Accepts:

    * A :class:`~dynastore.modules.catalog.canonical_index_read.CanonicalIndexInput`
      instance — the preferred path when the caller already has a batched PG
      read result. Delegates directly to
      :func:`~dynastore.modules.elasticsearch.canonical_doc.build_canonical_index_doc`.

    * A Feature pydantic model, a STAC item dict, or a GeoJSON Feature dict —
      adapts to a minimal ``row`` and calls ``build_canonical_index_doc``.
      Already-computed stat values (s2/h3/geohash spatial-cell keys, area,
      centroid, etc.) that ride flat on the source after ``map_row_to_feature``
      are carried through into ``stats{}`` via :func:`classify_container` so
      privatised items that legitimately have these values in PG do not lose
      them when written through the dict adapter.  No recomputation is done.

    The returned doc uses the canonical envelope:
    ``id``=geoid, ``catalog_id``, ``collection_id``, ``external_id``,
    ``_external_id`` tracker, ``geometry``, ``bbox``, ``properties``
    (user-only, no SYSTEM_FIELD_KEYS), ``stats`` (sidecar-derived or carried
    through from the source), ``system`` (SYSTEM_FIELD_KEYS present in the row).

    ``known_fields`` is passed to the underlying projection so undeclared
    property keys route to ``properties.extras`` when a strict overlay is
    active. Pass ``{}`` or omit for legacy fully-dynamic mode.

    ``asset_id`` is the ingestion-context asset identity (mirrors the
    public driver's ``_asset_id`` tracking field). It is projected into the
    canonical ``asset_id`` identity field.
    """
    from dynastore.modules.elasticsearch.canonical_doc import build_canonical_index_doc

    # --- CanonicalIndexInput fast path ---
    # When the caller already has the PG-fetched canonical input (e.g. the
    # test invariant and future callers that perform the batch read), delegate
    # straight to the canonical builder.
    try:
        from dynastore.modules.catalog.canonical_index_read import CanonicalIndexInput
        if isinstance(item, CanonicalIndexInput):
            doc = build_canonical_index_doc(
                item.row,
                resolved_sidecars=item.resolved_sidecars,
                known_fields=known_fields or {},
                catalog_id=catalog_id,
                collection_id=collection_id,
                geometry=item.geometry,
                bbox=item.bbox,
                user_properties=item.user_properties,
                access=item.access,
            )
            # The canonical builder writes ``id``=geoid; private queries
            # target the ``geoid`` root field (PRIVATE_ENVELOPE_FIELDS).
            # Write the alias so structural queries and the read inverse
            # (_private_source_to_feature) can address either name.
            if "id" in doc and "geoid" not in doc:
                doc["geoid"] = doc["id"]
            return doc
    except ImportError:
        pass

    # --- Feature / dict path ---
    # Extract raw src dict from whatever the caller supplied.
    # The CanonicalIndexInput path returned early above; item is a Feature,
    # STAC dict, or plain dict at this point.  We cast to Any to avoid
    # pyright narrowing confusion from the CanonicalIndexInput isinstance
    # check inside the try/except above.
    _item_any: Any = item
    src: Dict[str, Any]
    if hasattr(_item_any, "model_dump"):
        src = _item_any.model_dump(by_alias=True, exclude_none=True)
    elif isinstance(_item_any, dict):
        src = _item_any
    else:
        src = dict(_item_any)

    geoid = src.get("id") or src.get("geoid")
    raw_props = src.get("properties") or {}

    # Exclude SYSTEM_FIELD_KEYS from user_properties — they live in the
    # system container on the canonical doc.
    from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS
    _sys_keys = frozenset(SYSTEM_FIELD_KEYS)
    user_props = {k: v for k, v in raw_props.items() if k not in _sys_keys}

    # Resolve external_id: explicit arg wins; fall back to source's
    # _external_id tracker or properties.external_id.
    ext = (
        external_id
        if external_id is not None
        else (src.get("_external_id") or raw_props.get("external_id"))
    )

    # Resolve asset_id: explicit arg wins; fall back to source's _asset_id.
    aid = asset_id if asset_id is not None else src.get("_asset_id")

    # Build a minimal raw row that carries the identity fields the canonical
    # builder needs for the ``system`` section.  Sidecar-columnar stats values
    # are also passed when they appear at the top level (e.g. after a
    # map_row_to_feature call that keeps them flat).
    row: Dict[str, Any] = {"geoid": geoid}
    if ext is not None:
        row["external_id"] = str(ext)
    if aid is not None:
        row["asset_id"] = str(aid)
    # Carry any SYSTEM_FIELD_KEY columns that surfaced on the raw src dict
    # (e.g. bulk-reindex reads that keep geometry_hash / validity flat).
    for sk in _sys_keys:
        if sk in src and sk != "geoid":
            row[sk] = src[sk]
        elif sk in raw_props:
            row[sk] = raw_props[sk]

    geom = src.get("geometry")
    bbox_val = src.get("bbox")

    # Collect already-computed stat values that ride flat on the source dict
    # (s2_res*, h3_res*, geohash_*, area, centroid, …) and carry them through
    # into the canonical ``stats`` section.  System keys are excluded so the
    # ``system`` container always wins the overlap (#1585 round-trip constraint).
    # No recomputation is performed — this is carry-through only.
    from dynastore.modules.storage.computed_fields import _IDENTITY_FIELD_NAMES
    _stats_exclude = _sys_keys | _IDENTITY_FIELD_NAMES
    flat_stats = _collect_flat_stats(src, _stats_exclude)

    doc = build_canonical_index_doc(
        row,
        resolved_sidecars=[],
        known_fields=known_fields or {},
        catalog_id=catalog_id,
        collection_id=collection_id,
        geometry=geom if isinstance(geom, dict) else None,
        bbox=list(bbox_val) if bbox_val is not None else None,
        user_properties=user_props or None,
        access=None,
    )

    # Merge flat stats into the canonical stats section.  Existing stats (from
    # the sidecar path, if any) win; flat values only fill gaps.
    if flat_stats:
        existing_stats = doc.get("stats") or {}
        merged = {**flat_stats, **existing_stats}
        doc["stats"] = merged

    # The canonical builder writes ``id``=geoid; private queries target
    # the ``geoid`` root field (PRIVATE_ENVELOPE_FIELDS). Write the alias
    # so structural queries and the read inverse (_private_source_to_feature)
    # can address either name.
    if "id" in doc and "geoid" not in doc:
        doc["geoid"] = doc["id"]

    return doc
