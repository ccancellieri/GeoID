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
Elasticsearch index mapping + naming for the private items driver.

Driver-private — not imported by the platform :mod:`modules.elasticsearch.mappings`,
which holds only platform-wide STAC mappings (catalog/collection/item/asset).

Stores the full feature (geometry + properties + external_id) in a single
index per tenant (catalog). Access is gated by the DENY policy applied
when the private driver is active. Docs that would exceed the ES 10MB
per-doc limit are shrunk by ``simplify_to_fit``
(:mod:`dynastore.tools.geometry_simplify`), which records
``system.geometry_simplification.factor`` and ``.mode`` on the stored doc
(#1828 Phase 2; previously stored flat at the doc root).

Two index shapes are supported, selected per tenant by the operator via
:attr:`~dynastore.modules.storage.driver_config.ItemsElasticsearchPrivateDriverConfig.mapping`:

* **Legacy (default — empty overlay).** :data:`TENANT_FEATURE_MAPPING`:
  ``properties`` is a dynamic object so any tenant attribute indexes
  without mapping churn. Single-tenant indexes don't share the alias
  cap pressure of the public driver, so this stayed open.
* **Strict (non-empty overlay).** :func:`build_private_item_mapping`:
  the operator declares typed mappings for known fields; undeclared
  keys route to ``properties.extras`` (``flattened``) plus the root
  ``_search_text`` analyzed catch-all — the same cap-safe shape the
  public per-catalog index uses (#1295). Strict mode is defence in
  depth: the index can't blow the field cap even if a misbehaving
  collection sprays high-cardinality keys.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional


# Index-level settings (`index.mapping.total_fields.limit`) live on
# :class:`ElasticsearchIndexConfig` and are fetched via
# :func:`get_private_items_index_settings` in
# :mod:`dynastore.modules.elasticsearch.index_config`. Routed through the
# PluginConfig waterfall (`/configs/plugins/elasticsearch_index_config`).


# Reserved root-level fields of the tenant feature doc. Used by the
# strict-mode mapping builder so the operator overlay (which applies
# under ``properties.<key>``) never tries to redefine a root field
# (``geoid``, ``catalog_id``, ``geometry``, …). Kept in sync with the
# validator's ``_PRIVATE_RESERVED_ROOT_FIELDS`` in ``driver_config``.
_PRIVATE_RESERVED_ROOT_FIELDS = frozenset({
    "geoid",
    "catalog_id",
    "collection_id",
    "external_id",
    "asset_id",
    "geometry",
    "bbox",
    "properties",
    "extras",
    "system",
    "stats",
})


# GeoJSON / STAC top-level member names — never live inside
# ``properties`` on a tenant feature doc. Mirrors the public
# ``items_projection._RESERVED_MEMBER_KEYS`` set so the private
# projection helper drops the same structural leaks.
_RESERVED_MEMBER_KEYS = frozenset({
    "id",
    "type",
    "geometry",
    "bbox",
    "links",
    "assets",
    "collection",
    "stac_version",
    "stac_extensions",
})


def _tenant_root_fields() -> Dict[str, Any]:
    """Root-level field map shared by both private mapping shapes.

    Excludes ``properties``, ``stats``, and ``system`` — those slots are
    built per-shape below.

    Post-#1800: ``id`` is added as the canonical identity keyword (mirrors
    ``COMMON_PROPERTIES["id"]`` in the public items mapping). ``geoid`` is
    retained for backward compatibility with existing private-index queries.
    Internal write-time trackers (``_external_id``, ``_asset_id``, etc.)
    are also declared so the strict ``dynamic: false`` root does not reject
    docs that carry them.
    """
    return {
        # Canonical identity (post-#1800 — id == geoid in canonical shape).
        "id":                    {"type": "keyword"},
        # Kept for backward compat with existing private-index queries.
        "geoid":                 {"type": "keyword"},
        "catalog_id":            {"type": "keyword"},
        "collection_id":         {"type": "keyword"},
        "external_id":           {"type": "keyword"},
        "asset_id":              {"type": "keyword"},
        "geometry":              {"type": "geo_shape"},
        "bbox":                  {"type": "float"},
        # Write-time trackers (mirrors COMMON_PROPERTIES in the public mapping).
        # ``_external_id`` / ``_asset_id`` dropped in #1285 identity convergence
        # — identity lives only on the root ``external_id`` / ``asset_id``.
        "_valid_from":           {"type": "date"},
        "_valid_to":             {"type": "date"},
        # ``_simplification_factor`` / ``_simplification_mode`` removed in #1828
        # Phase 2 — simplification metadata is now stored under
        # ``system.geometry_simplification`` (see _PRIVATE_SYSTEM_FIELDS).
        # The back-compat read path in ``_private_source_to_feature`` still
        # accepts old docs that carry these flat root keys.
        "_search_text":          {"type": "text", "analyzer": "standard"},
    }


# Canonical stats container — geometry-derived statistics.
# Mirrors the ``stats`` nested object in the public items mapping (#1800).
_PRIVATE_STATS_FIELDS: Dict[str, Any] = {
    "area":     {"type": "double"},
    "centroid": {"type": "keyword"},  # WKB hex — not geo_point.
}

# Canonical system container — lifecycle SYSTEM_FIELD_KEYS only.
# Mirrors the ``system`` nested object in the public items mapping (#1800).
# The identity axes (geoid / external_id / asset_id) are NOT declared here —
# they live flat at the document root (declared in ``_tenant_root_fields``).
# Declaring them under ``system`` only encouraged the duplicate ``_source``
# copies that #1285 identity convergence removed.
_PRIVATE_SYSTEM_FIELDS: Dict[str, Any] = {
    "geometry_hash":    {"type": "keyword"},
    "attributes_hash":  {"type": "keyword"},
    # validity is the temporal window, typed as date_range to mirror the public
    # items mapping (#1828). The shared canonical_doc builder converts the PG
    # tstzrange Range object into the matching {gte|gt, lte|lt} range body.
    "validity":         {"type": "date_range"},
    "transaction_time": {"type": "date"},
    "deleted_at":       {"type": "date"},
    # Geometry simplification metadata (#1828 Phase 2).
    # Nested under system so it travels with other lifecycle fields rather
    # than polluting the doc root.
    "geometry_simplification": {
        "type": "object",
        "dynamic": False,
        "properties": {
            "factor": {"type": "float"},
            "mode":   {"type": "keyword"},
        },
    },
}

# Canonical nested containers for both mapping shapes.
_PRIVATE_CANONICAL_CONTAINERS: Dict[str, Any] = {
    "stats": {
        "dynamic": False,
        "properties": _PRIVATE_STATS_FIELDS,
    },
    "system": {
        "dynamic": False,
        "properties": _PRIVATE_SYSTEM_FIELDS,
    },
}


TENANT_FEATURE_MAPPING: Dict[str, Any] = {
    "dynamic": False,  # reject unknown top-level fields (typos, smuggling)
    "properties": {
        **_tenant_root_fields(),
        # Canonical containers (post-#1800).
        **_PRIVATE_CANONICAL_CONTAINERS,
        # Tenant attributes live under a dynamic sub-tree so new fields
        # are indexed without mapping updates.
        "properties":            {"type": "object", "dynamic": True},
    },
}


def build_private_item_mapping(
    overlay: Optional[Dict[str, Dict[str, Any]]],
) -> Dict[str, Any]:
    """Build the per-tenant private items mapping for a given overlay.

    Empty / None overlay → returns the legacy :data:`TENANT_FEATURE_MAPPING`
    shape (``properties`` is a dynamic object). This preserves backward
    compatibility for tenants that never set ``mapping`` on the private
    driver config.

    Non-empty overlay → returns the strict cap-safe shape:

    * ``dynamic: false`` at the root — only the reserved root fields are
      accepted at the top level of the doc.
    * ``properties.dynamic = false`` — only operator-declared keys
      survive as first-class typed paths; everything else must arrive
      under ``properties.extras``.
    * ``properties.extras`` is a ``flattened`` field — one mapping
      entry no matter how many distinct leaf keys arrive.
    * ``_search_text`` is a root ``text`` field carrying the analyzed
      view of the extras tail.
    * ``stats`` / ``system`` canonical nested objects (post-#1800) so
      geometry-derived statistics and identity/lifecycle fields are
      queryable at their canonical path regardless of which mapping shape
      is active.

    The projection helper (:func:`project_private_doc`) enforces the
    shape at write time; ES enforces it at the mapping boundary. Both
    must use the same overlay for a given index — guaranteed because
    both ``ensure_storage`` and every write call route through
    :func:`resolve_catalog_private_known_fields`.
    """
    if not overlay:
        return TENANT_FEATURE_MAPPING
    # Strip reserved root fields defensively — the validator already
    # rejects these at config-write time, but a hand-edited config
    # bypassing the validator shouldn't corrupt the doc-root mapping.
    typed: Dict[str, Any] = {
        k: v for k, v in overlay.items()
        if k not in _PRIVATE_RESERVED_ROOT_FIELDS
    }
    return {
        "dynamic": False,
        "properties": {
            **_tenant_root_fields(),
            # Canonical containers shared with both shapes.
            **_PRIVATE_CANONICAL_CONTAINERS,
            "properties": {
                "dynamic": False,
                "properties": {
                    **typed,
                    "extras": {"type": "flattened"},
                },
            },
        },
    }


async def resolve_catalog_private_known_fields(
    catalog_id: Optional[str],
) -> Dict[str, Dict[str, Any]]:
    """Async helper: fetch the per-catalog
    :class:`ItemsElasticsearchPrivateDriverConfig` and return its
    ``mapping`` overlay (empty dict when unset).

    Returns ``{}`` (legacy fully-dynamic mode) when:

    * ``catalog_id`` is ``None`` — caller is in alias/cross-catalog
      scope (the private driver has no aliases today, but the guard
      keeps the helper safe);
    * the configs protocol is not yet registered (cold boot, unit test);
    * the config fetch raises any exception (degrade-safe, never blocks
      writes).
    """
    if not catalog_id:
        return {}
    try:
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.storage.driver_config import (
            ItemsElasticsearchPrivateDriverConfig,
        )
        from dynastore.tools.discovery import get_protocol
    except Exception:
        return {}

    configs = get_protocol(ConfigsProtocol)
    if configs is None:
        return {}
    try:
        cfg = await configs.get_config(
            ItemsElasticsearchPrivateDriverConfig,
            catalog_id=catalog_id,
        )
    except Exception:
        return {}
    overlay = getattr(cfg, "mapping", None)
    if not isinstance(overlay, dict):
        return {}
    return dict(overlay)


def project_private_doc(
    doc: Dict[str, Any],
    known_fields: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Reshape a tenant feature doc for the strict-mode private mapping.

    No-op when ``known_fields`` is empty (legacy mode keeps the doc
    fully dynamic under ``properties``). When non-empty, undeclared
    ``properties.*`` keys move under ``properties.extras`` and the root
    ``_search_text`` field is populated with their flattened analyzed
    view — exactly mirroring the public
    :func:`~dynastore.modules.elasticsearch.items_projection.project_item_for_es`
    contract.

    Pure function; the input is not mutated. GeoJSON/STAC structural
    members that leaked under ``properties`` are quarantined into
    ``properties.extras.<member>`` rather than dropped — the canonical
    value still lives at the doc root, but the tenant's colliding
    attribute is preserved losslessly instead of silently discarded
    (#2642). An existing ``extras`` sub-bucket is merged forward so the
    projection is idempotent.
    """
    if not known_fields:
        return doc
    if not isinstance(doc, dict):
        return doc
    props = doc.get("properties")
    if not isinstance(props, dict):
        return doc

    new_props: Dict[str, Any] = {}
    extras: Dict[str, Any] = {}
    for k, v in props.items():
        if k in _RESERVED_MEMBER_KEYS:
            # Quarantine rather than drop: a tenant attribute whose key
            # collides with a GeoJSON/STAC structural member (e.g.
            # ``collection``) would shadow the doc root if left at
            # ``properties.<member>``. Route it into
            # ``properties.extras.<member>`` instead so the value
            # survives the write (#2642).
            extras[k] = v
            continue
        if k == "extras":
            if isinstance(v, dict):
                extras.update(v)
            continue
        if k in known_fields:
            new_props[k] = v
        else:
            extras[k] = v

    out = dict(doc)
    if extras:
        new_props["extras"] = extras
        search_text = _flatten_extras_for_search(extras)
        if search_text:
            out["_search_text"] = search_text
    out["properties"] = new_props
    return out


def _flatten_extras_for_search(extras: Dict[str, Any]) -> str:
    """Flatten an ``extras`` bucket to a single whitespace-joined string.

    Mirrors
    :func:`dynastore.modules.elasticsearch.items_projection._flatten_extras_for_search`
    — the strict private mapping uses the same ``_search_text`` field
    semantics as the public per-catalog index (#1295).
    """
    tokens: List[str] = []
    _collect_search_tokens(extras.values(), tokens)
    return " ".join(t for t in tokens if t)


def _collect_search_tokens(values: Iterable[Any], out: List[str]) -> None:
    for v in values:
        if v is None:
            continue
        if isinstance(v, str):
            out.append(v)
        elif isinstance(v, bool):
            # ``bool`` before ``int`` — ``bool`` is an ``int`` subclass.
            out.append("true" if v else "false")
        elif isinstance(v, (int, float)):
            out.append(str(v))
        elif isinstance(v, dict):
            try:
                out.append(json.dumps(v, separators=(",", ":"), ensure_ascii=False))
            except (TypeError, ValueError):
                _collect_search_tokens(v.values(), out)
        elif isinstance(v, (list, tuple)):
            try:
                out.append(json.dumps(list(v), separators=(",", ":"), ensure_ascii=False))
            except (TypeError, ValueError):
                _collect_search_tokens(v, out)
        else:
            out.append(str(v))


def get_private_index_name(prefix: str, catalog_id: str) -> str:
    """Per-catalog private items index. Owned by the private items
    driver; the platform never references this naming. Catalog-first
    naming mirrors ``get_tenant_items_index`` so all per-catalog indexes
    cluster lexicographically.
    """
    return f"{prefix}-{catalog_id}-private-items"
