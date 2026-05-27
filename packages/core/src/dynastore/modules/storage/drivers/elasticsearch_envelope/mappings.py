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
Elasticsearch index mapping + naming for the envelope items driver.

Driver-private — not imported by the platform :mod:`modules.elasticsearch.mappings`,
which holds only platform-wide STAC mappings (catalog/collection/item/asset).

Stores the full feature (geometry + properties + identity) in a single index
per tenant (catalog), plus a canonical *access envelope* (``visibility`` /
``owner`` / ``grant_subjects``) carried as typed root keyword fields. The
access fields are intentionally part of the STATIC mapping (root
``dynamic: false``) — never dynamically inferred — so that a row-level access
filter built from them is reliable: a field that is sometimes absent from the
mapping (because no document happened to populate it yet, or because a
mis-typed value was indexed dynamically) would make a ``terms`` filter
silently under- or mis-match, which for a security predicate is unacceptable.

Tenant attributes live under a ``properties`` sub-tree that stays
``dynamic: true`` so new attribute fields index without a mapping update.
"""

from __future__ import annotations

from typing import Any, Dict


# Index-level settings (``index.mapping.total_fields.limit``) live on
# :class:`ElasticsearchIndexConfig` and are fetched via
# :func:`get_private_items_index_settings` in
# :mod:`dynastore.modules.elasticsearch.index_config` (shared with the private
# items index — both are tenant-scoped feature indexes with the same field
# budget needs). Routed through the PluginConfig waterfall.


ENVELOPE_FEATURE_MAPPING: Dict[str, Any] = {
    "dynamic": False,  # reject unknown top-level fields (typos, smuggling)
    "properties": {
        # --- identity envelope (typed, never dynamic) ---
        "geoid":                 {"type": "keyword"},
        "catalog_id":            {"type": "keyword"},
        "collection_id":         {"type": "keyword"},
        "external_id":           {"type": "keyword"},
        "asset_id":              {"type": "keyword"},
        # --- access envelope (typed, never dynamic) ---
        # These three back the row-level ABAC ``terms`` predicates. They MUST
        # stay in the static mapping so a filter on them is deterministic.
        "visibility":            {"type": "keyword"},
        "owner":                 {"type": "keyword"},
        # ``grant_subjects`` is multi-valued (a keyword field in ES is
        # implicitly array-capable); a ``terms`` filter intersects it.
        # Retained in the mapping for read-tolerance on pre-#1441 docs;
        # new docs no longer populate this field.
        "grant_subjects":        {"type": "keyword"},
        # ``attrs`` holds per-document ABAC attributes stamped at write time
        # from the collection's AttributeStampingPolicy (#1441).  Sub-fields
        # are declared dynamic ``keyword`` so a new attribute key is
        # automatically mapped without a mapping update; the bounded
        # ``AttributeStampingPolicy.attribute_paths`` config prevents the
        # 1000-field explosion risk.
        "attrs": {
            "type": "object",
            "dynamic": True,
            "properties": {},  # sub-fields auto-mapped as keyword on first write
        },
        # --- geometry + simplification bookkeeping ---
        "geometry":              {"type": "geo_shape"},
        "bbox":                  {"type": "float"},
        "simplification_factor": {"type": "float"},
        "simplification_mode":   {"type": "keyword"},
        # Tenant attributes live under a dynamic sub-tree so new fields
        # are indexed without mapping updates.
        "properties":            {"type": "object", "dynamic": True},
    },
}


def get_envelope_index_name(prefix: str, catalog_id: str) -> str:
    """Per-catalog envelope items index ``{prefix}-{catalog_id}-envelope-items``.

    Owned by the envelope items driver; the platform never references this
    naming. Catalog-first naming mirrors ``get_private_index_name`` so all
    per-catalog indexes cluster lexicographically.
    """
    return f"{prefix}-{catalog_id}-envelope-items"
