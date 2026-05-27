"""
Elasticsearch index mappings for DynaStore STAC entities.

Design philosophy (post-#887):
  - **Items index** uses a strict three-tier known-fields shape (Tier 1 in
    code, Tier 2 per-catalog overlay, Tier 3 ``properties.extras`` dynamic
    lane). Tier-1 fields and the projection helper live in
    :mod:`.items_projection`; this module wires them into the ES mapping
    via :func:`build_item_mapping`.
  - **Catalog / collection / asset indexes** still rely on a small
    explicit-fields + dynamic-templates shape; tightening them is the
    final commit of the #887 series (parallel known-fields blocks for
    catalog / collection / asset metadata).
  - The previous platform-wide dynamic templates (per-language ``title`` /
    ``description`` generators, generic ``strings`` / ``numerics``
    catch-alls, ``proj:*`` specials) are retained for catalog / collection /
    asset until commit 3; the items factory drops them entirely.
"""
from typing import Any, Dict, List

from dynastore.modules.elasticsearch.items_projection import (
    LANGUAGE_ANALYZERS,
    build_known_fields,
)


def _localized_text_templates(field: str, ignore_above: int) -> List[Dict[str, Any]]:
    """Per-language dynamic templates for catalog / collection localized fields.

    Retained for non-items mappings (catalog / collection) where the
    field can appear at the top level OR nested under ``properties``.
    Items use the explicit ``_localized_text_field`` block from
    :mod:`.items_projection` instead and do not need these templates.
    """
    templates: List[Dict[str, Any]] = []
    for lang, analyzer in LANGUAGE_ANALYZERS.items():
        mapping: Dict[str, Any] = {
            "type": "text",
            "analyzer": analyzer,
            "fields": {
                "keyword": {"type": "keyword", "ignore_above": ignore_above},
            },
        }
        templates.append({
            f"{field}_{lang}_top": {
                "path_match": f"{field}.{lang}",
                "match_mapping_type": "string",
                "mapping": mapping,
            },
        })
        templates.append({
            f"{field}_{lang}_nested": {
                "path_match": f"*.{field}.{lang}",
                "match_mapping_type": "string",
                "mapping": mapping,
            },
        })
    templates.append({
        f"{field}s": {
            "path_match": f"*.{field}",
            "match_mapping_type": "string",
            "mapping": {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword", "ignore_above": ignore_above}},
            },
        },
    })
    return templates


# ---------------------------------------------------------------------------
# Dynamic templates retained for catalog / collection / asset indexes.
# The items mapping no longer uses these — see ITEM_MAPPING below.
# ---------------------------------------------------------------------------

DYNAMIC_TEMPLATES: List[Dict[str, Any]] = [
    *_localized_text_templates("title", ignore_above=512),
    *_localized_text_templates("description", ignore_above=1024),
    {
        "keywords": {
            "match": "keywords",
            "match_mapping_type": "string",
            "mapping": {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword"}},
            },
        }
    },
    {
        "hrefs": {
            "match": "href",
            "mapping": {"type": "keyword", "index": False, "doc_values": False},
        }
    },
    {
        "strings": {
            "match_mapping_type": "string",
            "mapping": {
                "type": "keyword",
                "fields": {"text": {"type": "text", "analyzer": "standard"}},
            },
        }
    },
    {
        "numerics": {
            "match_mapping_type": "long",
            "mapping": {"type": "float"},
        }
    },
]


# ---------------------------------------------------------------------------
# Common top-level fields. Extended with the internal ``_*`` write-time
# trackers attached by ItemsElasticsearchDriver.write_entities so the
# strict items root mapping accepts them.
# ---------------------------------------------------------------------------

COMMON_PROPERTIES: Dict[str, Any] = {
    # STAC mandatory identifiers & type flags
    "id":              {"type": "keyword"},
    "catalog_id":      {"type": "keyword"},
    "collection_id":   {"type": "keyword"},
    # STAC Item documents use the field name ``collection`` (not
    # ``collection_id``) — and that's the field both /search and the
    # ``items_es_ops`` term-filter target. Without an explicit keyword
    # mapping it falls back to dynamic-detected ``text``, against which
    # ``term``/``terms`` queries silently miss every exact value.
    "collection":      {"type": "keyword"},
    "type":            {"type": "keyword"},
    "stac_version":    {"type": "keyword"},
    "stac_extensions": {"type": "keyword"},
    # Links array — not searched, only returned; suppress indexing
    "links":           {"type": "object", "enabled": False},
    # Assets object — suppressed at root; indexed separately in 'assets' index
    "assets":          {"type": "object", "enabled": False},
    # Platform identifier mirrored at the doc root (also under properties).
    "geoid":           {"type": "keyword"},
    # Internal write-time trackers attached by ItemsElasticsearchDriver.
    # Required at root so the strict ``dynamic: false`` items mapping does
    # not reject the doc when the driver writes them.
    "_asset_id":              {"type": "keyword"},
    "_external_id":           {"type": "keyword"},
    "_valid_from":            {"type": "date"},
    "_valid_to":              {"type": "date"},
    "_simplification_factor": {"type": "float"},
    "_simplification_mode":   {"type": "keyword"},
    # Analyzed catch-all populated at write time from ``properties.extras``
    # values (see ``items_projection._flatten_extras_for_search``). Pairs
    # with the ``flattened`` extras lane to give the unknown-property tail
    # one analyzed-fulltext field plus one exact-per-key filter field —
    # two mapping entries total no matter how many distinct extension
    # keys arrive across the collections sharing this per-catalog index,
    # keeping the 1000-field index cap predictable (#1295).
    "_search_text":           {"type": "text", "analyzer": "standard"},
}

# STAC standard datetime fields shared with non-items entity types.
STAC_DATETIME_FIELDS: Dict[str, Any] = {
    "properties": {
        "properties": {
            "datetime":       {"type": "date"},
            "start_datetime": {"type": "date"},
            "end_datetime":   {"type": "date"},
            "created":        {"type": "date"},
            "updated":        {"type": "date"},
        }
    }
}

# ---------------------------------------------------------------------------
# Index mappings per entity type
# ---------------------------------------------------------------------------

CATALOG_MAPPING: Dict[str, Any] = {
    "dynamic": True,
    "dynamic_templates": DYNAMIC_TEMPLATES,
    "numeric_detection": False,
    "properties": {
        **COMMON_PROPERTIES,
        "created": {"type": "date"},
        "updated": {"type": "date"},
    },
}

COLLECTION_MAPPING: Dict[str, Any] = {
    "dynamic": True,
    "dynamic_templates": DYNAMIC_TEMPLATES,
    "numeric_detection": False,
    "properties": {
        **COMMON_PROPERTIES,
        "created": {"type": "date"},
        "updated": {"type": "date"},
        "extent": {
            "properties": {
                "spatial": {
                    "properties": {
                        "bbox": {"type": "float"},
                    }
                },
                "temporal": {
                    "properties": {
                        "interval": {"type": "object"},
                    }
                },
            }
        },
    },
}


def build_item_mapping(known_fields: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Build the strict items mapping for a catalog given its known-fields map.

    Shape:

    * ``dynamic: false`` at the root — only fields in
      :data:`COMMON_PROPERTIES` (plus ``geometry`` and ``bbox``) are
      accepted at the top level of the doc.
    * ``properties.dynamic = false`` — only keys in ``known_fields``
      survive as first-class typed paths; everything else must arrive
      under ``properties.extras``.
    * ``properties.extras`` is a ``flattened`` field — the entire bucket
      counts as **one** mapping entry regardless of how many distinct
      leaf keys arrive across the collections sharing this per-catalog
      index, capping field growth (#1295). ``flattened`` leaves are
      exact-match (``keyword``-semantics) only; analyzed full-text on
      the unknown tail rides on the root ``_search_text`` field, which
      :func:`items_projection.project_item_for_es` populates from the
      same extras values at write time.

    The projection helper (``items_projection.project_item_for_es``)
    enforces the shape at write time; ES enforces it at the mapping
    boundary. Both must use the same ``known_fields`` map for a given
    index — guaranteed because both ``ensure_storage`` and every write
    call route through :func:`build_known_fields`.
    """
    return {
        "dynamic": False,
        "properties": {
            **COMMON_PROPERTIES,
            "geometry": {"type": "geo_shape"},
            "bbox": {"type": "float"},
            "properties": {
                "dynamic": False,
                "properties": {
                    **known_fields,
                    "extras": {"type": "flattened"},
                },
            },
        },
    }


# Default items mapping (Tier 1 only) — used by call sites that do not
# resolve a per-catalog Tier-2 overlay. ``ensure_storage`` may switch to
# ``build_item_mapping(build_known_fields(cfg))`` once Tier 2 lands so
# it picks up the operator overlay.
ITEM_MAPPING: Dict[str, Any] = build_item_mapping(build_known_fields())


# Just the new top-level fields a cap-safe items index needs that an
# old ``object``-dynamic-extras index won't have. ``ensure_storage``
# patches an existing index with these via ``put_mapping`` (ES allows
# adding new fields to a live mapping). The ``extras`` field itself
# cannot be retyped from ``object`` to ``flattened`` in place — that
# needs a reindex, tracked as a separate follow-up to #1295.
ITEMS_INDEX_CAP_SAFE_MAPPING_PATCH: Dict[str, Any] = {
    "properties": {
        "_search_text": COMMON_PROPERTIES["_search_text"],
    },
}

ASSET_MAPPING: Dict[str, Any] = {
    "dynamic": True,
    "dynamic_templates": DYNAMIC_TEMPLATES,
    "numeric_detection": False,
    "properties": {
        "asset_id":      {"type": "keyword"},
        "catalog_id":    {"type": "keyword"},
        "collection_id": {"type": "keyword"},
        "item_id":       {"type": "keyword"},
        "asset_type":    {"type": "keyword"},
        "uri":           {"type": "keyword", "index": False, "doc_values": False},
        "owned_by":      {"type": "keyword"},
        "created_at":    {"type": "date"},
        "deleted_at":    {"type": "date"},
        # metadata is dynamic — tightened in commit 3 of the #887 series.
        "metadata":      {"type": "object", "dynamic": True},
    },
}


# ---------------------------------------------------------------------------
# Registry helpers
# ---------------------------------------------------------------------------

MAPPINGS: Dict[str, Dict[str, Any]] = {
    "catalog":    CATALOG_MAPPING,
    "collection": COLLECTION_MAPPING,
    "item":       ITEM_MAPPING,
    "asset":      ASSET_MAPPING,
}


def get_mapping(entity_type: str) -> Dict[str, Any]:
    """Return the Elasticsearch mapping for the given STAC entity type."""
    return MAPPINGS.get(entity_type, ITEM_MAPPING)


def get_index_name(prefix: str, entity_type: str) -> str:
    """Return the index name for the given entity type and prefix."""
    return f"{prefix}-{entity_type}s"


def get_all_index_names(prefix: str) -> List[Dict[str, Any]]:
    """Return all index names with their mappings — useful for bootstrapping."""
    return [
        {"name": get_index_name(prefix, entity_type), "mapping": mapping}
        for entity_type, mapping in MAPPINGS.items()
    ]


def get_tenant_items_index(prefix: str, catalog_id: str) -> str:
    """Per-catalog public items index. Owned by ``ItemsElasticsearchDriver``."""
    return f"{prefix}-{catalog_id}-items"


def get_public_items_alias(prefix: str) -> str:
    """Platform-wide alias spanning all per-catalog public items indexes."""
    return f"{prefix}-items"


def get_assets_index_name(prefix: str, catalog_id: str) -> str:
    """Return the name of the assets index for a catalog."""
    return f"{prefix}-{catalog_id}-assets"



def get_log_index_name(prefix: str) -> str:
    """Return the name of the logs index."""
    return f"{prefix}-logs"


LOG_MAPPING: Dict[str, Any] = {
    "dynamic": False,
    "properties": {
        "id": {"type": "keyword"},
        "catalog_id": {"type": "keyword"},
        "collection_id": {"type": "keyword"},
        "event_type": {"type": "keyword"},
        "level": {"type": "keyword"},
        "is_system": {"type": "boolean"},
        "message": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
        "timestamp": {"type": "date"},
    },
}
