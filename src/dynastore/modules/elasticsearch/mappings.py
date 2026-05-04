"""
Elasticsearch index mappings for DynaStore STAC entities.

Design philosophy:
  - Minimal explicit mappings – only STAC v1.1.0 standard fields that ES cannot
    auto-detect correctly (geometry, dates, geopoints) are explicitly typed.
  - Dynamic templates handle everything else (multilingual text fields, hrefs,
    custom extension fields like 'eo:*', 'sat:*', 'proj:*', etc.) generically
    without requiring per-field configuration.
  - Items, collections, and catalogs can freely add STAC extension fields; they
    will be indexed by the catch-all dynamic templates automatically.
"""
from typing import Any, Dict, List


# ISO 639-1 → built-in Elasticsearch analyzer name.
# Drives per-language stemming/tokenisation for title.{lang} / description.{lang}
# so search/sort behave correctly across the platform's supported locales.
# ``zh`` falls back to ``standard`` because ES has no built-in Chinese
# analyzer; deploy ICU or smartcn at the cluster level if better tokenisation
# is needed.
LANGUAGE_ANALYZERS: Dict[str, str] = {
    "en": "english",
    "fr": "french",
    "es": "spanish",
    "ru": "russian",
    "ar": "arabic",
    "it": "italian",
    "de": "german",
    "zh": "standard",
}


def _localized_text_templates(field: str, ignore_above: int) -> List[Dict[str, Any]]:
    """Per-language dynamic templates for a localized text field.

    For each supported locale, emit two templates so both shapes get the
    language-appropriate analyzer:

    - ``{field}.{lang}`` — top-level (catalogs/collections store ``title``
      / ``description`` directly on the document).
    - ``*.{field}.{lang}`` — nested (STAC items wrap them under
      ``properties``).

    Without the top-level template, catalog/collection ``title.en`` would
    fall through to the generic ``strings`` template and lose per-language
    stemming. ES ``path_match`` treats ``*`` as a path-segment wildcard, so
    a single pattern with ``*`` cannot match both shapes — both must be
    enumerated explicitly.

    The trailing catch-all ``*.{field}`` keeps backwards compatibility for
    documents that store an unlocalized string under a nested path. Top-
    level unlocalized strings keep falling through to the generic
    ``strings`` template (intentional — preserves existing behavior).
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
# Dynamic templates – applied in order, first match wins.
# These handle the common patterns found in any STAC document.
# ---------------------------------------------------------------------------

DYNAMIC_TEMPLATES: List[Dict[str, Any]] = [
    # --- Multilingual text fields (stored as objects with lang-code keys) ---
    # Per-locale analyzers for proper stemming, plus a fallback for unknown
    # / unlocalized values.
    *_localized_text_templates("title", ignore_above=512),
    *_localized_text_templates("description", ignore_above=1024),
    # --- Keywords (text + keyword for aggregations) ---
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
    # --- Suppress indexing of nested href/url fields (storage, not search) ---
    {
        "hrefs": {
            "match": "href",
            "mapping": {"type": "keyword", "index": False, "doc_values": False},
        }
    },
    # --- STAC extension: projection ---
    # proj:centroid → geo_point for distance queries
    {
        "proj_centroid": {
            "match": "proj:centroid",
            "mapping": {"type": "geo_point"},
        }
    },
    # proj:geometry / proj:projjson are complex objects, skip indexing
    {
        "proj_complex": {
            "match_pattern": "regex",
            "match": "proj:(projjson|geometry|bbox)",
            "mapping": {"type": "object", "enabled": False},
        }
    },
    # proj:epsg → integer
    {
        "proj_epsg": {
            "match": "proj:epsg",
            "mapping": {"type": "integer"},
        }
    },
    # --- Generic catch-all rules (must be last) ---
    # All other strings → keyword (good for filtering/aggregation)
    {
        "strings": {
            "match_mapping_type": "string",
            "mapping": {
                "type": "keyword",
                # .text sub-field for full-text search when needed
                "fields": {"text": {"type": "text", "analyzer": "standard"}},
            },
        }
    },
    # long integers → float for numeric range queries
    {
        "numerics": {
            "match_mapping_type": "long",
            "mapping": {"type": "float"},
        }
    },
]


# ---------------------------------------------------------------------------
# Explicit field mappings – only fields ES cannot auto-detect correctly.
# ---------------------------------------------------------------------------

# Fields common to all four entity types (item, collection, catalog, asset)
COMMON_PROPERTIES: Dict[str, Any] = {
    # STAC mandatory identifiers & type flags
    "id":           {"type": "keyword"},
    "catalog_id":   {"type": "keyword"},
    "collection_id": {"type": "keyword"},
    # STAC Item documents use the field name `collection` (not
    # `collection_id`) — and that's the field both /search and the
    # `items_es_ops` term-filter target. Without an explicit keyword
    # mapping it falls back to dynamic-detected `text`, against which
    # `term`/`terms` queries silently miss every exact value.
    "collection":   {"type": "keyword"},
    "type":         {"type": "keyword"},
    "stac_version": {"type": "keyword"},
    "stac_extensions": {"type": "keyword"},
    # Links array – not searched, only returned; suppress indexing
    "links":        {"type": "object", "enabled": False},
    # Assets object – suppressed at root; indexed separately in 'assets' index
    "assets":       {"type": "object", "enabled": False},
}

# STAC standard datetime fields shared across all entity types
STAC_DATETIME_FIELDS: Dict[str, Any] = {
    "properties": {
        "properties": {
            # STAC Item Common Metadata datetimes (RFC 3339)
            "datetime":       {"type": "date"},
            "start_datetime": {"type": "date"},
            "end_datetime":   {"type": "date"},
            "created":        {"type": "date"},
            "updated":        {"type": "date"},
            # All other item properties are handled by dynamic templates
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
        # Catalog-level dates
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
        # Spatial/temporal extent – explicitly typed for geo queries
        "extent": {
            "properties": {
                "spatial": {
                    "properties": {
                        "bbox": {"type": "float"},
                    }
                },
                "temporal": {
                    "properties": {
                        "interval": {"type": "date_range"},
                    }
                },
            }
        },
    },
}

ITEM_MAPPING: Dict[str, Any] = {
    "dynamic": True,
    "dynamic_templates": DYNAMIC_TEMPLATES,
    "numeric_detection": False,
    "properties": {
        **COMMON_PROPERTIES,
        # STAC Item geometry (required, must be geo_shape)
        "geometry": {"type": "geo_shape"},
        # Bounding box
        "bbox": {"type": "float"},
        # All item properties are dynamic; only dates need explicit typing
        **STAC_DATETIME_FIELDS,
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
        # Forward-compat for item-embedded asset promotion
        # (ItemAssetIndexer marker, no implementer in this PR).  Asset
        # docs sourced from item-embedded assets will populate item_id;
        # standalone catalog/collection assets leave it unset.
        "item_id":       {"type": "keyword"},
        "asset_type":    {"type": "keyword"},
        "uri":           {"type": "keyword", "index": False, "doc_values": False},
        "owned_by":      {"type": "keyword"},
        "created_at":    {"type": "date"},
        "deleted_at":    {"type": "date"},
        # metadata is dynamic — nested fields (e.g. metadata.license_id)
        # are auto-mapped by dynamic_templates (strings → keyword, longs → float)
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
    """Return all index names with their mappings – useful for bootstrapping."""
    return [
        {"name": get_index_name(prefix, entity_type), "mapping": mapping}
        for entity_type, mapping in MAPPINGS.items()
    ]


def get_tenant_items_index(prefix: str, catalog_id: str) -> str:
    """Per-catalog public items index. Owned by ``ItemsElasticsearchDriver``.

    Catalog-first naming so per-catalog indexes sort lexicographically next
    to each other in tooling (``dynastore-{cat}-items``,
    ``dynastore-{cat}-private-items``, ``dynastore-{cat}-assets``).
    """
    return f"{prefix}-{catalog_id}-items"


def get_public_items_alias(prefix: str) -> str:
    """Platform-wide alias spanning all per-catalog public items indexes.

    Used by OGC discovery search routes. Membership is managed by the
    items driver's ``ensure_storage`` (add) and routing-config
    apply-handler (remove on driver removal). Private items indexes
    are intentionally **not** members of this alias.
    """
    return f"{prefix}-items"


def get_assets_index_name(prefix: str, catalog_id: str) -> str:
    """Return the name of the assets index for a catalog."""
    return f"{prefix}-{catalog_id}-assets"


def get_search_index(
    prefix: str,
    entity_type: str,
    catalog_id: str | None = None,
) -> str:
    """Resolve the read-side index for the search service.

    - ``item``       → per-catalog ``{prefix}-{cat}-items`` when scoped, else
                       the public alias ``{prefix}-items``.
    - ``collection`` → singleton ``{prefix}-collections``. Scoped queries
                       filter via ``_routing=catalog_id`` rather than
                       targeting a different physical index.
    - ``catalog``    → singleton ``{prefix}-catalogs``.
    """
    if entity_type == "item":
        return (
            get_tenant_items_index(prefix, catalog_id)
            if catalog_id
            else get_public_items_alias(prefix)
        )
    return get_index_name(prefix, entity_type)


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
