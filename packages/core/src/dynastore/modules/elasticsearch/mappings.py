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
  - Post-#1800: ``build_item_mapping`` also emits nested ``stats``
    (geometry-derived statistics) and ``system`` (identity/lifecycle)
    containers when the incoming ``known_fields`` map carries
    :class:`~dynastore.models.protocols.field_definition.FieldDefinition`
    values tagged with the corresponding ``container``. Both containers are
    ``dynamic: false``; their ES types are pinned. Tier-1 plain-dict
    entries are unaffected (they carry no ``container`` attribute and
    continue to land in ``properties``).
"""
from typing import Any, Dict, List

from dynastore.modules.elasticsearch.items_projection import (
    LANGUAGE_ANALYZERS,
    _localized_text_field,
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
# Metadata container — typed, dynamic:false, shared across item/collection/
# catalog builders (refs #1828). Multilingual title/description/keywords with
# per-language analyzed text sub-fields via the existing _localized_text_field
# helper from items_projection.  Keywords are an array of keyword-exact tokens
# plus a .text analyzed sub-field for full-text on the same field.
# ---------------------------------------------------------------------------

def _build_metadata_container() -> Dict[str, Any]:
    """Return the canonical ``metadata`` mapping block (dynamic:false).

    Shared by item, collection, and catalog builders so they all emit the
    same typed metadata container. Language set is pinned to
    ``LANGUAGE_ANALYZERS`` (en/fr/es/ru/ar/it/de/zh) — unknown locales are
    blocked by the per-field ``dynamic: false`` on the parent object, matching
    the same guard in ``_localized_text_field`` (refs #1828).
    """
    return {
        "dynamic": False,
        "properties": {
            "title":       _localized_text_field(ignore_above=512),
            "description": _localized_text_field(ignore_above=1024),
            # keywords: array of exact tokens; .text sub-field for full-text.
            "keywords": {
                "type": "keyword",
                "fields": {"text": {"type": "text", "analyzer": "standard"}},
            },
        },
    }


_METADATA_CONTAINER: Dict[str, Any] = _build_metadata_container()

# geometry_simplification typed nested object inside the ``system`` container
# (refs #1828).  Drivers currently write the flat ``_simplification_factor`` /
# ``_simplification_mode`` root-level trackers (see COMMON_PROPERTIES below).
# Those flat fields are KEPT for backward compatibility until drivers migrate in
# Phase 2.  The typed nested version is added here so new writes can begin
# populating ``system.geometry_simplification`` without breaking old reads.
_SYSTEM_GEOMETRY_SIMPLIFICATION: Dict[str, Any] = {
    "dynamic": False,
    "properties": {
        "factor": {"type": "float"},
        "mode":   {"type": "keyword"},
    },
}

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

# ``CATALOG_MAPPING`` is assembled by :func:`build_catalog_mapping` (defined
# below) once the shared container helpers exist — same canonical shape as
# collections, minus the spatial/temporal extent.

# ``COLLECTION_MAPPING`` is assembled by :func:`build_collection_mapping`
# (defined below, after the shared container helpers) and assigned once those
# helpers exist. The canonical collection envelope (#1285/#1800) replaced the
# previous ``dynamic: true`` + dynamic-templates shape, whose per-key field
# growth let leaked item attributes poison the singleton collections index.


def _field_def_container(name: str, field_def: Any) -> str:
    """Return the container classification for a known-field entry.

    Supports both plain ES-type dicts (Tier-1 backward compat) and
    :class:`~dynastore.models.protocols.field_definition.FieldDefinition`
    instances carrying a ``container`` tag. Plain dicts have no container
    attribute and always land in ``properties``. FieldDefinition values are
    routed through :func:`~dynastore.modules.storage.computed_fields.classify_container`
    so the single classification SSOT is respected (refs #1800).
    """
    if not hasattr(field_def, "container"):
        # Plain dict (Tier-1 raw ES type entry) — always properties lane.
        return "properties"
    # FieldDefinition with a container tag: use the classifier SSOT.
    from dynastore.modules.storage.computed_fields import classify_container
    return classify_container(name, field_def)


def _field_def_es_type(field_def: Any) -> Dict[str, Any]:
    """Derive the ES type mapping fragment for a FieldDefinition or plain dict.

    Plain dicts (Tier-1) are returned unchanged.  FieldDefinition values are
    converted from the canonical ``data_type`` token to an ES type.

    Pinned types for the canonical containers (refs #1800):

    * ``system.external_id`` / ``system.asset_id`` / ``system.geometry_hash``
      / ``system.attributes_hash`` / ``system.validity`` → ``keyword``
    * ``system.transaction_time`` / ``system.deleted_at`` → ``date``
    * ``stats.area`` → ``double``
    * ``stats.centroid`` → ``keyword`` (WKB hex — single sortable/filterable
      token; NOT geo_point: the WKB encoding is not a lat/lon pair, so ES
      would either reject it or silently misinterpret the bytes.  Clients
      that need point queries on centroid should index a separate geo_point
      projection; the keyword field is for exact CQL2 matches and sorting).
    * ``stats.s2_*`` / ``stats.h3_*`` / ``stats.geohash_*`` → ``keyword``
    """
    if not hasattr(field_def, "data_type"):
        # Plain dict — return as-is.
        return field_def  # type: ignore[return-value]
    dt = (getattr(field_def, "data_type", "") or "").lower()
    if dt in ("timestamp", "date", "time"):
        return {"type": "date"}
    if dt in ("double", "numeric", "float"):
        return {"type": "double"}
    if dt in ("integer", "bigint"):
        return {"type": "long"}
    if dt == "boolean":
        return {"type": "boolean"}
    # string / uuid / binary / unknown canonical → keyword (safe for all
    # system and stats fields; properties-lane known fields with complex
    # localized structure are plain dicts from Tier 1, not FieldDefinition).
    return {"type": "keyword"}


def build_item_mapping(known_fields: Dict[str, Any]) -> Dict[str, Any]:
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
    * ``stats.dynamic = false`` (new, refs #1800) — typed nested object for
      geometry-derived statistics (``area``, ``centroid``, spatial cells).
      Only present when the ``known_fields`` map carries at least one entry
      with ``container="stats"``.
    * ``system.dynamic = false`` (new, refs #1800) — typed nested object for
      identity + lifecycle fields (``geometry_hash``, ``attributes_hash``,
      ``validity``, ``transaction_time``, ``deleted_at``).
      Only present when ``known_fields`` carries a system-tagged entry.

    The projection helper (``items_projection.project_item_for_es``)
    enforces the shape at write time; ES enforces it at the mapping
    boundary. Both must use the same ``known_fields`` map for a given
    index — guaranteed because both ``ensure_storage`` and every write
    call route through :func:`build_known_fields`.
    """
    # Partition the known-fields map by container so each bucket ends up in
    # the right nested object.  Plain-dict Tier-1 entries carry no container
    # attribute and always route to ``properties``.
    props_fields: Dict[str, Any] = {}
    stats_fields: Dict[str, Any] = {}
    system_fields: Dict[str, Any] = {}

    for name, field_def in known_fields.items():
        container = _field_def_container(name, field_def)
        es_type = _field_def_es_type(field_def)
        if container == "stats":
            stats_fields[name] = es_type
        elif container == "system":
            # NOTE (#1828 Phase 2): the target is to type ``validity`` as a
            # ``date_range`` and write it as an ES range object ({"gte","lte"}).
            # That mapping and the driver-side conversion from the PG tstzrange
            # value MUST land together — typing date_range here while the raw
            # tstzrange value is still written into ``system.validity`` (validity
            # is in SYSTEM_FIELD_KEYS) would fail ingest. So validity keeps its
            # current es_type until the Phase 2 write conversion. TODO (#1828):
            # validity -> date_range + driver writes {"gte": lower, "lte": upper}.
            system_fields[name] = es_type
        elif container in ("metadata", "identity"):
            # metadata: lands in the _METADATA_CONTAINER block below (emitted
            # statically with per-language analyzed sub-fields, not per-field).
            # identity: declared in COMMON_PROPERTIES at the doc root; not
            # duplicated inside properties/stats/system.
            pass
        else:
            # Default: properties lane.
            props_fields[name] = es_type

    # Assemble the top-level mapping.
    root_properties: Dict[str, Any] = {
        **COMMON_PROPERTIES,
        "geometry": {"type": "geo_shape"},
        "bbox": {"type": "float"},
        # metadata: typed dynamic:false container for multilingual
        # title/description/keywords (refs #1828). Always emitted so the
        # mapping is ready to accept ItemMetadataSidecar writes.
        "metadata": _METADATA_CONTAINER,
        "properties": {
            "dynamic": False,
            "properties": {
                **props_fields,
                "extras": {"type": "flattened"},
            },
        },
    }

    # Emit the ``stats`` container only when there are tagged stats fields.
    if stats_fields:
        root_properties["stats"] = {
            "dynamic": False,
            "properties": stats_fields,
        }

    # Emit the ``system`` container only when there are tagged system fields.
    # Always inject geometry_simplification into the system container when
    # system is emitted, so the nested structure is available for new writes
    # without a mapping update (refs #1828). The flat ``_simplification_factor``
    # / ``_simplification_mode`` root entries in COMMON_PROPERTIES are kept for
    # backward compatibility until Phase 2 drivers migrate.
    if system_fields:
        root_properties["system"] = {
            "dynamic": False,
            "properties": {
                **system_fields,
                "geometry_simplification": _SYSTEM_GEOMETRY_SIMPLIFICATION,
            },
        }

    return {
        "dynamic": False,
        "properties": root_properties,
    }


# Default items mapping (Tier 1 only) — used by call sites that do not
# resolve a per-catalog Tier-2 overlay. ``ensure_storage`` may switch to
# ``build_item_mapping(build_known_fields(cfg))`` once Tier 2 lands so
# it picks up the operator overlay.
ITEM_MAPPING: Dict[str, Any] = build_item_mapping(build_known_fields())


def build_collection_mapping(known_fields: Dict[str, Any]) -> Dict[str, Any]:
    """Build the strict canonical collection mapping (refs #1285/#1800).

    Mirrors :func:`build_item_mapping` at the collection level:

    * ``dynamic: false`` at the root — undeclared members ride in ``_source``
      unindexed (kept for round-trip) instead of minting a new mapping field,
      closing the per-key growth that once poisoned the singleton collections
      index when item attributes leaked in.
    * attributes live under ``properties`` — ``known_fields`` typed flat,
      everything else under the ``extras`` ``flattened`` lane, paired with the
      analyzed ``_search_text`` catch-all.
    * lifecycle (``created``/``updated``) in a typed ``system`` container.
    * structural members (``links``/``assets``/``providers``/``summaries``/
      ``stac_extensions``/``extent``) are declared so they round-trip and the
      spatial envelope stays queryable; ``access`` is an opaque IAM sidecar.
    """
    props_fields: Dict[str, Any] = {}
    for name, field_def in known_fields.items():
        if _field_def_container(name, field_def) in ("stats", "system", "identity"):
            continue
        props_fields[name] = _field_def_es_type(field_def)

    root_properties: Dict[str, Any] = {
        "id":              {"type": "keyword"},
        "catalog_id":      {"type": "keyword"},
        "collection_id":   {"type": "keyword"},
        "type":            {"type": "keyword"},
        "stac_version":    {"type": "keyword"},
        "stac_extensions": {"type": "keyword"},
        # Structural members: returned, not searched — suppress indexing but
        # keep in _source so the read projector reconstructs them verbatim.
        "links":       {"type": "object", "enabled": False},
        "assets":      {"type": "object", "enabled": False},
        "item_assets": {"type": "object", "enabled": False},
        "providers":   {"type": "object", "enabled": False},
        "summaries":   {"type": "object", "enabled": False},
        "extent": {
            "properties": {
                "spatial": {
                    "properties": {
                        "bbox": {"type": "float"},
                        # Enriched envelope written by the driver's _enrich_doc.
                        "bbox_shape": {"type": "geo_shape"},
                    }
                },
                "temporal": {
                    "properties": {
                        "interval": {"type": "object"},
                    }
                },
            }
        },
        # metadata: typed dynamic:false container for multilingual
        # title/description/keywords (refs #1828).
        "metadata": _METADATA_CONTAINER,
        "properties": {
            "dynamic": False,
            "properties": {
                **props_fields,
                "extras": {"type": "flattened"},
            },
        },
        "system": {
            "dynamic": False,
            "properties": {
                "created": {"type": "date"},
                "updated": {"type": "date"},
            },
        },
        # IAM authorization sidecar — opaque, never queried at the wire.
        "access": {"type": "object", "enabled": False},
        # Soft-delete tracker (driver delete_metadata(soft=True)); must stay
        # indexed so search_metadata's must_not _deleted filter works under
        # the strict dynamic:false mapping.
        "_deleted": {"type": "boolean"},
        "_search_text": {"type": "text", "analyzer": "standard"},
    }
    return {"dynamic": False, "properties": root_properties}


# Canonical collections mapping (Tier 1 known attributes). Replaces the former
# ``dynamic: true`` dynamic-template shape.
COLLECTION_MAPPING: Dict[str, Any] = build_collection_mapping(build_known_fields())


def build_catalog_mapping(known_fields: Dict[str, Any]) -> Dict[str, Any]:
    """Build the strict canonical catalog mapping (refs #1285/#1800).

    The thinnest metadata entity: identity + ``type``/``stac_version`` keyword,
    attributes under ``properties`` (typed known + ``extras`` flattened lane +
    ``_search_text``), lifecycle in a typed ``system`` container, ``links``
    suppressed, ``access`` opaque. ``dynamic: false`` — undeclared members ride
    in ``_source`` unindexed (kept for round-trip), no per-key growth.
    """
    props_fields: Dict[str, Any] = {}
    for name, field_def in known_fields.items():
        if _field_def_container(name, field_def) in ("stats", "system", "identity"):
            continue
        props_fields[name] = _field_def_es_type(field_def)

    root_properties: Dict[str, Any] = {
        "id":              {"type": "keyword"},
        "catalog_id":      {"type": "keyword"},
        "type":            {"type": "keyword"},
        "stac_version":    {"type": "keyword"},
        "stac_extensions": {"type": "keyword"},
        "links":           {"type": "object", "enabled": False},
        # metadata: typed dynamic:false container for multilingual
        # title/description/keywords (refs #1828).
        "metadata": _METADATA_CONTAINER,
        "properties": {
            "dynamic": False,
            "properties": {
                **props_fields,
                "extras": {"type": "flattened"},
            },
        },
        "system": {
            "dynamic": False,
            "properties": {
                "created": {"type": "date"},
                "updated": {"type": "date"},
            },
        },
        "access": {"type": "object", "enabled": False},
        "_deleted": {"type": "boolean"},
        "_search_text": {"type": "text", "analyzer": "standard"},
    }
    return {"dynamic": False, "properties": root_properties}


# Canonical catalogs mapping. Replaces the former dynamic-template shape.
CATALOG_MAPPING: Dict[str, Any] = build_catalog_mapping(build_known_fields())


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
