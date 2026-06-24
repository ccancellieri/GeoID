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


def _localized_keyword_field(ignore_above: int = 256) -> Dict[str, Any]:
    """Localized keyword-array field — one ``keyword`` sub-property per locale.

    Each locale value is an array of exact tokens (ES treats multi-values on
    a keyword field as an array natively).  A ``.text`` analyzed sub-field on
    each locale gives full-text search on the same data.  ``dynamic: false``
    on the parent blocks unknown locales, matching the same guard in
    :func:`~dynastore.modules.elasticsearch.items_projection._localized_text_field`.

    Used by the canonical ``metadata.keywords`` mapping (refs #1828).
    """
    return {
        "type": "object",
        "dynamic": False,
        "properties": {
            lang: {
                "type": "keyword",
                "ignore_above": ignore_above,
                "fields": {"text": {"type": "text", "analyzer": analyzer}},
            }
            for lang, analyzer in LANGUAGE_ANALYZERS.items()
        },
    }


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
            # keywords: per-language array of exact tokens (i18n write path).
            # The i18n layer wraps keywords as {"en": ["a", "b"], ...} before
            # indexing, so each locale sub-property must be keyword (not a
            # flat keyword at the parent level), otherwise ES rejects the
            # object value with mapper_parsing_exception (refs #1828).
            "keywords":    _localized_keyword_field(ignore_above=256),
        },
    }


_METADATA_CONTAINER: Dict[str, Any] = _build_metadata_container()

# geometry_simplification typed nested object inside the ``system`` container
# (refs #1828 Phase 2).  All write paths (public write_entities/index/index_bulk,
# private write_entities/index/index_bulk/PrivateIndexTask, envelope driver) now
# use the canonical ``system.geometry_simplification`` container.  The old flat
# ``_simplification_factor`` / ``_simplification_mode`` root entries in
# COMMON_PROPERTIES are retained solely so old docs (written before the #1828
# Phase 2 cutover) remain parseable; new writes never emit them.
_SYSTEM_GEOMETRY_SIMPLIFICATION: Dict[str, Any] = {
    "dynamic": False,
    "properties": {
        "factor": {"type": "float"},
        "mode":   {"type": "keyword"},
    },
}

# ---------------------------------------------------------------------------
# Canonical container type SSOT (refs #1285).
#
# The ``system`` and ``stats`` lanes carry a BOUNDED, well-known vocabulary, so
# the strict items mapping declares them UNCONDITIONALLY — unlike ``properties``
# (whose per-key growth is capped by the ``extras`` flattened lane), a fixed set
# of names can never explode the index. Declaring them on every per-catalog
# index — regardless of which collections it currently hosts — is what makes
# every system/stats field queryable AND uniformly typed across the
# cross-catalog ``/search`` alias: a field cannot be a ``double`` in one member
# index and a ``keyword`` in another, because the canonical type below always
# wins over any per-collection (queryable- or Tier-2-derived) override.
#
# Types are pinned to the values the write path actually emits
# (``canonical_doc.build_canonical_index_doc`` + the PG geometries sidecar):
#   * content-hash strings  -> keyword
#   * lifecycle timestamps   -> date ; validity window -> date_range
#   * scalar geometry stats  -> double ; integer counts -> long
#   * centroid ([x, y] WGS84 pair)    -> geo_point (``ignore_malformed`` so a
#       stray projected-SRID coordinate is kept in ``_source`` but never rejects
#       the doc — the canonical ES envelope is WGS84, matching the root
#       ``geometry``/``bbox``)
#   * bbox ([minx, miny, maxx, maxy]) -> float (mirrors the root ``bbox`` type)
#
# NOTE: the three IDENTITY axes (``geoid`` / ``external_id`` / ``asset_id``)
# are NOT in this container — per ``classify_container`` they live FLAT at the
# document root and are declared (and indexed as keyword) in COMMON_PROPERTIES.
# The read-side resolvers (``resolve_es_field_path`` / ``build_es_field_mapping``
# / ``parse_sort``) route identity filters/sorts to that flat root path, so the
# root declaration is what makes them queryable. ``system`` carries only the
# lifecycle + content-hash vocabulary below.
CANONICAL_SYSTEM_TYPES: Dict[str, Any] = {
    "geometry_hash":    {"type": "keyword"},
    "attributes_hash":  {"type": "keyword"},
    "validity":         {"type": "date_range"},
    "transaction_time": {"type": "date"},
    "deleted_at":       {"type": "date"},
}

# Only the fixed-name geometry stats whose emitted shape is verified are pinned
# here. Dynamically-named spatial cells (``s2_7``, ``h3_10``, ``geohash_6`` —
# resolution-dependent per collection) and 3D/temporal stats whose wire shape is
# not yet verified (``centroid_3d``, ``z_range``, ``temporal_duration``) are
# intentionally omitted: they are typed on demand from the collection queryables
# when present, and otherwise ride in ``_source`` (no type mismatch risk).
CANONICAL_STATS_TYPES: Dict[str, Any] = {
    "area":                    {"type": "double"},
    "volume":                  {"type": "double"},
    "perimeter":               {"type": "double"},
    "length":                  {"type": "double"},
    "circularity":             {"type": "double"},
    "convexity":               {"type": "double"},
    "aspect_ratio":            {"type": "double"},
    "surface_area":            {"type": "double"},
    "surface_to_volume_ratio": {"type": "double"},
    "net_floor_area":          {"type": "double"},
    "vertical_gradient":       {"type": "double"},
    "vertex_count":            {"type": "long"},
    "hole_count":              {"type": "long"},
    "centroid":                {"type": "geo_point", "ignore_malformed": True},
    # Z component of a 3D (POINTZ) centroid, split out from the 2D ``geo_point``
    # so a 3D source keeps its elevation as a queryable scalar (refs #2232).
    "centroid_z":              {"type": "double"},
    "bbox":                    {"type": "float"},
}

# ---------------------------------------------------------------------------
# Queryable advertisement (OGC API Part 3 / STAC Filter) — refs #2230
# ---------------------------------------------------------------------------


def _es_type_to_json_schema(es_def: Dict[str, Any]) -> Dict[str, Any]:
    """Map a canonical ES field type to a JSON-Schema queryable fragment."""
    t = es_def.get("type")
    if t in ("keyword", "text"):
        return {"type": "string"}
    if t == "date":
        return {"type": "string", "format": "date-time"}
    if t == "date_range":
        # JSON Schema has no native temporal-range type; advertise it as a
        # date-time string filterable with temporal operators.
        return {
            "type": "string",
            "format": "date-time",
            "description": "Temporal validity range; filter with temporal operators.",
        }
    if t in ("double", "float", "half_float", "scaled_float"):
        return {"type": "number"}
    if t in ("long", "integer", "short", "byte"):
        return {"type": "integer"}
    if t == "boolean":
        return {"type": "boolean"}
    if t in ("geo_point", "geo_shape"):
        return {"type": "object", "description": "Geo-point [longitude, latitude]."}
    return {"type": "string"}


def canonical_queryable_properties() -> Dict[str, Dict[str, Any]]:
    """JSON-Schema queryable fragments for the bounded canonical system/stats
    vocabulary that :func:`build_item_mapping` always emits (refs #2228).

    Keyed by the flat canonical field name (``area``, ``validity``,
    ``transaction_time`` …). The read resolver routes ``properties.<name>`` to
    its canonical ``stats.*`` / ``system.*`` ES path via ``classify_container``
    (see ``items_projection.resolve_es_field_path``), so advertising the flat
    name makes the field both discoverable and filterable. Driver-agnostic: any
    driver whose items live in the canonical ES index inherits these queryables
    (refs #2230, #1285).
    """
    props: Dict[str, Dict[str, Any]] = {}
    for name, es_def in {**CANONICAL_SYSTEM_TYPES, **CANONICAL_STATS_TYPES}.items():
        frag = _es_type_to_json_schema(es_def)
        frag.setdefault("title", name)
        props[name] = frag
    return props


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
    # Identity axes that live FLAT at the document root (refs #1285). The write
    # path mirrors these from the canonical envelope's identity section, and the
    # read-side resolvers route ``external_id`` / ``asset_id`` filters & sorts
    # here (see ``cql_to_es._ENVELOPE_FIELD_PATHS`` and ``resolve_es_field_path``
    # → identity branch). Without an explicit keyword mapping they fall under the
    # strict ``dynamic: false`` root, ride in ``_source`` un-indexed, and every
    # ``term``/``terms`` filter on them silently returns nothing — which is the
    # exact bug this declaration fixes. ``geoid`` is already declared below.
    "external_id":     {"type": "keyword"},
    "asset_id":        {"type": "keyword"},
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
    # ``_external_id`` / ``_asset_id`` were dropped in #1285 identity
    # convergence — identity lives only on the root ``external_id`` /
    # ``asset_id`` keywords, so the driver no longer writes the ``_``-mirrors.
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

    * ``system.geometry_hash`` / ``system.attributes_hash`` → ``keyword``
      (``system.validity`` → ``date_range``; identity ``external_id`` /
      ``asset_id`` are flat-root keyword fields, not system members)
    * ``system.transaction_time`` / ``system.deleted_at`` → ``date``
    * ``stats.area`` → ``double``
    * ``stats.centroid`` → ``geo_point`` (the geometries sidecar emits a
      ``[x, y]`` WGS84 coordinate pair via ``ARRAY[ST_X, ST_Y]``, which ES
      ingests natively as a geo_point; pinned with ``ignore_malformed`` in
      :data:`CANONICAL_STATS_TYPES` so a stray projected coordinate is kept in
      ``_source`` rather than rejecting the doc).
    * ``stats.s2_*`` / ``stats.h3_*`` / ``stats.geohash_*`` → ``keyword``

    NOTE: the canonical system/stats types are pinned in
    :data:`CANONICAL_SYSTEM_TYPES` / :data:`CANONICAL_STATS_TYPES` and win over
    whatever this function would derive for those well-known names; this mapping
    governs only the per-collection (queryable/Tier-2) fields the SSOT omits.
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
    * ``stats.dynamic = false`` (refs #1800/#1285) — typed nested object for
      geometry-derived statistics (``area``, ``centroid``, spatial cells).
      ALWAYS emitted: seeded from the bounded :data:`CANONICAL_STATS_TYPES`
      vocab, then extended with any extra stats names the ``known_fields`` map
      carries (e.g. resolution-dependent spatial cells).
    * ``system.dynamic = false`` (refs #1800/#1285) — typed nested object for
      lifecycle + content-hash fields (``geometry_hash``, ``attributes_hash``,
      ``validity``, ``transaction_time``, ``deleted_at``). ALWAYS emitted:
      seeded from the bounded :data:`CANONICAL_SYSTEM_TYPES` vocab so every
      system field is queryable and uniformly typed across the cross-catalog
      alias. The identity axes (``geoid``/``external_id``/``asset_id``) are NOT
      here — they are flat-at-root keyword fields in COMMON_PROPERTIES.

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
    # Seed the bounded canonical vocab so ``system`` and ``stats`` are ALWAYS
    # declared with their canonical types. Per-collection (queryable- or
    # Tier-2-derived) entries may add NEW names below, but ``setdefault`` means
    # they can never override a canonical type — this is what keeps the
    # cross-catalog ``/search`` alias uniformly typed (refs #1285). ``validity``
    # is seeded as ``date_range`` here, matching the driver-side conversion in
    # ``canonical_doc._validity_to_es_range`` (a raw tstzrange would be rejected).
    # Deep-ish copy: the leaf type dicts must NOT be shared with the module-level
    # CANONICAL_* SSOT (or with the cached ITEM_MAPPING constant), else a caller
    # that mutates a built mapping's leaf would corrupt the SSOT for every later
    # build. The leaves are flat one-level dicts, so a per-value ``dict(v)`` copy
    # is sufficient and cheap.
    stats_fields: Dict[str, Any] = {k: dict(v) for k, v in CANONICAL_STATS_TYPES.items()}
    system_fields: Dict[str, Any] = {k: dict(v) for k, v in CANONICAL_SYSTEM_TYPES.items()}

    for name, field_def in known_fields.items():
        container = _field_def_container(name, field_def)
        es_type = _field_def_es_type(field_def)
        if container == "stats":
            # Canonical wins; queryables only contribute names the SSOT omits
            # (e.g. resolution-dependent spatial cells ``s2_7`` / ``h3_10``).
            stats_fields.setdefault(name, es_type)
        elif container == "system":
            # Canonical (incl. ``validity`` as date_range) is already seeded, so
            # ``setdefault`` only ever adds NEW system names the SSOT omits.
            system_fields.setdefault(name, es_type)
        elif container in ("metadata", "identity"):
            # metadata: lands in the _METADATA_CONTAINER block below (emitted
            # statically with per-language analyzed sub-fields, not per-field).
            # identity (external_id / asset_id / geoid): seeded into the system
            # container above as the canonical searchable lane, and mirrored flat
            # at the doc root for the STAC/GeoJSON wire shape.
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

    # Both containers are ALWAYS emitted (the canonical seed guarantees they are
    # non-empty), so every per-catalog index — and the cross-catalog alias — can
    # query the full bounded system/stats vocabulary uniformly (refs #1285).
    # geometry_simplification is injected into the system container so the nested
    # structure is available for writes (refs #1828). The flat
    # ``_simplification_factor`` / ``_simplification_mode`` root entries in
    # COMMON_PROPERTIES are kept only for old-doc read-back compatibility.
    root_properties["stats"] = {
        "dynamic": False,
        "properties": stats_fields,
    }
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
