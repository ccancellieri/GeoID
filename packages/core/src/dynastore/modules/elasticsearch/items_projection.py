"""
Per-catalog known-fields catalog for the public items index mapping.

Three-tier model:

* **Tier 1** — :data:`TIER_1_FIELDS` (this module): platform-wide,
  code-resident, invariant across every per-catalog items index. Identical
  Tier 1 across all alias members gives the platform-wide alias
  ``{prefix}-items`` a stable cross-catalog contract (term/range queries
  against Tier 1 paths resolve uniformly on every member).
* **Tier 2** — per-catalog operator overlay via
  ``ItemsElasticsearchDriverConfig.mapping.additional_known_fields``
  (introduced in the follow-up commit). Additive only: collisions with
  Tier 1 are rejected at config-validate time. Snapshot at index-create;
  live edits take effect on next index rebuild.
* **Tier 3** — ``properties.extras`` long-tail lane. Anything not in
  Tier 1 ∪ Tier 2 lands here. Mapped as a single ``flattened`` field —
  the whole bucket is one mapping entry regardless of how many distinct
  leaf keys arrive across the collections sharing the per-catalog
  index (#1295). ``flattened`` leaves are keyword-exact (good for
  per-key term/exists filters, no analysis); a sibling root field
  ``_search_text`` populated at write time from the same extras values
  carries the analyzed full-text view of the tail. Two mapping entries
  total for the unknown long tail; nothing per-leaf.

The module exposes a small, pure-Python API consumed by the items
driver write path and the search service sort path:

* :func:`build_known_fields` — resolve the effective known-fields map for
  a catalog (Tier 1 ∪ that catalog's Tier 2). v1 just returns Tier 1.
* :func:`project_item_for_es` — reshape an item doc so unknown properties
  move under ``properties.extras``. Called at every write entry point
  before the bulk-action append.
* :func:`resolve_es_field_path` — given an arbitrary STAC path, return
  the ES path it actually lives at (rewrites unknown ``properties.<key>``
  to ``properties.extras.<key>``). Used by the search sort path.

Future CQL2→ES translator (none exists today) must route field paths
through :func:`resolve_es_field_path` so unknown-extension filters
survive the reshape.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Iterable, List, Optional

from dynastore.tools.language_utils import resolve_localized_field

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Language analyzers — must match the locales the platform officially
# supports for ``title`` / ``description``. Kept in sync with the previous
# ``LANGUAGE_ANALYZERS`` map in ``mappings.py``.
# ---------------------------------------------------------------------------

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


def _localized_text_field(ignore_above: int) -> Dict[str, Any]:
    """Localized text field — one ``text`` sub-property per supported locale.

    Replaces the dynamic-template per-locale generation. Explicit per-locale
    sub-properties + ``dynamic: false`` on the parent block unknown
    locales (so a typo like ``title.eng`` is rejected at ingest under the
    strict mapping).
    """
    return {
        "type": "object",
        "dynamic": False,
        "properties": {
            lang: {
                "type": "text",
                "analyzer": analyzer,
                "fields": {
                    "keyword": {"type": "keyword", "ignore_above": ignore_above},
                },
            }
            for lang, analyzer in LANGUAGE_ANALYZERS.items()
        },
    }


# ---------------------------------------------------------------------------
# STAC core item-property fields. These are the STAC v1.1.0 Common
# Metadata properties plus the platform's geoid identifier.
# ---------------------------------------------------------------------------

_STAC_CORE_FIELDS: Dict[str, Dict[str, Any]] = {
    # RFC 3339 datetimes
    "datetime":       {"type": "date"},
    "start_datetime": {"type": "date"},
    "end_datetime":   {"type": "date"},
    "created":        {"type": "date"},
    "updated":        {"type": "date"},
    # STAC Common Metadata
    "title":          _localized_text_field(ignore_above=512),
    "description":    _localized_text_field(ignore_above=1024),
    "license":        {"type": "keyword"},
    "platform":       {"type": "keyword"},
    "instruments":    {"type": "keyword"},
    "constellation":  {"type": "keyword"},
    "mission":        {"type": "keyword"},
    "gsd":            {"type": "float"},
    # Keywords — keyword + .text for full-text via properties.* wildcard
    "keywords": {
        "type": "keyword",
        "fields": {"text": {"type": "text", "analyzer": "standard"}},
    },
    # Providers array — large nested object, not searched; suppress index
    "providers":      {"type": "object", "enabled": False},
    # Platform identifier — mirrors top-level geoid when written under properties
    "geoid":          {"type": "keyword"},
}

# ---------------------------------------------------------------------------
# STAC extension field maps. One block per extension prefix actively used
# by the platform (per audit summary 2026-05-17). Operators who need
# extensions outside this set rely on the Tier 3 ``extras`` lane in v1;
# Tier 2 per-catalog overlay arrives in the follow-up commit.
# ---------------------------------------------------------------------------

_EO_FIELDS: Dict[str, Dict[str, Any]] = {
    "eo:cloud_cover":  {"type": "float"},
    "eo:snow_cover":   {"type": "float"},
    # bands is a nested array of band-objects; not queried directly
    "eo:bands":        {"type": "object", "enabled": False},
}

_PROJ_FIELDS: Dict[str, Dict[str, Any]] = {
    "proj:epsg":       {"type": "integer"},
    "proj:code":       {"type": "keyword"},
    "proj:centroid":   {"type": "geo_point"},
    # complex objects — not searched, suppress index
    "proj:bbox":       {"type": "object", "enabled": False},
    "proj:geometry":   {"type": "object", "enabled": False},
    "proj:projjson":   {"type": "object", "enabled": False},
    "proj:wkt2":       {"type": "keyword", "index": False, "doc_values": False},
    "proj:shape":      {"type": "integer"},
    "proj:transform":  {"type": "float"},
}

_RASTER_FIELDS: Dict[str, Dict[str, Any]] = {
    # bands is a large nested array; not queried directly
    "raster:bands":    {"type": "object", "enabled": False},
}

_VIEW_FIELDS: Dict[str, Dict[str, Any]] = {
    "view:off_nadir":        {"type": "float"},
    "view:incidence_angle":  {"type": "float"},
    "view:azimuth":          {"type": "float"},
    "view:sun_azimuth":      {"type": "float"},
    "view:sun_elevation":    {"type": "float"},
}

_SAR_FIELDS: Dict[str, Dict[str, Any]] = {
    "sar:instrument_mode":       {"type": "keyword"},
    "sar:frequency_band":        {"type": "keyword"},
    "sar:polarizations":         {"type": "keyword"},
    "sar:observation_direction": {"type": "keyword"},
    "sar:product_type":          {"type": "keyword"},
    "sar:resolution_range":      {"type": "float"},
    "sar:resolution_azimuth":    {"type": "float"},
    "sar:pixel_spacing_range":   {"type": "float"},
    "sar:pixel_spacing_azimuth": {"type": "float"},
    "sar:looks_range":           {"type": "integer"},
    "sar:looks_azimuth":         {"type": "integer"},
}

_SCIENTIFIC_FIELDS: Dict[str, Dict[str, Any]] = {
    "sci:doi":           {"type": "keyword"},
    "sci:citation":      {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 1024}},
    },
    "sci:publications":  {"type": "object", "enabled": False},
}

_PROCESSING_FIELDS: Dict[str, Dict[str, Any]] = {
    "processing:level":     {"type": "keyword"},
    "processing:datetime":  {"type": "date"},
    "processing:software":  {"type": "object", "enabled": False},
    "processing:lineage":   {"type": "text", "index": False},
    "processing:facility":  {"type": "keyword"},
    "processing:expression": {"type": "keyword", "index": False, "doc_values": False},
}

_CLASSIFICATION_FIELDS: Dict[str, Dict[str, Any]] = {
    "classification:classes": {"type": "object", "enabled": False},
    "classification:bitfields": {"type": "object", "enabled": False},
}

_VERSION_FIELDS: Dict[str, Dict[str, Any]] = {
    "version":       {"type": "keyword"},
    "deprecated":    {"type": "boolean"},
}

_FILE_FIELDS: Dict[str, Dict[str, Any]] = {
    "file:size":            {"type": "long"},
    "file:checksum":        {"type": "keyword", "index": False, "doc_values": False},
    "file:header_size":     {"type": "long"},
    "file:byte_order":      {"type": "keyword"},
    "file:values":          {"type": "object", "enabled": False},
}

_MLM_FIELDS: Dict[str, Dict[str, Any]] = {
    "mlm:name":          {"type": "keyword"},
    "mlm:architecture":  {"type": "keyword"},
    "mlm:framework":     {"type": "keyword"},
    "mlm:tasks":         {"type": "keyword"},
}

# FAO-derived non-STAC extensions emitted by the OTF storage drivers
# (`vector:*` from OGR shapefile/GeoPackage; `table:*` from columnar
# introspection). Not STAC-spec but actively populated.

_VECTOR_FIELDS: Dict[str, Dict[str, Any]] = {
    "vector:geometry_type":  {"type": "keyword"},
    "vector:count":          {"type": "long"},
}

_TABLE_FIELDS: Dict[str, Dict[str, Any]] = {
    "table:columns":     {"type": "object", "enabled": False},
    "table:row_count":   {"type": "long"},
}


# ---------------------------------------------------------------------------
# Tier 1 — merged platform-wide known-fields map. Kept here so an external
# audit can read the full surface in one place.
# ---------------------------------------------------------------------------

TIER_1_FIELDS: Dict[str, Dict[str, Any]] = {
    **_STAC_CORE_FIELDS,
    **_EO_FIELDS,
    **_PROJ_FIELDS,
    **_RASTER_FIELDS,
    **_VIEW_FIELDS,
    **_SAR_FIELDS,
    **_SCIENTIFIC_FIELDS,
    **_PROCESSING_FIELDS,
    **_CLASSIFICATION_FIELDS,
    **_VERSION_FIELDS,
    **_FILE_FIELDS,
    **_MLM_FIELDS,
    **_VECTOR_FIELDS,
    **_TABLE_FIELDS,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_known_fields(catalog_config: Optional[Any] = None) -> Dict[str, Dict[str, Any]]:
    """Resolve the effective known-fields map for a catalog.

    Returns ``Tier 1`` merged with the catalog's Tier-2 overlay
    (``ItemsElasticsearchDriverConfig.mapping``). Tier-2 is **additive
    only** — :func:`validate_tier_2` rejects collisions at config-write
    time, so by the time a config reaches this function the merge is
    guaranteed safe. The merge order (Tier 2 last) is defensive: if a
    bypass ever lands a colliding overlay, the operator's value still
    wins so the index mapping and the projection stay consistent.

    Returning a copy avoids accidental mutation of the module-level
    invariant. Callers that pass ``None`` get Tier 1 only — used by
    cross-catalog ``/search`` (against the platform alias) where Tier-2
    fields are not guaranteed to exist on every member.
    """
    out: Dict[str, Dict[str, Any]] = dict(TIER_1_FIELDS)
    if catalog_config is None:
        return out
    overlay = getattr(catalog_config, "mapping", None)
    if isinstance(overlay, dict) and overlay:
        out.update(overlay)
    return out


def validate_tier_2(
    tier_2: Dict[str, Any],
    tier_1: Optional[Dict[str, Dict[str, Any]]] = None,
) -> None:
    """Validate a Tier-2 overlay for additive-only safety.

    Raises :class:`ValueError` when ``tier_2`` carries a key that
    collides with Tier 1 at a different type — the alias contract
    requires uniform typing for Tier-1 paths across every member index.

    A collision is defined as ``tier_2[key]["type"] != tier_1[key]["type"]``;
    same-type re-declarations are tolerated (no-op overlay, useful for
    operators who want to mirror Tier-1 fields in their catalog config
    for documentation purposes).

    A Tier-2 value that is not a dict, or omits ``"type"``, is rejected —
    every entry must declare an explicit ES field type to keep the
    surface auditable.
    """
    if not isinstance(tier_2, dict):
        raise ValueError(
            "ItemsElasticsearchDriverConfig.mapping must be a dict of "
            "{stac_field: {ES field-type definition}}."
        )
    if not tier_2:
        return
    tier_1 = tier_1 if tier_1 is not None else TIER_1_FIELDS

    for key, value in tier_2.items():
        if not isinstance(value, dict) or "type" not in value:
            raise ValueError(
                f"ItemsElasticsearchDriverConfig.mapping[{key!r}] must be a "
                "dict with at least a 'type' field; got "
                f"{type(value).__name__}."
            )
        if key in tier_1:
            t1_type = tier_1[key].get("type")
            t2_type = value.get("type")
            if t1_type != t2_type:
                raise ValueError(
                    f"ItemsElasticsearchDriverConfig.mapping[{key!r}] type "
                    f"{t2_type!r} collides with platform Tier-1 type "
                    f"{t1_type!r}. Tier-2 is additive only — pick a "
                    "different key or remove the override."
                )


async def resolve_catalog_known_fields(catalog_id: Optional[str]) -> Dict[str, Dict[str, Any]]:
    """Async helper: fetch the per-catalog ``ItemsElasticsearchDriverConfig``
    and return the merged Tier-1 ∪ Tier-2 known-fields map.

    Falls back to Tier 1 only when:

    * ``catalog_id`` is ``None`` — caller is in alias/cross-catalog scope;
    * the configs protocol is not yet registered (cold boot, unit test);
    * the config fetch raises any exception (degrade-safe, never blocks
      writes).

    Used by write entry points so the projection helper writes Tier-2
    fields at their explicit ``properties.<key>`` path instead of routing
    them through ``extras``.
    """
    if not catalog_id:
        return dict(TIER_1_FIELDS)
    try:
        from dynastore.models.protocols.configs import ConfigsProtocol
        from dynastore.modules.storage.driver_config import (
            ItemsElasticsearchDriverConfig,
        )
        from dynastore.tools.discovery import get_protocol
    except Exception as exc:  # noqa: BLE001
        # Import failure means the configs or storage.driver_config module is
        # not available in this process scope (e.g. a minimal SCOPE without the
        # full platform stack).  Tier-1 fields are always safe as a fallback.
        logger.debug(
            "resolve_catalog_known_fields: import error for catalog %r; "
            "falling back to Tier-1 only: %s",
            catalog_id, exc,
        )
        return dict(TIER_1_FIELDS)

    configs = get_protocol(ConfigsProtocol)
    if configs is None:
        return dict(TIER_1_FIELDS)
    try:
        cfg = await configs.get_config(
            ItemsElasticsearchDriverConfig,
            catalog_id=catalog_id,
        )
    except Exception as exc:  # noqa: BLE001
        # Config row absent or DB unavailable.  The Tier-2 overlay is an
        # optional enrichment; writes proceed with Tier-1 only and will
        # pick up the overlay once the config is stored.
        logger.debug(
            "resolve_catalog_known_fields: get_config failed for catalog %r; "
            "falling back to Tier-1 only: %s",
            catalog_id, exc,
        )
        return dict(TIER_1_FIELDS)
    return build_known_fields(cfg)


# GeoJSON / STAC top-level member names. These identify or structure the item
# and must never appear inside ``properties``; if one leaks in it is dropped by
# the projection rather than wrapped into the ``extras`` lane (#1212).
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


def strip_reserved_members(properties: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of *properties* with GeoJSON/STAC structural members removed.

    The members in :data:`_RESERVED_MEMBER_KEYS` (``id``, ``type``,
    ``geometry``, …) are item-level identity and must never live inside
    ``properties``. The create/update echo and the raw-row read fallback both
    assemble response properties straight from stored data, which can still
    carry an ``id``: on Postgres the ``feature_id_expr AS id`` alias rides in
    via the attributes sidecar, and on Elasticsearch the write echo returns the
    in-memory feature as-is. Stripping here keeps the POST/PUT echo aligned with
    the GET contract that :func:`project_item_for_es` already enforces on the
    read/index path (#1232). Exposure knobs are unaffected: ``expose_geoid``
    adds a ``geoid`` property (not reserved, preserved) and ``created`` is gated
    separately.
    """
    if not isinstance(properties, dict):
        return properties
    return {k: v for k, v in properties.items() if k not in _RESERVED_MEMBER_KEYS}


def project_item_for_es(doc: Dict[str, Any], known_fields: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Reshape an item doc so unknown ``properties`` keys move to ``properties.extras``.

    Pure function — returns a new dict; the input is not mutated. Internal
    top-level ``_*`` fields (``_asset_id``, ``_external_id``,
    ``_simplification_factor``, …) and the STAC top-level keys
    (``geometry``, ``bbox``, ``id``, ``collection``, …) are left
    untouched. Only ``doc["properties"]`` is reshaped.

    A key is considered "known" when it appears in ``known_fields`` as a
    top-level entry — sub-paths like ``title.en`` are not interrogated
    here because ES routes them via the parent's sub-properties; if
    ``title`` is known, ``title.en`` is reachable as a sub-field.

    If ``doc["properties"]`` already carries an ``extras`` object (from a
    replay or upstream projection), its contents are merged forward so
    the projection is idempotent.
    """
    if not isinstance(doc, dict):
        return doc
    props = doc.get("properties")
    if not isinstance(props, dict):
        return doc

    new_props: Dict[str, Any] = {}
    extras: Dict[str, Any] = {}
    for k, v in props.items():
        if k in _RESERVED_MEMBER_KEYS:
            # GeoJSON/STAC structural members are item-level identity, never
            # user properties. If one leaks into ``properties`` (e.g. the input
            # feature's top-level ``id`` preserved as ``properties.id``) drop it
            # rather than routing it into ``extras`` — otherwise it surfaces as
            # ``extras: {"id": ...}`` on read and diverges across endpoints
            # (#1212). The canonical value already lives at the doc top level.
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
        # Populate the analyzed catch-all so the ``flattened`` extras
        # lane (exact-key only) is paired with a real fulltext field
        # for the unknown-property tail (#1295). The two together =
        # "fulltext or better" at two mapping entries flat, regardless
        # of how many distinct extension keys arrive.
        search_text = _flatten_extras_for_search(extras)
        if search_text:
            out["_search_text"] = search_text
    out["properties"] = new_props
    return out


def _flatten_extras_for_search(extras: Dict[str, Any]) -> str:
    """Flatten an ``extras`` bucket to a single whitespace-joined string.

    Feeds the root ``_search_text`` ES ``text`` field (standard
    analyzer). Strings/numbers/booleans pass through ``str``; nested
    dicts / lists are emitted as their compact JSON encoding so every
    leaf value contributes a token — JSON punctuation tokenizes
    harmlessly under the standard analyzer. ``None`` is skipped.

    Pure function; the input is not mutated.
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


def unproject_item_from_es(
    source: Dict[str, Any],
    lang: str = "en",
) -> Dict[str, Any]:
    """Rebuild the GeoJSON/STAC read contract from an indexed ``_source``.

    Inverse of :func:`project_item_for_es`. The write path routes unknown
    ``properties`` keys under ``properties.extras`` (Tier 3) and attaches
    internal ``_*`` tracking fields (``_external_id``, ``_asset_id``,
    ``_simplification_factor``, …) at the document top level. A raw
    ``_source`` therefore violates the read contract three ways:

    * attributes are nested under ``properties.extras``;
    * internal ``_*`` fields and any leaked echo keys sit at the top level
      (and would surface on the wire because :class:`dynastore.models.ogc.Feature`
      is ``extra="allow"``);
    * ``geometry`` may be an empty ``{}``.

    This helper restores the canonical shape:

    * ``properties.extras.*`` is hoisted back to flat ``properties.*`` (an
      existing flat key wins on collision — it is the more specific value);
    * when ``_source["metadata"]`` contains a typed multilingual container
      (``title`` / ``description`` / ``keywords``), each field is resolved
      to the requested ``lang`` (defaulting to ``"en"``) and placed onto
      the flat ``properties`` dict — matching the PG read contract. The raw
      ``metadata`` container is not surfaced on the wire;
    * only GeoJSON/STAC structural members (:data:`_RESERVED_MEMBER_KEYS`)
      survive at the top level — internal ``_*`` fields and leaked
      attribute/echo keys are dropped;
    * an empty/falsy ``geometry`` is normalised to ``None`` so the output is
      valid GeoJSON (``null`` geometry) instead of ``{}``.

    Pure function — returns a new dict, the input is not mutated.
    Read-policy exposure (id override, ``expose`` gating) is layered on by
    the caller after this structural normalisation.
    """
    return unproject_envelope_from_es(
        source,
        reserved_member_keys=_RESERVED_MEMBER_KEYS,
        default_type="Feature",
        null_empty_geometry=True,
        lang=lang,
    )


def unproject_envelope_from_es(
    source: Dict[str, Any],
    *,
    reserved_member_keys: "frozenset[str]",
    default_type: Optional[str] = None,
    null_empty_geometry: bool = False,
    lang: str = "en",
) -> Dict[str, Any]:
    """Level-agnostic read reconstruction from a canonical ``_source`` (#1285).

    Generalises :func:`unproject_item_from_es` so every entity level
    (catalog / collection / item / asset) restores its wire shape with the
    same three moves, parameterised by which structural members that level
    surfaces:

    * ``properties.extras.*`` is hoisted back to flat ``properties.*`` (an
      existing flat key wins on collision — it is the more specific value);
    * when ``_source["metadata"]`` carries a typed multilingual container
      (``title`` / ``description`` / ``keywords``), each present key is
      resolved to the requested ``lang`` (``"*"`` returns the full dict;
      otherwise falls back to ``"en"`` then the first available language)
      and written to flat ``properties`` via ``setdefault`` — the raw
      ``metadata`` container is kept out of the wire output, consistent with
      how ``system``/``stats``/``access`` are treated;
    * only the level's ``reserved_member_keys`` survive at the top level —
      flat identity (``catalog_id``/``collection_id``), internal ``_*`` fields
      and the ``system``/``stats``/``access`` containers are dropped;
    * when ``default_type`` is given it is stamped as the GeoJSON/STAC
      ``type``; when ``null_empty_geometry`` is set an empty/falsy
      ``geometry`` is normalised to ``None`` (valid GeoJSON ``null``).

    The per-(entity_level, protocol) projector registry (#1285) layers any
    attribute → wire-member remapping (e.g. a STAC Collection has no
    ``properties`` member) on top of this structural normalisation.

    Pure function — returns a new dict, the input is not mutated.
    """
    if not isinstance(source, dict):
        return source

    props_in = source.get("properties")
    props: Dict[str, Any] = dict(props_in) if isinstance(props_in, dict) else {}
    extras = props.pop("extras", None)
    if isinstance(extras, dict):
        for k, v in extras.items():
            props.setdefault(k, v)

    # Resolve the typed multilingual metadata container (#1828 Phase 2).
    # The raw ``metadata`` block is intentionally excluded from the wire output
    # (same treatment as ``system``/``stats``/``access``). Only the resolved
    # scalar/array values land on flat ``properties``.
    metadata = source.get("metadata")
    if isinstance(metadata, dict):
        for _meta_key in ("title", "description", "keywords"):
            _raw = metadata.get(_meta_key)
            if _raw is None:
                continue
            _resolved = resolve_localized_field(_raw, lang)
            if _resolved is not None:
                props.setdefault(_meta_key, _resolved)

    out: Dict[str, Any] = {"properties": props}
    if default_type is not None:
        out["type"] = default_type
    for key in reserved_member_keys:
        if key == "type":
            continue
        if key in source:
            out[key] = source[key]

    if null_empty_geometry and not out.get("geometry"):
        out["geometry"] = None

    return out


def resolve_es_field_path(
    stac_path: str,
    known_fields: Dict[str, Any],
) -> str:
    """Resolve a STAC field path to the ES path it actually lives at.

    Used by the search sort path (and any future CQL2→ES translator) so a
    query against an unknown extension field still hits the right
    bucket. Examples:

    * ``properties.datetime``  → ``properties.datetime`` (Tier 1)
    * ``properties.title.en``  → ``properties.title.en`` (Tier 1; ES
      routes ``.en`` to the analyzer-tagged sub-property)
    * ``properties.eo:cloud_cover`` → ``properties.eo:cloud_cover``
    * ``properties.area``      → ``stats.area`` (container="stats")
    * ``properties.geometry_hash`` → ``system.geometry_hash`` (container="system")
    * ``properties.foo:bar``   → ``properties.extras.foo:bar``
    * ``_external_id``         → ``_external_id`` (top-level passthrough)

    Top-level paths and anything not under ``properties.`` pass through
    unchanged. Sub-paths under a Tier-1 object field (e.g.
    ``properties.title.en``) keep their full path so ES can resolve the
    leaf via the parent's sub-properties.

    When ``known_fields`` carries :class:`~dynastore.models.protocols.field_definition.FieldDefinition`
    values with a ``container`` tag, the field is routed to its canonical
    container (``stats.*`` / ``system.*`` / flat root for identity) via
    :func:`~dynastore.modules.storage.computed_fields.classify_container`
    — the single classification SSOT (refs #1800).
    """
    if not stac_path.startswith("properties."):
        return stac_path
    tail = stac_path[len("properties."):]
    head, _, _ = tail.partition(".")

    if head == "extras":
        return stac_path

    field_def = known_fields.get(head)
    if field_def is None:
        # Unknown field — route to the extras long-tail lane.
        return f"properties.extras.{tail}"

    # Route based on container classification.
    from dynastore.modules.storage.computed_fields import classify_container
    container = classify_container(head, field_def)

    if container == "stats":
        return f"stats.{tail}"
    if container == "system":
        return f"system.{tail}"
    if container == "identity":
        # Identity fields (external_id, asset_id, geoid) live flat at the
        # document root; strip the ``properties.`` prefix.
        return tail

    # Default: keep the ``properties.<name>`` path unchanged (user attrs).
    return stac_path
