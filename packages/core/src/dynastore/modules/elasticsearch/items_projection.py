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
* **Tier 3** — ``properties.extras`` per-index dynamic lane. Anything not
  in Tier 1 ∪ Tier 2 lands here, indexed with per-index dynamic mapping
  (first-write-wins types). Searchable via the existing full-text
  ``properties.*`` wildcard; first-class filter/sort works locally but
  not cross-catalog.

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

from typing import Any, Dict, Optional


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
    except Exception:
        return dict(TIER_1_FIELDS)

    configs = get_protocol(ConfigsProtocol)
    if configs is None:
        return dict(TIER_1_FIELDS)
    try:
        cfg = await configs.get_config(
            ItemsElasticsearchDriverConfig,
            catalog_id=catalog_id,
        )
    except Exception:
        return dict(TIER_1_FIELDS)
    return build_known_fields(cfg)


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
        if k == "extras":
            if isinstance(v, dict):
                extras.update(v)
            continue
        if k in known_fields:
            new_props[k] = v
        else:
            extras[k] = v

    if extras:
        new_props["extras"] = extras

    out = dict(doc)
    out["properties"] = new_props
    return out


def resolve_es_field_path(
    stac_path: str,
    known_fields: Dict[str, Dict[str, Any]],
) -> str:
    """Resolve a STAC field path to the ES path it actually lives at.

    Used by the search sort path (and any future CQL2→ES translator) so a
    query against an unknown extension field still hits the right
    bucket. Examples:

    * ``properties.datetime``  → ``properties.datetime`` (Tier 1)
    * ``properties.title.en``  → ``properties.title.en`` (Tier 1; ES
      routes ``.en`` to the analyzer-tagged sub-property)
    * ``properties.eo:cloud_cover`` → ``properties.eo:cloud_cover``
    * ``properties.foo:bar``   → ``properties.extras.foo:bar``
    * ``_external_id``         → ``_external_id`` (top-level passthrough)

    Top-level paths and anything not under ``properties.`` pass through
    unchanged. Sub-paths under a Tier-1 object field (e.g.
    ``properties.title.en``) keep their full path so ES can resolve the
    leaf via the parent's sub-properties.
    """
    if not stac_path.startswith("properties."):
        return stac_path
    tail = stac_path[len("properties."):]
    head, _, _ = tail.partition(".")
    if head in known_fields or head == "extras":
        return stac_path
    return f"properties.extras.{tail}"
