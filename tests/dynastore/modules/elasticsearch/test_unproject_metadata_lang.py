"""Unit tests for metadata multilingual resolution in ES read reconstruction (#1828 Phase 2).

When an ES ``_source`` carries the typed ``metadata{}`` container
(``title`` / ``description`` / ``keywords`` as multilingual dicts), the
``unproject_item_from_es`` / ``unproject_envelope_from_es`` functions must:

* resolve each field per the requested language and place the scalar/array
  onto flat ``properties``;
* not surface the raw ``metadata`` container on the wire output;
* be a no-op when ``metadata`` is absent.
"""
from __future__ import annotations

from typing import Any, Dict

from dynastore.modules.elasticsearch.items_projection import (
    _RESERVED_MEMBER_KEYS,
    unproject_envelope_from_es,
    unproject_item_from_es,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_SOURCE: Dict[str, Any] = {
    "id": "item-1",
    "type": "Feature",
    "collection": "col",
    "collection_id": "col",
    "geometry": {"type": "Point", "coordinates": [10.0, 45.0]},
    "bbox": [10.0, 45.0, 10.0, 45.0],
    "properties": {
        "datetime": "2026-06-04T00:00:00Z",
    },
    # Internal containers — must NOT appear on the wire
    "system": {"geometry_hash": "abc123"},
    "stats": {"area": 0.5},
    "access": {"tenant_id": "t1"},
}


def _source_with_metadata(**fields: Any) -> Dict[str, Any]:
    """Return a copy of ``_BASE_SOURCE`` with the given metadata keys."""
    src = dict(_BASE_SOURCE)
    src["metadata"] = {k: v for k, v in fields.items()}
    return src


# ---------------------------------------------------------------------------
# Language resolution — title
# ---------------------------------------------------------------------------


def test_unproject_resolves_title_en() -> None:
    src = _source_with_metadata(title={"en": "English title", "fr": "Titre français"})
    out = unproject_item_from_es(src, lang="en")
    assert out["properties"]["title"] == "English title"


def test_unproject_resolves_title_fr() -> None:
    src = _source_with_metadata(title={"en": "English title", "fr": "Titre français"})
    out = unproject_item_from_es(src, lang="fr")
    assert out["properties"]["title"] == "Titre français"


def test_unproject_title_falls_back_to_en_when_lang_missing() -> None:
    src = _source_with_metadata(title={"en": "English title", "fr": "Titre français"})
    # Request a language that isn't in the dict → falls back to "en"
    out = unproject_item_from_es(src, lang="es")
    assert out["properties"]["title"] == "English title"


def test_unproject_title_falls_back_to_first_when_no_en() -> None:
    # No "en" key at all; first available should be returned for unknown lang
    src = _source_with_metadata(title={"fr": "Titre français", "ar": "عنوان"})
    out = unproject_item_from_es(src, lang="es")
    assert out["properties"]["title"] == "Titre français"


# ---------------------------------------------------------------------------
# Language resolution — description
# ---------------------------------------------------------------------------


def test_unproject_resolves_description_en() -> None:
    src = _source_with_metadata(description={"en": "A dataset.", "fr": "Un jeu de données."})
    out = unproject_item_from_es(src, lang="en")
    assert out["properties"]["description"] == "A dataset."


def test_unproject_resolves_description_fr() -> None:
    src = _source_with_metadata(description={"en": "A dataset.", "fr": "Un jeu de données."})
    out = unproject_item_from_es(src, lang="fr")
    assert out["properties"]["description"] == "Un jeu de données."


# ---------------------------------------------------------------------------
# Language resolution — keywords (list values)
# ---------------------------------------------------------------------------


def test_unproject_resolves_keywords_en() -> None:
    src = _source_with_metadata(
        keywords={"en": ["food", "agriculture"], "fr": ["alimentation", "agriculture"]}
    )
    out = unproject_item_from_es(src, lang="en")
    assert out["properties"]["keywords"] == ["food", "agriculture"]


def test_unproject_resolves_keywords_fr() -> None:
    src = _source_with_metadata(
        keywords={"en": ["food", "agriculture"], "fr": ["alimentation", "agriculture"]}
    )
    out = unproject_item_from_es(src, lang="fr")
    assert out["properties"]["keywords"] == ["alimentation", "agriculture"]


# ---------------------------------------------------------------------------
# Wildcard language — '*' returns full dict
# ---------------------------------------------------------------------------


def test_unproject_wildcard_lang_returns_full_dict() -> None:
    src = _source_with_metadata(title={"en": "English title", "fr": "Titre français"})
    out = unproject_item_from_es(src, lang="*")
    assert out["properties"]["title"] == {"en": "English title", "fr": "Titre français"}


# ---------------------------------------------------------------------------
# Default lang — "en" when no lang argument supplied
# ---------------------------------------------------------------------------


def test_unproject_default_lang_is_en() -> None:
    src = _source_with_metadata(title={"en": "Default English", "fr": "Français"})
    out = unproject_item_from_es(src)  # no lang arg
    assert out["properties"]["title"] == "Default English"


# ---------------------------------------------------------------------------
# Absent metadata is a no-op
# ---------------------------------------------------------------------------


def test_unproject_no_metadata_is_noop() -> None:
    # Source without any metadata key
    src = dict(_BASE_SOURCE)
    out = unproject_item_from_es(src, lang="en")
    assert "title" not in out["properties"]
    assert "description" not in out["properties"]
    assert "keywords" not in out["properties"]


def test_unproject_empty_metadata_dict_is_noop() -> None:
    src = dict(_BASE_SOURCE)
    src["metadata"] = {}
    out = unproject_item_from_es(src, lang="en")
    assert "title" not in out["properties"]
    assert "description" not in out["properties"]
    assert "keywords" not in out["properties"]


def test_unproject_partial_metadata_only_present_keys_land() -> None:
    # Only title present — description and keywords must not appear
    src = _source_with_metadata(title={"en": "Only title"})
    out = unproject_item_from_es(src, lang="en")
    assert out["properties"]["title"] == "Only title"
    assert "description" not in out["properties"]
    assert "keywords" not in out["properties"]


# ---------------------------------------------------------------------------
# Raw ``metadata`` container is NOT surfaced on the wire
# ---------------------------------------------------------------------------


def test_unproject_metadata_container_not_on_wire() -> None:
    src = _source_with_metadata(title={"en": "T"}, description={"en": "D"})
    out = unproject_item_from_es(src, lang="en")
    assert "metadata" not in out


# ---------------------------------------------------------------------------
# Properties flat key wins over metadata (setdefault semantics)
# ---------------------------------------------------------------------------


def test_unproject_existing_properties_title_wins_over_metadata() -> None:
    # If a flat ``title`` is already in properties (e.g. written there
    # explicitly), it takes precedence over the metadata container.
    src = dict(_BASE_SOURCE)
    src_props = dict(src["properties"])
    src_props["title"] = "Flat title wins"
    src = dict(src)
    src["properties"] = src_props
    src["metadata"] = {"title": {"en": "Should not win"}}
    out = unproject_item_from_es(src, lang="en")
    assert out["properties"]["title"] == "Flat title wins"


# ---------------------------------------------------------------------------
# Internal containers (system/stats/access) remain dropped — no regression
# ---------------------------------------------------------------------------


def test_unproject_internal_containers_still_dropped() -> None:
    src = _source_with_metadata(title={"en": "T"})
    out = unproject_item_from_es(src, lang="en")
    assert "system" not in out
    assert "stats" not in out
    assert "access" not in out


# ---------------------------------------------------------------------------
# unproject_envelope_from_es — lang kwarg works at the level-agnostic layer
# ---------------------------------------------------------------------------


def test_unproject_envelope_lang_kwarg() -> None:
    src = _source_with_metadata(title={"en": "Envelope EN", "fr": "Envelope FR"})
    out = unproject_envelope_from_es(
        src,
        reserved_member_keys=_RESERVED_MEMBER_KEYS,
        default_type="Feature",
        null_empty_geometry=True,
        lang="fr",
    )
    assert out["properties"]["title"] == "Envelope FR"


# ---------------------------------------------------------------------------
# Pure-function guarantee: input is not mutated
# ---------------------------------------------------------------------------


def test_unproject_does_not_mutate_source() -> None:
    src = _source_with_metadata(title={"en": "T"})
    meta_before = dict(src["metadata"])
    props_before = dict(src["properties"])
    unproject_item_from_es(src, lang="en")
    assert src["metadata"] == meta_before
    assert src["properties"] == props_before
