"""Naming contract for ES index helpers — Phase 2 target topology.

Target shape (user-specified):

  - ``dynastore-catalogs``                  singleton, full catalog object
  - ``dynastore-collections``               singleton, full collection object
  - ``dynastore-items``                     alias spanning all per-catalog public items indexes
  - ``dynastore-{catalog}-items``           per-catalog public items index
  - ``dynastore-{catalog}-private-items``   per-catalog private items index (NOT in public alias)
"""

from dynastore.modules.elasticsearch.mappings import (
    DYNAMIC_TEMPLATES,
    get_index_name,
    get_public_items_alias,
    get_tenant_items_index,
)
from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
    get_private_index_name,
)


PREFIX = "dynastore"


def test_catalogs_singleton_unchanged():
    assert get_index_name(PREFIX, "catalog") == "dynastore-catalogs"


def test_collections_singleton_unchanged():
    assert get_index_name(PREFIX, "collection") == "dynastore-collections"


def test_per_catalog_items_index_uses_catalog_first_naming():
    assert get_tenant_items_index(PREFIX, "adm2_catalog") == "dynastore-adm2_catalog-items"


def test_public_items_alias_drops_public_suffix():
    assert get_public_items_alias(PREFIX) == "dynastore-items"


def test_private_items_uses_catalog_first_naming():
    assert get_private_index_name(PREFIX, "adm2_catalog") == "dynastore-adm2_catalog-private-items"


def test_per_catalog_assets_index_uses_catalog_first_naming():
    from dynastore.modules.elasticsearch.mappings import get_assets_index_name

    assert get_assets_index_name(PREFIX, "adm2_catalog") == "dynastore-adm2_catalog-assets"


def _find_template(name: str) -> dict:
    for entry in DYNAMIC_TEMPLATES:
        if name in entry:
            return entry[name]
    raise AssertionError(f"dynamic template {name!r} not found")


def test_dynamic_templates_apply_per_language_analyzer_to_titles_top_level():
    # Each supported locale gets a top-level template so catalog/collection
    # ``title.en`` (which is at the document root) gets the language analyzer
    # rather than falling through to the generic ``strings`` template.
    expected_analyzers = {
        "en": "english",
        "fr": "french",
        "es": "spanish",
        "ru": "russian",
        "ar": "arabic",
        "it": "italian",
        "de": "german",
    }
    for lang, analyzer in expected_analyzers.items():
        tmpl = _find_template(f"title_{lang}_top")
        assert tmpl["path_match"] == f"title.{lang}"
        assert tmpl["mapping"]["analyzer"] == analyzer
        assert tmpl["mapping"]["type"] == "text"


def test_dynamic_templates_apply_per_language_analyzer_to_titles_nested():
    # And a nested template so STAC items (title under ``properties``) also
    # get the right analyzer. ES ``path_match`` ``*`` is a path-segment
    # wildcard, so a single pattern can't cover both shapes.
    for lang, analyzer in [("fr", "french"), ("ar", "arabic")]:
        tmpl = _find_template(f"title_{lang}_nested")
        assert tmpl["path_match"] == f"*.title.{lang}"
        assert tmpl["mapping"]["analyzer"] == analyzer


def test_dynamic_templates_apply_per_language_analyzer_to_descriptions():
    top = _find_template("description_fr_top")
    assert top["path_match"] == "description.fr"
    assert top["mapping"]["analyzer"] == "french"

    nested = _find_template("description_fr_nested")
    assert nested["path_match"] == "*.description.fr"
    assert nested["mapping"]["analyzer"] == "french"


def test_zh_falls_back_to_standard_analyzer():
    # ES has no built-in Chinese analyzer; we use 'standard' until smartcn/ICU
    # is added at the cluster level. Documented choice, not an oversight.
    top = _find_template("title_zh_top")
    assert top["mapping"]["analyzer"] == "standard"


def test_unlocalized_text_falls_through_to_catchall_for_nested_paths():
    # Nested unlocalized strings (e.g. STAC item ``properties.description``
    # set to a plain string) keep the standard analyzer via the catch-all
    # — preserves pre-Phase-2 behavior.
    tmpl = _find_template("descriptions")
    assert tmpl["path_match"] == "*.description"
    assert tmpl["mapping"]["analyzer"] == "standard"
