"""Unit tests for the #1462 PolicyContributorPreset migration.

Verifies that the 3 remaining extension presets (features_enable,
tiles_enable, auth_enable) are registered in the global preset registry
and that their contributor factories return the expected policy IDs.
Also pins that platform_demo._COMPOSE includes these names.
(search_enable was removed when the generic search extension was deleted.)
"""

from __future__ import annotations


def _import_preset_registry():
    """Import presets sub-package for each extension so registration runs."""
    import importlib
    for mod in (
        "dynastore.extensions.features.presets",
        "dynastore.extensions.tiles.presets",
        "dynastore.extensions.auth.presets",
    ):
        try:
            importlib.import_module(mod)
        except ImportError:
            pass


# ---------------------------------------------------------------------------
# features_enable
# ---------------------------------------------------------------------------

def test_features_enable_is_registered() -> None:
    """features_enable must appear in the global preset registry."""
    _import_preset_registry()
    from dynastore.modules.storage.presets.registry import find_preset
    preset = find_preset("features_enable")
    assert preset is not None
    assert preset.name == "features_enable"


def test_features_enable_contributor_returns_expected_policy_id() -> None:
    """features_enable contributor must yield the features_public_access policy."""
    _import_preset_registry()
    from dynastore.modules.storage.presets.registry import find_preset
    preset = find_preset("features_enable")
    contributor = preset._contributor_factory()
    policy_ids = [p.id for p in contributor.get_policies()]
    assert "features_public_access" in policy_ids


def test_features_enable_contributor_binds_anonymous_role() -> None:
    """features_enable must bind features_public_access to the anonymous role."""
    _import_preset_registry()
    from dynastore.modules.storage.presets.registry import find_preset
    preset = find_preset("features_enable")
    contributor = preset._contributor_factory()
    all_bound = [p for rb in contributor.get_role_bindings() for p in rb.policies]
    assert "features_public_access" in all_bound


# ---------------------------------------------------------------------------
# tiles_enable
# ---------------------------------------------------------------------------

def test_tiles_enable_is_registered() -> None:
    """tiles_enable must appear in the global preset registry."""
    _import_preset_registry()
    from dynastore.modules.storage.presets.registry import find_preset
    preset = find_preset("tiles_enable")
    assert preset is not None
    assert preset.name == "tiles_enable"


def test_tiles_enable_contributor_returns_expected_policy_id() -> None:
    """tiles_enable contributor must yield the tiles_public_access policy."""
    _import_preset_registry()
    from dynastore.modules.storage.presets.registry import find_preset
    preset = find_preset("tiles_enable")
    contributor = preset._contributor_factory()
    policy_ids = [p.id for p in contributor.get_policies()]
    assert "tiles_public_access" in policy_ids


# ---------------------------------------------------------------------------
# auth_enable
# ---------------------------------------------------------------------------

def test_auth_enable_is_registered() -> None:
    """auth_enable must appear in the global preset registry."""
    _import_preset_registry()
    from dynastore.modules.storage.presets.registry import find_preset
    preset = find_preset("auth_enable")
    assert preset is not None
    assert preset.name == "auth_enable"


def test_auth_enable_contributor_returns_expected_policy_id() -> None:
    """auth_enable contributor must yield the auth_extension_public policy."""
    _import_preset_registry()
    from dynastore.modules.storage.presets.registry import find_preset
    preset = find_preset("auth_enable")
    contributor = preset._contributor_factory()
    policy_ids = [p.id for p in contributor.get_policies()]
    assert "auth_extension_public" in policy_ids


# ---------------------------------------------------------------------------
# platform_demo includes all 4 new presets
# ---------------------------------------------------------------------------

def test_platform_demo_compose_includes_new_presets() -> None:
    """platform_demo._COMPOSE must list the 3 remaining extension presets from #1462."""
    from dynastore.modules.storage.presets.composites.platform_demo import _COMPOSE
    for name in ("features_enable", "tiles_enable", "auth_enable"):
        assert name in _COMPOSE, (
            f"platform_demo._COMPOSE missing '{name}' — the 3 remaining presets from #1462 "
            f"must be present so a fresh platform_demo apply seeds extension policies."
        )
    assert "search_enable" not in _COMPOSE, (
        "platform_demo._COMPOSE must not include 'search_enable' — "
        "the generic search extension has been deleted."
    )
