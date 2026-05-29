"""Verify that IamExtension.lifespan skips contributors covered by a preset.

After the per-extension preset refactor, each extension registers its policy
contributor through a preset module (``dynastore.extensions.<ext>.presets``).
The contributor lives *inside* the preset (a small wrapper class such as
``_STACPolicyContributor``), not on the service class. The lifespan guard
builds its ``covered`` set from each registered
``PolicyContributorPreset.contributor_class`` and skips any discovered
contributor whose class is in that set. These tests assert the registered
preset is a ``PolicyContributorPreset`` and that its ``contributor_class``
lands in the covered set after the extension's preset module is imported.
"""
from __future__ import annotations

import importlib

import pytest


# ---------------------------------------------------------------------------
# Fake contributor + fake service class
# ---------------------------------------------------------------------------

class _FakeContributorA:
    """A contributor that now has a registered PolicyContributorPreset."""

    def get_policies(self):
        from dynastore.models.auth import Policy
        return [Policy(
            id="fake_a_policy",
            description="Fake A",
            actions=["GET"],
            resources=["/fake_a/.*"],
            effect="ALLOW",
        )]

    def get_role_bindings(self):
        return []


class _FakeContributorB:
    """A contributor that does NOT have a registered preset (safety-net path)."""

    def get_policies(self):
        from dynastore.models.auth import Policy
        return [Policy(
            id="fake_b_policy",
            description="Fake B",
            actions=["GET"],
            resources=["/fake_b/.*"],
            effect="ALLOW",
        )]

    def get_role_bindings(self):
        return []


def _covered_contributor_classes() -> set:
    """Build the covered-classes set exactly as the lifespan guard does."""
    from dynastore.modules.storage.presets.policy_contributor_adapter import (
        PolicyContributorPreset,
    )
    from dynastore.modules.storage.presets.registry import find_preset, list_presets

    covered: set = set()
    for name in list_presets():
        preset = find_preset(name)
        if isinstance(preset, PolicyContributorPreset):
            covered.add(preset.contributor_class)
    return covered


# ---------------------------------------------------------------------------
# Test: contributor with a registered preset is covered
# ---------------------------------------------------------------------------

def test_preset_covered_contributor_is_skipped():
    """When a PolicyContributorPreset exists for a class, the lifespan loop skips it."""
    from dynastore.modules.storage.presets.policy_contributor_adapter import (
        PolicyContributorPreset,
    )
    from dynastore.modules.storage.presets.registry import (
        _REGISTRY,
        register_preset,
    )

    preset_name = "_test_fake_a_enable"
    # Clean up in case a previous test left a stale entry.
    _REGISTRY.pop(preset_name, None)

    preset = PolicyContributorPreset(
        name=preset_name,
        description="Fake A extension for test",
        keywords=("iam", "test"),
        contributor_factory=_FakeContributorA,
    )
    register_preset(preset)

    try:
        covered = _covered_contributor_classes()
        assert _FakeContributorA in covered, "_FakeContributorA should be covered"
        assert _FakeContributorB not in covered, "_FakeContributorB should not be covered"
    finally:
        _REGISTRY.pop(preset_name, None)


def test_uncovered_contributor_not_skipped():
    """Contributors without a matching preset are not in the covered set."""
    covered = _covered_contributor_classes()
    # _FakeContributorB has no preset registered in any test — must not be covered.
    assert _FakeContributorB not in covered


# ---------------------------------------------------------------------------
# Test: registered-preset extensions appear in the covered set after import
# ---------------------------------------------------------------------------

def test_stac_contributor_covered_after_extension_import():
    """Importing the STAC preset module registers ``stac_enable``; the wrapper
    contributor class it carries lands in the lifespan covered set."""
    try:
        presets_module = importlib.import_module(
            "dynastore.extensions.stac.presets"
        )
    except ImportError:
        pytest.skip("STAC extension not installed in this environment")

    from dynastore.modules.storage.presets.policy_contributor_adapter import (
        PolicyContributorPreset,
    )
    from dynastore.modules.storage.presets.registry import find_preset

    preset = find_preset("stac_enable")
    assert isinstance(preset, PolicyContributorPreset), (
        "stac_enable must be registered as a PolicyContributorPreset"
    )
    assert preset.contributor_class is presets_module._STACPolicyContributor

    covered = _covered_contributor_classes()
    assert preset.contributor_class in covered, (
        "stac_enable contributor class should be in the covered set after import"
    )


def test_admin_contributor_covered_after_extension_import():
    """Importing the admin preset module registers ``admin_enable``; the wrapper
    contributor class it carries lands in the lifespan covered set."""
    try:
        presets_module = importlib.import_module(
            "dynastore.extensions.admin.presets"
        )
    except ImportError:
        pytest.skip("Admin extension not installed")

    from dynastore.modules.storage.presets.policy_contributor_adapter import (
        PolicyContributorPreset,
    )
    from dynastore.modules.storage.presets.registry import find_preset

    preset = find_preset("admin_enable")
    assert isinstance(preset, PolicyContributorPreset), (
        "admin_enable must be registered as a PolicyContributorPreset"
    )
    assert preset.contributor_class is presets_module._AdminPolicyContributor

    covered = _covered_contributor_classes()
    assert preset.contributor_class in covered, (
        "admin_enable contributor class should be in the covered set after import"
    )
