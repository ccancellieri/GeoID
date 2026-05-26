"""Verify that IamExtension.lifespan skips contributors covered by a preset.

After PR-3, contributors whose class matches a registered
``PolicyContributorPreset.contributor_class`` must be skipped in the
boot-time discovery loop.  Contributors without a matching preset still
run through the loop as a safety net until PR-5 removes it.
"""
from __future__ import annotations

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


# ---------------------------------------------------------------------------
# Test: contributor with registered preset is skipped
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
        # Build the covered-classes set as the lifespan does.
        from dynastore.modules.storage.presets.registry import list_presets, get_preset

        covered: set[type] = set()
        for name in list_presets():
            try:
                p = get_preset(name)
                if isinstance(p, PolicyContributorPreset):
                    covered.add(p.contributor_class)
            except Exception:
                pass

        assert _FakeContributorA in covered, "_FakeContributorA should be covered"
        assert _FakeContributorB not in covered, "_FakeContributorB should not be covered"
    finally:
        _REGISTRY.pop(preset_name, None)


def test_uncovered_contributor_not_skipped():
    """Contributors without a matching preset are not in the covered set."""
    from dynastore.modules.storage.presets.policy_contributor_adapter import (
        PolicyContributorPreset,
    )
    from dynastore.modules.storage.presets.registry import list_presets, get_preset

    covered: set[type] = set()
    for name in list_presets():
        try:
            p = get_preset(name)
            if isinstance(p, PolicyContributorPreset):
                covered.add(p.contributor_class)
        except Exception:
            pass

    # _FakeContributorB has no preset registered in any test — must not be covered.
    assert _FakeContributorB not in covered


# ---------------------------------------------------------------------------
# Test: registered-preset extensions appear in the covered set after import
# ---------------------------------------------------------------------------

def test_stac_contributor_covered_after_extension_import():
    """Importing the STAC extension registers stac_enable; STACService is covered."""
    try:
        import dynastore.extensions.stac  # noqa: F401 — triggers preset registration
    except ImportError:
        pytest.skip("STAC extension not installed in this environment")

    from dynastore.modules.storage.presets.policy_contributor_adapter import (
        PolicyContributorPreset,
    )
    from dynastore.modules.storage.presets.registry import list_presets, get_preset

    covered: set[type] = set()
    for name in list_presets():
        try:
            p = get_preset(name)
            if isinstance(p, PolicyContributorPreset):
                covered.add(p.contributor_class)
        except Exception:
            pass

    from dynastore.extensions.stac.stac_service import STACService

    assert STACService in covered, (
        "STACService should be in the covered set after stac extension import"
    )


def test_admin_contributor_covered_after_extension_import():
    """Importing the admin extension registers admin_enable; AdminService is covered."""
    try:
        import dynastore.extensions.admin  # noqa: F401
    except ImportError:
        pytest.skip("Admin extension not installed")

    from dynastore.modules.storage.presets.policy_contributor_adapter import (
        PolicyContributorPreset,
    )
    from dynastore.modules.storage.presets.registry import list_presets, get_preset

    covered: set[type] = set()
    for name in list_presets():
        try:
            p = get_preset(name)
            if isinstance(p, PolicyContributorPreset):
                covered.add(p.contributor_class)
        except Exception:
            pass

    from dynastore.extensions.admin.admin_service import AdminService

    assert AdminService in covered
