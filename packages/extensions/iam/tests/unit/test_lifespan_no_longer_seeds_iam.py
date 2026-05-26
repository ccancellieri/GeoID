"""Verify that IamExtension.lifespan does NOT seed IAM service policies.

After PR-2 the lifespan contributor loop no longer includes IamExtension
itself as a PolicyContributor. The only seeding that should happen at lifespan
is the core default-policies call (sysadmin_full_access / public_access /
self_service_access). The IAM-specific policies live in the iam_baseline preset
and are applied explicitly by an operator.
"""
from __future__ import annotations

from unittest.mock import patch, AsyncMock, MagicMock
import pytest


# ---------------------------------------------------------------------------
# Test: IamExtension does not implement get_policies / get_role_bindings
# ---------------------------------------------------------------------------

def test_iam_extension_has_no_get_policies():
    """IamExtension must not declare get_policies() — it was removed in PR-2."""
    from dynastore.extensions.iam.service import IamExtension

    # The method must not exist on the class (removed, not just empty).
    assert not hasattr(IamExtension, "get_policies") or not callable(
        getattr(IamExtension, "get_policies", None)
    ), "IamExtension.get_policies() still exists after PR-2"


def test_iam_extension_has_no_get_role_bindings():
    """IamExtension must not declare get_role_bindings() — it was removed in PR-2."""
    from dynastore.extensions.iam.service import IamExtension

    assert not hasattr(IamExtension, "get_role_bindings") or not callable(
        getattr(IamExtension, "get_role_bindings", None)
    ), "IamExtension.get_role_bindings() still exists after PR-2"


# ---------------------------------------------------------------------------
# Test: IamExtension is NOT a PolicyContributor after PR-2
# ---------------------------------------------------------------------------

def test_iam_extension_not_policy_contributor():
    """IamExtension no longer satisfies PolicyContributor structurally."""
    from dynastore.extensions.iam.service import IamExtension
    from dynastore.models.protocols.policy_contributor import PolicyContributor

    ext = IamExtension.__new__(IamExtension)
    # Should NOT be recognised as PolicyContributor now that the methods are gone.
    assert not isinstance(ext, PolicyContributor), (
        "IamExtension still implements PolicyContributor after PR-2"
    )


# ---------------------------------------------------------------------------
# Test: iam_baseline preset is registered on IAM extension import
# ---------------------------------------------------------------------------

def test_iam_baseline_preset_registered_on_import():
    """Importing the IAM extension registers iam_baseline in the preset registry."""
    # The __init__.py imports presets which registers iam_baseline.
    import dynastore.extensions.iam  # noqa: F401
    from dynastore.modules.storage.presets.registry import get_preset

    preset = get_preset("iam_baseline")
    assert preset is not None
    assert preset.name == "iam_baseline"


# ---------------------------------------------------------------------------
# Test: module-level standalone functions removed from service.py
# ---------------------------------------------------------------------------

def test_standalone_iam_service_policies_removed():
    """iam_service_policies() must not exist as a public module-level function."""
    import dynastore.extensions.iam.service as svc_module

    assert not hasattr(svc_module, "iam_service_policies"), (
        "iam_service_policies() is still present in service.py after PR-2; "
        "it should be a private _iam_service_policies() in iam_baseline.py"
    )


def test_standalone_iam_service_role_bindings_removed():
    """iam_service_role_bindings() must not exist as a public module-level function."""
    import dynastore.extensions.iam.service as svc_module

    assert not hasattr(svc_module, "iam_service_role_bindings"), (
        "iam_service_role_bindings() is still present in service.py after PR-2"
    )
