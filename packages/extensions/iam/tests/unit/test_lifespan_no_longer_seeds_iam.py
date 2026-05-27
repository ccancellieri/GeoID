"""Verify that IamExtension.lifespan does NOT seed IAM service policies.

After PR-2 the lifespan contributor loop no longer includes IamExtension
itself as a PolicyContributor. After PR-5 the PolicyContributor protocol is
deleted entirely and the discovery loop removed. The IAM-specific policies
live in the iam_baseline preset and are applied explicitly by an operator.
"""
from __future__ import annotations


# ---------------------------------------------------------------------------
# Test: PolicyContributor protocol is gone (PR-5)
# ---------------------------------------------------------------------------

def test_policy_contributor_protocol_deleted():
    """PolicyContributor protocol module must not exist after PR-5."""
    import importlib
    import importlib.util

    spec = importlib.util.find_spec("dynastore.models.protocols.policy_contributor")
    assert spec is None, (
        "dynastore.models.protocols.policy_contributor still importable after PR-5; "
        "the PolicyContributor protocol must be fully deleted."
    )


# ---------------------------------------------------------------------------
# Test: IamExtension does not implement get_policies / get_role_bindings
# ---------------------------------------------------------------------------

def test_iam_extension_has_no_get_policies():
    """IamExtension must not declare get_policies()."""
    from dynastore.extensions.iam.service import IamExtension

    assert not hasattr(IamExtension, "get_policies") or not callable(
        getattr(IamExtension, "get_policies", None)
    ), "IamExtension.get_policies() still exists after PR-5"


def test_iam_extension_has_no_get_role_bindings():
    """IamExtension must not declare get_role_bindings()."""
    from dynastore.extensions.iam.service import IamExtension

    assert not hasattr(IamExtension, "get_role_bindings") or not callable(
        getattr(IamExtension, "get_role_bindings", None)
    ), "IamExtension.get_role_bindings() still exists after PR-5"


# ---------------------------------------------------------------------------
# Test: PermissionProtocol does not declare provision_default_policies (PR-5)
# ---------------------------------------------------------------------------

def test_permission_protocol_no_provision_default_policies():
    """PermissionProtocol must not declare provision_default_policies after PR-5."""
    from dynastore.models.protocols.policies import PermissionProtocol

    assert not hasattr(PermissionProtocol, "provision_default_policies"), (
        "PermissionProtocol.provision_default_policies still present after PR-5"
    )


# ---------------------------------------------------------------------------
# Test: PolicyService does not declare provision_default_policies (PR-5)
# ---------------------------------------------------------------------------

def test_policy_service_no_provision_default_policies():
    """PolicyService must not have provision_default_policies after PR-5."""
    from dynastore.modules.iam.policies import PolicyService

    assert not hasattr(PolicyService, "provision_default_policies"), (
        "PolicyService.provision_default_policies still present after PR-5"
    )


# ---------------------------------------------------------------------------
# Test: iam_baseline preset is registered on IAM extension import
# ---------------------------------------------------------------------------

def test_iam_baseline_preset_registered_on_import():
    """Importing the IAM extension registers iam_baseline in the preset registry."""
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
        "iam_service_policies() is still present in service.py after PR-5"
    )


def test_standalone_iam_service_role_bindings_removed():
    """iam_service_role_bindings() must not exist as a public module-level function."""
    import dynastore.extensions.iam.service as svc_module

    assert not hasattr(svc_module, "iam_service_role_bindings"), (
        "iam_service_role_bindings() is still present in service.py after PR-5"
    )

# ---------------------------------------------------------------------------
# Test: no core import of dynastore.extensions.iam (PR-5 acceptance gate)
# ---------------------------------------------------------------------------

def test_no_core_imports_of_iam_extension():
    """No module under packages/core must import from dynastore.extensions.iam."""
    import ast
    import pathlib

    core_root = pathlib.Path(__file__).parents[6] / "core" / "src"
    violations = []
    for py_file in core_root.rglob("*.py"):
        try:
            tree = ast.parse(py_file.read_text())
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                src = ast.unparse(node)
                if "dynastore.extensions.iam" in src:
                    # lazy/guarded imports in conditions.py are allowed
                    # (they guard the hot path behind a try block)
                    violations.append(f"{py_file.relative_to(core_root)}: {src}")

    # conditions.py has two guarded lazy imports for membership_cache — these
    # are expected and documented; all other core hits are failures.
    unexpected = [v for v in violations if "conditions.py" not in v]
    assert not unexpected, (
        "Core modules must not import from dynastore.extensions.iam:\n"
        + "\n".join(unexpected)
    )
