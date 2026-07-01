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

"""Acceptance test: IAM is just another optional extension.

PR-5 acceptance gate: dynastore must be capable of booting without the
``iam`` extension installed. These tests verify the structural invariants
that make boot-without-iam possible:

1. No core module has a hard import of ``dynastore.extensions.iam.*``.
2. ``PolicyContributor`` protocol is fully deleted.
3. ``provision_default_policies`` is removed from ``PolicyService`` and
   ``PermissionProtocol``.
4. Core modules that previously depended on boot-time IAM seeding no longer
   call ``provision_default_policies`` at lifespan.
5. The preset registry, preset admin routes, and the lifespan are free of
   hard-wired IAM references in core.

The full round-trip "spin up the app without IAM and call /health" requires
a live database and is exercised by the integration suite. These unit tests
assert the static structure that makes optionality possible.
"""
from __future__ import annotations

import ast
import importlib.util
import pathlib

import pytest

# ---------------------------------------------------------------------------
# Fixture: root of the packages tree (relative to this test file)
# ---------------------------------------------------------------------------

_PACKAGES_ROOT = pathlib.Path(__file__).parents[6]  # geoid/packages/
_CORE_SRC = _PACKAGES_ROOT / "core" / "src"


def _iter_py_files(root: pathlib.Path):
    yield from root.rglob("*.py")


def _imports_in_file(path: pathlib.Path) -> list[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (SyntaxError, OSError):
        return []
    result = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            result.append(ast.unparse(node))
    return result


# ---------------------------------------------------------------------------
# 1. PolicyContributor protocol is deleted
# ---------------------------------------------------------------------------

def test_policy_contributor_protocol_not_importable():
    """dynastore.models.protocols.policy_contributor must not exist."""
    spec = importlib.util.find_spec(
        "dynastore.models.protocols.policy_contributor"
    )
    assert spec is None, (
        "dynastore.models.protocols.policy_contributor is still importable; "
        "the PolicyContributor protocol file must be deleted (PR-5)."
    )


def test_no_policy_contributor_import_in_core():
    """No core source file imports from policy_contributor."""
    hits = []
    for py in _iter_py_files(_CORE_SRC):
        for stmt in _imports_in_file(py):
            if "policy_contributor" in stmt and "PolicyContributorPreset" not in stmt:
                hits.append(f"{py.relative_to(_CORE_SRC)}: {stmt}")
    assert not hits, (
        "Core source files still import from policy_contributor:\n" + "\n".join(hits)
    )


# ---------------------------------------------------------------------------
# 2. No hard imports of dynastore.extensions.iam in core
# ---------------------------------------------------------------------------

def test_no_hard_iam_extension_imports_in_core():
    """Core modules must not hard-import from dynastore.extensions.iam.

    Lazy/guarded imports in conditions.py for membership_cache are the only
    documented exception — they are inside try blocks and are not executed
    when the iam extension is absent.
    """
    unexpected = []
    for py in _iter_py_files(_CORE_SRC):
        for stmt in _imports_in_file(py):
            if "dynastore.extensions.iam" not in stmt:
                continue
            rel = py.relative_to(_CORE_SRC)
            # conditions.py has two guarded lazy imports — expected.
            if "conditions.py" in str(rel):
                continue
            unexpected.append(f"{rel}: {stmt}")

    assert not unexpected, (
        "Core source files have hard imports of dynastore.extensions.iam "
        "(IAM must be optional):\n" + "\n".join(unexpected)
    )


# ---------------------------------------------------------------------------
# 3. provision_default_policies removed from PolicyService + PermissionProtocol
# ---------------------------------------------------------------------------

def test_policy_service_no_provision_default_policies():
    """PolicyService must not have provision_default_policies after PR-5."""
    from dynastore.modules.iam.policies import PolicyService

    assert not hasattr(PolicyService, "provision_default_policies"), (
        "PolicyService.provision_default_policies still present — delete it (PR-5)."
    )


def test_permission_protocol_no_provision_default_policies():
    """PermissionProtocol must not declare provision_default_policies."""
    from dynastore.models.protocols.policies import PermissionProtocol

    assert not hasattr(PermissionProtocol, "provision_default_policies"), (
        "PermissionProtocol.provision_default_policies still declared — delete it (PR-5)."
    )


# ---------------------------------------------------------------------------
# 4. IamModule does not have a provision_default_policies wrapper
# ---------------------------------------------------------------------------

def test_iam_module_no_provision_default_policies():
    """IamModule must not expose provision_default_policies after PR-5."""
    # IamModule is guarded by the distribution presence check; when the iam
    # extension is not installed the import raises ImportError.  Use
    # importlib.util.find_spec to avoid that guard in the unit suite.
    spec = importlib.util.find_spec("dynastore.modules.iam.module")
    if spec is None:
        pytest.skip("dynastore.modules.iam.module not available in this environment")

    # Parse statically so we don't execute the distribution-presence guard.
    path = pathlib.Path(spec.origin)  # type: ignore[arg-type]
    tree = ast.parse(path.read_text(encoding="utf-8"))
    method_names = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
    }
    assert "provision_default_policies" not in method_names, (
        "IamModule.provision_default_policies still defined in module.py after PR-5."
    )


# ---------------------------------------------------------------------------
# 5. IamExtension.lifespan does not reference PolicyContributor
# ---------------------------------------------------------------------------

def test_iam_extension_lifespan_no_policy_contributor():
    """IamExtension.lifespan must not reference PolicyContributor."""
    spec = importlib.util.find_spec("dynastore.extensions.iam.service")
    if spec is None:
        pytest.skip("dynastore.extensions.iam.service not available")

    path = pathlib.Path(spec.origin)  # type: ignore[arg-type]
    source = path.read_text(encoding="utf-8")
    assert "PolicyContributor" not in source, (
        "service.py still references PolicyContributor — "
        "the contributor discovery loop must be fully removed (PR-5)."
    )


# ---------------------------------------------------------------------------
# 6. The public_access_normalize migration exists and is idempotent
# ---------------------------------------------------------------------------

def test_normalize_public_access_migration_exists():
    """The normalize_public_access migration module must be importable."""
    spec = importlib.util.find_spec(
        "dynastore.modules.iam.migrations.normalize_public_access"
    )
    assert spec is not None, (
        "normalize_public_access migration module not found; create it (PR-5)."
    )


def test_normalize_public_access_removes_stale_catchall():
    """_normalize_resources replaces /web/.* with enumerated safe paths."""
    from dynastore.modules.iam.migrations.normalize_public_access import (
        _normalize_resources,
        _STALE_PATTERN,
        _SAFE_WEB_PATHS,
    )

    resources_with_stale = ["/health", _STALE_PATTERN, "/auth/.*"]
    result = _normalize_resources(resources_with_stale)

    assert result is not None, "Expected normalization but got None"
    assert _STALE_PATTERN not in result, "Stale pattern still present after normalization"
    for path in _SAFE_WEB_PATHS:
        assert path in result, f"Expected safe path {path!r} missing from result"
    assert "/health" in result, "/health should be preserved"
    assert "/auth/.*" in result, "/auth/.* should be preserved"


def test_normalize_public_access_no_op_when_clean():
    """_normalize_resources returns None when the stale pattern is absent."""
    from dynastore.modules.iam.migrations.normalize_public_access import (
        _normalize_resources,
    )

    clean_resources = ["/health", "/web/?$", "/web/pages/.*"]
    result = _normalize_resources(clean_resources)
    assert result is None, (
        "Expected None (no-op) when stale pattern absent, got resources list"
    )
