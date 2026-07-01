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

"""Unit pins for the processes extension's IAM policy declarations
(un-fao/GeoID#2222).

Mirrors the mocking style of
``tests/dynastore/modules/iam/unit/test_evaluate_access_collection_scope.py``:
``PolicyService`` is exercised directly with a fake ``iam_storage`` (role
name -> ``Role``) and a fake ``get_policy`` lookup, so no DB / app is
needed. Conditions (``catalog_admin_required`` / ``catalog_membership_required``)
run through the real condition registry — only ``IamQueryProtocol`` (which
resolves catalog membership from storage) is left unregistered, so a
non-sysadmin principal without an explicit role-bypass is denied, exactly
as it would be in a deployment with no membership data for that principal.

Covers:
  - Platform-scope execution denies a non-sysadmin caller, allows sysadmin.
  - Catalog-scope execution denies a caller without catalog-admin standing,
    allows a caller carrying the sysadmin bypass role.
  - Platform-scope listing is open to anonymous.
  - Catalog-scope listing fails closed for anonymous (matches the tasks
    extension's ``tasks_read`` behaviour).
  - The preset registers all four policies and is discoverable via the
    extension's normal import chain.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest

from dynastore.extensions.processes.policies import (
    processes_policies,
    processes_role_bindings,
)
from dynastore.models.auth import Policy
from dynastore.modules.iam.conditions import EvaluationContext
from dynastore.modules.iam.models import Role
from dynastore.modules.iam.policies import PolicyService
from dynastore.models.protocols.authorization import IamRolesConfig


_POLICIES_BY_ID: Dict[str, Policy] = {p.id: p for p in processes_policies()}


def _roles_by_name() -> Dict[str, Role]:
    """Merge ``processes_role_bindings()`` into one ``Role`` per name,
    the way ``PolicyContributorPreset.apply`` additively binds policies to
    an existing role (multiple ``Role`` entries for the same name are not
    a bug — see ``tasks_role_bindings`` for the same pattern)."""
    merged: Dict[str, List[str]] = {}
    for role in processes_role_bindings():
        merged.setdefault(role.name, [])
        for pid in role.policies:
            if pid not in merged[role.name]:
                merged[role.name].append(pid)
    return {name: Role(id=name, name=name, policies=pids) for name, pids in merged.items()}


_ROLES_BY_NAME = _roles_by_name()


class _FakeIamStorage:
    async def get_role(self, role_id: str, schema: str = "iam", **_: Any) -> Optional[Role]:
        return _ROLES_BY_NAME.get(role_id)


def _service() -> PolicyService:
    svc = PolicyService.__new__(PolicyService)
    svc._state = None  # type: ignore[attr-defined]
    svc._engine = None  # type: ignore[attr-defined]
    svc.storage = None  # type: ignore[attr-defined]
    svc.iam_storage = _FakeIamStorage()  # type: ignore[attr-defined]
    svc._role_config = None  # type: ignore[attr-defined]

    async def _get_policy(pid: str, catalog_id: Any = None) -> Optional[Policy]:
        return _POLICIES_BY_ID.get(pid)

    async def _fixed_schema(catalog_id: Any, conn: Any = None) -> str:
        # No CatalogsProtocol registered in this pure-unit context — a fixed
        # non-"iam" schema for any given catalog_id is all evaluate_access
        # needs (it is only used to pick the catalog-vs-global policy lookup
        # branch, which _get_policy above ignores anyway).
        return "s_cat1" if catalog_id else "iam"

    svc.get_policy = _get_policy  # type: ignore[assignment,method-assign]
    svc._resolve_schema = _fixed_schema  # type: ignore[assignment,method-assign]
    return svc


async def _call(
    *,
    principals: List[str],
    path: str,
    method: str,
    catalog_id: Optional[str] = None,
    request_context: Optional[EvaluationContext] = None,
) -> tuple[bool, str]:
    return await _service().evaluate_access(
        principals=principals,
        path=path,
        method=method,
        catalog_id=catalog_id,
        request_context=request_context,
    )


def _ctx(catalog_id: str, principal_obj: Optional[Any]) -> EvaluationContext:
    return EvaluationContext(
        request=None,
        storage=None,  # type: ignore[arg-type]
        catalog_id=catalog_id,
        extras={"principal_obj": principal_obj},
    )


# ---------------------------------------------------------------------------
# Role-binding audience (structural — no evaluate_access needed)
# ---------------------------------------------------------------------------

def test_role_bindings_audience():
    cfg = IamRolesConfig()
    assert set(_ROLES_BY_NAME[cfg.sysadmin_role_name].policies) == {
        "processes_system_execute",
        "processes_admin",
    }
    assert _ROLES_BY_NAME[cfg.admin_role_name].policies == ["processes_admin"]
    assert set(_ROLES_BY_NAME[cfg.anonymous_role_name].policies) == {
        "processes_public_read",
        "processes_read",
    }


def test_policies_declares_expected_ids():
    assert set(_POLICIES_BY_ID) == {
        "processes_public_read",
        "processes_read",
        "processes_system_execute",
        "processes_admin",
    }


# ---------------------------------------------------------------------------
# Platform-scope execution
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_platform_execute_denies_non_sysadmin():
    allowed, reason = await _call(
        principals=[IamRolesConfig().admin_role_name],
        path="/processes/processes/gdal/execution",
        method="POST",
    )
    assert allowed is False, reason
    assert "Deny by Default" in reason


@pytest.mark.asyncio
async def test_platform_execute_allows_sysadmin():
    allowed, reason = await _call(
        principals=[IamRolesConfig().sysadmin_role_name],
        path="/processes/processes/gdal/execution",
        method="POST",
    )
    assert allowed is True, reason
    assert "processes_system_execute" in reason


# ---------------------------------------------------------------------------
# Catalog/collection-scope execution
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_catalog_execute_denies_non_catalog_admin():
    """An ``admin``-bound caller reaches ``processes_admin`` by resource/action
    match, but ``catalog_admin_required`` denies because there is no
    IamQueryProtocol registered to resolve catalog membership and the
    principal does not carry the sysadmin bypass role — exactly the outcome
    a real deployment gives a caller with no catalog-admin standing."""
    principal = SimpleNamespace(roles=["admin"], provider="test", subject_id="u1")
    allowed, reason = await _call(
        principals=[IamRolesConfig().admin_role_name],
        path="/processes/catalogs/cat1/processes/gdal/execution",
        method="POST",
        catalog_id="cat1",
        request_context=_ctx("cat1", principal),
    )
    assert allowed is False, reason
    assert "Deny by Default" in reason


@pytest.mark.asyncio
async def test_catalog_execute_denies_anonymous():
    allowed, reason = await _call(
        principals=[IamRolesConfig().admin_role_name],
        path="/processes/catalogs/cat1/processes/gdal/execution",
        method="POST",
        catalog_id="cat1",
        request_context=_ctx("cat1", None),
    )
    assert allowed is False, reason


@pytest.mark.asyncio
async def test_catalog_execute_allows_sysadmin_bypass():
    """A principal carrying the sysadmin role bypasses catalog_admin_required
    directly (no membership lookup needed) — mirrors how a real sysadmin
    reaches catalog-scoped execution."""
    principal = SimpleNamespace(roles=["sysadmin"], provider="test", subject_id="u1")
    allowed, reason = await _call(
        principals=[IamRolesConfig().sysadmin_role_name],
        path="/processes/catalogs/cat1/processes/gdal/execution",
        method="POST",
        catalog_id="cat1",
        request_context=_ctx("cat1", principal),
    )
    assert allowed is True, reason
    assert "processes_admin" in reason


@pytest.mark.asyncio
async def test_collection_execute_denies_non_catalog_admin():
    principal = SimpleNamespace(roles=["admin"], provider="test", subject_id="u1")
    allowed, reason = await _call(
        principals=[IamRolesConfig().admin_role_name],
        path="/processes/catalogs/cat1/collections/coll1/processes/gdal/execution",
        method="POST",
        catalog_id="cat1",
        request_context=_ctx("cat1", principal),
    )
    assert allowed is False, reason


# ---------------------------------------------------------------------------
# Listing / read — open at platform scope, membership-gated at catalog scope
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_platform_listing_open_for_anonymous():
    allowed, reason = await _call(
        principals=[IamRolesConfig().anonymous_role_name],
        path="/processes/processes",
        method="GET",
    )
    assert allowed is True, reason
    assert "processes_public_read" in reason


@pytest.mark.asyncio
async def test_platform_job_status_open_for_anonymous():
    allowed, reason = await _call(
        principals=[IamRolesConfig().anonymous_role_name],
        path="/processes/jobs/11111111-1111-1111-1111-111111111111",
        method="GET",
    )
    assert allowed is True, reason


@pytest.mark.asyncio
async def test_catalog_listing_denies_anonymous_without_membership():
    """Matches tasks_read's documented behaviour: catalog_membership_required
    fails closed for anonymous requests even though the read policy itself
    is bound to the anonymous/base role."""
    allowed, reason = await _call(
        principals=[IamRolesConfig().anonymous_role_name],
        path="/processes/catalogs/cat1/processes",
        method="GET",
        catalog_id="cat1",
        request_context=_ctx("cat1", None),
    )
    assert allowed is False, reason
    assert "Deny by Default" in reason


@pytest.mark.asyncio
async def test_catalog_listing_allows_member_via_sysadmin_bypass():
    principal = SimpleNamespace(roles=["sysadmin"], provider="test", subject_id="u1")
    allowed, reason = await _call(
        principals=[IamRolesConfig().anonymous_role_name],
        path="/processes/catalogs/cat1/processes",
        method="GET",
        catalog_id="cat1",
        request_context=_ctx("cat1", principal),
    )
    assert allowed is True, reason
    assert "processes_read" in reason


# ---------------------------------------------------------------------------
# Resource-pattern precision — execution paths must not leak into read
# policies, and vice versa.
# ---------------------------------------------------------------------------

def test_public_read_pattern_excludes_execution_path():
    pol = _POLICIES_BY_ID["processes_public_read"]
    assert pol.matches_resource("/processes/processes/gdal")
    assert not pol.matches_resource("/processes/processes/gdal/execution")


def test_admin_execute_pattern_excludes_bare_listing():
    pol = _POLICIES_BY_ID["processes_admin"]
    assert pol.matches_resource("/processes/catalogs/cat1/processes/gdal/execution")
    assert pol.matches_resource(
        "/processes/catalogs/cat1/collections/coll1/processes/gdal/execution"
    )
    assert not pol.matches_resource("/processes/catalogs/cat1/processes")


# ---------------------------------------------------------------------------
# Preset discovery
# ---------------------------------------------------------------------------

def test_preset_is_registered_via_extension_import():
    """Importing the processes extension package (the same import the
    entry-point loader triggers) must register the ``processes_enable``
    preset — a policies.py nobody imports would be dead code."""
    import dynastore.extensions.processes  # noqa: F401  -- triggers presets side-effect
    from dynastore.modules.storage.presets.registry import get_preset

    preset = get_preset("processes_enable")
    assert preset.name == "processes_enable"

    contributor = preset.contributor_class()
    policy_ids = {p.id for p in contributor.get_policies()}
    assert policy_ids == set(_POLICIES_BY_ID)
