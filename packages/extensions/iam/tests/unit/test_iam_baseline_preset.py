"""Unit tests for IamBaseline preset — apply, revoke, dry_run, params.

All tests mock the PresetContext to avoid any DB dependency.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from dynastore.extensions.iam.presets.iam_baseline import (
    IamBaseline,
    IamBaselineParams,
    _iam_service_policies,
    _iam_service_role_bindings,
)
from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    PresetContext,
    PresetPlan,
)
from dynastore.modules.storage.presets.protocol import PresetTier


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_preset() -> IamBaseline:
    return IamBaseline()


def _make_context(
    *,
    updated_policies: Optional[List[str]] = None,
    updated_roles: Optional[List[str]] = None,
    deleted_policies: Optional[List[str]] = None,
    existing_roles: Optional[Dict[str, Any]] = None,
) -> PresetContext:
    """Build a mock PresetContext with tracking collections."""
    if updated_policies is None:
        updated_policies = []
    if updated_roles is None:
        updated_roles = []
    if deleted_policies is None:
        deleted_policies = []
    if existing_roles is None:
        existing_roles = {}

    policy_svc = MagicMock()
    iam_svc = MagicMock()

    async def _update_policy(policy: Any) -> Any:
        updated_policies.append(policy.id)
        return policy

    async def _delete_policy(pid: str, catalog_id: Any = None) -> bool:
        deleted_policies.append(pid)
        return True

    async def _update_role(role: Any) -> Any:
        updated_roles.append(role.name)
        return role

    async def _list_roles() -> List[Any]:
        roles = []
        for name, pol_ids in existing_roles.items():
            r = MagicMock()
            r.name = name
            r.policies = list(pol_ids)
            r.model_copy = lambda update, _r=r: type("_R", (), {
                "name": _r.name,
                "policies": update.get("policies", _r.policies),
            })()
            roles.append(r)
        return roles

    async def _delete_role(name: str, cascade: bool = False) -> bool:
        return True

    policy_svc.update_policy = _update_policy
    policy_svc.delete_policy = _delete_policy
    iam_svc.update_role = _update_role
    iam_svc.list_roles = _list_roles
    iam_svc.delete_role = _delete_role

    return PresetContext(
        db=MagicMock(),
        iam=iam_svc,
        policy=policy_svc,
        config=MagicMock(),
        tasks=None,
        cron=None,
        libs=None,
        principal=None,
        scope="platform",
    )


# ---------------------------------------------------------------------------
# Static metadata
# ---------------------------------------------------------------------------

def test_preset_name():
    assert IamBaseline.name == "iam_baseline"


def test_preset_tier():
    assert IamBaseline.tier == PresetTier.PLATFORM


def test_preset_not_catalog_scopable():
    assert IamBaseline.catalog_scopable is False


def test_preset_keywords_include_iam():
    assert "iam" in IamBaseline.keywords


def test_preset_is_not_async():
    assert IamBaseline.is_async is False


def test_params_model():
    assert IamBaseline.params_model is IamBaselineParams


# ---------------------------------------------------------------------------
# IamBaselineParams defaults and validation
# ---------------------------------------------------------------------------

def test_default_delegation_role_names():
    p = IamBaselineParams()
    assert p.delegation_role_names == ["admin"]


def test_custom_delegation_role_names():
    p = IamBaselineParams(delegation_role_names=["catalog_admin", "data_curator"])
    assert "catalog_admin" in p.delegation_role_names
    assert "data_curator" in p.delegation_role_names


def test_empty_delegation_role_names():
    p = IamBaselineParams(delegation_role_names=[])
    assert p.delegation_role_names == []


# ---------------------------------------------------------------------------
# apply — writes expected policies and roles
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_upserts_iam_service_policies():
    preset = _make_preset()
    updated_policies: List[str] = []
    ctx = _make_context(updated_policies=updated_policies)
    params = IamBaselineParams()

    result = await preset.apply(params, "platform", ctx)

    # All IAM service policy ids must appear.
    iam_ids = {p.id for p in _iam_service_policies()}
    for pid in iam_ids:
        assert pid in updated_policies, f"policy {pid!r} not upserted"


@pytest.mark.asyncio
async def test_apply_upserts_admin_catalog_access():
    preset = _make_preset()
    updated_policies: List[str] = []
    ctx = _make_context(updated_policies=updated_policies)
    params = IamBaselineParams()

    await preset.apply(params, "platform", ctx)

    assert "admin_catalog_access" in updated_policies


@pytest.mark.asyncio
async def test_apply_upserts_catalog_preset_delegation():
    preset = _make_preset()
    updated_policies: List[str] = []
    ctx = _make_context(updated_policies=updated_policies)
    params = IamBaselineParams()

    await preset.apply(params, "platform", ctx)

    assert "catalog_preset_delegation" in updated_policies


@pytest.mark.asyncio
async def test_apply_returns_applied_descriptor():
    preset = _make_preset()
    ctx = _make_context()
    params = IamBaselineParams()

    result = await preset.apply(params, "platform", ctx)

    assert isinstance(result, AppliedDescriptor)
    assert "policy_ids" in result.payload
    assert "role_names" in result.payload


@pytest.mark.asyncio
async def test_apply_descriptor_contains_all_policy_ids():
    preset = _make_preset()
    ctx = _make_context()
    params = IamBaselineParams()

    result = await preset.apply(params, "platform", ctx)

    payload = result.payload
    assert "admin_authorization_api" in payload["policy_ids"]
    assert "self_service_authorization_api" in payload["policy_ids"]
    assert "admin_catalog_access" in payload["policy_ids"]
    assert "catalog_preset_delegation" in payload["policy_ids"]


@pytest.mark.asyncio
async def test_apply_upserts_iam_role_bindings():
    preset = _make_preset()
    updated_roles: List[str] = []
    ctx = _make_context(updated_roles=updated_roles)
    params = IamBaselineParams()

    await preset.apply(params, "platform", ctx)

    expected_roles = {r.name for r in _iam_service_role_bindings()}
    for rname in expected_roles:
        assert rname in updated_roles, f"role {rname!r} not upserted"


@pytest.mark.asyncio
async def test_apply_custom_delegation_role_names():
    """Custom delegation_role_names propagate into admin_catalog_access."""
    preset = _make_preset()
    captured_configs: Dict[str, Any] = {}

    policies_written: Dict[str, Any] = {}

    policy_svc = MagicMock()

    async def _capture_policy(policy: Any) -> Any:
        policies_written[policy.id] = policy
        return policy

    policy_svc.update_policy = _capture_policy

    iam_svc = MagicMock()
    iam_svc.update_role = AsyncMock()

    ctx = PresetContext(
        db=MagicMock(),
        iam=iam_svc,
        policy=policy_svc,
        config=MagicMock(),
        tasks=None, cron=None, libs=None, principal=None,
        scope="platform",
    )
    params = IamBaselineParams(delegation_role_names=["catalog_admin"])

    await preset.apply(params, "platform", ctx)

    admin_cat_pol = policies_written.get("admin_catalog_access")
    assert admin_cat_pol is not None
    condition = admin_cat_pol.conditions[0]
    assert condition.config["required_roles"] == ["catalog_admin"]


# ---------------------------------------------------------------------------
# revoke — removes exactly what apply wrote
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_revoke_deletes_iam_service_policies():
    preset = _make_preset()
    deleted_policies: List[str] = []
    ctx = _make_context(deleted_policies=deleted_policies)

    descriptor = AppliedDescriptor(payload={
        "policy_ids": ["admin_authorization_api", "self_service_authorization_api", "catalog_preset_delegation"],
        "role_names": [],
        "delegation_role_names": ["admin"],
    })

    await preset.revoke(descriptor, ctx)

    assert "admin_authorization_api" in deleted_policies
    assert "self_service_authorization_api" in deleted_policies
    assert "catalog_preset_delegation" in deleted_policies


@pytest.mark.asyncio
async def test_revoke_resets_admin_catalog_access_not_deletes():
    """admin_catalog_access should be reset to empty required_roles, not deleted."""
    preset = _make_preset()
    deleted_policies: List[str] = []
    updated_policies: List[str] = []
    ctx = _make_context(deleted_policies=deleted_policies, updated_policies=updated_policies)

    descriptor = AppliedDescriptor(payload={
        "policy_ids": ["admin_catalog_access"],
        "role_names": [],
        "delegation_role_names": ["admin"],
    })

    await preset.revoke(descriptor, ctx)

    assert "admin_catalog_access" not in deleted_policies
    assert "admin_catalog_access" in updated_policies


@pytest.mark.asyncio
async def test_revoke_strips_policies_from_shared_roles():
    """Shared roles (sysadmin, admin, user) must not be deleted — only stripped."""
    preset = _make_preset()
    updated_roles: List[str] = []
    deleted_roles: List[str] = []

    existing_roles = {
        "sysadmin": ["admin_authorization_api", "some_other_policy"],
        "admin": ["admin_authorization_api"],
        "user": ["self_service_authorization_api"],
    }

    policy_svc = MagicMock()

    async def _update_policy(policy: Any) -> Any:
        updated_roles.append(f"policy:{policy.id}")
        return policy

    policy_svc.update_policy = _update_policy
    policy_svc.delete_policy = AsyncMock(return_value=True)

    iam_svc = MagicMock()

    async def _list_roles() -> List[Any]:
        roles = []
        for name, pol_ids in existing_roles.items():
            r = MagicMock()
            r.name = name
            r.policies = list(pol_ids)

            def _model_copy(update: Dict, _pol_ids=list(pol_ids), _name=name):
                obj = MagicMock()
                obj.name = _name
                obj.policies = update.get("policies", _pol_ids)
                return obj

            r.model_copy = _model_copy
            roles.append(r)
        return roles

    async def _update_role(role: Any) -> None:
        updated_roles.append(role.name)

    async def _delete_role(name: str, cascade: bool = False) -> bool:
        deleted_roles.append(name)
        return True

    iam_svc.list_roles = _list_roles
    iam_svc.update_role = _update_role
    iam_svc.delete_role = _delete_role

    ctx = PresetContext(
        db=MagicMock(),
        iam=iam_svc,
        policy=policy_svc,
        config=MagicMock(),
        tasks=None, cron=None, libs=None, principal=None,
        scope="platform",
    )

    descriptor = AppliedDescriptor(payload={
        "policy_ids": [],
        "role_names": ["sysadmin", "admin", "user"],
        "delegation_role_names": ["admin"],
    })

    await preset.revoke(descriptor, ctx)

    # Shared roles must never be deleted.
    for rname in ("sysadmin", "admin", "user"):
        assert rname not in deleted_roles, f"shared role {rname!r} was deleted"
    # They should be updated (policy stripped).
    for rname in ("sysadmin", "admin", "user"):
        assert rname in updated_roles, f"shared role {rname!r} was not updated"


# ---------------------------------------------------------------------------
# dry_run — no DB writes, returns PresetPlan
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dry_run_returns_preset_plan():
    preset = _make_preset()
    ctx = _make_context()
    params = IamBaselineParams()

    plan = await preset.dry_run(params, "platform", ctx)

    assert isinstance(plan, PresetPlan)
    assert plan.preset_name == "iam_baseline"
    assert plan.scope_key == "platform"


@pytest.mark.asyncio
async def test_dry_run_no_db_writes():
    """dry_run must not call any write methods."""
    preset = _make_preset()

    write_calls: List[str] = []

    policy_svc = MagicMock()
    policy_svc.update_policy = AsyncMock(side_effect=lambda p: write_calls.append("update_policy") or p)
    policy_svc.delete_policy = AsyncMock(side_effect=lambda p: write_calls.append("delete_policy") or True)

    iam_svc = MagicMock()
    iam_svc.update_role = AsyncMock(side_effect=lambda r: write_calls.append("update_role") or r)
    iam_svc.delete_role = AsyncMock(side_effect=lambda n, **kw: write_calls.append("delete_role") or True)

    ctx = PresetContext(
        db=MagicMock(),
        iam=iam_svc,
        policy=policy_svc,
        config=MagicMock(),
        tasks=None, cron=None, libs=None, principal=None,
        scope="platform",
    )

    await preset.dry_run(IamBaselineParams(), "platform", ctx)

    assert write_calls == [], f"dry_run made unexpected writes: {write_calls}"


@pytest.mark.asyncio
async def test_dry_run_lists_expected_entries():
    preset = _make_preset()
    ctx = _make_context()
    params = IamBaselineParams()

    plan = await preset.dry_run(params, "platform", ctx)

    kinds = {e.kind for e in plan.entries}
    targets = {e.target for e in plan.entries}

    assert "upsert_policy" in kinds
    assert "upsert_role_binding" in kinds
    assert "admin_catalog_access" in targets
    assert "catalog_preset_delegation" in targets


# ---------------------------------------------------------------------------
# Idempotency — applying twice is safe
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_is_idempotent():
    """Calling apply twice does not raise and produces the same descriptor shape."""
    preset = _make_preset()
    ctx = _make_context()
    params = IamBaselineParams()

    result1 = await preset.apply(params, "platform", ctx)
    # Second call on a fresh context (simulating a force-apply).
    ctx2 = _make_context()
    result2 = await preset.apply(params, "platform", ctx2)

    assert result1.payload.keys() == result2.payload.keys()
    assert result1.payload["policy_ids"] == result2.payload["policy_ids"]


# ---------------------------------------------------------------------------
# allowed_preset_names — safe-subset gate on catalog_preset_delegation (#1426)
# ---------------------------------------------------------------------------

def test_default_allowed_preset_names_is_shape_subset():
    """Default allowlist must be the two catalog-scopable shape presets."""
    p = IamBaselineParams()
    assert p.allowed_preset_names == ["public_catalog", "private_catalog"]


def test_custom_allowed_preset_names_propagate():
    p = IamBaselineParams(allowed_preset_names=["public_catalog"])
    assert p.allowed_preset_names == ["public_catalog"]


@pytest.mark.asyncio
async def test_apply_writes_allowlist_into_catalog_preset_delegation():
    """The catalog_preset_delegation policy condition must carry the allowlist."""
    preset = _make_preset()
    policies_written: Dict[str, Any] = {}

    policy_svc = MagicMock()

    async def _capture_policy(policy: Any) -> Any:
        policies_written[policy.id] = policy
        return policy

    policy_svc.update_policy = _capture_policy
    policy_svc.delete_policy = AsyncMock(return_value=True)
    iam_svc = MagicMock()
    iam_svc.update_role = AsyncMock()
    iam_svc.list_roles = AsyncMock(return_value=[])
    iam_svc.delete_role = AsyncMock(return_value=True)

    ctx = PresetContext(
        db=MagicMock(),
        iam=iam_svc,
        policy=policy_svc,
        config=MagicMock(),
        tasks=None, cron=None, libs=None, principal=None,
        scope="platform",
    )
    params = IamBaselineParams(allowed_preset_names=["public_catalog"])

    await preset.apply(params, "platform", ctx)

    delegation_pol = policies_written.get("catalog_preset_delegation")
    assert delegation_pol is not None
    cond = delegation_pol.conditions[0]
    assert cond.config["required_roles"] == ["admin"]
    assert cond.config["allowed_preset_names"] == ["public_catalog"]


@pytest.mark.asyncio
async def test_apply_does_not_set_allowlist_on_admin_catalog_access():
    """admin_catalog_access gates ALL catalog admin routes, NOT just presets;
    it must NOT carry an allowlist — that would deny non-preset admin calls.
    """
    preset = _make_preset()
    policies_written: Dict[str, Any] = {}

    policy_svc = MagicMock()

    async def _capture_policy(policy: Any) -> Any:
        policies_written[policy.id] = policy
        return policy

    policy_svc.update_policy = _capture_policy
    policy_svc.delete_policy = AsyncMock(return_value=True)
    iam_svc = MagicMock()
    iam_svc.update_role = AsyncMock()
    iam_svc.list_roles = AsyncMock(return_value=[])
    iam_svc.delete_role = AsyncMock(return_value=True)

    ctx = PresetContext(
        db=MagicMock(),
        iam=iam_svc,
        policy=policy_svc,
        config=MagicMock(),
        tasks=None, cron=None, libs=None, principal=None,
        scope="platform",
    )
    params = IamBaselineParams()

    await preset.apply(params, "platform", ctx)

    admin_pol = policies_written.get("admin_catalog_access")
    assert admin_pol is not None
    cond = admin_pol.conditions[0]
    assert "allowed_preset_names" not in cond.config


@pytest.mark.asyncio
async def test_apply_descriptor_carries_allowed_preset_names():
    preset = _make_preset()
    ctx = _make_context()
    params = IamBaselineParams(allowed_preset_names=["public_catalog"])

    result = await preset.apply(params, "platform", ctx)

    assert result.payload["allowed_preset_names"] == ["public_catalog"]


@pytest.mark.asyncio
async def test_dry_run_includes_allowlist_in_delegation_entry():
    preset = _make_preset()
    ctx = _make_context()
    params = IamBaselineParams(allowed_preset_names=["public_catalog"])

    plan = await preset.dry_run(params, "platform", ctx)

    delegation_entries = [
        e for e in plan.entries if e.target == "catalog_preset_delegation"
    ]
    assert len(delegation_entries) == 1
    assert delegation_entries[0].detail.get("allowed_preset_names") == [
        "public_catalog"
    ]
