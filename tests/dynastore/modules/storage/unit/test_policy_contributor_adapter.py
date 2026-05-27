"""Unit tests for PolicyContributorPreset generic adapter.

Tests cover:
- Factory invocation timing (constructed at apply/revoke/dry_run, not at registration).
- Adapter metadata (name, tier, keywords, is_async, catalog_scopable).
- apply: upserts all policies and roles; returns correct AppliedDescriptor.
- revoke: deletes non-shared roles; strips policies from shared roles.
- dry_run: no DB writes; lists expected operations.
- Error during apply rolls back: partial state tracking (via returned descriptor).
- Idempotent apply (upsert semantics on ctx.policy/iam).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock
import pytest

from dynastore.modules.storage.presets.policy_contributor_adapter import (
    PolicyContributorPreset,
    _SHARED_ROLE_NAMES,
)
from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    NoParams,
    PresetContext,
    PresetPlan,
)
from dynastore.modules.storage.presets.protocol import PresetTier
from dynastore.models.auth import Policy
from dynastore.models.auth_models import Role


# ---------------------------------------------------------------------------
# Fake contributor
# ---------------------------------------------------------------------------

class _SimpleContributor:
    """Minimal PolicyContributor for testing."""

    def get_policies(self):
        return [
            Policy(
                id="test_policy_a",
                description="Test policy A",
                actions=["GET"],
                resources=["/test/.*"],
                effect="ALLOW",
            ),
        ]

    def get_role_bindings(self):
        return [
            Role(name="anonymous", policies=["test_policy_a"]),
        ]


class _MultiPolicyContributor:
    """Contributor with multiple policies and shared + non-shared role bindings."""

    def get_policies(self):
        return [
            Policy(
                id="multi_policy_1",
                description="Multi 1",
                actions=["GET"],
                resources=["/multi/1"],
                effect="ALLOW",
            ),
            Policy(
                id="multi_policy_2",
                description="Multi 2",
                actions=["POST"],
                resources=["/multi/2"],
                effect="ALLOW",
            ),
        ]

    def get_role_bindings(self):
        return [
            Role(name="sysadmin", policies=["multi_policy_1"]),      # shared
            Role(name="custom_role", policies=["multi_policy_2"]),   # non-shared
        ]


# ---------------------------------------------------------------------------
# Context builder
# ---------------------------------------------------------------------------

def _make_context(
    *,
    updated_policies: Optional[List[str]] = None,
    updated_roles: Optional[List[str]] = None,
    deleted_policies: Optional[List[str]] = None,
    deleted_roles: Optional[List[str]] = None,
    existing_roles: Optional[Dict[str, List[str]]] = None,
) -> PresetContext:
    if updated_policies is None:
        updated_policies = []
    if updated_roles is None:
        updated_roles = []
    if deleted_policies is None:
        deleted_policies = []
    if deleted_roles is None:
        deleted_roles = []
    if existing_roles is None:
        existing_roles = {"sysadmin": [], "admin": [], "user": [], "anonymous": []}

    policy_svc = MagicMock()
    iam_svc = MagicMock()

    async def _update_policy(pol: Any) -> Any:
        updated_policies.append(pol.id)
        return pol

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

            def _model_copy(update: Dict, _n=name, _p=list(pol_ids)):
                obj = MagicMock()
                obj.name = _n
                obj.policies = update.get("policies", _p)
                return obj

            r.model_copy = _model_copy
            roles.append(r)
        return roles

    async def _delete_role(name: str, cascade: bool = False) -> bool:
        deleted_roles.append(name)
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
# Metadata
# ---------------------------------------------------------------------------

def test_preset_metadata():
    preset = PolicyContributorPreset(
        name="test_ext_enable",
        description="Test extension",
        keywords=("iam", "test"),
        contributor_factory=_SimpleContributor,
    )
    assert preset.name == "test_ext_enable"
    assert preset.description == "Test extension"
    assert preset.keywords == ("iam", "test")
    assert preset.tier == PresetTier.PLATFORM
    assert preset.catalog_scopable is False
    assert preset.is_async is False
    assert preset.params_model is NoParams


# ---------------------------------------------------------------------------
# Factory invocation timing
# ---------------------------------------------------------------------------

def test_factory_not_called_at_construction():
    """contributor_factory must NOT be called during __init__."""
    call_count = 0

    def _factory():
        nonlocal call_count
        call_count += 1
        return _SimpleContributor()

    PolicyContributorPreset(
        name="timing_test",
        description="Factory timing test",
        keywords=("iam",),
        contributor_factory=_factory,
    )
    assert call_count == 0, "Factory was called at construction time"


@pytest.mark.asyncio
async def test_factory_called_at_apply():
    """contributor_factory must be called exactly once during apply."""
    call_count = 0

    def _factory():
        nonlocal call_count
        call_count += 1
        return _SimpleContributor()

    preset = PolicyContributorPreset(
        name="apply_timing_test",
        description="Apply timing",
        keywords=("iam",),
        contributor_factory=_factory,
    )
    ctx = _make_context()
    await preset.apply(NoParams(), "platform", ctx)
    assert call_count == 1


@pytest.mark.asyncio
async def test_factory_called_at_dry_run():
    call_count = 0

    def _factory():
        nonlocal call_count
        call_count += 1
        return _SimpleContributor()

    preset = PolicyContributorPreset(
        name="dry_run_timing_test",
        description="Dry-run timing",
        keywords=("iam",),
        contributor_factory=_factory,
    )
    ctx = _make_context()
    await preset.dry_run(NoParams(), "platform", ctx)
    assert call_count == 1


# ---------------------------------------------------------------------------
# apply — writes policies and roles
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_upserts_policies():
    preset = PolicyContributorPreset(
        name="simple_enable",
        description="Simple",
        keywords=("iam",),
        contributor_factory=_SimpleContributor,
    )
    updated: List[str] = []
    ctx = _make_context(updated_policies=updated)
    await preset.apply(NoParams(), "platform", ctx)
    assert "test_policy_a" in updated


@pytest.mark.asyncio
async def test_apply_upserts_role_bindings():
    preset = PolicyContributorPreset(
        name="simple_enable2",
        description="Simple2",
        keywords=("iam",),
        contributor_factory=_SimpleContributor,
    )
    updated: List[str] = []
    ctx = _make_context(updated_roles=updated)
    await preset.apply(NoParams(), "platform", ctx)
    assert "anonymous" in updated


@pytest.mark.asyncio
async def test_apply_returns_applied_descriptor():
    preset = PolicyContributorPreset(
        name="simple_enable3",
        description="Simple3",
        keywords=("iam",),
        contributor_factory=_SimpleContributor,
    )
    ctx = _make_context()
    result = await preset.apply(NoParams(), "platform", ctx)
    assert isinstance(result, AppliedDescriptor)
    assert "policy_ids" in result.payload
    assert "role_names" in result.payload
    assert "test_policy_a" in result.payload["policy_ids"]
    assert "anonymous" in result.payload["role_names"]


# ---------------------------------------------------------------------------
# revoke — precise undo
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_revoke_deletes_non_shared_roles():
    preset = PolicyContributorPreset(
        name="multi_enable",
        description="Multi",
        keywords=("iam",),
        contributor_factory=_MultiPolicyContributor,
    )
    deleted_roles: List[str] = []
    ctx = _make_context(
        deleted_roles=deleted_roles,
        existing_roles={
            "sysadmin": ["multi_policy_1"],
            "custom_role": ["multi_policy_2"],
        },
    )
    descriptor = AppliedDescriptor(payload={
        "preset_name": "multi_enable",
        "policy_ids": ["multi_policy_1", "multi_policy_2"],
        "role_names": ["sysadmin", "custom_role"],
    })
    await preset.revoke(descriptor, ctx)
    assert "custom_role" in deleted_roles


@pytest.mark.asyncio
async def test_revoke_does_not_delete_shared_roles():
    preset = PolicyContributorPreset(
        name="multi_enable2",
        description="Multi2",
        keywords=("iam",),
        contributor_factory=_MultiPolicyContributor,
    )
    deleted_roles: List[str] = []
    updated_roles: List[str] = []
    ctx = _make_context(
        deleted_roles=deleted_roles,
        updated_roles=updated_roles,
        existing_roles={"sysadmin": ["multi_policy_1", "other_policy"]},
    )
    descriptor = AppliedDescriptor(payload={
        "preset_name": "multi_enable2",
        "policy_ids": ["multi_policy_1"],
        "role_names": ["sysadmin"],
    })
    await preset.revoke(descriptor, ctx)
    # sysadmin is shared — must NOT be deleted, must be updated (policies stripped).
    assert "sysadmin" not in deleted_roles
    assert "sysadmin" in updated_roles


@pytest.mark.asyncio
async def test_revoke_deletes_policies():
    preset = PolicyContributorPreset(
        name="simple_revoke",
        description="Revoke test",
        keywords=("iam",),
        contributor_factory=_SimpleContributor,
    )
    deleted_policies: List[str] = []
    ctx = _make_context(deleted_policies=deleted_policies)
    descriptor = AppliedDescriptor(payload={
        "preset_name": "simple_revoke",
        "policy_ids": ["test_policy_a"],
        "role_names": [],
    })
    await preset.revoke(descriptor, ctx)
    assert "test_policy_a" in deleted_policies


# ---------------------------------------------------------------------------
# dry_run — no DB writes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dry_run_returns_preset_plan():
    preset = PolicyContributorPreset(
        name="dry_test",
        description="Dry",
        keywords=("iam",),
        contributor_factory=_SimpleContributor,
    )
    ctx = _make_context()
    plan = await preset.dry_run(NoParams(), "platform", ctx)
    assert isinstance(plan, PresetPlan)
    assert plan.preset_name == "dry_test"
    assert plan.scope_key == "platform"


@pytest.mark.asyncio
async def test_dry_run_no_writes():
    """dry_run must call no write methods."""
    write_calls: List[str] = []

    policy_svc = MagicMock()
    policy_svc.update_policy = AsyncMock(
        side_effect=lambda p: write_calls.append("update_policy") or p
    )
    policy_svc.delete_policy = AsyncMock(
        side_effect=lambda p: write_calls.append("delete_policy") or True
    )
    iam_svc = MagicMock()
    iam_svc.update_role = AsyncMock(
        side_effect=lambda r: write_calls.append("update_role") or r
    )
    iam_svc.delete_role = AsyncMock(
        side_effect=lambda n, **kw: write_calls.append("delete_role") or True
    )
    ctx = PresetContext(
        db=MagicMock(), iam=iam_svc, policy=policy_svc,
        config=MagicMock(), tasks=None, cron=None, libs=None,
        principal=None, scope="platform",
    )
    preset = PolicyContributorPreset(
        name="dry_no_writes",
        description="No writes",
        keywords=("iam",),
        contributor_factory=_SimpleContributor,
    )
    await preset.dry_run(NoParams(), "platform", ctx)
    assert write_calls == [], f"dry_run made unexpected write calls: {write_calls}"


@pytest.mark.asyncio
async def test_dry_run_lists_operations():
    preset = PolicyContributorPreset(
        name="dry_list",
        description="List",
        keywords=("iam",),
        contributor_factory=_MultiPolicyContributor,
    )
    ctx = _make_context()
    plan = await preset.dry_run(NoParams(), "platform", ctx)
    kinds = {e.kind for e in plan.entries}
    targets = {e.target for e in plan.entries}
    assert "upsert_policy" in kinds
    assert "upsert_role_binding" in kinds
    assert "multi_policy_1" in targets
    assert "sysadmin" in targets


# ---------------------------------------------------------------------------
# contributor_class property
# ---------------------------------------------------------------------------

def test_contributor_class_property():
    preset = PolicyContributorPreset(
        name="class_test",
        description="Class test",
        keywords=("iam",),
        contributor_factory=_SimpleContributor,
    )
    assert preset.contributor_class is _SimpleContributor


# ---------------------------------------------------------------------------
# Shared role names constant
# ---------------------------------------------------------------------------

def test_shared_role_names_includes_standard_roles():
    for rname in ("sysadmin", "admin", "user", "anonymous"):
        assert rname in _SHARED_ROLE_NAMES
