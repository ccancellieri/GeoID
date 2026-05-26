"""Unit tests for the ``default_roles_baseline`` preset.

All DB calls are mocked via ``unittest.mock``.  Tests run serially
under ``pytest -p no:xdist -p no:logging``.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from dynastore.models.auth_models import Role
from dynastore.models.protocols.authorization import (
    _DEFAULT_CATALOG_ROLES,
    _DEFAULT_PLATFORM_ROLES,
)
from dynastore.modules.iam.presets.default_roles_baseline import (
    DefaultRolesBaseline,
    _HIERARCHY_EDGES,
    _upsert_role,
)
from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    NoParams,
    PresetContext,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_iam(
    *,
    existing_roles: Optional[Dict[str, Role]] = None,
) -> MagicMock:
    """Return a mock IamService.

    ``update_role`` returns the role if it exists in ``existing_roles``,
    otherwise None.  ``create_role`` records the call and returns the role.
    ``delete_role`` / ``remove_role_hierarchy`` succeed silently.
    """
    roles: Dict[str, Role] = dict(existing_roles or {})
    created: List[Role] = []

    iam = MagicMock()

    async def _update_role(role: Role, catalog_id: Any = None) -> Optional[Role]:
        if role.name in roles:
            roles[role.name] = role
            return role
        return None

    async def _create_role(role: Role, catalog_id: Any = None) -> Role:
        created.append(role)
        roles[role.name] = role
        return role

    async def _delete_role(name: str, cascade: bool = False, catalog_id: Any = None) -> bool:
        roles.pop(name, None)
        return True

    async def _add_role_hierarchy(parent_role: str, child_role: str, catalog_id: Any = None) -> None:
        return None

    async def _remove_role_hierarchy(parent_role: str, child_role: str, catalog_id: Any = None) -> bool:
        return True

    iam.update_role = AsyncMock(side_effect=_update_role)
    iam.create_role = AsyncMock(side_effect=_create_role)
    iam.delete_role = AsyncMock(side_effect=_delete_role)
    iam.add_role_hierarchy = AsyncMock(side_effect=_add_role_hierarchy)
    iam.remove_role_hierarchy = AsyncMock(side_effect=_remove_role_hierarchy)
    iam._roles = roles
    iam._created = created
    return iam


def _make_ctx(iam: Any) -> PresetContext:
    return PresetContext(
        db=None,
        iam=iam,
        policy=None,
        config=None,
        tasks=None,
        cron=None,
        libs=None,
        principal=None,
        scope="platform",
    )


_PRESET = DefaultRolesBaseline()
_ALL_EXPECTED_ROLE_NAMES = [s.name for s in list(_DEFAULT_PLATFORM_ROLES) + list(_DEFAULT_CATALOG_ROLES)]


# ---------------------------------------------------------------------------
# apply on empty DB writes all expected roles + hierarchy
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_writes_all_roles_on_empty_db():
    iam = _make_iam()
    ctx = _make_ctx(iam)

    descriptor = await _PRESET.apply(NoParams(), "platform", ctx)

    assert isinstance(descriptor, AppliedDescriptor)
    written = descriptor.payload["role_names"]
    assert sorted(written) == sorted(_ALL_EXPECTED_ROLE_NAMES)

    # All roles were created (none pre-existed).
    assert iam.create_role.await_count == len(_ALL_EXPECTED_ROLE_NAMES)

    # Every hierarchy edge was registered.
    assert iam.add_role_hierarchy.await_count == len(_HIERARCHY_EDGES)


# ---------------------------------------------------------------------------
# revoke removes the roles written by apply
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_revoke_removes_roles_written_by_apply():
    all_roles = {s.name: Role(name=s.name, policies=list(s.policies)) for s in list(_DEFAULT_PLATFORM_ROLES) + list(_DEFAULT_CATALOG_ROLES)}
    iam = _make_iam(existing_roles=all_roles)
    ctx = _make_ctx(iam)

    descriptor = AppliedDescriptor(payload={
        "role_names": _ALL_EXPECTED_ROLE_NAMES,
        "hierarchy_edges": [[p, c] for p, c in _HIERARCHY_EDGES],
    })

    await _PRESET.revoke(descriptor, ctx)

    assert iam.delete_role.await_count == len(_ALL_EXPECTED_ROLE_NAMES)
    assert iam.remove_role_hierarchy.await_count == len(_HIERARCHY_EDGES)


# ---------------------------------------------------------------------------
# idempotent re-apply when roles already exist
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_is_idempotent_when_roles_exist():
    all_roles = {s.name: Role(name=s.name, policies=list(s.policies)) for s in list(_DEFAULT_PLATFORM_ROLES) + list(_DEFAULT_CATALOG_ROLES)}
    iam = _make_iam(existing_roles=all_roles)
    ctx = _make_ctx(iam)

    descriptor = await _PRESET.apply(NoParams(), "platform", ctx)

    # update_role called for each seed, create_role never called (all existed).
    assert iam.update_role.await_count == len(_ALL_EXPECTED_ROLE_NAMES)
    assert iam.create_role.await_count == 0

    # Descriptor still lists all role names.
    assert sorted(descriptor.payload["role_names"]) == sorted(_ALL_EXPECTED_ROLE_NAMES)


# ---------------------------------------------------------------------------
# dry_run returns plan without any writes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dry_run_returns_plan_without_writes():
    iam = _make_iam()
    ctx = _make_ctx(iam)

    plan = await _PRESET.dry_run(NoParams(), "platform", ctx)

    assert plan.preset_name == "default_roles_baseline"
    assert plan.scope_key == "platform"
    # No IAM calls.
    iam.create_role.assert_not_called()
    iam.update_role.assert_not_called()
    iam.add_role_hierarchy.assert_not_called()

    # Plan describes all roles and hierarchy edges.
    kinds = [e.kind for e in plan.entries]
    assert "upsert_role" in kinds
    assert "add_role_hierarchy" in kinds


# ---------------------------------------------------------------------------
# apply preserves operator-added roles not in the preset
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_preserves_operator_added_roles():
    operator_role = Role(name="operator_custom", policies=["some_policy"])
    existing = {"operator_custom": operator_role}
    iam = _make_iam(existing_roles=existing)
    ctx = _make_ctx(iam)

    descriptor = await _PRESET.apply(NoParams(), "platform", ctx)

    # operator_custom was not touched.
    delete_calls = [call.args[0] for call in iam.delete_role.await_args_list]
    assert "operator_custom" not in delete_calls

    # Preset only wrote its own roles.
    assert "operator_custom" not in descriptor.payload["role_names"]


# ---------------------------------------------------------------------------
# Self-lockout guard fires on revoke when "iam" is in keywords
# ---------------------------------------------------------------------------

def test_keywords_contain_iam():
    """``"iam"`` in keywords activates the self-lockout guard in the lifecycle."""
    assert "iam" in DefaultRolesBaseline.keywords


# ---------------------------------------------------------------------------
# _upsert_role helper: update path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_upsert_role_calls_update_when_role_exists():
    existing = Role(name="admin", policies=[])
    iam = _make_iam(existing_roles={"admin": existing})

    role = Role(name="admin", description="updated", policies=["new_policy"])
    await _upsert_role(iam, role)

    iam.update_role.assert_awaited_once()
    iam.create_role.assert_not_called()


# ---------------------------------------------------------------------------
# _upsert_role helper: create path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_upsert_role_calls_create_when_role_absent():
    iam = _make_iam()

    role = Role(name="brand_new", description="d", policies=[])
    await _upsert_role(iam, role)

    iam.create_role.assert_awaited_once()
    iam.update_role.assert_awaited_once()
