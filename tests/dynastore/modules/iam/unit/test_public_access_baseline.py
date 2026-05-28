"""Unit tests for the ``public_access_baseline`` preset and bootstrap.

All DB calls are mocked via ``unittest.mock``.  Tests run serially
under ``pytest -p no:xdist -p no:logging``.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.auth_models import Role
from dynastore.modules.iam.presets.public_access_baseline import (
    PublicAccessBaseline,
    _union_policy_into_role,
    _strip_policy_from_role,
    _POLICY_ID,
    _ROLE_NAME,
)
from dynastore.modules.storage.presets.preset import (
    AppliedDescriptor,
    NoParams,
    PresetContext,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_iam(*, existing_roles: Optional[Dict[str, Role]] = None) -> MagicMock:
    """Return a mock IamService with list_roles, update_role, create_role."""
    roles: Dict[str, Role] = dict(existing_roles or {})

    iam = MagicMock()

    async def _list_roles(catalog_id: Any = None) -> List[Role]:
        return list(roles.values())

    async def _update_role(role: Role, catalog_id: Any = None) -> Optional[Role]:
        if role.name in roles:
            roles[role.name] = role
            return role
        return None

    async def _create_role(role: Role, catalog_id: Any = None) -> Role:
        roles[role.name] = role
        return role

    async def _delete_role(name: str, cascade: bool = False, catalog_id: Any = None) -> bool:
        roles.pop(name, None)
        return True

    iam.list_roles = AsyncMock(side_effect=_list_roles)
    iam.update_role = AsyncMock(side_effect=_update_role)
    iam.create_role = AsyncMock(side_effect=_create_role)
    iam.delete_role = AsyncMock(side_effect=_delete_role)
    iam._roles = roles
    return iam


def _make_policy_svc(*, existing_policy_ids: Optional[List[str]] = None) -> MagicMock:
    """Return a mock PolicyService with update_policy, create_policy, delete_policy."""
    policy_ids: List[str] = list(existing_policy_ids or [])

    svc = MagicMock()

    async def _update_policy(policy: Any, catalog_id: Any = None) -> Optional[Any]:
        if policy.id in policy_ids:
            return policy
        return None

    async def _create_policy(policy: Any, catalog_id: Any = None) -> Any:
        policy_ids.append(policy.id)
        return policy

    async def _delete_policy(policy_id: str, catalog_id: Any = None) -> bool:
        if policy_id in policy_ids:
            policy_ids.remove(policy_id)
            return True
        return False

    svc.update_policy = AsyncMock(side_effect=_update_policy)
    svc.create_policy = AsyncMock(side_effect=_create_policy)
    svc.delete_policy = AsyncMock(side_effect=_delete_policy)
    svc._policy_ids = policy_ids
    return svc


def _make_ctx(iam: Any, policy_svc: Any) -> PresetContext:
    return PresetContext(
        db=None,
        iam=iam,
        policy=policy_svc,
        config=None,
        tasks=None,
        cron=None,
        libs=None,
        principal=None,
        scope="platform",
    )


_PRESET = PublicAccessBaseline()


# ---------------------------------------------------------------------------
# test_apply_writes_policy_and_role_binding
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_writes_policy_and_role_binding() -> None:
    """apply() upserts public_access Policy and unions it into unauthenticated."""
    existing_role = Role(name=_ROLE_NAME, policies=["auth_extension_public"])
    iam = _make_iam(existing_roles={_ROLE_NAME: existing_role})
    policy_svc = _make_policy_svc()
    ctx = _make_ctx(iam, policy_svc)

    descriptor = await _PRESET.apply(NoParams(), "platform", ctx)

    assert isinstance(descriptor, AppliedDescriptor)
    assert descriptor.payload["policy_ids"] == [_POLICY_ID]
    assert descriptor.payload["role_names"] == [_ROLE_NAME]

    # Policy was created (update returned None — not found).
    policy_svc.create_policy.assert_awaited_once()

    # Role was updated with public_access merged in; original policy preserved.
    iam.update_role.assert_awaited_once()
    updated_role: Role = iam.update_role.await_args[0][0]
    assert _POLICY_ID in updated_role.policies
    assert "auth_extension_public" in updated_role.policies


# ---------------------------------------------------------------------------
# test_apply_is_idempotent_when_policy_exists
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_is_idempotent_when_policy_already_on_role() -> None:
    """apply() when public_access already in role policies → no update_role call."""
    existing_role = Role(name=_ROLE_NAME, policies=[_POLICY_ID, "auth_extension_public"])
    iam = _make_iam(existing_roles={_ROLE_NAME: existing_role})
    policy_svc = _make_policy_svc(existing_policy_ids=[_POLICY_ID])
    ctx = _make_ctx(iam, policy_svc)

    await _PRESET.apply(NoParams(), "platform", ctx)

    # Policy updated (found) — no create.
    policy_svc.update_policy.assert_awaited_once()
    policy_svc.create_policy.assert_not_called()

    # Role not modified — policy already present.
    iam.update_role.assert_not_called()


# ---------------------------------------------------------------------------
# test_revoke_removes_policy_and_strips_binding
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_revoke_removes_policy_and_strips_binding() -> None:
    """revoke() deletes Policy + prunes public_access from role; other policies preserved."""
    existing_role = Role(name=_ROLE_NAME, policies=[_POLICY_ID, "auth_extension_public"])
    iam = _make_iam(existing_roles={_ROLE_NAME: existing_role})
    policy_svc = _make_policy_svc(existing_policy_ids=[_POLICY_ID])
    ctx = _make_ctx(iam, policy_svc)

    descriptor = AppliedDescriptor(payload={
        "preset_name": "public_access_baseline",
        "policy_ids": [_POLICY_ID],
        "role_names": [_ROLE_NAME],
    })

    await _PRESET.revoke(descriptor, ctx)

    # Policy deleted.
    policy_svc.delete_policy.assert_awaited_once_with(_POLICY_ID)

    # Role updated with public_access stripped; other policies kept; role NOT deleted.
    iam.update_role.assert_awaited_once()
    updated_role: Role = iam.update_role.await_args[0][0]
    assert _POLICY_ID not in updated_role.policies
    assert "auth_extension_public" in updated_role.policies
    iam.delete_role.assert_not_called()


# ---------------------------------------------------------------------------
# test_dry_run_emits_two_entries
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dry_run_emits_two_entries() -> None:
    """dry_run() returns PresetPlan with upsert_policy + update_role_binding."""
    iam = _make_iam()
    policy_svc = _make_policy_svc()
    ctx = _make_ctx(iam, policy_svc)

    plan = await _PRESET.dry_run(NoParams(), "platform", ctx)

    assert plan.preset_name == "public_access_baseline"
    assert plan.scope_key == "platform"
    assert len(plan.entries) == 2
    kinds = {e.kind for e in plan.entries}
    assert "upsert_policy" in kinds
    assert "update_role_binding" in kinds

    # No DB writes during dry_run.
    iam.create_role.assert_not_called()
    iam.update_role.assert_not_called()
    policy_svc.create_policy.assert_not_called()
    policy_svc.update_policy.assert_not_called()


# ---------------------------------------------------------------------------
# test_bootstrap_runs_when_sentinel_absent
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bootstrap_runs_when_sentinel_absent() -> None:
    """bootstrap_preset_if_absent applies preset + writes sentinel when absent."""
    from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent

    existing_role = Role(name=_ROLE_NAME, policies=["auth_extension_public"])
    iam_svc = _make_iam(existing_roles={_ROLE_NAME: existing_role})
    policy_svc = _make_policy_svc()

    insert_calls: list = []

    class _FakeLock:
        def __init__(self, *_a: Any, **_kw: Any) -> None:
            pass

        async def __aenter__(self) -> MagicMock:
            return MagicMock()  # non-None conn

        async def __aexit__(self, *_: Any) -> bool:
            return False

    call_count = [0]

    async def _mock_dql_execute(conn: Any, **kw: Any) -> Any:
        call_count[0] += 1
        if call_count[0] == 1:
            return None  # SELECT sentinel → absent
        insert_calls.append(kw)
        return None  # INSERT sentinel

    class _MockDQL:
        def __init__(self, *_a: Any, **_kw: Any) -> None:
            pass

        execute = AsyncMock(side_effect=_mock_dql_execute)

    with patch(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock", _FakeLock
    ), patch(
        "dynastore.modules.storage.presets.lifecycle.find_preset",
        return_value=_PRESET,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle._build_context",
        return_value=_make_ctx(iam_svc, policy_svc),
    ), patch(
        "dynastore.modules.storage.presets.lifecycle.DQLQuery",
        _MockDQL,
    ):
        result = await bootstrap_preset_if_absent(
            MagicMock(), preset_name="public_access_baseline"
        )

    assert result is True
    # Sentinel was inserted.
    assert len(insert_calls) == 1
    # The preset ran — policy was created (update returned None → create).
    policy_svc.create_policy.assert_awaited_once()


# ---------------------------------------------------------------------------
# test_bootstrap_skips_when_sentinel_present
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bootstrap_skips_when_sentinel_present() -> None:
    """bootstrap_preset_if_absent no-ops when sentinel row already exists."""
    from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent

    policy_svc = _make_policy_svc()
    iam_svc = _make_iam()

    class _FakeLock:
        def __init__(self, *_a: Any, **_kw: Any) -> None:
            pass

        async def __aenter__(self) -> MagicMock:
            return MagicMock()

        async def __aexit__(self, *_: Any) -> bool:
            return False

    async def _select_present(conn: Any, **kw: Any) -> Any:
        return (1,)  # SELECT sentinel → present

    class _MockDQL:
        def __init__(self, *_a: Any, **_kw: Any) -> None:
            pass

        execute = AsyncMock(side_effect=_select_present)

    with patch(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock", _FakeLock
    ), patch(
        "dynastore.modules.storage.presets.lifecycle.find_preset",
        return_value=_PRESET,
    ), patch(
        "dynastore.modules.storage.presets.lifecycle.DQLQuery",
        _MockDQL,
    ):
        result = await bootstrap_preset_if_absent(
            MagicMock(), preset_name="public_access_baseline"
        )

    assert result is False
    # Sentinel present → no preset apply ran.
    policy_svc.create_policy.assert_not_called()
    policy_svc.update_policy.assert_not_called()
    iam_svc.update_role.assert_not_called()


# ---------------------------------------------------------------------------
# test_bootstrap_skips_on_lock_timeout
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bootstrap_skips_on_lock_timeout() -> None:
    """bootstrap_preset_if_absent returns False without writing when lock times out."""
    from dynastore.modules.storage.presets.lifecycle import bootstrap_preset_if_absent

    policy_svc = _make_policy_svc()
    iam_svc = _make_iam()

    class _TimedOutLock:
        def __init__(self, *_a: Any, **_kw: Any) -> None:
            pass

        async def __aenter__(self) -> None:
            return None  # lock timeout — another worker holds it

        async def __aexit__(self, *_: Any) -> bool:
            return False

    with patch(
        "dynastore.modules.db_config.locking_tools.acquire_startup_lock", _TimedOutLock
    ), patch(
        "dynastore.modules.storage.presets.lifecycle.find_preset",
        return_value=_PRESET,
    ):
        result = await bootstrap_preset_if_absent(
            MagicMock(), preset_name="public_access_baseline"
        )

    assert result is False
    # Nothing written — lock was not acquired.
    policy_svc.create_policy.assert_not_called()
    policy_svc.update_policy.assert_not_called()
    iam_svc.update_role.assert_not_called()


# ---------------------------------------------------------------------------
# Helper unit tests — _union_policy_into_role / _strip_policy_from_role
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_union_policy_into_role_adds_when_absent() -> None:
    """_union_policy_into_role adds policy_id when role exists without it."""
    role = Role(name=_ROLE_NAME, policies=["other"])
    iam = _make_iam(existing_roles={_ROLE_NAME: role})

    await _union_policy_into_role(iam, _ROLE_NAME, _POLICY_ID)

    iam.update_role.assert_awaited_once()
    updated: Role = iam.update_role.await_args[0][0]
    assert _POLICY_ID in updated.policies
    assert "other" in updated.policies


@pytest.mark.asyncio
async def test_union_policy_into_role_skips_when_already_present() -> None:
    """_union_policy_into_role is a no-op when policy_id already in role."""
    role = Role(name=_ROLE_NAME, policies=[_POLICY_ID, "other"])
    iam = _make_iam(existing_roles={_ROLE_NAME: role})

    await _union_policy_into_role(iam, _ROLE_NAME, _POLICY_ID)

    iam.update_role.assert_not_called()


@pytest.mark.asyncio
async def test_union_policy_into_role_creates_when_role_absent() -> None:
    """_union_policy_into_role creates the role when it does not exist."""
    iam = _make_iam()

    await _union_policy_into_role(iam, _ROLE_NAME, _POLICY_ID)

    iam.create_role.assert_awaited_once()
    created: Role = iam.create_role.await_args[0][0]
    assert created.name == _ROLE_NAME
    assert _POLICY_ID in created.policies


@pytest.mark.asyncio
async def test_strip_policy_from_role_removes_and_preserves_others() -> None:
    """_strip_policy_from_role removes only the target policy."""
    role = Role(name=_ROLE_NAME, policies=[_POLICY_ID, "auth_extension_public"])
    iam = _make_iam(existing_roles={_ROLE_NAME: role})

    await _strip_policy_from_role(iam, _ROLE_NAME, _POLICY_ID)

    iam.update_role.assert_awaited_once()
    updated: Role = iam.update_role.await_args[0][0]
    assert _POLICY_ID not in updated.policies
    assert "auth_extension_public" in updated.policies


@pytest.mark.asyncio
async def test_strip_policy_from_role_noop_when_role_absent() -> None:
    """_strip_policy_from_role is silent when role does not exist."""
    iam = _make_iam()

    await _strip_policy_from_role(iam, _ROLE_NAME, _POLICY_ID)

    iam.update_role.assert_not_called()
