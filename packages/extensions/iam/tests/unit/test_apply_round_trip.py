"""Round-trip test: apply → revoke → apply leaves DB in same state as single apply.

Uses an in-memory state dict to simulate a minimal policy + role store.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock
import pytest

from dynastore.extensions.iam.presets.iam_baseline import (
    IamBaseline,
    IamBaselineParams,
    _iam_service_policies,
    _iam_service_role_bindings,
)
from dynastore.modules.storage.presets.preset import AppliedDescriptor, PresetContext


# ---------------------------------------------------------------------------
# In-memory store
# ---------------------------------------------------------------------------

class _InMemoryStore:
    """Minimal in-memory policy + role store for testing."""

    def __init__(self) -> None:
        self.policies: Dict[str, Any] = {}
        self.roles: Dict[str, Any] = {}

    def make_context(self) -> PresetContext:
        store = self

        policy_svc = MagicMock()
        iam_svc = MagicMock()

        async def _update_policy(policy: Any) -> Any:
            mock_pol = MagicMock()
            mock_pol.id = policy.id
            mock_pol.conditions = getattr(policy, "conditions", [])
            store.policies[policy.id] = mock_pol
            return mock_pol

        async def _delete_policy(pid: str, catalog_id: Any = None) -> bool:
            return bool(store.policies.pop(pid, None))

        async def _update_role(role: Any) -> Any:
            existing = store.roles.get(role.name)
            if existing is not None:
                # Merge policies (strip only what we removed).
                existing.policies = list(role.policies)
            else:
                mock_role = MagicMock()
                mock_role.name = role.name
                mock_role.policies = list(role.policies)
                store.roles[role.name] = mock_role
            return store.roles[role.name]

        async def _list_roles() -> List[Any]:
            roles = []
            for name, r in store.roles.items():
                obj = MagicMock()
                obj.name = name
                obj.policies = list(r.policies)

                def _copy(update: Dict, _name=name) -> Any:
                    new_obj = MagicMock()
                    new_obj.name = _name
                    new_obj.policies = update.get("policies", store.roles[_name].policies)
                    return new_obj

                obj.model_copy = _copy
                roles.append(obj)
            return roles

        async def _delete_role(name: str, cascade: bool = False) -> bool:
            return bool(store.roles.pop(name, None))

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
            tasks=None, cron=None, libs=None, principal=None,
            scope="platform",
        )


# ---------------------------------------------------------------------------
# Round-trip tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_revoke_apply_same_policy_ids():
    """apply → revoke → apply produces same policy id set as first apply."""
    preset = IamBaseline()
    store = _InMemoryStore()
    params = IamBaselineParams()

    # First apply.
    ctx1 = store.make_context()
    descriptor1 = await preset.apply(params, "platform", ctx1)
    policy_ids_after_first = set(store.policies.keys())

    # Revoke.
    ctx2 = store.make_context()
    await preset.revoke(descriptor1, ctx2)

    # Second apply.
    ctx3 = store.make_context()
    descriptor2 = await preset.apply(params, "platform", ctx3)
    policy_ids_after_second = set(store.policies.keys())

    assert policy_ids_after_first == policy_ids_after_second, (
        f"Policy ids differ after round-trip.\n"
        f"First: {sorted(policy_ids_after_first)}\n"
        f"Second: {sorted(policy_ids_after_second)}"
    )


@pytest.mark.asyncio
async def test_apply_revoke_apply_descriptor_shape_stable():
    """apply → revoke → apply produces descriptors with identical payload keys."""
    preset = IamBaseline()
    store = _InMemoryStore()
    params = IamBaselineParams()

    ctx1 = store.make_context()
    descriptor1: AppliedDescriptor = await preset.apply(params, "platform", ctx1)

    ctx2 = store.make_context()
    await preset.revoke(descriptor1, ctx2)

    ctx3 = store.make_context()
    descriptor2: AppliedDescriptor = await preset.apply(params, "platform", ctx3)

    assert descriptor1.payload.keys() == descriptor2.payload.keys()
    assert sorted(descriptor1.payload["policy_ids"]) == sorted(descriptor2.payload["policy_ids"])


@pytest.mark.asyncio
async def test_revoke_clears_iam_service_policies():
    """After revoke the IAM service policies are absent from the store."""
    preset = IamBaseline()
    store = _InMemoryStore()
    params = IamBaselineParams()

    ctx = store.make_context()
    descriptor = await preset.apply(params, "platform", ctx)

    ctx2 = store.make_context()
    await preset.revoke(descriptor, ctx2)

    iam_ids = {p.id for p in _iam_service_policies()}
    for pid in iam_ids:
        assert pid not in store.policies, (
            f"Policy {pid!r} still present after revoke"
        )
