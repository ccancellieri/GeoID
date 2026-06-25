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

"""Unit tests for atomic role-policy bind/unbind SQL primitives.

Covers geoid#1473 (PolicyContributorPreset clobbers Role.policies) and
geoid#1477 (all_users R-M-W race in _revoke_deny_policy / _apply_deny_policy).

All tests are SQL-string-level (no DB required) to keep them fast and
deterministic, following the pattern in test_insert_role_additive_on_conflict.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.models.auth import Policy
from dynastore.models.auth_models import Role
from dynastore.modules.storage.presets.policy_contributor_adapter import PolicyContributorPreset
from dynastore.modules.storage.presets.preset import NoParams, PresetContext


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


# ---------------------------------------------------------------------------
# BIND_POLICY_TO_ROLE — SQL shape
# ---------------------------------------------------------------------------

class TestBindPolicyToRoleSql:
    def test_bind_uses_jsonb_array_elements_text(self) -> None:
        """Bind must use jsonb_array_elements_text to iterate plain string IDs."""
        from dynastore.modules.iam.iam_queries import BIND_POLICY_TO_ROLE
        sql = _norm(BIND_POLICY_TO_ROLE.template)
        assert "jsonb_array_elements_text" in sql, (
            "BIND_POLICY_TO_ROLE must iterate the string-valued policies array "
            "via jsonb_array_elements_text."
        )

    def test_bind_filters_existing_id(self) -> None:
        """Bind must strip the existing entry before appending — dedup contract."""
        from dynastore.modules.iam.iam_queries import BIND_POLICY_TO_ROLE
        sql = _norm(BIND_POLICY_TO_ROLE.template)
        assert "!= :policy_id" in sql, (
            "BIND_POLICY_TO_ROLE must filter out the existing entry with the "
            "same policy_id to ensure idempotent append."
        )

    def test_bind_appends_new_entry(self) -> None:
        """Bind must append the new policy_id string via jsonb_build_array."""
        from dynastore.modules.iam.iam_queries import BIND_POLICY_TO_ROLE
        sql = _norm(BIND_POLICY_TO_ROLE.template)
        assert "jsonb_build_array" in sql, (
            "BIND_POLICY_TO_ROLE must append the new string via jsonb_build_array."
        )

    def test_bind_coalesces_null_to_empty_array(self) -> None:
        """NULL jsonb_agg (zero rows after filter) must coalesce to []."""
        from dynastore.modules.iam.iam_queries import BIND_POLICY_TO_ROLE
        sql = _norm(BIND_POLICY_TO_ROLE.template)
        assert "COALESCE" in sql, (
            "BIND_POLICY_TO_ROLE must COALESCE the jsonb_agg result to '[]'::jsonb "
            "when the filtered set is empty."
        )

    def test_bind_is_update_not_select(self) -> None:
        from dynastore.modules.iam.iam_queries import BIND_POLICY_TO_ROLE
        sql = _norm(BIND_POLICY_TO_ROLE.template).upper()
        assert sql.startswith("UPDATE"), (
            "BIND_POLICY_TO_ROLE must be an UPDATE statement."
        )


# ---------------------------------------------------------------------------
# UNBIND_POLICY_FROM_ROLE — SQL shape
# ---------------------------------------------------------------------------

class TestUnbindPolicyFromRoleSql:
    def test_unbind_uses_jsonb_array_elements_text(self) -> None:
        from dynastore.modules.iam.iam_queries import UNBIND_POLICY_FROM_ROLE
        sql = _norm(UNBIND_POLICY_FROM_ROLE.template)
        assert "jsonb_array_elements_text" in sql

    def test_unbind_filters_matching_id(self) -> None:
        """Unbind must remove entries equal to policy_id."""
        from dynastore.modules.iam.iam_queries import UNBIND_POLICY_FROM_ROLE
        sql = _norm(UNBIND_POLICY_FROM_ROLE.template)
        assert "!= :policy_id" in sql

    def test_unbind_coalesces_null_to_empty_array(self) -> None:
        from dynastore.modules.iam.iam_queries import UNBIND_POLICY_FROM_ROLE
        sql = _norm(UNBIND_POLICY_FROM_ROLE.template)
        assert "COALESCE" in sql

    def test_unbind_does_not_append(self) -> None:
        """Unbind must not append anything — no concat operator."""
        from dynastore.modules.iam.iam_queries import UNBIND_POLICY_FROM_ROLE
        sql = _norm(UNBIND_POLICY_FROM_ROLE.template)
        assert "jsonb_build_array" not in sql, (
            "UNBIND_POLICY_FROM_ROLE must not append a new entry."
        )

    def test_unbind_is_update_not_select(self) -> None:
        from dynastore.modules.iam.iam_queries import UNBIND_POLICY_FROM_ROLE
        sql = _norm(UNBIND_POLICY_FROM_ROLE.template).upper()
        assert sql.startswith("UPDATE")


# ---------------------------------------------------------------------------
# Contributor fixtures
# ---------------------------------------------------------------------------

class _PresetA:
    def get_policies(self):
        return [Policy(id="pol_a", description="A", actions=["GET"], resources=["/a"], effect="ALLOW")]

    def get_role_bindings(self):
        return [Role(name="anonymous", policies=["pol_a"])]


class _PresetB:
    def get_policies(self):
        return [Policy(id="pol_b", description="B", actions=["GET"], resources=["/b"], effect="ALLOW")]

    def get_role_bindings(self):
        return [Role(name="anonymous", policies=["pol_b"])]


def _make_ctx() -> tuple["PresetContext", "List[tuple]"]:
    """Return (ctx, bind_calls) where bind_calls records (role_name, policy_id) pairs."""
    bind_calls: List[tuple] = []

    async def _update_policy(pol: Any) -> Any:
        return pol

    async def _bind(role_name: str, policy_entry: Dict[str, Any], **_: Any) -> None:
        bind_calls.append((role_name, policy_entry))

    policy_svc = MagicMock()
    policy_svc.update_policy = _update_policy

    iam_svc = MagicMock()
    iam_svc.bind_policy_to_role = _bind
    iam_svc.update_role = AsyncMock()
    iam_svc.create_role = AsyncMock()

    ctx = PresetContext(
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
    return ctx, bind_calls


# ---------------------------------------------------------------------------
# PolicyContributorPreset.apply() — additive, no clobber (#1473)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_calls_bind_not_update_role() -> None:
    """apply() must call bind_policy_to_role, not update_role (no clobber)."""
    ctx, bind_calls = _make_ctx()

    preset = PolicyContributorPreset(
        name="preset_a", description="A", keywords=("iam",), contributor_factory=_PresetA,
    )
    await preset.apply(NoParams(), "platform", ctx)

    ctx.iam.update_role.assert_not_awaited()
    assert ("anonymous", {"id": "pol_a"}) in bind_calls


@pytest.mark.asyncio
async def test_apply_two_presets_additive() -> None:
    """Applying preset A then preset B must produce binds for both without clobbering."""
    ctx_a, calls_a = _make_ctx()
    ctx_b, calls_b = _make_ctx()

    preset_a = PolicyContributorPreset(
        name="pa", description="A", keywords=("iam",), contributor_factory=_PresetA,
    )
    preset_b = PolicyContributorPreset(
        name="pb", description="B", keywords=("iam",), contributor_factory=_PresetB,
    )

    await preset_a.apply(NoParams(), "platform", ctx_a)
    await preset_b.apply(NoParams(), "platform", ctx_b)

    assert ("anonymous", {"id": "pol_a"}) in calls_a
    assert ("anonymous", {"id": "pol_b"}) in calls_b

    # Neither preset called update_role — that would clobber prior entries.
    ctx_a.iam.update_role.assert_not_awaited()
    ctx_b.iam.update_role.assert_not_awaited()


@pytest.mark.asyncio
async def test_apply_idempotent_rebind() -> None:
    """Re-applying the same preset issues bind again; SQL dedup is server-side."""
    ctx, calls = _make_ctx()
    preset = PolicyContributorPreset(
        name="idem", description="Idem", keywords=("iam",), contributor_factory=_PresetA,
    )
    await preset.apply(NoParams(), "platform", ctx)
    await preset.apply(NoParams(), "platform", ctx)

    pol_a_binds = [c for c in calls if c == ("anonymous", {"id": "pol_a"})]
    assert len(pol_a_binds) == 2, (
        "Each apply call must issue a bind (SQL dedup is server-side). "
        "The Python layer must not skip the call."
    )


# ---------------------------------------------------------------------------
# _apply_deny_policy / _revoke_deny_policy — atomic via primitives (#1477)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_deny_policy_uses_bind_not_list_roles() -> None:
    """_apply_deny_policy must call bind_policy_to_role, not list_roles + update_role."""
    bind_called: List[tuple] = []

    async def _bind(role_name: str, policy_entry: Dict[str, Any], **_: Any) -> None:
        bind_called.append((role_name, policy_entry))

    fake_iam = MagicMock()
    fake_iam.list_roles = AsyncMock(return_value=[])
    fake_iam.update_role = AsyncMock()
    fake_iam.bind_policy_to_role = _bind

    fake_perm = MagicMock()
    fake_perm.create_policy = AsyncMock()
    fake_perm.update_policy = AsyncMock()

    from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
        ItemsElasticsearchPrivateDriver,
    )
    from dynastore.modules.iam.iam_service import IamService

    with patch("dynastore.tools.discovery.get_protocol") as mock_gp:
        mock_gp.side_effect = lambda p: fake_iam if p is IamService else fake_perm
        with patch(
            "dynastore.extensions.tools.conformance.get_ogc_service_prefixes",
            return_value=["stac"],
        ):
            await ItemsElasticsearchPrivateDriver._apply_deny_policy("cat-test")

    fake_iam.list_roles.assert_not_awaited()
    fake_iam.update_role.assert_not_awaited()
    assert any(r == "all_users" for r, _ in bind_called), (
        "bind_policy_to_role must be called for all_users."
    )


@pytest.mark.asyncio
async def test_revoke_deny_policy_uses_unbind_not_list_roles() -> None:
    """_revoke_deny_policy must call unbind_policy_from_role, not list_roles + update_role."""
    unbind_called: List[tuple] = []

    async def _unbind(role_name: str, policy_id: str, **_: Any) -> None:
        unbind_called.append((role_name, policy_id))

    fake_iam = MagicMock()
    fake_iam.list_roles = AsyncMock(return_value=[])
    fake_iam.update_role = AsyncMock()
    fake_iam.unbind_policy_from_role = _unbind

    fake_perm = MagicMock()
    fake_perm.delete_policy = AsyncMock()

    from dynastore.modules.storage.drivers.elasticsearch_private.driver import (
        ItemsElasticsearchPrivateDriver,
    )
    from dynastore.modules.iam.iam_service import IamService

    with patch("dynastore.tools.discovery.get_protocol") as mock_gp:
        mock_gp.side_effect = lambda p: fake_iam if p is IamService else fake_perm
        await ItemsElasticsearchPrivateDriver._revoke_deny_policy("cat-test")

    fake_iam.list_roles.assert_not_awaited()
    fake_iam.update_role.assert_not_awaited()
    assert ("all_users", "private_deny_cat-test") in unbind_called
