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

"""Pure-unit assertions for admin-role policies and anonymous write rate-limit.

Verifies that ``register_geoid_policies_for_catalog`` emits:

  * geoid_admin_features  — ALLOW, priority=400, expected resources
  * geoid_admin_data      — ALLOW, priority=400, expected resources
  * admin role enriched with both admin policy ids
  * geoid_anonymous_create_per_collection has a rate_limit condition
    (limit=_ANON_WRITE_LIMIT, window_seconds=_ANON_WRITE_WINDOW_SECONDS,
    scope=catalog) ordered AFTER collection_write_anonymous_allowed.

No Postgres DB is touched. Mocking pattern mirrors
test_catalog_policy_priorities.py and test_catalog_policies_partition.py.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.geoid import catalog_policies as cp
from dynastore.extensions.geoid.catalog_policies import (
    _ANON_WRITE_LIMIT,
    _ANON_WRITE_WINDOW_SECONDS,
    _PRIORITY_ADMIN,
)
from dynastore.models.auth import Policy
from dynastore.modules.iam.models import Role


# ---------------------------------------------------------------------------
# Helpers (mirrors test_catalog_policy_priorities.py)
# ---------------------------------------------------------------------------


def _make_async_cm(return_value: object = None):
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _cm(*args: Any, **kwargs: Any):
        yield return_value

    return _cm


def _run(coro: Any) -> Any:
    return asyncio.get_event_loop().run_until_complete(coro)


class _FakeRoleStorage:
    """Records get_role and create_role / update_role calls, keyed by role name."""

    def __init__(self) -> None:
        self._roles: Dict[str, Role] = {}
        self.update_calls: List[Role] = []
        self.create_calls: List[Role] = []

    async def get_role(self, name: str, schema: str = "iam", conn: Any = None) -> Optional[Role]:
        return self._roles.get(name)

    async def create_role(self, role: Role, schema: str = "iam", conn: Any = None) -> None:
        self._roles[role.name] = role
        self.create_calls.append(role)

    async def update_role(self, role: Role, schema: str = "iam", conn: Any = None) -> None:
        self._roles[role.name] = role
        self.update_calls.append(role)


def _make_env(catalog_id: str) -> tuple:
    """Build a mock environment and a fake IamModule-like PermissionProtocol."""
    mock_pm = MagicMock()
    mock_pm.get_policy = AsyncMock(return_value=None)
    mock_pm.create_policy = AsyncMock(return_value=MagicMock())

    mock_policy_storage = MagicMock()
    mock_policy_storage.ensure_policy_partition = AsyncMock()

    mock_engine = MagicMock()
    mock_db = MagicMock()
    mock_db.engine = mock_engine

    mock_catalogs = MagicMock()
    mock_catalogs.resolve_physical_schema = AsyncMock(return_value=f"tenant_{catalog_id}")

    return mock_pm, mock_policy_storage, mock_db, mock_catalogs


def _fake_get_protocol_factory(
    mock_db: Any,
    mock_catalogs: Any,
    iam_instance: Any,
) -> Any:
    from dynastore.models.protocols.policies import PermissionProtocol
    from dynastore.models.protocols import DatabaseProtocol, CatalogsProtocol

    def _fake(proto: Any) -> Any:
        if proto is PermissionProtocol:
            return iam_instance
        if proto is DatabaseProtocol:
            return mock_db
        if proto is CatalogsProtocol:
            return mock_catalogs
        return None

    return _fake


def _register(catalog_id: str) -> tuple:
    """Run register_geoid_policies_for_catalog and return (created_policies, role_storage).

    The isinstance(iam_module, IamModule) guard inside catalog_policies is
    bypassed by injecting a placeholder class into the lazily-imported
    ``dynastore.modules.iam.module`` namespace and making our fake instance
    a subclass of it.  This avoids importing the real IamModule (which raises
    ImportError in test environments without the dynastore-ext-iam distribution).
    """
    import sys

    mock_pm, mock_policy_storage, mock_db, mock_catalogs = _make_env(catalog_id)
    role_storage = _FakeRoleStorage()

    # Build a throwaway base class and a fake IAM-module instance that IS-A
    # of that base, then inject it into sys.modules under the real module path
    # so the local ``from dynastore.modules.iam.module import IamModule``
    # inside register_geoid_policies_for_catalog resolves to our base class.
    class _IamModuleBase:
        pass

    class _FakeIamModule(_IamModuleBase):
        def __init__(self) -> None:
            self.storage = role_storage
            self.get_policy = AsyncMock(return_value=None)
            self.create_policy = AsyncMock(return_value=MagicMock())

    iam_instance = _FakeIamModule()
    fake_get_protocol = _fake_get_protocol_factory(mock_db, mock_catalogs, iam_instance)

    # Create a stub module object that exposes IamModule = _IamModuleBase.
    import types
    stub_module = types.ModuleType("dynastore.modules.iam.module")
    stub_module.IamModule = _IamModuleBase  # type: ignore[attr-defined]

    original_module = sys.modules.get("dynastore.modules.iam.module")
    sys.modules["dynastore.modules.iam.module"] = stub_module
    try:
        with patch.object(cp, "get_protocol", fake_get_protocol), \
             patch(
                 "dynastore.extensions.geoid.catalog_policies.PostgresPolicyStorage",
                 return_value=mock_policy_storage,
             ), \
             patch(
                 "dynastore.extensions.geoid.catalog_policies.managed_transaction",
                 side_effect=_make_async_cm(MagicMock()),
             ), \
             patch(
                 "dynastore.extensions.geoid.catalog_policies.DriverContext",
                 return_value=MagicMock(),
             ):
            _run(cp.register_geoid_policies_for_catalog(catalog_id))
    finally:
        if original_module is None:
            sys.modules.pop("dynastore.modules.iam.module", None)
        else:
            sys.modules["dynastore.modules.iam.module"] = original_module

    created = {
        call.args[0].id: call.args[0]
        for call in iam_instance.create_policy.call_args_list
    }
    return created, role_storage


# ---------------------------------------------------------------------------
# Priority constant
# ---------------------------------------------------------------------------


def test_priority_admin_constant_value() -> None:
    """_PRIORITY_ADMIN is 400 (between enum DENYs at 100 and anon_create at 500)."""
    assert _PRIORITY_ADMIN == 400


def test_priority_admin_outranks_enum_deny() -> None:
    from dynastore.extensions.geoid.catalog_policies import _PRIORITY_ENUM_DENY
    assert _PRIORITY_ADMIN > _PRIORITY_ENUM_DENY


def test_priority_admin_below_anon_create() -> None:
    from dynastore.extensions.geoid.catalog_policies import _PRIORITY_ANON_CREATE
    assert _PRIORITY_ADMIN < _PRIORITY_ANON_CREATE


# ---------------------------------------------------------------------------
# Admin policy assertions
# ---------------------------------------------------------------------------


class TestAdminPoliciesRegistered:
    """geoid_admin_features and geoid_admin_data are emitted with correct attributes."""

    def _get(self) -> Dict[str, Policy]:
        created, _ = _register("cat-admin-test")
        return created

    def test_admin_features_emitted(self) -> None:
        assert "geoid_admin_features" in self._get(), (
            "register_geoid_policies_for_catalog must create geoid_admin_features"
        )

    def test_admin_data_emitted(self) -> None:
        assert "geoid_admin_data" in self._get(), (
            "register_geoid_policies_for_catalog must create geoid_admin_data"
        )

    def test_admin_features_effect_allow(self) -> None:
        assert self._get()["geoid_admin_features"].effect == "ALLOW"

    def test_admin_data_effect_allow(self) -> None:
        assert self._get()["geoid_admin_data"].effect == "ALLOW"

    def test_admin_features_priority(self) -> None:
        assert self._get()["geoid_admin_features"].priority == _PRIORITY_ADMIN

    def test_admin_data_priority(self) -> None:
        assert self._get()["geoid_admin_data"].priority == _PRIORITY_ADMIN

    def test_admin_features_resources_contain_features_path(self) -> None:
        resources = self._get()["geoid_admin_features"].resources or []
        assert any("features" in r for r in resources), (
            "geoid_admin_features resources must include a /features/catalogs/... pattern"
        )

    def test_admin_data_resources_contain_stac_and_search(self) -> None:
        resources = self._get()["geoid_admin_data"].resources or []
        has_stac = any("stac" in r for r in resources)
        has_search = any("search" in r for r in resources)
        assert has_stac and has_search, (
            "geoid_admin_data resources must include both /stac/... and /search/... patterns"
        )

    def test_admin_features_actions(self) -> None:
        actions = set(self._get()["geoid_admin_features"].actions or [])
        assert {"GET", "POST", "PUT", "PATCH", "DELETE"} == actions

    def test_admin_data_actions(self) -> None:
        actions = set(self._get()["geoid_admin_data"].actions or [])
        assert {"GET", "POST", "PUT", "PATCH", "DELETE"} == actions

    def test_admin_features_no_conditions(self) -> None:
        conds = self._get()["geoid_admin_features"].conditions or []
        assert conds == [], "geoid_admin_features must have no conditions (unconditional ALLOW)"

    def test_admin_data_no_conditions(self) -> None:
        conds = self._get()["geoid_admin_data"].conditions or []
        assert conds == [], "geoid_admin_data must have no conditions (unconditional ALLOW)"


# ---------------------------------------------------------------------------
# Admin role enrichment
# ---------------------------------------------------------------------------


class TestAdminRoleEnrichment:
    """The 'admin' role gets geoid_admin_features + geoid_admin_data."""

    def _role_storage(self) -> _FakeRoleStorage:
        _, rs = _register("cat-admin-role-test")
        return rs

    def test_admin_role_created_or_updated(self) -> None:
        rs = self._role_storage()
        touched = rs.create_calls + rs.update_calls
        names = [r.name for r in touched]
        assert "admin" in names, (
            "register_geoid_policies_for_catalog must create or update the 'admin' role"
        )

    def _admin_role(self) -> Role:
        rs = self._role_storage()
        touched = rs.create_calls + rs.update_calls
        for r in touched:
            if r.name == "admin":
                return r
        raise AssertionError("admin role not found")

    def test_admin_role_has_admin_features_policy(self) -> None:
        assert "geoid_admin_features" in self._admin_role().policies

    def test_admin_role_has_admin_data_policy(self) -> None:
        assert "geoid_admin_data" in self._admin_role().policies


# ---------------------------------------------------------------------------
# Anonymous write rate-limit on geoid_anonymous_create_per_collection
# ---------------------------------------------------------------------------


class TestAnonCreateRateLimit:
    """geoid_anonymous_create_per_collection must carry a rate_limit condition
    ordered AFTER collection_write_anonymous_allowed."""

    def _policy(self) -> Policy:
        created, _ = _register("cat-anon-rl-test")
        pol = created.get("geoid_anonymous_create_per_collection")
        assert pol is not None, "geoid_anonymous_create_per_collection must be registered"
        return pol

    def _conditions(self) -> list:
        return list(self._policy().conditions or [])

    def test_has_collection_write_condition(self) -> None:
        types = [c.get("type") if isinstance(c, dict) else getattr(c, "type", None)
                 for c in self._conditions()]
        assert "collection_write_anonymous_allowed" in types

    def test_has_rate_limit_condition(self) -> None:
        types = [c.get("type") if isinstance(c, dict) else getattr(c, "type", None)
                 for c in self._conditions()]
        assert "rate_limit" in types, (
            "geoid_anonymous_create_per_collection must carry a rate_limit condition"
        )

    def test_rate_limit_ordered_after_collection_write_gate(self) -> None:
        conds = self._conditions()
        types = [c.get("type") if isinstance(c, dict) else getattr(c, "type", None)
                 for c in conds]
        cw_idx = types.index("collection_write_anonymous_allowed")
        rl_idx = types.index("rate_limit")
        assert cw_idx < rl_idx, (
            "collection_write_anonymous_allowed must come before rate_limit "
            "(short-circuit: cheap opt-in gate first, then counter)"
        )

    def _rate_limit_config(self) -> dict:
        for c in self._conditions():
            ctype = c.get("type") if isinstance(c, dict) else getattr(c, "type", None)
            if ctype == "rate_limit":
                cfg = c.get("config") if isinstance(c, dict) else getattr(c, "config", {})
                return cfg or {}
        raise AssertionError("rate_limit condition not found")

    def test_rate_limit_limit_value(self) -> None:
        assert self._rate_limit_config()["limit"] == _ANON_WRITE_LIMIT

    def test_rate_limit_window_seconds(self) -> None:
        assert self._rate_limit_config()["window_seconds"] == _ANON_WRITE_WINDOW_SECONDS

    def test_rate_limit_scope_is_catalog(self) -> None:
        assert self._rate_limit_config()["scope"] == "catalog", (
            "scope must be 'catalog' so one counter covers all anonymous writes to the catalog"
        )

    def test_anon_write_limit_constant(self) -> None:
        assert _ANON_WRITE_LIMIT == 100_000

    def test_anon_write_window_constant(self) -> None:
        assert _ANON_WRITE_WINDOW_SECONDS == 86_400
