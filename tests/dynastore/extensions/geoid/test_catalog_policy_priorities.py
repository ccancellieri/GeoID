"""Priority-band assertions for geoid anonymous-access policies.

Pins the explicit priority bands introduced on the four policies registered
by ``register_geoid_policies_for_catalog``:

  * geoid_anonymous_lookup          ALLOW  priority=200
  * geoid_anonymous_stac_deny_*     DENY   priority=100
  * geoid_anonymous_features_deny_* DENY   priority=100
  * geoid_anonymous_create_*        ALLOW  priority=500

Also includes a focused evaluate_access-level regression test that
demonstrates the operator-catch-all DENY tie bug: when a generic DENY at
priority 0 is on the same role as the lookup ALLOW, the lookup ALLOW must
still win because it now sits at priority 200.

The evaluate_access test stubs out storage and condition evaluation so no
Postgres DB is touched.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.extensions.geoid import catalog_policies as cp
from dynastore.extensions.geoid.catalog_policies import (
    _PRIORITY_ANON_CREATE,
    _PRIORITY_ENUM_DENY,
    _PRIORITY_LOOKUP_ALLOW,
)
from dynastore.models.auth import Policy
from dynastore.modules.iam.models import Role
from dynastore.modules.iam.policies import PolicyService


# ---------------------------------------------------------------------------
# Helpers shared by the partition-test helper
# ---------------------------------------------------------------------------

def _make_async_cm(return_value: object = None):
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _cm(*args: Any, **kwargs: Any):
        yield return_value

    return _cm


def _run(coro: Any) -> Any:
    return asyncio.get_event_loop().run_until_complete(coro)


def _make_env(catalog_id: str) -> tuple:
    """Build the standard mock environment without DB access."""
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


def _fake_get_protocol_factory(mock_pm: Any, mock_db: Any, mock_catalogs: Any):
    from dynastore.models.protocols.policies import PermissionProtocol
    from dynastore.models.protocols import DatabaseProtocol, CatalogsProtocol

    def _fake(proto: Any) -> Any:
        if proto is PermissionProtocol:
            return mock_pm
        if proto is DatabaseProtocol:
            return mock_db
        if proto is CatalogsProtocol:
            return mock_catalogs
        return None

    return _fake


def _collect_policies(mock_pm: Any) -> Dict[str, Policy]:
    """Return {policy_id: Policy} for every create_policy call on *mock_pm*."""
    return {call.args[0].id: call.args[0] for call in mock_pm.create_policy.call_args_list}


# ---------------------------------------------------------------------------
# Constant sanity
# ---------------------------------------------------------------------------


def test_priority_constant_ordering() -> None:
    """Band ordering is preserved: enum DENY < lookup ALLOW < create ALLOW."""
    assert _PRIORITY_ENUM_DENY < _PRIORITY_LOOKUP_ALLOW < _PRIORITY_ANON_CREATE


def test_priority_constant_values() -> None:
    """Exact values are pinned to catch accidental drift."""
    assert _PRIORITY_ENUM_DENY == 100
    assert _PRIORITY_LOOKUP_ALLOW == 200
    assert _PRIORITY_ANON_CREATE == 500


# ---------------------------------------------------------------------------
# Priority band assertions on registered policies
# ---------------------------------------------------------------------------


class TestRegisteredPolicyPriorities:
    """Verify that register_geoid_policies_for_catalog emits each policy
    with its declared priority band."""

    def _registered(self, monkeypatch: Any) -> Dict[str, Policy]:
        catalog_id = "cat-priority-test"
        mock_pm, mock_policy_storage, mock_db, mock_catalogs = _make_env(catalog_id)
        monkeypatch.setattr(
            cp, "get_protocol", _fake_get_protocol_factory(mock_pm, mock_db, mock_catalogs)
        )
        with patch(
            "dynastore.extensions.geoid.catalog_policies.PostgresPolicyStorage",
            return_value=mock_policy_storage,
        ), patch(
            "dynastore.extensions.geoid.catalog_policies.managed_transaction",
            side_effect=_make_async_cm(MagicMock()),
        ), patch(
            "dynastore.extensions.geoid.catalog_policies.DriverContext",
            return_value=MagicMock(),
        ):
            _run(cp.register_geoid_policies_for_catalog(catalog_id))
        return _collect_policies(mock_pm)

    def test_lookup_allow_priority(self, monkeypatch: Any) -> None:
        registered = self._registered(monkeypatch)
        assert "geoid_anonymous_lookup" in registered
        assert registered["geoid_anonymous_lookup"].priority == _PRIORITY_LOOKUP_ALLOW

    def test_stac_enum_deny_priority(self, monkeypatch: Any) -> None:
        registered = self._registered(monkeypatch)
        assert "geoid_anonymous_stac_deny_lookup_only" in registered
        assert registered["geoid_anonymous_stac_deny_lookup_only"].priority == _PRIORITY_ENUM_DENY

    def test_features_enum_deny_priority(self, monkeypatch: Any) -> None:
        registered = self._registered(monkeypatch)
        assert "geoid_anonymous_features_deny_lookup_only" in registered
        assert registered["geoid_anonymous_features_deny_lookup_only"].priority == _PRIORITY_ENUM_DENY

    def test_create_allow_priority(self, monkeypatch: Any) -> None:
        registered = self._registered(monkeypatch)
        assert "geoid_anonymous_create_per_collection" in registered
        assert registered["geoid_anonymous_create_per_collection"].priority == _PRIORITY_ANON_CREATE


# ---------------------------------------------------------------------------
# evaluate_access regression: catch-all DENY at priority 0 must not block
# the needle-lookup ALLOW at priority 200.
# ---------------------------------------------------------------------------

_SCHEMA = "tenant_cat_regression"
_CATALOG_ID = "cat-regression"
_ANON_ROLE = "ANONYMOUS"

# Path exercised by the needle lookup
_LOOKUP_PATH = "/search/catalogs/cat-regression/items-search"


class _FakeIamStorage:
    """Minimal stub: role lookup in global schema only."""

    def __init__(self, roles: Dict[str, Role]) -> None:
        self._roles = roles

    async def resolve_effective_grants(
        self, principal_id: Any, **_: Any
    ) -> List[Dict[str, Any]]:
        return []

    async def get_role(
        self, role_id: str, schema: str = "iam", **_: Any
    ) -> Optional[Role]:
        return self._roles.get(role_id)


def _role(name: str, policy_ids: List[str]) -> Role:
    return Role(id=name, name=name, policies=policy_ids)


def _make_policy_service(policies: Dict[str, Policy]) -> PolicyService:
    """Construct a PolicyService with all DB access stubbed out."""
    storage = _FakeIamStorage(
        roles={
            _ANON_ROLE: _role(
                _ANON_ROLE,
                list(policies.keys()),
            )
        }
    )
    svc = PolicyService.__new__(PolicyService)
    svc._state = None  # type: ignore[attr-defined]
    svc._engine = None  # type: ignore[attr-defined]
    svc.storage = None  # type: ignore[attr-defined]
    svc.iam_storage = storage  # type: ignore[attr-defined]
    svc._role_config = None  # type: ignore[attr-defined]

    async def _fixed_schema(catalog_id: Any, conn: Any = None) -> str:
        return _SCHEMA if catalog_id else "iam"

    async def _get_policy(pid: str, catalog_id: Any = None) -> Optional[Policy]:
        return policies.get(pid)

    svc._resolve_schema = _fixed_schema  # type: ignore[assignment,method-assign]
    svc.get_policy = _get_policy  # type: ignore[assignment,method-assign]
    return svc


@pytest.mark.asyncio
async def test_lookup_allow_wins_over_catchall_deny_at_priority_zero() -> None:
    """Regression: lookup ALLOW at priority 200 beats a catch-all DENY at
    priority 0 on the same unauthenticated role.

    Before the priority bands were made explicit both policies defaulted to
    priority 0, causing DENY-on-tie (#915) to silently block the lookup surface.
    """
    # The lookup ALLOW policy — resource, action and conditions match the
    # needle-lookup path.
    lookup_allow = Policy(
        id="geoid_anonymous_lookup",
        effect="ALLOW",
        actions=["POST"],
        resources=[r"/search/catalogs/[^/]+/items-search(/.*)?"],
        conditions=[
            {"type": "catalog_lookup_public_allowed"},
            {"type": "lookup_only_search"},
        ],
        priority=_PRIORITY_LOOKUP_ALLOW,
    )

    # Operator catch-all DENY that matches every path and method.
    catchall_deny = Policy(
        id="operator_catchall_deny",
        effect="DENY",
        actions=[".*"],
        resources=[".*"],
        priority=0,
    )

    policies = {
        lookup_allow.id: lookup_allow,
        catchall_deny.id: catchall_deny,
    }
    svc = _make_policy_service(policies)

    # Patch out condition evaluation: both geoid conditions are treated as
    # passed (the catalog has opted in and the request is a valid needle lookup).
    # This keeps the test free of the full audience-handler stack.
    async def _all_conditions_pass(condition: Any, context: Any) -> bool:
        return True

    with patch.object(svc, "_evaluate_condition", side_effect=_all_conditions_pass):
        allowed, reason = await svc.evaluate_access(
            principals=[_ANON_ROLE],
            path=_LOOKUP_PATH,
            method="POST",
            catalog_id=_CATALOG_ID,
        )

    assert allowed is True, (
        f"Needle lookup should be ALLOWED when lookup ALLOW is at priority "
        f"{_PRIORITY_LOOKUP_ALLOW} and catch-all DENY is at priority 0; "
        f"got DENIED. reason={reason!r}"
    )


@pytest.mark.asyncio
async def test_catchall_deny_blocks_lookup_when_at_same_priority() -> None:
    """Tie-breaking invariant: when the catch-all DENY and lookup ALLOW share
    the same priority, DENY wins (#915).

    This test documents the *prior broken state* so any future priority-reset
    is caught: if someone accidentally sets the lookup ALLOW back to 0, this
    test should FAIL (the lookup ALLOW would lose → the regression is back).

    Actually we invert: we set the ALLOW to priority 0 here intentionally and
    assert the result is DENIED — confirming the engine's tie-breaking rule.
    """
    lookup_allow_at_zero = Policy(
        id="geoid_anonymous_lookup",
        effect="ALLOW",
        actions=["POST"],
        resources=[r"/search/catalogs/[^/]+/items-search(/.*)?"],
        conditions=[
            {"type": "catalog_lookup_public_allowed"},
            {"type": "lookup_only_search"},
        ],
        priority=0,  # intentionally set to 0 to document the broken state
    )

    catchall_deny = Policy(
        id="operator_catchall_deny",
        effect="DENY",
        actions=[".*"],
        resources=[".*"],
        priority=0,
    )

    policies = {
        lookup_allow_at_zero.id: lookup_allow_at_zero,
        catchall_deny.id: catchall_deny,
    }
    svc = _make_policy_service(policies)

    async def _all_conditions_pass(condition: Any, context: Any) -> bool:
        return True

    with patch.object(svc, "_evaluate_condition", side_effect=_all_conditions_pass):
        allowed, reason = await svc.evaluate_access(
            principals=[_ANON_ROLE],
            path=_LOOKUP_PATH,
            method="POST",
            catalog_id=_CATALOG_ID,
        )

    assert allowed is False, (
        "When ALLOW and DENY tie at priority 0 the engine must choose DENY. "
        f"got allowed=True. reason={reason!r}"
    )
