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

"""Pure-unit pins for policy-id injection into role-policy condition evaluation.

Root cause: rate_limit and max_count condition handlers namespace their
usage-counter rows by the owning policy's id, resolved via
_policy_id_for(config, ctx) -> ctx.extras["_policy_id_by_config_id"][id(config)].

The IamMiddleware populates that mapping only for inline principal
``custom_policies``.  Role-based policies reach ``evaluate_access`` without
that pre-population, so _policy_id_for() returns None and the handlers skip
enforcement (silent no-op).

The fix: ``evaluate_access`` now populates ``_policy_id_by_config_id`` for
each matching policy's condition configs just before evaluating them.

Covered here without a DB or Valkey:
  * evaluate_access injects the policy id into ctx.extras for a role policy
    carrying a rate_limit condition.
  * The fake UsageCounterProtocol's incr_if_below is called with the correct
    policy_id argument.
  * No injection happens when request_context is None (compile_read_filter path).
  * Pre-existing mapping entries (from middleware / grant quota step) are
    preserved (not overwritten).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.models.auth import Condition, Policy
from dynastore.modules.iam.models import Role
from dynastore.modules.iam.policies import PolicyService


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

_SCHEMA = "s_test_role_cond"
_CATALOG_ID = "catalog_role_cond_test"
_ROLE_NAME = "writer"
_POLICY_ID = "write_rate_limited"


class _FakeIamStorage:
    """Minimal stub: resolves a single role with one policy, no grants."""

    def __init__(self, roles: Dict[str, Role]) -> None:
        self._roles = roles

    async def resolve_effective_grants(
        self, principal_id: Any, **_: Any
    ) -> List[Dict[str, Any]]:
        return []

    async def get_role(self, role_id: str, schema: str = "iam", **_: Any) -> Optional[Role]:
        return self._roles.get(role_id)


class _FakeCounter:
    """Fake UsageCounterProtocol that records incr_if_below calls.

    Returns (1, True) by default — counter incremented, request allowed.
    """

    name = "fake_counter"
    priority = 0

    def __init__(self) -> None:
        self.calls: List[Tuple[str, str, int]] = []

    async def incr_if_below(
        self,
        policy_id: str,
        principal_key: str,
        limit: int,
        *,
        window_seconds: int = 60,
        amount: int = 1,
    ) -> Tuple[int, bool]:
        self.calls.append((policy_id, principal_key, limit))
        return (1, True)

    async def get(self, policy_id: str, principal_key: str, *, window_seconds: int = 60) -> int:
        return 0

    async def incr(
        self, policy_id: str, principal_key: str, *, window_seconds: int = 60, amount: int = 1
    ) -> int:
        return 1

    async def reset(self, policy_id: str, principal_key: str, *, window_seconds: int = 60) -> None:
        pass

    async def reap_expired(self) -> int:
        return 0


class _Ctx:
    """Minimal request-context stand-in.

    Carries ``extras`` (mirrors what IamMiddleware populates),
    ``principal_id`` (for scope=principal), ``catalog_id``, and a
    minimal ``request`` stub for _client_ip_from_request.
    """

    def __init__(self, catalog_id: str = _CATALOG_ID) -> None:
        self.extras: Dict[str, Any] = {}
        self.principal_id: Optional[str] = None
        self.catalog_id = catalog_id
        self.request: Any = None
        self.method = "POST"
        self.path = f"/stac/catalogs/{catalog_id}/collections/col1/items"


def _role(name: str, policy_ids: List[str]) -> Role:
    return Role(id=name, name=name, policies=policy_ids)


def _rate_limit_policy(policy_id: str, catalog_scope: bool = False) -> Policy:
    """Return a write-only ALLOW policy with a rate_limit condition."""
    scope = "catalog" if catalog_scope else "principal"
    return Policy(
        id=policy_id,
        effect="ALLOW",
        actions=["POST", "PUT", "PATCH", "DELETE"],
        resources=[".*"],
        conditions=[
            {
                "type": "rate_limit",
                "config": {
                    "limit": 100,
                    "window_seconds": 60,
                    "scope": scope,
                },
            }
        ],
    )


def _make_service(policy: Policy) -> PolicyService:
    """Construct a PolicyService with storage/engine stubbed out."""
    storage = _FakeIamStorage(
        roles={_ROLE_NAME: _role(_ROLE_NAME, [policy.id])}
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
        return policy if pid == policy.id else None

    svc._resolve_schema = _fixed_schema  # type: ignore[assignment,method-assign]
    svc.get_policy = _get_policy  # type: ignore[assignment,method-assign]
    return svc


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_role_policy_rate_limit_injects_policy_id_into_context() -> None:
    """evaluate_access must populate _policy_id_by_config_id for a role policy's
    condition configs before evaluating them, so rate_limit can namespace its
    counter row by policy id (not just by principal key).

    The fake counter's incr_if_below is called with the correct policy_id.
    """
    policy = _rate_limit_policy(_POLICY_ID)
    svc = _make_service(policy)
    ctx = _Ctx()
    # scope=principal requires a principal_id
    ctx.principal_id = "user-abc"

    fake_counter = _FakeCounter()
    with patch(
        "dynastore.modules.iam.conditions.get_protocol",
        return_value=fake_counter,
    ):
        allowed, reason = await svc.evaluate_access(
            principals=[_ROLE_NAME],
            path=f"/stac/catalogs/{_CATALOG_ID}/collections/col1/items",
            method="POST",
            request_context=ctx,
            catalog_id=_CATALOG_ID,
        )

    assert allowed is True, f"expected ALLOW, got DENIED: {reason}"
    assert len(fake_counter.calls) == 1, (
        "incr_if_below should be called exactly once for the one rate_limit condition"
    )
    used_policy_id, principal_key, limit = fake_counter.calls[0]
    assert used_policy_id == _POLICY_ID, (
        f"rate_limit handler must be called with policy_id={_POLICY_ID!r}, "
        f"got {used_policy_id!r}. "
        "This confirms that evaluate_access injects the policy id into the context."
    )
    assert limit == 100


@pytest.mark.asyncio
async def test_role_policy_rate_limit_catalog_scope_uses_catalog_id() -> None:
    """When scope=catalog the principal_key passed to incr_if_below is catalog_id."""
    policy = _rate_limit_policy(_POLICY_ID, catalog_scope=True)
    svc = _make_service(policy)
    ctx = _Ctx(catalog_id="my-cat")

    fake_counter = _FakeCounter()
    with patch(
        "dynastore.modules.iam.conditions.get_protocol",
        return_value=fake_counter,
    ):
        allowed, _ = await svc.evaluate_access(
            principals=[_ROLE_NAME],
            path="/stac/catalogs/my-cat/collections/col1/items",
            method="POST",
            request_context=ctx,
            catalog_id="my-cat",
        )

    assert allowed is True
    assert fake_counter.calls, "incr_if_below must be called for a matching rate_limit condition"
    used_policy_id, principal_key, _ = fake_counter.calls[0]
    assert used_policy_id == _POLICY_ID
    assert principal_key == "my-cat", (
        f"scope=catalog must use catalog_id as principal_key, got {principal_key!r}"
    )


@pytest.mark.asyncio
async def test_no_injection_when_request_context_is_none() -> None:
    """compile_read_filter-style calls pass request_context=None.

    No _policy_id_by_config_id population should be attempted (the injection
    block is guarded: ``if request_context is not None``). We verify this by
    using a policy with NO conditions so condition evaluation is not reached —
    just the policy matching and ranking path. The call must not raise.
    """
    # A plain allow policy with no conditions (safe to call with None context).
    policy = Policy(
        id=_POLICY_ID,
        effect="ALLOW",
        actions=["POST", "PUT", "PATCH", "DELETE"],
        resources=[".*"],
        conditions=[],
    )
    svc = _make_service(policy)

    # No request context — this mirrors the compile_read_filter code path.
    # evaluate_access should not crash and should return a valid result.
    allowed, reason = await svc.evaluate_access(
        principals=[_ROLE_NAME],
        path=f"/stac/catalogs/{_CATALOG_ID}/collections/col1/items",
        method="POST",
        request_context=None,
        catalog_id=_CATALOG_ID,
    )
    # The unconditional ALLOW policy matches, so access is allowed.
    assert allowed is True

    # Verify the injection guard directly: calling _resolve_effective_policies
    # with request_context=None on a rate_limit policy must not crash (the
    # guard ``if request_context is not None`` skips population entirely).
    rl_policy = _rate_limit_policy(_POLICY_ID)
    svc2 = _make_service(rl_policy)
    policies = await svc2._resolve_effective_policies(
        principals=[_ROLE_NAME],
        schema="s_test",
        catalog_id=_CATALOG_ID,
        request_context=None,
    )
    # The policy is returned — it was resolved; condition eval happens in
    # evaluate_access, not here.
    assert any(p.id == _POLICY_ID for p in policies)


@pytest.mark.asyncio
async def test_pre_existing_mapping_is_not_overwritten() -> None:
    """If _policy_id_by_config_id already has entries (from the middleware or
    grant-quota step), the evaluate_access injection must merge into it,
    not replace it.
    """
    policy = _rate_limit_policy(_POLICY_ID)
    svc = _make_service(policy)
    ctx = _Ctx()
    ctx.principal_id = "user-xyz"

    # Pre-populate a sentinel entry that must survive after evaluate_access.
    sentinel_key = object()
    ctx.extras["_policy_id_by_config_id"] = {id(sentinel_key): "pre-existing-policy"}

    fake_counter = _FakeCounter()
    with patch(
        "dynastore.modules.iam.conditions.get_protocol",
        return_value=fake_counter,
    ):
        await svc.evaluate_access(
            principals=[_ROLE_NAME],
            path=f"/stac/catalogs/{_CATALOG_ID}/collections/col1/items",
            method="POST",
            request_context=ctx,
            catalog_id=_CATALOG_ID,
        )

    mapping = ctx.extras.get("_policy_id_by_config_id", {})
    assert mapping.get(id(sentinel_key)) == "pre-existing-policy", (
        "Pre-existing _policy_id_by_config_id entries must not be removed by evaluate_access"
    )
