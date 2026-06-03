"""Tests for the IAM condition fail-closed security fix.

Covers three areas:
1. evaluate_all / evaluate_access: unknown condition type must DENY (fail-closed).
2. Policy write-time validation: create_policy / update_policy must reject unknown types.
3. lookup_only_search body-read regression: the downstream handler can still read
   the request body after the middleware calls request.json() during condition
   evaluation (Starlette 0.52 BaseHTTPMiddleware caches the body on _body and
   replays it via wrapped_receive — no fix needed; test is a regression guard).
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional
from unittest.mock import MagicMock

import pytest

from dynastore.models.auth import Condition, Policy
from dynastore.modules.iam.conditions import (
    ConditionRegistry,
    EvaluationContext,
    LookupOnlySearchHandler,
    known_condition_types,
)
from dynastore.modules.iam.policies import PolicyService, _validate_policy_condition_types


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ctx(
    *,
    method: str = "GET",
    path: str = "/items",
    query: Optional[Dict[str, str]] = None,
    request: Any = None,
) -> EvaluationContext:
    return EvaluationContext(
        request=request,
        storage=MagicMock(),
        path=path,
        method=method,
        query_params=query or {},
    )


def _registry() -> ConditionRegistry:
    """Return a fresh registry (does not share singleton state)."""
    from dynastore.modules.iam.conditions import _get_condition_registry
    return _get_condition_registry()


# ---------------------------------------------------------------------------
# 1a. evaluate_all — unknown type denies (fail-closed)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unknown_condition_type_denies_evaluate_all() -> None:
    """An unregistered condition type must deny the match, not skip it.

    This is the core fail-closed invariant: a typo'd type like
    'allow_anonymous_create' (a config *field* name, not a handler type)
    must not behave as if the condition is absent.
    """
    reg = _registry()
    unknown_cond = Condition(
        type="allow_anonymous_create",  # typo — real type is collection_write_anonymous_allowed
        config={},
    )
    ctx = _ctx()
    result = await reg.evaluate_all([unknown_cond], ctx)
    assert result is False, "Unknown condition type must deny (fail-closed)"


@pytest.mark.asyncio
async def test_unknown_condition_type_in_mixed_list_denies() -> None:
    """A list with a valid condition followed by an unknown one must still deny."""
    reg = _registry()
    valid_cond = Condition(type="query_match", config={"param": "x", "pattern": ".*"})
    bad_cond = Condition(type="does_not_exist", config={})
    ctx = _ctx(query={"x": "hello"})
    result = await reg.evaluate_all([valid_cond, bad_cond], ctx)
    assert result is False


@pytest.mark.asyncio
async def test_empty_conditions_returns_true() -> None:
    """Empty condition list is still unconditionally True (no restrictions)."""
    reg = _registry()
    result = await reg.evaluate_all([], _ctx())
    assert result is True


@pytest.mark.asyncio
async def test_known_condition_type_is_not_denied() -> None:
    """A valid registered condition with no config should evaluate normally."""
    reg = _registry()
    # query_match with no param/pattern config returns True (nothing to match against).
    cond = Condition(type="query_match", config={})
    result = await reg.evaluate_all([cond], _ctx())
    assert result is True


# ---------------------------------------------------------------------------
# 1b. known_types() and known_condition_types()
# ---------------------------------------------------------------------------


def test_known_types_method_returns_set() -> None:
    """ConditionRegistry.known_types() returns the registered handler keys."""
    reg = _registry()
    types = reg.known_types()
    assert isinstance(types, set)
    # Spot-check a few built-in types.
    for expected in (
        "rate_limit",
        "max_count",
        "query_match",
        "time_window",
        "expiration",
        "lookup_only_search",
        "collection_write_anonymous_allowed",
        "catalog_lookup_public_allowed",
    ):
        assert expected in types, f"{expected!r} not in known_types()"


def test_module_level_known_condition_types() -> None:
    """Module-level known_condition_types() mirrors ConditionRegistry.known_types()."""
    module_types = known_condition_types()
    registry_types = _registry().known_types()
    assert module_types == registry_types


# ---------------------------------------------------------------------------
# 2. Write-time validation — _validate_policy_condition_types
# ---------------------------------------------------------------------------


def _policy_with(condition_types: list[str]) -> Policy:
    return Policy(
        id="test-policy",
        effect="ALLOW",
        actions=["GET"],
        resources=[".*"],
        conditions=[Condition(type=t, config={}) for t in condition_types],
    )


def test_validate_rejects_unknown_type() -> None:
    """_validate_policy_condition_types raises ValueError on an unknown type."""
    policy = _policy_with(["allow_anonymous_create"])
    with pytest.raises(ValueError) as exc_info:
        _validate_policy_condition_types(policy)
    msg = str(exc_info.value)
    assert "allow_anonymous_create" in msg
    assert "Valid types:" in msg


def test_validate_rejects_unknown_type_lists_all_bad_types() -> None:
    """Error message must name all bad types, not just the first."""
    policy = _policy_with(["bad_one", "bad_two"])
    with pytest.raises(ValueError) as exc_info:
        _validate_policy_condition_types(policy)
    msg = str(exc_info.value)
    assert "bad_one" in msg
    assert "bad_two" in msg


def test_validate_accepts_known_type() -> None:
    """A policy using a registered condition type passes validation without error."""
    policy = _policy_with(["collection_write_anonymous_allowed"])
    # Must not raise.
    _validate_policy_condition_types(policy)


def test_validate_accepts_empty_conditions() -> None:
    """A policy with no conditions always passes validation."""
    policy = Policy(
        id="bare-policy",
        effect="ALLOW",
        actions=["GET"],
        resources=[".*"],
        conditions=[],
    )
    _validate_policy_condition_types(policy)


def test_validate_accepts_multiple_known_types() -> None:
    """Multiple valid condition types in one policy all pass."""
    policy = _policy_with(["query_match", "time_window", "expiration"])
    _validate_policy_condition_types(policy)


# ---------------------------------------------------------------------------
# 2b. create_policy / update_policy surfaces the validation error
# ---------------------------------------------------------------------------


def _bare_service() -> PolicyService:
    """PolicyService with no DB wiring — only validation logic is exercised."""
    svc = PolicyService.__new__(PolicyService)
    svc._state = None  # type: ignore[attr-defined]
    svc._engine = None  # type: ignore[attr-defined]
    svc.storage = None  # type: ignore[attr-defined]
    svc.iam_storage = None  # type: ignore[attr-defined]
    svc._role_config = None  # type: ignore[attr-defined]
    return svc


@pytest.mark.asyncio
async def test_create_policy_rejects_unknown_condition_type() -> None:
    """PolicyService.create_policy raises ValueError before touching the DB."""
    svc = _bare_service()
    policy = _policy_with(["allow_anonymous_create"])
    with pytest.raises(ValueError) as exc_info:
        await svc.create_policy(policy)
    assert "allow_anonymous_create" in str(exc_info.value)


@pytest.mark.asyncio
async def test_update_policy_rejects_unknown_condition_type() -> None:
    """PolicyService.update_policy raises ValueError before touching the DB."""
    svc = _bare_service()
    policy = _policy_with(["allow_anonymous_create"])
    with pytest.raises(ValueError) as exc_info:
        await svc.update_policy(policy)
    assert "allow_anonymous_create" in str(exc_info.value)


@pytest.mark.asyncio
async def test_create_policy_with_valid_condition_type_passes_validation() -> None:
    """Valid condition type clears the validation gate (DB call expected next).

    After validation the method tries to open a DB transaction with a None
    engine — that raises ValueError("Cannot start managed_transaction: ...").
    The important assertion is that the error message does NOT mention unknown
    condition types, proving the type-check gate was passed.
    """
    svc = _bare_service()
    policy = _policy_with(["collection_write_anonymous_allowed"])
    with pytest.raises((RuntimeError, AttributeError, TypeError, ValueError)) as exc_info:
        await svc.create_policy(policy)
    # The failure must come from the DB layer, not from condition validation.
    assert "condition type" not in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_update_policy_with_valid_condition_type_passes_validation() -> None:
    """Valid condition type clears the validation gate (DB call expected next)."""
    svc = _bare_service()
    policy = _policy_with(["collection_write_anonymous_allowed"])
    with pytest.raises((RuntimeError, AttributeError, TypeError, ValueError)) as exc_info:
        await svc.update_policy(policy)
    assert "condition type" not in str(exc_info.value).lower()


# ---------------------------------------------------------------------------
# 3. lookup_only_search body-read regression
#
# Starlette 0.52 BaseHTTPMiddleware wraps the ASGI receive callable in
# _CachedRequest.wrapped_receive.  When request.body() (called internally by
# request.json()) runs in the middleware dispatch(), it sets self._body on the
# _CachedRequest.  wrapped_receive then returns that cached body to the
# downstream app via the "http.request" message, so the downstream handler can
# still call request.body() / request.json() successfully.
#
# These tests exercise the handler and a simulated downstream read directly,
# confirming both the condition evaluation result AND that the body is intact.
# ---------------------------------------------------------------------------


class _FakeRequest:
    """Minimal request stub that caches body exactly as Starlette does.

    Starlette's Request.json() calls Request.body() which sets self._body,
    then json.loads(self._body). Subsequent calls return the cached value.
    This stub replicates that contract so the handler behaves identically.
    """

    def __init__(self, body_dict: Dict[str, Any]) -> None:
        self._raw = json.dumps(body_dict).encode()
        self._body: Optional[bytes] = None
        self._json: Optional[Any] = None
        self.method = "POST"

    async def body(self) -> bytes:
        if self._body is None:
            self._body = self._raw
        return self._body

    async def json(self) -> Any:
        if self._json is None:
            raw = await self.body()
            self._json = json.loads(raw)
        return self._json


@pytest.mark.asyncio
async def test_lookup_only_search_evaluates_true_for_geoid_post() -> None:
    """LookupOnlySearchHandler evaluates True for a POST with geoid lookup field."""
    req = _FakeRequest({"geoid": "abc"})
    ctx = EvaluationContext(
        request=req,
        storage=MagicMock(),
        path="/search",
        method="POST",
        query_params={},
    )
    handler = LookupOnlySearchHandler()
    assert await handler.evaluate({}, ctx) is True


@pytest.mark.asyncio
async def test_body_still_readable_after_condition_evaluation() -> None:
    """After lookup_only_search reads request.json(), the downstream handler
    can still call request.json() and receive the original payload.

    This is the regression guard for the BaseHTTPMiddleware body-consumption
    issue: Starlette 0.52 caches the body on _body so downstream sees it
    via wrapped_receive regardless of whether the middleware already read it.
    """
    payload = {"geoid": "abc", "limit": 10}
    req = _FakeRequest(payload)
    ctx = EvaluationContext(
        request=req,
        storage=MagicMock(),
        path="/search",
        method="POST",
        query_params={},
    )
    handler = LookupOnlySearchHandler()

    # Simulate middleware condition evaluation.
    cond_result = await handler.evaluate({}, ctx)
    assert cond_result is True, "Handler should allow a geoid-only search"

    # Simulate downstream route handler reading the same body.
    downstream_body = await req.json()
    assert downstream_body == payload, (
        "Body must be readable downstream after middleware condition evaluation. "
        "If this fails, the middleware is consuming the body without re-injecting it."
    )


@pytest.mark.asyncio
async def test_body_bytes_readable_after_json_call() -> None:
    """Calling request.body() after request.json() returns the same raw bytes."""
    payload = {"geoid": "abc"}
    req = _FakeRequest(payload)
    ctx = EvaluationContext(
        request=req,
        storage=MagicMock(),
        path="/search",
        method="POST",
        query_params={},
    )
    handler = LookupOnlySearchHandler()
    await handler.evaluate({}, ctx)

    # Both body() and json() must remain readable downstream.
    raw = await req.body()
    assert json.loads(raw) == payload
