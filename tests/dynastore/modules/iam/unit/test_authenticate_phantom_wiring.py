"""Wiring tests for the phantom-token resolution path in IamService (#1343).

These exercise ``_resolve_identity_maybe_cached`` — the seam that routes a
validated identity either through the phantom-token L1+L2 tier or the
authoritative DB resolver — including the JSON round-trip of the cached
``Principal``. The heavy provider / storage stack is stubbed; the cache module
itself is covered by ``test_phantom_token``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import pytest

from dynastore.modules.iam import phantom_token
from dynastore.modules.iam.iam_service import IamService
from dynastore.modules.iam.models import Principal


def _scale_cfg(**overrides: Any) -> Any:
    base = dict(
        phantom_token_resolution_enabled=True,
        binding_resolution_ttl_seconds=300,
        denylist_enabled=True,
        denylist_ttl_seconds=300,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class _StubService:
    """Minimal stand-in carrying just what the method under test calls."""

    def __init__(self, resolved: Optional[Tuple[List[str], Principal]]) -> None:
        self._resolved = resolved
        self.resolve_calls = 0

    async def _resolve_effective_identity(
        self, identity: Dict[str, Any], catalog_id: Optional[str], schema: str
    ) -> Optional[Tuple[List[str], Principal]]:
        self.resolve_calls += 1
        return self._resolved


@pytest.fixture(autouse=True)
def _reset() -> Any:
    phantom_token._reset_caches()
    yield
    phantom_token._reset_caches()


@pytest.mark.asyncio
async def test_inactive_uses_db_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(phantom_token, "phantom_token_active", lambda cfg: False)
    principal = Principal(subject_id="u1", provider="oidc", roles=["viewer"])
    stub = _StubService((["viewer"], principal))

    result = await IamService._resolve_identity_maybe_cached(
        stub,  # type: ignore[arg-type]
        {"sub": "u1", "provider": "oidc"},
        None,
        "iam",
        _scale_cfg(),
    )

    assert result is not None
    roles, p = result
    assert roles == ["viewer"]
    assert p is principal  # DB path returns the live object, not a round-trip
    assert stub.resolve_calls == 1


@pytest.mark.asyncio
async def test_active_round_trips_principal_through_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(phantom_token, "phantom_token_active", lambda cfg: True)

    # Simulate a cache MISS: resolve_bindings_cached just runs the resolver and
    # returns its JSON-safe dict (the real tier is unit-tested elsewhere).
    async def fake_cached(*, provider, subject_id, schema, resolver, cfg):  # noqa: ANN001
        return await resolver()

    monkeypatch.setattr(phantom_token, "resolve_bindings_cached", fake_cached)

    principal = Principal(subject_id="u1", provider="oidc", roles=["editor"])
    stub = _StubService((["editor"], principal))

    result = await IamService._resolve_identity_maybe_cached(
        stub,  # type: ignore[arg-type]
        {"sub": "u1", "provider": "oidc"},
        None,
        "iam",
        _scale_cfg(),
    )

    assert result is not None
    roles, p = result
    assert roles == ["editor"]
    # Rehydrated from the cached JSON projection — equal value, fresh instance.
    assert isinstance(p, Principal)
    assert p.subject_id == "u1"
    assert p.roles == ["editor"]
    assert stub.resolve_calls == 1


@pytest.mark.asyncio
async def test_active_cached_negative_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(phantom_token, "phantom_token_active", lambda cfg: True)

    async def fake_cached(*, provider, subject_id, schema, resolver, cfg):  # noqa: ANN001
        return await resolver()

    monkeypatch.setattr(phantom_token, "resolve_bindings_cached", fake_cached)
    stub = _StubService(None)  # identity maps to no principal

    result = await IamService._resolve_identity_maybe_cached(
        stub,  # type: ignore[arg-type]
        {"sub": "ghost", "provider": "oidc"},
        None,
        "iam",
        _scale_cfg(),
    )

    assert result is None


@pytest.mark.asyncio
async def test_active_cache_hit_rejects_inactive_principal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A stale positive entry for a now-deactivated principal must be denied on
    # the cache hit, not served (the account-state gate runs on every hit).
    monkeypatch.setattr(phantom_token, "phantom_token_active", lambda cfg: True)
    inactive = Principal(
        subject_id="u1", provider="oidc", roles=["editor"], is_active=False
    )

    async def fake_cached(**kwargs: Any) -> Any:
        return {"roles": ["editor"], "principal": inactive.model_dump(mode="json")}

    monkeypatch.setattr(phantom_token, "resolve_bindings_cached", fake_cached)
    stub = _StubService(None)

    result = await IamService._resolve_identity_maybe_cached(
        stub,  # type: ignore[arg-type]
        {"sub": "u1", "provider": "oidc"},
        None,
        "iam",
        _scale_cfg(),
    )

    assert result is None


@pytest.mark.asyncio
async def test_active_cache_hit_rejects_expired_principal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(phantom_token, "phantom_token_active", lambda cfg: True)
    expired = Principal(
        subject_id="u1",
        provider="oidc",
        roles=["editor"],
        valid_until=datetime.now(timezone.utc) - timedelta(hours=1),
    )

    async def fake_cached(**kwargs: Any) -> Any:
        return {"roles": ["editor"], "principal": expired.model_dump(mode="json")}

    monkeypatch.setattr(phantom_token, "resolve_bindings_cached", fake_cached)
    stub = _StubService(None)

    result = await IamService._resolve_identity_maybe_cached(
        stub,  # type: ignore[arg-type]
        {"sub": "u1", "provider": "oidc"},
        None,
        "iam",
        _scale_cfg(),
    )

    assert result is None


@pytest.mark.asyncio
async def test_active_but_no_subject_falls_back_to_db(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # phantom is "active" but the identity has no sub -> must not cache; DB path.
    monkeypatch.setattr(phantom_token, "phantom_token_active", lambda cfg: True)

    called = {"cache": 0}

    async def fake_cached(**kwargs: Any) -> Any:
        called["cache"] += 1
        return None

    monkeypatch.setattr(phantom_token, "resolve_bindings_cached", fake_cached)
    principal = Principal(subject_id="", provider="oidc", roles=["viewer"])
    stub = _StubService((["viewer"], principal))

    result = await IamService._resolve_identity_maybe_cached(
        stub,  # type: ignore[arg-type]
        {"provider": "oidc"},  # no "sub"
        None,
        "iam",
        _scale_cfg(),
    )

    assert result is not None
    assert called["cache"] == 0  # never reached the cache without a subject
    assert stub.resolve_calls == 1
