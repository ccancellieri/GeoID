"""Unit tests for :class:`IamService` admin denylist wrappers (#1343).

These cover the thin service-layer wrappers that the admin REST surface
uses (``deny_subject`` / ``undeny_subject`` / ``list_denylist``). The
phantom-token primitives themselves are exercised in
``test_phantom_token``; here we lock in:

  * the service-layer TTL clamp against
    :attr:`IamScaleConfig.denylist_ttl_seconds`,
  * the explicit ``DenylistBackendUnavailable`` translation,
  * the verbatim pass-through of the ``reason`` field and storage
    subject (no extra translation between service and store — the route
    layer is the only place that converts wire-form ``jti:`` / ``principal:``
    prefixes).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Optional

import pytest

from dynastore.modules.iam import phantom_token
from dynastore.modules.iam.iam_service import IamService


class _StubIam(IamService):  # type: ignore[misc]
    """Minimal IamService subclass that skips ``__init__`` — we only need the
    method bindings; the wrappers don't touch ``self.storage`` etc."""

    def __init__(self) -> None:  # pragma: no cover - trivial
        pass


def _patch_scale(monkeypatch: pytest.MonkeyPatch, *, ceiling: int = 300) -> None:
    cfg = SimpleNamespace(denylist_ttl_seconds=ceiling, denylist_enabled=True)

    async def _fake_get() -> Any:
        return cfg

    import dynastore.modules.iam.scale_config as sc

    monkeypatch.setattr(sc, "get_iam_scale_config", _fake_get)


@pytest.mark.asyncio
async def test_deny_subject_clamps_above_ceiling(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_scale(monkeypatch, ceiling=300)
    seen = {}

    async def _fake_deny(token_id: str, *, ttl_seconds: int, reason: Optional[str] = None) -> None:
        seen["token_id"] = token_id
        seen["ttl"] = ttl_seconds
        seen["reason"] = reason

    monkeypatch.setattr(phantom_token, "denylist_backend_available", lambda: True)
    monkeypatch.setattr(phantom_token, "deny", _fake_deny)

    eff = await _StubIam().deny_subject("abc", ttl_seconds=3600, reason="leak")
    assert eff == 300
    assert seen == {"token_id": "abc", "ttl": 300, "reason": "leak"}


@pytest.mark.asyncio
async def test_deny_subject_below_ceiling_keeps_request(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_scale(monkeypatch, ceiling=300)
    seen = {}

    async def _fake_deny(token_id: str, *, ttl_seconds: int, reason: Optional[str] = None) -> None:
        seen["ttl"] = ttl_seconds

    monkeypatch.setattr(phantom_token, "denylist_backend_available", lambda: True)
    monkeypatch.setattr(phantom_token, "deny", _fake_deny)

    eff = await _StubIam().deny_subject("abc", ttl_seconds=60, reason=None)
    assert eff == 60
    assert seen == {"ttl": 60}


@pytest.mark.asyncio
async def test_deny_subject_none_ttl_uses_ceiling(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_scale(monkeypatch, ceiling=300)
    monkeypatch.setattr(phantom_token, "denylist_backend_available", lambda: True)
    captured = {}

    async def _fake_deny(token_id: str, *, ttl_seconds: int, reason: Optional[str] = None) -> None:
        captured["ttl"] = ttl_seconds

    monkeypatch.setattr(phantom_token, "deny", _fake_deny)
    eff = await _StubIam().deny_subject("abc", ttl_seconds=None, reason=None)
    assert eff == 300
    assert captured == {"ttl": 300}


@pytest.mark.asyncio
async def test_deny_subject_raises_when_backend_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_scale(monkeypatch, ceiling=300)
    monkeypatch.setattr(phantom_token, "denylist_backend_available", lambda: False)
    # Even though ``deny()`` itself would no-op silently on a None backend,
    # the admin path explicitly raises so the REST surface can surface 503.
    with pytest.raises(phantom_token.DenylistBackendUnavailable):
        await _StubIam().deny_subject("abc", ttl_seconds=300, reason=None)


@pytest.mark.asyncio
async def test_undeny_subject_delegates_to_phantom_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = {}

    async def _fake_undeny(token_id: str) -> bool:
        called["token_id"] = token_id
        return True

    monkeypatch.setattr(phantom_token, "undeny", _fake_undeny)
    assert await _StubIam().undeny_subject("sub:user-1") is True
    assert called == {"token_id": "sub:user-1"}


@pytest.mark.asyncio
async def test_list_denylist_delegates_with_prefix_and_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = {}

    async def _fake_list(*, prefix: Optional[str] = None, limit: int = 100) -> list:
        captured["prefix"] = prefix
        captured["limit"] = limit
        return [{"token_id": "abc", "reason": None, "expires_at": None}]

    monkeypatch.setattr(phantom_token, "list_denied", _fake_list)
    out = await _StubIam().list_denylist(prefix="sub:", limit=42)
    assert captured == {"prefix": "sub:", "limit": 42}
    assert out == [{"token_id": "abc", "reason": None, "expires_at": None}]
