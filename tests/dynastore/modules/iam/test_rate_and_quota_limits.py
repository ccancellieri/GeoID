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

"""Rate-limit / quota condition handlers tied to UsageCounterProtocol.

Hermetic: the handlers are exercised against a fake counter that
implements :class:`UsageCounterProtocol`. Live atomicity against
docker-compose Valkey + PG belongs in the integration suite — these
tests verify dispatch logic, scope resolution, path/method gating,
deny-on-exceed behaviour, and the typed exception class raised on
deny (so the middleware can choose the right HTTP status).
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import pytest

from dynastore.models.protocols.usage_counter import UsageCounterProtocol
from dynastore.modules.iam.conditions import (
    EvaluationContext,
    MaxCountHandler,
    RateLimitHandler,
)
from dynastore.modules.iam.exceptions import (
    QuotaExceededError,
    RateLimitExceededError,
)


# ---------------------------------------------------------------------------
# Fake UsageCounterProtocol — registered for the duration of the test.
# ---------------------------------------------------------------------------


class _FakeCounter:
    name = "fake-counter"
    priority = 1  # winning priority so get_protocol() returns this

    def __init__(self) -> None:
        self.counts: dict = {}
        self.calls: list = []

    def _key(self, p, k, w):
        return (p, k, w)

    async def incr(self, policy_id, principal_key, *, window_seconds=None, amount=1):
        self.calls.append(("incr", policy_id, principal_key, window_seconds, amount))
        k = self._key(policy_id, principal_key, window_seconds)
        self.counts[k] = self.counts.get(k, 0) + amount
        return self.counts[k]

    async def get(self, policy_id, principal_key, *, window_seconds=None):
        self.calls.append(("get", policy_id, principal_key, window_seconds))
        return self.counts.get(self._key(policy_id, principal_key, window_seconds), 0)

    async def incr_if_below(
        self, policy_id, principal_key, limit, *, window_seconds=None, amount=1
    ) -> Tuple[int, bool]:
        self.calls.append(
            ("incr_if_below", policy_id, principal_key, limit, window_seconds, amount)
        )
        k = self._key(policy_id, principal_key, window_seconds)
        cur = self.counts.get(k, 0)
        if cur + amount > limit:
            return (cur, False)
        self.counts[k] = cur + amount
        return (self.counts[k], True)

    async def reset(self, policy_id, principal_key, *, window_seconds=None):
        self.counts.pop(self._key(policy_id, principal_key, window_seconds), None)

    async def reap_expired(self):
        return 0


@pytest.fixture
def counter():
    from dynastore.tools.discovery import register_plugin, unregister_plugin

    c = _FakeCounter()
    register_plugin(c)
    # Bust any cached protocol lookup so the new instance wins.
    try:
        from dynastore.tools.discovery import _get_protocol_cached
        _get_protocol_cached.cache_clear()
    except Exception:
        pass
    try:
        yield c
    finally:
        unregister_plugin(c)
        try:
            from dynastore.tools.discovery import _get_protocol_cached
            _get_protocol_cached.cache_clear()
        except Exception:
            pass


def _ctx(
    *,
    path: str = "/tiles/foo",
    method: str = "GET",
    principal_id: str = "alice",
    client_ip: Optional[str] = "1.2.3.4",
) -> EvaluationContext:
    # Stub request with a .client.host attribute matching Starlette.
    class _Client:
        host = client_ip

    class _Req:
        client = _Client() if client_ip is not None else None

    return EvaluationContext(
        request=_Req() if client_ip is not None else None,
        storage=None,  # not used by these handlers
        principal_id=principal_id,
        path=path,
        method=method,
        extras={},
    )


def _config(**kwargs: Any) -> Dict[str, Any]:
    base = {"_policy_id": "policy-X"}
    base.update(kwargs)
    return base


# ---------------------------------------------------------------------------
# RateLimitHandler
# ---------------------------------------------------------------------------


class TestRateLimit:
    @pytest.mark.asyncio
    async def test_allows_under_limit(self, counter):
        h = RateLimitHandler()
        cfg = _config(limit=3, window_seconds=60, scope="principal")
        ctx = _ctx()
        for _ in range(3):
            assert await h.evaluate(cfg, ctx) is True

    @pytest.mark.asyncio
    async def test_denies_over_limit_with_typed_exception(self, counter):
        h = RateLimitHandler()
        cfg = _config(limit=2, window_seconds=60, scope="principal")
        ctx = _ctx()
        await h.evaluate(cfg, ctx)
        await h.evaluate(cfg, ctx)
        with pytest.raises(RateLimitExceededError):
            await h.evaluate(cfg, ctx)

    @pytest.mark.asyncio
    async def test_path_pattern_gates_off_non_matching_requests(self, counter):
        h = RateLimitHandler()
        cfg = _config(limit=1, window_seconds=60, path_pattern="^/tiles/")
        # /assets does not match — should pass through without consuming budget.
        await h.evaluate(cfg, _ctx(path="/assets/x"))
        # The single allowed hit on /tiles/ still passes.
        assert await h.evaluate(cfg, _ctx(path="/tiles/x")) is True
        # The next /tiles/ hit denies.
        with pytest.raises(RateLimitExceededError):
            await h.evaluate(cfg, _ctx(path="/tiles/x"))

    @pytest.mark.asyncio
    async def test_methods_allowlist(self, counter):
        h = RateLimitHandler()
        cfg = _config(limit=1, window_seconds=60, methods=["GET"])
        # POST is excluded from this rate-limit.
        await h.evaluate(cfg, _ctx(method="POST"))
        await h.evaluate(cfg, _ctx(method="POST"))
        # GET allowed once, second denies.
        await h.evaluate(cfg, _ctx(method="GET"))
        with pytest.raises(RateLimitExceededError):
            await h.evaluate(cfg, _ctx(method="GET"))

    @pytest.mark.asyncio
    async def test_scope_client_ip_keys_by_remote_host(self, counter):
        h = RateLimitHandler()
        cfg = _config(limit=1, window_seconds=60, scope="client_ip")
        await h.evaluate(cfg, _ctx(client_ip="1.1.1.1"))
        # Different IP — different bucket — still allowed.
        assert await h.evaluate(cfg, _ctx(client_ip="2.2.2.2")) is True
        # Same IP again — denied.
        with pytest.raises(RateLimitExceededError):
            await h.evaluate(cfg, _ctx(client_ip="1.1.1.1"))

    @pytest.mark.asyncio
    async def test_no_counter_protocol_graceful_default(self):
        # With no fake counter fixture, get_protocol(UsageCounterProtocol) is None.
        # Graceful mode (default) allows the request.
        h = RateLimitHandler()
        cfg = _config(limit=1, window_seconds=60)
        assert await h.evaluate(cfg, _ctx()) is True

    @pytest.mark.asyncio
    async def test_no_counter_protocol_strict_raises(self):
        h = RateLimitHandler()
        cfg = _config(limit=1, window_seconds=60, mode="strict")
        with pytest.raises(RateLimitExceededError):
            await h.evaluate(cfg, _ctx())

    @pytest.mark.asyncio
    async def test_missing_policy_id_skips_enforcement(self, counter):
        # Caller (middleware) forgot to inject _policy_id — allow but no-op.
        h = RateLimitHandler()
        cfg = {"limit": 1, "window_seconds": 60}
        assert await h.evaluate(cfg, _ctx()) is True
        # Counter was NOT touched.
        assert not any(c[0] == "incr_if_below" for c in counter.calls)

    @pytest.mark.asyncio
    async def test_inspect_returns_usage_summary(self, counter):
        h = RateLimitHandler()
        cfg = _config(limit=5, window_seconds=60)
        ctx = _ctx()
        await h.evaluate(cfg, ctx)
        await h.evaluate(cfg, ctx)
        summary = await h.inspect(cfg, ctx)
        assert summary["type"] == "rate_limit"
        assert summary["limit"] == 5
        assert summary["used"] == 2
        assert summary["remaining"] == 3
        assert summary["window_seconds"] == 60
        assert "reset_at" in summary


# ---------------------------------------------------------------------------
# MaxCountHandler
# ---------------------------------------------------------------------------


class TestMaxCount:
    @pytest.mark.asyncio
    async def test_allows_under_quota(self, counter):
        h = MaxCountHandler()
        cfg = _config(limit=3, scope="principal")
        ctx = _ctx()
        for _ in range(3):
            assert await h.evaluate(cfg, ctx) is True

    @pytest.mark.asyncio
    async def test_denies_when_quota_exhausted(self, counter):
        h = MaxCountHandler()
        cfg = _config(limit=2)
        ctx = _ctx()
        await h.evaluate(cfg, ctx)
        await h.evaluate(cfg, ctx)
        with pytest.raises(QuotaExceededError):
            await h.evaluate(cfg, ctx)

    @pytest.mark.asyncio
    async def test_legacy_max_count_config_key_still_works(self, counter):
        # Operators may have stored quotas under "max_count" rather than "limit".
        h = MaxCountHandler()
        cfg = _config(max_count=1)
        ctx = _ctx()
        await h.evaluate(cfg, ctx)
        with pytest.raises(QuotaExceededError):
            await h.evaluate(cfg, ctx)

    @pytest.mark.asyncio
    async def test_inspect_reflects_lifetime_state(self, counter):
        h = MaxCountHandler()
        cfg = _config(limit=10)
        ctx = _ctx()
        for _ in range(4):
            await h.evaluate(cfg, ctx)
        summary = await h.inspect(cfg, ctx)
        assert summary["type"] == "max_count"
        assert summary["limit"] == 10
        assert summary["used"] == 4
        assert summary["remaining"] == 6
        # No reset_at for lifetime quotas.
        assert "reset_at" not in summary


# ---------------------------------------------------------------------------
# Protocol surface
# ---------------------------------------------------------------------------


def test_handlers_use_runtime_checkable_protocol():
    assert getattr(UsageCounterProtocol, "_is_runtime_protocol", False) is True
