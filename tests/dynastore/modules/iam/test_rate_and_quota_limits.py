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
    QueryParamHandler,
    AttributeMatchHandler,
    RateLimitHandler,
    _safe_regex_matches,
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
# _safe_regex_matches helper — invalid-pattern guard
# ---------------------------------------------------------------------------


class TestSafeRegexMatches:
    """Unit tests for the _safe_regex_matches helper."""

    def setup_method(self):
        from dynastore.modules.iam.conditions import _INVALID_REGEX_WARNED
        _INVALID_REGEX_WARNED.clear()

    def test_valid_pattern_matches(self):
        result = _safe_regex_matches("^/tiles/", "/tiles/foo", kind="path_pattern")
        assert result is True

    def test_valid_pattern_no_match(self):
        result = _safe_regex_matches("^/tiles/", "/assets/foo", kind="path_pattern")
        assert result is False

    def test_invalid_pattern_returns_none(self):
        result = _safe_regex_matches("*bad(", "/any/path", kind="path_pattern")
        assert result is None

    def test_invalid_bracket_returns_none(self):
        result = _safe_regex_matches("[", "/any/path", kind="query_match")
        assert result is None

    def test_invalid_unclosed_group_returns_none(self):
        result = _safe_regex_matches("(?P<unclosed", "/any/path", kind="path_pattern")
        assert result is None


# ---------------------------------------------------------------------------
# RateLimitHandler — invalid path_pattern does not crash (Layer A)
# ---------------------------------------------------------------------------


class TestRateLimitInvalidRegex:
    @pytest.mark.asyncio
    async def test_invalid_path_pattern_does_not_raise(self, counter, caplog):
        import logging
        from dynastore.modules.iam.conditions import _INVALID_REGEX_WARNED
        _INVALID_REGEX_WARNED.clear()  # ensure throttle doesn't suppress warning
        h = RateLimitHandler()
        cfg = _config(limit=1, window_seconds=60, path_pattern="*bad(")
        ctx = _ctx(path="/anything")
        # Must not raise — fail-open: condition treated as non-matching → allow
        with caplog.at_level(logging.WARNING, logger="dynastore.modules.iam.conditions"):
            result = await h.evaluate(cfg, ctx)
        assert result is True  # non-match direction = allow
        # WARNING was emitted
        assert any("*bad(" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_invalid_path_pattern_bracket_does_not_raise(self, counter, caplog):
        import logging
        from dynastore.modules.iam.conditions import _INVALID_REGEX_WARNED
        _INVALID_REGEX_WARNED.clear()
        h = RateLimitHandler()
        cfg = _config(limit=1, window_seconds=60, path_pattern="[")
        ctx = _ctx(path="/tiles/x")
        with caplog.at_level(logging.WARNING, logger="dynastore.modules.iam.conditions"):
            result = await h.evaluate(cfg, ctx)
        assert result is True
        assert any("path_pattern" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_valid_path_pattern_still_gates(self, counter):
        # Regression: a valid pattern must still work correctly.
        h = RateLimitHandler()
        cfg = _config(limit=1, window_seconds=60, path_pattern="^/tiles/")
        await h.evaluate(cfg, _ctx(path="/assets/x"))  # non-match, no budget consumed
        assert await h.evaluate(cfg, _ctx(path="/tiles/x")) is True
        with pytest.raises(RateLimitExceededError):
            await h.evaluate(cfg, _ctx(path="/tiles/x"))


# ---------------------------------------------------------------------------
# MaxCountHandler — invalid path_pattern does not crash (Layer A)
# ---------------------------------------------------------------------------


class TestMaxCountInvalidRegex:
    @pytest.mark.asyncio
    async def test_invalid_path_pattern_does_not_raise(self, counter, caplog):
        import logging
        from dynastore.modules.iam.conditions import _INVALID_REGEX_WARNED
        _INVALID_REGEX_WARNED.clear()
        h = MaxCountHandler()
        cfg = _config(limit=1, path_pattern="(?P<unclosed")
        ctx = _ctx(path="/items")
        with caplog.at_level(logging.WARNING, logger="dynastore.modules.iam.conditions"):
            result = await h.evaluate(cfg, ctx)
        assert result is True
        assert any("(?P<unclosed" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# QueryParamHandler — invalid pattern does not crash (Layer A)
# ---------------------------------------------------------------------------


class TestQueryParamHandlerInvalidRegex:
    def _qctx(self, params: dict) -> EvaluationContext:
        class _Req:
            headers: dict = {}
            client = None
        return EvaluationContext(
            request=_Req(),
            storage=None,
            query_params=params,
            path="/search",
            method="GET",
            extras={},
        )

    @pytest.mark.asyncio
    async def test_invalid_pattern_does_not_raise(self, caplog):
        import logging
        from dynastore.modules.iam.conditions import _INVALID_REGEX_WARNED
        _INVALID_REGEX_WARNED.clear()
        h = QueryParamHandler()
        cfg = {"param": "format", "pattern": "*bad_query("}
        ctx = self._qctx({"format": "json"})
        with caplog.at_level(logging.WARNING, logger="dynastore.modules.iam.conditions"):
            result = await h.evaluate(cfg, ctx)
        # No-match direction for query_match: val present but pattern bad → False
        assert result is False
        assert any("*bad_query(" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_valid_pattern_still_works(self):
        h = QueryParamHandler()
        cfg = {"param": "format", "pattern": "^(json|geojson)$"}
        ctx = self._qctx({"format": "json"})
        assert await h.evaluate(cfg, ctx) is True

    @pytest.mark.asyncio
    async def test_valid_pattern_no_match_returns_false(self):
        h = QueryParamHandler()
        cfg = {"param": "format", "pattern": "^json$"}
        ctx = self._qctx({"format": "xml"})
        assert await h.evaluate(cfg, ctx) is False


# ---------------------------------------------------------------------------
# AttributeMatchHandler — invalid regex operator does not crash (Layer A)
# ---------------------------------------------------------------------------


class TestAttributeMatchHandlerInvalidRegex:
    def _actx(self, path: str = "/foo") -> EvaluationContext:
        class _Req:
            headers: dict = {}
            client = None
        return EvaluationContext(
            request=_Req(),
            storage=None,
            query_params={},
            path=path,
            method="GET",
            extras={},
        )

    @pytest.mark.asyncio
    async def test_invalid_regex_in_match_operator_does_not_raise(self, caplog):
        import logging
        from dynastore.modules.iam.conditions import _INVALID_REGEX_WARNED
        _INVALID_REGEX_WARNED.clear()
        h = AttributeMatchHandler()
        cfg = {"attribute": "path", "operator": "regex", "value": "*bad_attr("}
        ctx = self._actx(path="/some/path")
        with caplog.at_level(logging.WARNING, logger="dynastore.modules.iam.conditions"):
            result = await h.evaluate(cfg, ctx)
        # No-match direction for attribute match: regex fails → False
        assert result is False
        assert any("*bad_attr(" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_valid_regex_still_matches(self):
        h = AttributeMatchHandler()
        cfg = {"attribute": "path", "operator": "regex", "value": "^/tiles/"}
        ctx = self._actx(path="/tiles/foo")
        assert await h.evaluate(cfg, ctx) is True

    @pytest.mark.asyncio
    async def test_valid_regex_no_match_returns_false(self):
        h = AttributeMatchHandler()
        cfg = {"attribute": "path", "operator": "regex", "value": "^/tiles/"}
        ctx = self._actx(path="/assets/foo")
        assert await h.evaluate(cfg, ctx) is False


# ---------------------------------------------------------------------------
# Protocol surface
# ---------------------------------------------------------------------------


def test_handlers_use_runtime_checkable_protocol():
    assert getattr(UsageCounterProtocol, "_is_runtime_protocol", False) is True
