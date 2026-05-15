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

"""Protocol-conformance + key-shape tests for the Valkey usage counter.

Live INCRBY atomicity belongs in the integration suite (PR-A4) where a
docker-compose Valkey is available. This module verifies the facade
delegates correctly to whatever :class:`CountingCacheBackend` the cache
manager hands it.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dynastore.models.protocols.cache import CountingCacheBackend
from dynastore.models.protocols.usage_counter import UsageCounterProtocol
from dynastore.modules.iam.usage_counter_bucket import bucket_for
from dynastore.modules.iam.usage_counter_valkey import (
    ValkeyUsageCounter,
    _key_for,
    _ttl_seconds,
)


# ---------------------------------------------------------------------------
# Fake CountingCacheBackend — records calls without spinning up Valkey.
# ---------------------------------------------------------------------------


class _FakeCountingBackend:
    """In-memory CountingCacheBackend stand-in for unit tests."""

    name = "fake-counting"
    priority = 999

    def __init__(self) -> None:
        self._store: dict = {}
        self.calls: list = []

    async def get(self, key):  # CacheBackend.get
        self.calls.append(("get", key))
        return self._store.get(key)

    async def set(self, key, value, *, ttl=None, exist=None):
        self.calls.append(("set", key, ttl))
        self._store[key] = value
        return True

    async def clear(self, *, key=None, namespace=None, tags=None):
        self.calls.append(("clear", key, namespace))
        if key is not None and key in self._store:
            del self._store[key]
            return True
        return False

    async def exists(self, key):
        return key in self._store

    async def close(self):
        return None

    async def get_count(self, key):
        self.calls.append(("get_count", key))
        return self._store.get(key)

    async def incr(self, key, amount=1, *, ttl=None):
        self.calls.append(("incr", key, amount, ttl))
        self._store[key] = int(self._store.get(key, 0)) + amount
        return self._store[key]

    async def incr_if_below(self, key, limit, amount=1, *, ttl=None):
        self.calls.append(("incr_if_below", key, limit, amount, ttl))
        cur = int(self._store.get(key, 0))
        if cur + amount > limit:
            return (cur, False)
        self._store[key] = cur + amount
        return (self._store[key], True)

    async def expireat(self, key, ts):
        self.calls.append(("expireat", key, ts))
        return True


class TestKeyShape:
    def test_key_is_bucket_aligned(self):
        bucket = datetime(2026, 5, 15, 12, 34, 0, tzinfo=timezone.utc)
        key = _key_for("policy-X", "principal-Y", bucket)
        # Bucket is encoded as Unix-second timestamp.
        assert key == f"uc:policy-X:principal-Y:{int(bucket.timestamp())}"

    def test_same_bucket_yields_same_key(self):
        a = datetime(2026, 5, 15, 12, 34, 1, tzinfo=timezone.utc)
        b = datetime(2026, 5, 15, 12, 34, 59, tzinfo=timezone.utc)
        bucket_a = bucket_for(60, now=a)
        bucket_b = bucket_for(60, now=b)
        assert _key_for("p", "k", bucket_a) == _key_for("p", "k", bucket_b)


class TestTtlComputation:
    def test_lifetime_quota_has_no_ttl(self):
        bucket = bucket_for(None)
        assert _ttl_seconds(None, bucket) is None

    def test_rate_window_has_grace_ttl(self):
        # Freshly-floored bucket; grace = 1 full window minimum.
        bucket = bucket_for(60)
        ttl = _ttl_seconds(60, bucket)
        assert ttl is not None
        assert ttl >= 60  # at least one full window of grace
        assert ttl <= 120  # never more than the 2-window cap


class TestProtocolConformance:
    def test_valkey_driver_methods_match_protocol(self):
        for method in ("incr", "get", "incr_if_below", "reset", "reap_expired"):
            assert callable(getattr(ValkeyUsageCounter, method)), method
        assert hasattr(ValkeyUsageCounter, "name")
        assert hasattr(ValkeyUsageCounter, "priority")

    def test_protocol_is_runtime_checkable(self):
        assert getattr(UsageCounterProtocol, "_is_runtime_protocol", False) is True

    def test_counting_protocol_is_runtime_checkable(self):
        # Layered driver isinstance-checks the active backend.
        assert getattr(CountingCacheBackend, "_is_runtime_protocol", False) is True

    def test_fake_backend_satisfies_counting_protocol(self):
        # Sanity check the fake — if this drifts from CountingCacheBackend
        # the rest of the test suite is meaningless.
        assert isinstance(_FakeCountingBackend(), CountingCacheBackend)


class TestDelegation:
    @pytest.mark.asyncio
    async def test_incr_if_below_delegates_with_bucketed_key(self):
        backend = _FakeCountingBackend()
        driver = ValkeyUsageCounter(backend=backend)

        new_count, allowed = await driver.incr_if_below(
            "p1", "user-7", limit=5, window_seconds=60
        )
        assert allowed is True
        assert new_count == 1

        # Replay the same call — should still be allowed up to limit.
        for _ in range(4):
            _, allowed = await driver.incr_if_below(
                "p1", "user-7", limit=5, window_seconds=60
            )
            assert allowed is True

        # Sixth call exceeds limit.
        new_count, allowed = await driver.incr_if_below(
            "p1", "user-7", limit=5, window_seconds=60
        )
        assert allowed is False
        assert new_count == 5

    @pytest.mark.asyncio
    async def test_get_uses_get_count_not_generic_get(self):
        # ``get()`` must call ``get_count`` so the msgpack decode path is
        # bypassed for native INCRBY integers.
        backend = _FakeCountingBackend()
        driver = ValkeyUsageCounter(backend=backend)
        await driver.incr("p1", "user-7", window_seconds=60)
        await driver.get("p1", "user-7", window_seconds=60)
        assert any(call[0] == "get_count" for call in backend.calls)
        assert not any(call[0] == "get" for call in backend.calls)

    @pytest.mark.asyncio
    async def test_reset_clears_the_bucket_key(self):
        backend = _FakeCountingBackend()
        driver = ValkeyUsageCounter(backend=backend)
        await driver.incr("p1", "user-7", window_seconds=60)
        await driver.reset("p1", "user-7", window_seconds=60)
        assert await driver.get("p1", "user-7", window_seconds=60) == 0

    @pytest.mark.asyncio
    async def test_reap_expired_is_a_noop(self):
        # Valkey expires natively — drivers report zero rows reaped.
        backend = _FakeCountingBackend()
        driver = ValkeyUsageCounter(backend=backend)
        assert await driver.reap_expired() == 0

    @pytest.mark.asyncio
    async def test_lifetime_quota_passes_none_ttl(self):
        backend = _FakeCountingBackend()
        driver = ValkeyUsageCounter(backend=backend)
        await driver.incr("p1", "user-7", window_seconds=None)
        # ttl in the recorded ``incr`` call must be None.
        incr_call = next(c for c in backend.calls if c[0] == "incr")
        assert incr_call[3] is None
