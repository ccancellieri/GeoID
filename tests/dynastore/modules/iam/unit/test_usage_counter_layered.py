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

"""Hermetic tests for :class:`LayeredUsageCounter`.

Live atomicity (one pod + one Valkey + one PG, hammered concurrently)
is covered by the integration suite added with PR-A4. This module
verifies the routing logic: which calls go to Valkey, which calls
fall through to PG, lifetime-vs-window write-through behaviour, and
reset/reap delegation.
"""

from __future__ import annotations

from typing import Optional, Tuple

import pytest

from dynastore.models.protocols.usage_counter import UsageCounterProtocol
from dynastore.modules.iam.usage_counter_layered import LayeredUsageCounter


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _FakeDriver:
    """Bare UsageCounterProtocol stand-in — records calls and returns scripted results."""

    name = "fake"
    priority = 999

    def __init__(self) -> None:
        self.calls: list = []
        self.counts: dict = {}
        self.fail_modes: dict = {}  # method -> exception instance

    def _key(self, p, k, w):
        return (p, k, w)

    async def incr(self, policy_id, principal_key, *, window_seconds=None, amount=1):
        self.calls.append(("incr", policy_id, principal_key, window_seconds, amount))
        if "incr" in self.fail_modes:
            raise self.fail_modes["incr"]
        k = self._key(policy_id, principal_key, window_seconds)
        self.counts[k] = self.counts.get(k, 0) + amount
        return self.counts[k]

    async def get(self, policy_id, principal_key, *, window_seconds=None):
        self.calls.append(("get", policy_id, principal_key, window_seconds))
        if "get" in self.fail_modes:
            raise self.fail_modes["get"]
        return self.counts.get(self._key(policy_id, principal_key, window_seconds), 0)

    async def incr_if_below(
        self, policy_id, principal_key, limit, *, window_seconds=None, amount=1
    ) -> Tuple[int, bool]:
        self.calls.append(
            (
                "incr_if_below",
                policy_id,
                principal_key,
                limit,
                window_seconds,
                amount,
            )
        )
        if "incr_if_below" in self.fail_modes:
            raise self.fail_modes["incr_if_below"]
        k = self._key(policy_id, principal_key, window_seconds)
        cur = self.counts.get(k, 0)
        if cur + amount > limit:
            return (cur, False)
        self.counts[k] = cur + amount
        return (self.counts[k], True)

    async def reset(self, policy_id, principal_key, *, window_seconds=None):
        self.calls.append(("reset", policy_id, principal_key, window_seconds))
        if "reset" in self.fail_modes:
            raise self.fail_modes["reset"]
        self.counts.pop(self._key(policy_id, principal_key, window_seconds), None)

    async def reap_expired(self):
        self.calls.append(("reap_expired",))
        if "reap_expired" in self.fail_modes:
            raise self.fail_modes["reap_expired"]
        return 7  # arbitrary marker


@pytest.fixture
async def layered():
    valkey = _FakeDriver()
    pg = _FakeDriver()
    # Tiny threshold / interval so flush triggers fast in tests.
    drv = LayeredUsageCounter(valkey=valkey, postgres=pg, flush_threshold=2, flush_interval=0.05)
    await drv.start()
    try:
        yield drv, valkey, pg
    finally:
        await drv.stop()


class TestProtocolShape:
    def test_layered_methods_match_protocol(self):
        for method in ("incr", "get", "incr_if_below", "reset", "reap_expired"):
            assert callable(getattr(LayeredUsageCounter, method)), method
        assert hasattr(LayeredUsageCounter, "name")
        assert hasattr(LayeredUsageCounter, "priority")

    def test_priority_beats_standalone_drivers(self):
        # Postgres=50, Valkey=100, Layered=200.
        assert LayeredUsageCounter.priority > 100

    def test_protocol_is_runtime_checkable(self):
        assert getattr(UsageCounterProtocol, "_is_runtime_protocol", False) is True


class TestIncrIfBelowRouting:
    @pytest.mark.asyncio
    async def test_rate_window_hits_valkey_first(self, layered):
        drv, valkey, pg = layered
        new_count, allowed = await drv.incr_if_below(
            "p1", "u1", limit=10, window_seconds=60
        )
        assert allowed is True
        assert new_count == 1
        # Valkey was called first; PG write-through happened via buffer
        # (may be deferred — assert at least valkey was hit synchronously).
        assert ("incr_if_below", "p1", "u1", 10, 60, 1) in valkey.calls

    @pytest.mark.asyncio
    async def test_valkey_failure_falls_through_to_pg(self, layered):
        drv, valkey, pg = layered
        valkey.fail_modes["incr_if_below"] = RuntimeError("valkey down")
        new_count, allowed = await drv.incr_if_below(
            "p1", "u1", limit=10, window_seconds=60
        )
        # PG took over, count came from PG's CAS upsert.
        assert allowed is True
        assert new_count == 1
        assert ("incr_if_below", "p1", "u1", 10, 60, 1) in pg.calls

    @pytest.mark.asyncio
    async def test_deny_does_not_record_pg_delta(self, layered):
        drv, valkey, pg = layered
        # Pre-fill Valkey to the cap so the next call is denied.
        valkey.counts[("p1", "u1", 60)] = 10
        new_count, allowed = await drv.incr_if_below(
            "p1", "u1", limit=10, window_seconds=60
        )
        assert allowed is False
        assert new_count == 10
        # PG should not have received an incr from this denied call.
        # (Lifetime quota write-through tests below verify the positive case.)
        assert not any(c[0] == "incr" for c in pg.calls)


class TestLifetimeQuotaWriteThrough:
    @pytest.mark.asyncio
    async def test_lifetime_quota_writes_pg_synchronously(self, layered):
        drv, valkey, pg = layered
        new_count, allowed = await drv.incr_if_below(
            "p-lifetime", "u1", limit=3, window_seconds=None
        )
        assert allowed is True
        # Synchronous PG write-through — no buffer wait needed.
        assert ("incr", "p-lifetime", "u1", None, 1) in pg.calls

    @pytest.mark.asyncio
    async def test_rate_window_does_not_write_pg_synchronously(self, layered):
        drv, valkey, pg = layered
        await drv.incr_if_below("p1", "u1", limit=10, window_seconds=60)
        # PG incr is deferred to the buffer; it must NOT have happened
        # in the same await as the Valkey call.
        assert not any(
            c[0] == "incr" and c[3] == 60 for c in pg.calls
        ), pg.calls


class TestReset:
    @pytest.mark.asyncio
    async def test_reset_clears_both_tiers(self, layered):
        drv, valkey, pg = layered
        valkey.counts[("p1", "u1", 60)] = 5
        pg.counts[("p1", "u1", 60)] = 5
        await drv.reset("p1", "u1", window_seconds=60)
        assert ("reset", "p1", "u1", 60) in valkey.calls
        assert ("reset", "p1", "u1", 60) in pg.calls
        assert ("p1", "u1", 60) not in valkey.counts
        assert ("p1", "u1", 60) not in pg.counts

    @pytest.mark.asyncio
    async def test_reset_tolerates_valkey_failure(self, layered):
        drv, valkey, pg = layered
        valkey.fail_modes["reset"] = RuntimeError("valkey down")
        # Should not raise — PG is the authoritative store for reset.
        await drv.reset("p1", "u1", window_seconds=60)
        assert ("reset", "p1", "u1", 60) in pg.calls


class TestReapAndGet:
    @pytest.mark.asyncio
    async def test_reap_expired_delegates_to_pg(self, layered):
        drv, valkey, pg = layered
        count = await drv.reap_expired()
        assert count == 7
        assert ("reap_expired",) in pg.calls
        # Valkey has native TTL — no scan needed.
        assert ("reap_expired",) not in valkey.calls

    @pytest.mark.asyncio
    async def test_get_prefers_valkey_when_warm(self, layered):
        drv, valkey, pg = layered
        valkey.counts[("p1", "u1", 60)] = 7
        pg.counts[("p1", "u1", 60)] = 4  # stale
        value = await drv.get("p1", "u1", window_seconds=60)
        assert value == 7  # Valkey wins

    @pytest.mark.asyncio
    async def test_get_falls_back_to_pg_when_valkey_cold(self, layered):
        drv, valkey, pg = layered
        # Valkey returns 0 (no row); PG has the durable count.
        pg.counts[("p1", "u1", 60)] = 4
        value = await drv.get("p1", "u1", window_seconds=60)
        assert value == 4

    @pytest.mark.asyncio
    async def test_get_falls_back_when_valkey_raises(self, layered):
        drv, valkey, pg = layered
        valkey.fail_modes["get"] = RuntimeError("valkey down")
        pg.counts[("p1", "u1", 60)] = 4
        value = await drv.get("p1", "u1", window_seconds=60)
        assert value == 4
