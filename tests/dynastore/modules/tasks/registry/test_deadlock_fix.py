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
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Tests for the registry deadlock fix (issue #2271).

Covers three properties:
(a) upsert_rows sorts rows by (service, task_key) before locking.
(b) retry_on_lock_conflict is applied: a simulated 40P01 is retried, not silently dropped.
(c) heartbeat SQL locks in PK order (FOR UPDATE sub-select) and liveness refreshes on
    cache-hit ticks even when the structural UPSERT is skipped.
(d) In-process digest memo prevents per-tick UPSERT storm when Valkey is unavailable.
(g) RegistryHeartbeatService — unified BackgroundService form declares correct policy.
"""
from __future__ import annotations

import asyncio
import contextlib
from typing import List

import pytest

from dynastore.modules.tasks.registry import publisher as pub
from dynastore.modules.tasks.registry import repository as repo
from dynastore.modules.tasks.registry.model import CapabilityRow
from dynastore.tools.cache import cache_clear


# ---------------------------------------------------------------------------
# Helpers shared across tests
# ---------------------------------------------------------------------------

def _row(service: str, task_key: str) -> CapabilityRow:
    return CapabilityRow(
        service=service,
        task_key=task_key,
        kind="task",
        required_capability=None,
        mandatory=False,
        affinity_tier=None,
        service_version="1.0.0",
        service_commit="abc123",
        version="abc123",
    )


class _SyncOnlyEngine:
    """Mimics a job's sync-driver engine (see test_repository.py for context)."""

    def begin(self):
        @contextlib.contextmanager
        def _cm():
            yield object()
        return _cm()

    connect = begin


# ---------------------------------------------------------------------------
# (a) upsert_rows sorts by (service, task_key) before locking
# ---------------------------------------------------------------------------

def test_upsert_rows_sorts_deterministically(monkeypatch):
    """Rows passed in reverse-PK order must be executed in (service, task_key) ASC order."""
    observed_order: List[tuple] = []

    @contextlib.asynccontextmanager
    async def _fake_mt(_engine):
        yield object()

    async def _fake_execute(self, _conn, **kw):
        observed_order.append((kw.get("service"), kw.get("task_key")))
        return None

    monkeypatch.setattr(repo, "managed_transaction", _fake_mt)
    monkeypatch.setattr(repo.DQLQuery, "execute", _fake_execute)

    rows = [
        _row("svc-A", "zzz"),
        _row("svc-A", "aaa"),
        _row("svc-A", "mmm"),
    ]
    asyncio.run(repo.upsert_rows(_SyncOnlyEngine(), rows))

    assert observed_order == [("svc-A", "aaa"), ("svc-A", "mmm"), ("svc-A", "zzz")], (
        "upsert_rows must execute in (service, task_key) ASC order regardless of input order"
    )


def test_upsert_rows_sort_is_stable_across_services(monkeypatch):
    """Multi-service rows sort by service first, then task_key."""
    observed_order: List[tuple] = []

    @contextlib.asynccontextmanager
    async def _fake_mt(_engine):
        yield object()

    async def _fake_execute(self, _conn, **kw):
        observed_order.append((kw.get("service"), kw.get("task_key")))
        return None

    monkeypatch.setattr(repo, "managed_transaction", _fake_mt)
    monkeypatch.setattr(repo.DQLQuery, "execute", _fake_execute)

    rows = [
        _row("svc-Z", "beta"),
        _row("svc-A", "gamma"),
        _row("svc-Z", "alpha"),
        _row("svc-A", "delta"),
    ]
    asyncio.run(repo.upsert_rows(_SyncOnlyEngine(), rows))

    assert observed_order == [
        ("svc-A", "delta"),
        ("svc-A", "gamma"),
        ("svc-Z", "alpha"),
        ("svc-Z", "beta"),
    ]


# ---------------------------------------------------------------------------
# (b) retry_on_lock_conflict wraps upsert_rows and heartbeat
# ---------------------------------------------------------------------------

def test_upsert_rows_retries_on_deadlock(monkeypatch):
    """A 40P01 deadlock on the first attempt must be retried, not silently dropped."""
    attempt_counter = {"n": 0}
    sentinel_conn = object()

    @contextlib.asynccontextmanager
    async def _fake_mt(_engine):
        attempt_counter["n"] += 1
        if attempt_counter["n"] == 1:
            raise Exception("ERROR:  deadlock detected (SQLSTATE 40P01)")
        yield sentinel_conn

    async def _fake_execute(self, _conn, **kw):
        return None

    monkeypatch.setattr(repo, "managed_transaction", _fake_mt)
    monkeypatch.setattr(repo.DQLQuery, "execute", _fake_execute)

    # Should not raise — retries after the simulated 40P01.
    rows = [_row("svc-A", "ingestion")]
    result = asyncio.run(repo.upsert_rows(_SyncOnlyEngine(), rows))
    assert result == 1
    assert attempt_counter["n"] == 2, "expected one failure then one success"


def test_heartbeat_retries_on_deadlock(monkeypatch):
    """A 40P01 on heartbeat must be retried, not silently dropped."""
    attempt_counter = {"n": 0}
    sentinel_conn = object()

    @contextlib.asynccontextmanager
    async def _fake_mt(_engine):
        attempt_counter["n"] += 1
        if attempt_counter["n"] == 1:
            raise Exception("deadlock detected (SQLSTATE 40P01)")
        yield sentinel_conn

    async def _fake_execute(self, _conn, **kw):
        return None

    monkeypatch.setattr(repo, "managed_transaction", _fake_mt)
    monkeypatch.setattr(repo.DQLQuery, "execute", _fake_execute)

    asyncio.run(repo.heartbeat(_SyncOnlyEngine(), "svc-A"))
    assert attempt_counter["n"] == 2, "heartbeat must retry once on 40P01"


def test_upsert_rows_raises_after_max_retries(monkeypatch):
    """Exhausting all retries on persistent deadlock must raise, not silently succeed."""
    @contextlib.asynccontextmanager
    async def _always_deadlock(_engine):
        # Raise before yielding to simulate a transaction that never opens.
        # The yield is required to satisfy @asynccontextmanager's generator
        # protocol; pyright flags it as unreachable because the raise always
        # fires first, which is intentional here.
        raise Exception("deadlock detected (SQLSTATE 40P01)")
        yield  # pyright: ignore[reportUnreachable]

    monkeypatch.setattr(repo, "managed_transaction", _always_deadlock)

    with pytest.raises(Exception, match="deadlock"):
        asyncio.run(repo.upsert_rows(_SyncOnlyEngine(), [_row("svc-A", "task1")]))


# ---------------------------------------------------------------------------
# (c) heartbeat SQL uses PK-ordered FOR UPDATE sub-select
# ---------------------------------------------------------------------------

def test_heartbeat_sql_uses_pk_ordered_for_update():
    """The heartbeat SQL must lock rows in (service, task_key) ASC order via FOR UPDATE."""
    sql = repo._HEARTBEAT_SQL.lower()
    assert "for update" in sql, "heartbeat must use FOR UPDATE to acquire row locks"
    assert "order by service, task_key" in sql, (
        "heartbeat FOR UPDATE must order by (service, task_key) to match UPSERT lock order"
    )


def test_heartbeat_still_fires_on_cache_hit_tick(monkeypatch):
    """Even when the UPSERT is skipped (cache/memo hit), the heartbeat must still run.

    This validates liveness: a tick where the structural write is suppressed must
    still refresh last_seen so the mandatory-ownership check sees this pod as live.
    """
    calls = {"upsert": 0, "heartbeat": 0}
    rows = [_row("worker", "gdal")]

    async def _count_upsert(engine, r):
        calls["upsert"] += 1
        return len(r)

    async def _count_heartbeat(engine, service):
        calls["heartbeat"] += 1

    monkeypatch.setattr(pub, "collect_local_inventory", lambda: ("worker", "c1", "1.0.0", rows))
    monkeypatch.setattr(pub.repository, "upsert_rows", _count_upsert)
    monkeypatch.setattr(pub.repository, "heartbeat", _count_heartbeat)
    cache_clear(pub._publish_if_new)
    pub._local_published.clear()

    async def _run():
        engine = object()
        # First tick: full publish (upsert + heartbeat).
        await pub.publish_inventory(engine)
        # Second tick: in-process memo hit (upsert skipped) but heartbeat still fires.
        await pub.publish_inventory(engine)

    asyncio.run(_run())

    assert calls["upsert"] == 1, "UPSERT must only run once (digest-gated)"
    assert calls["heartbeat"] == 2, "heartbeat must run on every tick, even cache-hit ticks"


# ---------------------------------------------------------------------------
# (d) In-process digest memo prevents UPSERT storm when Valkey is down
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_local_memo_suppresses_upsert_when_cache_blips(monkeypatch):
    """When _publish_if_new raises (simulating Valkey down), the first call still
    records the digest in _local_published after a *successful* upsert. On
    subsequent ticks the memo prevents re-entering _publish_if_new entirely."""
    calls = {"upsert": 0, "heartbeat": 0}
    rows = [_row("worker", "ingestion")]

    async def _count_upsert(engine, r):
        calls["upsert"] += 1
        return len(r)

    async def _count_heartbeat(engine, service):
        calls["heartbeat"] += 1

    monkeypatch.setattr(pub, "collect_local_inventory", lambda: ("worker", "c1", "1.0.0", rows))
    monkeypatch.setattr(pub.repository, "upsert_rows", _count_upsert)
    monkeypatch.setattr(pub.repository, "heartbeat", _count_heartbeat)
    cache_clear(pub._publish_if_new)
    pub._local_published.clear()

    engine = object()
    # Three ticks: memo should suppress UPSERT after the first.
    await pub.publish_inventory(engine)
    await pub.publish_inventory(engine)
    await pub.publish_inventory(engine)

    assert calls["upsert"] == 1, "in-process memo must suppress repeated UPSERTs"
    assert calls["heartbeat"] == 3, "heartbeat runs every tick regardless"


@pytest.mark.asyncio
async def test_local_memo_does_not_suppress_new_digest(monkeypatch):
    """A new build (different digest) bypasses the in-process memo and triggers a
    fresh UPSERT even when the old digest is already in the memo."""
    calls = {"upsert": 0, "heartbeat": 0}

    async def _count_upsert(engine, r):
        calls["upsert"] += 1
        return len(r)

    async def _count_heartbeat(engine, service):
        calls["heartbeat"] += 1

    monkeypatch.setattr(pub.repository, "upsert_rows", _count_upsert)
    monkeypatch.setattr(pub.repository, "heartbeat", _count_heartbeat)
    cache_clear(pub._publish_if_new)
    pub._local_published.clear()

    engine = object()
    rows_v1 = [_row("worker", "ingestion")]
    rows_v2 = [_row("worker", "ingestion"), _row("worker", "gdal")]

    monkeypatch.setattr(pub, "collect_local_inventory", lambda: ("worker", "c1", "1.0.0", rows_v1))
    await pub.publish_inventory(engine)

    # New build → different digest → memo misses → UPSERT runs again.
    monkeypatch.setattr(pub, "collect_local_inventory", lambda: ("worker", "c2", "1.0.0", rows_v2))
    await pub.publish_inventory(engine)

    assert calls["upsert"] == 2, "a new digest must bypass the in-process memo"


# ---------------------------------------------------------------------------
# (g) RegistryHeartbeatService — unified BackgroundService form
# ---------------------------------------------------------------------------

def test_registry_heartbeat_service_declares_policy(monkeypatch):
    """The service declares LEADER_ONLY + SKIP_EPHEMERAL and a service-scoped key.

    Leadership election and ephemeral gating are no longer hand-wired in the
    function — they are policy fields the BackgroundSupervisor enforces. The
    advisory key must still embed the service name so the lock identity matches
    the legacy run_registry_heartbeat key across a rolling deploy.
    """
    from dynastore.tools.background_service import Leadership, LeaseRenewalMode, PodPolicy

    monkeypatch.setattr(pub, "get_service_name", lambda: "my-service")
    svc = pub.RegistryHeartbeatService(refresh_seconds=15.0)

    assert svc.name == "task_registry_heartbeat"
    assert svc.leadership is Leadership.LEADER_ONLY
    assert svc.pod_policy is PodPolicy.SKIP_EPHEMERAL
    assert svc.cadence_seconds == 15.0
    assert svc.lock_key == "task-registry-heartbeat:my-service", (
        "advisory key must be preserved verbatim for rolling-deploy lock identity"
    )
    # #2900: default cadence (30s) equals the lease TTL, so this service
    # holds tenure across ticks instead of re-electing per tick.
    assert svc.lease_renewal_mode is LeaseRenewalMode.HEARTBEAT


@pytest.mark.asyncio
async def test_registry_heartbeat_service_tick_publishes(monkeypatch):
    """tick() delegates to publish_inventory with the context engine — the
    cadence/leadership/skip plumbing lives in the supervisor, not here."""
    from dynastore.tools.background_service import ServiceContext

    published: list = []

    async def _spy_publish(engine):
        published.append(engine)

    monkeypatch.setattr(pub, "publish_inventory", _spy_publish)

    svc = pub.RegistryHeartbeatService()
    engine = object()
    ctx = ServiceContext(
        engine=engine, shutdown=asyncio.Event(), is_ephemeral=False, name="svc"
    )
    await svc.tick(ctx)

    assert published == [engine]
