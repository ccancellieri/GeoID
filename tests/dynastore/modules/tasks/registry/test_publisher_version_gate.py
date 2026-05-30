"""Version gate via the shared @cached decorator: the first publish of a build
UPSERTs; the same build digest is a cache hit (heartbeat only); a new build
digest UPSERTs again. Exercises the real cache (local in-memory backend)."""
from __future__ import annotations

import pytest

from dynastore.modules.tasks.registry import publisher as pub
from dynastore.modules.tasks.registry.model import CapabilityRow


def _rows():
    return [
        CapabilityRow(
            service="worker", task_key="gdal", kind="process", modes=["async"],
            required_capability=None, mandatory=False, affinity_tier=None,
            service_version="1.0.0", service_commit="c1", version="c1",
        )
    ]


def _acount(calls, key):
    async def _f(*a, **k):
        calls[key] += 1
        return 0
    return _f


@pytest.mark.asyncio
async def test_same_build_upserts_once_then_heartbeats(monkeypatch):
    calls = {"upsert": 0, "heartbeat": 0}
    rows = _rows()
    monkeypatch.setattr(pub, "collect_local_inventory", lambda: ("worker", "c1", "1.0.0", rows))
    monkeypatch.setattr(pub.repository, "upsert_rows", _acount(calls, "upsert"))
    monkeypatch.setattr(pub.repository, "heartbeat", _acount(calls, "heartbeat"))
    # The @cached backend is a process-wide singleton; reset it so a prior test
    # cannot make the first publish here a spurious cache hit.
    pub._publish_if_new.cache_clear()

    # First tick: cache miss on (service, digest) -> UPSERT + heartbeat.
    await pub.publish_inventory(engine=object())
    # Second tick, identical build: cache hit -> NO upsert, heartbeat still runs.
    await pub.publish_inventory(engine=object())

    assert calls["upsert"] == 1
    assert calls["heartbeat"] == 2


@pytest.mark.asyncio
async def test_new_build_digest_reupserts(monkeypatch):
    calls = {"upsert": 0, "heartbeat": 0}
    monkeypatch.setattr(pub.repository, "upsert_rows", _acount(calls, "upsert"))
    monkeypatch.setattr(pub.repository, "heartbeat", _acount(calls, "heartbeat"))
    pub._publish_if_new.cache_clear()

    # Build c1.
    monkeypatch.setattr(pub, "collect_local_inventory", lambda: ("worker", "c1", "1.0.0", _rows()))
    await pub.publish_inventory(engine=object())
    # New build c2 -> different digest -> cache miss -> UPSERT again.
    monkeypatch.setattr(pub, "collect_local_inventory", lambda: ("worker", "c2", "1.0.0", _rows()))
    await pub.publish_inventory(engine=object())

    assert calls["upsert"] == 2


@pytest.mark.asyncio
async def test_no_service_identity_is_noop(monkeypatch):
    # No service identity (e.g. instance config absent) -> nothing to publish or
    # heartbeat; publish_inventory must be a clean no-op, not an error.
    calls = {"upsert": 0, "heartbeat": 0}
    monkeypatch.setattr(pub, "collect_local_inventory", lambda: ("", "c1", "1.0.0", []))
    monkeypatch.setattr(pub.repository, "upsert_rows", _acount(calls, "upsert"))
    monkeypatch.setattr(pub.repository, "heartbeat", _acount(calls, "heartbeat"))
    pub._publish_if_new.cache_clear()

    await pub.publish_inventory(engine=object())

    assert calls == {"upsert": 0, "heartbeat": 0}
