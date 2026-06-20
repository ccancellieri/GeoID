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

"""Version gate via the shared @cached decorator: the first publish of a build
UPSERTs; the same build digest is a cache hit (heartbeat only); a new build
digest UPSERTs again. Exercises the real cache (local in-memory backend)."""
from __future__ import annotations

import pytest

from dynastore.modules.tasks.registry import publisher as pub
from dynastore.modules.tasks.registry.model import CapabilityRow
from dynastore.tools.cache import cache_clear


def _rows():
    return [
        CapabilityRow(
            service="worker", task_key="gdal", kind="process",
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
    # Both the distributed cache and the in-process memo are process-wide singletons;
    # reset both so a prior test run cannot produce a spurious cache or memo hit.
    cache_clear(pub._publish_if_new)
    pub._local_published.clear()

    # First tick: cache miss on (service, digest) -> UPSERT + heartbeat.
    await pub.publish_inventory(engine=object())
    # Second tick, identical build: in-process memo hit -> NO upsert, heartbeat still runs.
    await pub.publish_inventory(engine=object())

    assert calls["upsert"] == 1
    assert calls["heartbeat"] == 2


@pytest.mark.asyncio
async def test_new_build_digest_reupserts(monkeypatch):
    calls = {"upsert": 0, "heartbeat": 0}
    monkeypatch.setattr(pub.repository, "upsert_rows", _acount(calls, "upsert"))
    monkeypatch.setattr(pub.repository, "heartbeat", _acount(calls, "heartbeat"))
    cache_clear(pub._publish_if_new)
    pub._local_published.clear()

    # Build c1.
    monkeypatch.setattr(pub, "collect_local_inventory", lambda: ("worker", "c1", "1.0.0", _rows()))
    await pub.publish_inventory(engine=object())
    # New build c2 -> different digest -> memo miss -> UPSERT again.
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
    cache_clear(pub._publish_if_new)
    pub._local_published.clear()

    await pub.publish_inventory(engine=object())

    assert calls == {"upsert": 0, "heartbeat": 0}
