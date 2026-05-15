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

"""Live reconfig: ``EngineInstanceCache.update_config`` swaps snapshot entries (#827).

Pre-fix, ``_on_valkey_engine_config_change`` did
``engine_cache.evict(ref) + engine_cache.get(ref)`` — but ``.get`` reads
the engine config from the *resolver snapshot*, which was populated once
at boot.  A subsequent ``PUT /configs/plugins/valkey_engine_config`` then
persisted to DB, fired the handler, but the snapshot still held the
boot-time config — so re-instantiation rebuilt the same client against
the same stale ``cluster_mode`` / ``discovery_host`` / TLS settings.

Symptoms on review env (rev 00286-l4q):
  * ``PUT cluster_mode=false`` → 200 OK in DB.
  * Apply handler fires → ``CACHE RECONNECT: success=false`` with
    "Valkey Cluster cannot be connected" — i.e. cluster client built
    against the stale ``cluster_mode=true``.

This file pins the writer+update path: a fresh config pushed through
``update_config`` MUST be the one observed on the next ``get``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from dynastore.modules.db_config.engine_instance_cache import EngineInstanceCache
from dynastore.modules.db_config.engine_resolver import make_resolver, make_writer


class _FakeEngine:
    """Minimal EngineInstanceProtocol-shaped object for round-trip tests."""

    engine_class: str = "valkey_engine"
    enabled: bool = True
    lifecycle = type("L", (), {"policy": "global", "ttl_seconds": None})()

    def __init__(self, tag: str) -> None:
        self.tag = tag
        self.engine_init = AsyncMock(return_value=f"client-{tag}")
        self.engine_release = AsyncMock(return_value=None)

    @classmethod
    def class_key(cls) -> str:
        return "valkey_engine_config"


@pytest.mark.asyncio
async def test_update_config_swaps_snapshot_and_evicts_instance():
    """After ``update_config(new_cfg)``, ``.get`` returns an instance built
    from the new config, not the boot-time one."""
    boot_cfg = _FakeEngine("boot")
    snapshot = {
        "valkey_engine_config": boot_cfg,
        "valkey_engine": boot_cfg,
    }
    cache = EngineInstanceCache(
        engine_resolver=make_resolver(snapshot),
        engine_writer=make_writer(snapshot),
    )

    # Prime: get warms the cached instance against boot_cfg.
    client1 = await cache.get("valkey_engine")
    assert client1 == "client-boot"
    boot_cfg.engine_init.assert_awaited_once()

    # Swap in fresh config (simulating a PUT-triggered apply).
    fresh_cfg = _FakeEngine("fresh")
    await cache.update_config(fresh_cfg)

    # Snapshot must now point at fresh_cfg under both keys.
    assert snapshot["valkey_engine_config"] is fresh_cfg
    assert snapshot["valkey_engine"] is fresh_cfg

    # Cached instance must have been evicted with engine_release on boot_cfg.
    boot_cfg.engine_release.assert_awaited()

    # Next get must rebuild against fresh_cfg, not boot_cfg.
    client2 = await cache.get("valkey_engine")
    assert client2 == "client-fresh"
    fresh_cfg.engine_init.assert_awaited_once()
    # boot_cfg.engine_init not called again — the old config is gone.
    assert boot_cfg.engine_init.await_count == 1


@pytest.mark.asyncio
async def test_update_config_raises_without_writer():
    """Read-only caches (no writer wired) must surface a clear error
    rather than silently no-op the snapshot swap."""
    snapshot = {"valkey_engine_config": _FakeEngine("boot")}
    cache = EngineInstanceCache(engine_resolver=make_resolver(snapshot))

    with pytest.raises(RuntimeError, match="no engine_writer"):
        await cache.update_config(_FakeEngine("fresh"))


def test_make_writer_indexes_under_both_keys_when_distinct():
    """``class_key()`` and ``engine_class`` may differ — writer must
    update both (mirrors :func:`build_engine_snapshot`)."""
    snapshot: dict = {}
    writer = make_writer(snapshot)
    cfg = _FakeEngine("x")

    writer(cfg)

    assert snapshot["valkey_engine_config"] is cfg
    assert snapshot["valkey_engine"] is cfg


def test_make_writer_skips_engine_class_when_equal_to_class_key():
    """If ``engine_class == class_key()`` (no discriminator divergence),
    writer must not double-index — same dict entry, same identity."""
    snapshot: dict = {}
    writer = make_writer(snapshot)

    class _MergedEngine(_FakeEngine):
        engine_class = "valkey_engine_config"  # same as class_key()

    cfg = _MergedEngine("m")
    writer(cfg)

    # Only the one key — no spurious second entry.
    assert list(snapshot.keys()) == ["valkey_engine_config"]
    assert snapshot["valkey_engine_config"] is cfg
