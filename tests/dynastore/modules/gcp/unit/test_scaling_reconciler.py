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

"""Unit tests for ``GcpScalingReconciler._reconcile_once`` — the wiring that
turns aggregated signals into a single ``set_min_instances`` call, and the
safety guards around it (disabled policy, unknown current floor)."""

from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from dynastore.modules.gcp.scaling_reconciler import GcpScalingReconciler
from dynastore.modules.scaling.aggregator import SIGNALS_CACHE_KEY
from dynastore.modules.scaling.config import ScalingPolicyConfig


@pytest.fixture(autouse=True)
def disable_managed_eventing():
    """Neutralize the DB-bound autouse fixture from gcp/conftest.py — these
    tests are pure in-memory reconciler wiring with fakes."""
    return None


def _fake_configs(policy: ScalingPolicyConfig, duckdb_cfg=None):
    """Fake ``ConfigsProtocol`` that discriminates by class — unlike a
    single-class stub, the pool-autosize actuator needs both
    ``ScalingPolicyConfig`` (the policy) and ``DuckdbEngineConfig`` (the
    actuated resource) resolvable from the same fake.

    ``get_config_versioned`` (#2707) pairs the stored value with a fake
    monotonic version token — bumped on every ``set_config`` so a caller
    that re-reads after a write observes a fresh token, mirroring the
    real ``updated_at``-derived token.
    """
    from dynastore.modules.db_config.engine_config import DuckdbEngineConfig

    store = {ScalingPolicyConfig: policy, DuckdbEngineConfig: duckdb_cfg or DuckdbEngineConfig()}
    versions = {DuckdbEngineConfig: "v0"}

    def _bump_version(cls, cfg, **_kw):
        store[cls] = cfg
        versions[cls] = f"v{int(versions.get(cls, 'v0')[1:]) + 1}"
        return cfg

    set_config = AsyncMock(side_effect=_bump_version)

    async def get_config(cls):
        return store[cls]

    async def get_config_versioned(cls):
        return store[cls], versions.get(cls)

    return SimpleNamespace(
        get_config=get_config,
        get_config_versioned=get_config_versioned,
        set_config=set_config,
        _store=store,
        _versions=versions,
    )


def _fake_backend(doc):
    backend = SimpleNamespace()
    backend.get = AsyncMock(return_value=doc)
    backend.set = AsyncMock()
    return backend


def _patch_cache(reconciler_module, backend):
    manager = SimpleNamespace(get_async_backend=lambda: backend)
    return patch.object(reconciler_module, "get_cache_manager", lambda: manager)


@pytest.mark.asyncio
async def test_holds_when_current_min_unknown():
    """A transient ``get_min_instances() -> None`` must NOT be treated as
    ``min_replicas``; the reconciler holds the tick and never actuates."""
    from dynastore.modules.gcp import scaling_reconciler as mod

    platform = SimpleNamespace(
        get_min_instances=AsyncMock(return_value=None),
        set_min_instances=AsyncMock(),
    )
    policy = ScalingPolicyConfig(enabled=True)
    reconciler = GcpScalingReconciler(platform=platform, configs=_fake_configs(policy))

    with _patch_cache(mod, _fake_backend({"instances": {}, "global": {}})):
        await reconciler._reconcile_once()

    platform.set_min_instances.assert_not_awaited()


@pytest.mark.asyncio
async def test_disabled_policy_skips_entirely():
    from dynastore.modules.gcp import scaling_reconciler as mod

    platform = SimpleNamespace(
        get_min_instances=AsyncMock(return_value=2),
        set_min_instances=AsyncMock(),
    )
    policy = ScalingPolicyConfig(enabled=False)
    reconciler = GcpScalingReconciler(platform=platform, configs=_fake_configs(policy))

    with _patch_cache(mod, _fake_backend({"instances": {}, "global": {}})):
        await reconciler._reconcile_once()

    platform.get_min_instances.assert_not_awaited()
    platform.set_min_instances.assert_not_awaited()


@pytest.mark.asyncio
async def test_actuates_on_hot_fleet_signal():
    """End-to-end wiring: a fresh hot instance signal drives one scale-out
    step from the known current floor."""
    from dynastore.modules.gcp import scaling_reconciler as mod

    now = time.time()
    doc = {
        "instances": {
            "host-a:1": {
                "ts": now,
                "signals": [
                    {
                        "source": "duckdb_pool",
                        "metric": "pool_saturation",
                        "value": 0.95,
                        "scope": "instance",
                        "ts": now,
                    }
                ],
            }
        },
        "global": {},
    }
    platform = SimpleNamespace(
        get_min_instances=AsyncMock(return_value=2),
        set_min_instances=AsyncMock(),
    )
    policy = ScalingPolicyConfig(
        enabled=True, scale_out_saturation=0.80, step=1, scale_out_cooldown_seconds=0
    )
    reconciler = GcpScalingReconciler(platform=platform, configs=_fake_configs(policy))

    with _patch_cache(mod, _fake_backend(doc)):
        await reconciler._reconcile_once()

    platform.set_min_instances.assert_awaited_once_with(3)


@pytest.mark.asyncio
async def test_memory_recommendation_logged_not_actuated(caplog):
    """Memory is a SLOW revision-roll actuator — a high reading must be
    logged as a recommendation, never fed into ``set_min_instances``."""
    import logging

    from dynastore.modules.gcp import scaling_reconciler as mod

    now = time.time()
    doc = {
        "instances": {
            "host-a:1": {
                "ts": now,
                "signals": [
                    {
                        "source": "duckdb_pool", "metric": "pool_saturation",
                        "value": 0.1, "scope": "instance", "ts": now,
                    }
                ],
            }
        },
        "global": {
            "monitoring_signal_provider:memory_utilization": {
                "ts": now,
                "signal": {
                    "source": "monitoring_signal_provider", "metric": "memory_utilization",
                    "value": 0.92, "scope": "global", "ts": now,
                },
            }
        },
    }
    platform = SimpleNamespace(
        get_min_instances=AsyncMock(return_value=2),
        set_min_instances=AsyncMock(),
    )
    policy = ScalingPolicyConfig(
        enabled=True, min_replicas=2, memory_recommendation_ceiling=0.85,
    )
    reconciler = GcpScalingReconciler(platform=platform, configs=_fake_configs(policy))

    with _patch_cache(mod, _fake_backend(doc)), caplog.at_level(logging.WARNING, logger=mod.logger.name):
        await reconciler._reconcile_once()

    platform.set_min_instances.assert_not_awaited()
    assert any("memory_utilization" in r.getMessage() for r in caplog.records)


def _doc_with_duckdb_pool_and_cpu(pool_value: float, cpu_value: float) -> dict:
    now = time.time()
    return {
        "instances": {
            "host-a:1": {
                "ts": now,
                "signals": [
                    {
                        "source": "duckdb_pool", "metric": "pool_saturation",
                        "value": pool_value, "scope": "instance", "ts": now,
                    }
                ],
            }
        },
        "global": {
            "monitoring_signal_provider:cpu_utilization": {
                "ts": now,
                "signal": {
                    "source": "monitoring_signal_provider", "metric": "cpu_utilization",
                    "value": cpu_value, "scope": "global", "ts": now,
                },
            }
        },
    }


@pytest.mark.asyncio
async def test_duckdb_pool_autosize_off_by_default_never_writes_config():
    """``duckdb_pool_autosize`` defaults False — a saturated+idle fleet must
    not trigger a ``DuckdbEngineConfig`` write even though the aggregator
    hold-branch condition is met."""
    from dynastore.modules.gcp import scaling_reconciler as mod

    platform = SimpleNamespace(
        get_min_instances=AsyncMock(return_value=2),
        set_min_instances=AsyncMock(),
    )
    policy = ScalingPolicyConfig(enabled=True, scale_out_saturation=0.80, cpu_idle_ceiling=0.30)
    configs = _fake_configs(policy)
    reconciler = GcpScalingReconciler(platform=platform, configs=configs)

    with _patch_cache(mod, _fake_backend(_doc_with_duckdb_pool_and_cpu(0.95, 0.1))):
        await reconciler._reconcile_once()

    configs.set_config.assert_not_awaited()


@pytest.mark.asyncio
async def test_duckdb_pool_autosize_bumps_pool_size_when_saturated_and_cpu_idle():
    from dynastore.modules.db_config.engine_config import DuckdbEngineConfig
    from dynastore.modules.gcp import scaling_reconciler as mod

    platform = SimpleNamespace(
        get_min_instances=AsyncMock(return_value=2),
        set_min_instances=AsyncMock(),
    )
    policy = ScalingPolicyConfig(
        enabled=True, scale_out_saturation=0.80, cpu_idle_ceiling=0.30,
        duckdb_pool_autosize=True, duckdb_pool_step=2, duckdb_pool_size_max=32,
        duckdb_pool_cooldown_seconds=0,
    )
    configs = _fake_configs(policy, duckdb_cfg=DuckdbEngineConfig(pool_size=8))
    reconciler = GcpScalingReconciler(platform=platform, configs=configs)

    with _patch_cache(mod, _fake_backend(_doc_with_duckdb_pool_and_cpu(0.95, 0.1))):
        await reconciler._reconcile_once()

    configs.set_config.assert_awaited_once()
    written_cls, written_cfg = configs.set_config.await_args.args
    assert written_cls is DuckdbEngineConfig
    assert written_cfg.pool_size == 10


@pytest.mark.asyncio
async def test_duckdb_pool_autosize_respects_cooldown():
    """A second saturated tick inside ``duckdb_pool_cooldown_seconds`` of the
    reconciler's own last pool actuation must not write again."""
    from dynastore.modules.db_config.engine_config import DuckdbEngineConfig
    from dynastore.modules.gcp import scaling_reconciler as mod

    platform = SimpleNamespace(
        get_min_instances=AsyncMock(return_value=2),
        set_min_instances=AsyncMock(),
    )
    policy = ScalingPolicyConfig(
        enabled=True, scale_out_saturation=0.80, cpu_idle_ceiling=0.30,
        duckdb_pool_autosize=True, duckdb_pool_step=2, duckdb_pool_size_max=32,
        duckdb_pool_cooldown_seconds=120,
    )
    configs = _fake_configs(policy, duckdb_cfg=DuckdbEngineConfig(pool_size=8))
    reconciler = GcpScalingReconciler(platform=platform, configs=configs)

    with _patch_cache(mod, _fake_backend(_doc_with_duckdb_pool_and_cpu(0.95, 0.1))):
        await reconciler._reconcile_once()
    assert configs.set_config.await_count == 1

    with _patch_cache(mod, _fake_backend(_doc_with_duckdb_pool_and_cpu(0.95, 0.1))):
        await reconciler._reconcile_once()
    assert configs.set_config.await_count == 1  # still one — inside cooldown


@pytest.mark.asyncio
async def test_duckdb_pool_autosize_uses_versioned_read_and_passes_token_to_write():
    """#2707: the write site reads via ``get_config_versioned`` and threads
    the token through ``set_config(..., expected_version=...)`` — closing
    the read-modify-write race the plain re-read mitigation only narrowed."""
    from dynastore.modules.db_config.engine_config import DuckdbEngineConfig
    from dynastore.modules.gcp import scaling_reconciler as mod

    platform = SimpleNamespace(
        get_min_instances=AsyncMock(return_value=2),
        set_min_instances=AsyncMock(),
    )
    policy = ScalingPolicyConfig(
        enabled=True, scale_out_saturation=0.80, cpu_idle_ceiling=0.30,
        duckdb_pool_autosize=True, duckdb_pool_step=2, duckdb_pool_size_max=32,
        duckdb_pool_cooldown_seconds=0,
    )
    configs = _fake_configs(policy, duckdb_cfg=DuckdbEngineConfig(pool_size=8))
    reconciler = GcpScalingReconciler(platform=platform, configs=configs)

    with _patch_cache(mod, _fake_backend(_doc_with_duckdb_pool_and_cpu(0.95, 0.1))):
        await reconciler._reconcile_once()

    configs.set_config.assert_awaited_once()
    _, kwargs = configs.set_config.await_args
    assert kwargs.get("expected_version") == "v0"


@pytest.mark.asyncio
async def test_duckdb_pool_autosize_skips_tick_on_cas_conflict_without_raising():
    """A concurrent writer wins the CAS race — the actuator must swallow
    ``ConfigVersionConflictError`` (not treat it as a hard failure) and
    must not advance ``_last_pool_change_ts``, so the next tick isn't
    blocked by the cooldown from a bump that never actually landed."""
    from dynastore.modules.db_config.engine_config import DuckdbEngineConfig
    from dynastore.modules.db_config.exceptions import ConfigVersionConflictError
    from dynastore.modules.gcp import scaling_reconciler as mod

    platform = SimpleNamespace(
        get_min_instances=AsyncMock(return_value=2),
        set_min_instances=AsyncMock(),
    )
    policy = ScalingPolicyConfig(
        enabled=True, scale_out_saturation=0.80, cpu_idle_ceiling=0.30,
        duckdb_pool_autosize=True, duckdb_pool_step=2, duckdb_pool_size_max=32,
        duckdb_pool_cooldown_seconds=0,
    )
    configs = _fake_configs(policy, duckdb_cfg=DuckdbEngineConfig(pool_size=8))
    configs.set_config = AsyncMock(side_effect=ConfigVersionConflictError("lost the race"))
    reconciler = GcpScalingReconciler(platform=platform, configs=configs)

    with _patch_cache(mod, _fake_backend(_doc_with_duckdb_pool_and_cpu(0.95, 0.1))):
        await reconciler._reconcile_once()  # must not raise

    configs.set_config.assert_awaited_once()
    assert reconciler._last_pool_change_ts == 0.0


@pytest.mark.asyncio
async def test_duckdb_pool_autosize_retries_and_succeeds_on_next_tick_after_conflict():
    """First tick loses the CAS race (skipped, state unchanged); second
    tick re-reads fresh state and its write lands."""
    from dynastore.modules.db_config.engine_config import DuckdbEngineConfig
    from dynastore.modules.db_config.exceptions import ConfigVersionConflictError
    from dynastore.modules.gcp import scaling_reconciler as mod

    platform = SimpleNamespace(
        get_min_instances=AsyncMock(return_value=2),
        set_min_instances=AsyncMock(),
    )
    policy = ScalingPolicyConfig(
        enabled=True, scale_out_saturation=0.80, cpu_idle_ceiling=0.30,
        duckdb_pool_autosize=True, duckdb_pool_step=2, duckdb_pool_size_max=32,
        duckdb_pool_cooldown_seconds=0,
    )
    configs = _fake_configs(policy, duckdb_cfg=DuckdbEngineConfig(pool_size=8))

    call_count = {"n": 0}
    real_set_config = configs.set_config

    async def _flaky_set_config(cls, cfg, **kw):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise ConfigVersionConflictError("lost the race")
        return await real_set_config(cls, cfg, **kw)

    configs.set_config = AsyncMock(side_effect=_flaky_set_config)
    reconciler = GcpScalingReconciler(platform=platform, configs=configs)

    with _patch_cache(mod, _fake_backend(_doc_with_duckdb_pool_and_cpu(0.95, 0.1))):
        await reconciler._reconcile_once()  # tick 1: conflict, skipped

    assert configs.set_config.await_count == 1
    assert configs._store[DuckdbEngineConfig].pool_size == 8  # unchanged

    with _patch_cache(mod, _fake_backend(_doc_with_duckdb_pool_and_cpu(0.95, 0.1))):
        await reconciler._reconcile_once()  # tick 2: retries, succeeds

    assert configs.set_config.await_count == 2
    assert configs._store[DuckdbEngineConfig].pool_size == 10


@pytest.mark.asyncio
async def test_reads_signals_from_the_shared_cache_key():
    """Guards against a key-name drift between publisher and reconciler."""
    from dynastore.modules.gcp import scaling_reconciler as mod

    backend = _fake_backend({"instances": {}, "global": {}})
    platform = SimpleNamespace(
        get_min_instances=AsyncMock(return_value=1),
        set_min_instances=AsyncMock(),
    )
    policy = ScalingPolicyConfig(enabled=True)
    reconciler = GcpScalingReconciler(platform=platform, configs=_fake_configs(policy))

    with _patch_cache(mod, backend):
        await reconciler._reconcile_once()

    backend.get.assert_awaited_once_with(SIGNALS_CACHE_KEY)
