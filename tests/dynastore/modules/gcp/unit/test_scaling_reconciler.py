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


def _fake_configs(policy: ScalingPolicyConfig):
    async def get_config(_cls):
        return policy

    return SimpleNamespace(get_config=get_config)


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
        enabled=True, scale_out_saturation=0.80, step=1, cooldown_seconds=0
    )
    reconciler = GcpScalingReconciler(platform=platform, configs=_fake_configs(policy))

    with _patch_cache(mod, _fake_backend(doc)):
        await reconciler._reconcile_once()

    platform.set_min_instances.assert_awaited_once_with(3)


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
