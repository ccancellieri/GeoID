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

"""Unit tests for ``EngineInstanceCacheSweepService`` as a ``PeriodicService``.

Mirrors tests/dynastore/modules/gcp/unit/test_liveness_reconciler_service.py.
Verifies:

1. Policy fields: name, leadership==RUN_EVERYWHERE, pod_policy==ALL,
   cadence_seconds matches the cache's sweep_interval_seconds.
2. tick() delegates to EngineInstanceCache.sweep() exactly once per call.
3. tick() is fail-soft: an exception from sweep() is logged as WARNING and
   swallowed rather than propagated to the supervisor.

No real DB required — EngineInstanceCache.sweep is monkeypatched throughout.
"""
from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock

import pytest

from dynastore.tools.background_service import Leadership, PodPolicy, ServiceContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ctx(*, is_ephemeral: bool = False) -> ServiceContext:
    return ServiceContext(
        engine=object(),
        shutdown=asyncio.Event(),
        is_ephemeral=is_ephemeral,
        name="test-svc",
    )


def _make_cache(sweep_interval: float = 60.0):
    from dynastore.modules.db_config.engine_instance_cache import EngineInstanceCache

    return EngineInstanceCache(
        engine_resolver=lambda ref: None,
        sweep_interval_seconds=sweep_interval,
    )


def _make_service(sweep_interval: float = 60.0):
    from dynastore.modules.db_config.engine_instance_cache import (
        EngineInstanceCacheSweepService,
    )

    return EngineInstanceCacheSweepService(_make_cache(sweep_interval))


# ---------------------------------------------------------------------------
# Policy fields
# ---------------------------------------------------------------------------


class TestEngineInstanceCacheSweepServicePolicyFields:
    def test_name(self):
        svc = _make_service()
        assert svc.name == "engine_instance_cache_sweep"

    def test_leadership_is_run_everywhere(self):
        svc = _make_service()
        assert svc.leadership is Leadership.RUN_EVERYWHERE

    def test_pod_policy_is_all(self):
        svc = _make_service()
        assert svc.pod_policy is PodPolicy.ALL

    def test_cadence_seconds_matches_sweep_interval(self):
        svc = _make_service(sweep_interval=120.0)
        assert svc.cadence_seconds == 120.0

    def test_cadence_seconds_default(self):
        svc = _make_service()
        assert svc.cadence_seconds == 60.0

    def test_lock_key_is_none(self):
        """RUN_EVERYWHERE services do not need an advisory lock key."""
        svc = _make_service()
        assert svc.lock_key is None


# ---------------------------------------------------------------------------
# tick() delegation
# ---------------------------------------------------------------------------


class TestEngineInstanceCacheSweepServiceTick:
    @pytest.mark.asyncio
    async def test_tick_delegates_to_sweep(self, monkeypatch):
        """tick() must call cache.sweep() exactly once per invocation."""
        from dynastore.modules.db_config.engine_instance_cache import (
            EngineInstanceCacheSweepService,
        )

        cache = _make_cache()
        sweep_mock = AsyncMock(return_value=0)
        monkeypatch.setattr(cache, "sweep", sweep_mock)

        svc = EngineInstanceCacheSweepService(cache)
        await svc.tick(_ctx())

        sweep_mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_tick_is_fail_soft(self, monkeypatch):
        """An exception from cache.sweep() must not propagate out of tick() —
        the supervisor must remain alive for the next cadence tick."""
        from dynastore.modules.db_config.engine_instance_cache import (
            EngineInstanceCacheSweepService,
        )

        cache = _make_cache()
        monkeypatch.setattr(cache, "sweep", AsyncMock(side_effect=RuntimeError("sweep exploded")))

        svc = EngineInstanceCacheSweepService(cache)
        # Must not raise.
        await svc.tick(_ctx())

    @pytest.mark.asyncio
    async def test_tick_fail_soft_logs_warning(self, monkeypatch, caplog):
        """When sweep() raises, tick() must log at WARNING level so the
        failure is operationally visible despite being swallowed."""
        from dynastore.modules.db_config.engine_instance_cache import (
            EngineInstanceCacheSweepService,
        )

        cache = _make_cache()
        monkeypatch.setattr(
            cache, "sweep", AsyncMock(side_effect=RuntimeError("injected failure"))
        )

        svc = EngineInstanceCacheSweepService(cache)
        with caplog.at_level(
            logging.WARNING,
            logger="dynastore.modules.db_config.engine_instance_cache",
        ):
            await svc.tick(_ctx())

        warning_records = [r for r in caplog.records if r.levelname == "WARNING"]
        assert warning_records, "tick() must log at WARNING when sweep() raises"
