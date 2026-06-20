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

"""Unit tests for ``IamRuleCacheRefreshService``.

Mirrors ``tests/dynastore/modules/gcp/unit/test_liveness_reconciler.py``:
pins the service policy fields and verifies tick() delegates to the underlying
refresh helpers without reimplementing the cache logic.
"""

from __future__ import annotations

import asyncio

import pytest

from dynastore.modules.iam.compiled_rule_cache import (
    IamRuleCacheRefreshService,
    _CONFIG_REFRESH_INTERVAL,
    _reset_for_tests,
)
from dynastore.tools.background_service import (
    Leadership,
    PodPolicy,
    ServiceContext,
)


@pytest.fixture(autouse=True)
def _reset():
    """Isolate module-global cache state between tests."""
    _reset_for_tests()
    yield
    _reset_for_tests()


# ---------------------------------------------------------------------------
# Policy fields
# ---------------------------------------------------------------------------


def test_name():
    svc = IamRuleCacheRefreshService()
    assert svc.name == "iam_rule_cache_refresher"


def test_leadership_run_everywhere():
    svc = IamRuleCacheRefreshService()
    assert svc.leadership is Leadership.RUN_EVERYWHERE


def test_pod_policy_all():
    svc = IamRuleCacheRefreshService()
    assert svc.pod_policy is PodPolicy.ALL


def test_cadence_seconds_equals_config_refresh_interval():
    svc = IamRuleCacheRefreshService()
    assert svc.cadence_seconds == _CONFIG_REFRESH_INTERVAL


# ---------------------------------------------------------------------------
# tick() delegates to the underlying refresh helpers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tick_calls_refresh_config_snapshot(monkeypatch):
    """tick() must invoke refresh_config_snapshot so the TTL/maxsize knobs are
    refreshed from IamScaleConfig on every cadence cycle."""
    import dynastore.modules.iam.compiled_rule_cache as _mod
    from unittest.mock import AsyncMock

    refresh_mock = AsyncMock()
    version_mock = AsyncMock(return_value=0)
    monkeypatch.setattr(_mod, "refresh_config_snapshot", refresh_mock)
    monkeypatch.setattr(_mod, "iam_rule_version_async", version_mock)

    svc = IamRuleCacheRefreshService()
    ctx = ServiceContext(
        engine=None,
        shutdown=asyncio.Event(),
        is_ephemeral=False,
        name="test-host",
    )
    await svc.tick(ctx)

    refresh_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_tick_calls_iam_rule_version_async(monkeypatch):
    """tick() must invoke iam_rule_version_async('iam') so the sync snapshot
    is updated for the hot path (iam_rule_version())."""
    import dynastore.modules.iam.compiled_rule_cache as _mod
    from unittest.mock import AsyncMock

    refresh_mock = AsyncMock()
    version_mock = AsyncMock(return_value=0)
    monkeypatch.setattr(_mod, "refresh_config_snapshot", refresh_mock)
    monkeypatch.setattr(_mod, "iam_rule_version_async", version_mock)

    svc = IamRuleCacheRefreshService()
    ctx = ServiceContext(
        engine=None,
        shutdown=asyncio.Event(),
        is_ephemeral=False,
        name="test-host",
    )
    await svc.tick(ctx)

    version_mock.assert_awaited_once_with("iam")


@pytest.mark.asyncio
async def test_tick_survives_version_error(monkeypatch):
    """A failure in iam_rule_version_async must be swallowed (logged at DEBUG)
    so one bad pass does not kill the supervisor loop."""
    import dynastore.modules.iam.compiled_rule_cache as _mod
    from unittest.mock import AsyncMock

    refresh_mock = AsyncMock()
    monkeypatch.setattr(_mod, "refresh_config_snapshot", refresh_mock)
    monkeypatch.setattr(
        _mod, "iam_rule_version_async", AsyncMock(side_effect=RuntimeError("backend down"))
    )

    svc = IamRuleCacheRefreshService()
    ctx = ServiceContext(
        engine=None,
        shutdown=asyncio.Event(),
        is_ephemeral=False,
        name="test-host",
    )
    # Must not raise.
    await svc.tick(ctx)

    # The config snapshot was still attempted before the version error.
    refresh_mock.assert_awaited_once()


# ---------------------------------------------------------------------------
# Inherited PeriodicService run() loop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_periodic_run_loop_ticks_then_stops_on_shutdown(monkeypatch):
    """PeriodicService.run drives tick() at least once and exits promptly
    once the shutdown event is set."""
    import dynastore.modules.iam.compiled_rule_cache as _mod
    from unittest.mock import AsyncMock

    monkeypatch.setattr(_mod, "refresh_config_snapshot", AsyncMock())
    monkeypatch.setattr(_mod, "iam_rule_version_async", AsyncMock(return_value=0))

    svc = IamRuleCacheRefreshService()
    svc.cadence_seconds = 0.01  # speed up the loop for the test

    ran = {"n": 0}
    original_tick = svc.tick

    async def _counting_tick(ctx: ServiceContext) -> None:
        ran["n"] += 1
        await original_tick(ctx)

    monkeypatch.setattr(svc, "tick", _counting_tick)

    ctx = ServiceContext(
        engine=None,
        shutdown=asyncio.Event(),
        is_ephemeral=False,
        name="test-host",
    )
    loop_task = asyncio.create_task(svc.run(ctx))
    await asyncio.sleep(0.05)
    ctx.shutdown.set()
    await asyncio.wait_for(loop_task, timeout=1.0)

    assert ran["n"] >= 1
    assert loop_task.done()
