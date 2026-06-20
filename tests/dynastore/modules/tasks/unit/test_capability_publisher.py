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

"""Unit tests for the capability publisher (#502).

Contract:
- ``_refresh_once`` calls ``backend.set`` once per local capability with
  the canonical key + provided TTL.
- A failing ``backend.set`` does not abort the batch — remaining
  capabilities still get refreshed.
- ``_refresh_once`` returns 0 silently when no async backend is
  registered (capability oracle will fail-open downstream).
- ``CapabilityPublisherService.tick()`` collects capabilities and
  calls _refresh_once, swallowing exceptions.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tasks import capability_publisher
from dynastore.modules.tasks.capability_oracle import capability_key
from dynastore.modules.tasks.capability_publisher import CapabilityPublisherService
from dynastore.tools.background_service import ServiceContext


def _ctx() -> ServiceContext:
    return ServiceContext(
        engine=object(),
        shutdown=asyncio.Event(),
        is_ephemeral=False,
        name="test",
    )


@pytest.mark.asyncio
async def test_refresh_once_writes_one_key_per_capability():
    backend = MagicMock()
    backend.set = AsyncMock(return_value=True)
    mgr = MagicMock()
    mgr.get_async_backend = MagicMock(return_value=backend)
    with patch("dynastore.tools.cache.get_cache_manager", return_value=mgr):
        written = await capability_publisher._refresh_once(
            ["a", "b", "c"], ttl_seconds=60.0,
        )
    assert written == 3
    assert backend.set.await_count == 3
    keys = sorted(call.args[0] for call in backend.set.await_args_list)
    assert keys == [capability_key("a"), capability_key("b"), capability_key("c")]
    # ttl propagated
    for call in backend.set.await_args_list:
        assert call.kwargs.get("ttl") == 60.0


@pytest.mark.asyncio
async def test_refresh_once_isolates_per_capability_failures():
    backend = MagicMock()

    async def flaky_set(key, value, *, ttl):
        if "boom" in key:
            raise RuntimeError("write timeout")
        return True

    backend.set = AsyncMock(side_effect=flaky_set)
    mgr = MagicMock()
    mgr.get_async_backend = MagicMock(return_value=backend)
    with patch("dynastore.tools.cache.get_cache_manager", return_value=mgr):
        written = await capability_publisher._refresh_once(
            ["ok-1", "boom", "ok-2"], ttl_seconds=60.0,
        )
    assert written == 2


@pytest.mark.asyncio
async def test_refresh_once_returns_zero_when_no_backend():
    mgr = MagicMock()
    mgr.get_async_backend = MagicMock(
        side_effect=RuntimeError("No async cache backends registered"),
    )
    with patch("dynastore.tools.cache.get_cache_manager", return_value=mgr):
        assert await capability_publisher._refresh_once(["a"], ttl_seconds=60.0) == 0


@pytest.mark.asyncio
async def test_tick_uses_local_capability_enumeration():
    """tick() collects local capabilities and writes sentinel keys."""
    backend = MagicMock()
    backend.set = AsyncMock(return_value=True)
    mgr = MagicMock()
    mgr.get_async_backend = MagicMock(return_value=backend)
    with patch.object(
        capability_publisher, "_collect_local_capabilities",
        return_value=["catalog_elasticsearch_driver"],
    ), patch("dynastore.tools.cache.get_cache_manager", return_value=mgr):
        svc = CapabilityPublisherService(ttl_seconds=60.0, refresh_seconds=30.0)
        await svc.tick(_ctx())

    backend.set.assert_awaited()
    assert backend.set.await_args_list[0].args[0] == capability_key(
        "catalog_elasticsearch_driver",
    )


@pytest.mark.asyncio
async def test_tick_swallows_exceptions():
    """A failing _refresh_once must not propagate — tick() must be fail-soft."""
    async def _fail_refresh(caps, *, ttl_seconds):
        raise RuntimeError("cache down")

    with patch.object(
        capability_publisher, "_collect_local_capabilities",
        return_value=["cap_a"],
    ), patch.object(
        capability_publisher, "_refresh_once",
        side_effect=_fail_refresh,
    ):
        svc = CapabilityPublisherService()
        # Must not raise
        await svc.tick(_ctx())
