"""Unit tests for the capability publisher (#502).

Contract:
- ``_refresh_once`` calls ``backend.set`` once per local capability with
  the canonical key + provided TTL.
- A failing ``backend.set`` does not abort the batch — remaining
  capabilities still get refreshed.
- ``_refresh_once`` returns 0 silently when no async backend is
  registered (capability oracle will fail-open downstream).
- The loop ``run_capability_publisher`` exits cleanly when
  ``shutdown_event`` is set.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tasks import capability_publisher
from dynastore.modules.tasks.capability_oracle import capability_key


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
async def test_run_publisher_exits_on_shutdown():
    shutdown = asyncio.Event()
    shutdown.set()
    with patch.object(
        capability_publisher, "_collect_local_capabilities", return_value=[],
    ):
        # First refresh runs (no-op, empty capabilities), then we hit the
        # shutdown wait — which returns immediately because the event is set.
        await asyncio.wait_for(
            capability_publisher.run_capability_publisher(
                shutdown, ttl_seconds=60.0, refresh_seconds=30.0,
            ),
            timeout=1.0,
        )


@pytest.mark.asyncio
async def test_run_publisher_uses_local_capability_enumeration():
    shutdown = asyncio.Event()
    shutdown.set()
    backend = MagicMock()
    backend.set = AsyncMock(return_value=True)
    mgr = MagicMock()
    mgr.get_async_backend = MagicMock(return_value=backend)
    with patch.object(
        capability_publisher, "_collect_local_capabilities",
        return_value=["catalog_elasticsearch_driver"],
    ), patch("dynastore.tools.cache.get_cache_manager", return_value=mgr):
        await asyncio.wait_for(
            capability_publisher.run_capability_publisher(
                shutdown, ttl_seconds=60.0, refresh_seconds=30.0,
            ),
            timeout=1.0,
        )
    backend.set.assert_awaited()
    assert backend.set.await_args_list[0].args[0] == capability_key(
        "catalog_elasticsearch_driver",
    )
