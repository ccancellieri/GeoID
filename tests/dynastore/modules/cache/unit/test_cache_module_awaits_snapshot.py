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

"""CacheModule awaits the engine snapshot refresh task — #833.

DBConfigModule (priority 0) populates ``app_state.engine_cache`` by handing
the same dict to ``asyncio.create_task(refresh_snapshot_until_ready(...))``
and yielding from its lifespan immediately.  CacheModule (priority 9) then
ran ``engine_cache.get("valkey_engine")`` *before* the task had a chance to
fill the dict, raising KeyError and silently degrading to the local
in-memory cache even though the engine snapshot would have resolved a
moment later.

The fix publishes the task handle on ``app_state.engine_snapshot_refresh_task``
and CacheModule awaits it before any engine_cache read.  This file pins
that contract.
"""

from __future__ import annotations

import asyncio
import types
from typing import Any

import pytest


def _make_app_state(**kwargs: Any) -> types.SimpleNamespace:
    """A SimpleNamespace stand-in for the production app_state."""
    return types.SimpleNamespace(**kwargs)


async def test_cache_module_awaits_refresh_task_before_engine_cache_get(
    monkeypatch,
):
    """The refresh task must complete before engine_cache.get is consulted.

    Models the production boot-race: snapshot dict starts empty, refresh
    task is scheduled separately and fills it asynchronously.  CacheModule
    must NOT race into the empty dict.
    """
    from dynastore.modules.cache.cache_module import CacheModule

    # Drive a deterministic timeline by gating the refresh task on an Event
    # we control — the test fails fast if CacheModule reads engine_cache
    # before the event fires.
    refresh_started = asyncio.Event()
    snapshot_ready = asyncio.Event()
    get_call_count = {"n": 0}

    async def _slow_refresh() -> bool:
        refresh_started.set()
        await asyncio.sleep(0.05)  # give CacheModule time to (incorrectly) race
        snapshot_ready.set()
        return True

    refresh_task = asyncio.create_task(_slow_refresh())

    # Engine cache stub: KeyError until snapshot_ready is set, then returns
    # a sentinel client.  This is what catches a wrong-ordered await.
    sentinel_client = object()

    class _EngineCacheStub:
        async def get(self, ref: str) -> object:
            get_call_count["n"] += 1
            if not snapshot_ready.is_set():
                raise KeyError(ref)
            return sentinel_client

    engine_cache = _EngineCacheStub()

    # CacheModule reads ValkeyCacheBackend at import-time inside the lifespan;
    # short-circuit the backend construction so we don't need real Valkey.
    import dynastore.tools.cache_valkey as cv
    monkeypatch.setattr(cv, "_CACHE_DEPS_OK", True)

    backend_built = {"hit": False}

    class _FakeBackend:
        def __init__(self, *a: Any, **kw: Any) -> None:
            backend_built["hit"] = True
            self.client = kw.get("client")
            self._closed = False

        async def close(self) -> None:
            self._closed = True

    monkeypatch.setattr(cv, "ValkeyCacheBackend", _FakeBackend)

    # Short-circuit the cache-config probe so the test stays unit-tier.
    async def _no_cfg(*_a: Any, **_kw: Any) -> Any:
        from dynastore.modules.cache.cache_config import CachePluginConfig
        return CachePluginConfig()

    monkeypatch.setattr(
        "dynastore.modules.cache.cache_module._load_cache_config", _no_cfg,
    )

    app_state = _make_app_state(
        engine_cache=engine_cache,
        engine_snapshot_refresh_task=refresh_task,
    )

    module = CacheModule(app_state=app_state)
    async with module.lifespan(app_state):
        # Engine path must have been taken (snapshot was ready by the time
        # the get fired) — get was called exactly once and returned the
        # sentinel client.
        assert backend_built["hit"] is True, (
            "engine-mode backend must have been built — a refresh-await "
            "failure would have left engine_cache.get KeyError-ing and the "
            "module degrading to the local in-memory cache instead"
        )
        assert refresh_started.is_set()
        assert snapshot_ready.is_set()
        assert get_call_count["n"] == 1


async def test_cache_module_no_refresh_task_attr_degrades_to_local(
    monkeypatch,
):
    """Back-compat: pre-#833 test stubs lack the attribute — must still work.

    Lots of in-tree tests build an app_state without
    ``engine_snapshot_refresh_task`` (the attribute itself is new).  The
    module must read it via ``getattr(..., None)`` so old callers don't
    AttributeError.
    """
    from dynastore.modules.cache.cache_module import CacheModule

    monkeypatch.delenv("VALKEY_URL", raising=False)
    # No engine_cache, no refresh task — should reach the local in-memory
    # cache early-yield without raising AttributeError on missing attr.
    app_state = _make_app_state()  # no engine_cache, no task attr

    module = CacheModule(app_state=app_state)
    entered = False
    async with module.lifespan(app_state):
        entered = True
    assert entered


async def test_cache_module_handles_failed_refresh_task_gracefully(
    monkeypatch, caplog,
):
    """Refresh task raising must not crash CacheModule.lifespan.

    ``refresh_snapshot_until_ready`` is best-effort; if it raises (e.g.
    cancelled on shutdown, or hits an unexpected error), CacheModule must
    log + proceed rather than propagate.  Drives the failure deterministically
    by pre-completing the task before lifespan starts so the await observes
    a stored exception (not a runtime race).
    """
    from dynastore.modules.cache.cache_module import CacheModule

    async def _failing_refresh() -> bool:
        raise RuntimeError("simulated refresh failure")

    refresh_task = asyncio.create_task(_failing_refresh())
    # Drain it so the exception is stored on the task before lifespan starts;
    # CacheModule's ``await refresh_task`` will then re-raise from the
    # already-completed task.
    with pytest.raises(RuntimeError):
        await refresh_task

    # No VALKEY_URL → CacheModule will pick the local-cache early-yield
    # branch, which still requires the await-failure path to not raise.
    monkeypatch.delenv("VALKEY_URL", raising=False)

    app_state = _make_app_state(
        engine_cache=None,
        engine_snapshot_refresh_task=refresh_task,
    )

    module = CacheModule(app_state=app_state)
    with caplog.at_level("WARNING", logger="dynastore.modules.cache.cache_module"):
        entered = False
        async with module.lifespan(app_state):
            entered = True

    assert entered, "lifespan must yield even when refresh task fails"
    assert any(
        "did not complete cleanly" in rec.getMessage()
        for rec in caplog.records
    ), "must log WARNING when the refresh task raised"


async def test_cache_module_skips_await_when_refresh_task_already_done(
    monkeypatch,
):
    """Hot path: refresh already completed → no extra await, no log spam."""
    from dynastore.modules.cache.cache_module import CacheModule

    async def _already_done() -> bool:
        return True

    refresh_task = asyncio.create_task(_already_done())
    await refresh_task  # finish it before CacheModule sees it

    monkeypatch.delenv("VALKEY_URL", raising=False)
    app_state = _make_app_state(
        engine_cache=None,
        engine_snapshot_refresh_task=refresh_task,
    )

    module = CacheModule(app_state=app_state)
    entered = False
    async with module.lifespan(app_state):
        entered = True
    assert entered
