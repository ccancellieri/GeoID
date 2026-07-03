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

"""CacheModule's handling of the engine snapshot refresh task — #833, #2908.

DBConfigModule (priority 0) populates ``app_state.engine_cache`` by handing
the same dict to ``asyncio.create_task(refresh_snapshot_until_ready(...))``
and yielding from its lifespan immediately.  CacheModule (priority 9) then
ran ``engine_cache.get("valkey_engine")`` *before* the task had a chance to
fill the dict, raising KeyError and silently degrading to the local
in-memory cache even though the engine snapshot would have resolved a
moment later.

#833 originally "fixed" this by publishing the task handle on
``app_state.engine_snapshot_refresh_task`` and having CacheModule await it
before any engine_cache read. That was itself wrong: the task cannot
complete before DBService (priority 10) installs the connection pool, and
module lifespans enter in strict priority order, so CacheModule (priority
9) awaiting it always burned the task's entire retry budget on every single
boot — and, once #2908 made that budget degrade to an unbounded keep-alive
retry instead of giving up, would deadlock startup outright (DBService can
never start while CacheModule is still awaiting).

CacheModule now only ever inspects the task if it is ALREADY done —
never awaits a pending one — logging the same WARNING if it completed with
an error. The LOCAL -> VALKEY boot-upgrade loop (#2857,
``_boot_upgrade_to_valkey``) is what actually bridges the late-arriving
snapshot. This file pins that contract.
"""

from __future__ import annotations

import asyncio
import types
from typing import Any

import pytest


def _make_app_state(**kwargs: Any) -> types.SimpleNamespace:
    """A SimpleNamespace stand-in for the production app_state."""
    return types.SimpleNamespace(**kwargs)


async def test_cache_module_does_not_await_pending_refresh_task(
    monkeypatch,
):
    """#2908: CacheModule must NOT await a still-pending refresh task.

    Awaiting a pending task here would block CacheModule (priority 9) from
    finishing its lifespan — and DBService (priority 10), the only thing
    that can ever complete that task, cannot start until CacheModule does.
    CacheModule must proceed immediately with whatever the snapshot
    already has, degrade to LOCAL, and arm the #2857 boot-upgrade loop
    instead of waiting.
    """
    import dynastore.modules.cache.cache_module as cm
    from dynastore.modules.cache.cache_module import CacheModule

    cm._current_backend = None

    refresh_started = asyncio.Event()

    async def _never_completes_during_this_test() -> bool:
        refresh_started.set()
        await asyncio.sleep(3600)  # would time the test out if ever awaited
        return True

    refresh_task = asyncio.create_task(_never_completes_during_this_test())

    get_call_count = {"n": 0}

    class _EngineCacheStub:
        async def get(self, ref: str) -> object:
            get_call_count["n"] += 1
            raise KeyError(ref)  # snapshot is still empty at this point

    engine_cache = _EngineCacheStub()

    # CacheModule reads ValkeyCacheBackend/_CACHE_DEPS_OK at import-time
    # inside the lifespan; drive it down the full LOCAL-degrade path (rather
    # than the deps-missing early-return) so the boot-upgrade task gets
    # armed too.
    import dynastore.tools.cache_valkey as cv
    monkeypatch.setattr(cv, "_CACHE_DEPS_OK", True)

    async def _no_cfg(*_a: Any, **_kw: Any) -> Any:
        from dynastore.modules.cache.cache_config import CachePluginConfig
        return CachePluginConfig()

    monkeypatch.setattr(
        "dynastore.modules.cache.cache_module._load_cache_config", _no_cfg,
    )

    app_state = _make_app_state(
        engine_cache=engine_cache,
        engine_snapshot_refresh_task=refresh_task,
        engine_snapshot_refresh=None,
    )

    module = CacheModule(app_state=app_state)

    async def _enter_and_check() -> bool:
        async with module.lifespan(app_state):
            # Lifespan reached its yield without ever awaiting refresh_task.
            assert get_call_count["n"] == 1, (
                "engine_cache.get must have been consulted immediately, "
                "not gated behind the pending refresh task"
            )
            assert not refresh_task.done(), (
                "refresh task must still be pending — CacheModule must not "
                "have awaited it"
            )
            assert cm._current_backend is None, "still LOCAL at this point"
            return True

    # Bounded so a regression back to awaiting the pending task fails fast
    # with a clear timeout instead of hanging the whole test run.
    entered = await asyncio.wait_for(_enter_and_check(), timeout=2.0)
    assert entered

    refresh_task.cancel()
    try:
        await refresh_task
    except asyncio.CancelledError:
        pass


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
    """An ALREADY-done, failed refresh task must not crash CacheModule.lifespan.

    ``refresh_snapshot_until_ready`` is best-effort; CacheModule only ever
    inspects the task if it is already done (#2908 — see module docstring),
    and if that inspection surfaces an exception (e.g. cancelled on
    shutdown, or an unexpected error) it must log + proceed rather than
    propagate.  Drives the failure deterministically by pre-completing the
    task before lifespan starts.
    """
    from dynastore.modules.cache.cache_module import CacheModule

    async def _failing_refresh() -> bool:
        raise RuntimeError("simulated refresh failure")

    refresh_task = asyncio.create_task(_failing_refresh())
    # Drain it so the exception is stored on the task, and it reports
    # ``done() is True``, before lifespan starts.
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
