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

"""Unit cover for the Valkey live-reconnect apply handler.

`_on_valkey_engine_config_change` is the most novel piece of the Valkey
Platform Engine promotion (commit c0ab9953). It runs when an operator
PATCHes ValkeyEngineConfig at runtime and must:

  1. Close + unregister the old backend.
  2. Evict the old engine instance.
  3. Re-get the engine (lazy re-init from the new config).
  4. Probe + register the new backend.

Each step has a failure mode that the prior version glossed with bare
log lines. These tests pin the contract end-to-end with stubs so a future
refactor can't silently regress to "log + carry on with stale backend".
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.cache import cache_module as cm
from dynastore.modules.db_config.engine_config import ValkeyEngineConfig
from dynastore.models.plugin_config import (
    PluginConfig,
    _APPLY_HANDLERS,
)


# --------------------------------------------------------------------------
# PluginConfig.unregister_apply_handler
# --------------------------------------------------------------------------


class _DummyConfig(PluginConfig):
    """Throw-away PluginConfig subclass for handler-registry tests.

    Marked abstract so it bypasses the ``_address`` requirement — these
    tests exercise the apply-handler registry plumbing, not the config
    persistence path.
    """

    is_abstract_base = True


def _drop_test_handlers() -> None:
    """Strip _DummyConfig handlers between tests so they don't leak."""
    _APPLY_HANDLERS.pop(_DummyConfig, None)


def test_unregister_apply_handler_removes_from_registry() -> None:
    """Symmetric inverse of register_apply_handler.

    Regression cover: pre-fix, callers reached for
    ``cls.get_apply_handlers().remove(handler)`` which mutates the copy
    returned by ``get_apply_handlers`` and silently no-ops on the
    registry — the bound handler kept firing after shutdown.
    """
    _drop_test_handlers()

    async def handler(*_args: Any, **_kw: Any) -> None: ...

    _DummyConfig.register_apply_handler(handler)
    assert handler in _DummyConfig.get_apply_handlers()

    removed = _DummyConfig.unregister_apply_handler(handler)
    assert removed is True
    assert handler not in _DummyConfig.get_apply_handlers()
    # Underlying registry list is empty (not just the copy).
    assert _APPLY_HANDLERS.get(_DummyConfig, []) == []


def test_unregister_apply_handler_returns_false_when_absent() -> None:
    """Unknown handler => False (no exception)."""
    _drop_test_handlers()

    async def handler(*_args: Any, **_kw: Any) -> None: ...

    assert _DummyConfig.unregister_apply_handler(handler) is False


# --------------------------------------------------------------------------
# _on_valkey_engine_config_change — success path
# --------------------------------------------------------------------------


def _make_app_state(engine_cache: Any) -> Any:
    """Build the minimal app_state shape the apply handler reads."""
    ns = MagicMock()
    ns.engine_cache = engine_cache
    return ns


def _stub_engine_cache(client: Any) -> Any:
    """A stub EngineInstanceCache that returns ``client`` from .get."""
    ec = MagicMock()
    ec.get = AsyncMock(return_value=client)
    ec.evict = AsyncMock(return_value=None)
    # ``_on_valkey_engine_config_change`` awaits ``update_config`` before
    # ``evict`` (snapshot push + re-init, #827); without an AsyncMock here
    # the handler hits a TypeError, swallows it via except Exception, and
    # then ``evict.assert_awaited_once`` fails downstream with a confusing
    # "Awaited 0 times" because the handler returned early.
    ec.update_config = AsyncMock(return_value=None)
    return ec


@pytest.mark.asyncio
async def test_apply_handler_success_swaps_backend_and_logs_metric(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Success path: old backend closed + unregistered; new backend probed,
    registered, swapped in; structured CACHE RECONNECT metric line emitted.
    """
    old_backend = MagicMock()
    old_backend.close = AsyncMock(return_value=None)

    new_client = MagicMock()
    engine_cache = _stub_engine_cache(new_client)

    cm._app_state = _make_app_state(engine_cache)
    cm._current_backend = old_backend

    new_backend = MagicMock()
    new_backend.info = AsyncMock(
        return_value={
            "server": {"redis_version": "7.4.0", "redis_mode": "cluster"},
            "memory": {"used_memory_human": "1M"},
        }
    )
    new_backend.close = AsyncMock(return_value=None)

    manager = MagicMock()
    manager.unregister_backend = MagicMock()
    manager.register_backend = MagicMock()

    with (
        patch(
            "dynastore.tools.cache_valkey.ValkeyCacheBackend", return_value=new_backend
        ),
        patch("dynastore.tools.cache.get_cache_manager", return_value=manager),
        patch("dynastore.tools.cache._notify_backend_upgrade") as notify,
        caplog.at_level("INFO"),
    ):
        await cm._on_valkey_engine_config_change(None, None, None, None)

    # 1. Old backend torn down via manager + close.
    manager.unregister_backend.assert_called_once_with(old_backend)
    old_backend.close.assert_awaited_once()

    # 2. Snapshot refreshed (#827) + engine re-fetched. ``update_config``
    # internally calls ``evict`` then writes the new config; mocking
    # ``update_config`` as an AsyncMock means the inner ``evict`` is not
    # observed at this layer — assert on the public step instead.
    engine_cache.update_config.assert_awaited_once_with(None)
    engine_cache.get.assert_awaited_once_with("valkey_engine")

    # 3. New backend probed + registered + notified.
    new_backend.info.assert_awaited_once()
    manager.register_backend.assert_called_once_with(new_backend)
    notify.assert_called_once()

    # 4. Module-level state advanced.
    assert cm._current_backend is new_backend

    # 5. Structured reconnect metric line emitted.
    metric_lines = [r for r in caplog.records if "CACHE RECONNECT" in r.getMessage()]
    assert len(metric_lines) == 1
    msg = metric_lines[0].getMessage()
    assert "success=true" in msg
    assert "version=7.4.0" in msg
    assert "mode=cluster" in msg
    assert "duration_ms=" in msg


# --------------------------------------------------------------------------
# _on_valkey_engine_config_change — failure paths
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_apply_handler_engine_init_failure_degrades_to_l1(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """If engine_cache.get fails, no new backend is registered; metric line
    records stage=engine_init and the error type."""
    old_backend = MagicMock()
    old_backend.close = AsyncMock(return_value=None)

    engine_cache = MagicMock()
    engine_cache.evict = AsyncMock(return_value=None)
    engine_cache.get = AsyncMock(side_effect=RuntimeError("disabled"))

    cm._app_state = _make_app_state(engine_cache)
    cm._current_backend = old_backend

    manager = MagicMock()

    with (
        patch("dynastore.tools.cache.get_cache_manager", return_value=manager),
        caplog.at_level("INFO"),
    ):
        await cm._on_valkey_engine_config_change(None, None, None, None)

    # Old backend was still torn down.
    manager.unregister_backend.assert_called_once_with(old_backend)
    old_backend.close.assert_awaited_once()

    # No new backend registered.
    manager.register_backend.assert_not_called()
    assert cm._current_backend is None

    # Metric records the failed stage.
    metric_lines = [r for r in caplog.records if "CACHE RECONNECT" in r.getMessage()]
    assert len(metric_lines) == 1
    msg = metric_lines[0].getMessage()
    assert "success=false" in msg
    assert "stage=engine_init" in msg
    assert "RuntimeError" in msg


@pytest.mark.asyncio
async def test_apply_handler_probe_failure_degrades_to_l1(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """If the new backend's probe fails (timeout / network), it is NOT
    registered; module state remains backend-less; metric records
    stage=probe."""
    old_backend = MagicMock()
    old_backend.close = AsyncMock(return_value=None)

    new_client = MagicMock()
    engine_cache = _stub_engine_cache(new_client)

    cm._app_state = _make_app_state(engine_cache)
    cm._current_backend = old_backend

    new_backend = MagicMock()
    new_backend.info = AsyncMock(side_effect=asyncio.TimeoutError())
    new_backend.close = AsyncMock(return_value=None)

    manager = MagicMock()

    with (
        patch(
            "dynastore.tools.cache_valkey.ValkeyCacheBackend", return_value=new_backend
        ),
        patch("dynastore.tools.cache.get_cache_manager", return_value=manager),
        patch("dynastore.tools.cache._notify_backend_upgrade") as notify,
        caplog.at_level("INFO"),
    ):
        await cm._on_valkey_engine_config_change(None, None, None, None)

    # Old backend torn down, new one NOT registered.
    manager.unregister_backend.assert_called_once_with(old_backend)
    manager.register_backend.assert_not_called()
    notify.assert_not_called()

    # No advancement of module state.
    assert cm._current_backend is None

    # Metric line for probe failure.
    metric_lines = [r for r in caplog.records if "CACHE RECONNECT" in r.getMessage()]
    assert len(metric_lines) == 1
    msg = metric_lines[0].getMessage()
    assert "success=false" in msg
    assert "stage=probe" in msg
    assert "TimeoutError" in msg


@pytest.mark.asyncio
async def test_apply_handler_no_engine_cache_is_a_warning_no_op() -> None:
    """If app_state has no engine_cache, the handler logs and returns
    without touching the backend — config change just takes effect on
    next restart."""
    cm._app_state = _make_app_state(engine_cache=None)
    old_backend = MagicMock()
    cm._current_backend = old_backend

    await cm._on_valkey_engine_config_change(None, None, None, None)

    # No teardown attempted.
    assert cm._current_backend is old_backend


# --------------------------------------------------------------------------
# Lifespan finally-block correctly removes the handler from the registry
# --------------------------------------------------------------------------


def test_lifespan_finally_uses_unregister_apply_handler() -> None:
    """End-to-end: registering then unregistering via the new API must leave
    the registry empty — not the silently-broken
    ``get_apply_handlers().remove(...)`` pattern.
    """
    _APPLY_HANDLERS.pop(ValkeyEngineConfig, None)

    handler = cm._on_valkey_engine_config_change
    ValkeyEngineConfig.register_apply_handler(handler)
    assert handler in _APPLY_HANDLERS.get(ValkeyEngineConfig, [])

    assert ValkeyEngineConfig.unregister_apply_handler(handler) is True
    assert handler not in _APPLY_HANDLERS.get(ValkeyEngineConfig, [])
