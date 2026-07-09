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

"""Regression test: ``CachePluginConfig`` apply handler re-applies threshold.

Background: ``ValkeyCacheBackend.__init__`` captures
``circuit_breaker_threshold`` (``cache_valkey.py:555``) and never re-reads
it. Before this fix, a ``PUT /configs?plugin_id=cache_plugin_config`` that
bumped the threshold persisted the new value in the config snapshot but
the live backend kept the OLD value — exactly the "Read-once, never
re-applied" failure mode #756 describes. The only path that picked up the
new value was a Valkey engine config change (which rebuilds the backend).

Fix: register ``_on_cache_plugin_config_change`` as the apply handler for
``CachePluginConfig``; it pushes runtime settings onto the live
``_current_backend`` in-place. ``probe_timeout_seconds`` is only consumed on
(re)connect, and ``oracle_inner_timeout_seconds`` is hot-read per dispatch
by the tasks dispatcher.

This test pins both halves: source-level guard that the handler exists
and is wired in lifespan, and empirical pin that calling the handler
updates ``_circuit_breaker_threshold`` and ``required`` on a fake live backend.
"""
from __future__ import annotations

import asyncio
import inspect


def test_handler_exists_with_correct_signature():
    """``_on_cache_plugin_config_change`` must exist and accept the
    standard apply-handler signature ``(config, catalog_id, collection_id,
    conn)`` so the platform config service can call it."""
    from dynastore.modules.cache import cache_module

    assert hasattr(cache_module, "_on_cache_plugin_config_change"), (
        "expected CachePluginConfig apply handler to be defined at "
        "module scope (mirrors _on_valkey_engine_config_change)"
    )
    handler = cache_module._on_cache_plugin_config_change
    sig = inspect.signature(handler)
    assert len(sig.parameters) == 4, (
        f"apply handler should take 4 args (config, catalog_id, "
        f"collection_id, conn); got signature {sig!r}"
    )


def test_handler_pushes_threshold_onto_live_backend():
    """Empirical pin: invoking the handler with a fresh config and a
    live backend exposing ``_circuit_breaker_threshold`` updates the
    attribute in-place."""
    from dynastore.modules.cache import cache_module

    class _FakeBackend:
        def __init__(self):
            self._circuit_breaker_threshold = 3  # old default

    fake = _FakeBackend()
    cache_module._current_backend = fake
    try:
        class _FakeCfg:
            circuit_breaker_threshold = 7

        asyncio.run(
            cache_module._on_cache_plugin_config_change(
                _FakeCfg(), None, None, None
            )
        )
        assert fake._circuit_breaker_threshold == 7, (
            "apply handler must push the new circuit_breaker_threshold "
            "onto the live backend in-place — got "
            f"{fake._circuit_breaker_threshold!r}"
        )
    finally:
        cache_module._current_backend = None


def test_handler_pushes_required_flag_onto_live_backend():
    """``shared_backend_required`` must take effect immediately on a live
    backend so circuit-breaker behavior cannot drift until the next reconnect."""
    from dynastore.modules.cache import cache_module

    class _FakeBackend:
        def __init__(self):
            self._circuit_breaker_threshold = 3
            self.required = False

    fake = _FakeBackend()
    cache_module._current_backend = fake
    try:
        class _FakeCfg:
            circuit_breaker_threshold = 3
            shared_backend_required = True

        asyncio.run(
            cache_module._on_cache_plugin_config_change(
                _FakeCfg(), None, None, None
            )
        )
        assert fake.required is True
    finally:
        cache_module._current_backend = None


def test_handler_is_noop_when_no_live_backend():
    """The handler must tolerate ``_current_backend is None`` (cache
    degraded to L1-only) without raising — the new value will take
    effect on the next backend build."""
    from dynastore.modules.cache import cache_module

    cache_module._current_backend = None

    class _FakeCfg:
        circuit_breaker_threshold = 9

    # Should not raise.
    asyncio.run(
        cache_module._on_cache_plugin_config_change(
            _FakeCfg(), None, None, None
        )
    )


def test_handler_pushes_l1_settings_even_without_live_backend():
    """``l1_default_ttl_seconds`` / ``l1_memory_percent`` govern the
    in-process L1 layer, which exists even when the cache is degraded to
    local-only — so the handler must push them into ``tools.cache``
    BEFORE the no-backend early return."""
    from dynastore.modules.cache import cache_module
    from dynastore.tools import cache as tools_cache

    cache_module._current_backend = None
    old_ttl = tools_cache._l1_default_ttl_value
    old_pct = tools_cache._l1_memory_percent_value

    class _FakeCfg:
        circuit_breaker_threshold = 3
        l1_default_ttl_seconds = 12.5
        l1_memory_percent = 33.0

    try:
        asyncio.run(
            cache_module._on_cache_plugin_config_change(
                _FakeCfg(), None, None, None
            )
        )
        assert tools_cache._l1_default_ttl_value == 12.5
        assert tools_cache._l1_memory_percent_value == 33.0
    finally:
        tools_cache.set_l1_runtime_config(
            l1_default_ttl_seconds=old_ttl, l1_memory_percent=old_pct
        )


def test_lifespan_registers_and_unregisters_handler():
    """Source-level pin: lifespan must register and unregister the
    handler so the platform config service routes apply calls through
    it. Matches the existing ``_on_valkey_engine_config_change``
    wiring shape — both live next to each other in
    ``CacheModule.lifespan``."""
    from dynastore.modules.cache.cache_module import CacheModule

    src = inspect.getsource(CacheModule.lifespan)
    assert (
        "CachePluginConfig.register_apply_handler" in src
        and "_on_cache_plugin_config_change" in src
    ), (
        "CacheModule.lifespan must register "
        "_on_cache_plugin_config_change as the apply handler for "
        "CachePluginConfig; otherwise PUT /configs?plugin_id=cache_"
        "plugin_config silently no-ops on circuit_breaker_threshold"
    )
    assert "CachePluginConfig.unregister_apply_handler" in src, (
        "lifespan must unregister the handler on shutdown so test "
        "isolation + module-disable hot-paths don't leak the binding"
    )


def test_handler_only_touches_runtime_cache_settings_not_other_fields():
    """Defensive: the handler should not blanket-overwrite arbitrary
    fields on the backend (e.g. ``_consecutive_failures``) — its job is
    to live-apply runtime cache settings only. A regression
    that reset failure counters via this handler would silently
    deplete the circuit breaker."""
    from dynastore.modules.cache import cache_module

    class _FakeBackend:
        def __init__(self):
            self._circuit_breaker_threshold = 3
            self._consecutive_failures = 2  # accumulated failures

    fake = _FakeBackend()
    cache_module._current_backend = fake
    try:
        class _FakeCfg:
            circuit_breaker_threshold = 10

        asyncio.run(
            cache_module._on_cache_plugin_config_change(
                _FakeCfg(), None, None, None
            )
        )
        assert fake._circuit_breaker_threshold == 10
        assert fake._consecutive_failures == 2, (
            "apply handler must NOT reset _consecutive_failures — "
            "doing so would silently rearm a circuit breaker that "
            "was about to trip, masking ongoing Valkey degradation"
        )
    finally:
        cache_module._current_backend = None
