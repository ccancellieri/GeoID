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

"""Resolved-endpoint visibility on the Valkey (re)connect banners (#2812).

During the #2812 dev outage, services were pinned to a Valkey endpoint
running a different mode (cluster) than the client (standalone). The
reconnect banner logged only ``version``/``mode``, never the endpoint
itself, which hid the endpoint drift for days. These tests pin:

  1. ``resolve_valkey_target`` derives a redacted ``host:port (mode)``
     string from either the discovery endpoint or the connection URL.
  2. The cold-boot connect banner logs that resolved host (previously a
     dead ``"<engine>"`` placeholder that was never actually resolved).
  3. The live-reconnect banner (config-PATCH apply / circuit-breaker
     recovery) logs it too — the line the issue called out as omitting it.
"""

from __future__ import annotations

import types
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from dynastore.tools.cache_valkey import resolve_valkey_target


# --------------------------------------------------------------------------
# resolve_valkey_target
# --------------------------------------------------------------------------


def test_resolve_target_prefers_discovery_host_in_cluster_mode() -> None:
    target = resolve_valkey_target(
        url="valkey://ignored:6379",
        discovery_host="10.132.0.9",
        discovery_port=6379,
        cluster_mode=True,
    )
    assert target == "10.132.0.9:6379 (cluster)"


def test_resolve_target_parses_host_port_from_url_standalone() -> None:
    target = resolve_valkey_target(
        url="valkey://10.0.0.1:6380/0", cluster_mode=False
    )
    assert target == "10.0.0.1:6380 (standalone)"


def test_resolve_target_never_leaks_credentials() -> None:
    target = resolve_valkey_target(
        url="valkey://user:s3cr3t@10.0.0.1:6379/0", cluster_mode=False
    )
    assert "s3cr3t" not in target
    assert target == "10.0.0.1:6379 (standalone)"


def test_resolve_target_unresolved_when_nothing_configured() -> None:
    target = resolve_valkey_target(cluster_mode=False)
    assert target == "<unresolved> (standalone)"


# --------------------------------------------------------------------------
# Cold-boot connect banner
# --------------------------------------------------------------------------


def _make_app_state(**kwargs: Any) -> types.SimpleNamespace:
    return types.SimpleNamespace(**kwargs)


class _ProbeOkBackend:
    """Fake ``ValkeyCacheBackend`` that always probes clean (standalone)."""

    def __init__(self, *_a: Any, **kw: Any) -> None:
        self.client = kw.get("client")

    async def info(self) -> dict:
        return {
            "server": {"redis_version": "7.4.0", "redis_mode": "standalone"},
            "memory": {"used_memory_human": "1M"},
        }

    async def topology(self) -> dict:
        return {"is_cluster": False, "primaries": 0, "replicas": 0, "slots": []}

    async def close(self) -> None:
        pass


async def test_lifespan_connect_banner_logs_resolved_host(monkeypatch, caplog):
    """The cold-boot connect banner must report the resolved endpoint the
    engine actually built the client against, not a dead placeholder."""
    from dynastore.modules.cache.cache_module import CacheModule
    import dynastore.tools.cache_valkey as cv

    monkeypatch.setattr(cv, "_CACHE_DEPS_OK", True)
    monkeypatch.setattr(cv, "ValkeyCacheBackend", _ProbeOkBackend)

    client = types.SimpleNamespace(_ds_resolved_target="10.0.0.5:6379 (standalone)")

    class _EngineCacheStub:
        async def get(self, ref: str) -> object:
            return client

    async def _no_cfg(*_a: Any, **_kw: Any) -> Any:
        from dynastore.modules.cache.cache_config import CachePluginConfig

        return CachePluginConfig()

    monkeypatch.setattr(
        "dynastore.modules.cache.cache_module._load_cache_config", _no_cfg
    )

    manager = MagicMock()
    app_state = _make_app_state(engine_cache=_EngineCacheStub())

    module = CacheModule(app_state=app_state)
    with (
        patch("dynastore.tools.cache.get_cache_manager", return_value=manager),
        patch("dynastore.tools.cache._notify_backend_upgrade"),
        caplog.at_level("INFO"),
    ):
        async with module.lifespan(app_state):
            pass

    banner_lines = [
        r.getMessage()
        for r in caplog.records
        if "CACHE BACKEND: VALKEY (shared" in r.getMessage()
    ]
    assert len(banner_lines) == 1
    assert "host=10.0.0.5:6379 (standalone)" in banner_lines[0]

    probe_lines = [
        r.getMessage() for r in caplog.records if "CacheModule: Valkey OK" in r.getMessage()
    ]
    assert len(probe_lines) == 1
    assert "10.0.0.5:6379 (standalone)" in probe_lines[0]


# --------------------------------------------------------------------------
# Live-reconnect banner (config-PATCH apply / circuit-breaker recovery)
# --------------------------------------------------------------------------


def _stub_engine_cache(client: Any) -> Any:
    ec = MagicMock()
    ec.get = AsyncMock(return_value=client)
    ec.evict = AsyncMock(return_value=None)
    ec.update_config = AsyncMock(return_value=None)
    return ec


async def test_reconnect_banner_logs_resolved_host(monkeypatch, caplog):
    """The reconnect banner used to omit the endpoint entirely — the exact
    gap the #2812 issue called out (endpoint drift hidden for days)."""
    from dynastore.modules.cache import cache_module as cm

    old_backend = MagicMock()
    old_backend.close = AsyncMock(return_value=None)

    new_client = types.SimpleNamespace(
        _ds_resolved_target="10.0.0.9:6379 (cluster)"
    )
    engine_cache = _stub_engine_cache(new_client)

    cm._app_state = types.SimpleNamespace(engine_cache=engine_cache)
    cm._current_backend = old_backend

    new_backend = MagicMock()
    new_backend.info = AsyncMock(
        return_value={
            "server": {"redis_version": "7.4.0", "redis_mode": "cluster"},
            "memory": {"used_memory_human": "1M"},
        }
    )
    new_backend.topology = AsyncMock(
        return_value={"is_cluster": True, "primaries": 1, "replicas": 0, "slots": []}
    )
    new_backend.close = AsyncMock(return_value=None)

    manager = MagicMock()

    with (
        patch(
            "dynastore.tools.cache_valkey.ValkeyCacheBackend", return_value=new_backend
        ),
        patch("dynastore.tools.cache.get_cache_manager", return_value=manager),
        patch("dynastore.tools.cache._notify_backend_upgrade"),
        caplog.at_level("INFO"),
    ):
        await cm._on_valkey_engine_config_change(None, None, None, None)

    reconnect_lines = [
        r.getMessage()
        for r in caplog.records
        if r.getMessage().startswith("CACHE RECONNECT: success=true")
    ]
    assert len(reconnect_lines) == 1
    assert "host=10.0.0.9:6379 (cluster)" in reconnect_lines[0]

    banner_lines = [
        r.getMessage()
        for r in caplog.records
        if "CACHE BACKEND: VALKEY (reconnected)" in r.getMessage()
    ]
    assert len(banner_lines) == 1
    assert "host=10.0.0.9:6379 (cluster)" in banner_lines[0]
