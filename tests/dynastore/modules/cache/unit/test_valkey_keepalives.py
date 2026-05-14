#    Copyright 2025 FAO
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

"""Unit tests for Valkey backend socket_timeout + TCP keepalive wiring.

Regression cover for the review-env cluster read-timeout incident: valkey-py
hard-defaults socket_timeout to 5s and socket_keepalive to False, so an idle
Cloud Run -> Memorystore socket is dropped by Cloud NAT and a cold topology
fetch can exceed 5s. The ValkeyCachePluginConfig tunables must reach the
connection pool kwargs.
"""

from __future__ import annotations

import socket

from dynastore.modules.cache.cache_config import (
    CachePluginConfig,
    ValkeyCachePluginConfig,
)
from dynastore.tools.cache_valkey import ValkeyCacheBackend, _build_keepalive_options


def test_valkey_specific_params_off_standard_config() -> None:
    """Standard CachePluginConfig must not carry Valkey-specific knobs."""
    standard_fields = set(CachePluginConfig.model_fields)
    for impl_field in (
        "socket_timeout_seconds",
        "tcp_keepalive_idle_seconds",
        "tcp_keepalive_interval_seconds",
        "tcp_keepalive_count",
    ):
        assert impl_field not in standard_fields
        assert impl_field in ValkeyCachePluginConfig.model_fields


def test_valkey_config_keepalive_defaults() -> None:
    """ValkeyCachePluginConfig ships explicit socket_timeout + keepalive defaults."""
    cfg = ValkeyCachePluginConfig()
    assert cfg.socket_timeout_seconds == 15.0
    assert cfg.tcp_keepalive_idle_seconds == 300
    assert cfg.tcp_keepalive_interval_seconds == 30
    assert cfg.tcp_keepalive_count == 5
    # Inherits the standard cache fields.
    assert cfg.probe_timeout_seconds == 5.0
    assert cfg.circuit_breaker_threshold == 3


def test_build_keepalive_options_maps_available_constants() -> None:
    """_build_keepalive_options emits only the socket constants the OS exposes."""
    opts = _build_keepalive_options(300, 30, 5)
    if hasattr(socket, "TCP_KEEPIDLE"):
        assert opts[socket.TCP_KEEPIDLE] == 300
    if hasattr(socket, "TCP_KEEPINTVL"):
        assert opts[socket.TCP_KEEPINTVL] == 30
    if hasattr(socket, "TCP_KEEPCNT"):
        assert opts[socket.TCP_KEEPCNT] == 5
    # Nothing supplied -> empty mapping (no spurious keepalive opts).
    assert _build_keepalive_options(None, None, None) == {}


def test_backend_wires_socket_timeout_and_keepalives_into_pool() -> None:
    """ValkeyCacheBackend forwards the tunables to the connection pool kwargs."""
    cfg = ValkeyCachePluginConfig()
    backend = ValkeyCacheBackend(
        url="valkey://localhost:6379",
        socket_connect_timeout=cfg.socket_connect_timeout_seconds,
        socket_timeout=cfg.socket_timeout_seconds,
        tcp_keepalive_idle=cfg.tcp_keepalive_idle_seconds,
        tcp_keepalive_interval=cfg.tcp_keepalive_interval_seconds,
        tcp_keepalive_count=cfg.tcp_keepalive_count,
    )
    pool_kwargs = backend._pool.connection_kwargs  # type: ignore[union-attr]
    assert pool_kwargs["socket_timeout"] == 15.0
    assert pool_kwargs["socket_keepalive"] is True
    assert pool_kwargs["socket_keepalive_options"]  # non-empty on Linux/macOS


def test_backend_omits_keepalives_when_no_tunables_supplied() -> None:
    """Backwards-compat: no keepalive kwargs leak in when nothing is passed."""
    backend = ValkeyCacheBackend(url="valkey://localhost:6379")
    pool_kwargs = backend._pool.connection_kwargs  # type: ignore[union-attr]
    assert "socket_timeout" not in pool_kwargs
    assert "socket_keepalive" not in pool_kwargs
    assert "socket_keepalive_options" not in pool_kwargs
