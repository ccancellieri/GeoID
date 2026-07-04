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

"""Unit tests for Valkey backend socket_timeout + TCP keepalive wiring.

Regression cover for the review-env cluster read-timeout incident: valkey-py
hard-defaults socket_timeout to 5s and socket_keepalive to False, so an idle
Cloud Run -> Memorystore socket is dropped by Cloud NAT and a cold topology
fetch can exceed 5s. The ValkeyEngineConfig tunables must reach the
connection pool kwargs.
"""

from __future__ import annotations

import socket
from unittest.mock import patch

import pytest

from dynastore.modules.db_config.engine_config import ValkeyEngineConfig
from dynastore.tools.cache_valkey import (
    ValkeyCacheBackend,
    _build_keepalive_options,
    build_valkey_client,
)


# --------------------------------------------------------------------------
# ValkeyEngineConfig — connection tunables (moved from CachePluginConfig)
# --------------------------------------------------------------------------


def test_valkey_engine_config_exposes_socket_timeout_and_keepalive_defaults():
    cfg = ValkeyEngineConfig()
    # Read timeout is short by design — a cache fails fast to the source
    # rather than blocking the hot path on an unhealthy backend. 5s is
    # generous headroom for a healthy same-VPC op.
    assert cfg.socket_timeout_seconds == 5.0
    assert cfg.socket_connect_timeout_seconds == 3.0
    # Idle window sits well under Cloud NAT's ~1200s established-conn timeout,
    # matching the DB pool keepalive parity (#655).
    assert cfg.tcp_keepalive_idle_seconds == 300
    assert cfg.tcp_keepalive_interval_seconds == 30
    assert cfg.tcp_keepalive_count == 5


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


def test_build_valkey_client_wires_socket_timeout_and_keepalives() -> None:
    """build_valkey_client forwards the tunables to the connection pool kwargs."""
    client, pool = build_valkey_client(
        url="valkey://localhost:6379",
        socket_connect_timeout=3.0,
        socket_timeout=5.0,
        tcp_keepalive_idle=300,
        tcp_keepalive_interval=30,
        tcp_keepalive_count=5,
    )
    pool_kwargs = pool.connection_kwargs  # type: ignore[union-attr]
    assert pool_kwargs["socket_timeout"] == 5.0
    assert pool_kwargs["socket_connect_timeout"] == 3.0
    assert pool_kwargs["socket_keepalive"] is True
    assert pool_kwargs["socket_keepalive_options"]  # non-empty on Linux/macOS


def test_build_valkey_client_omits_keepalives_when_no_tunables_supplied() -> None:
    """Backwards-compat: no keepalive kwargs leak in when nothing is passed."""
    client, pool = build_valkey_client(url="valkey://localhost:6379")
    pool_kwargs = pool.connection_kwargs  # type: ignore[union-attr]
    assert "socket_timeout" not in pool_kwargs
    assert "socket_connect_timeout" not in pool_kwargs
    assert "socket_keepalive" not in pool_kwargs
    assert "socket_keepalive_options" not in pool_kwargs


# --------------------------------------------------------------------------
# ValkeyEngineConfig — health checks + bounded retry (#2902)
# --------------------------------------------------------------------------


def test_valkey_engine_config_exposes_health_check_and_retry_defaults() -> None:
    cfg = ValkeyEngineConfig()
    assert cfg.health_check_interval_seconds == 30
    assert cfg.retry_attempts == 3


def test_build_valkey_client_wires_health_check_interval() -> None:
    client, pool = build_valkey_client(
        url="valkey://localhost:6379",
        health_check_interval=30,
    )
    pool_kwargs = pool.connection_kwargs  # type: ignore[union-attr]
    assert pool_kwargs["health_check_interval"] == 30


def test_build_valkey_client_wires_bounded_retry_when_configured() -> None:
    """A configured ``retry_attempts`` produces a ``Retry`` object bounded to
    connection-class errors, plus ``retry_on_timeout=True`` for the
    standalone client's asyncio timeout coverage."""
    from valkey.exceptions import ConnectionError as ValkeyConnectionError
    from valkey.retry import Retry

    client, pool = build_valkey_client(
        url="valkey://localhost:6379",
        retry_attempts=3,
    )
    pool_kwargs = pool.connection_kwargs  # type: ignore[union-attr]
    assert pool_kwargs["retry_on_timeout"] is True
    retry = pool_kwargs["retry"]
    assert isinstance(retry, Retry)
    assert retry._retries == 3
    # Only connection-class errors are retried, never application errors.
    assert ValkeyConnectionError in retry._supported_errors
    assert TimeoutError in retry._supported_errors


def test_build_valkey_client_omits_retry_when_retry_attempts_not_supplied() -> None:
    client, pool = build_valkey_client(url="valkey://localhost:6379")
    pool_kwargs = pool.connection_kwargs  # type: ignore[union-attr]
    assert "retry" not in pool_kwargs
    assert "retry_on_timeout" not in pool_kwargs


def test_build_valkey_client_omits_retry_when_retry_attempts_zero() -> None:
    """``retry_attempts=0`` means "no retry" — must not build a Retry(0)."""
    client, pool = build_valkey_client(url="valkey://localhost:6379", retry_attempts=0)
    pool_kwargs = pool.connection_kwargs  # type: ignore[union-attr]
    assert "retry" not in pool_kwargs
    assert "retry_on_timeout" not in pool_kwargs


def test_build_valkey_client_cluster_mode_strips_retry_on_timeout() -> None:
    """``ValkeyCluster.__init__`` has no ``retry_on_timeout`` parameter — passing
    it would raise ``TypeError``. Cluster mode must still receive ``retry`` +
    ``health_check_interval`` (both supported there)."""
    with patch("valkey.asyncio.cluster.ValkeyCluster") as MockCluster:
        build_valkey_client(
            cluster_mode=True,
            discovery_host="10.132.0.9",
            discovery_port=6379,
            health_check_interval=30,
            retry_attempts=3,
        )

    MockCluster.assert_called_once()
    _args, kwargs = MockCluster.call_args
    assert "retry_on_timeout" not in kwargs
    assert kwargs["health_check_interval"] == 30
    assert kwargs["retry"]._retries == 3


async def test_engine_init_plumbs_health_check_and_retry_to_builder() -> None:
    """``ValkeyEngineConfig.engine_init`` forwards the new tunables to
    ``build_valkey_client`` so a config change actually reaches the client."""
    cfg = ValkeyEngineConfig(
        connection_url="valkey://localhost:6379",  # type: ignore[arg-type]
        health_check_interval_seconds=45,
        retry_attempts=5,
    )
    with patch(
        "dynastore.tools.cache_valkey.build_valkey_client",
        return_value=(object(), None),
    ) as mock_build:
        await cfg.engine_init()

    mock_build.assert_called_once()
    _args, kwargs = mock_build.call_args
    assert kwargs["health_check_interval"] == 45
    assert kwargs["retry_attempts"] == 5
