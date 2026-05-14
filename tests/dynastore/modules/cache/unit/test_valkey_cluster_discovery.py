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

"""Unit cover for Valkey cluster discovery wiring + the bootstrap fallback.

The GCP Memorystore for Valkey CLUSTER pattern requires connecting through
the discovery endpoint only — ``dynamic_startup_nodes=False`` so the client
does not refresh topology via CLUSTER SLOTS, ``require_full_coverage=False``
so partial slot coverage during failover doesn't crash the boot path.

These tests pin the defaults and the wiring so a future refactor can't
silently revert them.
"""

from __future__ import annotations

from unittest.mock import patch

from dynastore.modules.db_config.engine_config import ValkeyEngineConfig
from dynastore.tools.cache_valkey import ValkeyCacheBackend, build_valkey_client


# --------------------------------------------------------------------------
# ValkeyEngineConfig — Memorystore CLUSTER discovery defaults
# --------------------------------------------------------------------------


def test_valkey_engine_config_cluster_discovery_defaults() -> None:
    """Pin the cluster-mode defaults that the Memorystore pattern requires."""
    cfg = ValkeyEngineConfig()
    # Cluster mode is opt-in — standalone is the default.
    assert cfg.cluster_mode is False
    # Both flags must default OFF for the Memorystore discovery-only pattern:
    #   - require_full_coverage=False tolerates partial slot coverage
    #     during topology transitions / failover.
    #   - dynamic_startup_nodes=False stops the client from refreshing
    #     topology via CLUSTER SLOTS (Memorystore blocks direct backend
    #     connections — only the discovery endpoint answers).
    assert cfg.require_full_coverage is False
    assert cfg.dynamic_startup_nodes is False
    # Discovery endpoint is opt-in (set via configs API in cluster deployments).
    assert cfg.discovery_host is None
    assert cfg.discovery_port == 6379


# --------------------------------------------------------------------------
# build_valkey_client — cluster discovery routes through ValkeyCluster(host=..)
# --------------------------------------------------------------------------


def test_build_valkey_client_cluster_with_discovery_host_uses_host_port() -> None:
    """In cluster_mode + discovery_host set, route via ValkeyCluster(host, port).

    valkey-py's ``ValkeyCluster.from_url`` would resolve the URL to a set
    of startup nodes and try to refresh topology — the wrong shape for
    Memorystore CLUSTER where only the discovery endpoint is reachable.
    The constructor form pinned here is the one that respects the
    ``dynamic_startup_nodes`` flag.
    """
    with patch("valkey.asyncio.cluster.ValkeyCluster") as MockCluster:
        client, pool = build_valkey_client(
            cluster_mode=True,
            discovery_host="10.132.0.9",
            discovery_port=6379,
            require_full_coverage=False,
            dynamic_startup_nodes=False,
            socket_timeout=5.0,
            socket_connect_timeout=3.0,
        )

    # No connection pool exposed in cluster mode (cluster owns per-node pools).
    assert pool is None

    # ValkeyCluster(...) was used, not ValkeyCluster.from_url(...).
    MockCluster.assert_called_once()
    _args, kwargs = MockCluster.call_args
    assert kwargs["host"] == "10.132.0.9"
    assert kwargs["port"] == 6379
    assert kwargs["require_full_coverage"] is False
    assert kwargs["dynamic_startup_nodes"] is False
    assert kwargs["socket_timeout"] == 5.0
    assert kwargs["socket_connect_timeout"] == 3.0


def test_build_valkey_client_cluster_without_discovery_host_falls_back_to_from_url() -> None:
    """Without a discovery_host, the cluster client must come from a URL."""
    with patch("valkey.asyncio.cluster.ValkeyCluster") as MockCluster:
        MockCluster.from_url.return_value = object()
        client, pool = build_valkey_client(
            url="valkey://10.0.0.1:6379",
            cluster_mode=True,
        )

    assert pool is None
    MockCluster.from_url.assert_called_once()
    url_arg, *_ = MockCluster.from_url.call_args.args
    assert url_arg == "valkey://10.0.0.1:6379"


# --------------------------------------------------------------------------
# ValkeyCacheBackend — legacy env-fallback path retains socket hardening
# --------------------------------------------------------------------------


def test_legacy_backend_url_path_propagates_socket_hardening() -> None:
    """The ``url=``-only legacy bootstrap path must still harden the socket.

    Regression cover: CacheModule's legacy fallback (used when
    ``DBConfigModule`` is not in the SCOPE so ``app_state.engine_cache``
    is absent) builds ``ValkeyCacheBackend(url=...)`` directly. The
    socket_timeout / TCP keepalive params must reach the connection pool
    even via that path, or the un-hardened-socket regression that
    #720 / #724 closed for the engine-driven mode re-opens here.
    """
    cfg = ValkeyEngineConfig()
    backend = ValkeyCacheBackend(
        url="valkey://localhost:6379",
        socket_connect_timeout=cfg.socket_connect_timeout_seconds,
        socket_timeout=cfg.socket_timeout_seconds,
        tcp_keepalive_idle=cfg.tcp_keepalive_idle_seconds,
        tcp_keepalive_interval=cfg.tcp_keepalive_interval_seconds,
        tcp_keepalive_count=cfg.tcp_keepalive_count,
    )
    pool_kwargs = backend._pool.connection_kwargs  # type: ignore[union-attr]
    assert pool_kwargs["socket_timeout"] == cfg.socket_timeout_seconds
    assert pool_kwargs["socket_connect_timeout"] == cfg.socket_connect_timeout_seconds
    assert pool_kwargs["socket_keepalive"] is True
    assert pool_kwargs["socket_keepalive_options"]  # non-empty on Linux/macOS
