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
from dynastore.tools.cache_valkey import (
    ValkeyCacheBackend,
    build_discovery_port_remap,
    build_valkey_client,
)


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
    # Discovery-port remap is opt-in — only Memorystore Valkey 9 advertises
    # unreachable internal shard addresses, so the default must not perturb
    # Valkey 8 / other clusters that advertise reachable node addresses.
    assert cfg.discovery_port_remap is False
    # Discovery endpoint is opt-in (set via configs API in cluster deployments).
    assert cfg.discovery_host is None
    assert cfg.discovery_port == 6379


# --------------------------------------------------------------------------
# build_discovery_port_remap — Memorystore Valkey 9 unreachable-shard fix
# --------------------------------------------------------------------------


def test_build_discovery_port_remap_forces_discovery_port_keeps_host() -> None:
    """The remap rewrites the port to discovery_port and preserves the host.

    Memorystore Valkey 9 CLUSTER advertises internal shard addresses at the
    cluster-bus port range (e.g. ``10.132.0.10:11026``). Each shard's host is
    still its own reachable PSC IP, so the remap must keep the host and only
    correct the port (dynastore#264).
    """
    remap = build_discovery_port_remap(6379)
    # Internal cluster-bus address → reachable PSC endpoint, same host.
    assert remap(("10.132.0.10", 11026)) == ("10.132.0.10", 6379)
    assert remap(("10.132.0.9", 11007)) == ("10.132.0.9", 6379)
    # Already-correct port is left as the discovery port (idempotent).
    assert remap(("10.132.0.9", 6379)) == ("10.132.0.9", 6379)


def test_build_discovery_port_remap_honours_non_default_port() -> None:
    """A non-6379 discovery port is propagated to every remapped address."""
    remap = build_discovery_port_remap(6380)
    assert remap(("10.132.0.10", 11026)) == ("10.132.0.10", 6380)


def test_build_valkey_client_cluster_wires_address_remap_when_enabled() -> None:
    """``discovery_port_remap=True`` wires an ``address_remap`` callable.

    The callable handed to ValkeyCluster must rewrite a discovered internal
    shard address to the reachable discovery port while keeping the host.
    """
    with patch("valkey.asyncio.cluster.ValkeyCluster") as MockCluster:
        build_valkey_client(
            cluster_mode=True,
            discovery_host="10.132.0.9",
            discovery_port=6379,
            discovery_port_remap=True,
        )

    MockCluster.assert_called_once()
    _args, kwargs = MockCluster.call_args
    remap = kwargs["address_remap"]
    assert callable(remap)
    assert remap(("10.132.0.10", 11026)) == ("10.132.0.10", 6379)


def test_build_valkey_client_cluster_no_remap_by_default() -> None:
    """Without the flag, no ``address_remap`` is passed (Valkey 8 unaffected)."""
    with patch("valkey.asyncio.cluster.ValkeyCluster") as MockCluster:
        build_valkey_client(
            cluster_mode=True,
            discovery_host="10.132.0.9",
            discovery_port=6379,
        )

    MockCluster.assert_called_once()
    _args, kwargs = MockCluster.call_args
    assert "address_remap" not in kwargs


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
# build_valkey_client — the single client builder retains socket hardening
# --------------------------------------------------------------------------


def test_build_valkey_client_propagates_socket_hardening() -> None:
    """The single client builder must harden the socket.

    Regression cover: ``build_valkey_client`` — the one client-construction
    path (``ValkeyEngineConfig.engine_init`` calls it, ``CacheModule`` wraps
    the result) — must propagate socket_timeout / TCP keepalive params to the
    connection pool, or the un-hardened-socket regression that #720 / #724
    closed re-opens.
    """
    cfg = ValkeyEngineConfig()
    _client, pool = build_valkey_client(
        url="valkey://localhost:6379",
        socket_connect_timeout=cfg.socket_connect_timeout_seconds,
        socket_timeout=cfg.socket_timeout_seconds,
        tcp_keepalive_idle=cfg.tcp_keepalive_idle_seconds,
        tcp_keepalive_interval=cfg.tcp_keepalive_interval_seconds,
        tcp_keepalive_count=cfg.tcp_keepalive_count,
    )
    pool_kwargs = pool.connection_kwargs  # type: ignore[union-attr]
    assert pool_kwargs["socket_timeout"] == cfg.socket_timeout_seconds
    assert pool_kwargs["socket_connect_timeout"] == cfg.socket_connect_timeout_seconds
    assert pool_kwargs["socket_keepalive"] is True
    assert pool_kwargs["socket_keepalive_options"]  # non-empty on Linux/macOS


# --------------------------------------------------------------------------
# ValkeyEngineConfig.engine_init — plumbs discovery_port_remap to the builder
# --------------------------------------------------------------------------


async def test_engine_init_plumbs_discovery_port_remap_to_builder() -> None:
    """The engine config forwards ``discovery_port_remap`` to build_valkey_client.

    Pins the wiring so a Memorystore Valkey 9 operator who flips the config
    knob actually gets the address-remap behaviour built into the client.
    """
    cfg = ValkeyEngineConfig(
        cluster_mode=True,
        discovery_host="10.132.0.9",  # type: ignore[arg-type]
        discovery_port=6379,
        discovery_port_remap=True,
    )
    with patch(
        "dynastore.tools.cache_valkey.build_valkey_client",
        return_value=(object(), None),
    ) as mock_build:
        await cfg.engine_init()

    mock_build.assert_called_once()
    _args, kwargs = mock_build.call_args
    assert kwargs["discovery_port_remap"] is True
    assert kwargs["cluster_mode"] is True
    assert kwargs["discovery_host"] == "10.132.0.9"
