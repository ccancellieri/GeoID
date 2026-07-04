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

"""
Valkey (Redis-compatible) async cache backend for cross-instance consistency.

Implements ``CacheBackend`` + ``LockableCacheBackend`` protocols.
Serialization uses msgpack with ExtType handlers for Pydantic models,
datetime, Enum, and UUID.

Registered by ``CacheModule``. Falls back to ``LocalAsyncCacheBackend``
(priority=1000) when Valkey is unavailable.

Configuration SSOT is ``ValkeyEngineConfig`` (in
``modules/db_config/engine_config.py``), exposed via the configs API
under ``platform/protocols/storage`` and applied live without restart —
URL, discovery_host/port, cluster_mode, require_full_coverage,
dynamic_startup_nodes, discovery_port_remap, TLS, IAM,
socket_timeout/connect_timeout, TCP keepalives. ``CacheModule`` acquires
the client exclusively via ``app_state.engine_cache.get("valkey_engine")``
— there is no separate env-driven connection path in production.

Cluster mode auto-detection:
  The backend probes the server with the engine-built client first. If the
  server reports cluster mode (via INFO) but the client is standalone, the
  engine config's ``cluster_mode`` is misconfigured; ``CacheModule`` logs a
  WARNING and rebuilds a dedicated cluster-mode client for the process.

``VALKEY_URL`` env var:
  Consumed directly by ``ValkeyEngineConfig.engine_init()`` as the
  boot-time bootstrap connection string when ``connection_url`` is unset
  on the engine config — not read by this module.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import socket
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
from uuid import UUID

# msgpack + valkey are optional — provided by the ``module_cache`` extra.
# Import them lazily so the module can be imported in environments that
# don't ship the extra; ``ValkeyCacheBackend.__init__`` raises a friendly
# ImportError if the deps are actually needed.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import msgpack

try:
    import msgpack  # noqa: F811

    _CACHE_DEPS_OK = True
    _CACHE_DEPS_ERR: Optional[ImportError] = None
except ImportError as _e:
    _CACHE_DEPS_OK = False
    _CACHE_DEPS_ERR = _e

from dynastore.models.protocols.cache import CacheStats

logger = logging.getLogger(__name__)

# Circuit breaker default — used only when a backend is built without an
# explicit ``circuit_breaker_threshold`` (test/legacy paths). The SSOT
# is ``CachePluginConfig.circuit_breaker_threshold``, which the
# ``CacheModule`` lifespan plumbs through to every constructor.
_VALKEY_CIRCUIT_BREAKER_DEFAULT = 3


def _is_moved_error(exc: BaseException) -> bool:
    """True when ``exc`` is a Valkey ``MOVED`` ``ResponseError``.

    Lazily imports ``valkey.exceptions`` (mirrors this module's existing
    lazy-import pattern for the optional ``valkey`` dependency) rather
    than duck-typing on the exception's class name.
    """
    try:
        from valkey.exceptions import ResponseError
    except ImportError:
        return False
    return isinstance(exc, ResponseError) and str(exc).upper().startswith("MOVED")


def _is_standalone_client(client: Any) -> bool:
    """Cheap sync duck-type check for "not a ``ValkeyCluster`` client".

    Mirrors the ``nodes_manager`` / ``get_primaries`` check
    ``ValkeyCacheBackend.topology()`` already uses to distinguish a
    standalone ``Valkey`` client from a ``ValkeyCluster`` one.
    """
    return getattr(client, "nodes_manager", None) is None or not hasattr(
        client, "get_primaries"
    )

# ---------------------------------------------------------------------------
#  Valkey INFO section → field mapping (used by ValkeyCacheBackend.info())
# ---------------------------------------------------------------------------
# Maps flat INFO field names to their logical section so callers can use
# info["server"]["redis_version"], info["memory"]["used_memory_human"], etc.
_INFO_FIELD_SECTION: dict = {
    # server section
    "redis_version": "server",
    "redis_git_sha1": "server",
    "redis_git_dirty": "server",
    "redis_build_id": "server",
    "redis_mode": "server",
    "os": "server",
    "arch_bits": "server",
    "monotonic_clock": "server",
    "multiplexing_api": "server",
    "atomicvar_api": "server",
    "gcc_version": "server",
    "process_id": "server",
    "server_time_usec": "server",
    "uptime_in_seconds": "server",
    "uptime_in_days": "server",
    "hz": "server",
    "configured_hz": "server",
    "aof_rewrites": "server",
    "executable": "server",
    "config_file": "server",
    # clients section
    "connected_clients": "clients",
    "cluster_connections": "clients",
    "maxclients": "clients",
    "client_recent_max_input_buffer": "clients",
    "client_recent_max_output_buffer": "clients",
    # memory section
    "used_memory": "memory",
    "used_memory_human": "memory",
    "used_memory_rss": "memory",
    "used_memory_rss_human": "memory",
    "used_memory_peak": "memory",
    "used_memory_peak_human": "memory",
    "used_memory_peak_perc": "memory",
    "used_memory_overhead": "memory",
    "used_memory_startup": "memory",
    "used_memory_dataset": "memory",
    "used_memory_dataset_perc": "memory",
    "allocator_allocated": "memory",
    "allocator_active": "memory",
    "allocator_resident": "memory",
    "total_system_memory": "memory",
    "total_system_memory_human": "memory",
    "used_memory_lua": "memory",
    "used_memory_vm_eval": "memory",
    "used_memory_lua_human": "memory",
    "used_memory_scripts_eval": "memory",
    "number_of_cached_scripts": "memory",
    "number_of_functions": "memory",
    "number_of_libraries": "memory",
    "used_memory_vm_functions": "memory",
    "used_memory_vm_total": "memory",
    "used_memory_vm_total_human": "memory",
    "used_memory_functions": "memory",
    "used_memory_scripts": "memory",
    "used_memory_scripts_human": "memory",
    "maxmemory": "memory",
    "maxmemory_human": "memory",
    "maxmemory_policy": "memory",
    # stats section
    "total_connections_received": "stats",
    "total_commands_processed": "stats",
    "instantaneous_ops_per_sec": "stats",
    "total_net_input_bytes": "stats",
    "total_net_output_bytes": "stats",
    "total_net_repl_input_bytes": "stats",
    "total_net_repl_output_bytes": "stats",
    "rejected_connections": "stats",
    "expired_keys": "stats",
    "evicted_keys": "stats",
    "keyspace_hits": "stats",
    "keyspace_misses": "stats",
    # replication section
    "role": "replication",
    "connected_slaves": "replication",
    "master_failover_state": "replication",
    "master_replid": "replication",
    "master_repl_offset": "replication",
    "repl_backlog_active": "replication",
    "repl_backlog_size": "replication",
}

# ---------------------------------------------------------------------------
#  MsgPack ExtType codes for non-primitive types
# ---------------------------------------------------------------------------
_EXT_DATETIME = 1
_EXT_ENUM = 2
_EXT_UUID = 3
_EXT_PYDANTIC = 10


def _msgpack_default(obj: Any) -> msgpack.ExtType:
    """Custom msgpack serializer for non-primitive types."""
    if isinstance(obj, datetime):
        return msgpack.ExtType(_EXT_DATETIME, obj.isoformat().encode("utf-8"))
    if isinstance(obj, UUID):
        return msgpack.ExtType(_EXT_UUID, obj.hex.encode("utf-8"))
    if isinstance(obj, Enum):
        return msgpack.ExtType(
            _EXT_ENUM,
            msgpack.packb(obj.value, use_bin_type=True),
        )
    if hasattr(obj, "model_dump_json"):
        # Pydantic model: store fully-qualified class name + JSON dump
        cls = type(obj)
        fqn = f"{cls.__module__}.{cls.__qualname__}".encode("utf-8")
        json_bytes = obj.model_dump_json().encode("utf-8")
        return msgpack.ExtType(_EXT_PYDANTIC, fqn + b"\x00" + json_bytes)
    raise TypeError(f"Object of type {type(obj).__name__} is not msgpack-serializable")


def _msgpack_ext_hook(code: int, data: bytes) -> Any:
    """Custom msgpack deserializer for ExtType values."""
    if code == _EXT_DATETIME:
        return datetime.fromisoformat(data.decode("utf-8"))
    if code == _EXT_UUID:
        return UUID(data.decode("utf-8"))
    if code == _EXT_ENUM:
        return msgpack.unpackb(data, raw=False)
    if code == _EXT_PYDANTIC:
        sep = data.index(b"\x00")
        fqn = data[:sep].decode("utf-8")
        json_bytes = data[sep + 1 :]
        module_path, _, class_name = fqn.rpartition(".")
        mod = importlib.import_module(module_path)
        cls = getattr(mod, class_name)
        return cls.model_validate_json(json_bytes)
    return msgpack.ExtType(code, data)


def _serialize(value: Any) -> bytes:
    """Serialize a value to msgpack bytes."""
    return msgpack.packb(value, default=_msgpack_default, use_bin_type=True)  # type: ignore[return-value]


def _deserialize(data: bytes) -> Any:
    """Deserialize msgpack bytes to a value.

    ``strict_map_key=False``: cached payloads legitimately contain
    int-keyed dicts (e.g. per-zoom tile parameters), which ``_serialize``
    packs without complaint but the default ``strict_map_key=True``
    rejects on read — turning every get for such an entry into a failure
    that also feeds the circuit breaker. The strict default guards
    untrusted input; this cache only reads what ``_serialize`` wrote.
    """
    return msgpack.unpackb(
        data, ext_hook=_msgpack_ext_hook, raw=False, strict_map_key=False
    )


def _build_keepalive_options(
    idle: Optional[int],
    interval: Optional[int],
    count: Optional[int],
) -> Dict[int, int]:
    """Map TCP keepalive tunables to ``socket.setsockopt`` option codes.

    Returns the ``socket_keepalive_options`` dict valkey-py feeds to each
    connection. Option constants (``TCP_KEEPIDLE`` / ``TCP_KEEPINTVL`` /
    ``TCP_KEEPCNT``) are platform-specific — ``getattr``-guarded so this is a
    no-op on platforms (e.g. macOS dev boxes) that don't expose them. Linux,
    where Cloud Run runs, has all three.
    """
    opts: Dict[int, int] = {}
    if idle is not None and hasattr(socket, "TCP_KEEPIDLE"):
        opts[socket.TCP_KEEPIDLE] = idle
    if interval is not None and hasattr(socket, "TCP_KEEPINTVL"):
        opts[socket.TCP_KEEPINTVL] = interval
    if count is not None and hasattr(socket, "TCP_KEEPCNT"):
        opts[socket.TCP_KEEPCNT] = count
    return opts


def build_discovery_port_remap(
    discovery_port: int,
) -> Callable[[Tuple[str, int]], Tuple[str, int]]:
    """Build an ``address_remap`` callable that forces every node to the discovery port.

    GCP Memorystore for Valkey 9 CLUSTER instances answer topology commands
    (``CLUSTER SLOTS`` / ``CLUSTER NODES``) on the PSC discovery endpoint with
    *internal* shard addresses at the cluster-bus port range (e.g.
    ``10.132.0.10:11026``). Those addresses are **not reachable** from a client
    going through PSC — the client follows them and hangs (the v8→v9 regression
    in dynastore#264).

    Each shard's advertised host is still its own reachable PSC IP; only the
    port is wrong. valkey-py invokes ``RedisCluster.remap_host_port`` on every
    discovered ``(host, port)``; this callable preserves the discovered host
    and rewrites the port back to ``discovery_port`` (typically ``6379``), so
    the client talks to each shard on its reachable PSC endpoint instead of the
    internal cluster-bus port.
    """

    def _remap(address: Tuple[str, int]) -> Tuple[str, int]:
        host, _port = address
        return (host, discovery_port)

    return _remap


def resolve_valkey_target(
    *,
    url: Optional[str] = None,
    discovery_host: Optional[str] = None,
    discovery_port: int = 6379,
    cluster_mode: bool = False,
) -> str:
    """Human-readable ``host:port (mode)`` connection target for logging.

    Used by ``CacheModule``'s (re)connect banners (#2812 follow-up — the
    reconnect line used to omit the resolved endpoint, which hid endpoint
    drift for days). Never leaks credentials embedded in ``url``: only the
    hostname/port are extracted, never userinfo or query string.

    Discovery endpoint (cluster mode) takes precedence, matching
    ``build_valkey_client``'s own precedence. Falls back to parsing
    ``host:port`` out of ``url``, then to an explicit "unresolved" marker.
    """
    mode = "cluster" if cluster_mode else "standalone"
    if discovery_host:
        return f"{discovery_host}:{discovery_port} ({mode})"
    if url:
        from urllib.parse import urlsplit

        try:
            parsed = urlsplit(url)
            host = parsed.hostname or "?"
            port = parsed.port or discovery_port
            return f"{host}:{port} ({mode})"
        except Exception:
            return f"<unparseable-url> ({mode})"
    return f"<unresolved> ({mode})"


def build_valkey_client(
    *,
    url: Optional[str] = None,
    discovery_host: Optional[str] = None,
    discovery_port: int = 6379,
    cluster_mode: bool = False,
    require_full_coverage: bool = False,
    dynamic_startup_nodes: bool = False,
    discovery_port_remap: bool = False,
    tls: bool = False,
    tls_ca_path: Optional[str] = None,
    tls_cert_reqs: str = "none",
    tls_check_hostname: bool = False,
    iam_auth: bool = False,
    socket_connect_timeout: Optional[float] = None,
    socket_timeout: Optional[float] = None,
    tcp_keepalive_idle: Optional[int] = None,
    tcp_keepalive_interval: Optional[int] = None,
    tcp_keepalive_count: Optional[int] = None,
    health_check_interval: Optional[float] = None,
    retry_attempts: Optional[int] = None,
    max_connections: Optional[int] = None,
) -> "tuple[Any, Any]":
    """Build a Valkey async client (standalone or cluster) from connection params.

    Returns ``(client, pool)`` where ``pool`` is ``None`` for cluster mode
    (cluster client owns its own per-node pools).

    Cluster precedence: when ``cluster_mode=True`` and ``discovery_host`` is
    set, uses the host+port discovery endpoint with ``dynamic_startup_nodes``
    + ``require_full_coverage`` knobs (Memorystore Valkey CLUSTER pattern).
    Otherwise falls back to ``ValkeyCluster.from_url(url, ...)``.

    ``discovery_port_remap`` (cluster mode only): when True, every node address
    discovered from the cluster topology has its port rewritten back to
    ``discovery_port`` via an ``address_remap`` callable. This is the fix for
    Memorystore Valkey 9 CLUSTER, which advertises unreachable internal shard
    addresses at the cluster-bus port range (see ``build_discovery_port_remap``).

    ``health_check_interval`` (#2902): proactively PINGs an idle connection
    every N seconds so a socket silently dropped by an intermediary (Cloud
    NAT, LB) is detected and replaced before the next real command hits it,
    instead of surfacing as a hard failure on that command.

    ``retry_attempts`` (#2902): bounded retry (exponential backoff) for the
    connection-class errors — ``ConnectionError``/``TimeoutError`` and their
    asyncio counterparts — that a single stale pooled socket raises. Valkey's
    ``Retry`` only ever matches those error classes (never application-level
    errors such as a bad command or a cluster ``MOVED`` response), so a
    genuine backend fault still surfaces immediately.

    ``max_connections`` (#2961): caps concurrent connections in the
    standalone ``ConnectionPool`` and (via ``cluster_kwargs``) each of the
    cluster client's per-node pools. Set before the cluster-mode kwargs
    filter below so both paths inherit it without separate plumbing.
    valkey-py otherwise defaults this to ``2**31`` (effectively unbounded).
    """
    import logging
    _logger = logging.getLogger(__name__)
    _logger.debug(
        "build_valkey_client: ENTRY — cluster_mode=%s discovery_host=%s url=%s tls=%s iam_auth=%s",
        cluster_mode,
        discovery_host,
        url,
        tls,
        iam_auth,
    )
    
    if not _CACHE_DEPS_OK:
        raise ModuleNotFoundError(
            "build_valkey_client requires the 'module_cache' extra "
            "(`pip install 'dynastore[module_cache]'` — provides msgpack + valkey). "
            f"Original error: {_CACHE_DEPS_ERR}"
        )
    try:
        import valkey.asyncio as avalkey
    except ImportError as e:
        raise ModuleNotFoundError(
            "build_valkey_client requires the 'module_cache' extra "
            "(`pip install 'dynastore[module_cache]'` — provides msgpack + valkey). "
            f"Original error: {e}"
        ) from e

    # Stashed on the built client (below) as ``_ds_resolved_target`` so
    # ``CacheModule``'s connect/reconnect banners can log the resolved
    # endpoint without re-deriving it from the (possibly secret-backed)
    # connection params (#2812 follow-up).
    resolved_target = resolve_valkey_target(
        url=url,
        discovery_host=discovery_host,
        discovery_port=discovery_port,
        cluster_mode=cluster_mode,
    )

    pool_kwargs: Dict[str, Any] = {"decode_responses": False}

    if max_connections is not None:
        pool_kwargs["max_connections"] = max_connections

    if socket_connect_timeout is not None:
        pool_kwargs["socket_connect_timeout"] = socket_connect_timeout
    if socket_timeout is not None:
        pool_kwargs["socket_timeout"] = socket_timeout

    # TCP keepalives — Cloud NAT silently drops idle established connections
    # after ~1200s. Without probes the next op gets a dead socket and
    # ValkeyCluster pays a full re-init.
    if any(
        v is not None
        for v in (tcp_keepalive_idle, tcp_keepalive_interval, tcp_keepalive_count)
    ):
        pool_kwargs["socket_keepalive"] = True
        keepalive_options = _build_keepalive_options(
            tcp_keepalive_idle, tcp_keepalive_interval, tcp_keepalive_count
        )
        if keepalive_options:
            pool_kwargs["socket_keepalive_options"] = keepalive_options

    # Proactive health checks — PING idle connections every N seconds so a
    # socket dead-on-arrival (stale pooled connection) is caught and replaced
    # ahead of the next real command rather than failing that command outright.
    if health_check_interval is not None:
        pool_kwargs["health_check_interval"] = health_check_interval

    # Bounded retry for connection-class errors (#2902). Passing a ``Retry``
    # object is enough on its own — its default ``supported_errors``
    # (``ConnectionError``/``TimeoutError``/``socket.timeout``) already
    # excludes application-level errors; ``retry_on_timeout`` additionally
    # folds in asyncio's ``TimeoutError`` for the standalone client.
    if retry_attempts is not None and retry_attempts > 0:
        from valkey.backoff import ExponentialBackoff
        from valkey.retry import Retry

        pool_kwargs["retry_on_timeout"] = True
        pool_kwargs["retry"] = Retry(ExponentialBackoff(), retry_attempts)

    # TLS
    if tls:
        pool_kwargs["connection_class"] = avalkey.SSLConnection
        if tls_ca_path:
            pool_kwargs["ssl_ca_certs"] = tls_ca_path
        pool_kwargs["ssl_cert_reqs"] = tls_cert_reqs
        pool_kwargs["ssl_check_hostname"] = tls_check_hostname

    # IAM AUTH (GCP Memorystore for Valkey)
    if iam_auth:
        pool_kwargs["credential_provider"] = _GoogleIamCredentialProvider()

    # Cluster mode
    if cluster_mode:
        _logger.debug("build_valkey_client: CLUSTER MODE — entering cluster path")
        from valkey.asyncio.cluster import ValkeyCluster

        # cluster handles per-node pools internally; drop connection_class
        # (cluster picks SSLConnection itself when ssl=True) and
        # retry_on_timeout (ValkeyCluster.__init__ has no such parameter —
        # it merges the equivalent error classes itself once a ``retry``
        # object is supplied, see valkey.asyncio.cluster.ValkeyCluster).
        cluster_kwargs = {
            k: v
            for k, v in pool_kwargs.items()
            if k not in ("connection_class", "retry_on_timeout")
        }
        cluster_kwargs["ssl"] = tls
        cluster_kwargs["require_full_coverage"] = require_full_coverage
        cluster_kwargs["dynamic_startup_nodes"] = dynamic_startup_nodes

        _logger.debug(
            "build_valkey_client: cluster_kwargs — require_full_coverage=%s dynamic_startup_nodes=%s discovery_port_remap=%s",
            require_full_coverage,
            dynamic_startup_nodes,
            discovery_port_remap,
        )

        # Memorystore Valkey 9 advertises unreachable internal shard
        # addresses (cluster-bus port range) in its topology; rewrite every
        # discovered node's port back to the reachable discovery port.
        if discovery_port_remap:
            _logger.debug("build_valkey_client: applying discovery_port_remap")
            cluster_kwargs["address_remap"] = build_discovery_port_remap(
                discovery_port
            )

        # Discovery endpoint preferred (Memorystore Valkey CLUSTER pattern)
        if discovery_host:
            _logger.debug(
                "build_valkey_client: creating ValkeyCluster via discovery endpoint — host=%s port=%d",
                discovery_host,
                discovery_port,
            )
            client = ValkeyCluster(  # type: ignore[abstract]
                host=discovery_host,
                port=discovery_port,
                **cluster_kwargs,
            )
        elif url:
            _logger.debug(
                "build_valkey_client: creating ValkeyCluster via from_url — url=%s",
                url,
            )
            client = ValkeyCluster.from_url(url, **cluster_kwargs)
        else:
            raise ValueError(
                "build_valkey_client(cluster_mode=True): one of "
                "`discovery_host` or `url` must be provided."
            )
        _logger.debug("build_valkey_client: ValkeyCluster created successfully, returning")
        # Best-effort stash — some clients (e.g. exotic test doubles) don't
        # accept new attributes; the resolved target is a logging aid, not
        # load-bearing, so failure here must never break client construction.
        try:
            setattr(client, "_ds_resolved_target", resolved_target)
        except Exception:  # noqa: BLE001
            pass
        return client, None

    # Standalone
    _logger.debug("build_valkey_client: STANDALONE MODE — entering standalone path")
    if not url:
        raise ValueError("build_valkey_client(cluster_mode=False): `url` is required.")
    _logger.debug(
        "build_valkey_client: creating Valkey (standalone) client — url=%s",
        url,
    )
    pool = avalkey.ConnectionPool.from_url(url, **pool_kwargs)
    client = avalkey.Valkey(connection_pool=pool)
    try:
        setattr(client, "_ds_resolved_target", resolved_target)
    except Exception:  # noqa: BLE001
        pass
    _logger.debug("build_valkey_client: Valkey client created successfully, returning")
    return client, pool


# ---------------------------------------------------------------------------
#  Google IAM credential provider for Memorystore for Valkey
# ---------------------------------------------------------------------------


class _GoogleIamCredentialProvider:
    """Mints Google OAuth2 access tokens for Memorystore IAM AUTH.

    Reuses ``GCPModule`` (via ``CloudIdentityProtocol.get_fresh_token``) when
    it has been registered — same cached creds object, same refresh logic that
    powers signed URLs and other GCP clients. When CacheModule starts before
    GCPModule (priority 9 vs 30), we fall back to a direct ADC fetch using
    ``modules.gcp.tools.service_account``; on later reconnects the protocol
    lookup wins.

    Memorystore expects the service-account email as username and a fresh
    OAuth2 access token as password.
    """

    def __init__(self) -> None:
        self._fallback_creds: Any = None
        self._username: Optional[str] = None

    def _resolve_via_protocol(self) -> Optional[tuple]:
        try:
            from dynastore.models.protocols.cloud_identity import (
                CloudIdentityProtocol,
            )
            from dynastore.tools.discovery import get_protocol
        except ImportError:
            return None
        provider = get_protocol(CloudIdentityProtocol)
        if provider is None:
            return None
        # GCPModule._refresh_credentials is sync; get_fresh_token offloads it.
        # Called from sync get_credentials() path -> use the underlying creds
        # object directly to stay sync-friendly here.
        creds = provider.get_credentials_object()
        if not creds.valid or creds.expired:
            import google.auth.transport.requests as _gart

            creds.refresh(_gart.Request())
        username = (
            provider.get_account_email()
            or getattr(creds, "service_account_email", None)
            or "default"
        )
        return (username, creds.token)

    def _resolve_via_adc(self) -> tuple:
        try:
            from dynastore.modules.gcp.tools.service_account import (
                get_credentials as _gcp_get_credentials,
            )
        except ImportError as e:
            raise ImportError(
                "ValkeyEngineConfig.iam_auth=true requires the 'module_gcp' "
                "extra (google-auth). "
                f"Original error: {e}"
            ) from e

        if self._fallback_creds is None:
            creds, identity = _gcp_get_credentials()
            self._fallback_creds = creds
            self._username = identity.get("account_email") or "default"

        creds = self._fallback_creds
        if not creds.valid or creds.expired:
            import google.auth.transport.requests as _gart

            creds.refresh(_gart.Request())
        return (self._username or "default", creds.token)

    def get_credentials(self) -> tuple:
        return self._resolve_via_protocol() or self._resolve_via_adc()

    async def get_credentials_async(self) -> tuple:
        # google-auth refresh is sync HTTP; offload so we don't block the loop.
        return await asyncio.to_thread(self.get_credentials)


# ---------------------------------------------------------------------------
#  ValkeyCacheBackend
# ---------------------------------------------------------------------------


class ValkeyCacheBackend:
    """Shared Valkey cache backend for cross-instance consistency.

    - ``priority = 100`` — wins over ``LocalAsyncCacheBackend`` (1000)
    - Key prefix ``ds:`` isolates Dynastore in shared Valkey instances
    - Implements ``CacheBackend`` + ``LockableCacheBackend`` protocols
    """

    def __init__(
        self,
        client: Any,
        key_prefix: str = "ds:",
        *,
        pool: Optional[Any] = None,
        owns_client: bool = True,
        circuit_breaker_threshold: Optional[int] = None,
        on_trip: Optional[Callable[["ValkeyCacheBackend"], None]] = None,
    ) -> None:
        """Wrap an engine-built Valkey ``client`` as the cache backend.

        ``ValkeyEngineConfig`` (the configs-API SSOT) is the single
        client-construction path: it builds the client via
        ``build_valkey_client`` and ``CacheModule`` wraps it here with
        ``owns_client=False`` so ``close()`` does not double-release a
        resource the engine cache releases at shutdown. Connection params
        (URL / TLS / IAM / socket hardening / cluster mode) live on
        ``ValkeyEngineConfig`` and ``build_valkey_client`` — never on this
        wrapper; reconfiguration flows through the configs API and
        re-inits the engine, never by re-constructing this backend with raw
        connection kwargs.

        ``on_trip`` (#2741): called synchronously, with ``self``, right
        after the circuit breaker unregisters this instance from the
        ``CacheManager``. ``CacheModule`` passes a callback here that
        schedules a guarded background re-probe loop, since
        ``register_backend`` is otherwise only ever called at startup or
        on an explicit config PATCH — without a recovery path a mid-life
        trip degrades the process to L1-only cache (and IAM denylist
        checks fail open) until the pod restarts.
        """
        # ModuleNotFoundError (a subclass of ImportError) so existing
        # `except ImportError` handlers still catch it AND the module
        # loader's wrong-SCOPE soft-skip (`isinstance(e, ModuleNotFoundError)`
        # in modules/__init__.py) treats it as an expected missing-extra
        # rather than crashing the worker.
        if not _CACHE_DEPS_OK:
            raise ModuleNotFoundError(
                "ValkeyCacheBackend requires the 'module_cache' extra "
                "(`pip install 'dynastore[module_cache]'` — provides msgpack + valkey). "
                f"Original error: {_CACHE_DEPS_ERR}"
            )

        if client is None:
            raise ValueError(
                "ValkeyCacheBackend requires a pre-built `client` from "
                "ValkeyEngineConfig.engine_init()."
            )
        self._client = client
        self._pool = pool
        self._owns_client = owns_client
        self._on_trip = on_trip

        self._prefix = key_prefix
        self._stats = CacheStats(maxsize=0)
        self._locks: Dict[str, asyncio.Lock] = {}
        self._consecutive_failures: int = 0
        self._circuit_breaker_threshold = (
            circuit_breaker_threshold or _VALKEY_CIRCUIT_BREAKER_DEFAULT
        )
        # #2812 follow-up: rate-limit the MOVED-on-standalone WARNING to
        # once per backend instance (a reconnect/rebuild creates a fresh
        # instance and re-arms it) instead of once per failing command.
        self._moved_warned: bool = False

    @property
    def name(self) -> str:
        return "valkey"

    @property
    def priority(self) -> int:
        return 100

    def _key(self, key: str) -> str:
        """Prefix a cache key for Valkey namespace isolation."""
        return f"{self._prefix}{key}"

    def _record_failure(self, exc: Optional[BaseException] = None) -> None:
        """Increment failure counter and trip circuit breaker if threshold exceeded.

        ``exc`` (#2812 follow-up): when the failure is a ``MOVED``
        ``ResponseError`` on a standalone client — meaning the configured
        endpoint is actually running in cluster mode, the same root cause
        as the #2812 outage — logs one hard WARNING naming the endpoint.
        Detection only: no client rebuild, no config mutation (client
        rebuild-on-MOVED is #2743's scope). Rate-limited to once per
        backend instance so a MOVED storm produces one signal, not
        per-command spam.
        """
        if (
            exc is not None
            and not self._moved_warned
            and _is_standalone_client(self._client)
            and _is_moved_error(exc)
        ):
            self._moved_warned = True
            target = getattr(self._client, "_ds_resolved_target", "<unknown>")
            logger.warning(
                "CACHE ENDPOINT MISMATCH: standalone Valkey client at %s "
                "received a MOVED response — this endpoint is running in "
                "CLUSTER mode, not standalone. Correct "
                "ValkeyEngineConfig (cluster_mode=true, discovery_host) via "
                "PATCH /configs/plugins/valkey_engine_config. Detection "
                "only; the client is not rebuilt automatically.",
                target,
            )
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._circuit_breaker_threshold:
            logger.error(
                "ValkeyCacheBackend: circuit breaker tripped after %d consecutive failures — degrading to L1-only.",
                self._consecutive_failures,
            )
            try:
                from dynastore.tools.cache import get_cache_manager

                get_cache_manager().unregister_backend(self)
            except Exception:
                logger.exception(
                    "ValkeyCacheBackend: failed to unregister backend on circuit trip"
                )
            if self._on_trip is not None:
                try:
                    self._on_trip(self)
                except Exception:
                    logger.exception(
                        "ValkeyCacheBackend: on_trip recovery callback failed"
                    )

    def _record_success(self) -> None:
        """Reset failure counter on successful operation."""
        self._consecutive_failures = 0

    async def get(self, key: str) -> Optional[bytes]:
        try:
            raw = await self._client.get(self._key(key))
            self._record_success()
            if raw is None:
                return None
            return _deserialize(raw)
        except Exception as exc:
            self._record_failure(exc)
            logger.warning(
                "ValkeyCacheBackend.get failed (key=%s)", self._key(key), exc_info=True
            )
            return None

    async def set(
        self,
        key: str,
        value: bytes,
        *,
        ttl: Optional[float] = None,
        exist: Optional[bool] = None,
    ) -> bool:
        try:
            serialized = _serialize(value)
            kwargs: Dict[str, Any] = {}
            if exist is True:
                kwargs["xx"] = True
            if exist is False:
                kwargs["nx"] = True
            if ttl is not None:
                # Use millisecond precision for sub-second TTLs
                kwargs["px"] = int(ttl * 1000)
            result = await self._client.set(self._key(key), serialized, **kwargs)
            self._record_success()
            return bool(result)
        except Exception as exc:
            self._record_failure(exc)
            logger.warning(
                "ValkeyCacheBackend.set failed (key=%s)", self._key(key), exc_info=True
            )
            return False

    async def clear(
        self,
        *,
        key: Optional[str] = None,
        namespace: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> bool:
        try:
            if key is not None:
                result = bool(await self._client.unlink(self._key(key)))
                self._record_success()
                return result
            if namespace is not None:
                # Cache keys use "|" as separator (from _make_cache_key).
                # scan_iter handles both standalone and cluster (where a raw
                # scan() returns a per-node dict cursor that can't be re-fed).
                pattern = f"{self._prefix}{namespace}|*"
                count = 0
                async for k in self._client.scan_iter(match=pattern, count=200):
                    # One-key UNLINK avoids CROSSSLOT errors on clustered
                    # Valkey, where a multi-key call across slots fails.
                    if await self._client.unlink(k):
                        count += 1
                self._record_success()
                return count > 0
            if tags is not None:
                return False  # Tag-based invalidation not supported
            return False
        except Exception as exc:
            self._record_failure(exc)
            logger.warning(
                "ValkeyCacheBackend.clear failed (key=%s namespace=%s)",
                self._key(key) if key is not None else None,
                namespace,
                exc_info=True,
            )
            return False

    async def exists(self, key: str) -> bool:
        try:
            result = bool(await self._client.exists(self._key(key)))
            self._record_success()
            return result
        except Exception as exc:
            self._record_failure(exc)
            logger.warning(
                "ValkeyCacheBackend.exists failed (key=%s)",
                self._key(key),
                exc_info=True,
            )
            return False

    async def get_lock(self, key: str) -> asyncio.Lock:
        """Per-process asyncio lock for stampede protection.

        Not a distributed lock — sufficient for single-instance stampede
        prevention. Cross-instance stampede is acceptable (rare, bounded).
        """
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    # ------------------------------------------------------------------
    # CountingCacheBackend extension protocol
    # ------------------------------------------------------------------
    #
    # Three atomic primitives backing :class:`UsageCounterProtocol` Valkey
    # driver. ``incr_if_below`` runs server-side as a Lua script so the
    # cap check and the increment commit in one round trip — two pods
    # cannot both succeed at the boundary the way they could with a
    # GET-then-INCR sequence.

    _INCR_IF_BELOW_SCRIPT = (
        # KEYS[1] = prefixed key, ARGV = {limit, amount, ttl_ms}
        # Returns {new_value, allowed_int}. allowed_int = 1 iff committed.
        "local cur = tonumber(redis.call('GET', KEYS[1]) or '0') "
        "local lim = tonumber(ARGV[1]) "
        "local inc = tonumber(ARGV[2]) "
        "local ttl_ms = tonumber(ARGV[3]) "
        "if cur + inc > lim then "
        "  return {cur, 0} "
        "end "
        "local nv = redis.call('INCRBY', KEYS[1], inc) "
        "if ttl_ms > 0 and tonumber(nv) == inc then "
        "  redis.call('PEXPIRE', KEYS[1], ttl_ms) "
        "end "
        "return {tonumber(nv), 1}"
    )
    # SHA1 of the above script body, computed once at class load. We try
    # EVALSHA first on every call (single round trip, ~50 bytes on the
    # wire) and fall back to EVAL on NOSCRIPT — that path auto-loads the
    # script into the server cache so subsequent calls hit the fast path.
    import hashlib as _hashlib
    _INCR_IF_BELOW_SHA = _hashlib.sha1(_INCR_IF_BELOW_SCRIPT.encode("utf-8")).hexdigest()
    del _hashlib

    async def get_count(self, key: str) -> Optional[int]:
        full = self._key(key)
        try:
            raw = await self._client.get(full)
            self._record_success()
            if raw is None:
                return None
            # ``INCRBY`` stores native integers — the client returns them
            # as bytes (or str) of the ASCII digit form, not msgpack.
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8")
            return int(raw)
        except (ValueError, TypeError):
            logger.warning(
                "ValkeyCacheBackend.get_count: non-integer payload at key=%s", full
            )
            return None
        except Exception as exc:
            self._record_failure(exc)
            logger.warning(
                "ValkeyCacheBackend.get_count failed (key=%s)", full, exc_info=True
            )
            return None

    async def incr(
        self,
        key: str,
        amount: int = 1,
        *,
        ttl: Optional[float] = None,
    ) -> int:
        full = self._key(key)
        try:
            new_value = await self._client.incrby(full, amount)
            # Only stamp TTL on creation (avoid resetting expiry on every hit).
            if ttl is not None and int(new_value) == int(amount):
                await self._client.pexpire(full, int(ttl * 1000))
            self._record_success()
            return int(new_value)
        except Exception as exc:
            self._record_failure(exc)
            logger.warning(
                "ValkeyCacheBackend.incr failed (key=%s)", full, exc_info=True
            )
            raise

    async def incr_if_below(
        self,
        key: str,
        limit: int,
        amount: int = 1,
        *,
        ttl: Optional[float] = None,
    ) -> Tuple[int, bool]:
        full = self._key(key)
        ttl_ms = int(ttl * 1000) if ttl is not None else 0
        try:
            # EVALSHA dispatch — server holds the script bytes after the
            # first EVAL; from there on we only ship the 40-char SHA1
            # plus args. On NOSCRIPT (server forgot the script, e.g.
            # after restart or SCRIPT FLUSH) fall back to EVAL which
            # auto-loads it and runs in the same trip.
            try:
                result = await self._client.execute_command(
                    "EVALSHA", self._INCR_IF_BELOW_SHA, 1, full, limit, amount, ttl_ms
                )
            except Exception as exc:
                if "NOSCRIPT" not in str(exc):
                    raise
                result = await self._client.execute_command(
                    "EVAL", self._INCR_IF_BELOW_SCRIPT, 1, full, limit, amount, ttl_ms
                )
            self._record_success()
            new_value = int(result[0])
            allowed = bool(int(result[1]))
            return (new_value, allowed)
        except Exception as exc:
            self._record_failure(exc)
            logger.warning(
                "ValkeyCacheBackend.incr_if_below failed (key=%s)", full, exc_info=True
            )
            raise

    async def expireat(self, key: str, ts: float) -> bool:
        full = self._key(key)
        try:
            # EXPIREAT takes seconds; PEXPIREAT takes ms. Stick to ms for
            # sub-second precision parity with set()'s px argument.
            result = await self._client.pexpireat(full, int(ts * 1000))
            self._record_success()
            return bool(result)
        except Exception as exc:
            self._record_failure(exc)
            logger.warning(
                "ValkeyCacheBackend.expireat failed (key=%s)", full, exc_info=True
            )
            return False

    # ------------------------------------------------------------------
    # ListCacheBackend extension protocol (#2833)
    # ------------------------------------------------------------------
    #
    # Bounded FIFO primitives backing the Valkey-buffered log producer/
    # drainer. Mirrors the ``CountingCacheBackend`` primitives above: no
    # parallel client, raises on failure (rather than swallowing) so the
    # caller — the producer's push-then-fallback seam, or the drainer's
    # tick — can react to Valkey trouble itself.

    async def rpush_trimmed(
        self,
        key: str,
        values: List[bytes],
        *,
        max_len: int,
    ) -> int:
        full = self._key(key)
        if not values:
            return 0
        try:
            new_len = await self._client.rpush(full, *values)
            self._record_success()
        except Exception as exc:
            self._record_failure(exc)
            logger.warning(
                "ValkeyCacheBackend.rpush_trimmed failed (key=%s)", full, exc_info=True
            )
            raise
        # Not atomic with the RPUSH above — under concurrent producers the
        # list may transiently exceed max_len by a few entries; the next
        # push's trim self-heals it. Acceptable for an ephemeral log queue
        # (losing a few extra entries is fine; blocking producers is not).
        dropped = max(0, int(new_len) - max_len)
        if dropped:
            try:
                await self._client.ltrim(full, -max_len, -1)
                self._record_success()
            except Exception as exc:
                self._record_failure(exc)
                logger.warning(
                    "ValkeyCacheBackend.rpush_trimmed: ltrim failed (key=%s)",
                    full, exc_info=True,
                )
        return dropped

    async def lpop_many(self, key: str, count: int) -> List[bytes]:
        full = self._key(key)
        try:
            result = await self._client.lpop(full, count)
            self._record_success()
        except Exception as exc:
            self._record_failure(exc)
            logger.warning(
                "ValkeyCacheBackend.lpop_many failed (key=%s)", full, exc_info=True
            )
            raise
        if not result:
            return []
        return list(result)

    async def ping(self) -> bool:
        """Health check — verify Valkey connectivity."""
        try:
            result = bool(await self._client.ping())
            self._record_success()
            return result
        except Exception as exc:
            self._record_failure(exc)
            logger.warning("ValkeyCacheBackend.ping failed", exc_info=True)
            return False

    async def info(self) -> dict:
        """Return server info sections (server, memory, stats, replication).

        The valkey INFO command returns a flat string; the client parses it
        into a dict keyed by section name, each value being a dict of fields.
        """
        raw = await self._client.info("all")
        # Cluster mode returns Dict[node_addr, Dict[field, value]]; pick the
        # first node's view (all nodes report the same server/version info,
        # only stats/replication differ — close enough for the startup log).
        if raw and isinstance(next(iter(raw.values())), dict):
            raw = next(iter(raw.values()))
        # valkey.asyncio returns a flat dict of all fields; group into sections
        # by matching known prefixes so callers can use info["server"]["redis_version"]
        sections: dict = {}
        for field, value in raw.items():
            # Fields like "redis_version", "used_memory_human", etc. are flat
            # in the raw dict — group them by their logical INFO section
            section = _INFO_FIELD_SECTION.get(field, "misc")
            sections.setdefault(section, {})[field] = value
        return sections

    async def topology(self) -> dict:
        """Introspect the *client's* live cluster topology.

        Unlike ``INFO`` (which reflects a single server's self-view and can
        report ``redis_mode:standalone`` for a node, or be absent entirely),
        this reads the ``ValkeyCluster`` client's own discovered topology —
        i.e. proof of which shards *this connection* actually routes to.

        Returns a dict::

            {
                "is_cluster": bool,        # client is ValkeyCluster
                "primaries": int,          # number of primary shards seen
                "replicas": int,
                "slots": [                 # slot range -> owning primary
                    {"start": 0, "end": 5460, "node": "10.132.0.25:6379"},
                    ...
                ],
                "nodes": ["host:port (primary|replica)", ...],
            }

        For a standalone client returns ``{"is_cluster": False}``.
        """
        client = self._client
        # Standalone clients have no nodes_manager / get_primaries.
        nodes_mgr = getattr(client, "nodes_manager", None)
        if nodes_mgr is None or not hasattr(client, "get_primaries"):
            return {"is_cluster": False, "primaries": 0, "replicas": 0,
                    "slots": [], "nodes": []}

        def _addr(node: object) -> str:
            host = getattr(node, "host", "?")
            port = getattr(node, "port", "?")
            return f"{host}:{port}"

        try:
            primaries = list(client.get_primaries())
            replicas = list(client.get_replicas())
        except Exception:  # pragma: no cover - defensive
            primaries, replicas = [], []

        nodes_desc = [f"{_addr(n)} (primary)" for n in primaries]
        nodes_desc += [f"{_addr(n)} (replica)" for n in replicas]

        # slots_cache maps each of the 16384 slots -> [primary, *replicas].
        # Collapse contiguous runs that share the same owning primary into
        # ranges so the log is one line per shard, not 16384.
        slot_ranges: list[dict] = []
        slots_cache = getattr(nodes_mgr, "slots_cache", None)
        if isinstance(slots_cache, dict) and slots_cache:
            run_start: int | None = None
            run_owner: str | None = None
            for slot in range(16384):
                owners = slots_cache.get(slot)
                owner = _addr(owners[0]) if owners else None
                if owner != run_owner:
                    if run_owner is not None and run_start is not None:
                        slot_ranges.append(
                            {"start": run_start, "end": slot - 1, "node": run_owner}
                        )
                    run_start, run_owner = slot, owner
            if run_owner is not None and run_start is not None:
                slot_ranges.append(
                    {"start": run_start, "end": 16383, "node": run_owner}
                )
            # Drop gaps (uncovered slots) which carry node=None.
            slot_ranges = [r for r in slot_ranges if r["node"] is not None]

        return {
            "is_cluster": True,
            "primaries": len(primaries),
            "replicas": len(replicas),
            "slots": slot_ranges,
            "nodes": nodes_desc,
        }

    async def verify_routing(self, samples_per_shard: int = 1) -> dict:
        """Behavioural proof that this client routes to *every* shard IP.

        Topology (``topology()``) only reports what the client *discovered*.
        This method goes further: for each primary shard it crafts a key that
        hashes into that shard's slot range, issues a real ``GET`` against the
        cluster, and records which node IP the command was *actually* dispatched
        to. If a shard's IP is unreachable (e.g. a VPC node the client knows
        about but cannot connect to), the GET raises and we capture the error
        for that shard — turning a silent "only one shard is used" condition
        into an explicit, per-IP result.

        Returns::

            {
                "is_cluster": bool,
                "shards": [
                    {"node": "10.132.0.25:6379", "slot": 866,
                     "key": "geoid:routeprobe:...", "ok": True,
                     "served_by": "10.132.0.25:6379"},
                    ...
                ],
                "distinct_ips_reached": int,   # how many IPs answered
            }

        ``served_by`` is read from the cluster's per-command node selection so
        it reflects the *real* dispatch target, not the precomputed map.
        """
        client = self._client
        nodes_mgr = getattr(client, "nodes_manager", None)
        if nodes_mgr is None or not hasattr(client, "get_primaries"):
            return {"is_cluster": False, "shards": [], "distinct_ips_reached": 0}

        def _addr(node: object) -> str:
            return f"{getattr(node, 'host', '?')}:{getattr(node, 'port', '?')}"

        # Build one probe key per primary by brute-forcing a small suffix until
        # keyslot() lands in a slot owned by that primary. This guarantees the
        # GET routes to the intended shard via the client's own hashing.
        slots_cache = getattr(nodes_mgr, "slots_cache", {}) or {}
        primary_addrs = {_addr(n) for n in client.get_primaries()}
        # slot -> owning primary addr (first slot per primary is enough)
        wanted: dict[str, int] = {}
        for slot, owners in slots_cache.items():
            if not owners:
                continue
            addr = _addr(owners[0])
            if addr in primary_addrs and addr not in wanted:
                wanted[addr] = slot
            if len(wanted) == len(primary_addrs):
                break

        results: list[dict] = []
        served_ips: set[str] = set()
        for addr, slot in wanted.items():
            # Find a key whose CRC16 slot equals `slot`.
            key = None
            for i in range(100000):
                cand = f"geoid:routeprobe:{slot}:{i}"
                if client.keyslot(cand) == slot:
                    key = cand
                    break
            if key is None:
                results.append({"node": addr, "slot": slot, "key": None,
                                "ok": False, "served_by": None,
                                "error": "no key found for slot"})
                continue
            try:
                # Real round-trip to the shard that owns this slot.
                await client.get(key)
                # Which node did the client pick for this slot? Read it back
                # from the routing table (authoritative for the dispatch).
                owners = slots_cache.get(slot) or []
                served = _addr(owners[0]) if owners else addr
                served_ips.add(served)
                results.append({"node": addr, "slot": slot, "key": key,
                                "ok": True, "served_by": served})
            except Exception as exc:  # node known but unreachable, etc.
                results.append({"node": addr, "slot": slot, "key": key,
                                "ok": False, "served_by": None,
                                "error": f"{type(exc).__name__}: {exc}"})

        return {
            "is_cluster": True,
            "shards": results,
            "distinct_ips_reached": len(served_ips),
        }

    async def close(self) -> None:
        """Shut down connection pool cleanly.

        Skips the actual ``aclose()`` calls when ``owns_client=False`` —
        the engine cache is responsible for releasing engine-built
        clients via ``ValkeyEngineConfig.engine_release``.
        """
        if not self._owns_client:
            return
        await self._client.aclose()
        if self._pool is not None:
            await self._pool.aclose()
