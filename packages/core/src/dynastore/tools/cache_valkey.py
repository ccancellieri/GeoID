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

"""
Valkey (Redis-compatible) async cache backend for cross-instance consistency.

Implements ``CacheBackend`` + ``LockableCacheBackend`` protocols.
Serialization uses msgpack with ExtType handlers for Pydantic models,
datetime, Enum, and UUID.

Registered by ``CacheModule``. Falls back to ``LocalAsyncCacheBackend``
(priority=1000) when Valkey is unavailable.

Configuration SSOT is ``ValkeyEngineConfig`` (in
``modules/db_config/engine_config.py``), exposed via the configs API
under ``platform/module_cache`` and applied live without restart —
URL, discovery_host/port, cluster_mode, require_full_coverage,
dynamic_startup_nodes, TLS, IAM, socket_timeout/connect_timeout,
TCP keepalives.

Env-var bootstrap fallback (used only when the engine cache isn't
available, e.g. a minimal worker SCOPE that omits ``DBConfigModule``):

  VALKEY_URL           — connection URL (e.g. ``valkey://10.0.0.1:6379``)
  VALKEY_TLS           — ``true`` to wrap the connection in TLS (independent
                         of URL scheme). Required for GCP Memorystore IAM mode.
  VALKEY_TLS_CA_PATH   — optional path to a server CA bundle for verification.
                         If unset and TLS is on, cert/hostname checks are
                         disabled (acceptable on private VPC).
  VALKEY_IAM_AUTH      — ``true`` to authenticate via a Google OAuth2 access
                         token minted from ADC. Requires ``google-auth``
                         (provided by the ``module_gcp`` extra).
  VALKEY_CLUSTER       — ``true`` for GCP Memorystore for Valkey CLUSTER
                         instances. Uses ``ValkeyCluster`` (handles MOVED/ASK
                         redirects, per-node pools). For Memorystore CLUSTER
                         the discovery-only pattern (host+port,
                         ``dynamic_startup_nodes=False``) is the engine-driven
                         path — there is no env-var alias for it.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
import socket
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
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
    """Deserialize msgpack bytes to a value."""
    return msgpack.unpackb(data, ext_hook=_msgpack_ext_hook, raw=False)


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


def build_valkey_client(
    *,
    url: Optional[str] = None,
    discovery_host: Optional[str] = None,
    discovery_port: int = 6379,
    cluster_mode: bool = False,
    require_full_coverage: bool = False,
    dynamic_startup_nodes: bool = False,
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
) -> "tuple[Any, Any]":
    """Build a Valkey async client (standalone or cluster) from connection params.

    Returns ``(client, pool)`` where ``pool`` is ``None`` for cluster mode
    (cluster client owns its own per-node pools).

    Cluster precedence: when ``cluster_mode=True`` and ``discovery_host`` is
    set, uses the host+port discovery endpoint with ``dynamic_startup_nodes``
    + ``require_full_coverage`` knobs (Memorystore Valkey CLUSTER pattern).
    Otherwise falls back to ``ValkeyCluster.from_url(url, ...)``.
    """
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

    pool_kwargs: Dict[str, Any] = {"decode_responses": False}

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
        from valkey.asyncio.cluster import ValkeyCluster

        # cluster handles per-node pools internally; drop connection_class
        # (cluster picks SSLConnection itself when ssl=True).
        cluster_kwargs = {
            k: v for k, v in pool_kwargs.items() if k != "connection_class"
        }
        cluster_kwargs["ssl"] = tls
        cluster_kwargs["require_full_coverage"] = require_full_coverage
        cluster_kwargs["dynamic_startup_nodes"] = dynamic_startup_nodes

        # Discovery endpoint preferred (Memorystore Valkey CLUSTER pattern)
        if discovery_host:
            client = ValkeyCluster(
                host=discovery_host,
                port=discovery_port,
                **cluster_kwargs,
            )
        elif url:
            client = ValkeyCluster.from_url(url, **cluster_kwargs)
        else:
            raise ValueError(
                "build_valkey_client(cluster_mode=True): one of "
                "`discovery_host` or `url` must be provided."
            )
        return client, None

    # Standalone
    if not url:
        raise ValueError("build_valkey_client(cluster_mode=False): `url` is required.")
    pool = avalkey.ConnectionPool.from_url(url, **pool_kwargs)
    client = avalkey.Valkey(connection_pool=pool)
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
                "VALKEY_IAM_AUTH=true requires the 'module_gcp' extra "
                "(google-auth). "
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
        url: Optional[str] = None,
        key_prefix: str = "ds:",
        socket_connect_timeout: Optional[float] = None,
        socket_timeout: Optional[float] = None,
        tcp_keepalive_idle: Optional[int] = None,
        tcp_keepalive_interval: Optional[int] = None,
        tcp_keepalive_count: Optional[int] = None,
        circuit_breaker_threshold: Optional[int] = None,
        *,
        client: Optional[Any] = None,
        pool: Optional[Any] = None,
        owns_client: bool = True,
    ) -> None:
        """Construct a Valkey cache backend.

        Two construction modes:

        1. **Engine-driven (preferred)** — pass a pre-built ``client``
           (and optional ``pool``). The engine (``ValkeyEngineConfig``)
           owns lifecycle; set ``owns_client=False`` so ``close()`` does
           not double-release a resource the engine cache will release.

        2. **Legacy env-driven** — pass ``url`` (and optional timeout /
           keepalive kwargs). The backend builds its own client via
           ``build_valkey_client(...)`` consuming the legacy
           ``VALKEY_TLS`` / ``VALKEY_IAM_AUTH`` / ``VALKEY_CLUSTER`` env
           vars for back-compat. Used as the bootstrap fallback when
           the engine is unavailable.
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

        if client is not None:
            # Mode 1: engine-driven — caller owns lifecycle.
            self._client = client
            self._pool = pool
            self._owns_client = owns_client
        else:
            # Mode 2: legacy env-driven path — preserved for tests and the
            # bootstrap fallback when no engine_cache is wired.
            if not url:
                raise ValueError(
                    "ValkeyCacheBackend: either `client` or `url` must be provided."
                )
            tls = os.getenv("VALKEY_TLS", "").lower() in ("1", "true", "yes")
            tls_ca_path = os.getenv("VALKEY_TLS_CA_PATH")
            iam_auth = os.getenv("VALKEY_IAM_AUTH", "").lower() in ("1", "true", "yes")
            cluster_mode = os.getenv("VALKEY_CLUSTER", "").lower() in (
                "1",
                "true",
                "yes",
            )
            built_client, built_pool = build_valkey_client(
                url=url,
                cluster_mode=cluster_mode,
                tls=tls,
                tls_ca_path=tls_ca_path,
                tls_cert_reqs="required" if tls_ca_path else "none",
                tls_check_hostname=bool(tls_ca_path),
                iam_auth=iam_auth,
                socket_connect_timeout=socket_connect_timeout,
                socket_timeout=socket_timeout,
                tcp_keepalive_idle=tcp_keepalive_idle,
                tcp_keepalive_interval=tcp_keepalive_interval,
                tcp_keepalive_count=tcp_keepalive_count,
            )
            self._client = built_client
            self._pool = built_pool
            self._owns_client = True

        self._prefix = key_prefix
        self._stats = CacheStats(maxsize=0)
        self._locks: Dict[str, asyncio.Lock] = {}
        self._consecutive_failures: int = 0
        self._circuit_breaker_threshold = (
            circuit_breaker_threshold or _VALKEY_CIRCUIT_BREAKER_DEFAULT
        )

    @property
    def name(self) -> str:
        return "valkey"

    @property
    def priority(self) -> int:
        return 100

    def _key(self, key: str) -> str:
        """Prefix a cache key for Valkey namespace isolation."""
        return f"{self._prefix}{key}"

    def _record_failure(self) -> None:
        """Increment failure counter and trip circuit breaker if threshold exceeded."""
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
        except Exception:
            self._record_failure()
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
        except Exception:
            self._record_failure()
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
        except Exception:
            self._record_failure()
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
        except Exception:
            self._record_failure()
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
        except Exception:
            self._record_failure()
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
        except Exception:
            self._record_failure()
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
        except Exception:
            self._record_failure()
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
        except Exception:
            self._record_failure()
            logger.warning(
                "ValkeyCacheBackend.expireat failed (key=%s)", full, exc_info=True
            )
            return False

    async def ping(self) -> bool:
        """Health check — verify Valkey connectivity."""
        try:
            result = bool(await self._client.ping())
            self._record_success()
            return result
        except Exception:
            self._record_failure()
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
