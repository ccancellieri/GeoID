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

Registered by ``CacheModule`` when ``VALKEY_URL`` is set.
Falls back to ``LocalAsyncCacheBackend`` (priority=1000) when unavailable.

Env vars:
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
                         redirects, per-node pools).
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import os
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
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

_VALKEY_CIRCUIT_BREAKER_THRESHOLD = int(os.getenv("VALKEY_CIRCUIT_BREAKER_THRESHOLD", "3"))

# ---------------------------------------------------------------------------
#  Valkey INFO section → field mapping (used by ValkeyCacheBackend.info())
# ---------------------------------------------------------------------------
# Maps flat INFO field names to their logical section so callers can use
# info["server"]["redis_version"], info["memory"]["used_memory_human"], etc.
_INFO_FIELD_SECTION: dict = {
    # server section
    "redis_version": "server", "redis_git_sha1": "server", "redis_git_dirty": "server",
    "redis_build_id": "server", "redis_mode": "server", "os": "server",
    "arch_bits": "server", "monotonic_clock": "server", "multiplexing_api": "server",
    "atomicvar_api": "server", "gcc_version": "server", "process_id": "server",
    "server_time_usec": "server", "uptime_in_seconds": "server",
    "uptime_in_days": "server", "hz": "server", "configured_hz": "server",
    "aof_rewrites": "server", "executable": "server", "config_file": "server",
    # clients section
    "connected_clients": "clients", "cluster_connections": "clients",
    "maxclients": "clients", "client_recent_max_input_buffer": "clients",
    "client_recent_max_output_buffer": "clients",
    # memory section
    "used_memory": "memory", "used_memory_human": "memory",
    "used_memory_rss": "memory", "used_memory_rss_human": "memory",
    "used_memory_peak": "memory", "used_memory_peak_human": "memory",
    "used_memory_peak_perc": "memory", "used_memory_overhead": "memory",
    "used_memory_startup": "memory", "used_memory_dataset": "memory",
    "used_memory_dataset_perc": "memory", "allocator_allocated": "memory",
    "allocator_active": "memory", "allocator_resident": "memory",
    "total_system_memory": "memory", "total_system_memory_human": "memory",
    "used_memory_lua": "memory", "used_memory_vm_eval": "memory",
    "used_memory_lua_human": "memory", "used_memory_scripts_eval": "memory",
    "number_of_cached_scripts": "memory", "number_of_functions": "memory",
    "number_of_libraries": "memory", "used_memory_vm_functions": "memory",
    "used_memory_vm_total": "memory", "used_memory_vm_total_human": "memory",
    "used_memory_functions": "memory", "used_memory_scripts": "memory",
    "used_memory_scripts_human": "memory", "maxmemory": "memory",
    "maxmemory_human": "memory", "maxmemory_policy": "memory",
    # stats section
    "total_connections_received": "stats", "total_commands_processed": "stats",
    "instantaneous_ops_per_sec": "stats", "total_net_input_bytes": "stats",
    "total_net_output_bytes": "stats", "total_net_repl_input_bytes": "stats",
    "total_net_repl_output_bytes": "stats", "rejected_connections": "stats",
    "expired_keys": "stats", "evicted_keys": "stats", "keyspace_hits": "stats",
    "keyspace_misses": "stats",
    # replication section
    "role": "replication", "connected_slaves": "replication",
    "master_failover_state": "replication", "master_replid": "replication",
    "master_repl_offset": "replication", "repl_backlog_active": "replication",
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
        json_bytes = data[sep + 1:]
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
        # Called from sync get_credentials() path → use the underlying creds
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

    def __init__(self, url: str, key_prefix: str = "ds:") -> None:
        if not _CACHE_DEPS_OK:
            raise ImportError(
                "ValkeyCacheBackend requires the 'module_cache' extra "
                "(`pip install 'dynastore[module_cache]'` — provides msgpack + valkey). "
                f"Original error: {_CACHE_DEPS_ERR}"
            )
        try:
            import valkey.asyncio as avalkey
        except ImportError as e:
            raise ImportError(
                "ValkeyCacheBackend requires the 'module_cache' extra "
                "(`pip install 'dynastore[module_cache]'` — provides msgpack + valkey). "
                f"Original error: {e}"
            ) from e

        pool_kwargs: Dict[str, Any] = {"decode_responses": False}

        # TLS: VALKEY_TLS=true forces TLS regardless of URL scheme.
        # On Memorystore private VPC, traffic stays in Google's network so
        # cert-verify and hostname checks are commonly disabled when no CA
        # bundle is provided (VALKEY_TLS_CA_PATH overrides).
        if os.getenv("VALKEY_TLS", "").lower() in ("1", "true", "yes"):
            pool_kwargs["connection_class"] = avalkey.SSLConnection
            ca_path = os.getenv("VALKEY_TLS_CA_PATH")
            if ca_path:
                pool_kwargs["ssl_ca_certs"] = ca_path
            else:
                pool_kwargs["ssl_cert_reqs"] = "none"
                pool_kwargs["ssl_check_hostname"] = False

        # IAM auth: mint a Google OAuth2 access token per-connection and
        # pass it as the Valkey AUTH password. Tokens auto-refresh because
        # ``GoogleIamCredentialProvider.get_credentials`` is invoked by the
        # pool on every new connection / reauth.
        if os.getenv("VALKEY_IAM_AUTH", "").lower() in ("1", "true", "yes"):
            pool_kwargs["credential_provider"] = _GoogleIamCredentialProvider()

        # Cluster mode: GCP Memorystore for Valkey CLUSTER instances expose a
        # cluster discovery endpoint. A standalone client misroutes commands
        # and surfaces NOAUTH-shaped errors. ValkeyCluster handles MOVED/ASK
        # redirects and per-node connection pools.
        if os.getenv("VALKEY_CLUSTER", "").lower() in ("1", "true", "yes"):
            from valkey.asyncio.cluster import ValkeyCluster
            # ConnectionPool kwargs flow through; cluster takes the URL directly.
            # `decode_responses=False` is the default; pass remaining kwargs
            # except `connection_class` (cluster picks its own per-node).
            cluster_kwargs = {k: v for k, v in pool_kwargs.items()
                              if k != "connection_class"}
            cluster_kwargs["ssl"] = "connection_class" in pool_kwargs
            self._pool = None  # cluster owns its pools internally
            self._client = ValkeyCluster.from_url(url, **cluster_kwargs)
        else:
            self._pool = avalkey.ConnectionPool.from_url(url, **pool_kwargs)
            self._client = avalkey.Valkey(connection_pool=self._pool)
        self._prefix = key_prefix
        self._stats = CacheStats(maxsize=0)
        self._locks: Dict[str, asyncio.Lock] = {}
        self._consecutive_failures: int = 0

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
        if self._consecutive_failures >= _VALKEY_CIRCUIT_BREAKER_THRESHOLD:
            logger.error(
                "ValkeyCacheBackend: circuit breaker tripped after %d consecutive failures — degrading to L1-only.",
                self._consecutive_failures,
            )
            try:
                from dynastore.tools.cache import get_cache_manager
                get_cache_manager().unregister_backend(self)
            except Exception:
                logger.exception("ValkeyCacheBackend: failed to unregister backend on circuit trip")

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
            logger.debug("ValkeyCacheBackend.get failed", exc_info=True)
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
            logger.debug("ValkeyCacheBackend.set failed", exc_info=True)
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
                # SCAN + UNLINK by prefix pattern
                # Cache keys use "|" as separator (from _make_cache_key)
                pattern = f"{self._prefix}{namespace}|*"
                count = 0
                cursor: int = 0
                while True:
                    cursor, keys = await self._client.scan(
                        cursor, match=pattern, count=200
                    )
                    if keys:
                        await self._client.unlink(*keys)
                        count += len(keys)
                    if cursor == 0:
                        break
                self._record_success()
                return count > 0
            if tags is not None:
                return False  # Tag-based invalidation not supported
            return False
        except Exception:
            self._record_failure()
            logger.debug("ValkeyCacheBackend.clear failed", exc_info=True)
            return False

    async def exists(self, key: str) -> bool:
        try:
            result = bool(await self._client.exists(self._key(key)))
            self._record_success()
            return result
        except Exception:
            self._record_failure()
            logger.debug("ValkeyCacheBackend.exists failed", exc_info=True)
            return False

    async def get_lock(self, key: str) -> asyncio.Lock:
        """Per-process asyncio lock for stampede protection.

        Not a distributed lock — sufficient for single-instance stampede
        prevention. Cross-instance stampede is acceptable (rare, bounded).
        """
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    async def ping(self) -> bool:
        """Health check — verify Valkey connectivity."""
        try:
            result = bool(await self._client.ping())
            self._record_success()
            return result
        except Exception:
            self._record_failure()
            logger.debug("ValkeyCacheBackend.ping failed", exc_info=True)
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
        """Shut down connection pool cleanly."""
        await self._client.aclose()
        if self._pool is not None:
            await self._pool.aclose()
