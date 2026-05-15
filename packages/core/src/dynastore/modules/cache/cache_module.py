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
CacheModule — registers a shared Valkey cache backend.

Primary mode (engine-driven):
    Acquires the Valkey client from ``app_state.engine_cache`` (backed by
    ``ValkeyEngineConfig``).  Connection params (URL/TLS/IAM/cluster) are
    mutable via the configs API; changes trigger a live rebuild without
    restart.

Legacy fallback (env-driven):
    When ``app_state.engine_cache`` is unavailable, falls back to the
    old ``VALKEY_URL`` / ``VALKEY_TLS`` / ``VALKEY_IAM_AUTH`` /
    ``VALKEY_CLUSTER`` env vars.  Preserves compatibility with
    deployments that haven't provisioned the engine config.

Cache-layer settings (probe_timeout, circuit_breaker) remain on
``CachePluginConfig``.

Add ``module_cache`` to the deployment scope extras to activate::

    scope_catalog = ["dynastore[...,module_cache]"]
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncGenerator, Optional

from dynastore.modules.protocols import ModuleProtocol

if TYPE_CHECKING:
    from dynastore.modules.cache.cache_config import CachePluginConfig
    from dynastore.modules.db_config.engine_instance_cache import EngineInstanceCache

logger = logging.getLogger(__name__)

# Module-level state for the apply-handler closure.
# Set during CacheModule.lifespan; used by _on_valkey_engine_config_change.
_current_backend: Optional[Any] = None
_app_state: Optional[Any] = None
_apply_lock: asyncio.Lock = asyncio.Lock()


async def _load_cache_config() -> "CachePluginConfig":
    """Load cache config from PluginConfig protocol.

    Falls back to defaults if config is missing or protocol unavailable
    (e.g., during early bootstrap before ConfigProtocol is registered).
    """
    try:
        from dynastore.modules.cache.cache_config import CachePluginConfig
        from dynastore.models.protocols.configs import ConfigsProtocol

        try:
            from dynastore.frameworks.plugin import get_protocol

            configs_proto = get_protocol(ConfigsProtocol)
        except Exception as e:
            logger.debug(
                "CacheModule: ConfigsProtocol not available yet (%s), using defaults", e
            )
            return CachePluginConfig()

        try:
            cfg = await configs_proto.get_config(CachePluginConfig)
            if cfg:
                return cfg
        except Exception as e:
            logger.debug(
                "CacheModule: failed to load CachePluginConfig (%s), using defaults", e
            )

    except Exception as e:
        logger.debug("CacheModule: config protocol unavailable (%s), using defaults", e)

    from dynastore.modules.cache.cache_config import CachePluginConfig

    return CachePluginConfig()


async def _on_valkey_engine_config_change(
    config: Any,
    _old: Any,
    _ctx: Any,
    _conn: Any,
) -> None:
    """Apply handler for ValkeyEngineConfig — live reconnect on config change.

    Called by PlatformConfigService after a PATCH to the engine config.
    Sequence:
      1. Close + unregister the old backend.
      2. Evict the old engine instance from engine_cache.
      3. Re-get the engine (lazy re-init with new config).
      4. Build + probe + register the new backend.
    """
    global _current_backend, _app_state

    engine_cache: Optional[EngineInstanceCache] = getattr(
        _app_state, "engine_cache", None
    )
    if engine_cache is None:
        logger.warning(
            "ValkeyEngineConfig apply handler: no engine_cache on app_state; "
            "cannot reconnect. Config change will take effect on next restart."
        )
        return

    _t0 = asyncio.get_event_loop().time()
    async with _apply_lock:
        # 1. Close + unregister old backend.
        old_backend = _current_backend
        if old_backend is not None:
            try:
                from dynastore.tools.cache import get_cache_manager

                get_cache_manager().unregister_backend(old_backend)
            except Exception:
                logger.exception(
                    "ValkeyEngineConfig apply handler: unregister_backend failed"
                )
            try:
                await old_backend.close()
            except Exception:
                logger.exception(
                    "ValkeyEngineConfig apply handler: backend.close failed"
                )
            _current_backend = None

        # 2. Evict the old engine instance.
        try:
            await engine_cache.evict("valkey_engine")
        except Exception:
            logger.exception(
                "ValkeyEngineConfig apply handler: engine_cache.evict failed"
            )

        # 3. Re-get the engine (lazy re-init).
        try:
            client = await engine_cache.get("valkey_engine")
        except Exception as e:
            _dur_ms = int((asyncio.get_event_loop().time() - _t0) * 1000)
            logger.error(
                "ValkeyEngineConfig apply handler: failed to re-init engine (%s). "
                "Cache degrades to L1-only until next successful config apply.",
                e,
            )
            logger.info(
                "CACHE RECONNECT: success=false stage=engine_init "
                "duration_ms=%d error=%s",
                _dur_ms, type(e).__name__,
            )
            return

        # 4. Build + probe + register new backend.
        from dynastore.tools.cache_valkey import ValkeyCacheBackend
        from dynastore.tools.cache import _notify_backend_upgrade, get_cache_manager

        cache_cfg = await _load_cache_config()
        new_backend = ValkeyCacheBackend(
            client=client,
            owns_client=False,
            circuit_breaker_threshold=cache_cfg.circuit_breaker_threshold,
        )

        try:
            info = await asyncio.wait_for(
                new_backend.info(), timeout=cache_cfg.probe_timeout_seconds
            )
            version = info.get("server", {}).get("redis_version", "?")
            mode = info.get("server", {}).get("redis_mode", "standalone")
            logger.info(
                "CacheModule (reconnect): Valkey OK — version=%s mode=%s",
                version,
                mode,
            )
        except Exception as exc:
            _reason = (
                "probe timed out" if isinstance(exc, asyncio.TimeoutError) else str(exc)
            )
            _dur_ms = int((asyncio.get_event_loop().time() - _t0) * 1000)
            logger.error(
                "CacheModule (reconnect): Valkey probe failed (%s). "
                "Cache degrades to L1-only.",
                _reason,
            )
            logger.info(
                "CACHE RECONNECT: success=false stage=probe "
                "duration_ms=%d error=%s",
                _dur_ms, type(exc).__name__,
            )
            return

        get_cache_manager().register_backend(new_backend)
        _notify_backend_upgrade()
        _current_backend = new_backend
        _dur_ms = int((asyncio.get_event_loop().time() - _t0) * 1000)
        logger.info(
            "CACHE BACKEND: VALKEY (reconnected) — version=%s mode=%s", version, mode
        )
        logger.info(
            "CACHE RECONNECT: success=true version=%s mode=%s duration_ms=%d",
            version, mode, _dur_ms,
        )


class CacheModule(ModuleProtocol):
    """SCOPE-controlled module that wires Valkey as the shared cache backend.

    Priority 9 — starts after DBConfigModule (0) so ``app_state.engine_cache``
    is available, but before DBService (10) so the backend is registered
    before any module that uses ``@cached`` in its lifespan.
    """

    priority: int = 9

    def __init__(self, app_state: object) -> None:
        self.app_state = app_state

    @asynccontextmanager
    async def lifespan(self, app_state: object) -> AsyncGenerator[None, None]:
        global _current_backend, _app_state
        _app_state = app_state

        # Load cache-layer config (probe_timeout, circuit_breaker).
        cache_cfg = await _load_cache_config()

        # Try engine-driven mode first.
        engine_cache: Optional[EngineInstanceCache] = getattr(
            app_state, "engine_cache", None
        )
        backend = None
        client = None
        engine_mode = False
        _safe_url = "<engine>"

        if engine_cache is not None:
            # Graceful-skip when the ``module_cache`` extra isn't installed.
            from dynastore.tools.cache_valkey import _CACHE_DEPS_OK

            if not _CACHE_DEPS_OK:
                logger.warning(
                    "CACHE BACKEND: LOCAL (in-memory, per-instance) — "
                    "engine_cache present but 'module_cache' extra not in "
                    "SCOPE (msgpack/valkey not installed); skipping Valkey."
                )
                yield
                return

            try:
                client = await engine_cache.get("valkey_engine")
                engine_mode = True
            except KeyError:
                logger.info(
                    "CacheModule: valkey_engine not registered in engine_cache; "
                    "falling back to VALKEY_URL env."
                )
            except RuntimeError as e:
                if "disabled" in str(e).lower():
                    logger.info(
                        "CacheModule: valkey_engine is disabled; "
                        "falling back to VALKEY_URL env."
                    )
                else:
                    raise

        # Legacy fallback: env-driven mode.
        if not engine_mode:
            valkey_url = os.getenv("VALKEY_URL")
            if not valkey_url:
                logger.warning(
                    "CACHE BACKEND: LOCAL (in-memory, per-instance) — "
                    "VALKEY_URL not set; cross-instance consistency NOT guaranteed."
                )
                yield
                return

            from dynastore.tools.cache_valkey import _CACHE_DEPS_OK

            if not _CACHE_DEPS_OK:
                logger.warning(
                    "CACHE BACKEND: LOCAL (in-memory, per-instance) — VALKEY_URL "
                    "is set but the 'module_cache' extra is not in this "
                    "deployment's SCOPE (msgpack/valkey not installed); skipping "
                    "the Valkey backend."
                )
                yield
                return

            _safe_url = valkey_url.split("@")[-1] if "@" in valkey_url else valkey_url
            _tls = os.getenv("VALKEY_TLS", "").lower() in ("1", "true", "yes")
            _iam = os.getenv("VALKEY_IAM_AUTH", "").lower() in ("1", "true", "yes")
            _cluster = os.getenv("VALKEY_CLUSTER", "").lower() in ("1", "true", "yes")
            logger.info(
                "CacheModule (legacy): Connecting to Valkey at %s (tls=%s, iam_auth=%s, cluster=%s, probe_timeout=%ss) …",
                _safe_url,
                _tls,
                _iam,
                _cluster,
                cache_cfg.probe_timeout_seconds,
            )
            try:
                # Pull the connection-hardening defaults from ValkeyEngineConfig
                # so the bootstrap-fallback path is NOT a hole that re-opens the
                # un-hardened-socket regression that #720 / #724 closed for the
                # engine-driven mode (idle Cloud NAT drops + cold cluster-topology
                # fetch exceeding valkey-py's 5s hard default).
                from dynastore.modules.db_config.engine_config import (
                    ValkeyEngineConfig,
                )
                from dynastore.tools.cache_valkey import ValkeyCacheBackend

                _engine_defaults = ValkeyEngineConfig()
                backend = ValkeyCacheBackend(
                    url=valkey_url,
                    socket_connect_timeout=_engine_defaults.socket_connect_timeout_seconds,
                    socket_timeout=_engine_defaults.socket_timeout_seconds,
                    tcp_keepalive_idle=_engine_defaults.tcp_keepalive_idle_seconds,
                    tcp_keepalive_interval=_engine_defaults.tcp_keepalive_interval_seconds,
                    tcp_keepalive_count=_engine_defaults.tcp_keepalive_count,
                    circuit_breaker_threshold=cache_cfg.circuit_breaker_threshold,
                )
            except Exception as exc:
                logger.warning(
                    "CacheModule (legacy): Cannot initialise Valkey backend (%s) — falling back to local cache.",
                    exc,
                )
                logger.warning(
                    "CACHE BACKEND: LOCAL (in-memory, per-instance) — "
                    "Valkey unavailable; cross-instance consistency NOT guaranteed."
                )
                yield
                return

        # Engine-driven mode: wrap the pre-built client.
        if engine_mode and client is not None:
            from dynastore.tools.cache_valkey import ValkeyCacheBackend

            backend = ValkeyCacheBackend(
                client=client,
                owns_client=False,
                circuit_breaker_threshold=cache_cfg.circuit_breaker_threshold,
            )

        if backend is None:
            logger.warning(
                "CACHE BACKEND: LOCAL (in-memory, per-instance) — "
                "no Valkey backend constructed; cross-instance consistency NOT guaranteed."
            )
            yield
            return

        # Probe the backend.
        try:
            info = await asyncio.wait_for(
                backend.info(), timeout=cache_cfg.probe_timeout_seconds
            )
            version = info.get("server", {}).get("redis_version", "?")
            mode = info.get("server", {}).get("redis_mode", "standalone")
            used_mb = info.get("memory", {}).get("used_memory_human", "?")
            logger.info(
                "CacheModule: Valkey OK — version=%s mode=%s used_memory=%s host=%s",
                version,
                mode,
                used_mb,
                _safe_url,
            )
        except Exception as exc:
            _reason = (
                "probe timed out" if isinstance(exc, asyncio.TimeoutError) else str(exc)
            )
            logger.warning(
                "CacheModule: Valkey unreachable at %s (%s) — falling back to local cache.",
                _safe_url,
                _reason,
            )
            logger.warning(
                "CACHE BACKEND: LOCAL (in-memory, per-instance) — "
                "Valkey connection failed; cross-instance consistency NOT guaranteed."
            )
            await backend.close()
            yield
            return

        from dynastore.tools.cache import _notify_backend_upgrade, get_cache_manager

        get_cache_manager().register_backend(backend)
        _notify_backend_upgrade()
        _current_backend = backend
        logger.info(
            "CACHE BACKEND: VALKEY (shared, cross-instance, %s) — host=%s version=%s mode=%s used_memory=%s",
            "engine" if engine_mode else "legacy",
            _safe_url,
            version,
            mode,
            used_mb,
        )

        # Register the apply handler (engine mode only — live reconnect).
        if engine_mode:
            try:
                from dynastore.modules.db_config.engine_config import ValkeyEngineConfig

                ValkeyEngineConfig.register_apply_handler(
                    _on_valkey_engine_config_change
                )
            except Exception:
                logger.exception(
                    "CacheModule: failed to register ValkeyEngineConfig apply handler"
                )

        try:
            yield
        finally:
            if engine_mode:
                try:
                    from dynastore.modules.db_config.engine_config import (
                        ValkeyEngineConfig,
                    )

                    ValkeyEngineConfig.unregister_apply_handler(
                        _on_valkey_engine_config_change
                    )
                except Exception:
                    pass
            await backend.close()
            _current_backend = None
            logger.info("CacheModule: Valkey connection closed.")
