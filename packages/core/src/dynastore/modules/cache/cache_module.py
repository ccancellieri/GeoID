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
CacheModule — registers a shared Valkey cache backend.

Primary mode (engine-driven):
    Acquires the Valkey client from ``app_state.engine_cache`` (backed by
    ``ValkeyEngineConfig``).  Connection params (URL/TLS/IAM/cluster) are
    mutable via the configs API; changes trigger a live rebuild without
    restart.

    Config defaults are seeded from ``docker/config/defaults/valkey-engine-config.json``.
    Initial cluster detection (if no config exists yet) is automatic: the module probes
    with a standalone client first; if the server reports cluster mode, it rebuilds
    as ValkeyCluster transparently.

Legacy fallback (env-driven):
    When ``app_state.engine_cache`` is unavailable, falls back to reading
    ``VALKEY_URL`` from the environment (for backward compatibility).
    Cluster detection still works: a standalone client probes the server,
    detects cluster mode from the INFO response, and rebuilds as ValkeyCluster
    if needed. Note: ``VALKEY_CLUSTER``, ``VALKEY_TLS``, ``VALKEY_IAM_AUTH``,
    and ``VALKEY_TLS_CA_PATH`` env vars are deprecated; use ``ValkeyEngineConfig``
    (via the configs API) for live-reconfig instead.

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
from dynastore.tools.async_utils import LoopLocalLock

if TYPE_CHECKING:
    from dynastore.modules.cache.cache_config import CachePluginConfig
    from dynastore.modules.db_config.engine_instance_cache import EngineInstanceCache

logger = logging.getLogger(__name__)

# Module-level state for the apply-handler closure.
# Set during CacheModule.lifespan; used by _on_valkey_engine_config_change.
_current_backend: Optional[Any] = None
_app_state: Optional[Any] = None
_apply_lock: LoopLocalLock = LoopLocalLock()

# Bounded LocalAsyncCacheBackend fallback log (#629).
# The "CACHE BACKEND: LOCAL" fall-back path is hit on every cold start when
# Valkey is unreachable AND on every reconnect-attempt failure that the
# circuit breaker trips.  Sustained Valkey unavailability would otherwise
# spam INFO/WARNING lines once per worker per request cycle.  Mirroring the
# bounded-log pattern from ``routed_resolver._FALLBACK_WARNED``: log INFO
# on the first occurrence per process, demote subsequent occurrences to
# DEBUG.  Reset only on a successful Valkey backend registration so a
# legitimate re-degrade after a flap re-emits at INFO.
_LOCAL_FALLBACK_LOGGED: bool = False

# Default for the legacy env-driven fallback path's connect timeout when
# ``VALKEY_SOCKET_CONNECT_TIMEOUT`` is unset (#629).  Picked at 10s — more
# generous than the engine-driven default (3s) because the legacy path is
# the cold-boot fallback when no engine snapshot is wired, which is exactly
# when discovery latency spikes (cluster topology fetch, IAM token mint,
# TLS handshake) tend to stack up.  A short timeout there cascades to
# LocalAsyncCacheBackend fallback → distributed-lock contention → DB pool
# stall (see #629).
_VALKEY_SOCKET_CONNECT_TIMEOUT_DEFAULT: float = 10.0


def _resolve_socket_connect_timeout(engine_default: float) -> float:
    """Resolve the legacy-fallback socket-connect timeout.

    Precedence:
      1. ``VALKEY_SOCKET_CONNECT_TIMEOUT`` env var (operators tune review env).
      2. ``engine_default`` (``ValkeyEngineConfig().socket_connect_timeout_seconds``)
         when it differs from the package default of 3.0s — operators may
         already be using the engine-config knob.
      3. ``_VALKEY_SOCKET_CONNECT_TIMEOUT_DEFAULT`` (10s) — see #629
         rationale on why we raise the floor for the legacy fallback path.

    Invalid env-var values fall through to the default and emit a WARNING
    so the operator can see their tuning was rejected (rare).
    """
    raw = os.getenv("VALKEY_SOCKET_CONNECT_TIMEOUT")
    if raw is not None and raw.strip():
        try:
            return float(raw)
        except ValueError:
            logger.warning(
                "VALKEY_SOCKET_CONNECT_TIMEOUT=%r is not a number; "
                "using default %.1fs",
                raw, _VALKEY_SOCKET_CONNECT_TIMEOUT_DEFAULT,
            )
    # Engine default may have been tuned via ValkeyEngineConfig already
    # (3.0s on the unmodified field default).  Prefer the larger of (engine
    # default, 10s floor) so existing engine-config tuning is never silently
    # tightened by this fallback.
    return max(engine_default, _VALKEY_SOCKET_CONNECT_TIMEOUT_DEFAULT)


def _log_local_fallback(message: str, *args: Any) -> None:
    """Log a ``CACHE BACKEND: LOCAL`` fallback line with first-time INFO,
    subsequent DEBUG (#629).

    Bounded so sustained Valkey unavailability does not flood logs while
    preserving operator visibility of the first transition.  WARNING is
    deliberately not used here (the cache-degrades-to-L1 path is a
    designed degraded mode, not a hard error — same rationale as the
    ``routed_resolver`` fallback log promotion).
    """
    global _LOCAL_FALLBACK_LOGGED
    if not _LOCAL_FALLBACK_LOGGED:
        _LOCAL_FALLBACK_LOGGED = True
        logger.info(message + " (further occurrences in this process logged at DEBUG)", *args)
    else:
        logger.debug(message, *args)


async def _load_cache_config() -> "CachePluginConfig":
    """Load cache config from PluginConfig protocol.

    Falls back to defaults if config is missing or protocol unavailable
    (e.g., during early bootstrap before ConfigProtocol is registered).
    """
    try:
        from dynastore.modules.cache.cache_config import CachePluginConfig
        from dynastore.models.protocols.configs import ConfigsProtocol

        try:
            from dynastore.tools.discovery import get_protocol

            configs_proto = get_protocol(ConfigsProtocol)
        except Exception as e:
            logger.debug(
                "CacheModule: ConfigsProtocol not available yet (%s), using defaults", e
            )
            return CachePluginConfig()

        if configs_proto is None:
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
    _catalog_id: Any,
    _collection_id: Any,
    _conn: Any,
) -> None:
    """Apply handler for ValkeyEngineConfig — live reconnect on config change.

    Called by PlatformConfigService after a PATCH to the engine config.
    Sequence:
      1. Close + unregister the old backend.
      2. Push the fresh ``config`` into the engine_cache snapshot + evict
         the cached instance (#827).
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

        # 2. Push the fresh config into the snapshot + evict the cached
        # instance.  Without the snapshot swap, the next ``get`` would
        # rebuild the client against the stale boot-time config (#827).
        try:
            await engine_cache.update_config(config)
        except Exception:
            logger.exception(
                "ValkeyEngineConfig apply handler: engine_cache.update_config failed"
            )

        # 3. Re-get the engine (lazy re-init with the freshly-stamped config).
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
        # Re-arm the bounded fallback log so the next degrade-to-LOCAL
        # cycle (if any) re-emits at INFO. #629
        global _LOCAL_FALLBACK_LOGGED
        _LOCAL_FALLBACK_LOGGED = False
        _dur_ms = int((asyncio.get_event_loop().time() - _t0) * 1000)
        logger.info(
            "CACHE BACKEND: VALKEY (reconnected) — version=%s mode=%s", version, mode
        )
        logger.info(
            "CACHE RECONNECT: success=true version=%s mode=%s duration_ms=%d",
            version, mode, _dur_ms,
        )


async def _on_cache_plugin_config_change(
    config: Any,
    _catalog_id: Any,
    _collection_id: Any,
    _conn: Any,
) -> None:
    """Apply handler for ``CachePluginConfig`` — live re-apply circuit
    breaker threshold on the current Valkey backend.

    ``ValkeyCacheBackend`` captures ``circuit_breaker_threshold`` at
    construction (``cache_valkey.py:555``) and never re-reads it.
    Without this handler a ``PUT /configs?plugin_id=cache_plugin_config``
    that bumps the threshold silently no-ops until the next engine
    reconnect (which is what would rebuild the backend through
    ``_on_valkey_engine_config_change``).  That is exactly the
    "read-once, never re-applied" failure mode #756 describes.

    Other fields on ``CachePluginConfig``:

    * ``probe_timeout_seconds`` — only consumed during (re)connect via
      ``_load_cache_config()``; the next reconnect already picks up
      the new value, no live update needed.
    * ``oracle_inner_timeout_seconds`` — hot-read per dispatch in
      ``modules/tasks/dispatcher.py`` (calls ``configs_proto.get_config
      (CachePluginConfig)`` each time), no live update needed.

    So this handler only has to push the threshold onto the live
    backend.  Safe to no-op when ``_current_backend`` is ``None``
    (e.g. cache degraded to L1-only) — the next reconnect will pick
    up the new value via ``_load_cache_config()``.
    """
    global _current_backend

    backend = _current_backend
    if backend is None:
        logger.debug(
            "CachePluginConfig apply handler: no live backend; "
            "new threshold=%s will take effect on next backend build.",
            getattr(config, "circuit_breaker_threshold", "<unset>"),
        )
        return

    new_threshold = getattr(config, "circuit_breaker_threshold", None)
    if new_threshold is None:
        return

    # Set the attribute on whatever backend type is live — the test
    # double + ``ValkeyCacheBackend`` both expose it as ``_circuit_breaker_threshold``.
    try:
        setattr(backend, "_circuit_breaker_threshold", int(new_threshold))
        logger.info(
            "CachePluginConfig: circuit_breaker_threshold live-applied = %d",
            int(new_threshold),
        )
    except Exception:
        logger.exception(
            "CachePluginConfig apply handler: failed to update "
            "_circuit_breaker_threshold on live backend (%s)",
            type(backend).__name__,
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

        # GeoID #833: DBConfigModule (priority 0) fires the engine-snapshot
        # population as a fire-and-forget asyncio.Task, so when CacheModule
        # (priority 9) starts the engine_cache object exists but its
        # snapshot dict is still empty.  Awaiting the published task handle
        # bridges that race — without this, engine_cache.get raises KeyError
        # and we fall into the legacy VALKEY_URL/VALKEY_CLUSTER env fallback
        # path against whatever topology the deployment env declares
        # (mis-matched topology = circuit-breaker trip every cold start).
        # ``getattr`` keeps back-compat with test stubs that pre-date #833.
        refresh_task = getattr(app_state, "engine_snapshot_refresh_task", None)
        if refresh_task is not None:
            # Awaiting a completed task is cheap (immediate return/raise) so
            # we do NOT gate on ``not done()`` — a TOCTOU race could otherwise
            # let a task that completed-with-exception slip past unobserved.
            try:
                await refresh_task
            except (asyncio.CancelledError, Exception) as exc:  # noqa: BLE001
                # Failed/cancelled refresh is non-fatal here — the engine_cache
                # read below will simply KeyError and the legacy fallback
                # path (now correctly matching env topology after the
                # apps.review.yml fix) takes over.  Logged at WARNING so
                # operators can spot the boot-order regression.
                logger.warning(
                    "CacheModule: engine snapshot refresh task did not complete "
                    "cleanly (%s); proceeding with whatever the snapshot has.",
                    exc,
                )

        if engine_cache is not None:
            # Graceful-skip when the ``module_cache`` extra isn't installed.
            from dynastore.tools.cache_valkey import _CACHE_DEPS_OK

            if not _CACHE_DEPS_OK:
                _log_local_fallback(
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
            except ValueError as e:
                # ``ValkeyEngineConfig.engine_init`` -> ``build_valkey_client``
                # raises ``ValueError`` when neither ``connection_url`` nor a
                # ``discovery_host`` is configured (e.g. empty defaults in
                # integration tests, notebooks, demos, fresh installs).
                # Treat it like the "disabled" RuntimeError path: fall through
                # to the legacy env-driven fallback (which itself falls back
                # to LOCAL in-memory cache when ``VALKEY_URL`` is also unset).
                # WARNING level so misconfigured production deployments are
                # still visible in logs without aborting the whole lifespan.
                logger.warning(
                    "CacheModule: valkey_engine misconfigured (%s); "
                    "falling back to VALKEY_URL env. If this is production, "
                    "set VALKEY_URL or VALKEY_DISCOVERY_HOST to restore the "
                    "Valkey backend.",
                    e,
                )

        # Legacy fallback: env-driven mode.
        if not engine_mode:
            valkey_url = os.getenv("VALKEY_URL")
            if not valkey_url:
                _log_local_fallback(
                    "CACHE BACKEND: LOCAL (in-memory, per-instance) — "
                    "VALKEY_URL not set; cross-instance consistency NOT guaranteed."
                )
                yield
                return

            from dynastore.tools.cache_valkey import _CACHE_DEPS_OK

            if not _CACHE_DEPS_OK:
                _log_local_fallback(
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
            # Pull the connection-hardening defaults from ValkeyEngineConfig
            # so the bootstrap-fallback path is NOT a hole that re-opens the
            # un-hardened-socket regression that #720 / #724 closed for the
            # engine-driven mode (idle Cloud NAT drops + cold cluster-topology
            # fetch exceeding valkey-py's 5s hard default).
            from dynastore.modules.db_config.engine_config import (
                ValkeyEngineConfig,
            )

            _engine_defaults = ValkeyEngineConfig()
            _socket_connect_timeout = _resolve_socket_connect_timeout(
                _engine_defaults.socket_connect_timeout_seconds
            )
            logger.info(
                "CacheModule (legacy): Connecting to Valkey at %s (tls=%s, iam_auth=%s, cluster=%s, probe_timeout=%ss, socket_connect_timeout=%ss) …",
                _safe_url,
                _tls,
                _iam,
                _cluster,
                cache_cfg.probe_timeout_seconds,
                _socket_connect_timeout,
            )
            try:
                from dynastore.tools.cache_valkey import ValkeyCacheBackend

                backend = ValkeyCacheBackend(
                    url=valkey_url,
                    socket_connect_timeout=_socket_connect_timeout,
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
                _log_local_fallback(
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
            _log_local_fallback(
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
            
            # Auto-detect cluster mode: if server reports cluster but we
            # connected with a standalone client, rebuild as ValkeyCluster.
            if mode == "cluster" and not engine_mode:
                logger.info(
                    "CacheModule: detected cluster mode; rebuilding as ValkeyCluster"
                )
                await backend.close()
                try:
                    from dynastore.tools.cache_valkey import ValkeyCacheBackend
                    backend = ValkeyCacheBackend(
                        url=valkey_url,
                        socket_connect_timeout=_socket_connect_timeout,
                        socket_timeout=_engine_defaults.socket_timeout_seconds,
                        tcp_keepalive_idle=_engine_defaults.tcp_keepalive_idle_seconds,
                        tcp_keepalive_interval=_engine_defaults.tcp_keepalive_interval_seconds,
                        tcp_keepalive_count=_engine_defaults.tcp_keepalive_count,
                        circuit_breaker_threshold=cache_cfg.circuit_breaker_threshold,
                        cluster_mode=True,
                    )
                except Exception as exc:
                    logger.warning(
                        "CacheModule: failed to rebuild as ValkeyCluster (%s); using standalone",
                        exc,
                    )
                    from dynastore.tools.cache_valkey import ValkeyCacheBackend
                    backend = ValkeyCacheBackend(
                        url=valkey_url,
                        socket_connect_timeout=_socket_connect_timeout,
                        socket_timeout=_engine_defaults.socket_timeout_seconds,
                        tcp_keepalive_idle=_engine_defaults.tcp_keepalive_idle_seconds,
                        tcp_keepalive_interval=_engine_defaults.tcp_keepalive_interval_seconds,
                        tcp_keepalive_count=_engine_defaults.tcp_keepalive_count,
                        circuit_breaker_threshold=cache_cfg.circuit_breaker_threshold,
                        cluster_mode=False,
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
            _log_local_fallback(
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
        # Re-arm the bounded fallback log so a later re-degrade after a
        # successful flap re-emits at INFO (instead of staying suppressed
        # at DEBUG for the rest of the process lifetime). #629
        global _LOCAL_FALLBACK_LOGGED
        _LOCAL_FALLBACK_LOGGED = False
        logger.info(
            "CACHE BACKEND: VALKEY (shared, cross-instance, %s) — host=%s version=%s mode=%s used_memory=%s",
            "engine" if engine_mode else "legacy",
            _safe_url,
            version,
            mode,
            used_mb,
        )

        # Register the apply handler unconditionally so a later
        # PUT /configs/plugins/valkey_engine_config can trigger a live
        # reconnect even when the boot snapshot fell back to the legacy
        # env-driven path (DBService not yet up when DBConfigModule built
        # its snapshot — see #818).  The handler is null-safe wrt the
        # backend type: it closes whatever ``_current_backend`` is and
        # then re-gets the engine, which by post-boot wait-and-retry will
        # have been populated by ``refresh_snapshot_until_ready``.
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
            from dynastore.modules.cache.cache_config import CachePluginConfig

            CachePluginConfig.register_apply_handler(
                _on_cache_plugin_config_change
            )
        except Exception:
            logger.exception(
                "CacheModule: failed to register CachePluginConfig apply handler"
            )

        try:
            yield
        finally:
            try:
                from dynastore.modules.db_config.engine_config import (
                    ValkeyEngineConfig,
                )

                ValkeyEngineConfig.unregister_apply_handler(
                    _on_valkey_engine_config_change
                )
            except Exception:
                pass
            try:
                from dynastore.modules.cache.cache_config import (
                    CachePluginConfig,
                )

                CachePluginConfig.unregister_apply_handler(
                    _on_cache_plugin_config_change
                )
            except Exception:
                pass
            await backend.close()
            _current_backend = None
            logger.info("CacheModule: Valkey connection closed.")
