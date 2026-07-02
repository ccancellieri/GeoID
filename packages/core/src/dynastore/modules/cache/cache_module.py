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

Engine-driven (the only supported path):
    Acquires the Valkey client from ``app_state.engine_cache`` (backed by
    ``ValkeyEngineConfig``).  Connection params (URL/TLS/IAM/cluster) are
    mutable via the configs API; changes trigger a live rebuild without
    restart.

    Config defaults are seeded from ``docker/config/defaults/valkey-engine-config.json``.
    Initial cluster detection (if no config exists yet) is automatic: the module probes
    with the engine-built client first; if the server reports cluster mode but the
    client is standalone, it rebuilds a dedicated cluster-mode client transparently
    and logs a WARNING that the stored ``cluster_mode`` config is misconfigured.

    When ``app_state.engine_cache`` is absent, or the engine has no connection
    configured (no ``VALKEY_URL``/``connection_url``/``discovery_host``), or the
    ``module_cache`` extra isn't installed, the module degrades to
    ``LocalAsyncCacheBackend`` (in-memory, per-instance) rather than crashing the
    lifespan.

Cache-layer settings (probe_timeout, circuit_breaker) remain on
``CachePluginConfig``.

Add ``module_cache`` to the deployment scope extras to activate::

    scope_catalog = ["dynastore[...,module_cache]"]
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncGenerator, Optional

from dynastore.modules.protocols import ModuleProtocol
from dynastore.tools.async_utils import LoopLocalLock

if TYPE_CHECKING:
    from dynastore.modules.cache.cache_config import CachePluginConfig
    from dynastore.modules.db_config.engine_config import ValkeyEngineConfig
    from dynastore.modules.db_config.engine_instance_cache import EngineInstanceCache

logger = logging.getLogger(__name__)

# Module-level state for the apply-handler closure.
# Set during CacheModule.lifespan; used by _on_valkey_engine_config_change.
_current_backend: Optional[Any] = None
_app_state: Optional[Any] = None
_apply_lock: LoopLocalLock = LoopLocalLock()

# Circuit-breaker recovery loop (#2741). ``register_backend`` is otherwise
# only ever called at startup or from an explicit config-PATCH apply
# handler, so a mid-life circuit trip (``ValkeyCacheBackend._record_failure``
# unregistering itself after 3 consecutive failures) would otherwise never
# recover — the pod stays on L1-only cache (and IAM denylist checks fail
# open) until it restarts. Tracks the single in-flight recovery task so a
# burst of trips schedules at most one loop.
_recovery_task: "Optional[asyncio.Task[None]]" = None
_CB_RECOVERY_INITIAL_DELAY: float = 5.0
_CB_RECOVERY_MAX_DELAY: float = 60.0

# Sentinel default for ``_on_valkey_engine_config_change``'s ``config`` arg.
# The boot-order upgrade path (``_boot_upgrade_to_valkey``) calls the handler
# with no config to say "keep whatever the engine snapshot already resolved,
# just (re)build the backend" — distinct from a real config-change apply
# (which pushes the new config into the snapshot) and from an explicit
# ``None`` config, which callers/tests may still pass through step 2.
_KEEP_SNAPSHOT_CONFIG: Any = object()

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


async def _load_valkey_engine_config() -> "ValkeyEngineConfig":
    """Load the live ``ValkeyEngineConfig`` snapshot from the PluginConfig protocol.

    Mirrors ``_load_cache_config`` above.  Used by the shared cluster
    auto-detect/correct helper (:func:`_detect_and_correct_cluster_mismatch`)
    to obtain the connection params that produced the already-built engine
    client, so the cluster-mode rebuild can reuse
    ``ValkeyEngineConfig.engine_init()`` instead of re-deriving
    URL/TLS/IAM/discovery resolution here.
    """
    from dynastore.modules.db_config.engine_config import ValkeyEngineConfig

    try:
        from dynastore.models.protocols.configs import ConfigsProtocol

        try:
            from dynastore.tools.discovery import get_protocol

            configs_proto = get_protocol(ConfigsProtocol)
        except Exception as e:
            logger.debug(
                "CacheModule: ConfigsProtocol not available for cluster "
                "auto-detect rebuild (%s), using engine defaults", e
            )
            return ValkeyEngineConfig()

        if configs_proto is None:
            return ValkeyEngineConfig()

        try:
            cfg = await configs_proto.get_config(ValkeyEngineConfig)
            if cfg:
                return cfg
        except Exception as e:
            logger.debug(
                "CacheModule: failed to load ValkeyEngineConfig for cluster "
                "auto-detect rebuild (%s), using engine defaults", e
            )

    except Exception as e:
        logger.debug(
            "CacheModule: config protocol unavailable for cluster "
            "auto-detect rebuild (%s), using engine defaults", e
        )

    return ValkeyEngineConfig()


async def _detect_and_correct_cluster_mismatch(
    *,
    server_redis_mode: Optional[str],
    client_is_cluster: bool,
    cache_cfg: "CachePluginConfig",
    engine_cache: "Optional[EngineInstanceCache]",
) -> Optional[Any]:
    """Shared cluster-mode mismatch detector/corrector.

    If the connected server reports cluster mode but the currently probed
    client is standalone, the stored ``ValkeyEngineConfig.cluster_mode`` is
    misconfigured. Config stays the SSOT — an operator WARNING points at
    the fix — this only builds a same-process, in-memory corrected client
    so the cache routes correctly in the meantime, mirroring
    ``ValkeyEngineConfig.engine_init()`` with a local
    ``cluster_mode=True`` override rather than persisting the correction.

    Called from both the cold-boot probe (``CacheModule.lifespan``) and
    the live reconnect path (``_on_valkey_engine_config_change`` — which
    backs both a config-PATCH apply and the circuit-breaker recovery
    loop). Without this, a pod that boot-corrected a misconfiguration
    would silently regress to a standalone client on its next reconnect
    (e.g. after a transient circuit-breaker trip): the recovery path used
    to rebuild straight from the still-misconfigured stored config,
    undoing the correction and routing a rebuilt cluster's traffic through
    a standalone client — misrouting plus an endless trip/recover cycle,
    logged as a plain "recovered".

    Returns the corrected backend, or ``None`` when there is no mismatch
    or the correction attempt failed (caller keeps using what it already
    has). On success, evicts the superseded standalone entry from
    ``engine_cache`` (best-effort) so its connection pool does not idle
    until process shutdown (#2743).
    """
    if not (server_redis_mode == "cluster" and not client_is_cluster):
        return None

    logger.warning(
        "CacheModule: server reports cluster mode but "
        "ValkeyEngineConfig.cluster_mode=False built a "
        "standalone client — the stored engine config is "
        "misconfigured. Correct it via PATCH "
        "/configs/plugins/valkey_engine_config "
        "(cluster_mode=true). Rebuilding a cluster-mode client "
        "for this process in the meantime."
    )
    try:
        from dynastore.tools.cache_valkey import ValkeyCacheBackend

        valkey_cfg = await _load_valkey_engine_config()
        cluster_cfg = valkey_cfg.model_copy(update={"cluster_mode": True})
        new_client = await cluster_cfg.engine_init()
        new_backend = ValkeyCacheBackend(
            client=new_client,
            owns_client=True,
            circuit_breaker_threshold=cache_cfg.circuit_breaker_threshold,
            on_trip=_on_backend_trip,
        )
    except Exception as exc:
        logger.warning(
            "CacheModule: failed to rebuild a cluster-mode "
            "client (%s); continuing with the standalone "
            "client — commands may misroute until the engine "
            "config is corrected.",
            exc,
        )
        return None

    if engine_cache is not None:
        try:
            await engine_cache.evict("valkey_engine")
        except Exception:
            logger.warning(
                "CacheModule: failed to evict the superseded "
                "standalone Valkey client after cluster-mode "
                "rebuild; it will stay idle until process "
                "shutdown.",
                exc_info=True,
            )
    return new_backend


# Sentinel default for ``_on_valkey_engine_config_change``'s ``guard_current``
# kwarg — "no guard, proceed unconditionally" (the normal config-PATCH apply
# and boot-upgrade paths). The circuit-breaker recovery loop passes the
# backend instance it observed at wake-up so the re-check happens atomically
# with the teardown decision, once ``_apply_lock`` is held (#2741 finding 2).
_NO_GUARD: Any = object()


async def _on_valkey_engine_config_change(
    config: Any = _KEEP_SNAPSHOT_CONFIG,
    _catalog_id: Any = None,
    _collection_id: Any = None,
    _conn: Any = None,
    *,
    guard_current: Any = _NO_GUARD,
) -> None:
    """Apply handler for ValkeyEngineConfig — live reconnect on config change.

    Called by PlatformConfigService after a PATCH to the engine config.
    Sequence:
      1. Close + unregister the old backend.
      2. Push the fresh ``config`` into the engine_cache snapshot + evict
         the cached instance (#827).
      3. Re-get the engine (lazy re-init with new config).
      4. Build + probe + register the new backend, correcting a
         cluster-mode mismatch if the server disagrees with the client
         (:func:`_detect_and_correct_cluster_mismatch`).

    Called with no ``config`` (``_KEEP_SNAPSHOT_CONFIG``) on the boot-order
    upgrade path (driven by ``_boot_upgrade_to_valkey``) and the
    circuit-breaker recovery loop (``_recover_after_circuit_trip``): step 2
    is skipped so the engine snapshot already populated post-pool by
    ``refresh_snapshot_until_ready`` is kept as-is, and only the backend is
    (re)built, probed, and registered.

    ``guard_current`` (#2741 finding 2): when set, the reconnect is
    aborted — before touching anything — unless ``_current_backend`` is
    still identically the instance the caller observed before deciding to
    reconnect. The recovery loop passes the tripped backend it woke up to
    replace; without this, a concurrent config-PATCH apply that wins the
    race to ``_apply_lock`` first would install a healthy backend, and the
    recovery loop's own (now-redundant) call would tear that healthy
    backend down and rebuild it again — an unregister gap that is an
    avoidable extra IAM fail-open blip. Checked *inside* the lock so the
    check is atomic with the teardown decision, not a racy check-then-act.
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
        if guard_current is not _NO_GUARD and _current_backend is not guard_current:
            logger.debug(
                "ValkeyEngineConfig apply handler: _current_backend already "
                "changed since this reconnect attempt was scheduled (a "
                "concurrent apply won the race); skipping."
            )
            return

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
        # Skipped on the boot-order upgrade path (``_KEEP_SNAPSHOT_CONFIG``):
        # the snapshot already holds the seeded config once the pool is up.
        if config is not _KEEP_SNAPSHOT_CONFIG:
            try:
                await engine_cache.update_config(config)
            except Exception:
                logger.exception(
                    "ValkeyEngineConfig apply handler: engine_cache.update_config failed"
                )

        # 3. Re-get the engine (lazy re-init with the freshly-stamped config).
        try:
            client = await engine_cache.get("valkey_engine")
            # ``build_valkey_client`` stashes the resolved host:port/discovery
            # endpoint on the client so the reconnect banner below reports the
            # actual endpoint instead of omitting it (#2812 follow-up — the
            # reconnect line used to log no host at all, which hid the
            # endpoint drift that caused the outage for days).
            _target = getattr(client, "_ds_resolved_target", "<engine>")
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
            on_trip=_on_backend_trip,
        )

        try:
            info = await asyncio.wait_for(
                new_backend.info(), timeout=cache_cfg.probe_timeout_seconds
            )
            version = info.get("server", {}).get("redis_version", "?")
            # ``redis_mode`` is the *server node's* self-view and may be
            # absent in the parsed INFO (then the literal default below would
            # lie). Prefer the *client's* discovered topology as ground truth
            # for whether THIS connection is sharding across a cluster.
            topo = await new_backend.topology()
            if topo.get("is_cluster"):
                mode = "cluster"
            else:
                mode = info.get("server", {}).get("redis_mode") or "standalone"
            logger.info(
                "CacheModule (reconnect): Valkey OK — host=%s version=%s mode=%s "
                "redis_mode=%s primaries=%d replicas=%d",
                _target,
                version,
                mode,
                info.get("server", {}).get("redis_mode", "<absent>"),
                topo.get("primaries", 0),
                topo.get("replicas", 0),
            )
            # Definitive client-side proof of which shard owns which slots.
            for r in topo.get("slots", []):
                logger.info(
                    "CacheModule (reconnect): cluster slot map — "
                    "slots %d-%d -> %s",
                    r["start"], r["end"], r["node"],
                )
            # Behavioural proof: actually round-trip a key into each shard and
            # confirm the IP is reachable (catches "node discovered but the VPC
            # can't reach it" — i.e. effectively single-shard).
            if topo.get("is_cluster"):
                try:
                    routing = await asyncio.wait_for(
                        new_backend.verify_routing(),
                        timeout=cache_cfg.probe_timeout_seconds,
                    )
                    for s in routing.get("shards", []):
                        if s.get("ok"):
                            logger.info(
                                "CacheModule (reconnect): shard reachable — "
                                "%s (slot %s)",
                                s["served_by"], s["slot"],
                            )
                        else:
                            logger.warning(
                                "CacheModule (reconnect): shard UNREACHABLE — "
                                "%s (slot %s) error=%s",
                                s["node"], s["slot"], s.get("error"),
                            )
                    logger.info(
                        "CacheModule (reconnect): routing verified — "
                        "distinct_ips_reached=%d/%d",
                        routing.get("distinct_ips_reached", 0),
                        topo.get("primaries", 0),
                    )
                except Exception as exc:  # never block reconnect on diagnostics
                    logger.warning(
                        "CacheModule (reconnect): routing verification skipped "
                        "(%s)", exc,
                    )
        except Exception as exc:
            _reason = (
                "probe timed out" if isinstance(exc, asyncio.TimeoutError) else str(exc)
            )
            _dur_ms = int((asyncio.get_event_loop().time() - _t0) * 1000)
            logger.error(
                "CacheModule (reconnect): Valkey probe failed at %s (%s). "
                "Cache degrades to L1-only.",
                _target,
                _reason,
            )
            logger.info(
                "CACHE RECONNECT: success=false stage=probe host=%s "
                "duration_ms=%d error=%s",
                _target, _dur_ms, type(exc).__name__,
            )
            return

        # Cluster-mode mismatch correction (#2741 finding 1) — shared with
        # the cold-boot probe (``CacheModule.lifespan``) so a boot-time
        # correction survives every later reconnect (config-PATCH apply or
        # circuit-breaker recovery) instead of silently reverting to the
        # still-misconfigured stored config.
        corrected = await _detect_and_correct_cluster_mismatch(
            server_redis_mode=info.get("server", {}).get("redis_mode"),
            client_is_cluster=topo.get("is_cluster", False),
            cache_cfg=cache_cfg,
            engine_cache=engine_cache,
        )
        if corrected is not None:
            new_backend = corrected

        get_cache_manager().register_backend(new_backend)
        _notify_backend_upgrade()
        _current_backend = new_backend
        # Re-arm the bounded fallback log so the next degrade-to-LOCAL
        # cycle (if any) re-emits at INFO. #629
        global _LOCAL_FALLBACK_LOGGED
        _LOCAL_FALLBACK_LOGGED = False
        _dur_ms = int((asyncio.get_event_loop().time() - _t0) * 1000)
        logger.info(
            "CACHE BACKEND: VALKEY (reconnected) — host=%s version=%s mode=%s",
            _target, version, mode,
        )
        logger.info(
            "CACHE RECONNECT: success=true host=%s version=%s mode=%s duration_ms=%d",
            _target, version, mode, _dur_ms,
        )


def _on_backend_trip(backend: Any) -> None:
    """Circuit-breaker trip callback (#2741), passed to every
    ``ValkeyCacheBackend`` this module constructs.

    Called synchronously by ``ValkeyCacheBackend._record_failure`` right
    after it unregisters itself from the ``CacheManager`` on 3 consecutive
    failures. Schedules a guarded background re-probe loop so a pod that
    lives for hours after a transient blip does not stay on L1-only cache
    (and IAM denylist checks fail open) for its whole lifetime. Idempotent:
    a trip while a recovery loop is already in flight is a no-op — the
    running loop re-probes on its own schedule regardless of which backend
    instance triggered it.
    """
    global _recovery_task
    if _recovery_task is not None and not _recovery_task.done():
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        logger.warning(
            "CacheModule: circuit breaker tripped but no running event loop; "
            "cannot schedule a recovery probe."
        )
        return
    _recovery_task = loop.create_task(
        _recover_after_circuit_trip(backend), name="cache-valkey-cb-recovery"
    )


async def _recover_after_circuit_trip(tripped_backend: Any) -> None:
    """Re-probe Valkey with exponential backoff until the backend recovers.

    Reuses ``_on_valkey_engine_config_change`` (no ``config`` arg — keeps
    the current engine snapshot, only rebuilds + probes + registers the
    backend, correcting a cluster-mode mismatch if present) so the
    recovery path exercises the exact same reconnect logic a config-PATCH
    apply would. Stops as soon as either this loop lands a fresh backend,
    or a concurrent reconnect (e.g. an operator's config PATCH) already
    replaced ``tripped_backend`` while this loop was sleeping. Runs until
    cancelled — a tripped pod may live for hours or days on Cloud Run, so
    the retry budget is unbounded, unlike the one-shot boot-order upgrade.

    The pre-sleep check below is a cheap fast path only (skip an
    already-known-superseded attempt without even touching the lock); the
    authoritative check is ``guard_current`` inside
    ``_on_valkey_engine_config_change``, re-evaluated atomically once
    ``_apply_lock`` is held (#2741 finding 2) — without it, a config-PATCH
    apply that wins the race to the lock right after this fast path passes
    would have its freshly-installed healthy backend torn down and rebuilt
    by this loop's own (now-redundant) reconnect, an avoidable extra IAM
    fail-open blip. The guard passed on each attempt is the ``_current_backend``
    *this loop itself just observed* (``tripped_backend`` on the first
    attempt; ``None`` on a later attempt once a prior failed reconnect left
    the cache degraded) — not always the original ``tripped_backend`` —
    so a concurrent actor is what trips the guard, never this loop's own
    prior attempts.
    """
    delay = _CB_RECOVERY_INITIAL_DELAY
    try:
        while True:
            await asyncio.sleep(delay)
            observed = _current_backend
            if observed is not None and observed is not tripped_backend:
                # Superseded by a concurrent reconnect while we waited.
                return
            try:
                await _on_valkey_engine_config_change(guard_current=observed)
            except Exception:
                logger.debug(
                    "CacheModule: circuit-breaker recovery probe failed",
                    exc_info=True,
                )
            if _current_backend is not None:
                logger.info(
                    "CacheModule: Valkey circuit breaker recovered — "
                    "backend re-registered."
                )
                return
            delay = min(delay * 2.0, _CB_RECOVERY_MAX_DELAY)
    finally:
        global _recovery_task
        _recovery_task = None


async def _cancel_recovery_task() -> None:
    """Cancel and await the in-flight circuit-breaker recovery loop, if any."""
    global _recovery_task
    task = _recovery_task
    if task is not None and not task.done():
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
    _recovery_task = None


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


# Bounded retry budget for the boot-order LOCAL -> Valkey upgrade.  Mirrors
# ``engine_resolver.refresh_snapshot_until_ready`` (30 attempts, 0.5s -> 5s
# exponential backoff) so the two boot-order retry loops stay in step.
_BOOT_UPGRADE_MAX_ATTEMPTS: int = 30
_BOOT_UPGRADE_INITIAL_DELAY: float = 0.5
_BOOT_UPGRADE_MAX_DELAY: float = 5.0


def _register_engine_apply_handlers() -> None:
    """Register the ValkeyEngineConfig + CachePluginConfig apply handlers.

    Registered even when boot fell back to LOCAL so a later config change
    (PATCH /configs/plugins/valkey_engine_config) drives a live connection
    rebuild via ``_on_valkey_engine_config_change``.
    """
    try:
        from dynastore.modules.db_config.engine_config import ValkeyEngineConfig

        ValkeyEngineConfig.register_apply_handler(_on_valkey_engine_config_change)
    except Exception:
        logger.exception(
            "CacheModule: failed to register ValkeyEngineConfig apply handler"
        )
    try:
        from dynastore.modules.cache.cache_config import CachePluginConfig

        CachePluginConfig.register_apply_handler(_on_cache_plugin_config_change)
    except Exception:
        logger.exception(
            "CacheModule: failed to register CachePluginConfig apply handler"
        )


def _unregister_engine_apply_handlers() -> None:
    """Best-effort unregister of both cache apply handlers on shutdown."""
    try:
        from dynastore.modules.db_config.engine_config import ValkeyEngineConfig

        ValkeyEngineConfig.unregister_apply_handler(_on_valkey_engine_config_change)
    except Exception:
        pass
    try:
        from dynastore.modules.cache.cache_config import CachePluginConfig

        CachePluginConfig.unregister_apply_handler(_on_cache_plugin_config_change)
    except Exception:
        pass


async def _boot_upgrade_to_valkey(engine_cache: "EngineInstanceCache") -> None:
    """Upgrade a boot-order LOCAL fallback to the shared Valkey backend.

    CacheModule (priority 9) initialises before DBService (priority 10)
    creates the pool and before TasksModule (priority 15) seeds
    ``valkey_engine_config``, so ``engine_cache.get('valkey_engine')`` can
    only resolve AFTER this module yields.  Poll with bounded exponential
    backoff and, on the first successful resolve, drive the existing
    config-apply reconnect (build + probe + register the Valkey backend and
    bump the ``@cached`` backend generation).  ``@cached`` consumers
    re-resolve their backend lazily via ``_notify_backend_upgrade`` so none
    of them need a restart.

    Without this, a boot-order degrade to LOCAL latches per-instance for the
    whole process lifetime — the snapshot refresh repopulates the engine
    cache moments later, but CacheModule never re-checks it.
    """
    delay = _BOOT_UPGRADE_INITIAL_DELAY
    for attempt in range(1, _BOOT_UPGRADE_MAX_ATTEMPTS + 1):
        if _current_backend is not None:
            # Already upgraded (e.g. via a concurrent config apply).
            return
        try:
            await engine_cache.get("valkey_engine")
        except Exception:
            await asyncio.sleep(delay)
            delay = min(delay * 2, _BOOT_UPGRADE_MAX_DELAY)
            continue
        # Engine resolvable now — reconnect using the current snapshot
        # (no config arg -> _KEEP_SNAPSHOT_CONFIG keeps the seeded config,
        # only builds the backend).
        await _on_valkey_engine_config_change()
        if _current_backend is not None:
            logger.info(
                "CacheModule: boot upgrade LOCAL -> VALKEY succeeded "
                "(attempt %d/%d).",
                attempt, _BOOT_UPGRADE_MAX_ATTEMPTS,
            )
            return
        # Engine resolved but the reconnect did not register a backend — the
        # Valkey server itself was transiently unreachable (still coming up,
        # brief network blip). That is distinct from "engine not yet
        # resolvable": keep retrying with the same backoff budget rather than
        # latching LOCAL on a momentary probe failure.
        logger.warning(
            "CacheModule: engine resolved but the Valkey reconnect did not "
            "register a backend (attempt %d/%d); retrying.",
            attempt, _BOOT_UPGRADE_MAX_ATTEMPTS,
        )
        await asyncio.sleep(delay)
        delay = min(delay * 2, _BOOT_UPGRADE_MAX_DELAY)
    logger.warning(
        "CacheModule: Valkey backend not established after %d attempts "
        "(engine snapshot never resolved, or the Valkey server stayed "
        "unreachable); cache stays LOCAL for this process. A later PATCH "
        "/configs/plugins/valkey_engine_config will still trigger a "
        "reconnect.",
        _BOOT_UPGRADE_MAX_ATTEMPTS,
    )


@asynccontextmanager
async def _degraded_local_lifespan(
    engine_cache: "Optional[EngineInstanceCache]",
) -> AsyncGenerator[None, None]:
    """Yield in LOCAL-cache mode while keeping a live path back to Valkey.

    Used by every boot-order degrade path that COULD later reach Valkey
    (i.e. the engine snapshot was simply not ready yet, or the boot probe
    failed transiently) — as opposed to the deps-missing path where Valkey
    is impossible.  Registers the apply handlers and spawns a bounded
    background upgrade task, then cleans both up on shutdown (closing the
    Valkey backend if the upgrade succeeded).
    """
    global _current_backend
    upgrade_task: Optional["asyncio.Task[None]"] = None
    handlers_registered = False
    if engine_cache is not None:
        _register_engine_apply_handlers()
        handlers_registered = True
        upgrade_task = asyncio.create_task(
            _boot_upgrade_to_valkey(engine_cache),
            name="cache-boot-upgrade-to-valkey",
        )
    try:
        yield
    finally:
        if upgrade_task is not None:
            upgrade_task.cancel()
            try:
                await upgrade_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        await _cancel_recovery_task()
        if handlers_registered:
            _unregister_engine_apply_handlers()
        if _current_backend is not None:
            try:
                await _current_backend.close()
            except Exception:
                logger.exception(
                    "CacheModule: closing upgraded Valkey backend failed"
                )
            _current_backend = None
            logger.info("CacheModule: Valkey connection closed.")


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
        # and the module degrades to the local in-memory cache even though
        # the engine snapshot would have resolved a moment later.
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
                # read below will simply KeyError and the local in-memory
                # fallback below takes over.  Logged at WARNING so operators
                # can spot the boot-order regression.
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
                    "degrading to local in-memory cache."
                )
            except RuntimeError as e:
                if "disabled" in str(e).lower():
                    logger.info(
                        "CacheModule: valkey_engine is disabled; "
                        "degrading to local in-memory cache."
                    )
                else:
                    raise
            except ValueError as e:
                # ``ValkeyEngineConfig.engine_init`` -> ``build_valkey_client``
                # raises ``ValueError`` when neither ``connection_url`` nor a
                # ``discovery_host`` is configured (e.g. empty defaults in
                # integration tests, notebooks, demos, fresh installs).
                # ``client`` stays None and the ``backend is None`` check
                # below degrades to the local in-memory cache.  WARNING
                # level so misconfigured production deployments are still
                # visible in logs without aborting the whole lifespan.
                logger.warning(
                    "CacheModule: valkey_engine misconfigured (%s); "
                    "degrading to local in-memory cache. If this is "
                    "production, set ValkeyEngineConfig.connection_url (or "
                    "VALKEY_URL / discovery_host) to restore the Valkey "
                    "backend.",
                    e,
                )

        # Engine-driven mode: wrap the pre-built client.
        if engine_mode and client is not None:
            from dynastore.tools.cache_valkey import ValkeyCacheBackend

            # ``build_valkey_client`` stashes the resolved host:port/discovery
            # endpoint on the client so the connect banner below reports the
            # actual endpoint rather than the "<engine>" placeholder (#2812
            # follow-up).
            _safe_url = getattr(client, "_ds_resolved_target", "<engine>")

            backend = ValkeyCacheBackend(
                client=client,
                owns_client=False,
                circuit_breaker_threshold=cache_cfg.circuit_breaker_threshold,
                on_trip=_on_backend_trip,
            )

        if backend is None:
            _log_local_fallback(
                "CACHE BACKEND: LOCAL (in-memory, per-instance) — "
                "no Valkey backend constructed; cross-instance consistency NOT guaranteed."
            )
            # Boot-order degrade: the engine snapshot resolves only after
            # DBService (10) + config seeding (15), i.e. after this module
            # yields.  Keep a live path back to Valkey instead of latching
            # LOCAL for the process lifetime.
            async with _degraded_local_lifespan(engine_cache):
                yield
            return

        # Probe the backend.
        try:
            info = await asyncio.wait_for(
                backend.info(), timeout=cache_cfg.probe_timeout_seconds
            )
            version = info.get("server", {}).get("redis_version", "?")
            used_mb = info.get("memory", {}).get("used_memory_human", "?")
            # ``redis_mode`` from INFO is the server node's self-view and may
            # be absent in the parsed dict; the client's discovered topology
            # is the ground truth for whether THIS connection is a cluster.
            topo = await backend.topology()
            server_redis_mode = info.get("server", {}).get("redis_mode")
            if topo.get("is_cluster"):
                mode = "cluster"
            else:
                mode = server_redis_mode or "standalone"
            logger.info(
                "CacheModule: Valkey OK — version=%s mode=%s redis_mode=%s "
                "primaries=%d replicas=%d used_memory=%s host=%s",
                version,
                mode,
                server_redis_mode or "<absent>",
                topo.get("primaries", 0),
                topo.get("replicas", 0),
                used_mb,
                _safe_url,
            )
            for r in topo.get("slots", []):
                logger.info(
                    "CacheModule: cluster slot map — slots %d-%d -> %s",
                    r["start"], r["end"], r["node"],
                )

            # Auto-detect cluster mode: if the server reports cluster but the
            # engine built a standalone client, the stored
            # ``ValkeyEngineConfig.cluster_mode`` is misconfigured.
            # ``_detect_and_correct_cluster_mismatch`` is shared with the
            # live reconnect path (config-PATCH apply / circuit-breaker
            # recovery, ``_on_valkey_engine_config_change``) so a
            # boot-time correction is not silently undone on a later
            # reconnect (#2741 finding 1).
            corrected = await _detect_and_correct_cluster_mismatch(
                server_redis_mode=server_redis_mode,
                client_is_cluster=topo.get("is_cluster", False),
                cache_cfg=cache_cfg,
                engine_cache=engine_cache,
            )
            if corrected is not None:
                backend = corrected
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
            # Transient boot probe failure: keep the apply handler live and
            # attempt one background reconnect rather than latching LOCAL.
            async with _degraded_local_lifespan(engine_cache):
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
            "CACHE BACKEND: VALKEY (shared, cross-instance, engine) — host=%s version=%s mode=%s used_memory=%s",
            _safe_url,
            version,
            mode,
            used_mb,
        )

        # Register the apply handler unconditionally so a later
        # PUT /configs/plugins/valkey_engine_config can trigger a live
        # reconnect even when the boot snapshot was built before
        # DBService came up (DBConfigModule populated an empty engine
        # snapshot — see #818).  The handler is null-safe wrt the
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
            await _cancel_recovery_task()
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
