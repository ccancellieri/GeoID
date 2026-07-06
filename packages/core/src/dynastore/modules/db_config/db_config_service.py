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

from __future__ import annotations

import asyncio
import functools
import logging
from contextlib import asynccontextmanager, AsyncExitStack
from typing import Any, Awaitable, Callable, Dict, Optional, Protocol, runtime_checkable

from dynastore.modules import ModuleProtocol
from dynastore.tools.background_service import BackgroundSupervisor, ServiceContext
from dynastore.tools.discovery import register_plugin, unregister_plugin

# Side-effect import — ensures the F.1 engine PluginConfig classes
# (PostgresqlEngineConfig, ElasticsearchEngineConfig, etc.) register in
# TypedModelRegistry at module-load time so the configs API surfaces
# them under platform.protocols.storage.*.  DBConfigModule loads at priority=0,
# so this is the earliest reliable trigger.
from . import engine_config as _engine_config  # noqa: F401
from .config_reload_config import ConfigReloadConfig
from .config_reload_service import ConfigReloadService
from .db_config import DBConfig
from .engine_instance_cache import EngineInstanceCache, EngineInstanceCacheSweepService
from .engine_resolver import (
    build_engine_snapshot,
    make_resolver,
    make_writer,
    refresh_snapshot_until_ready,
    try_refresh_snapshot_once,
)
from .platform_config_service import PlatformConfigService

logger = logging.getLogger(__name__)


@runtime_checkable
class DBConfigAppState(Protocol):
    db_config: Optional[DBConfig]
    engine: Any
    sync_engine: Any
    engine_cache: Optional[EngineInstanceCache]
    # Populated by a fire-and-forget retry task (DBConfigModule, priority 0)
    # so a long-lived resolver closure observes entries as they load.  This
    # handle used to be awaited by downstream priority-9 CacheModule before
    # reading engine_cache.get(...) (#833) — but that task cannot complete
    # before DBService (priority 10) installs the pool, and module lifespans
    # enter in strict priority order, so CacheModule awaiting it here always
    # burned its full retry budget on every boot and would deadlock startup
    # once #2908 made that budget an unbounded keep-alive retry instead of a
    # bounded give-up.  CacheModule now only ever inspects this task if it is
    # ALREADY done (never awaits a pending one); the LOCAL -> VALKEY
    # boot-upgrade loop (#2857) is what actually bridges the late-arriving
    # snapshot. See GeoID #833, #2908.
    engine_snapshot_refresh_task: "Optional[asyncio.Task[bool]]"
    # On-demand single-attempt re-resolver, bound to the same snapshot dict
    # and ``PlatformConfigService`` used above.  ``engine_snapshot_refresh_task``
    # degrades to a slow (``max_delay``-cadence) keep-alive retry once its
    # bounded budget is exhausted rather than giving up permanently (#2908),
    # but it is never awaited by CacheModule (see above) — the boot-upgrade
    # loop calls this instead, on its own faster cadence, to re-run
    # ``build_engine_snapshot`` itself rather than only re-probing a
    # snapshot dict the background task fills far too slowly for boot-time
    # use. See GeoID #2857.
    engine_snapshot_refresh: "Callable[[], Awaitable[bool]]"


class DBConfigModule(ModuleProtocol):
    priority: int = 0
    
    def __init__(self):
        logger.info("DBConfigModule: Initialized.")

    def get_config(self) -> DBConfig:
        """Returns the current database configuration."""
        cfg = self.app_state.db_config
        if cfg is None:
            raise RuntimeError("DBConfigModule.get_config() called before lifespan start")
        return cfg

    async def _build_engine_cache(
        self, pcfg: PlatformConfigService, app_state: "DBConfigAppState"
    ) -> tuple[
        EngineInstanceCache,
        "Optional[asyncio.Task[bool]]",
        BackgroundSupervisor,
        asyncio.Event,
        "Callable[[], Awaitable[bool]]",
    ]:
        """Snapshot platform engines + return (cache, refresh_task, supervisor,
        shutdown, on_demand_refresh).

        The initial ``build_engine_snapshot`` call runs synchronously, but at
        this point in lifespan ``DBService`` (priority 10) has not yet
        installed the connection pool, so every per-engine fetch fails with
        ``Cannot start managed_transaction: db_resource is None.`` and the
        snapshot returns empty.  Without recovery, ``EngineInstanceCache.get``
        would then raise ``KeyError`` forever on this process.

        To bridge the boot-order gap we hand the same snapshot dict to a
        background retry task (``refresh_snapshot_until_ready``); the
        resolver closure observes the dict by reference, so as soon as
        ``DBService`` brings the pool up the retry populates entries and
        every later ``engine_cache.get`` call resolves them.  The task is
        cancelled on lifespan teardown.

        See GeoID #818 for the regression context.

        The returned supervisor also carries ``ConfigReloadService`` (Layer A
        platform-config hot-reload watcher) alongside the engine-cache sweep —
        both are RUN_EVERYWHERE services with no leadership election, so
        sharing one supervisor/shutdown-event pair is correct.
        """
        from dynastore.modules.db_config.instance import get_service_name

        snapshot: Dict[str, Any] = {}
        await build_engine_snapshot(pcfg, into=snapshot)
        cache = EngineInstanceCache(
            engine_resolver=make_resolver(snapshot),
            engine_writer=make_writer(snapshot),
        )

        sweep_shutdown = asyncio.Event()
        supervisor = BackgroundSupervisor()
        supervisor.register(EngineInstanceCacheSweepService(cache))

        # Layer A config hot-reload watcher — read ConfigReloadConfig once at
        # boot (TasksModule-style; no env var) so operators can size the
        # reload interval or disable the watcher via the configs API. A
        # failure to read it (e.g. DB pool not yet up at this priority-0
        # lifespan point) falls back to enabled with the class default,
        # mirroring TasksModule's tolerant TasksPluginConfig load.
        reload_enabled = True
        reload_interval_seconds = 30.0
        try:
            reload_cfg = await pcfg.get_config(ConfigReloadConfig)
            if isinstance(reload_cfg, ConfigReloadConfig):
                reload_enabled = reload_cfg.enabled
                reload_interval_seconds = reload_cfg.reload_interval_seconds
        except Exception as e:
            logger.warning(
                f"DBConfigModule: Failed to load ConfigReloadConfig, "
                f"defaulting to enabled={reload_enabled}: {e}"
            )
        supervisor.register(
            ConfigReloadService(
                pcfg,
                enabled=reload_enabled,
                reload_interval_seconds=reload_interval_seconds,
            )
        )

        engine = (
            getattr(app_state, "engine", None)
            or getattr(app_state, "sync_engine", None)
        )
        sweep_ctx = ServiceContext(
            engine=engine,
            shutdown=sweep_shutdown,
            is_ephemeral=bool(getattr(app_state, "ephemeral_job", False)),
            name=get_service_name() or "unknown",
        )
        supervisor.start(sweep_ctx)

        refresh_task: "Optional[asyncio.Task[bool]]" = None
        if not snapshot:
            refresh_task = asyncio.create_task(
                refresh_snapshot_until_ready(snapshot, pcfg),
                name="engine_snapshot_refresh",
            )
        # Bound to this same ``snapshot`` dict + ``pcfg`` — a single
        # ``build_engine_snapshot`` attempt any later recovery loop can call
        # on its own cadence, reusing the one machinery that actually
        # populates the snapshot rather than re-probing a dict nothing else
        # will ever write to again once ``refresh_task`` exhausts. #2857
        on_demand_refresh = functools.partial(
            try_refresh_snapshot_once, snapshot, pcfg
        )
        return cache, refresh_task, supervisor, sweep_shutdown, on_demand_refresh

    @staticmethod
    async def _cancel_refresh_task(task: "asyncio.Task[bool]") -> None:
        """Cancel a still-running engine snapshot refresh task on teardown."""
        if task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass  # expected — we just cancelled it
        except Exception:
            logger.warning(
                "Engine snapshot refresh task errored during teardown", exc_info=True
            )

    @staticmethod
    async def _clear_refresh_task_ref(app_state: DBConfigAppState) -> None:
        """Drop the refresh-task reference from app_state on teardown."""
        if hasattr(app_state, "engine_snapshot_refresh_task"):
            app_state.engine_snapshot_refresh_task = None

    @staticmethod
    async def _teardown_sweep_supervisor(
        supervisor: BackgroundSupervisor, shutdown: asyncio.Event
    ) -> None:
        """Signal and drain this module's background supervisor (engine-cache
        sweep + the config hot-reload watcher)."""
        shutdown.set()
        await supervisor.stop()

    @staticmethod
    async def _teardown_engine_cache(app_state: DBConfigAppState) -> None:
        """Close the engine cache + drop the reference from app_state."""
        cache = getattr(app_state, "engine_cache", None)
        if cache is None:
            return
        try:
            await cache.close()
        finally:
            app_state.engine_cache = None

    @asynccontextmanager
    async def lifespan(self, app_state: DBConfigAppState):
        """Manages the configuration's presence in the app state during the app's lifecycle."""
        logger.info("DBConfigModule: Lifespan starting up.")
        self.app_state = app_state
        
        async with AsyncExitStack() as stack:
            # 1. Platform configuration
            app_state.db_config = DBConfig()
            # Make a dangerously-small connection pool LOUD and SAFE before any
            # engine is built from this config (dynastore #320).
            app_state.db_config.validate_pool_sizing()
            
            # 2. Platform Config Manager (Plugin)
            # Read engine directly from app_state to avoid circular dependency via get_engine(),
            # which would scan all DatabaseProtocol providers (including TilesModule) before
            # the DB engine has been set up.
            engine = (
                getattr(app_state, "engine", None)
                or getattr(app_state, "sync_engine", None)
            )
            
            pcfg = PlatformConfigService(engine=engine)
            await stack.enter_async_context(pcfg.lifespan(app_state))
            register_plugin(pcfg)
            stack.callback(unregister_plugin, pcfg)

            # 3. Engine instance cache (Cycle F.6) — snapshots platform-tier
            # engine configs at boot, exposes lazy-instantiating cache.
            # Until F.4c lands, no driver consumes this in production paths,
            # but admin tooling + tests use it via app_state.engine_cache.
            engine_cache, refresh_task, sweep_supervisor, sweep_shutdown, on_demand_refresh = (
                await self._build_engine_cache(pcfg, app_state)
            )
            app_state.engine_cache = engine_cache
            # Publish the task handle so downstream modules (e.g. CacheModule
            # at priority 9) can await snapshot completion before consulting
            # engine_cache.get(...) — #833.  None when the synchronous initial
            # build already populated the snapshot (no race to bridge).
            app_state.engine_snapshot_refresh_task = refresh_task
            # Publish the on-demand single-attempt refresher so a later
            # recovery loop (e.g. CacheModule's boot-upgrade loop) can
            # re-run build_engine_snapshot itself instead of only ever
            # re-probing a snapshot dict that the one-shot refresh_task
            # above stopped writing to once its own budget exhausted. #2857
            app_state.engine_snapshot_refresh = on_demand_refresh
            if refresh_task is not None:
                stack.push_async_callback(
                    self._cancel_refresh_task, refresh_task
                )
            stack.push_async_callback(self._clear_refresh_task_ref, app_state)
            # Stop the sweep supervisor before releasing cached instances.
            stack.push_async_callback(
                self._teardown_sweep_supervisor, sweep_supervisor, sweep_shutdown
            )
            stack.push_async_callback(self._teardown_engine_cache, app_state)

            yield

        # Stack callbacks fire on ``async with`` exit — clear ``db_config``
        # AFTER teardown so any engine_release impl reading DBConfig (or
        # the DSN derived from it) still sees a populated app_state.
        if hasattr(self.app_state, 'db_config'):
            self.app_state.db_config = None

        logger.info("DBConfigModule: Lifespan shutting down.")
