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
CatalogModule: Composition root for catalog, asset, and config management.

This module orchestrates the initialization and lifecycle of:
- CatalogService (Catalog and Collection CRUD)
- AssetManager (Asset lifecycle)
- ConfigManager (Hierarchical configurations)
- LogService (Buffered event logging)

It implements multiple protocols (CatalogsProtocol, AssetsProtocol, ConfigsProtocol, LogsProtocol, DatabaseProtocol)
by delegating to its internal services.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, FrozenSet, List, Optional, Any, Dict, Union, Set

if TYPE_CHECKING:
    from dynastore.models.shared_models import CatalogUpdate, CollectionUpdate
    from dynastore.modules.catalog.catalog_config import CollectionPluginConfig
    from geojson_pydantic import Feature
    from dynastore.models.query_builder import QueryResponse
    from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType
    from dynastore.modules.storage.hints import Hint

from dynastore.modules import ModuleProtocol
from dynastore.modules.db_config.query_executor import (
    managed_transaction,
    DDLQuery,
    DbResource,
)
from dynastore.modules.db_config.maintenance_tools import (
    ensure_schema_exists,
)
from dynastore.tools.protocol_helpers import get_engine
from dynastore.modules.catalog.models import (
    Catalog,
    Collection,
)
from dynastore.models.protocols import (
    CatalogsProtocol,
    ItemsProtocol,
    CollectionsProtocol,
    AssetsProtocol,
    ConfigsProtocol,
    DatabaseProtocol,
    LocalizationProtocol,
    LogsProtocol,
)
from dynastore.tools.discovery import register_plugin, unregister_plugin, get_protocol
from dynastore.modules.catalog.catalog_service import CatalogService
from dynastore.modules.catalog.collection_service import CollectionService
from dynastore.modules.catalog.item_service import ItemService
from dynastore.models.driver_context import DriverContext
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.catalog.config_service import ConfigService
from dynastore.modules.catalog.asset_service import AssetService, AssetEventType
from dynastore.modules.catalog.properties_service import PropertiesService
from dynastore.modules.catalog.localization_service import LocalizationService
from dynastore.modules.catalog.event_service import (
    EventService,
    CatalogEventType,
    register_event_listener,
    emit_event,
)
from dynastore.modules.catalog.log_manager import LogService

logger = logging.getLogger(__name__)


def _register_cascade_owners(
    registry: Any,
    owner_modules: List[tuple[str, str]],
) -> None:
    """Import each cascade-owner module and call its ``register_owners(registry)``.

    Three outcomes per module, each with a distinct log signal so missed
    registrations cannot leak silently (#1469):

    * ``ModuleNotFoundError`` on import → DEBUG. Expected when an optional
      extension wheel is not installed in this SCOPE.
    * Any other ``Exception`` on import → ERROR (``cascade_owner_import_failed``).
      The module is supposed to be present; cleanup of that resource type
      will be skipped on catalog hard-delete and resources will leak.
    * ``register_owners`` itself raises after a clean import → ERROR
      (``cascade_owner_registration_failed``). Same leak risk.
    """
    import importlib

    for module_path, label in owner_modules:
        try:
            mod = importlib.import_module(module_path)
        except ModuleNotFoundError:
            logger.debug(
                "CatalogModule: %s cascade owner module %r not installed; "
                "skipping registration.",
                label, module_path,
            )
            continue
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "cascade_owner_import_failed: module=%r label=%r error=%s "
                "— owner NOT registered; cleanup of %s resources will be "
                "skipped on catalog hard-delete and those resources WILL "
                "LEAK. Fix the import error.",
                module_path, label, exc, label,
                exc_info=True,
            )
            continue
        try:
            mod.register_owners(registry)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "cascade_owner_registration_failed: module=%r label=%r "
                "error=%s — owner NOT registered; cleanup of %s resources "
                "will be skipped on catalog hard-delete and those resources "
                "WILL LEAK.",
                module_path, label, exc, label,
                exc_info=True,
            )


# --- Asset event bridge: AssetEventType → CatalogEventType via EventsProtocol ---

_ASSET_EVENT_MAP = {
    AssetEventType.ASSET_CREATED: CatalogEventType.ASSET_CREATION,
    AssetEventType.ASSET_UPDATED: CatalogEventType.ASSET_UPDATE,
    AssetEventType.ASSET_DELETED: CatalogEventType.ASSET_DELETION,
    AssetEventType.ASSET_HARD_DELETED: CatalogEventType.ASSET_HARD_DELETION,
}


async def _asset_event_bridge(
    event_type: AssetEventType,
    data: dict,
    db_resource: Optional[DbResource] = None,
) -> None:
    """Bridge AssetService events to CatalogEventType.

    When ``db_resource`` is supplied, the event is also persisted to the
    global events outbox in the same transaction as the asset write — making
    it replayable for async subscribers (bucket annotation, reverse cascade,
    etc.). Without ``db_resource`` only in-process sync listeners fire.
    """
    catalog_event = _ASSET_EVENT_MAP.get(event_type)
    if catalog_event:
        await emit_event(
            catalog_event,
            catalog_id=data.get("catalog_id"),
            collection_id=data.get("collection_id"),
            asset_id=data.get("asset_id"),
            payload=data,
            db_resource=db_resource,
        )


# --- Legacy Constants and DDL (Shared by Module Initialization) ---

# M2.5b — post-refactor ``catalog.catalogs`` shape: technical registry
# columns only.  All descriptive metadata (title, description, keywords,
# license, conforms_to, links, assets, stac_*, extra_metadata) lives in
# ``catalog.catalog_core`` / ``_stac`` and is accessed through
# the catalog-metadata router (:mod:`catalog_router`).
#
# Delete-and-rebuild policy: no legacy columns carried over.  Fresh
# deployments get this canonical shape directly from the CREATE TABLE
# below.
CATALOGS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS catalog.catalogs (
    id VARCHAR PRIMARY KEY,
    external_id VARCHAR NOT NULL,
    provisioning_status VARCHAR(50) NOT NULL DEFAULT 'ready',
    provisioning_checklist JSONB DEFAULT NULL,
    first_ready_at TIMESTAMPTZ DEFAULT NULL,
    deleted_at TIMESTAMPTZ DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS catalogs_external_uq
    ON catalog.catalogs (external_id)
    WHERE deleted_at IS NULL;
"""

SHARED_PROPERTIES_SCHEMA = """
CREATE TABLE IF NOT EXISTS catalog.shared_properties (
    key_name VARCHAR PRIMARY KEY,
    key_value VARCHAR NOT NULL,
    owner_code VARCHAR,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
"""



_module_instance: Optional[ModuleProtocol] = None
class CatalogModule(ModuleProtocol):
    priority: int = 20
    """
    Manages catalog lifecycle.
    Functions as a composition root, initializing services and registering them as protocol providers.
    Must start after DBService (priority=10) and TasksModule (priority=15),
    which creates tables that the event consumer depends on.
    Priority < 20 would cause hard-abort on startup failure; 20 is still foundational.
    """

    # Inner-service registrations during ``lifespan`` (see ``register_plugin(svc)``
    # in the for-loop further down). The lifespan-end audit reads this tuple to
    # learn that this module is expected to make these protocols resolvable —
    # MRO walk alone wouldn't see them, since ``CatalogService`` / ``AssetService``
    # / ``ConfigService`` / ``LogService`` are inner services, not bases of
    # ``CatalogModule`` itself.
    provides_extra = (
        CatalogsProtocol,
        AssetsProtocol,
        ConfigsProtocol,
        LogsProtocol,
    )


    def __init__(self):
        self.app_state: Optional[Any] = None
        self.log_service: Optional[LogService] = None
        self.catalog_service: Optional[CatalogService] = None
        self.collection_service: Optional[CollectionService] = None
        self.items_service: Optional[ItemService] = None
        self.config_service: Optional[ConfigService] = None
        self.asset_service: Optional[AssetService] = None
        self.properties_service: Optional[PropertiesService] = None
        self.localization_service: Optional[LocalizationService] = None
        self.event_service: Optional[EventService] = None

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        """Standard CatalogModule lifespan with physical storage initialization."""
        self.app_state = app_state

        engine = get_engine()

        if not engine:
            logger.critical("CatalogModule: No DB engine found during startup.")
            yield
            return

        global _module_instance
        _module_instance = self

        # --- Instantiate and register internal services ---
        self.log_service = LogService()
        # Build the collection/item services first, then inject them into the
        # CatalogService facade. CatalogService auto-creates its own internal
        # CollectionService/ItemService when none are supplied; without this
        # injection the module would hold a SECOND CollectionService with its
        # own private read cache, so a write+invalidate routed through one
        # instance would leave stale entries readable through the other.
        self.collection_service = CollectionService(engine=engine)
        self.items_service = ItemService(engine=engine)
        self.catalog_service = CatalogService(  # type: ignore[abstract]
            engine=engine,
            collection_service=self.collection_service,
            item_service=self.items_service,
        )
        self.config_service = ConfigService(engine=engine)
        self.asset_service = AssetService(engine=engine, event_emitter=_asset_event_bridge)  # type: ignore[abstract]
        self.properties_service = PropertiesService(engine=engine)
        self.localization_service = LocalizationService()
        self.event_service = EventService()

        from dynastore.modules.catalog.drivers.pg_asset_driver import AssetPostgresqlDriver
        self.pg_asset_driver = AssetPostgresqlDriver(engine=engine)

        from dynastore.modules.storage.drivers.postgresql import ItemsPostgresqlDriver
        self.pg_storage_driver = ItemsPostgresqlDriver()  # type: ignore[abstract]

        from contextlib import AsyncExitStack
        # Track the services registered with the discovery registry so the
        # lifespan teardown can remove exactly them. Leaving them registered
        # leaks stale instances into the process-global registry on a
        # re-entered lifespan, which can hand callers a stale CollectionService
        # whose private read cache is never invalidated by the live instance's
        # writes (manifests as stale localized reads after an update).
        registered_services = (
            self.log_service,
            self.catalog_service,
            self.collection_service,
            self.items_service,
            self.config_service,
            self.asset_service,
            self.pg_asset_driver,
            self.pg_storage_driver,
            self.properties_service,
            self.localization_service,
            self.event_service,
        )
        async with AsyncExitStack() as stack:
            for svc in registered_services:
                # Enter plugin lifespan if it exists
                if hasattr(svc, "lifespan"):
                    await stack.enter_async_context(svc.lifespan(app_state))  # type: ignore[attr-defined]

                # Register for discovery
                register_plugin(svc)

            logger.info("Initialized CatalogModule services.")

            # Register cascade cleanup owners before the registry is frozen.
            # Each driver module or extension that owns external resources
            # contributes its owners here.  The registry is frozen after all
            # registrations so no late registration can occur at request time.
            from dynastore.modules.catalog.cascade_registry import cascade_cleanup_registry

            _owner_modules = [
                (
                    "dynastore.modules.storage.drivers.routing_driven_cascade_owner",
                    "routing-driven storage driver cleanup",
                ),
                (
                    "dynastore.modules.iam.cascade_owner",
                    "IAM catalog-scoped policies",
                ),
                (
                    "dynastore.modules.tiles.cascade_owner",
                    "tile preseed",
                ),
                (
                    "dynastore.extensions.gcp.cascade_owner",
                    "GCS bucket/prefix",
                ),
                (
                    "dynastore.extensions.proxy.cascade_owner",
                    "proxy short URLs",
                ),
                (
                    "dynastore.modules.stats.cascade_owner",
                    "stats telemetry ES index",
                ),
                (
                    "dynastore.modules.catalog.maintenance_cascade_owner",
                    "pending tasks/events for deleted element",
                ),
            ]
            _register_cascade_owners(cascade_cleanup_registry, _owner_modules)
            # Freeze the registry immediately after all owners have been
            # registered via _register_cascade_owners above.  All cascade
            # owners are contributed through that call (modules and
            # extensions alike), so the fence can live here rather than in
            # main.py. Removes the last named-module coupling from main.py.
            # See geoid#1683.
            from dynastore.modules.catalog.cascade_registry import finalize_cascade_registry
            finalize_cascade_registry()

            # Wire AssetEntitySyncSubscriber to drive AssetIndexer fan-out
            # from the events bus. Replaces the legacy per-driver listener
            # blocks (one less coupling between drivers and the events bus).
            from dynastore.modules.catalog.asset_sync import (
                register_asset_entity_sync_subscriber,
                register_item_forward_cascade_subscriber,
                register_item_reverse_cascade_subscriber,
            )
            register_asset_entity_sync_subscriber()
            register_item_reverse_cascade_subscriber()
            register_item_forward_cascade_subscriber()

            # Register catalog_core as priority-0 provisioner.  It runs the
            # tenant schema DDL and lifecycle hooks that every other provisioner
            # (GCP, ES, …) depends on.  Priority 0 guarantees it executes in its
            # own group before any priority-100 provisioner group.
            from dynastore.modules.catalog.provisioning_registry import (
                provisioning_registry as _prov_registry,
                SCOPE_CATALOG,
            )

            async def _catalog_core_is_active(catalog_id: str, conn=None) -> bool:
                return True

            async def _catalog_core_provision(
                catalog_id: str,
                external_id=None,
                scope: str = "catalog",
                operation: str = "provision",
                collection_id=None,
                **_kw,
            ) -> None:
                """Run the core tenant DDL for a catalog.

                Called by CatalogProvisionTask via call_hook(**ctx).  Opens its
                own managed transaction because the task runs outside any caller
                transaction.  The checklist was already seeded by
                _create_catalog_async before the task was enqueued.  Lifecycle
                events (CATALOG_CREATION / AFTER_CATALOG_CREATION) are emitted
                by CatalogProvisionTask.run() after all checklist steps complete.
                """
                from dynastore.modules.catalog.catalog_service import (
                    get_catalog_engine,
                    _invalidate_catalog_model_cache,
                    _invalidate_catalog_external_id_cache,
                )
                from dynastore.modules.db_config.query_executor import managed_transaction
                from dynastore.tools.protocol_helpers import resolve
                from dynastore.models.protocols import CatalogsProtocol

                catalogs = resolve(CatalogsProtocol)
                catalog_model = await catalogs.get_catalog_model(catalog_id)
                if catalog_model is None:
                    raise RuntimeError(
                        f"catalog_core provisioner: catalog '{catalog_id}' not found"
                    )

                run_core_init = getattr(catalogs, "_run_core_init", None)
                if run_core_init is None:
                    raise RuntimeError(
                        f"CatalogsProtocol implementation {type(catalogs).__name__} "
                        "does not expose _run_core_init; cannot run catalog_core provisioner"
                    )

                _ext_id = external_id or getattr(catalog_model, "external_id", None) or catalog_id
                physical_schema = catalog_id

                async with managed_transaction(get_catalog_engine()) as conn:
                    await run_core_init(
                        conn,
                        catalog_model,
                        _ext_id,
                        physical_schema,
                    )

                _invalidate_catalog_model_cache(catalog_id)
                _invalidate_catalog_external_id_cache(_ext_id)

            async def _catalog_core_deprovision(
                catalog_id: str,
                external_id=None,
                scope: str = "catalog",
                operation: str = "deprovision_hard",
                collection_id=None,
                config_snapshot=None,
                **_kw,
            ) -> None:
                """Deprovision the core tenant schema for a catalog (#2340).

                Called by CatalogProvisionTask with operation='deprovision_hard'.
                Mirrors _purge_catalog_storage: snapshots cascade refs, drops
                the schema CASCADE, and hard-deletes the registry row.

                For the PostgreSQL driver, catalog_id IS the physical schema name.
                Runs in its own managed transaction because the task is outside
                any caller transaction. Lifecycle events (CATALOG_HARD_DELETION,
                AFTER_CATALOG_HARD_DELETION) are emitted by CatalogProvisionTask
                after all deprovision steps complete.
                """
                from dynastore.modules.catalog.catalog_service import (
                    get_catalog_engine,
                    _hard_delete_catalog_query,
                    _invalidate_catalog_model_cache,
                    _invalidate_catalog_external_id_cache,
                )
                from dynastore.modules.db_config.query_executor import (
                    managed_transaction,
                    DQLQuery,
                    ResultHandler,
                )
                from dynastore.modules.db_config.locking_tools import safe_drop_relation
                from dynastore.modules.catalog.cascade_runtime import CascadeOrchestrator
                from dynastore.modules.catalog.resource_owner import CleanupMode, ResourceScope, ScopeRef

                orchestrator = CascadeOrchestrator()
                scope_ref = ScopeRef(scope=ResourceScope.CATALOG, catalog_id=catalog_id)

                async with managed_transaction(get_catalog_engine()) as conn:
                    # Snapshot cascade refs BEFORE schema drop while DB rows are readable.
                    cascade_task_id = await orchestrator.snapshot_and_enqueue(
                        conn, scope_ref, CleanupMode.HARD
                    )

                    # For the PostgreSQL driver, catalog_id IS the physical schema.
                    # Resolve it (works on tombstoned rows too).
                    physical_schema = await DQLQuery(
                        "SELECT id FROM catalog.catalogs WHERE id = :catalog_id;",
                        result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
                    ).execute(conn, catalog_id=catalog_id)

                    if physical_schema:
                        # DROP SCHEMA CASCADE with retry on lock contention.
                        await safe_drop_relation(
                            conn,
                            schema=physical_schema,
                            relation=physical_schema,
                            kind="schema",
                            cascade=True,
                            max_retries=5,
                        )

                    # Hard-delete the registry row (cascades to catalog_core/stac).
                    await _hard_delete_catalog_query.execute(conn, id=catalog_id)

                    if cascade_task_id is not None:
                        logger.info(
                            "catalog_core deprovision: enqueued cascade cleanup task %s for catalog %r.",
                            cascade_task_id, catalog_id,
                        )

                _invalidate_catalog_model_cache(catalog_id)
                if external_id:
                    _invalidate_catalog_external_id_cache(external_id)

            _prov_registry.register(
                "catalog_core",
                _catalog_core_is_active,
                priority=0,
                scope=SCOPE_CATALOG,
                name="Core tenant schema",
                description="Creates tenant schema, core tables, and lifecycle hooks.",
                provision=_catalog_core_provision,
                deprovision=_catalog_core_deprovision,
            )

            # Wire render preseed subscriber — enqueues durable render_preseed
            # obligations on AFTER_ASSET_CREATION when the feature is enabled
            # via RenderPreseedConfig (disabled by default).
            try:
                from dynastore.modules.renders.preseed_sync import (
                    register_render_preseed_subscriber,
                )
                register_render_preseed_subscriber()
            except Exception as _exc:  # noqa: BLE001
                logger.warning(
                    "CatalogModule: render preseed subscriber registration failed "
                    "(non-fatal): %s", _exc,
                )

            # 4. Initialize Storage & Schemas
            # Hub/sidecar creation is handled by ItemsPostgresqlDriver.ensure_storage()
            # which is called from _create_collection_internal(). No lifecycle hook needed.

            async with managed_transaction(engine) as conn:
                await ensure_schema_exists(conn, "catalog")

                # System-level logs table removed (#2749) — logs are
                # Elasticsearch-only now; no PG DDL for them anywhere.

                await DDLQuery(
                    CATALOGS_TABLE_DDL + SHARED_PROPERTIES_SCHEMA
                ).execute(conn)

                # Metadata-domain tables: catalog.catalog_core +
                # catalog.catalog_stac.  The only collection- and
                # catalog-metadata storage path after the M2.5 hard cut.
                from dynastore.modules.catalog.db_init.core_tables import (
                    ensure_global_core_tables,
                )
                await ensure_global_core_tables(conn)

                # Ensure stored procedures (replacing init.sql)
                from dynastore.modules.catalog.db_init.stored_procedures import (
                    ensure_stored_procedures,
                )

                await ensure_stored_procedures(conn)

            # 5. Register Internal Observers
            # Observers will use get_protocol() to access services
            register_event_listener(
                CatalogEventType.AFTER_CATALOG_HARD_DELETION, self._on_catalog_hard_deletion
            )
            register_event_listener(
                CatalogEventType.CATALOG_DELETION, self._on_catalog_deletion
            )
            register_event_listener(
                CatalogEventType.AFTER_COLLECTION_HARD_DELETION,
                self._on_collection_hard_deletion,
            )
            register_event_listener(
                CatalogEventType.COLLECTION_DELETION, self._on_collection_deletion
            )
            from dynastore.modules.catalog.event_service import async_event_listener

            @async_event_listener("task.failed")
            async def _on_task_failed_impl(**kwargs):
                await self._on_task_failed(**kwargs)

            # M3.1b — register the catalog_metadata_changed → ReindexWorker
            # listener so mutations emitted by ``catalog_router`` propagate to
            # every configured secondary-index driver. The listener is invoked
            # by ``EventDrainTask`` (the control-plane drain of ``tasks.events``)
            # via ``EventService.dispatch_to_listeners``; see
            # ``reindex_listener.py`` for the rationale on listener-over-
            # standalone-consumer.
            from dynastore.modules.catalog.reindex_listener import (
                register_reindex_listener,
            )
            register_reindex_listener(self.event_service)


            # 7–10. Start leader-elected background services via BackgroundSupervisor.
            from dynastore.modules.db_config.instance import get_service_name
            from dynastore.tools.background_service import (
                BackgroundSupervisor,
                ServiceContext,
            )
            from dynastore.modules.catalog.soft_delete_reaper import (
                SoftDeleteReaper,
                load_reaper_config,
            )
            from dynastore.modules.catalog.maintenance_supervisor import (
                MaintenanceSupervisor,
                build_supervisor_config,
                register_supervisor_jobs,
                unschedule_superseded_cron_jobs,
            )
            from dynastore.modules.catalog.lifecycle_reaper import (
                LifecycleReaper,
                load_lifecycle_reaper_config,
            )
            from dynastore.modules.db.db_contention_monitor import (
                DbContentionMonitor,
                load_db_contention_monitor_config,
            )
            from dynastore.modules.db.instance_liveness import (
                InstanceLivenessHeartbeat,
            )
            from dynastore.modules.db.zombie_session_reaper import (
                ZombieSessionReaper,
                load_zombie_session_reaper_config,
            )
            from dynastore.modules.scaling.publisher import ScalingSignalPublisher
            # Import-time side effect: registers the lowest-priority fallback
            # PlatformScalingProtocol so the control loop has somewhere safe
            # to land on deployments with no platform-specific actuator.
            import dynastore.modules.scaling.noop_actuator  # noqa: F401

            _bg_shutdown = asyncio.Event()
            bg_supervisor = BackgroundSupervisor()
            # ScalingSignalProtocol providers registered alongside the
            # BackgroundSupervisor services (rather than via the
            # ``registered_services`` tuple, which enters ``.lifespan()`` on
            # each entry — these have none) so they unregister cleanly below.
            _scaling_plugins: list = []

            try:
                reaper_cfg = await load_reaper_config()
                bg_supervisor.register(SoftDeleteReaper(reaper_cfg))
                logger.info(
                    "CatalogModule: soft-delete reaper registered "
                    "(grace=%ds, interval=%ds).",
                    reaper_cfg.soft_grace_period_seconds,
                    reaper_cfg.reaper_interval_seconds,
                )
            except Exception as exc:  # noqa: BLE001 — never block startup
                logger.warning(
                    "CatalogModule: soft-delete reaper failed to configure: %s — "
                    "soft-deleted entities will not be automatically promoted.",
                    exc,
                )

            try:
                supervisor_cfg = build_supervisor_config()
                # Clean-cut safety: drop any pre-existing events/logs/IAM pg_cron
                # jobs this supervisor now owns so they cannot double-run on a
                # non-fresh deploy (no-op when pg_cron is absent).
                await unschedule_superseded_cron_jobs(engine)
                await register_supervisor_jobs(engine)
                bg_supervisor.register(MaintenanceSupervisor(supervisor_cfg))
                logger.info("CatalogModule: maintenance supervisor registered.")
            except Exception as exc:  # noqa: BLE001 — never block startup
                logger.warning(
                    "CatalogModule: maintenance supervisor failed to configure: %s — "
                    "events/logs/IAM pruning will not run automatically.",
                    exc,
                )

            try:
                from dynastore.modules.catalog.log_drainer import LogDrainer
                from dynastore.modules.catalog.log_service_config import (
                    load as load_log_service_config,
                )

                log_service_cfg = await load_log_service_config()
                bg_supervisor.register(LogDrainer(log_service_cfg))
                logger.info(
                    "CatalogModule: log drainer registered (interval=%ss).",
                    log_service_cfg.valkey_drain_interval_seconds,
                )
            except Exception as exc:  # noqa: BLE001 — never block startup
                logger.warning(
                    "CatalogModule: log drainer failed to configure: %s — "
                    "Valkey-buffered logs will not be drained (the direct-"
                    "to-backend dispatch fallback still writes logs).",
                    exc,
                )

            try:
                lifecycle_reaper_cfg = await load_lifecycle_reaper_config()
                bg_supervisor.register(LifecycleReaper(lifecycle_reaper_cfg))
                logger.info(
                    "CatalogModule: lifecycle reaper registered "
                    "(threshold=%ds, interval=%ds).",
                    lifecycle_reaper_cfg.stuck_threshold_seconds,
                    lifecycle_reaper_cfg.reaper_interval_seconds,
                )
            except Exception as exc:  # noqa: BLE001 — never block startup
                logger.warning(
                    "CatalogModule: lifecycle reaper failed to configure: %s — "
                    "stuck PROVISIONING/DELETING collections will not be "
                    "automatically reconciled.",
                    exc,
                )

            try:
                contention_cfg = load_db_contention_monitor_config()
                if contention_cfg.enabled:
                    monitor = DbContentionMonitor(contention_cfg)
                    bg_supervisor.register(monitor)
                    # Same instance registered for discovery so the scaling
                    # publisher can read the global conn_pressure signal this
                    # monitor's leader ticks populate on ``_last_conn_pressure``.
                    register_plugin(monitor)
                    _scaling_plugins.append(monitor)
                    logger.info(
                        "CatalogModule: DB contention monitor registered "
                        "(interval=%ds, slow_query=%ds, lock_wait=%ds).",
                        contention_cfg.interval_seconds,
                        contention_cfg.slow_query_seconds,
                        contention_cfg.lock_wait_seconds,
                    )
            except Exception as exc:  # noqa: BLE001 — never block startup
                logger.warning(
                    "CatalogModule: DB contention monitor failed to configure: %s — "
                    "lock/slow-query contention will not be logged automatically.",
                    exc,
                )

            try:
                # Always registered — its tick() live-reads
                # ZombieSessionReaperConfig.enabled and does zero DB work
                # while the reaper is off (the default), so this never adds
                # background load on its own and a live configs-API PATCH
                # enabling the reaper takes effect without a pod restart.
                bg_supervisor.register(InstanceLivenessHeartbeat())
                logger.info("CatalogModule: instance liveness heartbeat registered.")
            except Exception as exc:  # noqa: BLE001 — never block startup
                logger.warning(
                    "CatalogModule: instance liveness heartbeat failed to "
                    "configure: %s — the zombie-session reaper will see no "
                    "live instances and will not reap anyone (fail-safe).",
                    exc,
                )

            try:
                zombie_reaper_cfg = await load_zombie_session_reaper_config()
                bg_supervisor.register(ZombieSessionReaper(zombie_reaper_cfg))
                logger.info(
                    "CatalogModule: zombie-session reaper registered "
                    "(enabled=%s, idle_threshold=%ds, interval=%ds).",
                    zombie_reaper_cfg.enabled,
                    zombie_reaper_cfg.idle_threshold_seconds,
                    zombie_reaper_cfg.reaper_interval_seconds,
                )
            except Exception as exc:  # noqa: BLE001 — never block startup
                logger.warning(
                    "CatalogModule: zombie-session reaper failed to configure: "
                    "%s — dead-instance sessions will not be automatically "
                    "reaped.",
                    exc,
                )

            try:
                from dynastore.modules.storage.drivers.duckdb import (
                    DuckDbPoolSignalProvider,
                )

                duckdb_signal_provider = DuckDbPoolSignalProvider()
                register_plugin(duckdb_signal_provider)
                _scaling_plugins.append(duckdb_signal_provider)
            except Exception as exc:  # noqa: BLE001 — never block startup
                logger.warning(
                    "CatalogModule: DuckDB pool signal provider failed to "
                    "register: %s — DuckDB pool saturation will not feed the "
                    "autoscaling control loop.",
                    exc,
                )

            try:
                bg_supervisor.register(ScalingSignalPublisher(self.config_service))
                logger.info("CatalogModule: scaling signal publisher registered.")
            except Exception as exc:  # noqa: BLE001 — never block startup
                logger.warning(
                    "CatalogModule: scaling signal publisher failed to "
                    "configure: %s — autoscaling signals will not be "
                    "published.",
                    exc,
                )

            bg_ctx = ServiceContext(
                engine=engine,
                shutdown=_bg_shutdown,
                is_ephemeral=bool(getattr(app_state, "ephemeral_job", False)),
                name=get_service_name() or "unknown",
            )
            try:
                # start() is inside the try so the finally always drains the
                # supervisor — if start() itself raises after submitting some
                # services, those tasks are still stopped and _bg_shutdown is set.
                bg_supervisor.start(bg_ctx)
                yield
            finally:
                _bg_shutdown.set()
                await bg_supervisor.stop()
                # Services cleanup handled by AsyncExitStack (stack.close() via __aexit__)
                # Remove the services from the discovery registry so a future
                # lifespan does not leave stale instances behind them.
                for svc in registered_services:
                    unregister_plugin(svc)
                for svc in _scaling_plugins:
                    unregister_plugin(svc)

    # === Private service accessors (assert-narrowed for pyright) ===

    @property
    def _cs(self) -> CatalogService:
        assert self.catalog_service is not None
        return self.catalog_service

    @property
    def _col_svc(self) -> CollectionService:
        assert self.collection_service is not None
        return self.collection_service

    @property
    def _item_svc(self) -> ItemService:
        assert self.items_service is not None
        return self.items_service

    # === Unified Protocol Properties (Delegation) ===

    @property
    def items(self) -> ItemsProtocol:
        return self._cs.items

    @property
    def collections(self) -> CollectionsProtocol:
        return self._cs.collections

    @property
    def localization(self) -> LocalizationProtocol:
        assert self.localization_service is not None
        return self.localization_service

    # === Delegated CRUD Methods ===

    async def get_catalog(
        self, catalog_id: str, lang: str = "en", ctx: Optional[DriverContext] = None
    ) -> Catalog:
        return await self._cs.get_catalog(
            catalog_id, lang=lang, ctx=ctx
        )

    async def get_catalog_model(
        self, catalog_id: str, ctx: Optional[DriverContext] = None
    ) -> Optional[Catalog]:
        return await self._cs.get_catalog_model(
            catalog_id, ctx=ctx
        )

    async def create_catalog(
        self,
        catalog_data: Union[Dict[str, Any], Catalog],
        lang: str = "en",
        ctx: Optional[DriverContext] = None,
    ) -> Catalog:
        return await self._cs.create_catalog(
            catalog_data, lang=lang, ctx=ctx
        )

    async def update_catalog(
        self,
        catalog_id: str,
        updates: Union[Dict[str, Any], "CatalogUpdate"],
        lang: str = "en",
        ctx: Optional[DriverContext] = None,
    ) -> Optional[Catalog]:
        return await self._cs.update_catalog(
            catalog_id, updates, lang=lang, ctx=ctx
        )

    async def delete_catalog(
        self, catalog_id: str, force: bool = False, ctx: Optional[DriverContext] = None
    ) -> bool:
        return await self._cs.delete_catalog(
            catalog_id, force=force, ctx=ctx
        )

    async def get_hard_delete_task(self, catalog_id: str) -> Optional[Any]:
        return await self._cs.get_hard_delete_task(catalog_id)

    async def delete_catalog_language(
        self, catalog_id: str, lang: str, ctx: Optional[DriverContext] = None
    ) -> bool:
        return await self._cs.delete_catalog_language(
            catalog_id, lang, ctx=ctx
        )

    async def list_catalogs(
        self,
        limit: int = 10,
        offset: int = 0,
        lang: str = "en",
        ctx: Optional[DriverContext] = None,
        q: Optional[str] = None,
        ids: Optional[Set[str]] = None,
        include_unready: bool = False,
    ) -> List[Catalog]:
        return await self._cs.list_catalogs(
            limit=limit, offset=offset, lang=lang, ctx=ctx, q=q, ids=ids,
            include_unready=include_unready,
        )

    async def search_catalogs(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> List[Catalog]:
        return await self._cs.search_catalogs(
            filters=filters, limit=limit, offset=offset, db_resource=db_resource
        )

    # === Collection Operations ===

    async def get_collection(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str = "en",
        ctx: Optional[DriverContext] = None,
    ) -> Optional[Collection]:
        return await self._col_svc.get_collection(
            catalog_id, collection_id, lang=lang, ctx=ctx
        )

    async def create_collection(
        self,
        catalog_id: str,
        collection_data: Union[Dict[str, Any], Collection],
        lang: str = "en",
        ctx: Optional[DriverContext] = None,
        **kwargs,
    ) -> Collection:
        return await self._col_svc.create_collection(
            catalog_id, collection_data, lang=lang, ctx=ctx, **kwargs
        )

    async def update_collection(
        self,
        catalog_id: str,
        collection_id: str,
        updates: Union[Dict[str, Any], "CollectionUpdate"],
        lang: str = "en",
        ctx: Optional[DriverContext] = None,
    ) -> Optional[Collection]:
        return await self._col_svc.update_collection(
            catalog_id, collection_id, updates, lang=lang, ctx=ctx  # type: ignore[arg-type]
        )

    async def delete_collection(
        self,
        catalog_id: str,
        collection_id: str,
        force: bool = False,
        ctx: Optional[DriverContext] = None,
    ) -> bool:
        return await self._col_svc.delete_collection(
            catalog_id, collection_id, force=force, ctx=ctx
        )

    async def delete_collection_language(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str,
        ctx: Optional[DriverContext] = None,
    ) -> bool:
        return await self._col_svc.delete_collection_language(
            catalog_id, collection_id, lang, ctx=ctx
        )

    async def list_collections(
        self,
        catalog_id: str,
        limit: int = 10,
        offset: int = 0,
        lang: str = "en",
        ctx: Optional[DriverContext] = None,
        q: Optional[str] = None,
    ) -> List[Any]:
        return await self._col_svc.list_collections(
            catalog_id, limit=limit, offset=offset, lang=lang, ctx=ctx, q=q
        )

    # === Item Operations ===

    async def upsert(
        self,
        catalog_id: str,
        collection_id: str,
        items: Union[Dict[str, Any], List[Dict[str, Any]], Any],
        ctx: Optional[DriverContext] = None,
        processing_context: Optional[Dict[str, Any]] = None,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]], Any]:
        """Create or update items (single or bulk)."""
        return await self._item_svc.upsert(
            catalog_id,
            collection_id,
            items,
            ctx=ctx,
            processing_context=processing_context,
        )

    async def get_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        ctx: Optional[DriverContext] = None,
        lang: str = "en",
        context: Optional[Any] = None,
        access_filter: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        return await self._item_svc.get_item(  # type: ignore[return-value]
            catalog_id, collection_id, item_id,
            ctx=ctx, lang=lang, context=context, access_filter=access_filter,
        )

    async def delete_item(
        self,
        catalog_id: str,
        collection_id: str,
        item_id: str,
        ctx: Optional[DriverContext] = None,
        caller_id: Optional[str] = None,
    ) -> int:
        return await self._item_svc.delete_item(
            catalog_id, collection_id, item_id, ctx=ctx, caller_id=caller_id
        )

    async def delete_item_language(
        self,
        catalog_id: str,
        collection_id: str,
        ext_id: str,
        lang: str,
        ctx: Optional[DriverContext] = None,
    ) -> int:
        return await self._item_svc.delete_item_language(
            catalog_id, collection_id, ext_id, lang, ctx=ctx
        )

    async def search_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        ctx: Optional[DriverContext] = None,
        consumer: "Optional[ConsumerType]" = None,
    ) -> "List[Feature]":
        from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType as _CT
        return await self._item_svc.search_items(  # type: ignore[return-value]
            catalog_id, collection_id, request, config=config, ctx=ctx,
            consumer=consumer or _CT.GENERIC,
        )

    async def stream_items(
        self,
        catalog_id: str,
        collection_id: str,
        request: QueryRequest,
        config: Optional[ConfigsProtocol] = None,
        ctx: Optional[DriverContext] = None,
        consumer: "Optional[ConsumerType]" = None,
        hints: "FrozenSet[Hint]" = frozenset(),
    ) -> "QueryResponse":
        from dynastore.modules.storage.drivers.pg_sidecars.base import ConsumerType as _CT
        return await self._item_svc.stream_items(
            catalog_id, collection_id, request, config=config, ctx=ctx,
            consumer=consumer or _CT.GENERIC, hints=hints,
        )

    # === Schema/Table Resolution ===

    async def resolve_physical_schema(
        self,
        catalog_id: Optional[str] = None,
        ctx: Optional[DriverContext] = None,
        allow_missing: bool = False,
    ) -> Optional[str]:
        return await self._cs.resolve_physical_schema(
            catalog_id, ctx=ctx, allow_missing=allow_missing  # type: ignore[arg-type]
        )

    async def resolve_datasource(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        operation: str = "READ",
        hints: Optional[FrozenSet["Hint"]] = None,
    ):
        return await self._cs.resolve_datasource(
            catalog_id, collection_id, operation=operation, hints=hints
        )

    async def resolve_physical_table(
        self, catalog_id: str, collection_id: str, db_resource: Optional[Any] = None
    ) -> Optional[str]:
        return await self._cs.resolve_physical_table(
            catalog_id, collection_id, db_resource=db_resource
        )

    async def set_physical_table(
        self,
        catalog_id: str,
        collection_id: str,
        physical_table: str,
        db_resource: Optional[Any] = None,
    ) -> None:
        return await self._cs.set_physical_table(
            catalog_id, collection_id, physical_table, db_resource=db_resource
        )

    async def ensure_catalog_exists(
        self, catalog_id: str, lang: str = "en", ctx: Optional[DriverContext] = None
    ) -> None:
        return await self._cs.ensure_catalog_exists(
            catalog_id, lang=lang, ctx=ctx
        )

    async def ensure_collection_exists(
        self,
        catalog_id: str,
        collection_id: str,
        lang: str = "en",
        ctx: Optional[DriverContext] = None,
    ) -> None:
        db_resource = ctx.db_resource if ctx else None
        return await self._col_svc.ensure_collection_exists(
            db_resource, catalog_id, collection_id, lang=lang  # type: ignore[arg-type]
        )

    async def ensure_physical_table_exists(
        self,
        catalog_id: str,
        collection_id: str,
        config: "CollectionPluginConfig",
        db_resource: Optional[Any] = None,
    ) -> None:
        return await self._item_svc.ensure_physical_table_exists(
            catalog_id, collection_id, config, db_resource=db_resource  # type: ignore[arg-type]
        )

    async def ensure_partition_exists(
        self,
        catalog_id: str,
        collection_id: str,
        config: "CollectionPluginConfig",
        partition_value: Any,
        ctx: Optional[DriverContext] = None,
    ) -> None:
        return await self._item_svc.ensure_partition_exists(
            catalog_id, collection_id, config, partition_value, ctx=ctx  # type: ignore[arg-type]
        )

    @property
    def assets(self) -> AssetsProtocol:
        assert self.asset_service is not None
        return self.asset_service

    @property
    def configs(self) -> ConfigsProtocol:
        assert self.config_service is not None
        return self.config_service

    @property
    def count_items_by_asset_id_query(self) -> Any:
        return self._item_svc.count_items_by_asset_id_query

    async def get_collection_config(
        self, catalog_id: str, collection_id: str, ctx: Optional[DriverContext] = None
    ) -> "CollectionPluginConfig":
        return await self._cs.get_collection_config(
            catalog_id, collection_id, ctx=ctx
        )

    async def get_collection_column_names(
        self, catalog_id: str, collection_id: str, ctx: Optional[DriverContext] = None
    ) -> Set[str]:
        return await self._cs.get_collection_column_names(
            catalog_id, collection_id, ctx=ctx
        )

    # --- Internal Observers for Module Maintenance ---

    # --- Internal Observers (Using Protocols) ---

    async def _on_catalog_hard_deletion(self, catalog_id: str, **kwargs):
        """Final physical destruction for catalog (Assets, Schema, Record)."""
        logger.info(f"Finalizing deletion for catalog '{catalog_id}'")

        # Resolve dependencies via protocols. Asset rows and per-catalog
        # configs are torn down by ``DROP SCHEMA ... CASCADE`` below (or by the
        # emitting CatalogService when it passes ``physical_schema``), so only
        # the catalog resolver and the DB handle are needed here.
        catalogs = get_protocol(CatalogsProtocol)
        db = get_protocol(DatabaseProtocol)

        db_resource = kwargs.get("db_resource")
        if not db_resource and db:
            db_resource = db.engine

        if not db_resource:
            logger.error(
                "No database resource available for catalog hard deletion cleanup."
            )
            return

        # Check if physical_schema was provided in event payload (from CatalogService)
        physical_schema = kwargs.get("physical_schema")

        # 1. Purge Assets
        # If schema is dropped, assets table is gone.
        # But if AssetManager uses external storage (e.g. S3), it might need cleanup?
        # Current implementation: AssetManager.delete_assets operates on DB table.
        # If schema is gone, delete_assets will fail or return 0.
        # However, we can't easily check if schema is gone without querying DB or checking the passed arg.

        # If physical_schema is provided, it implies it was known before deletion.
        # But if it was already dropped by the emitter, we can't run delete_assets query on it.
        # So we skip Asset deletion if schema is dropped.

        # BUT: AssetManager logic might handle file deletions (S3)?
        # The current implementation of delete_assets only deletes rows.
        # So skipping is correct if table is gone.

        # 2. Drop logical record and configuration (Redundant if CatalogService handled it?)
        # CatalogService deletes schema and row.
        # We only need to cleanup if CatalogService failed to do so, or if this event came from elsewhere.

        # If physical_schema is passed, it likely means the emitter (CatalogService) already handled schema drop.
        if physical_schema:
            logger.info(
                f"Schema {physical_schema} was handled by emitter. Skipping redundant cleanup."
            )
            return

        # Fallback for manual events or failures: try to cleanup if still exists
        async with managed_transaction(db_resource) as conn:
            phys_schema = None
            if catalogs:
                phys_schema = await catalogs.resolve_physical_schema(
                    catalog_id, ctx=DriverContext(db_resource=conn), allow_missing=True
                )

            # If schema exists, drop it
            if phys_schema:
                # If we found schema, assets table exists, so we can try to purge assets first?
                # Actually dropping schema cascades to assets table.
                await DDLQuery(f'DROP SCHEMA IF EXISTS "{phys_schema}" CASCADE;').execute(
                    conn
                )

    async def _on_catalog_deletion(self, catalog_id: str, **kwargs):
        """Soft deletion of catalog assets."""
        assets = get_protocol(AssetsProtocol)
        if assets:
            await assets.delete_assets(
                catalog_id=catalog_id, hard=False, db_resource=kwargs.get("db_resource")  # type: ignore[misc]
            )

    async def _on_collection_hard_deletion(
        self, catalog_id: str, collection_id: str, **kwargs
    ):
        """Purge assets for a hard-deleted collection.

        Runs INSIDE the collection delete transaction (``db_resource=conn``).
        Only the canonical PG asset rows are removed here — atomic with the
        items-table drop.  External (Elasticsearch) asset teardown is owned by
        the async cascade_cleanup task (RoutingDrivenCascadeOwner ->
        AssetElasticsearchDriver.drop_storage, collection-granular), so it must
        NOT run inline: ES HTTP I/O on the delete connection would hold it idle
        in-transaction past idle_in_transaction_session_timeout and the commit
        would fail.  Hence ``external=False``.
        """
        assets = get_protocol(AssetsProtocol)
        if assets:
            await assets.delete_assets(
                catalog_id=catalog_id,
                collection_id=collection_id,
                hard=True,
                external=False,
                db_resource=kwargs.get("db_resource"),  # type: ignore[misc]
            )

    async def _on_collection_deletion(
        self, catalog_id: str, collection_id: str, **kwargs
    ):
        """Soft deletion of collection assets."""
        assets = get_protocol(AssetsProtocol)
        if assets:
            await assets.delete_assets(
                catalog_id=catalog_id,
                collection_id=collection_id,
                hard=False,
                db_resource=kwargs.get("db_resource"),  # type: ignore[misc]
            )

    async def _on_task_failed(
        self,
        task_id: str,
        task_type: str,
        error_message: str,
        severity: str = "unrecoverable",
        inputs: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        """Generic task failure handler. Routes provisioning rollback by task_type.

        A failed provisioning task (``catalog_provision`` / ``gcp_provision_catalog``)
        carries the target catalog in ``inputs['catalog_id']``. The catalog's
        ``provisioning_status`` is already flipped to ``failed`` by the checklist
        machinery — a failing provisioner marks its own step ``failed`` and any
        still-pending steps are drained to ``failed`` on terminal task exit
        (see ``_drain_provisioning_checklist``). What that path does *not* capture
        is the human-readable failure reason, so this handler records it in
        ``extra_metadata['provisioning_error']`` for operator diagnostics, and
        re-asserts ``provisioning_status='failed'`` as idempotent defense-in-depth
        for the edge case where the task dies before any step mark and the
        best-effort drain also fails.

        Routing is by ``task_type`` (reliably propagated from the failure
        emitter) rather than a catalog lifecycle event: ``TaskCreate`` carries no
        ``originating_event`` and the emitter cannot supply one without a schema
        change, so the previous ``originating_event`` gate never fired.
        """
        logger.warning(
            f"Task '{task_type}' ({task_id}) FAILED [{severity}]: {error_message}"
        )
        inputs = inputs or {}

        # Catalog-provisioning task types that carry a target ``catalog_id`` and
        # whose failure must be reflected on the catalog row.
        _PROVISIONING_TASK_TYPES = {"catalog_provision", "gcp_provision_catalog"}

        if task_type in _PROVISIONING_TASK_TYPES:
            catalog_id = inputs.get("catalog_id")
            if catalog_id:
                logger.info(
                    f"Recording provisioning failure for catalog '{catalog_id}' "
                    f"(triggered by '{task_type}' failure, severity={severity})."
                )
                catalogs = get_protocol(CatalogsProtocol)
                if catalogs:
                    _dr = kwargs.get("db_resource")
                    _ctx = DriverContext(db_resource=_dr) if _dr else None
                    # (1) Re-assert provisioning_status='failed' so the
                    #     fail-fast guard at the API layer rejects write
                    #     operations on this catalog (endpoints call
                    #     ``require_catalog_ready`` which reads this column).
                    #     Idempotent: the checklist path usually set it already.
                    try:
                        await catalogs.update_provisioning_status(
                            catalog_id, "failed", ctx=_ctx,
                        )
                    except Exception as e:
                        logger.error(
                            f"Rollback for catalog '{catalog_id}': failed to "
                            f"set provisioning_status='failed': {e}"
                        )
                    # (2) Record the error detail in ``extra_metadata`` —
                    #     best-effort diagnostic for operators; this is the
                    #     part the checklist path does not capture.
                    try:
                        await catalogs.update_catalog(
                            catalog_id,
                            {"extra_metadata": {"provisioning_error": error_message}},
                            ctx=_ctx,
                        )
                    except Exception as e:
                        logger.error(
                            f"Rollback for catalog '{catalog_id}': failed to "
                            f"record provisioning_error in extra_metadata: {e}"
                        )

# --- Module level proxies for common protocol operations ---

async def list_catalogs(*args, **kwargs):
    """Module-level proxy for CatalogsProtocol.list_catalogs"""
    cat = get_protocol(CatalogsProtocol)
    if cat:
        return await cat.list_catalogs(*args, **kwargs)
    return []


async def get_catalog(*args, **kwargs):
    """Module-level proxy for CatalogsProtocol.get_catalog"""
    cat = get_protocol(CatalogsProtocol)
    if cat:
        return await cat.get_catalog(*args, **kwargs)
    return None


async def get_collection(*args, **kwargs):
    """Module-level proxy for CollectionsProtocol.get_collection"""
    coll = get_protocol(CollectionsProtocol)
    if coll:
        return await coll.get_collection(*args, **kwargs)
    return None


async def list_collections(*args, **kwargs):
    """Module-level proxy for CollectionsProtocol.list_collections"""
    coll = get_protocol(CollectionsProtocol)
    if coll:
        return await coll.list_collections(*args, **kwargs)
    return []


async def get_collection_config(*args, **kwargs):
    """Module-level proxy for CatalogsProtocol.get_collection_config"""
    from dynastore.models.protocols import CatalogsProtocol as _CatalogsProtocol
    conf = get_protocol(_CatalogsProtocol)
    if conf:
        return await conf.get_collection_config(*args, **kwargs)
    return None
