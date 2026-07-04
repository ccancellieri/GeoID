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

"""Lifecycle-state reaper for collections stuck in PROVISIONING or DELETING.

Background loop that periodically finds collections whose transitional
``lifecycle_status`` overlay has been set for longer than a configurable
threshold — indicating that the pod which owned the in-flight operation
crashed mid-window — and reconciles them:

* PROVISIONING older than threshold  → clear ``lifecycle_status`` to NULL
  (the collection is treated as ACTIVE).  There is no FAILED state today;
  that is tracked as a follow-up.
* DELETING older than threshold      → re-drive ``delete_collection(force=True)``,
  which is idempotent and self-heals the stuck purge.

Architecture contract
---------------------
- Scheduling: a leader-elected asyncio background loop (``run_leader_loop``
  with a pg advisory lock) runs the reaper callable on every pod; exactly
  one pod wins the lock and performs the scan per cadence tick.
- No-race invariant: the scan uses ``FOR UPDATE SKIP LOCKED`` so concurrent
  reaper instances never double-process the same row.
- Config: ``LifecycleReaperConfig`` (PluginConfig) with master switch
  (default False — opt-in after soaking), threshold seconds, interval, and
  batch size.  Read once at loop start; live changes apply on next restart.
- No DB DDL: the ``lifecycle_status`` column and ``created_at`` timestamp
  are already present from #2066.  No schema change is needed here.
- FAILED state: not added in this PR.  A collection cleared from PROVISIONING
  becomes ACTIVE; operators can soft-delete or hard-delete as needed.
  A FAILED state can be introduced later as a dedicated lifecycle value.
"""

from __future__ import annotations

import logging
from typing import ClassVar, Optional, Tuple, Union

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.modules.catalog.collection_service import (
    _invalidate_collection_lifecycle_caches,
)
from dynastore.modules.tasks.durable.lock_registry import (
    LIFECYCLE_REAPER_ADVISORY_LOCK_KEY as _LIFECYCLE_REAPER_ADVISORY_LOCK_KEY,
)
from dynastore.tools.background_service import (
    Leadership,
    PeriodicService,
    PodPolicy,
    ServiceContext,
)
from dynastore.tools.discovery import get_protocol
from dynastore.tools.protocol_helpers import get_engine

logger = logging.getLogger(__name__)

# Advisory lock key — see modules/tasks/durable/lock_registry.py, the
# central registry of every leader-elected loop's key.


class LifecycleReaperConfig(PluginConfig):
    """Configuration for the collection lifecycle-state reaper background loop."""

    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "catalog")

    enabled: Mutable[bool] = Field(
        default=False,
        description=(
            "Master switch for the lifecycle-state reaper.  Defaults to False so "
            "operators can soak on a non-production environment first.  When True, "
            "the reaper periodically clears stale PROVISIONING overlays back to ACTIVE "
            "and re-drives stale DELETING collections through force=True delete."
        ),
    )

    stuck_threshold_seconds: Mutable[int] = Field(
        default=900,  # 15 minutes
        ge=60,
        description=(
            "Seconds a collection must remain in a transitional state (PROVISIONING "
            "or DELETING) before the reaper considers it stuck and reconciles it.  "
            "Default: 900 (15 minutes).  Minimum: 60 seconds (lab / test "
            "environments).  Changes take effect on the next pod restart."
        ),
    )

    reaper_interval_seconds: Mutable[float] = Field(
        default=300.0,  # 5 minutes
        ge=30.0,
        description=(
            "How often (seconds) the reaper scans for stuck collections.  "
            "Default: 300 (5 minutes).  Changes take effect on the next pod restart."
        ),
    )

    batch_size: Mutable[int] = Field(
        default=50,
        ge=1,
        le=500,
        description=(
            "Maximum collections processed per reaper tick per status.  "
            "Guards against long-running scans when many collections are stuck."
        ),
    )


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

# Find collections stuck in PROVISIONING past the threshold, across all active
# tenant schemas.  Returns catalog ids (schema name = catalog id) so we can
# enumerate per-tenant.
_ACTIVE_CATALOG_SCHEMAS_SQL = (
    "SELECT id FROM catalog.catalogs "
    "WHERE deleted_at IS NULL ORDER BY id"
)

# Per-tenant: find collection IDs stuck in PROVISIONING past the threshold.
# Uses FOR UPDATE SKIP LOCKED so two concurrent reaper instances never race on
# the same rows.
_STUCK_PROVISIONING_SQL = (
    'SELECT id FROM "{schema}".collections '
    "WHERE lifecycle_status = 'provisioning' "
    "  AND created_at + (:threshold_seconds || ' seconds')::INTERVAL < NOW() "
    "ORDER BY created_at "
    "LIMIT :limit "
    "FOR UPDATE SKIP LOCKED"
)

# Per-tenant: find collection IDs stuck in DELETING past the threshold.
_STUCK_DELETING_SQL = (
    'SELECT id FROM "{schema}".collections '
    "WHERE lifecycle_status = 'deleting' "
    "  AND created_at + (:threshold_seconds || ' seconds')::INTERVAL < NOW() "
    "ORDER BY created_at "
    "LIMIT :limit "
    "FOR UPDATE SKIP LOCKED"
)

# Clear the PROVISIONING overlay for a stuck collection (back to ACTIVE).
_CLEAR_PROVISIONING_SQL = (
    'UPDATE "{schema}".collections '
    "SET lifecycle_status = NULL "
    "WHERE id = :id AND lifecycle_status = 'provisioning'"
)


# ---------------------------------------------------------------------------
# LifecycleReaper
# ---------------------------------------------------------------------------


class LifecycleReaper(PeriodicService):
    """Periodic reaper that reconciles collections stuck in transitional states.

    Implements ``PeriodicService``: ``BackgroundSupervisor`` handles leadership
    election via ``_LIFECYCLE_REAPER_ADVISORY_LOCK_KEY`` and the configured
    cadence.  Each tick calls ``run_once()`` which scans for collections stuck
    in PROVISIONING or DELETING past the threshold and reconciles them.
    """

    name = "lifecycle_reaper"
    leadership = Leadership.LEADER_ONLY
    pod_policy = PodPolicy.SKIP_EPHEMERAL

    def __init__(self, config: LifecycleReaperConfig) -> None:
        self._config = config
        self.cadence_seconds = config.reaper_interval_seconds
        self.lock_key: Optional[Union[int, str]] = _LIFECYCLE_REAPER_ADVISORY_LOCK_KEY

    async def tick(self, ctx: ServiceContext) -> None:
        """One full scan for stuck PROVISIONING and DELETING collections."""
        await self.run_once()

    async def run_once(self) -> None:
        """Perform one full scan for stuck PROVISIONING and DELETING collections.

        Safe to call directly in tests; raises on unexpected errors so the
        outer loop can log and retry.
        """
        if not self._config.enabled:
            logger.debug("lifecycle_reaper: disabled via config — skipping scan.")
            return

        threshold = self._config.stuck_threshold_seconds
        limit = self._config.batch_size

        catalog_ids = await self._list_active_catalogs()
        if not catalog_ids:
            logger.debug("lifecycle_reaper: no active catalogs; skipping scan.")
            return

        for catalog_id in catalog_ids:
            await self._reap_provisioning_in_schema(
                catalog_id, catalog_id, threshold, limit
            )
            await self._reap_deleting_in_schema(
                catalog_id, catalog_id, threshold, limit
            )

    # ------------------------------------------------------------------
    # Catalog enumeration
    # ------------------------------------------------------------------

    async def _list_active_catalogs(self) -> list[str]:
        """Return catalog ids (= schema names) for all non-deleted catalogs."""
        engine = get_engine()
        if engine is None:
            logger.warning(
                "lifecycle_reaper: no DB engine — skipping catalog enumeration."
            )
            return []

        try:
            async with managed_transaction(engine) as conn:
                rows = await DQLQuery(
                    _ACTIVE_CATALOG_SCHEMAS_SQL,
                    result_handler=ResultHandler.ALL,
                ).execute(conn)
        except Exception as exc:
            logger.warning(
                "lifecycle_reaper: failed to enumerate active catalogs: %s", exc
            )
            return []

        return [r[0] for r in rows] if rows else []

    # ------------------------------------------------------------------
    # PROVISIONING reap: clear overlay back to NULL (→ ACTIVE)
    # ------------------------------------------------------------------

    async def _reap_provisioning_in_schema(
        self,
        catalog_id: str,
        schema: str,
        threshold_seconds: int,
        limit: int,
    ) -> None:
        """Clear stale PROVISIONING overlays in one tenant schema."""
        engine = get_engine()
        if engine is None:
            return

        query_sql = _STUCK_PROVISIONING_SQL.format(schema=schema)
        try:
            async with managed_transaction(engine) as conn:
                rows = await DQLQuery(
                    query_sql,
                    result_handler=ResultHandler.ALL,
                ).execute(conn, threshold_seconds=threshold_seconds, limit=limit)
        except Exception as exc:
            logger.warning(
                "lifecycle_reaper: failed to query stuck PROVISIONING collections "
                "in schema %r (catalog %r): %s",
                schema, catalog_id, exc,
            )
            return

        if not rows:
            return

        collection_ids = [r[0] for r in rows]
        logger.warning(
            "lifecycle_reaper: found %d stuck PROVISIONING collection(s) in "
            "catalog %r (threshold=%ds): %s — clearing to ACTIVE.",
            len(collection_ids), catalog_id, threshold_seconds, collection_ids,
        )

        for collection_id in collection_ids:
            await self._clear_provisioning(
                catalog_id, schema, collection_id
            )

    async def _clear_provisioning(
        self,
        catalog_id: str,
        schema: str,
        collection_id: str,
    ) -> None:
        """Clear the PROVISIONING overlay for one collection (→ ACTIVE).

        Runs in its own committed transaction so the state change is
        visible cross-pod immediately.  Invalidates the in-process
        liveness/routing caches so subsequent reads see ACTIVE.
        """
        engine = get_engine()
        if engine is None:
            return

        update_sql = _CLEAR_PROVISIONING_SQL.format(schema=schema)
        try:
            async with managed_transaction(engine) as conn:
                rows = await DQLQuery(
                    update_sql,
                    result_handler=ResultHandler.ROWCOUNT,
                ).execute(conn, id=collection_id)
        except Exception as exc:
            logger.exception(
                "lifecycle_reaper: failed to clear PROVISIONING for "
                "%r/%r — will retry next tick.",
                catalog_id, collection_id,
            )
            raise RuntimeError(
                f"lifecycle_reaper: failed to clear PROVISIONING for "
                f"{catalog_id!r}/{collection_id!r}"
            ) from exc

        if rows:
            # Invalidate in-process liveness/routing caches so subsequent
            # reads see ACTIVE, not stale PROVISIONING.
            try:
                _invalidate_collection_lifecycle_caches(catalog_id, collection_id)
            except Exception as exc:
                logger.warning(
                    "lifecycle_reaper: cache invalidation failed for "
                    "%r/%r after PROVISIONING clear: %s",
                    catalog_id, collection_id, exc,
                )
            logger.info(
                "lifecycle_reaper: cleared PROVISIONING for %r/%r "
                "(threshold=%ds) — collection is now ACTIVE.",
                catalog_id, collection_id, self._config.stuck_threshold_seconds,
            )
        else:
            # Row was already updated by another reaper pod or concurrently
            # cleared by the pod's own finalizer — no-op.
            logger.debug(
                "lifecycle_reaper: %r/%r PROVISIONING already cleared "
                "(another pod won the race).",
                catalog_id, collection_id,
            )

    # ------------------------------------------------------------------
    # DELETING reap: re-drive force-delete (idempotent self-heal)
    # ------------------------------------------------------------------

    async def _reap_deleting_in_schema(
        self,
        catalog_id: str,
        schema: str,
        threshold_seconds: int,
        limit: int,
    ) -> None:
        """Re-drive stuck DELETING collections in one tenant schema."""
        engine = get_engine()
        if engine is None:
            return

        query_sql = _STUCK_DELETING_SQL.format(schema=schema)
        try:
            async with managed_transaction(engine) as conn:
                rows = await DQLQuery(
                    query_sql,
                    result_handler=ResultHandler.ALL,
                ).execute(conn, threshold_seconds=threshold_seconds, limit=limit)
        except Exception as exc:
            logger.warning(
                "lifecycle_reaper: failed to query stuck DELETING collections "
                "in schema %r (catalog %r): %s",
                schema, catalog_id, exc,
            )
            return

        if not rows:
            return

        collection_ids = [r[0] for r in rows]
        logger.warning(
            "lifecycle_reaper: found %d stuck DELETING collection(s) in "
            "catalog %r (threshold=%ds): %s — re-driving force delete.",
            len(collection_ids), catalog_id, threshold_seconds, collection_ids,
        )

        from dynastore.models.protocols import CatalogsProtocol

        catalogs_svc = get_protocol(CatalogsProtocol)
        if catalogs_svc is None:
            logger.error(
                "lifecycle_reaper: CatalogsProtocol not available — "
                "cannot re-drive stuck DELETING collections this tick."
            )
            return

        for collection_id in collection_ids:
            try:
                await catalogs_svc.delete_collection(
                    catalog_id, collection_id, force=True
                )
                logger.info(
                    "lifecycle_reaper: re-drove force delete for stuck "
                    "DELETING collection %r/%r (threshold=%ds).",
                    catalog_id, collection_id, threshold_seconds,
                )
            except Exception:
                logger.exception(
                    "lifecycle_reaper: failed to re-drive force delete for "
                    "%r/%r — will retry next tick.",
                    catalog_id, collection_id,
                )


async def load_lifecycle_reaper_config() -> LifecycleReaperConfig:
    """Load ``LifecycleReaperConfig`` from the platform config store.

    Falls back to the default instance if the store is unavailable or the
    config has not been set.
    """
    try:
        from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        config_mgr = get_protocol(PlatformConfigsProtocol)
        if config_mgr is not None:
            cfg = await config_mgr.get_config(LifecycleReaperConfig)
            if isinstance(cfg, LifecycleReaperConfig):
                return cfg
    except Exception as exc:
        logger.warning(
            "lifecycle_reaper: failed to load LifecycleReaperConfig "
            "(%s) — using defaults.", exc,
        )
    return LifecycleReaperConfig()
