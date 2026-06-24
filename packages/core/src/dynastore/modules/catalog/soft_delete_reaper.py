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

"""Soft-delete TTL reaper — promotes SOFT→HARD after a grace period.

Background loop that periodically finds catalogs and collections whose
``deleted_at`` timestamp has aged past the configured grace window and
re-runs the hard-delete path (cascade snapshot + enqueue + physical teardown).

Architecture contract
---------------------
- Scheduling: a leader-elected asyncio background loop (``run_leader_loop``
  with a pg advisory lock) runs the reaper callable on every pod; exactly
  one pod wins the lock and performs the scan per cadence tick.
- No-race invariant: the query uses ``FOR UPDATE SKIP LOCKED`` so concurrent
  reaper instances and concurrent operator hard-deletes never double-process
  the same entity.  In-flight cascade tasks are checked via a sub-select
  against ``tasks.tasks`` — if a cascade task already exists and is not yet
  terminal, the entity is skipped this round.
- Reuse of existing hard-delete path: ``delete_catalog(force=True)`` and
  ``delete_collection(force=True)`` are called directly; they own all lifecycle
  events, cascade snapshot-and-enqueue, and physical teardown.  The reaper
  adds zero new DDL or cleanup logic.
- Config: ``SoftDeleteReaperConfig`` (PluginConfig) with a
  ``soft_grace_period_seconds`` field (default 7 days).  Read once at loop
  start; live changes apply on next pod restart (same model as
  ``TasksPluginConfig.hard_retry_cap``).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Tuple, Union

from pydantic import Field

from dynastore.models.mutability import Mutable
from dynastore.models.plugin_config import PluginConfig
from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.tools.background_service import (
    Leadership,
    PeriodicService,
    PodPolicy,
    ServiceContext,
)
from dynastore.tools.discovery import get_protocol
from dynastore.tools.protocol_helpers import get_engine

if TYPE_CHECKING:
    from dynastore.modules.db_config.query_executor import DbResource

logger = logging.getLogger(__name__)

# Advisory lock key for leader election — must not collide with other loops.
_REAPER_ADVISORY_LOCK_KEY = 0x5D3A7E1F_C2B84961  # deterministic constant


class SoftDeleteReaperConfig(PluginConfig):
    """Configuration for the soft-delete TTL reaper background loop."""

    _address: ClassVar[Tuple[str, ...]] = ("platform", "modules", "catalog")

    enabled: Mutable[bool] = Field(
        default=False,
        description=(
            "Master switch for the soft-delete TTL reaper.  Defaults to False: "
            "the reaper performs an irreversible hard delete (physical schema "
            "teardown) of any entity soft-deleted longer than the grace window, "
            "including tombstones created before this reaper existed, so it is "
            "opt-in.  An operator enables it via the configs API after reviewing "
            "existing soft-deleted entities and soaking on a non-production "
            "environment.  When False the background loop starts but performs "
            "no scans."
        ),
    )

    soft_grace_period_seconds: Mutable[int] = Field(
        default=604800,  # 7 days
        ge=60,
        description=(
            "Seconds a catalog or collection must remain soft-deleted before "
            "the reaper promotes it to a hard delete.  Default: 604800 (7 days). "
            "Minimum: 60 seconds (lab / test environments).  Changes take effect "
            "on the next pod restart."
        ),
    )

    reaper_interval_seconds: Mutable[float] = Field(
        default=3600.0,  # 1 hour
        ge=30.0,
        description=(
            "How often (seconds) the reaper scans for entities past the grace "
            "window.  Default: 3600 (1 hour).  Lower values increase scan "
            "frequency; higher values reduce DB load.  Changes take effect on "
            "the next pod restart."
        ),
    )

    batch_size: Mutable[int] = Field(
        default=50,
        ge=1,
        le=500,
        description=(
            "Maximum entities processed per reaper tick (catalogs + collections "
            "each capped separately).  Guards against long-running transactions "
            "when many entities expire simultaneously."
        ),
    )


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

# Returns catalog IDs whose deleted_at has aged past the grace interval AND
# that have no in-flight (non-terminal) cascade_cleanup task in system.tasks.
# FOR UPDATE SKIP LOCKED prevents two reaper instances from racing.
_OVERDUE_CATALOGS_SQL = """
SELECT id
FROM catalog.catalogs
WHERE deleted_at IS NOT NULL
  AND deleted_at + (:grace_seconds || ' seconds')::INTERVAL < NOW()
  AND NOT EXISTS (
    SELECT 1
    FROM tasks.tasks t
    WHERE t.task_type = 'cascade_cleanup'
      AND t.status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER')
      AND t.inputs->>'scope_ref' IS NOT NULL
      AND (t.inputs->'scope_ref'->>'scope') = 'catalog'
      AND (t.inputs->'scope_ref'->>'catalog_id') = catalog.catalogs.id
  )
ORDER BY deleted_at
LIMIT :limit
FOR UPDATE SKIP LOCKED
"""

# Returns (catalog_id, collection_id) pairs for collections whose deleted_at
# has aged past the grace interval and have no in-flight cascade task.
_OVERDUE_COLLECTIONS_SQL = """
SELECT cat.id AS catalog_id, col.id AS collection_id
FROM catalog.catalogs cat
JOIN LATERAL (
    SELECT id
    FROM "{schema}".collections col_inner
    WHERE col_inner.deleted_at IS NOT NULL
      AND col_inner.deleted_at + (:grace_seconds || ' seconds')::INTERVAL < NOW()
      AND NOT EXISTS (
        SELECT 1
        FROM tasks.tasks t
        WHERE t.task_type = 'cascade_cleanup'
          AND t.status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER')
          AND t.inputs->>'scope_ref' IS NOT NULL
          AND (t.inputs->'scope_ref'->>'scope') = 'collection'
          AND (t.inputs->'scope_ref'->>'catalog_id') = cat.id
          AND (t.inputs->'scope_ref'->>'collection_id') = col_inner.id
      )
    ORDER BY col_inner.deleted_at
    LIMIT :limit
    FOR UPDATE SKIP LOCKED
) col ON true
WHERE cat.deleted_at IS NULL
ORDER BY cat.id, col.id
"""


class SoftDeleteReaper(PeriodicService):
    """Periodic reaper that promotes soft-deleted entities to hard-deleted.

    Implements ``PeriodicService``: ``BackgroundSupervisor`` handles leadership
    election via ``_REAPER_ADVISORY_LOCK_KEY`` and the configured cadence.
    Each tick calls ``run_once()`` which scans for entities past the grace
    window and hard-deletes them via the existing force-delete path.
    """

    name = "soft_delete_reaper"
    leadership = Leadership.LEADER_ONLY
    pod_policy = PodPolicy.SKIP_EPHEMERAL

    def __init__(self, config: SoftDeleteReaperConfig) -> None:
        self._config = config
        self.cadence_seconds = config.reaper_interval_seconds
        self.lock_key: Optional[Union[int, str]] = _REAPER_ADVISORY_LOCK_KEY

    async def tick(self, ctx: ServiceContext) -> None:
        """One reaper scan across catalogs and collections."""
        await self.run_once()

    async def run_once(self) -> None:
        """Perform one full reaper scan across catalogs and collections.

        Safe to call directly in tests; raises on unexpected errors so the
        outer loop can log and retry.
        """
        if not self._config.enabled:
            logger.debug("soft_delete_reaper: disabled via config — skipping scan.")
            return

        grace = self._config.soft_grace_period_seconds
        limit = self._config.batch_size

        await self._reap_catalogs(grace, limit)
        await self._reap_collections(grace, limit)

    # ------------------------------------------------------------------
    # Catalog reaping
    # ------------------------------------------------------------------

    async def _reap_catalogs(self, grace_seconds: int, limit: int) -> None:
        """Find and hard-delete catalogs past the grace window."""
        engine = get_engine()
        if engine is None:
            logger.warning("soft_delete_reaper: no DB engine — skipping catalog scan.")
            return

        async with managed_transaction(engine) as conn:
            rows = await DQLQuery(
                _OVERDUE_CATALOGS_SQL,
                result_handler=ResultHandler.ALL,
            ).execute(conn, grace_seconds=grace_seconds, limit=limit)

        if not rows:
            logger.debug("soft_delete_reaper: no overdue catalogs this tick.")
            return

        catalog_ids = [r[0] for r in rows]
        logger.info(
            "soft_delete_reaper: found %d overdue catalog(s): %s",
            len(catalog_ids), catalog_ids,
        )

        from dynastore.models.protocols import CatalogsProtocol

        catalogs_svc = get_protocol(CatalogsProtocol)
        if catalogs_svc is None:
            logger.error(
                "soft_delete_reaper: CatalogsProtocol not available — "
                "cannot hard-delete catalogs this tick."
            )
            return

        for catalog_id in catalog_ids:
            try:
                await catalogs_svc.delete_catalog(catalog_id, force=True)
                logger.info(
                    "soft_delete_reaper: hard-deleted catalog %r (grace=%ds).",
                    catalog_id, grace_seconds,
                )
            except Exception:
                logger.exception(
                    "soft_delete_reaper: failed to hard-delete catalog %r — "
                    "will retry next tick.",
                    catalog_id,
                )

    # ------------------------------------------------------------------
    # Collection reaping
    # ------------------------------------------------------------------

    async def _reap_collections(self, grace_seconds: int, limit: int) -> None:
        """Find and hard-delete collections past the grace window."""
        engine = get_engine()
        if engine is None:
            logger.warning(
                "soft_delete_reaper: no DB engine — skipping collection scan."
            )
            return

        # Collections live in per-tenant schemas.  Enumerate active catalogs
        # first, then query each tenant schema for overdue collections.
        catalog_ids = await self._list_active_catalog_ids(engine)
        if not catalog_ids:
            logger.debug(
                "soft_delete_reaper: no active catalogs; skipping collection scan."
            )
            return

        from dynastore.models.protocols import CatalogsProtocol

        catalogs_svc = get_protocol(CatalogsProtocol)
        if catalogs_svc is None:
            logger.error(
                "soft_delete_reaper: CatalogsProtocol not available — "
                "cannot hard-delete collections this tick."
            )
            return

        for catalog_id in catalog_ids:
            await self._reap_collections_in_schema(
                catalog_id, catalog_id, grace_seconds, limit, catalogs_svc
            )

    async def _list_active_catalog_ids(
        self, engine: "DbResource"
    ) -> list[str]:
        """Return catalog ids (= schema names) for all non-deleted catalogs."""
        async with managed_transaction(engine) as conn:
            rows = await DQLQuery(
                "SELECT id FROM catalog.catalogs "
                "WHERE deleted_at IS NULL ORDER BY id",
                result_handler=ResultHandler.ALL,
            ).execute(conn)
        return [r[0] for r in rows] if rows else []

    async def _reap_collections_in_schema(
        self,
        catalog_id: str,
        schema: str,
        grace_seconds: int,
        limit: int,
        catalogs_svc: Any,
    ) -> None:
        """Scan one tenant schema for overdue soft-deleted collections."""
        engine = get_engine()
        if engine is None:
            return

        # The LATERAL join query uses a literal schema name; build it safely.
        query_sql = (
            "SELECT col.id AS collection_id "
            f'FROM "{schema}".collections col '
            "WHERE col.deleted_at IS NOT NULL "
            "  AND col.deleted_at + (:grace_seconds || ' seconds')::INTERVAL < NOW() "
            "  AND NOT EXISTS ( "
            "    SELECT 1 FROM tasks.tasks t "
            "    WHERE t.task_type = 'cascade_cleanup' "
            "      AND t.status NOT IN ('COMPLETED', 'FAILED', 'DEAD_LETTER') "
            "      AND t.inputs->>'scope_ref' IS NOT NULL "
            "      AND (t.inputs->'scope_ref'->>'scope') = 'collection' "
            "      AND (t.inputs->'scope_ref'->>'catalog_id') = :catalog_id "
            "      AND (t.inputs->'scope_ref'->>'collection_id') = col.id "
            "  ) "
            "ORDER BY col.deleted_at "
            "LIMIT :limit "
            "FOR UPDATE SKIP LOCKED"
        )

        try:
            async with managed_transaction(engine) as conn:
                rows = await DQLQuery(
                    query_sql,
                    result_handler=ResultHandler.ALL,
                ).execute(
                    conn,
                    grace_seconds=grace_seconds,
                    catalog_id=catalog_id,
                    limit=limit,
                )
        except Exception:
            logger.exception(
                "soft_delete_reaper: failed to query collections in "
                "schema %r (catalog %r).",
                schema, catalog_id,
            )
            return

        if not rows:
            return

        collection_ids = [r[0] for r in rows]
        logger.info(
            "soft_delete_reaper: found %d overdue collection(s) in catalog %r: %s",
            len(collection_ids), catalog_id, collection_ids,
        )

        for collection_id in collection_ids:
            try:
                await catalogs_svc.delete_collection(
                    catalog_id, collection_id, force=True
                )
                logger.info(
                    "soft_delete_reaper: hard-deleted collection %r/%r "
                    "(grace=%ds).",
                    catalog_id, collection_id, grace_seconds,
                )
            except Exception:
                logger.exception(
                    "soft_delete_reaper: failed to hard-delete collection "
                    "%r/%r — will retry next tick.",
                    catalog_id, collection_id,
                )


async def load_reaper_config() -> SoftDeleteReaperConfig:
    """Load ``SoftDeleteReaperConfig`` from the platform config store.

    Falls back to the default instance if the store is unavailable or
    the config has not been set.
    """
    try:
        from dynastore.models.protocols.platform_configs import PlatformConfigsProtocol
        from dynastore.tools.discovery import get_protocol

        config_mgr = get_protocol(PlatformConfigsProtocol)
        if config_mgr is not None:
            cfg = await config_mgr.get_config(SoftDeleteReaperConfig)
            if isinstance(cfg, SoftDeleteReaperConfig):
                return cfg
    except Exception as exc:
        logger.warning(
            "soft_delete_reaper: failed to load SoftDeleteReaperConfig "
            "(%s) — using defaults.", exc,
        )
    return SoftDeleteReaperConfig()
