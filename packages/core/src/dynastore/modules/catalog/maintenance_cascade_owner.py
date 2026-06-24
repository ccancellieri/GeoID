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

"""Cascade cleanup owner for pending maintenance work.

:class:`MaintenancePendingOwner` cancels still-pending ``tasks.tasks`` rows
that belong to a catalog or collection being hard-deleted.  Completed /
terminal rows are immutable and age out via the monthly partition-drop GC —
they are not deleted per-tenant.

Only PENDING rows are touched.  ACTIVE rows may be mid-flight (including the
cascade_cleanup task that drives this very owner); they are left untouched so
the worker can complete or fail them naturally.

The tasks table lives in the ``tasks`` PG schema (``get_task_schema()``).
Rows are discriminated by the ``schema_name`` column, which is the tenant
physical schema (e.g. ``s_abc123``).  The cascade_cleanup task itself is
created with ``schema_name='system'`` and is therefore structurally excluded
by the tenant-schema filter — a defensive ``AND schema_name <> 'system'``
predicate reinforces this.

Event rows in ``tasks.events`` are reclaimed by the DROP-PARTITION retention
GC when their day partition is dropped; there is no per-row cancel needed.
The legacy ``events.events`` table has been removed.

``physical_schema`` is resolved at ``describe_scope`` time (inside the delete
transaction, while ``catalog.catalogs`` still has the row) and snapshotted
into :attr:`CleanupRef.metadata`.  ``cleanup_one`` reads it from the snapshot
so it does not need to re-query a row that may no longer exist.

Register via :func:`register_owners` from the catalog module lifespan BEFORE
the :class:`~dynastore.modules.catalog.cascade_registry.CascadeCleanupRegistry`
is frozen.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any, ClassVar, Iterable

from dynastore.modules.catalog.resource_owner import (
    BaseResourceOwner,
    CleanupMode,
    CleanupOutcome,
    CleanupRef,
    ResourceScope,
    ScopeRef,
)

if TYPE_CHECKING:
    from dynastore.modules.catalog.cascade_registry import CascadeCleanupRegistry

logger = logging.getLogger(__name__)

# PG schema that holds the partitioned tasks table.
# Mirrors tasks_module.get_task_schema() without importing the module at load time.
_TASKS_PG_SCHEMA = os.getenv("DYNASTORE_TASK_SCHEMA", "tasks")

# Reason written to error_message when rows are cancelled by cascade teardown.
_CANCEL_REASON = "cancelled: owning element was hard-deleted"


class MaintenancePendingOwner(BaseResourceOwner):
    """Cancels pending tasks rows when their owner element is deleted.

    Supported scopes: CATALOG and COLLECTION (default from
    :class:`~dynastore.modules.catalog.resource_owner.BaseResourceOwner`).
    """

    owner_id: ClassVar[str] = "maintenance.pending_work"

    def supported_scopes(self) -> Iterable[ResourceScope]:
        return (ResourceScope.CATALOG, ResourceScope.COLLECTION)

    async def describe_scope(
        self, scope_ref: ScopeRef, conn: Any
    ) -> list[CleanupRef]:
        """Snapshot the tenant physical_schema for later use in cleanup_one.

        The catalog row is still alive inside the delete transaction when
        describe_scope is called, so the lookup is safe here.  The resolved
        physical_schema is embedded in metadata so cleanup_one can operate
        after the catalog row is gone.

        Returns an empty list when the physical_schema cannot be resolved
        (catalog not found or already deleted).
        """
        from dynastore.modules.db_config.query_executor import (
            DQLQuery,
            ResultHandler,
        )

        schema: str | None = await DQLQuery(
            "SELECT id FROM catalog.catalogs"
            " WHERE id = :catalog_id AND deleted_at IS NULL;",
            result_handler=ResultHandler.SCALAR_ONE_OR_NONE,
        ).execute(conn, catalog_id=scope_ref.catalog_id)

        if not schema:
            logger.warning(
                "MaintenancePendingOwner: could not resolve schema "
                "for catalog_id=%r — skipping cleanup ref.",
                scope_ref.catalog_id,
            )
            return []

        metadata: dict[str, Any] = {
            "schema": schema,
            "catalog_id": scope_ref.catalog_id,
        }
        if scope_ref.scope == ResourceScope.COLLECTION and scope_ref.collection_id:
            metadata["collection_id"] = scope_ref.collection_id

        locator = scope_ref.catalog_id
        if scope_ref.collection_id:
            locator = f"{scope_ref.catalog_id}/{scope_ref.collection_id}"

        return [
            CleanupRef(
                kind="maintenance_pending",
                locator=locator,
                owner_id=self.owner_id,
                metadata=metadata,
            )
        ]

    async def cleanup_one(
        self,
        ref: CleanupRef,
        mode: CleanupMode,
        *,
        dry_run: bool = False,
    ) -> CleanupOutcome:
        """Mark PENDING tasks for the deleted element as DEAD_LETTER.

        SOFT mode is a no-op (maintenance rows are not tombstoned, only
        hard-deleted owners are relevant).  dry_run logs the intent and
        returns DONE without touching the database.

        Only PENDING rows are updated; ACTIVE rows may be mid-flight.
        The tasks schema_name='system' guard is structural (tenant schema
        never equals 'system') and is reinforced by an explicit predicate.

        Event rows in ``tasks.events`` are reclaimed by DROP-PARTITION
        retention and do not need per-row cancellation.
        """
        if mode == CleanupMode.SOFT:
            return CleanupOutcome.DONE

        schema: str = ref.metadata.get("schema", "")
        catalog_id: str = ref.metadata.get("catalog_id", ref.locator)
        collection_id: str | None = ref.metadata.get("collection_id")

        if not schema:
            logger.error(
                "MaintenancePendingOwner: CleanupRef missing 'schema' in metadata "
                "(locator=%r) — cannot proceed.",
                ref.locator,
            )
            return CleanupOutcome.DEAD

        if dry_run:
            logger.info(
                "MaintenancePendingOwner: dry-run — would cancel PENDING tasks "
                "for schema=%r catalog_id=%r collection_id=%r.",
                schema, catalog_id, collection_id,
            )
            return CleanupOutcome.DONE

        try:
            from dynastore.modules.db_config.query_executor import (
                managed_transaction,
            )
            from dynastore.tools.protocol_helpers import get_engine

            engine = get_engine()
            if engine is None:
                logger.error(
                    "MaintenancePendingOwner: no DB engine — cannot cancel "
                    "maintenance rows for schema=%r.",
                    schema,
                )
                return CleanupOutcome.RETRY

            async with managed_transaction(engine) as conn:
                tasks_rows = await _cancel_pending_tasks(conn, schema, collection_id)

            logger.info(
                "MaintenancePendingOwner: cancelled %d task(s) "
                "for schema=%r catalog_id=%r collection_id=%r.",
                tasks_rows, schema, catalog_id, collection_id,
            )
            return CleanupOutcome.DONE

        except Exception as exc:  # noqa: BLE001
            logger.error(
                "MaintenancePendingOwner: failed to cancel pending work for "
                "schema=%r catalog_id=%r collection_id=%r: %s",
                schema, catalog_id, collection_id, exc, exc_info=True,
            )
            return CleanupOutcome.RETRY


# ---------------------------------------------------------------------------
# SQL helpers — extracted to keep cleanup_one under 20 lines
# ---------------------------------------------------------------------------


async def _cancel_pending_tasks(
    conn: Any,
    schema: str,
    collection_id: str | None,
) -> int:
    """UPDATE tasks.tasks PENDING → DEAD_LETTER for the given tenant schema.

    Scoped by ``catalog_id = :schema`` (the catalog internal id, which equals
    the tenant schema name).  ``AND catalog_id <> 'system'`` is a defensive
    guard that ensures the cascade_cleanup task itself — which uses
    ``catalog_id='system'`` — is never touched; the tenant filter already
    structurally excludes it.

    COLLECTION scope adds ``AND collection_id = :collection_id``.
    Only PENDING rows are touched; ACTIVE rows may be mid-flight.
    """
    from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler

    collection_clause = (
        "AND collection_id = :collection_id" if collection_id is not None else ""
    )
    sql = (
        f"UPDATE {_TASKS_PG_SCHEMA}.tasks"
        f" SET status = 'DEAD_LETTER',"
        f"     error_message = :reason,"
        f"     finished_at = NOW()"
        f" WHERE catalog_id = :schema"
        f"   AND catalog_id <> 'system'"
        f"   AND status = 'PENDING'"
        f"   {collection_clause}"
    )
    params: dict[str, Any] = {"schema": schema, "reason": _CANCEL_REASON}
    if collection_id is not None:
        params["collection_id"] = collection_id

    result: int | None = await DQLQuery(
        sql, result_handler=ResultHandler.ROWCOUNT
    ).execute(conn, **params)
    return result or 0


def register_owners(registry: "CascadeCleanupRegistry") -> None:
    """Register maintenance cascade owners into *registry*.

    Call from the catalog module lifespan BEFORE
    :meth:`~dynastore.modules.catalog.cascade_registry.CascadeCleanupRegistry.freeze`.
    """
    registry.register(MaintenancePendingOwner())
    logger.info(
        "CatalogModule: registered cascade owner %r.",
        MaintenancePendingOwner.owner_id,
    )
