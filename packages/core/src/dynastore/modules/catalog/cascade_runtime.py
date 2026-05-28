#    Copyright 2026 FAO
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

"""Cascade orchestrator — snapshot and enqueue cleanup work.

The :class:`CascadeOrchestrator` is the single entry point for triggering
cascade cleanup when a scope (catalog, collection, …) is hard-deleted.

Transaction boundary constraint (#1456)
---------------------------------------
The task framework's ``create_task`` function always opens its own
``managed_transaction`` — it cannot participate in the caller's open
connection.  This means the enqueue step is *not* atomic with the
schema-drop: if the process crashes between ``DROP SCHEMA`` and the task
INSERT, the cleanup task is lost.

Mitigation: the task payload is a *snapshot* of the CleanupRefs collected
*before* the schema drop, inside the caller's transaction.  The snapshot is
the only state needed to run cleanup.  A future improvement (#1456 TODO)
could write the snapshot into a durable staging row within the caller's
transaction using a bare DQLQuery INSERT — but that requires a dedicated
staging table (new DDL), which violates the "no new tables" constraint for
this chunk.

For now: the caller MUST call ``snapshot_and_enqueue`` BEFORE the schema
drop so that ``describe_scope`` can still read the schema rows.  The
enqueue happens after the caller's transaction commits.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional

from dynastore.modules.catalog.cascade_registry import (
    CascadeCleanupRegistry,
    cascade_cleanup_registry,
)
from dynastore.modules.catalog.resource_owner import CleanupMode, ResourceScope, ScopeRef

if TYPE_CHECKING:
    from dynastore.modules.db_config.query_executor import DbResource

logger = logging.getLogger(__name__)


def _get_default_registry() -> CascadeCleanupRegistry:
    return cascade_cleanup_registry


class CascadeOrchestrator:
    """Snapshot CleanupRefs from all registered owners and enqueue cleanup.

    Args:
        registry: The :class:`CascadeCleanupRegistry` to use.  Defaults to
            the process-global singleton.  Pass a fresh instance in tests.
    """

    def __init__(
        self, registry: Optional[CascadeCleanupRegistry] = None
    ) -> None:
        self._registry = registry if registry is not None else _get_default_registry()

    async def snapshot_and_enqueue(
        self,
        conn: "DbResource",
        scope_ref: ScopeRef,
        mode: CleanupMode,
    ) -> Optional[str]:
        """Collect CleanupRefs from all owners and enqueue a cascade_cleanup task.

        Must be called BEFORE the schema drop so that ``describe_scope`` can
        read live rows.  The enqueue happens in a separate transaction after
        the caller's transaction commits — see module docstring for the
        atomicity constraint.

        Args:
            conn: Active database connection (caller's transaction — used only
                for ``describe_scope`` calls; the task INSERT is in its own tx).
            scope_ref: Identifies the entity being deleted.
            mode: ``SOFT`` or ``HARD`` cleanup policy.

        Returns:
            The task_id string if a task was created, ``None`` if no refs were
            found (nothing to clean up).
        """
        owners = self._registry.owners_for_scope(scope_ref.scope)
        all_refs: list[dict[str, Any]] = []

        for owner in owners:
            try:
                refs = await owner.describe_scope(scope_ref, conn)
            except Exception as exc:
                # Fail-closed (#1456/#1469): re-raise so the caller's
                # transaction rolls back and the schema drop is aborted.
                # Log first with full owner context — without this signal
                # the operator sees only the rolled-back delete and has
                # no way to attribute the failure to a specific owner.
                logger.error(
                    "cascade_describe_scope_failed: owner_id=%r scope=%r "
                    "catalog_id=%r collection_id=%r mode=%r error=%s — "
                    "snapshot aborted; caller transaction will roll back "
                    "and the schema drop will NOT proceed (fail-closed). "
                    "Fix the owner or unregister it to unblock the delete.",
                    owner.owner_id, scope_ref.scope.value,
                    scope_ref.catalog_id, scope_ref.collection_id,
                    mode.value, exc,
                    exc_info=True,
                )
                raise
            for ref in refs:
                all_refs.append(ref.to_json())

        if not all_refs:
            logger.debug(
                "cascade_runtime: no CleanupRefs from %d owner(s) for %r — "
                "skipping task enqueue.",
                len(owners), scope_ref,
            )
            return None

        try:
            task_id = await self._enqueue(scope_ref, mode, all_refs)
        except Exception as exc:
            # Fail-closed (#1456/#1469): re-raise. The schema drop will be
            # aborted, but every CleanupRef captured this round is lost
            # because the caller's transaction rolls back. The operator
            # needs the ref count to assess blast radius on retry.
            logger.error(
                "cascade_enqueue_failed: scope=%r catalog_id=%r mode=%r "
                "ref_count=%d error=%s — task INSERT failed; caller "
                "transaction will roll back and the schema drop will NOT "
                "proceed (fail-closed). Investigate the task storage "
                "before retrying the delete.",
                scope_ref.scope.value, scope_ref.catalog_id, mode.value,
                len(all_refs), exc,
                exc_info=True,
            )
            raise
        logger.info(
            "cascade_runtime: enqueued cascade_cleanup task %s "
            "(%d ref(s)) for scope %r catalog_id=%r.",
            task_id, len(all_refs), scope_ref.scope, scope_ref.catalog_id,
        )
        return task_id

    async def _enqueue(
        self,
        scope_ref: ScopeRef,
        mode: CleanupMode,
        refs: list[dict[str, Any]],
    ) -> str:
        """Insert a cascade_cleanup task row and return its task_id.

        NOTE (#1456): ``create_task`` opens its own managed_transaction, so
        this INSERT is NOT in the caller's transaction.  See module docstring.
        """
        from dynastore.models.tasks import TaskCreate, TaskScope
        from dynastore.modules.tasks.tasks_module import create_task
        from dynastore.tools.protocol_helpers import get_engine

        inputs_payload = {
            "scope_ref": {
                "scope": scope_ref.scope.value,
                "catalog_id": scope_ref.catalog_id,
                "collection_id": scope_ref.collection_id,
                "asset_id": scope_ref.asset_id,
                "item_id": scope_ref.item_id,
            },
            "mode": mode.value,
            "refs": refs,
        }

        task_data = TaskCreate(
            task_type="cascade_cleanup",
            caller_id="system:cascade_orchestrator",
            inputs=inputs_payload,
            scope=TaskScope.SYSTEM,
        )

        engine = get_engine()
        if engine is None:
            raise RuntimeError(
                "cascade_runtime: no database engine available — "
                "ensure DBModule is initialized before CascadeOrchestrator.snapshot_and_enqueue."
            )
        task = await create_task(engine, task_data, schema="system")
        if task is None:
            raise RuntimeError(
                "cascade_runtime: create_task returned None (dedup hit — unexpected)."
            )
        return str(task.task_id)
