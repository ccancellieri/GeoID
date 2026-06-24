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

"""cascade_cleanup task runner.

Deserializes :class:`CleanupRef` instances from the task payload, looks up
each owner in the :data:`~dynastore.modules.catalog.cascade_registry.cascade_cleanup_registry`,
and calls :meth:`~dynastore.modules.catalog.resource_owner.ResourceOwnerProtocol.cleanup_one`.

Outcome contract:
- All DONE  → task completes successfully.
- Any RETRY → RuntimeError raised so the task framework retries with backoff.
  Per-ref retry count is incremented in the task payload on each attempt.
  After ``max_ref_retries`` attempts (default 5, configurable in task inputs),
  the ref is marked DEAD and processing continues so other refs are not blocked.
- Any DEAD  → permanent failure; does not block other refs.  When DEAD refs
  remain after all refs are processed, the task raises
  :class:`PermanentTaskFailure` so the dispatcher dead-letters it immediately
  **without retrying**.  A DEAD ref can never succeed on re-run — its owner is
  unregistered, its payload is malformed, the owner returned DEAD, or per-ref
  retries are exhausted.  Raising the retryable ``RuntimeError`` here instead
  would let the dispatcher reset the row to PENDING and re-claim the same doomed
  task every backoff cycle forever, emitting a ``task.failed`` event each time —
  an unbounded loop that never drains.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List

from dynastore.tasks.protocols import TaskProtocol
from dynastore.tasks.cascade_cleanup.inputs import CascadeCleanupInputs
from dynastore.modules.tasks.models import PermanentTaskFailure

logger = logging.getLogger(__name__)

_DEFAULT_MAX_REF_RETRIES = 5
_RETRY_COUNT_KEY = "_retry_count"


def _increment_ref_retry(ref_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of *ref_dict* with ``_retry_count`` incremented by 1."""
    updated = dict(ref_dict)
    meta = dict(updated.get("metadata") or {})
    meta[_RETRY_COUNT_KEY] = int(meta.get(_RETRY_COUNT_KEY, 0)) + 1
    updated["metadata"] = meta
    return updated


def _get_ref_retry_count(ref_dict: Dict[str, Any]) -> int:
    meta = ref_dict.get("metadata") or {}
    return int(meta.get(_RETRY_COUNT_KEY, 0))


async def _persist_updated_refs(
    task_id: Any,
    schema: str,
    updated_refs: List[Dict[str, Any]],
    original_inputs: Dict[str, Any],
) -> None:
    """Write updated ref list (with incremented retry counts) back to the task row."""
    try:
        from dynastore.modules.db_config.query_executor import DQLQuery, ResultHandler, managed_transaction
        from dynastore.modules.tasks.tasks_module import get_task_schema
        from dynastore.tools.protocol_helpers import get_engine

        engine = get_engine()
        if engine is None:
            logger.warning("cascade_cleanup: no engine — cannot persist retry counts.")
            return

        task_schema = get_task_schema()
        new_inputs = {**original_inputs, "refs": updated_refs}
        async with managed_transaction(engine) as conn:
            await DQLQuery(
                f"UPDATE {task_schema}.tasks SET inputs = :inputs"
                " WHERE task_id = :task_id AND catalog_id = :schema_name;",
                result_handler=ResultHandler.NONE,
            ).execute(
                conn,
                inputs=json.dumps(new_inputs),
                task_id=task_id,
                schema_name=schema,
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "cascade_cleanup: failed to persist retry counts (non-fatal): %s", exc,
        )


class CascadeCleanupTask(TaskProtocol):
    """Drain one cascade_cleanup row by invoking each owner's cleanup_one."""

    task_type = "cascade_cleanup"
    priority = 60
    # The cascade orchestrator enqueues this on the catalog tier; its cleanup-owner
    # registry only exists there. It MUST have a live catalog-tier owner or the
    # platform leaks resources — the mandatory-ownership guarantee enforces this.
    mandatory = True
    affinity_tier = "catalog"

    def is_available(self) -> bool:  # pragma: no cover
        return True

    async def run(self, payload: Any) -> Dict[str, Any]:
        from dynastore.modules.catalog.cascade_registry import cascade_cleanup_registry
        from dynastore.modules.catalog.resource_owner import CleanupMode, CleanupOutcome, CleanupRef

        inputs_raw = getattr(payload, "inputs", None) or {}
        inputs = CascadeCleanupInputs.model_validate(inputs_raw)
        mode = CleanupMode(inputs.mode)
        max_ref_retries: int = int(
            (inputs_raw.get("max_ref_retries") if isinstance(inputs_raw, dict) else None)
            or _DEFAULT_MAX_REF_RETRIES
        )

        task_id = getattr(payload, "task_id", None)
        # cascade_cleanup rows are system-scoped (catalog_id='system'); the
        # TaskPayload carries no tenant field, so this resolves to 'system',
        # which is exactly the row this task's retry-count UPDATE must match.
        schema: str = getattr(payload, "catalog_id", None) or "system"

        dead_refs: list[str] = []
        retry_refs: list[str] = []
        done_count = 0

        updated_ref_dicts: List[Dict[str, Any]] = []

        for ref_dict in inputs.refs:
            try:
                ref = CleanupRef.from_json(ref_dict)
            except (KeyError, TypeError) as exc:
                logger.error(
                    "cascade_cleanup: malformed ref dict %r — skipping: %s",
                    ref_dict, exc,
                )
                dead_refs.append(str(ref_dict))
                updated_ref_dicts.append(ref_dict)
                continue

            retry_count = _get_ref_retry_count(ref_dict)
            if retry_count >= max_ref_retries:
                logger.error(
                    "cascade_cleanup: ref %r (owner=%r) exhausted %d retries — DEAD.",
                    ref.locator, ref.owner_id, retry_count,
                )
                dead_refs.append(ref.locator)
                updated_ref_dicts.append(ref_dict)
                continue

            try:
                owner = cascade_cleanup_registry.get(ref.owner_id)
            except KeyError:
                logger.error(
                    "cascade_cleanup: no owner registered for owner_id=%r "
                    "(ref kind=%r locator=%r) — marking DEAD.",
                    ref.owner_id, ref.kind, ref.locator,
                )
                dead_refs.append(ref.locator)
                updated_ref_dicts.append(ref_dict)
                continue

            try:
                outcome = await owner.cleanup_one(ref, mode)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "cascade_cleanup: owner %r raised for %r (attempt %d/%d): %s",
                    ref.owner_id, ref.locator, retry_count + 1, max_ref_retries,
                    exc, exc_info=True,
                )
                incremented = _increment_ref_retry(ref_dict)
                retry_refs.append(ref.locator)
                updated_ref_dicts.append(incremented)
                continue

            if outcome == CleanupOutcome.DONE:
                done_count += 1
                updated_ref_dicts.append(ref_dict)
            elif outcome == CleanupOutcome.RETRY:
                logger.warning(
                    "cascade_cleanup: owner %r returned RETRY for %r (attempt %d/%d).",
                    ref.owner_id, ref.locator, retry_count + 1, max_ref_retries,
                )
                incremented = _increment_ref_retry(ref_dict)
                retry_refs.append(ref.locator)
                updated_ref_dicts.append(incremented)
            elif outcome == CleanupOutcome.DEAD:
                logger.error(
                    "cascade_cleanup: owner %r returned DEAD for %r — permanent failure.",
                    ref.owner_id, ref.locator,
                )
                dead_refs.append(ref.locator)
                updated_ref_dicts.append(ref_dict)

        if retry_refs and task_id is not None:
            await _persist_updated_refs(task_id, schema, updated_ref_dicts, inputs_raw)

        if retry_refs:
            raise RuntimeError(
                f"cascade_cleanup: {len(retry_refs)} ref(s) need retry: {retry_refs}"
            )

        if dead_refs:
            # DEAD refs are terminal: re-running cannot make them succeed, so
            # raise PermanentTaskFailure (NOT the retryable RuntimeError used for
            # retry_refs above).  The dispatcher dead-letters the row instead of
            # resetting it to PENDING — without this, a doomed cascade_cleanup
            # task (e.g. one whose payload names a cleanup owner retired by a
            # later refactor) is re-claimed every backoff cycle forever and
            # emits a task.failed event each time, so the events outbox never
            # drains.
            raise PermanentTaskFailure(
                f"cascade_cleanup: {len(dead_refs)} ref(s) permanently failed "
                f"(DEAD): {dead_refs}"
            )

        return {
            "status": "ok",
            "done": done_count,
            "dead": len(dead_refs),
            "retry": len(retry_refs),
        }
