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

"""cascade_cleanup task runner.

Deserializes :class:`CleanupRef` instances from the task payload, looks up
each owner in the :data:`~dynastore.modules.catalog.cascade_registry.cascade_cleanup_registry`,
and calls :meth:`~dynastore.modules.catalog.resource_owner.ResourceOwnerProtocol.cleanup_one`.

Outcome contract:
- All DONE  → task completes successfully.
- Any RETRY → RuntimeError raised so the task framework retries with backoff.
- Any DEAD  → logged as error; processing continues so other refs are not
              blocked, but the task is marked FAILED at the end.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from dynastore.tasks.protocols import TaskProtocol
from dynastore.tasks.cascade_cleanup.inputs import CascadeCleanupInputs

logger = logging.getLogger(__name__)


class CascadeCleanupTask(TaskProtocol):
    """Drain one cascade_cleanup row by invoking each owner's cleanup_one."""

    task_type = "cascade_cleanup"
    priority = 60

    def is_available(self) -> bool:  # pragma: no cover
        return True

    async def run(self, payload: Any) -> Dict[str, Any]:
        from dynastore.modules.catalog.cascade_registry import cascade_cleanup_registry
        from dynastore.modules.catalog.resource_owner import CleanupMode, CleanupOutcome, CleanupRef

        inputs_raw = getattr(payload, "inputs", None) or {}
        inputs = CascadeCleanupInputs.model_validate(inputs_raw)
        mode = CleanupMode(inputs.mode)

        dead_refs: list[str] = []
        retry_refs: list[str] = []
        done_count = 0

        for ref_dict in inputs.refs:
            try:
                ref = CleanupRef.from_json(ref_dict)
            except (KeyError, TypeError) as exc:
                logger.error(
                    "cascade_cleanup: malformed ref dict %r — skipping: %s",
                    ref_dict, exc,
                )
                dead_refs.append(str(ref_dict))
                continue

            try:
                owner = cascade_cleanup_registry.get(ref.owner_id)
            except KeyError:
                logger.error(
                    "cascade_cleanup: no owner registered for owner_id=%r "
                    "(ref kind=%r locator=%r) — skipping.",
                    ref.owner_id, ref.kind, ref.locator,
                )
                dead_refs.append(ref.locator)
                continue

            try:
                outcome = await owner.cleanup_one(ref, mode)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "cascade_cleanup: owner %r raised unexpectedly for %r — "
                    "treating as RETRY: %s",
                    ref.owner_id, ref.locator, exc, exc_info=True,
                )
                retry_refs.append(ref.locator)
                continue

            if outcome == CleanupOutcome.DONE:
                done_count += 1
            elif outcome == CleanupOutcome.RETRY:
                logger.warning(
                    "cascade_cleanup: owner %r returned RETRY for %r.",
                    ref.owner_id, ref.locator,
                )
                retry_refs.append(ref.locator)
            elif outcome == CleanupOutcome.DEAD:
                logger.error(
                    "cascade_cleanup: owner %r returned DEAD for %r — "
                    "permanent failure, continuing.",
                    ref.owner_id, ref.locator,
                )
                dead_refs.append(ref.locator)

        if retry_refs:
            raise RuntimeError(
                f"cascade_cleanup: {len(retry_refs)} ref(s) need retry: {retry_refs}"
            )

        if dead_refs:
            raise RuntimeError(
                f"cascade_cleanup: {len(dead_refs)} ref(s) permanently failed "
                f"(DEAD): {dead_refs}"
            )

        return {
            "status": "ok",
            "done": done_count,
            "dead": len(dead_refs),
            "retry": len(retry_refs),
        }
