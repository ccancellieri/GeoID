#    Copyright 2025 FAO
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

"""Runner-agnostic execution-liveness Protocol.

The pg_cron reaper resets a lapsed-lease ``ACTIVE`` task back to ``PENDING``
purely on a timer — correct for an in-process runner (the work genuinely died
with the pod), but wrong for a runner whose execution outlives the dispatching
process. A Cloud Run Job spawned fire-and-forget keeps running through its
cold-start window while the spawn lease quietly lapses; the reaper then
reclaims the row mid-boot and a second execution is spawned.

This module defines the contract that lets the reconciler ask the **owning
runner** "is the execution backing this task actually alive?" before the reaper
acts. It is import-light on purpose — no ``google.*`` imports leak into the
dispatch hot path. Each runner whose executions outlive the dispatcher
implements :class:`LivenessProbeProtocol` its own way; in-process runners do
not implement it at all, so :func:`resolve_probe` returns ``None`` for their
owners and the reconciler no-ops — preserving today's reaper-only behavior.
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Optional, Protocol, runtime_checkable

if TYPE_CHECKING:
    from dynastore.models.tasks import Task


class LivenessVerdict(str, Enum):
    """The reconciler's possible reads of an execution's state.

    Each verdict maps to exactly one reconciler action:

    - ``ALIVE``              — execution confirmed running/pending → extend the lease.
    - ``DEAD``               — execution gone/cancelled → ``fail_task(retry=True)`` now.
    - ``UNKNOWN``            — probe inconclusive → leave for the pg_cron reaper backstop.
    - ``TERMINAL_SUCCEEDED`` — execution exited 0 but the row is still ACTIVE →
      reconcile the row to COMPLETED.
    - ``TERMINAL_FAILED``    — execution exited non-zero but the row is still
      ACTIVE → ``fail_task(retry=True)`` now.
    """

    ALIVE = "alive"
    DEAD = "dead"
    UNKNOWN = "unknown"
    TERMINAL_SUCCEEDED = "terminal_succeeded"
    TERMINAL_FAILED = "terminal_failed"


@runtime_checkable
class LivenessProbeProtocol(Protocol):
    """Contract for a runner that can report whether its executions are alive.

    Deliberately separate from ``RunnerProtocol``: that Protocol is
    ``runtime_checkable`` and consumed structurally in many places, so adding a
    method to it would break every implementer and structural check. A standalone
    Protocol lets the reconciler resolve probes independently via
    ``get_protocols(LivenessProbeProtocol)`` and lets a future runner ship a
    probe without touching its ``run()`` path.

    ``probe_liveness`` MUST NOT raise — on any error it returns
    :attr:`LivenessVerdict.UNKNOWN` so the reconciler degrades to the reaper
    backstop rather than crashing its loop.
    """

    runner_type: str

    def owns(self, owner_id: str) -> bool:
        """True if this runner is the one that stamped ``owner_id`` on a task row."""
        ...

    async def probe_liveness(self, task: "Task") -> LivenessVerdict:
        """Report the liveness of the execution backing ``task``. Never raises."""
        ...


def resolve_probe(owner_id: Optional[str]) -> Optional[LivenessProbeProtocol]:
    """Return the registered probe that owns ``owner_id``, or ``None``.

    Resolution keys on ``owner_id`` — the durable record of *who claimed this
    row* — not on ``can_handle(task_type)``. ``None`` means the owner is an
    in-process / ephemeral / unrecognized runner: the reconciler no-ops and the
    pg_cron reaper handles the row, exactly as today.
    """
    if not owner_id:
        return None
    from dynastore.tools.discovery import get_protocols

    for probe in get_protocols(LivenessProbeProtocol):
        try:
            if probe.owns(owner_id):
                return probe
        except Exception:  # noqa: BLE001 — a broken probe must not block resolution
            continue
    return None
