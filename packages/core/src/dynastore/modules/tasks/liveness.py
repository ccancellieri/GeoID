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

"""Runner-agnostic execution-liveness Protocol.

The maintenance reaper resets a lapsed-lease ``ACTIVE`` task back to ``PENDING``
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
    from dynastore.models.tasks import Task, LogPage


class LivenessVerdict(str, Enum):
    """The reconciler's possible reads of an execution's state.

    Each verdict maps to exactly one reconciler action:

    - ``ALIVE``              — execution confirmed running/pending → extend the lease.
    - ``DEAD``               — execution gone/cancelled → ``fail_task(retry=True)`` now.
    - ``UNKNOWN``            — probe inconclusive → leave for the maintenance reaper backstop.
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
    maintenance reaper handles the row, exactly as today.
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


@runtime_checkable
class StopSignalProtocol(Protocol):
    """Contract for a runner that can signal its out-of-process execution to stop.

    Deliberately a SEPARATE ``@runtime_checkable Protocol`` rather than new
    methods on :class:`LivenessProbeProtocol`.  Adding methods to an existing
    ``@runtime_checkable`` protocol drops every current implementer out of
    structural ``isinstance`` / ``get_protocols`` discovery — which breaks
    liveness probing entirely.  A standalone sibling lets the reconciler
    resolve stop-signal runners independently via
    ``get_protocols(StopSignalProtocol)`` and lets a future runner ship stop
    support without touching its probe path.

    ``signal_stop`` and ``force_stop`` MUST NOT raise — on any error they
    return ``False`` so the reconciler degrades gracefully rather than
    crashing its loop.

    IAM perms needed by the runner service account:
      ``run.executions.cancel``  — for ``signal_stop``
      ``run.executions.delete``  — for ``force_stop``
    """

    runner_type: str

    def owns(self, owner_id: str) -> bool:
        """True if this runner is the one that stamped ``owner_id`` on a task row."""
        ...

    async def signal_stop(self, task: "Task") -> bool:
        """Initiate a graceful stop of the execution backing ``task``.

        Idempotent: returns ``True`` when a stop was (or already had been)
        requested; ``False`` when no execution handle is available.  Never
        raises — any error is logged and returns ``False``.
        """
        ...

    async def force_stop(self, task: "Task") -> bool:
        """Force-teardown the execution backing ``task`` (escalation path).

        Returns ``True`` when the teardown request was sent; ``False`` when no
        execution handle is available or the request failed.  Never raises.
        """
        ...


def resolve_stop_signal(owner_id: Optional[str]) -> Optional[StopSignalProtocol]:
    """Return the registered stop-signal runner that owns ``owner_id``, or ``None``.

    Mirrors :func:`resolve_probe` — resolution keys on ``owner_id``, not on
    ``can_handle(task_type)``.  ``None`` means the owner is an in-process /
    ephemeral / unrecognized runner that has no stop-signal capability: the
    reconciler will stamp ``dismiss_confirmed_at`` without an explicit stop
    signal (there is nothing running to stop).
    """
    if not owner_id:
        return None
    from dynastore.tools.discovery import get_protocols

    for runner in get_protocols(StopSignalProtocol):
        try:
            if runner.owns(owner_id):
                return runner
        except Exception:  # noqa: BLE001 — a broken runner must not block resolution
            continue
    return None


@runtime_checkable
class LogSourceProtocol(Protocol):
    """Contract for a runner that can fetch logs for its out-of-process execution.

    Deliberately a SEPARATE ``@runtime_checkable Protocol`` rather than new
    methods on :class:`LivenessProbeProtocol` or :class:`StopSignalProtocol` —
    same rationale as those two: adding a method to an existing
    ``@runtime_checkable`` protocol drops every current implementer out of
    structural ``isinstance`` / ``get_protocols`` discovery. A standalone
    sibling lets the vendor-extension logs route resolve log sources
    independently via ``get_protocols(LogSourceProtocol)`` and lets a future
    runner ship log access without touching its probe/stop-signal path.

    ``fetch_logs`` MUST NOT raise — on any error it returns an empty
    :class:`~dynastore.models.tasks.LogPage` with a human-readable ``note``
    so a caller (e.g. missing IAM permission on the runner service account)
    degrades to "no logs available" rather than turning a log fetch into a
    500 on the job-status surface.
    """

    runner_type: str

    def owns(self, owner_id: str) -> bool:
        """True if this runner is the one that stamped ``owner_id`` on a task row."""
        ...

    async def fetch_logs(
        self,
        task: "Task",
        *,
        limit: int = 200,
        cursor: Optional[str] = None,
        order: str = "asc",
    ) -> "LogPage":
        """Return a page of logs for the execution backing ``task``.

        Best-effort: never raises. Any error (including a missing IAM
        permission) is logged and swallowed, returning an empty
        :class:`~dynastore.models.tasks.LogPage` carrying a ``note`` that
        explains why logs are unavailable.
        """
        ...


def resolve_log_source(owner_id: Optional[str]) -> Optional[LogSourceProtocol]:
    """Return the registered log source that owns ``owner_id``, or ``None``.

    Mirrors :func:`resolve_stop_signal` — resolution keys on ``owner_id``,
    not on ``can_handle(task_type)``. ``None`` means the owner is an
    in-process / ephemeral / unrecognized runner that has no remote log
    source: the caller returns an empty ``LogPage`` with an explanatory note.
    """
    if not owner_id:
        return None
    from dynastore.tools.discovery import get_protocols

    for source in get_protocols(LogSourceProtocol):
        try:
            if source.owns(owner_id):
                return source
        except Exception:  # noqa: BLE001 — a broken log source must not block resolution
            continue
    return None
