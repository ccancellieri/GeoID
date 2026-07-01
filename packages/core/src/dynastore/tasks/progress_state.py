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

"""Process-local durable-progress tracker shared between a task's reporter
and the generic heartbeat loop (``main_task.py``).

A task reporter (e.g. ``DatabaseStatusReporter.update_progress``) already
writes ``progress`` to the DB on its own cadence, but that cadence is tied to
the task's own batching (e.g. once per processed chunk). If the container is
SIGTERM-killed between two reporter writes, the row still reflects the last
*committed* value — accurate, just possibly stale by one batch. Recording the
same value here lets the heartbeat loop (which already runs on a short, fixed
interval to extend ``locked_until``) persist it again on its own cadence, as
a belt-and-braces measure independent of any single task's reporting cadence.

Executor-agnostic and import-light — no ``google.*`` / GCP dependency. A
Cloud Run Job pod runs exactly one task per container lifetime, so a
process-wide dict keyed by ``task_id`` is sufficient: there is never more
than one in-flight value per key.
"""

from __future__ import annotations

from typing import Dict, Optional

_last_progress: Dict[str, int] = {}


def record_progress(task_id: object, progress: int) -> None:
    """Record the latest known progress percentage for ``task_id``.

    Called by task reporters immediately after their own DB write so the
    heartbeat loop can persist the same value again on its own cadence.
    """
    _last_progress[str(task_id)] = progress


def get_progress(task_id: object) -> Optional[int]:
    """Return the last recorded progress for ``task_id``, or ``None`` if unknown."""
    return _last_progress.get(str(task_id))


def clear_progress(task_id: object) -> None:
    """Drop the tracked progress for ``task_id`` (e.g. on task completion)."""
    _last_progress.pop(str(task_id), None)
