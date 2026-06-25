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

"""Liveness-aware reaper for orphaned task scratch directories.

Task workers (zip extraction, tile preseed, etc.) create scratch directories
under the root provided by ``TempDirProtocol`` using the shared
``TASK_DIR_PREFIX``.  Each directory carries a ``.owner`` JSON sidecar
written at creation time.  This module provides a best-effort sweep that
reclaims directories whose owning task is no longer alive, so the shared
volume (GCSFuse, NFS, or local disk) doesn't accumulate stale data across
concurrent Cloud Run or on-premise process instances.

A 31-day GCS lifecycle policy (or equivalent retention on other backends)
is the long-tail backstop for any dirs that escape this sweep.
"""

from __future__ import annotations

import glob
import json
import logging
import os
import shutil
import time
from typing import Any, Optional
from uuid import UUID

from dynastore.models.tasks import TaskStatusEnum
from dynastore.models.protocols.temp_dir import TASK_DIR_PREFIX

logger = logging.getLogger(__name__)

# Terminal task statuses — a directory whose owner is in one of these states
# (or is gone from the DB entirely) is safe to reclaim.
_TERMINAL: frozenset[TaskStatusEnum] = frozenset({
    TaskStatusEnum.COMPLETED,
    TaskStatusEnum.FAILED,
    TaskStatusEnum.DISMISSED,
    TaskStatusEnum.DEAD_LETTER,
})

# Sentinel returned by _lookup_task when the lookup itself fails.
# Distinct from None (task not found in DB) so _maybe_reap can stay safe
# and not accidentally delete a directory whose task status is unknown.
_LOOKUP_ERROR = object()


def _resolve_tmpdir() -> str:
    """Return the temp-directory root from ``TempDirProtocol``, with fallback."""
    try:
        from dynastore.tools.protocol_helpers import resolve
        from dynastore.models.protocols.temp_dir import TempDirProtocol
        return resolve(TempDirProtocol).get_tmpdir()
    except Exception:  # noqa: BLE001 — protocol is optional
        from dynastore.models.protocols.temp_dir import DefaultTempDir
        return DefaultTempDir().get_tmpdir()


async def reap_orphan_task_dirs(
    engine: Any,
    *,
    tmpdir: Optional[str] = None,
    min_age_seconds: int = 120,
) -> int:
    """Reclaim orphaned ``dynastore_task_*`` scratch directories under *tmpdir*.

    Covers ALL task types that adopt the ``TempDirProtocol.mkdtemp()``
    convention (zip extraction, tile preseed, etc.).

    A directory is reclaimed only when ALL of the following hold:
    - Its mtime is at least *min_age_seconds* old (avoids racing a sibling
      that just created the directory and hasn't finished writing the sidecar).
    - It has a readable, valid ``.owner`` JSON sidecar with a ``task_id``.
    - Its owning task is absent from the DB (gone) or in a terminal status.

    Directories without a valid sidecar are left alone — the storage backend's
    retention policy (e.g. 31-day GCS lifecycle) is the backstop for those.

    Returns the number of directories reclaimed.  Never raises; all per-dir
    errors are caught and logged so one bad entry never aborts the sweep.
    """
    root = tmpdir or _resolve_tmpdir()
    pattern = os.path.join(root, f"{TASK_DIR_PREFIX}*")
    candidates = [p for p in glob.glob(pattern) if os.path.isdir(p)]

    reclaimed = 0
    now = time.time()

    for dirpath in candidates:
        try:
            did_reap = await _maybe_reap(dirpath, engine, now, min_age_seconds)
            if did_reap:
                reclaimed += 1
        except Exception:  # noqa: BLE001
            logger.debug(
                "temp_reaper: unexpected error processing %r — skipping",
                dirpath, exc_info=True,
            )

    logger.info(
        "temp_reaper: reclaimed %d orphan task dir(s) of %d scanned",
        reclaimed, len(candidates),
    )
    return reclaimed


async def _maybe_reap(
    dirpath: str,
    engine: Any,
    now: float,
    min_age_seconds: int,
) -> bool:
    """Evaluate one candidate directory and remove it if safe. Returns True if reclaimed."""
    # Age guard: skip dirs created or modified too recently.
    try:
        mtime = os.path.getmtime(dirpath)
    except OSError:
        logger.debug("temp_reaper: cannot stat %r — skipping", dirpath)
        return False

    if (now - mtime) < min_age_seconds:
        logger.debug(
            "temp_reaper: %r is too young (%.0fs < %ds) — skipping",
            dirpath, now - mtime, min_age_seconds,
        )
        return False

    # Read and validate the .owner sidecar.
    owner_path = os.path.join(dirpath, ".owner")
    try:
        with open(owner_path) as _f:
            owner = json.load(_f)
        task_id_str: Optional[str] = owner.get("task_id")
        schema: Optional[str] = owner.get("schema")
    except (OSError, json.JSONDecodeError, AttributeError):
        logger.debug(
            "temp_reaper: %r has missing or invalid .owner sidecar — skipping (retention policy applies)",
            dirpath,
        )
        return False

    if not task_id_str:
        logger.debug(
            "temp_reaper: %r .owner sidecar has no task_id — skipping",
            dirpath,
        )
        return False

    # Look up the owning task.  _LOOKUP_ERROR means "could not determine" — skip.
    task = await _lookup_task(engine, task_id_str, schema)

    if task is _LOOKUP_ERROR:
        return False

    # Reclaim when the owner is gone (None) or in a terminal status.
    if task is None:
        logger.debug("temp_reaper: task %s not found (gone) — reclaiming %r", task_id_str, dirpath)
    elif task.status in _TERMINAL:
        logger.debug(
            "temp_reaper: task %s is %s (terminal) — reclaiming %r",
            task_id_str, task.status, dirpath,
        )
    else:
        logger.debug(
            "temp_reaper: task %s is %s (alive) — keeping %r",
            task_id_str, task.status, dirpath,
        )
        return False

    shutil.rmtree(dirpath, ignore_errors=True)
    return True


async def _lookup_task(engine: Any, task_id_str: str, schema: Optional[str]) -> Any:
    """Return the Task row, None if absent, or _LOOKUP_ERROR if lookup failed."""
    try:
        from dynastore.tools.protocol_helpers import resolve
        from dynastore.models.protocols.tasks import TasksProtocol

        tasks_mgr = resolve(TasksProtocol)
        task_uuid = UUID(task_id_str)

        if schema:
            return await tasks_mgr.get_task(engine, task_uuid, schema=schema)

        # No schema in sidecar — fall back to the unscoped lookup so we can
        # still make a liveness decision without the tenant hint.
        from dynastore.modules.tasks.tasks_module import get_task_by_id_unscoped
        return await get_task_by_id_unscoped(engine, task_uuid)
    except Exception:  # noqa: BLE001
        logger.debug(
            "temp_reaper: task lookup failed for %r — treating as alive (skipping)",
            task_id_str, exc_info=True,
        )
        return _LOOKUP_ERROR
