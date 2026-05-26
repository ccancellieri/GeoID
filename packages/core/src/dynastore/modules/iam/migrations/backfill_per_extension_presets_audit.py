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

"""Backfill migration: insert ``applied`` audit rows for per-extension presets.

Run once after ``iam.applied_presets`` DDL and all IAM tables are ready.

Idempotent: for each ``PolicyContributorPreset`` registered in the preset
registry, inserts an ``applied`` row at scope ``platform`` if and only if
no row already exists for that ``(preset_name, scope_key)`` pair.

On first run after the upgrade the migration snapshots the current DB state
of each contributor's policies and role bindings from the live database.
The ``revoke_descriptor`` therefore reflects *what is actually in the DB*
rather than the contributor's canonical declaration — this ensures
``DELETE /{name}`` always undoes exactly what is present.

Divergence guard: if the live DB differs from what the contributor would
declare, the descriptor still reflects actual DB state.  The
``params_snapshot`` is written as ``{}`` (system backfill — no caller
params).

PR-3 of umbrella #1412.
"""
from __future__ import annotations

import json
import logging
from typing import Any, List

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)

logger = logging.getLogger(__name__)

_SCOPE_KEY = "platform"

# ---------------------------------------------------------------------------
# Module-level queries (patchable in tests)
# ---------------------------------------------------------------------------

SELECT_EXISTING = DQLQuery(
    """
    SELECT 1 FROM iam.applied_presets
    WHERE preset_name = :preset_name
      AND scope_key   = :scope_key
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

INSERT_APPLIED = DQLQuery(
    """
    INSERT INTO iam.applied_presets
        (preset_name, scope_key, state, applied_at, applied_by,
         params_snapshot, revoke_descriptor, updated_at)
    VALUES
        (:preset_name, :scope_key, 'applied', NOW(), NULL,
         :params_snapshot, :revoke_descriptor, NOW())
    ON CONFLICT (preset_name, scope_key) DO NOTHING
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)


async def _snapshot_policy_ids(
    policy_storage: Any,
    policy_ids: List[str],
    conn: Any,
) -> List[str]:
    """Return the subset of ``policy_ids`` that actually exist in the DB."""
    present: List[str] = []
    for pid in policy_ids:
        try:
            row = await policy_storage.get_policy(
                pid, schema="iam", conn=conn, partition_key="global"
            )
            if row is not None:
                present.append(pid)
        except Exception as exc:
            logger.debug("backfill: could not probe policy %r: %s", pid, exc)
    return present


async def _snapshot_role_names(
    iam_storage: Any,
    role_names: List[str],
    conn: Any,
) -> List[str]:
    """Return the subset of ``role_names`` that actually exist in the DB."""
    present: List[str] = []
    for rname in role_names:
        try:
            row = await iam_storage.get_role(rname, schema="iam", conn=conn)
            if row is not None:
                present.append(rname)
        except Exception as exc:
            logger.debug("backfill: could not probe role %r: %s", rname, exc)
    return present


async def run_backfill(
    engine: Any,
    policy_storage: Any,
    iam_storage: Any,
) -> None:
    """Insert synthetic ``applied`` audit rows for all registered per-extension presets.

    For each ``PolicyContributorPreset`` in the global registry, probes the
    current DB state of its contributor's policies and role bindings and
    writes an ``applied`` row whose ``revoke_descriptor`` reflects reality.
    """
    from dynastore.modules.storage.presets.policy_contributor_adapter import (
        PolicyContributorPreset,
    )
    from dynastore.modules.storage.presets.registry import list_presets, get_preset

    for preset_name in list_presets():
        try:
            preset = get_preset(preset_name)
        except KeyError:
            continue
        if not isinstance(preset, PolicyContributorPreset):
            continue

        async with managed_transaction(engine) as conn:
            existing = await SELECT_EXISTING.execute(
                conn, preset_name=preset_name, scope_key=_SCOPE_KEY
            )
            if existing is not None:
                logger.debug(
                    "backfill: %r audit row already present — skipping", preset_name
                )
                continue

            # Determine what the contributor declares.
            try:
                contributor = preset._contributor_factory()  # noqa: SLF001
                declared_policy_ids = [p.id for p in (contributor.get_policies() or [])]
                declared_role_names = [r.name for r in (contributor.get_role_bindings() or [])]
            except Exception as exc:
                logger.warning(
                    "backfill: could not introspect contributor for %r: %s — skipping",
                    preset_name,
                    exc,
                )
                continue

            # Snapshot actual DB state.
            actual_policy_ids = await _snapshot_policy_ids(
                policy_storage, declared_policy_ids, conn
            )
            actual_role_names = await _snapshot_role_names(
                iam_storage, declared_role_names, conn
            )

            revoke_descriptor = {
                "preset_name": preset_name,
                "policy_ids": actual_policy_ids,
                "role_names": actual_role_names,
            }

            await INSERT_APPLIED.execute(
                conn,
                preset_name=preset_name,
                scope_key=_SCOPE_KEY,
                params_snapshot=json.dumps({}),
                revoke_descriptor=json.dumps(revoke_descriptor),
            )
            logger.info(
                "backfill: inserted %r audit row (policies=%s, roles=%s)",
                preset_name,
                actual_policy_ids,
                actual_role_names,
            )
