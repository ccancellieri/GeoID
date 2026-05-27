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

"""Migration: backfill ``allowed_preset_names`` on ``catalog_preset_delegation``.

Pre-#1426 ``iam_baseline`` shipped the ``catalog_preset_delegation`` policy
with a ``catalog_admin_required`` condition that gated by role only. Without
the ``allowed_preset_names`` safe-subset guard, a catalog-tier admin could
POST/DELETE *any* registered preset at their catalog scope — including
security-sensitive routing presets that pin global drivers.

This one-shot migration adds ``allowed_preset_names: ["public_catalog",
"private_catalog"]`` to the policy's condition config when the key is absent.
Operators who deliberately set their own allowlist (key present) are left
untouched.

Idempotent: completion is recorded in ``iam.applied_presets`` as the synthetic
preset name ``catalog_preset_delegation_allowlist`` so subsequent runs skip
the work.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)

logger = logging.getLogger(__name__)

_PRESET_NAME = "catalog_preset_delegation_allowlist"
_SCOPE_KEY = "platform"

# Match the defaults shipped by ``IamBaselineParams.allowed_preset_names``.
_DEFAULT_ALLOWLIST: List[str] = ["public_catalog", "private_catalog"]


SELECT_APPLIED = DQLQuery(
    """
    SELECT 1 FROM iam.applied_presets
    WHERE preset_name = :preset_name
      AND scope_key   = :scope_key
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

SELECT_POLICY = DQLQuery(
    """
    SELECT conditions
    FROM iam.policies
    WHERE id = 'catalog_preset_delegation'
      AND partition_key = 'global'
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

UPDATE_POLICY_CONDITIONS = DQLQuery(
    """
    UPDATE iam.policies
    SET conditions = :conditions::jsonb,
        updated_at = NOW()
    WHERE id = 'catalog_preset_delegation'
      AND partition_key = 'global'
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


def _augment_conditions(conditions: List[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
    """Return the augmented condition list, or None if no change is needed.

    Adds ``allowed_preset_names`` to each ``catalog_admin_required`` condition
    that does not already declare one. Other condition types are passed through
    unchanged. Returns ``None`` when every targeted condition already carries
    the key so the caller can skip the UPDATE.
    """
    if not isinstance(conditions, list):
        return None
    changed = False
    result: List[Dict[str, Any]] = []
    for cond in conditions:
        if not isinstance(cond, dict):
            result.append(cond)
            continue
        if cond.get("type") != "catalog_admin_required":
            result.append(cond)
            continue
        cfg = cond.get("config")
        if not isinstance(cfg, dict):
            cfg = {}
        if "allowed_preset_names" in cfg:
            result.append(cond)
            continue
        new_cfg = dict(cfg)
        new_cfg["allowed_preset_names"] = list(_DEFAULT_ALLOWLIST)
        new_cond = dict(cond)
        new_cond["config"] = new_cfg
        result.append(new_cond)
        changed = True
    return result if changed else None


async def run_migration(engine: Any) -> None:
    """Backfill the allowlist and record completion.

    ``engine`` must be an async SQLAlchemy engine. The function manages its
    own transaction.
    """
    async with managed_transaction(engine) as conn:
        already = await SELECT_APPLIED.execute(
            conn, preset_name=_PRESET_NAME, scope_key=_SCOPE_KEY
        )
        if already is not None:
            logger.debug(
                "tighten_catalog_preset_allowlist: already applied — skipping"
            )
            return

        row = await SELECT_POLICY.execute(conn)
        if row is None:
            logger.info(
                "tighten_catalog_preset_allowlist: catalog_preset_delegation "
                "policy not found — nothing to tighten"
            )
            await _record_applied(
                conn, note="catalog_preset_delegation absent; no-op"
            )
            return

        raw = row[0] if isinstance(row, (list, tuple)) else row.get("conditions")
        if isinstance(raw, str):
            try:
                conditions = json.loads(raw)
            except (ValueError, TypeError):
                conditions = []
        elif isinstance(raw, list):
            conditions = raw
        else:
            conditions = []

        augmented = _augment_conditions(conditions)
        if augmented is None:
            logger.debug(
                "tighten_catalog_preset_allowlist: condition already declares "
                "allowed_preset_names — no update needed"
            )
        else:
            await UPDATE_POLICY_CONDITIONS.execute(
                conn, conditions=json.dumps(augmented)
            )
            logger.info(
                "tighten_catalog_preset_allowlist: backfilled "
                "allowed_preset_names=%s on catalog_preset_delegation",
                _DEFAULT_ALLOWLIST,
            )

        await _record_applied(
            conn,
            note=(
                "added default allowlist"
                if augmented is not None
                else "allowlist already present"
            ),
        )


async def _record_applied(conn: Any, note: str) -> None:
    await INSERT_APPLIED.execute(
        conn,
        preset_name=_PRESET_NAME,
        scope_key=_SCOPE_KEY,
        params_snapshot=json.dumps({"default_allowlist": _DEFAULT_ALLOWLIST}),
        revoke_descriptor=json.dumps({"note": note}),
    )
