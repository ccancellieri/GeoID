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

"""Backfill migration: insert an ``applied`` audit row for ``iam_baseline``.

Run once during IamModule lifespan after the ``iam.applied_presets`` table
DDL and the IAM tables themselves are ready.

Idempotent: a no-op if ``(preset_name='iam_baseline', scope_key='platform')``
already exists. On first run after the upgrade from contributor-loop seeding
the migration snapshots the current DB state of the IAM extension's own
policies + role bindings and writes a synthetic ``applied`` row whose
``revoke_descriptor`` reflects what is *actually in the DB* — not the
canonical contributor declaration. This ensures ``DELETE iam_baseline`` undoes
exactly what is present rather than a hypothetical canonical shape.

Divergence guard: if the current DB policies differ from the contributor
declaration the descriptor still reflects the actual DB state, so revoke is
always correct. The ``params_snapshot`` is written as ``{}`` (system backfill
— no caller params).
"""
from __future__ import annotations

import json
import logging
from typing import Any

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)

logger = logging.getLogger(__name__)

# Preset name and scope written into the audit row.
_PRESET_NAME = "iam_baseline"
_SCOPE_KEY = "platform"

# Policy IDs contributed by the IAM extension's own service.
_POLICY_IDS = [
    "admin_authorization_api",
    "self_service_authorization_api",
    "admin_catalog_access",
    "catalog_preset_delegation",
]

# Shared role names contributed by the IAM extension.
_ROLE_NAMES = ["admin", "sysadmin", "user"]

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


async def run_backfill(
    engine: Any,
    policy_storage: Any,
    iam_storage: Any,
) -> None:
    """Insert a synthetic ``applied`` audit row for ``iam_baseline``.

    ``engine`` must be an async SQLAlchemy engine (DbResource). The function
    opens its own transaction internally.

    ``policy_storage`` and ``iam_storage`` are used read-only to snapshot
    existing policies and roles so the revoke_descriptor reflects actual DB
    state.
    """
    async with managed_transaction(engine) as conn:
        existing = await SELECT_EXISTING.execute(
            conn, preset_name=_PRESET_NAME, scope_key=_SCOPE_KEY
        )
        if existing is not None:
            logger.debug(
                "backfill: iam_baseline audit row already present at scope %r — skipping",
                _SCOPE_KEY,
            )
            return

        # Snapshot actual present policy IDs.
        actual_policy_ids: list[str] = []
        try:
            for pid in _POLICY_IDS:
                row = await policy_storage.get_policy(
                    pid, schema="iam", conn=conn, partition_key="global"
                )
                if row is not None:
                    actual_policy_ids.append(pid)
        except Exception as exc:
            logger.warning("backfill: could not read IAM policies — %s; using empty list", exc)

        # Snapshot actual present role names.
        actual_role_names: list[str] = []
        try:
            for rname in _ROLE_NAMES:
                row = await iam_storage.get_role(rname, schema="iam", conn=conn)
                if row is not None:
                    actual_role_names.append(rname)
        except Exception as exc:
            logger.warning("backfill: could not read IAM roles — %s; using empty list", exc)

        revoke_descriptor = {
            "policy_ids": actual_policy_ids,
            "role_names": actual_role_names,
            "delegation_role_names": ["admin"],
        }

        await INSERT_APPLIED.execute(
            conn,
            preset_name=_PRESET_NAME,
            scope_key=_SCOPE_KEY,
            params_snapshot=json.dumps({}),
            revoke_descriptor=json.dumps(revoke_descriptor),
        )
        logger.info(
            "backfill: inserted iam_baseline audit row "
            "(policies=%s, roles=%s)",
            actual_policy_ids,
            actual_role_names,
        )
