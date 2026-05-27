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

"""IAM destructive cleanup migration (PR-6 of the IAM-at-scale sequence).

Drops schema artefacts that were verified dead before any live consumer
existed:

1. ``iam.policies_sysadmin`` and ``iam.policies_auth`` partition tables.
   ``_derive_partition_key`` never returns ``"sysadmin"`` or ``"auth"``
   as a partition key, so these tables were never populated and carry no
   rows.  They were created by early prototype code that has since been
   removed.  Kept: ``policies_global`` (partition key ``"global"``) and
   ``policies_default`` (DEFAULT catch-all).

2. ``iam.principals.policy`` column.  Always NULL; the INSERT query never
   wrote it, and no SELECT reads it.  Confirmed absent from the model
   and all DQL.

3. ``iam.roles.level`` column.  Always 0; ``LIST_ROLES`` already uses
   ``ORDER BY name ASC``.  Confirmed absent from the model and DQL.

``roles.parent_roles`` is intentionally NOT dropped here — it is marked
deprecated on the Pydantic model and retained for backward-compatible
DTO deserialization until a separate cleanup removes the admin DTOs.

Idempotency
-----------
All DDL uses ``IF EXISTS`` so it is safe to re-run on a fresh DB (where
the artefacts were never created).  A sentinel row in
``iam.applied_presets`` prevents the DDL block from executing a second
time on a DB where it already ran.

Failure handling
----------------
The function is called from ``IamModule.lifespan`` in a ``try/except``
block; any exception is logged as a WARNING and does not abort boot.
Non-fatal is correct: the dropped artefacts are dead weight, not
load-bearing.
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

_SENTINEL_KEY = "iam_cleanup_v1"
_SCOPE_KEY = "platform"

# ---------------------------------------------------------------------------
# Module-level query objects (patchable in tests, following established pattern)
# ---------------------------------------------------------------------------

SELECT_SENTINEL = DQLQuery(
    """
    SELECT 1 FROM iam.applied_presets
    WHERE preset_name = :preset_name
      AND scope_key   = :scope_key
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

INSERT_SENTINEL = DQLQuery(
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

# ---------------------------------------------------------------------------
# DDL — all guarded with IF EXISTS for idempotency on fresh DBs
# ---------------------------------------------------------------------------

# The DDL statements are built as raw strings and executed via
# ``conn.execute(text(...))`` inside the advisory-locked transaction,
# following the established pattern at IamModule.lifespan:728-738.
# Using raw text() here is acceptable because:
#   a) the schema is always "iam" (hard-coded platform schema, no interpolation),
#   b) the patterns are identical to the seed block that already uses this idiom.
_DDL_STEPS = [
    # 1a. Drop dead policy partition: policies_sysadmin
    "DROP TABLE IF EXISTS iam.policies_sysadmin;",
    # 1b. Drop dead policy partition: policies_auth
    "DROP TABLE IF EXISTS iam.policies_auth;",
    # 2. Drop dead column: principals.policy (always NULL, never read)
    "ALTER TABLE IF EXISTS iam.principals DROP COLUMN IF EXISTS policy;",
    # 3. Drop dead column: roles.level (always 0, ORDER BY rewritten to name ASC)
    "ALTER TABLE IF EXISTS iam.roles DROP COLUMN IF EXISTS level;",
]


async def _run_ddl_steps(conn: Any) -> None:
    """Execute each cleanup DDL statement inside the caller's transaction."""
    from sqlalchemy import text

    for ddl in _DDL_STEPS:
        await conn.execute(text(ddl))  # type: ignore[misc]
        logger.debug("iam_cleanup_v1: executed: %s", ddl.strip())


async def run_migration(engine: Any) -> None:
    """Run the one-shot IAM cleanup migration.

    Acquires the same advisory lock as the IAM seed block so concurrent
    boot pods do not race.  Idempotent: a sentinel row in
    ``iam.applied_presets`` causes an immediate return on subsequent
    invocations.

    ``engine`` must be an async SQLAlchemy engine.
    """
    async with managed_transaction(engine) as conn:
        from sqlalchemy import text

        # Acquire the cross-pod advisory lock before reading the sentinel.
        # Auto-released on COMMIT/ROLLBACK — no leak risk.
        await conn.execute(  # type: ignore[misc]
            text("SELECT pg_advisory_xact_lock(hashtext('iam_seed:iam'))")
        )

        already = await SELECT_SENTINEL.execute(
            conn, preset_name=_SENTINEL_KEY, scope_key=_SCOPE_KEY
        )
        if already is not None:
            logger.debug("iam_cleanup_v1: already applied — skipping")
            return

        await _run_ddl_steps(conn)

        await INSERT_SENTINEL.execute(
            conn,
            preset_name=_SENTINEL_KEY,
            scope_key=_SCOPE_KEY,
            params_snapshot=json.dumps(
                {
                    "dropped_tables": [
                        "iam.policies_sysadmin",
                        "iam.policies_auth",
                    ],
                    "dropped_columns": [
                        "iam.principals.policy",
                        "iam.roles.level",
                    ],
                }
            ),
            revoke_descriptor=json.dumps(
                {
                    "note": (
                        "Dropped artefacts were verified dead before this migration ran. "
                        "No revoke path: restoring them requires a schema rebuild."
                    )
                }
            ),
        )
        logger.info(
            "iam_cleanup_v1: dropped dead partitions "
            "(policies_sysadmin, policies_auth) and dead columns "
            "(principals.policy, roles.level)"
        )
