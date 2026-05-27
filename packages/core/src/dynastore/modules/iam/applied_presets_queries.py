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

"""DDL + DQL/DML for ``iam.applied_presets`` — preset audit table.

State machine transitions::

    pending
      → in_progress (apply started)
          → applied (success, revoke_descriptor populated)
          → failed  (apply error, last_error populated, revoke_descriptor may be partial)
    applied
      → revoke_pending → revoke_in_progress → revoked
                                             → revoke_failed (partial revoke_descriptor remains)
    applied
      → partial (async child in composite failed; composite row only)

Row uniqueness is ``(preset_name, scope_key)``. Rows survive revoke for
forensic history; a fresh apply at the same ``(name, scope)`` upserts in
place (lifecycle layer handles idempotency and lock ordering).
"""
from __future__ import annotations

from dynastore.modules.db_config.query_executor import DDLQuery, DQLQuery, ResultHandler

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

CREATE_APPLIED_PRESETS_TABLE = DDLQuery("""
    CREATE TABLE IF NOT EXISTS iam.applied_presets (
        preset_name         TEXT        NOT NULL,
        scope_key           TEXT        NOT NULL,
        state               TEXT        NOT NULL DEFAULT 'pending',
        applied_at          TIMESTAMPTZ,
        applied_by          UUID,
        params_snapshot     JSONB,
        revoke_descriptor   JSONB,
        apply_task_id       UUID,
        revoke_task_id      UUID,
        last_error          TEXT,
        updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (preset_name, scope_key),
        CONSTRAINT applied_presets_state_check CHECK (
            state IN (
                'pending', 'in_progress', 'applied',
                'revoke_pending', 'revoke_in_progress', 'revoked',
                'failed', 'revoke_failed', 'partial'
            )
        )
    );
""")

CREATE_APPLIED_PRESETS_STATE_IDX = DDLQuery("""
    CREATE INDEX IF NOT EXISTS idx_applied_presets_state
        ON iam.applied_presets (state);
""")

CREATE_APPLIED_PRESETS_SCOPE_IDX = DDLQuery("""
    CREATE INDEX IF NOT EXISTS idx_applied_presets_scope_key
        ON iam.applied_presets (scope_key);
""")

# ---------------------------------------------------------------------------
# DML — state transitions
# ---------------------------------------------------------------------------

UPSERT_PENDING = DQLQuery(
    """
    INSERT INTO iam.applied_presets
        (preset_name, scope_key, state, applied_by, params_snapshot, updated_at)
    VALUES
        (:preset_name, :scope_key, 'pending', :applied_by, :params_snapshot, NOW())
    ON CONFLICT (preset_name, scope_key) DO UPDATE SET
        state           = 'pending',
        applied_by      = EXCLUDED.applied_by,
        params_snapshot = EXCLUDED.params_snapshot,
        revoke_descriptor = NULL,
        apply_task_id   = NULL,
        revoke_task_id  = NULL,
        last_error      = NULL,
        updated_at      = NOW()
    RETURNING *
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

SELECT_FOR_UPDATE = DQLQuery(
    """
    SELECT * FROM iam.applied_presets
    WHERE preset_name = :preset_name
      AND scope_key   = :scope_key
    FOR UPDATE
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

SELECT_ROW = DQLQuery(
    """
    SELECT * FROM iam.applied_presets
    WHERE preset_name = :preset_name
      AND scope_key   = :scope_key
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

MARK_IN_PROGRESS = DQLQuery(
    """
    UPDATE iam.applied_presets SET
        state         = 'in_progress',
        apply_task_id = :task_id,
        updated_at    = NOW()
    WHERE preset_name = :preset_name
      AND scope_key   = :scope_key
    RETURNING *
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

MARK_APPLIED = DQLQuery(
    """
    UPDATE iam.applied_presets SET
        state             = 'applied',
        applied_at        = NOW(),
        revoke_descriptor = :revoke_descriptor,
        last_error        = NULL,
        updated_at        = NOW()
    WHERE preset_name = :preset_name
      AND scope_key   = :scope_key
    RETURNING *
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

MARK_FAILED = DQLQuery(
    """
    UPDATE iam.applied_presets SET
        state      = 'failed',
        last_error = :last_error,
        updated_at = NOW()
    WHERE preset_name = :preset_name
      AND scope_key   = :scope_key
    RETURNING *
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

MARK_REVOKE_PENDING = DQLQuery(
    """
    UPDATE iam.applied_presets SET
        state          = 'revoke_pending',
        revoke_task_id = :task_id,
        updated_at     = NOW()
    WHERE preset_name = :preset_name
      AND scope_key   = :scope_key
    RETURNING *
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

MARK_REVOKE_IN_PROGRESS = DQLQuery(
    """
    UPDATE iam.applied_presets SET
        state      = 'revoke_in_progress',
        updated_at = NOW()
    WHERE preset_name = :preset_name
      AND scope_key   = :scope_key
    RETURNING *
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

MARK_REVOKED = DQLQuery(
    """
    UPDATE iam.applied_presets SET
        state             = 'revoked',
        revoke_descriptor = NULL,
        last_error        = NULL,
        updated_at        = NOW()
    WHERE preset_name = :preset_name
      AND scope_key   = :scope_key
    RETURNING *
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

MARK_REVOKE_FAILED = DQLQuery(
    """
    UPDATE iam.applied_presets SET
        state      = 'revoke_failed',
        last_error = :last_error,
        updated_at = NOW()
    WHERE preset_name = :preset_name
      AND scope_key   = :scope_key
    RETURNING *
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

MARK_PARTIAL = DQLQuery(
    """
    UPDATE iam.applied_presets SET
        state      = 'partial',
        last_error = :last_error,
        updated_at = NOW()
    WHERE preset_name = :preset_name
      AND scope_key   = :scope_key
    RETURNING *
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

# ---------------------------------------------------------------------------
# DQL — list / search
# ---------------------------------------------------------------------------

LIST_BY_SCOPE_PREFIX = DQLQuery(
    """
    SELECT * FROM iam.applied_presets
    WHERE scope_key LIKE :scope_prefix
      AND (:preset_name IS NULL OR preset_name = :preset_name)
      AND (:state IS NULL OR state = :state)
    ORDER BY preset_name
    LIMIT :limit
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

LIST_BY_SCOPE_PREFIX_CURSOR = DQLQuery(
    """
    SELECT * FROM iam.applied_presets
    WHERE scope_key LIKE :scope_prefix
      AND (:preset_name IS NULL OR preset_name = :preset_name)
      AND (:state IS NULL OR state = :state)
      AND preset_name > :cursor
    ORDER BY preset_name
    LIMIT :limit
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

# ---------------------------------------------------------------------------
# DQL — list by exact scope, keyset on (applied_at DESC, preset_name)
# ---------------------------------------------------------------------------

LIST_FOR_SCOPE = DQLQuery(
    """
    SELECT * FROM iam.applied_presets
    WHERE scope_key = :scope_key
      AND state = :state
    ORDER BY applied_at DESC NULLS LAST, preset_name
    LIMIT :limit
    """,
    result_handler=ResultHandler.ALL_DICTS,
)

LIST_FOR_SCOPE_CURSOR = DQLQuery(
    """
    SELECT * FROM iam.applied_presets
    WHERE scope_key = :scope_key
      AND state = :state
      AND (
          applied_at < :cursor_applied_at
          OR (applied_at IS NOT DISTINCT FROM :cursor_applied_at AND preset_name > :cursor_preset_name)
      )
    ORDER BY applied_at DESC NULLS LAST, preset_name
    LIMIT :limit
    """,
    result_handler=ResultHandler.ALL_DICTS,
)
