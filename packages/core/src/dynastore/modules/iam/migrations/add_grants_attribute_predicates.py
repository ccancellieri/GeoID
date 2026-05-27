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

"""Migration: add ``attribute_predicates`` JSONB column to ``{schema}.grants``.

Pre-#1441 the grants table had no attribute-predicate column.  This migration
adds the column with a ``NOT NULL DEFAULT '[]'`` so existing rows behave as
before (no predicates = pure RBAC, full backwards compatibility).

The column is added to EVERY schema that contains a ``grants`` table (both the
platform ``iam`` schema and any tenant catalog schema that has been
initialised).  The query is idempotent: ``ALTER TABLE … ADD COLUMN IF NOT
EXISTS`` is a no-op when the column already exists.

Idempotent: completion is recorded in ``iam.applied_presets`` as the synthetic
key ``add_grants_attribute_predicates`` so subsequent boots skip the work after
the first successful run.
"""
from __future__ import annotations

import json
import logging
from typing import Any, List

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    ResultHandler,
    managed_transaction,
)

logger = logging.getLogger(__name__)

_PRESET_NAME = "add_grants_attribute_predicates"
_SCOPE_KEY = "platform"


SELECT_APPLIED = DQLQuery(
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

# Idempotent: ADD COLUMN IF NOT EXISTS is a no-op on warm DBs.
_ALTER_GRANTS_SQL = (
    "ALTER TABLE {schema}.grants "
    "ADD COLUMN IF NOT EXISTS attribute_predicates "
    "JSONB NOT NULL DEFAULT '[]';"
)

# List all schemas that own a grants table.  We add the column to every
# tenant schema so the predicate path is uniform across platform + catalog
# scopes without needing a per-tenant migration step.
_LIST_SCHEMAS_WITH_GRANTS = DQLQuery(
    """
    SELECT nsp.nspname AS schema_name
    FROM pg_class rel
    JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
    WHERE rel.relname = 'grants'
      AND rel.relkind = 'r'
    ORDER BY nsp.nspname
    """,
    result_handler=ResultHandler.ALL_DICTS,
)


async def run_migration(engine: Any) -> None:
    """Add ``attribute_predicates`` to every ``grants`` table found.

    ``engine`` must be an async SQLAlchemy engine.  The function manages its
    own transaction.
    """
    async with managed_transaction(engine) as conn:
        already = await SELECT_APPLIED.execute(
            conn, preset_name=_PRESET_NAME, scope_key=_SCOPE_KEY
        )
        if already is not None:
            logger.debug(
                "add_grants_attribute_predicates: already applied — skipping"
            )
            return

        schemas_with_grants: List[str] = await _collect_grant_schemas(conn)

        for schema in schemas_with_grants:
            sql = _ALTER_GRANTS_SQL.format(schema=f'"{schema}"')
            try:
                await DDLQuery(sql).execute(conn)
                logger.info(
                    "add_grants_attribute_predicates: column added to %s.grants",
                    schema,
                )
            except Exception as exc:
                # Non-fatal per schema: log and continue so a single bad
                # tenant schema does not block the platform schema update.
                logger.warning(
                    "add_grants_attribute_predicates: failed on schema %s: %s",
                    schema,
                    exc,
                )

        await _record_applied(
            conn,
            note=f"column added to schemas: {schemas_with_grants}",
        )


async def _collect_grant_schemas(conn: Any) -> List[str]:
    """Return schema names that contain a ``grants`` relation."""
    try:
        rows = await _LIST_SCHEMAS_WITH_GRANTS.execute(conn)
        if not rows:
            return []
        return [row["schema_name"] for row in rows if row.get("schema_name")]
    except Exception as exc:
        logger.warning(
            "add_grants_attribute_predicates: could not list schemas — "
            "falling back to 'iam' only: %s",
            exc,
        )
        return ["iam"]


async def _record_applied(conn: Any, note: str) -> None:
    await INSERT_APPLIED.execute(
        conn,
        preset_name=_PRESET_NAME,
        scope_key=_SCOPE_KEY,
        params_snapshot=json.dumps({"column": "attribute_predicates"}),
        revoke_descriptor=json.dumps({"note": note}),
    )
