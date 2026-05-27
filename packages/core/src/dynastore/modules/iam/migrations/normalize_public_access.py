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

"""Migration: normalize the ``public_access`` policy resource list.

Removes the pre-2026-04-29 overly-broad ``/web/.*`` catch-all pattern from
the ``public_access`` policy in the global ``iam`` schema when it is still
present.  That pattern accidentally allowed anonymous access to gated
dashboard endpoints.  The fix (commit eaa8cbf89) replaced the catch-all with
an enumerated safe-sub-path list.

With ``_ALWAYS_REFRESH`` removed in PR-5 the old broken row would no longer
be auto-rewritten at boot, so this one-shot migration normalizes it explicitly.
Operator edits to *other* resources in the ``public_access`` row are preserved
— the migration only replaces the single catch-all entry when it is present.

Idempotent: records completion in ``iam.applied_presets`` as the synthetic
preset name ``public_access_normalize`` so subsequent runs skip the work.
"""
from __future__ import annotations

import json
import logging
from typing import Any, List, Optional

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)

logger = logging.getLogger(__name__)

_PRESET_NAME = "public_access_normalize"
_SCOPE_KEY = "platform"

# The unanchored catch-all that the pre-fix row contained.
_STALE_PATTERN = "/web/.*"

# Replacement enumerated safe sub-paths that supersede the catch-all.
_SAFE_WEB_PATHS: List[str] = [
    "/web/?$",
    "/web/pages/.*",
    "/web/extension-static/.*",
    "/web/geoid/.*",
    "/web/static/.*",
    "/web/website/.*",
    "/web/docs-content/.*",
    "/web/docs-manifest",
    "/web/config/.*",
    "/web/health",
    "/web/dashboard/?$",
    "/web/lite/.*",
]

# ---------------------------------------------------------------------------
# Queries (module-level so they are patchable in tests)
# ---------------------------------------------------------------------------

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
    SELECT resources
    FROM iam.policies
    WHERE id = 'public_access'
      AND partition_key = 'global'
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)

UPDATE_POLICY_RESOURCES = DQLQuery(
    """
    UPDATE iam.policies
    SET resources = :resources::jsonb,
        updated_at = NOW()
    WHERE id = 'public_access'
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


def _normalize_resources(resources: List[str]) -> Optional[List[str]]:
    """Return the corrected resource list, or None if no change is needed.

    Replaces the stale ``/web/.*`` catch-all with the enumerated safe
    sub-paths.  Other entries are preserved unchanged.  Returns ``None`` when
    the stale pattern is absent so the caller can skip the UPDATE.
    """
    if _STALE_PATTERN not in resources:
        return None

    result: List[str] = []
    replaced = False
    for entry in resources:
        if entry == _STALE_PATTERN:
            if not replaced:
                result.extend(_SAFE_WEB_PATHS)
                replaced = True
        else:
            result.append(entry)
    return result


async def run_migration(engine: Any) -> None:
    """Normalize the ``public_access`` policy and record completion.

    ``engine`` must be an async SQLAlchemy engine.  The function manages its
    own transaction.
    """
    async with managed_transaction(engine) as conn:
        # Idempotency guard — skip if already applied.
        already = await SELECT_APPLIED.execute(
            conn, preset_name=_PRESET_NAME, scope_key=_SCOPE_KEY
        )
        if already is not None:
            logger.debug(
                "normalize_public_access: already applied — skipping"
            )
            return

        # Read current resource list.
        row = await SELECT_POLICY.execute(conn)
        if row is None:
            logger.info(
                "normalize_public_access: public_access policy not found "
                "in iam schema — nothing to normalize"
            )
            await _record_applied(conn)
            return

        raw = row[0] if isinstance(row, (list, tuple)) else row.get("resources")
        if isinstance(raw, str):
            try:
                resources: List[str] = json.loads(raw)
            except (ValueError, TypeError):
                resources = []
        elif isinstance(raw, list):
            resources = raw
        else:
            resources = []

        normalized = _normalize_resources(resources)
        if normalized is None:
            logger.debug(
                "normalize_public_access: stale /web/.* pattern absent — "
                "no update needed"
            )
        else:
            await UPDATE_POLICY_RESOURCES.execute(
                conn, resources=json.dumps(normalized)
            )
            logger.info(
                "normalize_public_access: replaced /web/.* catch-all with "
                "enumerated safe sub-paths in public_access policy"
            )

        await INSERT_APPLIED.execute(
            conn,
            preset_name=_PRESET_NAME,
            scope_key=_SCOPE_KEY,
            params_snapshot=json.dumps({}),
            revoke_descriptor=json.dumps({"note": "one-shot normalization; no revoke path"}),
        )


async def _record_applied(conn: Any) -> None:
    """Record the migration as applied when the policy row is absent."""
    await INSERT_APPLIED.execute(
        conn,
        preset_name=_PRESET_NAME,
        scope_key=_SCOPE_KEY,
        params_snapshot=json.dumps({}),
        revoke_descriptor=json.dumps({"note": "public_access policy absent; no-op"}),
    )
