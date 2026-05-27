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

"""Backfill migration: insert an ``applied`` audit row for ``default_roles_baseline``.

Idempotent: no-op if ``(preset_name='default_roles_baseline', scope_key='platform')``
already exists in ``iam.applied_presets``.

On first run after upgrade the migration snapshots the current role rows
and hierarchy edges actually present in the ``iam`` schema so the
``revoke_descriptor`` reflects DB state — not the canonical declaration.
Divergence between the DB and the canonical declaration is preserved so
DELETE removes exactly what is present, not hypothetical defaults.
"""
from __future__ import annotations

import json
import logging
from typing import Any, List, Tuple

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)
from dynastore.models.protocols.authorization import (
    _DEFAULT_CATALOG_ROLES,
    _DEFAULT_PLATFORM_ROLES,
)

logger = logging.getLogger(__name__)

_PRESET_NAME = "default_roles_baseline"
_SCOPE_KEY = "platform"

# Canonical role names from the preset.
_ROLE_NAMES: List[str] = [
    s.name
    for s in list(_DEFAULT_PLATFORM_ROLES) + list(_DEFAULT_CATALOG_ROLES)
]

# Canonical hierarchy edges derived from RoleSeed.parent.
_HIERARCHY_EDGES: List[Tuple[str, str]] = [
    (s.parent, s.name)
    for s in list(_DEFAULT_PLATFORM_ROLES) + list(_DEFAULT_CATALOG_ROLES)
    if s.parent
]

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

SELECT_ROLE = DQLQuery(
    "SELECT name FROM iam.roles WHERE name = :name OR id = :name",
    result_handler=ResultHandler.ONE_OR_NONE,
)

SELECT_HIERARCHY_EDGE = DQLQuery(
    """
    SELECT 1 FROM iam.role_hierarchy
    WHERE parent_role = :parent_role AND child_role = :child_role
    """,
    result_handler=ResultHandler.ONE_OR_NONE,
)


async def run_backfill(engine: Any) -> None:
    """Insert a synthetic ``applied`` audit row for ``default_roles_baseline``.

    ``engine`` must be an async SQLAlchemy engine (DbResource).
    Opens its own transaction internally.
    """
    async with managed_transaction(engine) as conn:
        existing = await SELECT_EXISTING.execute(
            conn, preset_name=_PRESET_NAME, scope_key=_SCOPE_KEY
        )
        if existing is not None:
            logger.debug(
                "backfill: default_roles_baseline audit row already present — skipping",
            )
            return

        # Snapshot actual role names present in the iam schema.
        actual_role_names: List[str] = []
        for rname in _ROLE_NAMES:
            try:
                row = await SELECT_ROLE.execute(conn, name=rname)
                if row is not None:
                    actual_role_names.append(rname)
            except Exception as exc:
                logger.warning(
                    "backfill: could not read role %r — %s; skipping", rname, exc
                )

        # Snapshot actual hierarchy edges present in the iam schema.
        actual_edges: List[List[str]] = []
        for parent, child in _HIERARCHY_EDGES:
            try:
                row = await SELECT_HIERARCHY_EDGE.execute(
                    conn, parent_role=parent, child_role=child
                )
                if row is not None:
                    actual_edges.append([parent, child])
            except Exception as exc:
                logger.warning(
                    "backfill: could not read hierarchy edge %r → %r — %s; skipping",
                    parent,
                    child,
                    exc,
                )

        revoke_descriptor = {
            "role_names": actual_role_names,
            "hierarchy_edges": actual_edges,
        }

        await INSERT_APPLIED.execute(
            conn,
            preset_name=_PRESET_NAME,
            scope_key=_SCOPE_KEY,
            params_snapshot=json.dumps({}),
            revoke_descriptor=json.dumps(revoke_descriptor),
        )
        logger.info(
            "backfill: inserted default_roles_baseline audit row "
            "(roles=%s, edges=%s)",
            actual_role_names,
            actual_edges,
        )
