#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Global one-shot migration: purge inert ``viewer`` role rows (#660).

`#643` slice 1 (PR #651) dropped ``viewer`` from the default
``IamRolesConfig.roles`` seed. Hierarchy is now
``sysadmin > admin > editor > user > anonymous``. The slice-1 ship uses
``INSERT ... ON CONFLICT DO NOTHING``, so any pre-existing ``viewer``
row in ``iam.roles`` (or per-catalog ``{schema}.roles``) persists on
review and prod tenants seeded before slice 1.

Today these rows are inert (``viewer`` is no longer in the
``known_roles`` allowlist enforced by ``IamService`` and no shipped
policy binds to it), but they pollute the role registry surface area
and confuse operators inspecting tenant configuration. This migration
deletes them — only when the row has no operator-attached policies and
no operator-attached metadata so any deliberate use of the name is
preserved.

Design mirrors :mod:`dynastore.modules.db_config.asset_routing_config_purge_migration`:

* ``pg_class`` / ``pg_namespace`` scan locates every schema that owns a
  ``roles`` table — covers both ``iam`` (platform) and every catalog
  schema.
* Each per-schema DELETE runs in its own ``managed_transaction`` so a
  single schema failure does not block the rest.
* Guarded by ``policies = '[]'::jsonb AND metadata = '{}'::jsonb`` so
  operators who repurposed the name (attached policies or stamped
  metadata) keep their row.
* Self-idempotent: subsequent invocations issue the same DELETEs but
  the rowcount is zero.

The associated ``{schema}.grants`` rows (``object_kind = 'role' AND
object_ref = 'viewer'``) are deleted in a second pass so any stale
grant referencing the role is cleaned even when the row above was
operator-customized and survived. Grants are independent: an unscoped
``viewer`` grant on a row that no longer exists is dead weight.
"""

from __future__ import annotations

import logging
from typing import Any

from dynastore.modules.db_config.query_executor import (
    DQLQuery,
    ResultHandler,
    managed_transaction,
)

logger = logging.getLogger(__name__)

_VIEWER_ROLE_NAME = "viewer"


_find_role_tables_query = DQLQuery(
    """
    SELECT n.nspname AS schema_name
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relkind = 'r'
       AND c.relname = 'roles'
       AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    """,
    result_handler=ResultHandler.ALL_DICTS,
)


async def purge_viewer_role_rows(engine: Any) -> int:
    """Delete inert ``viewer`` role rows across every schema.

    Returns the total number of rows deleted across both ``roles`` and
    ``grants`` tables (``0`` if ``engine`` is None or the tree is
    already clean). Safe to call on every startup.
    """
    if engine is None:
        return 0

    async with managed_transaction(engine) as conn:
        rows = await _find_role_tables_query.execute(conn)

    schemas = [row["schema_name"] for row in rows]

    deleted_total = 0
    for schema in schemas:
        delete_role_query = DQLQuery(
            f'DELETE FROM "{schema}".roles '
            f"WHERE name = :role_name "
            f"  AND policies = '[]'::jsonb "
            f"  AND metadata = '{{}}'::jsonb;",
            result_handler=ResultHandler.ROWCOUNT,
        )
        try:
            async with managed_transaction(engine) as conn:
                rowcount = await delete_role_query.execute(
                    conn, role_name=_VIEWER_ROLE_NAME,
                )
            n = int(rowcount or 0)
            if n > 0:
                logger.info(
                    "viewer role purge: deleted %d inert row(s) from %s.roles",
                    n, schema,
                )
            deleted_total += n
        except Exception as exc:  # noqa: BLE001 — never block startup
            logger.error(
                "viewer role purge: failed on %s.roles: %s", schema, exc,
            )

        # Grants referencing the role — independently cleaned. A grants
        # row that points to a no-longer-existent role is dead weight,
        # regardless of whether the role row was operator-customized
        # and survived above.
        delete_grants_query = DQLQuery(
            f'DELETE FROM "{schema}".grants '
            f"WHERE object_kind = 'role' AND object_ref = :role_name;",
            result_handler=ResultHandler.ROWCOUNT,
        )
        try:
            async with managed_transaction(engine) as conn:
                rowcount = await delete_grants_query.execute(
                    conn, role_name=_VIEWER_ROLE_NAME,
                )
            n = int(rowcount or 0)
            if n > 0:
                logger.info(
                    "viewer role purge: deleted %d grant row(s) from %s.grants",
                    n, schema,
                )
            deleted_total += n
        except Exception as exc:  # noqa: BLE001 — grants table may
            # not exist in every schema where a roles table exists
            # (test scaffolds, legacy schemas). Skip and continue.
            logger.debug(
                "viewer role purge: skipping grants on %s (%s)", schema, exc,
            )

    if deleted_total == 0:
        logger.debug("viewer role purge: nothing to delete.")
    return deleted_total


__all__ = ["purge_viewer_role_rows"]
