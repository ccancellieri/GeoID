#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""Global one-shot migration: rebuild stale unique ``idx_*_attributes_ext_id``
indexes across every tenant schema.

Pre-2026-04-18 the attributes-sidecar DDL emitted the unique index as
``(geoid, external_id)``. When the feature table also carries a ``validity``
column the upsert target ``ON CONFLICT (geoid, validity)`` misses the 2-column
shape and the row write fails with SQLSTATE 23505. The DDL was corrected to
``(geoid, validity, external_id)`` but ``CREATE UNIQUE INDEX IF NOT EXISTS``
cannot rewrite an existing index.

Previously this lived in ``extensions/dimensions/dimensions_extension.py``
and only inspected the ``_dimensions_`` catalog's schema. Any catalog with
validity-tracking attributes sidecars that pre-dated the DDL fix still
carried the stale shape and hit 23505 on re-ingestion.

Design
------
* Scanned by ``pg_index`` across **every schema** in one query — the
  pattern (unique + 2-column + table ending in ``_attributes`` + has a
  ``validity`` column) is narrow enough that the scan is cheap and
  self-idempotent: once rebuilt, the row drops out of the result set.
* No ``PropertiesProtocol`` bookkeeping. The query itself is the "done
  flag": if it returns zero rows the migration is over.
* Each rebuild runs in its own ``managed_transaction`` so a single failed
  table doesn't block the rest.
"""

from __future__ import annotations

import logging
from typing import Any

from dynastore.modules.db_config.query_executor import (
    DDLQuery,
    DQLQuery,
    ResultHandler,
    managed_transaction,
)

logger = logging.getLogger(__name__)


_find_stale_query = DQLQuery(
    """
    SELECT n.nspname AS schema_name,
           c.relname  AS table_name,
           i.relname  AS index_name
      FROM pg_index x
      JOIN pg_class i ON i.oid = x.indexrelid
      JOIN pg_class c ON c.oid = x.indrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE x.indisunique
       AND c.relname LIKE '%\\_attributes' ESCAPE '\\'
       AND i.relname LIKE 'idx\\_%\\_attributes\\_ext\\_id' ESCAPE '\\'
       AND array_length(x.indkey::int[], 1) = 2
       AND EXISTS (
           SELECT 1 FROM pg_attribute a
            WHERE a.attrelid = c.oid
              AND a.attname = 'validity'
              AND a.attnum > 0
       )
    """,
    result_handler=ResultHandler.ALL_DICTS,
)


async def migrate_stale_attributes_ext_id_indexes(engine: Any) -> int:
    """Rebuild every stale 2-column ``idx_*_attributes_ext_id`` in the
    database.

    Returns the number of indexes rebuilt (``0`` when the tree is clean).
    Safe to call on every startup: the scan is narrow, the rebuild is
    driven by query output, and duplicate invocations are no-ops.
    """
    if engine is None:
        return 0

    async with managed_transaction(engine) as conn:
        rows = await _find_stale_query.execute(conn)

    if not rows:
        return 0

    logger.warning(
        "Stale-index migration: rebuilding %d unique index(es) from "
        "(geoid, external_id) → (geoid, validity, external_id).",
        len(rows),
    )

    rebuilt = 0
    for row in rows:
        schema = row["schema_name"]
        table_name = row["table_name"]
        index_name = row["index_name"]
        rebuild_sql = (
            f'DROP INDEX IF EXISTS "{schema}"."{index_name}"; '
            f'CREATE UNIQUE INDEX "{index_name}" '
            f'ON "{schema}"."{table_name}" (geoid, validity, external_id);'
        )
        try:
            async with managed_transaction(engine) as conn:
                await DDLQuery(rebuild_sql).execute(conn)
            rebuilt += 1
            logger.info(
                "Stale-index migration: rebuilt %s.%s", schema, index_name,
            )
        except Exception as exc:  # noqa: BLE001 — surface but keep going
            logger.error(
                "Stale-index migration: failed to rebuild %s.%s: %s",
                schema, index_name, exc,
            )
    return rebuilt


__all__ = ["migrate_stale_attributes_ext_id_indexes"]
