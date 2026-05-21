#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0

"""One-shot additive migration: ``catalog.catalogs.provisioning_checklist``.

#1175 introduces a per-catalog provisioning checklist (a JSONB map of
``{provisioner_key: step_status}``) that drives ``provisioning_status``.
Fresh deployments get the column from ``CATALOGS_TABLE_DDL``; this migration
backfills it on already-provisioned databases.

Additive and self-idempotent: ``ADD COLUMN IF NOT EXISTS`` is a no-op once the
column exists, and the column is nullable (NULL = no checklist, i.e. legacy /
on-prem catalogs that were already ``ready``). Never blocks startup.
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

_ADD_COLUMN_SQL = (
    "ALTER TABLE catalog.catalogs "
    "ADD COLUMN IF NOT EXISTS provisioning_checklist JSONB DEFAULT NULL;"
)


async def migrate_provisioning_checklist_column(engine: Any) -> bool:
    """Ensure ``catalog.catalogs.provisioning_checklist`` exists.

    Returns ``True`` when the statement executed (idempotent), ``False`` when
    the engine is unavailable. Safe to call on every startup.
    """
    if engine is None:
        return False
    async with managed_transaction(engine) as conn:
        await DQLQuery(_ADD_COLUMN_SQL, result_handler=ResultHandler.NONE).execute(conn)
    logger.debug("provisioning_checklist column ensured on catalog.catalogs")
    return True


__all__ = ["migrate_provisioning_checklist_column"]
