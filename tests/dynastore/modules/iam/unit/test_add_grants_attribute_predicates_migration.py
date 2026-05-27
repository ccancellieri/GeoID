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

"""Tests for the add_grants_attribute_predicates migration (#1441).

Covers:
1. Happy path — ALTER TABLE runs on each schema returned by the introspect query.
2. Idempotency — already applied → run_migration returns early, no ALTER called.
3. Schema introspect failure → falls back to 'iam' only (non-fatal).
4. Per-schema ALTER failure → logged, other schemas continue (non-fatal).
"""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

from dynastore.modules.iam.migrations import (
    add_grants_attribute_predicates as m,
)


# ---------------------------------------------------------------------------
# Transaction context manager stub
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _txn(_engine: Any):
    yield MagicMock()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_migration_runs_alter_on_all_schemas():
    """ALTER TABLE is called once per schema found by the introspect query."""
    engine = MagicMock()
    ddl_execute_calls: List[Any] = []
    insert_calls: List[Dict[str, Any]] = []

    async def _select_applied(_resource, **kw):
        return None  # not yet applied

    async def _list_schemas(_resource, **kw):
        return [{"schema_name": "iam"}, {"schema_name": "tenant_abc"}]

    async def _insert(_resource, **kw):
        insert_calls.append(kw)

    async def _ddl_execute(self, conn, **kw):
        ddl_execute_calls.append(self)

    conn = MagicMock()

    @asynccontextmanager
    async def _txn_conn(_engine):
        yield conn

    from dynastore.modules.db_config.query_executor import DDLQuery

    with patch.object(m, "managed_transaction", side_effect=_txn_conn), \
         patch.object(m.SELECT_APPLIED, "execute", _select_applied), \
         patch.object(m._LIST_SCHEMAS_WITH_GRANTS, "execute", _list_schemas), \
         patch.object(m.INSERT_APPLIED, "execute", _insert), \
         patch.object(DDLQuery, "execute", _ddl_execute):
        await m.run_migration(engine)

    # DDLQuery.execute was called once per schema.
    assert len(ddl_execute_calls) == 2
    assert len(insert_calls) == 1
    assert insert_calls[0]["preset_name"] == m._PRESET_NAME


@pytest.mark.asyncio
async def test_migration_idempotent_when_audit_row_exists():
    """Already applied → no ALTER, no INSERT."""
    engine = MagicMock()
    ddl_execute_calls: List[Any] = []
    conn = MagicMock()

    @asynccontextmanager
    async def _txn_conn(_engine):
        yield conn

    async def _select_applied(_resource, **kw):
        return (1,)  # already applied

    async def _ddl_execute(self, c, **kw):
        ddl_execute_calls.append(self)

    from dynastore.modules.db_config.query_executor import DDLQuery

    with patch.object(m, "managed_transaction", side_effect=_txn_conn), \
         patch.object(m.SELECT_APPLIED, "execute", _select_applied), \
         patch.object(DDLQuery, "execute", _ddl_execute):
        await m.run_migration(engine)

    assert ddl_execute_calls == []


@pytest.mark.asyncio
async def test_migration_falls_back_to_iam_on_introspect_failure():
    """If schema list query fails, migration still runs on 'iam'."""
    engine = MagicMock()
    ddl_execute_calls: List[Any] = []
    insert_calls: List[Any] = []

    async def _select_applied(_resource, **kw):
        return None

    async def _list_schemas_fail(_resource, **kw):
        raise RuntimeError("pg_class unavailable")

    async def _insert(_resource, **kw):
        insert_calls.append(kw)

    async def _ddl_execute(self, conn, **kw):
        ddl_execute_calls.append(self)

    conn = MagicMock()

    @asynccontextmanager
    async def _txn_conn(_engine):
        yield conn

    from dynastore.modules.db_config.query_executor import DDLQuery

    with patch.object(m, "managed_transaction", side_effect=_txn_conn), \
         patch.object(m.SELECT_APPLIED, "execute", _select_applied), \
         patch.object(m._LIST_SCHEMAS_WITH_GRANTS, "execute", _list_schemas_fail), \
         patch.object(m.INSERT_APPLIED, "execute", _insert), \
         patch.object(DDLQuery, "execute", _ddl_execute):
        await m.run_migration(engine)

    # Still runs ALTER on 'iam' fallback.
    assert len(ddl_execute_calls) == 1
    assert len(insert_calls) == 1


@pytest.mark.asyncio
async def test_migration_continues_on_per_schema_alter_failure():
    """ALTER failure on one schema is logged but doesn't block the rest."""
    engine = MagicMock()
    insert_calls: List[Any] = []
    call_count = 0

    async def _select_applied(_resource, **kw):
        return None

    async def _list_schemas(_resource, **kw):
        return [{"schema_name": "iam"}, {"schema_name": "bad_tenant"}]

    async def _insert(_resource, **kw):
        insert_calls.append(kw)

    async def _ddl_execute_with_failure(self, conn, **kw):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("tenant schema error")

    conn = MagicMock()

    @asynccontextmanager
    async def _txn_conn(_engine):
        yield conn

    from dynastore.modules.db_config.query_executor import DDLQuery

    with patch.object(m, "managed_transaction", side_effect=_txn_conn), \
         patch.object(m.SELECT_APPLIED, "execute", _select_applied), \
         patch.object(m._LIST_SCHEMAS_WITH_GRANTS, "execute", _list_schemas), \
         patch.object(m.INSERT_APPLIED, "execute", _insert), \
         patch.object(DDLQuery, "execute", _ddl_execute_with_failure):
        # Must NOT raise.
        await m.run_migration(engine)

    # Completion is still recorded.
    assert len(insert_calls) == 1
