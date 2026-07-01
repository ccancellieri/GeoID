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

"""Regression tests for per-tenant IAM table provisioning (#2569, #2610).

IAM is an optional, self-contained module and owns its own per-tenant
persistence.  Core provisioning creates only the collections + config tables
(``_build_tenant_core_ddl_batch``) and never any IAM table.  The four
per-tenant IAM tables — ``roles``, ``role_hierarchy``, ``grants``, ``policies``
— are created solely by ``PostgresIamStorage._initialize_schema``, invoked by
the ``critical`` catalog lifecycle hook ``initialize_iam_tenant`` inside the
creation transaction.

Warm-start / self-heal (#2569 residual): that method's DDLBatch keys on a
combined sentinel over ALL FOUR tenant tables, so a catalog missing any one
(e.g. provisioned before a table was added) has the batch re-run and the
missing table(s) created on the next provision — no full tear-down.  Because
``policies`` is now inside that same batch (rather than a swallowed savepoint),
a catalog can no longer end up with ``roles``/``grants`` but no ``policies``
(the #2610 failure that made authorization fail closed with a 403).
"""

from __future__ import annotations

import asyncio
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers — minimal stubs that do not touch a real DB
# ---------------------------------------------------------------------------


class _FakeConn:
    """Stand-in connection; existence-check closures only need it for dispatch."""


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Core batch must not own any IAM table (separation of concerns)
# ---------------------------------------------------------------------------


def test_build_tenant_core_ddl_batch_does_not_contain_iam_steps():
    """The core DDL batch must contain no IAM tables — IAM owns its own
    per-tenant persistence.  Its two steps are TENANT_COLLECTIONS_DDL and
    tenant_configs_sql only."""
    from dynastore.modules.db_config.query_executor import DDLBatch
    from dynastore.modules.catalog.catalog_service import _build_tenant_core_ddl_batch

    batch = _build_tenant_core_ddl_batch("s_test")

    assert isinstance(batch, DDLBatch)
    assert len(batch.steps) == 2, (
        f"Core batch must have exactly 2 steps (collections + configs); "
        f"got {len(batch.steps)}.  IAM tables must not be created by core."
    )


def test_core_batch_sentinel_checks_collection_configs():
    """The core DDL batch sentinel must key on 'collection_configs'."""
    sentinel_queries: List[str] = []

    def _fake_check(conn, table_name, schema="platform"):
        sentinel_queries.append(table_name)
        return False

    with patch(
        "dynastore.modules.db_config.locking_tools.check_table_exists",
        side_effect=_fake_check,
    ):
        from dynastore.modules.catalog.catalog_service import _build_tenant_core_ddl_batch

        batch = _build_tenant_core_ddl_batch("s_coretest")

    _run(
        batch.sentinel._executor._call_existence_check(
            _FakeConn(), {"schema": "s_coretest"}
        )
    )

    assert "collection_configs" in sentinel_queries, (
        "Core batch sentinel must check 'collection_configs'"
    )


def test_core_batch_sentinel_never_checks_iam_tables():
    """The core batch sentinel must never key on an IAM table — core is
    IAM-agnostic after #2610."""
    sentinel_queries: List[str] = []

    def _fake_check(conn, table_name, schema="platform"):
        sentinel_queries.append(table_name)
        return False

    with patch(
        "dynastore.modules.db_config.locking_tools.check_table_exists",
        side_effect=_fake_check,
    ):
        from dynastore.modules.catalog.catalog_service import _build_tenant_core_ddl_batch

        batch = _build_tenant_core_ddl_batch("s_coretest")

    _run(
        batch.sentinel._executor._call_existence_check(
            _FakeConn(), {"schema": "s_coretest"}
        )
    )

    for iam_table in ("roles", "role_hierarchy", "grants", "policies"):
        assert iam_table not in sentinel_queries, (
            f"Core batch sentinel must not check IAM table {iam_table!r}"
        )


# ---------------------------------------------------------------------------
# _initialize_schema per-tenant path (postgres_iam_storage) — sole owner of
# the four per-tenant IAM tables; combined 4-table sentinel drives self-heal.
# ---------------------------------------------------------------------------


def test_initialize_schema_tenant_grants_present_role_hierarchy_absent_heals():
    """Regression for #2569 residual in postgres_iam_storage._initialize_schema.

    When the tenant schema has ``grants`` but not ``role_hierarchy``, the
    combined sentinel must return False so that the DDLBatch runs all tenant
    steps (roles, role_hierarchy, grants, policies).

    A fully-provisioned batch (all 4 tables present) would result in just the
    2 post-batch index execute() calls (CREATE_GRANTS_UNIQUE_WITH_RESOURCE +
    CREATE_IDX_GRANTS_RESOURCE).  A batch that runs adds 4 more step calls.
    """
    from dynastore.modules.db_config.query_executor import DDLQuery

    # Simulate: grants is present, role_hierarchy is absent.
    tables_present = {"grants", "roles"}  # role_hierarchy intentionally missing

    def _fake_check(conn, table_name, schema="platform"):
        return table_name in tables_present

    executed: List[object] = []

    async def _fake_step_execute(self, conn, **kwargs):
        executed.append(self)

    mock_ensure_schema = AsyncMock()
    mock_index = MagicMock(execute=AsyncMock())

    with (
        patch(
            "dynastore.modules.db_config.locking_tools.check_table_exists",
            side_effect=_fake_check,
        ),
        patch.object(DDLQuery, "execute", _fake_step_execute),
        patch(
            "dynastore.modules.db_config.maintenance_tools.ensure_schema_exists",
            mock_ensure_schema,
        ),
        patch(
            "dynastore.modules.iam.postgres_iam_storage.CREATE_GRANTS_UNIQUE_WITH_RESOURCE",
            mock_index,
        ),
        patch(
            "dynastore.modules.iam.postgres_iam_storage.CREATE_IDX_GRANTS_RESOURCE",
            mock_index,
        ),
    ):
        from dynastore.modules.iam.postgres_iam_storage import PostgresIamStorage

        storage = PostgresIamStorage.__new__(PostgresIamStorage)
        _run(storage._initialize_schema(_FakeConn(), schema="c_tenant_broken"))

    # With the batch running (sentinel False), all 4 tenant steps execute.
    # (The 2 post-batch index calls hit mock_index.execute, not DDLQuery.execute.)
    assert len(executed) == 4, (
        f"Expected 4 tenant DDL steps to execute when role_hierarchy is absent; "
        f"got {len(executed)}.  The combined sentinel must return False when any "
        f"per-tenant IAM table is missing."
    )


def test_initialize_schema_tenant_policies_absent_heals():
    """Direct #2610 coverage: a catalog with roles/role_hierarchy/grants but no
    ``policies`` must re-run the batch so ``policies`` is (re)created.  This is
    the exact broken state the issue reports — with ``policies`` now inside the
    combined-sentinel batch, its absence forces the heal."""
    from dynastore.modules.db_config.query_executor import DDLQuery

    # policies intentionally missing; the other three present.
    tables_present = {"roles", "role_hierarchy", "grants"}

    def _fake_check(conn, table_name, schema="platform"):
        return table_name in tables_present

    executed: List[object] = []

    async def _fake_step_execute(self, conn, **kwargs):
        executed.append(self)

    mock_ensure_schema = AsyncMock()
    mock_index = MagicMock(execute=AsyncMock())

    with (
        patch(
            "dynastore.modules.db_config.locking_tools.check_table_exists",
            side_effect=_fake_check,
        ),
        patch.object(DDLQuery, "execute", _fake_step_execute),
        patch(
            "dynastore.modules.db_config.maintenance_tools.ensure_schema_exists",
            mock_ensure_schema,
        ),
        patch(
            "dynastore.modules.iam.postgres_iam_storage.CREATE_GRANTS_UNIQUE_WITH_RESOURCE",
            mock_index,
        ),
        patch(
            "dynastore.modules.iam.postgres_iam_storage.CREATE_IDX_GRANTS_RESOURCE",
            mock_index,
        ),
    ):
        from dynastore.modules.iam.postgres_iam_storage import PostgresIamStorage

        storage = PostgresIamStorage.__new__(PostgresIamStorage)
        _run(storage._initialize_schema(_FakeConn(), schema="c_tenant_no_policies"))

    assert len(executed) == 4, (
        f"Expected all 4 tenant DDL steps to execute when 'policies' is absent; "
        f"got {len(executed)}.  A missing 'policies' table must force the heal "
        f"(the #2610 failure mode)."
    )


def test_initialize_schema_tenant_all_tables_present_batch_skips():
    """Warm-start no-op for postgres_iam_storage._initialize_schema: when all
    four tenant IAM tables are present, the combined sentinel returns True and
    the DDLBatch is skipped — only the two post-batch index DDLQuery.execute()
    calls reach the patched recorder (via the non-batch index DDLQuery instances).
    """
    from dynastore.modules.db_config.query_executor import DDLQuery

    # All four tenant tables present → sentinel True → batch skips.
    tables_present = {"roles", "role_hierarchy", "grants", "policies"}

    def _fake_check(conn, table_name, schema="platform"):
        return table_name in tables_present

    executed: List[object] = []

    async def _fake_step_execute(self, conn, **kwargs):
        executed.append(self)

    mock_ensure_schema = AsyncMock()
    mock_index = MagicMock(execute=AsyncMock())

    with (
        patch(
            "dynastore.modules.db_config.locking_tools.check_table_exists",
            side_effect=_fake_check,
        ),
        patch.object(DDLQuery, "execute", _fake_step_execute),
        patch(
            "dynastore.modules.db_config.maintenance_tools.ensure_schema_exists",
            mock_ensure_schema,
        ),
        patch(
            "dynastore.modules.iam.postgres_iam_storage.CREATE_GRANTS_UNIQUE_WITH_RESOURCE",
            mock_index,
        ),
        patch(
            "dynastore.modules.iam.postgres_iam_storage.CREATE_IDX_GRANTS_RESOURCE",
            mock_index,
        ),
    ):
        from dynastore.modules.iam.postgres_iam_storage import PostgresIamStorage

        storage = PostgresIamStorage.__new__(PostgresIamStorage)
        _run(storage._initialize_schema(_FakeConn(), schema="c_tenant_full"))

    # Batch was skipped: no tenant step DDLQuery.execute() was called.
    assert len(executed) == 0, (
        f"Expected 0 tenant DDL steps when all IAM tables present (batch must skip); "
        f"got {len(executed)}"
    )
