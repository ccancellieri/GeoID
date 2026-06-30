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

"""Regression tests for issue #2569: role_hierarchy table missing on warm-start catalogs.

A catalog provisioned before IAM tables were added to the core DDL batch has
``collection_configs`` (the old sentinel) but no ``grants`` / ``role_hierarchy``.
The fix splits the DDL into two independent batches, each with its own sentinel:

  * ``_build_tenant_core_ddl_batch`` — sentinel: ``collection_configs``
  * ``_build_tenant_iam_ddl_batch``  — sentinel: ``grants``

When ``collection_configs`` exists but ``grants`` is absent, the core batch skips
(correct — collections already provisioned) while the IAM batch runs and creates
the missing tables.
"""

from __future__ import annotations

import asyncio
from typing import List
from unittest.mock import patch


# ---------------------------------------------------------------------------
# Helpers — minimal stubs that do not touch a real DB
# ---------------------------------------------------------------------------


class _FakeConn:
    """Stand-in connection; existence-check closures only need it for dispatch."""


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Structural tests — batch shape
# ---------------------------------------------------------------------------


def test_build_tenant_iam_ddl_batch_returns_ddlbatch_with_three_steps():
    """_build_tenant_iam_ddl_batch must return a DDLBatch with exactly three
    IAM steps (roles, role_hierarchy, grants)."""
    from dynastore.modules.db_config.query_executor import DDLBatch
    from dynastore.modules.catalog.catalog_service import _build_tenant_iam_ddl_batch

    batch = _build_tenant_iam_ddl_batch("s_test")

    assert isinstance(batch, DDLBatch)
    assert len(batch.steps) == 3, (
        f"Expected 3 IAM steps (roles, role_hierarchy, grants); got {len(batch.steps)}"
    )


def test_build_tenant_core_ddl_batch_does_not_contain_iam_steps():
    """_build_tenant_core_ddl_batch must no longer include IAM tables.
    Its two steps are TENANT_COLLECTIONS_DDL and tenant_configs_sql."""
    from dynastore.modules.db_config.query_executor import DDLBatch
    from dynastore.modules.catalog.catalog_service import _build_tenant_core_ddl_batch

    batch = _build_tenant_core_ddl_batch("s_test")

    assert isinstance(batch, DDLBatch)
    assert len(batch.steps) == 2, (
        f"Core batch must have exactly 2 steps (collections + configs); "
        f"got {len(batch.steps)}.  IAM steps must be in _build_tenant_iam_ddl_batch."
    )


# ---------------------------------------------------------------------------
# Sentinel-correctness tests — which table each batch keys on
# ---------------------------------------------------------------------------


def test_iam_batch_sentinel_checks_grants_not_collection_configs():
    """The IAM DDL batch sentinel must key on 'grants', not 'collection_configs'.

    This is the structural fix for issue #2569: an old catalog that has
    collection_configs but no grants must cause the IAM batch to run.
    """
    sentinel_queries: List[str] = []

    def _fake_check(conn, table_name, schema="platform"):
        sentinel_queries.append(table_name)
        return table_name == "grants"  # only grants present

    with patch(
        "dynastore.modules.db_config.locking_tools.check_table_exists",
        side_effect=_fake_check,
    ):
        from dynastore.modules.catalog.catalog_service import _build_tenant_iam_ddl_batch

        batch = _build_tenant_iam_ddl_batch("s_warmtest")

    # Invoke the sentinel existence check
    result = _run(
        batch.sentinel._executor._call_existence_check(
            _FakeConn(), {"schema": "s_warmtest"}
        )
    )

    assert "grants" in sentinel_queries, (
        "IAM sentinel must call check_table_exists for 'grants'"
    )
    assert "collection_configs" not in sentinel_queries, (
        "IAM sentinel must NOT key on 'collection_configs'"
    )
    assert result is True  # grants present → IAM batch would skip (fast path)


def test_core_batch_sentinel_checks_collection_configs():
    """The core DDL batch sentinel must continue to key on 'collection_configs'."""
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


# ---------------------------------------------------------------------------
# Warm-start scenario (issue #2569 regression test)
# ---------------------------------------------------------------------------


def test_warm_start_collection_configs_present_grants_absent_iam_sentinel_returns_false():
    """Regression test for issue #2569.

    Scenario: catalog was provisioned before IAM was added to the core batch.
    State: collection_configs EXISTS, grants ABSENT.

    Expected behaviour:
      * Core batch sentinel returns True  → core batch skips (correct).
      * IAM batch sentinel returns False  → IAM batch runs → missing tables created.
    """
    tables_present = {"collection_configs"}

    def _fake_check(conn, table_name, schema="platform"):
        return table_name in tables_present

    with patch(
        "dynastore.modules.db_config.locking_tools.check_table_exists",
        side_effect=_fake_check,
    ):
        from dynastore.modules.catalog.catalog_service import (
            _build_tenant_core_ddl_batch,
            _build_tenant_iam_ddl_batch,
        )

        schema = "s_legacy_catalog"
        core_batch = _build_tenant_core_ddl_batch(schema)
        iam_batch = _build_tenant_iam_ddl_batch(schema)

    fake_conn = _FakeConn()

    core_sentinel_exists = _run(
        core_batch.sentinel._executor._call_existence_check(
            fake_conn, {"schema": schema}
        )
    )
    iam_sentinel_exists = _run(
        iam_batch.sentinel._executor._call_existence_check(
            fake_conn, {"schema": schema}
        )
    )

    assert core_sentinel_exists is True, (
        "Core sentinel must return True (collection_configs present) "
        "so the collections batch is correctly skipped"
    )
    assert iam_sentinel_exists is False, (
        "IAM sentinel must return False (grants absent) "
        "so the IAM batch runs and creates the missing tables"
    )


def test_warm_start_fully_provisioned_both_sentinels_return_true():
    """Fully-provisioned catalog: both sentinels return True → both batches skip."""
    tables_present = {"collection_configs", "grants"}

    def _fake_check(conn, table_name, schema="platform"):
        return table_name in tables_present

    with patch(
        "dynastore.modules.db_config.locking_tools.check_table_exists",
        side_effect=_fake_check,
    ):
        from dynastore.modules.catalog.catalog_service import (
            _build_tenant_core_ddl_batch,
            _build_tenant_iam_ddl_batch,
        )

        schema = "s_full_catalog"
        core_batch = _build_tenant_core_ddl_batch(schema)
        iam_batch = _build_tenant_iam_ddl_batch(schema)

    fake_conn = _FakeConn()

    core_sentinel_exists = _run(
        core_batch.sentinel._executor._call_existence_check(
            fake_conn, {"schema": schema}
        )
    )
    iam_sentinel_exists = _run(
        iam_batch.sentinel._executor._call_existence_check(
            fake_conn, {"schema": schema}
        )
    )

    assert core_sentinel_exists is True, "Core sentinel must return True on fully-provisioned catalog"
    assert iam_sentinel_exists is True, "IAM sentinel must return True on fully-provisioned catalog"


def test_cold_start_both_sentinels_return_false():
    """Brand-new schema (no tables): both sentinels return False → both batches run."""
    def _fake_check(conn, table_name, schema="platform"):
        return False  # nothing exists yet

    with patch(
        "dynastore.modules.db_config.locking_tools.check_table_exists",
        side_effect=_fake_check,
    ):
        from dynastore.modules.catalog.catalog_service import (
            _build_tenant_core_ddl_batch,
            _build_tenant_iam_ddl_batch,
        )

        schema = "s_new_catalog"
        core_batch = _build_tenant_core_ddl_batch(schema)
        iam_batch = _build_tenant_iam_ddl_batch(schema)

    fake_conn = _FakeConn()

    core_sentinel_exists = _run(
        core_batch.sentinel._executor._call_existence_check(
            fake_conn, {"schema": schema}
        )
    )
    iam_sentinel_exists = _run(
        iam_batch.sentinel._executor._call_existence_check(
            fake_conn, {"schema": schema}
        )
    )

    assert core_sentinel_exists is False, "Core sentinel must return False on cold start"
    assert iam_sentinel_exists is False, "IAM sentinel must return False on cold start"


# ---------------------------------------------------------------------------
# Execute-path tests — drive DDLBatch.execute() to prove the heal actually runs
# (the sentinel tests above only exercise the existence check, not execute()).
# ---------------------------------------------------------------------------


def test_iam_batch_execute_runs_three_steps_when_grants_absent():
    """Heal path: with grants absent, DDLBatch.execute() runs all three IAM
    steps (roles, role_hierarchy, grants). This is the exact behaviour #2569
    depends on — a legacy catalog gets its missing IAM tables created."""
    from dynastore.modules.db_config.query_executor import DDLQuery

    def _fake_check(conn, table_name, schema="platform"):
        return False  # grants absent → sentinel False → steps run

    executed: List[object] = []

    async def _fake_step_execute(self, conn, **kwargs):
        executed.append(self)

    with patch(
        "dynastore.modules.db_config.locking_tools.check_table_exists",
        side_effect=_fake_check,
    ), patch.object(DDLQuery, "execute", _fake_step_execute):
        from dynastore.modules.catalog.catalog_service import _build_tenant_iam_ddl_batch

        batch = _build_tenant_iam_ddl_batch("s_heal")
        _run(batch.execute(_FakeConn(), schema="s_heal"))

    assert len(executed) == 3, (
        f"Expected all 3 IAM steps to execute when grants is absent; got {len(executed)}"
    )


def test_iam_batch_execute_skips_all_steps_when_grants_present():
    """No-op path: with grants present, DDLBatch.execute() skips every step
    (fast-path return on the sentinel), so a fully-provisioned catalog does
    no DDL work."""
    from dynastore.modules.db_config.query_executor import DDLQuery

    def _fake_check(conn, table_name, schema="platform"):
        return table_name == "grants"  # grants present → sentinel True → skip

    executed: List[object] = []

    async def _fake_step_execute(self, conn, **kwargs):
        executed.append(self)

    with patch(
        "dynastore.modules.db_config.locking_tools.check_table_exists",
        side_effect=_fake_check,
    ), patch.object(DDLQuery, "execute", _fake_step_execute):
        from dynastore.modules.catalog.catalog_service import _build_tenant_iam_ddl_batch

        batch = _build_tenant_iam_ddl_batch("s_full")
        _run(batch.execute(_FakeConn(), schema="s_full"))

    assert len(executed) == 0, (
        f"Expected zero IAM steps to execute when grants is present; got {len(executed)}"
    )
