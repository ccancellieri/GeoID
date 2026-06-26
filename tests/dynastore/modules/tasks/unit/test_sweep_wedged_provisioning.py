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
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Unit tests for sweep_wedged_provisioning_catalogs (#1902).

Covers the reconciler that finds catalogs stuck in 'provisioning' with no
live/queued provisioning task and calls drain_pending_checklist_steps on each.

No DB, no GCP — all SQL and protocol calls are mocked.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.tasks.tasks_module import sweep_wedged_provisioning_catalogs


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _fake_managed_transaction(_engine):
    yield MagicMock()


def _make_rows(catalog_ids: List[str]) -> List[Dict[str, Any]]:
    return [{"catalog_id": cat_id} for cat_id in catalog_ids]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sweep_drains_wedged_catalogs():
    """Catalogs returned by the scan query are passed to drain_pending_checklist_steps."""
    fake_scan_query = AsyncMock()
    fake_scan_query.execute = AsyncMock(
        return_value=_make_rows(["cat-wedged-1", "cat-wedged-2"])
    )

    mock_catalogs = MagicMock()
    mock_catalogs.drain_pending_checklist_steps = AsyncMock(return_value=True)

    with (
        patch(
            "dynastore.modules.tasks.tasks_module.managed_transaction",
            _fake_managed_transaction,
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.DQLQuery",
            return_value=fake_scan_query,
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.check_table_exists",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_catalogs,
        ),
    ):
        drained = await sweep_wedged_provisioning_catalogs(
            engine=object(), min_age_s=600.0, sample_limit=50,
        )

    assert drained == 2
    assert mock_catalogs.drain_pending_checklist_steps.await_count == 2
    calls = mock_catalogs.drain_pending_checklist_steps.call_args_list
    catalog_ids_drained = [c.args[0] for c in calls]
    assert "cat-wedged-1" in catalog_ids_drained
    assert "cat-wedged-2" in catalog_ids_drained


@pytest.mark.asyncio
async def test_sweep_returns_zero_when_no_wedged_catalogs():
    """When the scan returns no rows, the function returns 0 and no drains run."""
    fake_scan_query = AsyncMock()
    fake_scan_query.execute = AsyncMock(return_value=[])

    mock_catalogs = MagicMock()
    mock_catalogs.drain_pending_checklist_steps = AsyncMock(return_value=False)

    with (
        patch(
            "dynastore.modules.tasks.tasks_module.managed_transaction",
            _fake_managed_transaction,
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.DQLQuery",
            return_value=fake_scan_query,
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.check_table_exists",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_catalogs,
        ),
    ):
        drained = await sweep_wedged_provisioning_catalogs(
            engine=object(), min_age_s=600.0, sample_limit=50,
        )

    assert drained == 0
    mock_catalogs.drain_pending_checklist_steps.assert_not_awaited()


@pytest.mark.asyncio
async def test_sweep_returns_zero_when_catalogs_table_absent():
    """When catalog.catalogs doesn't exist (fresh DB), sweep returns 0 safely."""
    with (
        patch(
            "dynastore.modules.tasks.tasks_module.managed_transaction",
            _fake_managed_transaction,
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.check_table_exists",
            new=AsyncMock(return_value=False),
        ),
    ):
        drained = await sweep_wedged_provisioning_catalogs(
            engine=object(), min_age_s=600.0, sample_limit=50,
        )

    assert drained == 0


@pytest.mark.asyncio
async def test_sweep_continues_when_one_drain_fails():
    """A drain failure for one catalog must not stop the remaining ones."""
    fake_scan_query = AsyncMock()
    fake_scan_query.execute = AsyncMock(
        return_value=_make_rows(["cat-ok", "cat-broken", "cat-ok-2"])
    )

    drain_calls: list = []

    async def _drain_side_effect(catalog_id, **kwargs):
        drain_calls.append(catalog_id)
        if catalog_id == "cat-broken":
            raise RuntimeError("simulated drain failure")
        return True

    mock_catalogs = MagicMock()
    mock_catalogs.drain_pending_checklist_steps = AsyncMock(
        side_effect=_drain_side_effect
    )

    with (
        patch(
            "dynastore.modules.tasks.tasks_module.managed_transaction",
            _fake_managed_transaction,
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.DQLQuery",
            return_value=fake_scan_query,
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.check_table_exists",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=mock_catalogs,
        ),
    ):
        drained = await sweep_wedged_provisioning_catalogs(
            engine=object(), min_age_s=600.0, sample_limit=50,
        )

    # Two of three drained successfully; the broken one was skipped.
    assert drained == 2
    assert len(drain_calls) == 3  # all three were attempted


@pytest.mark.asyncio
async def test_sweep_returns_zero_when_protocol_unavailable():
    """When CatalogsProtocol is not registered, sweep logs a warning and returns 0."""
    fake_scan_query = AsyncMock()
    fake_scan_query.execute = AsyncMock(
        return_value=_make_rows(["cat-wedged"])
    )

    with (
        patch(
            "dynastore.modules.tasks.tasks_module.managed_transaction",
            _fake_managed_transaction,
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.DQLQuery",
            return_value=fake_scan_query,
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.check_table_exists",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "dynastore.tools.discovery.get_protocol",
            return_value=None,
        ),
    ):
        drained = await sweep_wedged_provisioning_catalogs(
            engine=object(), min_age_s=600.0, sample_limit=50,
        )

    assert drained == 0


# ---------------------------------------------------------------------------
# Regression: sweep must exclude BOTH task types from the NOT EXISTS guard
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sweep_sql_covers_both_provision_task_types() -> None:
    """The NOT EXISTS guard must match both 'gcp_provision_catalog' and
    'catalog_provision' so that an active catalog_provision task prevents
    the sweep from draining a still-running provisioning catalog.

    Before the fix, only 'gcp_provision_catalog' was matched, causing the
    sweep to mark catalogs 'failed' while their catalog_provision task was
    still ACTIVE.
    """
    captured_sql: list[str] = []

    class _CapturingSqlQuery:
        def __init__(self, sql: str, **kwargs: object) -> None:
            captured_sql.append(sql)
            self._rows: list = []

        async def execute(self, _conn: object, **_kw: object) -> list:
            return self._rows

    with (
        patch(
            "dynastore.modules.tasks.tasks_module.managed_transaction",
            _fake_managed_transaction,
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.DQLQuery",
            _CapturingSqlQuery,
        ),
        patch(
            "dynastore.modules.tasks.tasks_module.check_table_exists",
            new=AsyncMock(return_value=True),
        ),
    ):
        await sweep_wedged_provisioning_catalogs(
            engine=object(), min_age_s=600.0, sample_limit=50,
        )

    assert captured_sql, "DQLQuery must have been instantiated with a SQL string"
    sql = captured_sql[0]
    assert "catalog_provision" in sql, (
        "Sweep SQL must include 'catalog_provision' in the NOT EXISTS guard; "
        f"got:\n{sql}"
    )
    assert "gcp_provision_catalog" in sql, (
        "Sweep SQL must still include legacy 'gcp_provision_catalog'; "
        f"got:\n{sql}"
    )
