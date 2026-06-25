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

"""Unit tests for CatalogService.reset_checklist_for_reprovision (#2395).

The reprovision trigger resets the to-be-rerun checklist steps to 'pending' and
flips the catalog to 'provisioning' so its status transitions monotonically
back to 'ready'. All DB I/O is mocked.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.provisioning_registry import (
    STEP_COMPLETE,
    STEP_DEGRADED,
    STEP_FAILED,
    STEP_PENDING,
    STEP_SKIPPED,
    STATUS_PROVISIONING,
)


def _make_service():
    from dynastore.modules.catalog.catalog_service import CatalogService

    return CatalogService.__new__(CatalogService)


def _txn_ctx(conn):
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


async def _run_reset(checklist, *, force=False):
    """Invoke reset_checklist_for_reprovision; return (result, written_kwargs)."""
    svc = _make_service()
    conn = MagicMock()

    get_q = MagicMock()
    get_q.execute = AsyncMock(
        return_value=(
            {"provisioning_checklist": json.dumps(checklist)}
            if checklist is not None
            else None
        )
    )
    set_q = MagicMock()
    set_q.execute = AsyncMock()

    with (
        patch(
            "dynastore.modules.catalog.catalog_service._get_provisioning_checklist_query",
            get_q,
        ),
        patch(
            "dynastore.modules.catalog.catalog_service._set_provisioning_checklist_query",
            set_q,
        ),
        patch(
            "dynastore.modules.catalog.catalog_service.managed_transaction",
            return_value=_txn_ctx(conn),
        ),
        patch(
            "dynastore.modules.catalog.catalog_service.get_catalog_engine",
            return_value=MagicMock(),
        ),
    ):
        result = await svc.reset_checklist_for_reprovision("c_x", force=force)

    written = set_q.execute.await_args.kwargs if set_q.execute.await_args else None
    return result, written


class TestResetChecklistForReprovision:
    @pytest.mark.asyncio
    async def test_failed_and_degraded_reset_to_pending_complete_kept(self):
        checklist = {
            "catalog_core": STEP_COMPLETE,
            "gcp_bucket": STEP_FAILED,
            "gcp_eventing": STEP_DEGRADED,
        }
        result, written = await _run_reset(checklist)

        assert result == {
            "catalog_core": STEP_COMPLETE,
            "gcp_bucket": STEP_PENDING,
            "gcp_eventing": STEP_PENDING,
        }
        assert written["status"] == STATUS_PROVISIONING
        assert json.loads(written["checklist"]) == result

    @pytest.mark.asyncio
    async def test_skipped_steps_are_kept(self):
        checklist = {"gcp_bucket": STEP_SKIPPED, "gcp_eventing": STEP_FAILED}
        result, _written = await _run_reset(checklist)
        assert result == {"gcp_bucket": STEP_SKIPPED, "gcp_eventing": STEP_PENDING}

    @pytest.mark.asyncio
    async def test_force_resets_every_step(self):
        checklist = {"catalog_core": STEP_COMPLETE, "gcp_bucket": STEP_SKIPPED}
        result, written = await _run_reset(checklist, force=True)
        assert result == {"catalog_core": STEP_PENDING, "gcp_bucket": STEP_PENDING}
        assert written["status"] == STATUS_PROVISIONING

    @pytest.mark.asyncio
    async def test_missing_row_returns_empty_and_writes_nothing(self):
        result, written = await _run_reset(None)
        assert result == {}
        assert written is None

    @pytest.mark.asyncio
    async def test_empty_checklist_returns_empty_and_writes_nothing(self):
        result, written = await _run_reset({})
        assert result == {}
        assert written is None
