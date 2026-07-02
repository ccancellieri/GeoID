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

"""Unit tests for CatalogService.reset_checklist_for_reprovision (#2395, #2678).

The reprovision trigger resets the to-be-rerun checklist steps to 'pending'.
Since un-fao/GeoID#2678 it also has to (a) leave ``deferred`` steps alone
unless the caller explicitly opts back in, and (b) only flip the catalog to
'provisioning' when the resulting checklist actually has unsatisfied work —
otherwise a generic reprovision sweep touching an already-``ready`` catalog
would wedge it in 'provisioning' forever (nothing left pending for the
executor task to run, so nothing ever flips it back). All DB I/O is mocked.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dynastore.modules.catalog.provisioning_registry import (
    STEP_COMPLETE,
    STEP_DEFERRED,
    STEP_DEGRADED,
    STEP_FAILED,
    STEP_PENDING,
    STEP_SKIPPED,
    STATUS_PROVISIONING,
    STATUS_READY,
)


def _make_service():
    from dynastore.modules.catalog.catalog_service import CatalogService

    return CatalogService.__new__(CatalogService)


def _txn_ctx(conn):
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


class _CapturingQuery:
    """Stand-in for the inline ``DQLQuery`` the terminal UPDATE builds.

    Mirrors the pattern used by the ``mark_provisioning_step`` /
    ``drain_pending_checklist_steps`` first_ready_at tests.
    """

    captured_sql: list = []
    captured_kwargs: list = []

    def __init__(self, sql, result_handler=None, **_kw):
        type(self).captured_sql.append(sql)

    async def execute(self, *_a, **kwargs):
        type(self).captured_kwargs.append(kwargs)


async def _run_reset(checklist, *, force=False, include_deferred=False, ensure_keys=None):
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

    _CapturingQuery.captured_sql = []
    _CapturingQuery.captured_kwargs = []

    with (
        patch(
            "dynastore.modules.catalog.catalog_service._get_provisioning_checklist_query",
            get_q,
        ),
        patch(
            "dynastore.modules.catalog.catalog_service.DQLQuery",
            new=_CapturingQuery,
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
        result = await svc.reset_checklist_for_reprovision(
            "c_x",
            force=force,
            include_deferred=include_deferred,
            ensure_keys=ensure_keys,
        )

    written = _CapturingQuery.captured_kwargs[0] if _CapturingQuery.captured_kwargs else None
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
        # Genuine unsatisfied work remains -> status stays 'provisioning'.
        assert written["st"] == STATUS_PROVISIONING
        assert json.loads(written["cl"]) == result

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
        assert written["st"] == STATUS_PROVISIONING

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


class TestDeferredStepsSurviveReprovision:
    """un-fao/GeoID#2678 — a 'deferred' step is left alone by a generic
    reprovision run (this is the fix for the catalog-wide sweep resurrecting
    a bucket the operator intentionally held back)."""

    @pytest.mark.asyncio
    async def test_deferred_left_untouched_by_default(self):
        checklist = {"catalog_core": STEP_COMPLETE, "gcp_bucket": STEP_DEFERRED}
        result, _written = await _run_reset(checklist)
        assert result == {"catalog_core": STEP_COMPLETE, "gcp_bucket": STEP_DEFERRED}

    @pytest.mark.asyncio
    async def test_force_does_not_resurrect_deferred(self):
        # force=True replays already-satisfied steps but must NOT touch a
        # deferred one — force and deferred are orthogonal knobs.
        checklist = {"catalog_core": STEP_COMPLETE, "gcp_bucket": STEP_DEFERRED}
        result, _written = await _run_reset(checklist, force=True)
        assert result == {"catalog_core": STEP_PENDING, "gcp_bucket": STEP_DEFERRED}

    @pytest.mark.asyncio
    async def test_include_deferred_resets_deferred_to_pending(self):
        checklist = {"catalog_core": STEP_COMPLETE, "gcp_bucket": STEP_DEFERRED}
        result, written = await _run_reset(checklist, include_deferred=True)
        assert result == {"catalog_core": STEP_COMPLETE, "gcp_bucket": STEP_PENDING}
        assert written["st"] == STATUS_PROVISIONING

    @pytest.mark.asyncio
    async def test_ensure_keys_does_not_disturb_existing_deferred_entry(self):
        # A generic reprovision run's active_provisioners() includes the
        # deferrable provisioner (defer=False default), so it lands in
        # ensure_keys — but the key is already present as 'deferred', so the
        # fold-in must not clobber it back to 'pending'.
        checklist = {"catalog_core": STEP_COMPLETE, "gcp_bucket": STEP_DEFERRED}
        result, _written = await _run_reset(
            checklist, ensure_keys=["catalog_core", "gcp_bucket"]
        )
        assert result == {"catalog_core": STEP_COMPLETE, "gcp_bucket": STEP_DEFERRED}


class TestResetDoesNotWedgeAlreadySatisfiedCatalog:
    """un-fao/GeoID#2678 — forcing status='provisioning' unconditionally would
    wedge a catalog that has nothing left to do (a generic sweep touching an
    already-'ready' catalog): the executor would find zero unsatisfied steps
    to run and never call mark_provisioning_step to flip it back."""

    @pytest.mark.asyncio
    async def test_all_terminal_good_writes_ready_not_provisioning(self):
        checklist = {
            "catalog_core": STEP_COMPLETE,
            "gcp_bucket": STEP_DEFERRED,
            "gcp_eventing": STEP_SKIPPED,
        }
        result, written = await _run_reset(checklist)
        assert result == checklist
        assert written["st"] == STATUS_READY

    @pytest.mark.asyncio
    async def test_any_remaining_pending_still_writes_provisioning(self):
        checklist = {"catalog_core": STEP_COMPLETE, "gcp_bucket": STEP_FAILED}
        result, written = await _run_reset(checklist)
        assert result == {"catalog_core": STEP_COMPLETE, "gcp_bucket": STEP_PENDING}
        assert written["st"] == STATUS_PROVISIONING
