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

"""Unit tests for CatalogService.drain_pending_checklist_steps (#1902).

Covers the structural termination guarantee: when a provisioning task exits
without marking every checklist step, drain_pending_checklist_steps closes
the gap so no catalog stays wedged in 'provisioning' indefinitely.

All DB I/O is mocked — these are pure unit tests.
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
    STATUS_FAILED,
    STATUS_READY,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_service():
    """Construct a CatalogService stub without running __init__."""
    from dynastore.modules.catalog.catalog_service import CatalogService
    svc = CatalogService.__new__(CatalogService)
    return svc


def _make_checklist_row(checklist: dict) -> dict:
    """Simulate a DB row with provisioning_checklist as a JSON string."""
    return {"provisioning_checklist": json.dumps(checklist)}


# ---------------------------------------------------------------------------
# Core drain behaviour
# ---------------------------------------------------------------------------

class TestDrainPendingChecklistSteps:
    """drain_pending_checklist_steps marks pending steps terminal and re-evaluates."""

    @pytest.mark.asyncio
    async def test_pending_steps_become_degraded_and_catalog_reaches_ready(self):
        """Two pending steps → both marked 'degraded' → catalog becomes ready."""
        checklist = {"gcp_bucket": STEP_COMPLETE, "gcp_eventing": STEP_PENDING}
        svc = _make_service()

        async def _fake_provision_write(engine, fn):
            # Simulate executing the inner coroutine with a fake conn.
            conn = MagicMock()
            conn.execute = AsyncMock()
            # Patch execute calls recorded inside DQLQuery.execute.
            return await fn(conn)

        async def _fake_get_checklist(conn, **kwargs):
            return _make_checklist_row(checklist)

        execute_calls = []

        async def _fake_dql_execute(conn, **kwargs):
            execute_calls.append(kwargs)

        fake_get_query = MagicMock()
        fake_get_query.execute = AsyncMock(side_effect=_fake_get_checklist)
        fake_update_query = MagicMock()
        fake_update_query.execute = AsyncMock(side_effect=_fake_dql_execute)

        fake_model = MagicMock()
        fake_model.return_value = None

        with (
            patch(
                "dynastore.modules.catalog.catalog_service._provisioning_write_with_retry",
                new=_fake_provision_write,
            ),
            patch(
                "dynastore.modules.catalog.catalog_service._get_provisioning_checklist_query",
                fake_get_query,
            ),
            patch(
                "dynastore.modules.catalog.catalog_service.DQLQuery",
                return_value=fake_update_query,
            ),
            patch(
                "dynastore.modules.catalog.catalog_service.get_catalog_engine",
                return_value=MagicMock(),
            ),
            patch.object(svc, "get_catalog_model", new=AsyncMock(return_value=None)),
            patch(
                "dynastore.modules.catalog.catalog_service._invalidate_catalog_model_cache",
            ),
        ):
            result = await svc.drain_pending_checklist_steps(
                "cat-test", terminal_status=STEP_DEGRADED,
            )

        assert result is True
        # The UPDATE must have been called with status=STATUS_READY since
        # gcp_bucket=complete + gcp_eventing=degraded → evaluate_checklist=ready.
        update_call = execute_calls[0]
        assert update_call.get("st") == STATUS_READY
        written_checklist = json.loads(update_call.get("cl", "{}"))
        assert written_checklist["gcp_eventing"] == STEP_DEGRADED
        assert written_checklist["gcp_bucket"] == STEP_COMPLETE

    @pytest.mark.asyncio
    async def test_already_terminal_steps_are_not_touched(self):
        """When all steps are already terminal, drain returns False (no work)."""
        checklist = {"gcp_bucket": STEP_COMPLETE, "gcp_eventing": STEP_DEGRADED}
        svc = _make_service()

        async def _fake_provision_write(engine, fn):
            conn = MagicMock()
            return await fn(conn)

        async def _fake_get_checklist(conn, **kwargs):
            return _make_checklist_row(checklist)

        fake_get_query = MagicMock()
        fake_get_query.execute = AsyncMock(side_effect=_fake_get_checklist)

        with (
            patch(
                "dynastore.modules.catalog.catalog_service._provisioning_write_with_retry",
                new=_fake_provision_write,
            ),
            patch(
                "dynastore.modules.catalog.catalog_service._get_provisioning_checklist_query",
                fake_get_query,
            ),
            patch(
                "dynastore.modules.catalog.catalog_service.get_catalog_engine",
                return_value=MagicMock(),
            ),
        ):
            result = await svc.drain_pending_checklist_steps("cat-test")

        assert result is False

    @pytest.mark.asyncio
    async def test_catalog_not_found_returns_false(self):
        """When the catalog row is missing, drain returns False without error."""
        svc = _make_service()

        async def _fake_provision_write(engine, fn):
            conn = MagicMock()
            return await fn(conn)

        async def _fake_get_empty(conn, **kwargs):
            return None  # row not found

        fake_get_query = MagicMock()
        fake_get_query.execute = AsyncMock(side_effect=_fake_get_empty)

        with (
            patch(
                "dynastore.modules.catalog.catalog_service._provisioning_write_with_retry",
                new=_fake_provision_write,
            ),
            patch(
                "dynastore.modules.catalog.catalog_service._get_provisioning_checklist_query",
                fake_get_query,
            ),
            patch(
                "dynastore.modules.catalog.catalog_service.get_catalog_engine",
                return_value=MagicMock(),
            ),
        ):
            result = await svc.drain_pending_checklist_steps("no-such-cat")

        assert result is False

    @pytest.mark.asyncio
    async def test_no_checklist_returns_false(self):
        """A catalog without a checklist (on-prem / already-ready legacy) is a no-op."""
        svc = _make_service()

        async def _fake_provision_write(engine, fn):
            conn = MagicMock()
            return await fn(conn)

        async def _fake_get_null_checklist(conn, **kwargs):
            return {"provisioning_checklist": None}

        fake_get_query = MagicMock()
        fake_get_query.execute = AsyncMock(side_effect=_fake_get_null_checklist)

        with (
            patch(
                "dynastore.modules.catalog.catalog_service._provisioning_write_with_retry",
                new=_fake_provision_write,
            ),
            patch(
                "dynastore.modules.catalog.catalog_service._get_provisioning_checklist_query",
                fake_get_query,
            ),
            patch(
                "dynastore.modules.catalog.catalog_service.get_catalog_engine",
                return_value=MagicMock(),
            ),
        ):
            result = await svc.drain_pending_checklist_steps("legacy-cat")

        assert result is False

    @pytest.mark.asyncio
    async def test_failed_terminal_status_marks_catalog_failed(self):
        """When terminal_status='failed' and a step was pending, catalog → failed."""
        checklist = {"gcp_bucket": STEP_PENDING, "gcp_eventing": STEP_PENDING}
        svc = _make_service()

        execute_calls = []

        async def _fake_provision_write(engine, fn):
            conn = MagicMock()
            return await fn(conn)

        async def _fake_get_checklist(conn, **kwargs):
            return _make_checklist_row(checklist)

        async def _fake_dql_execute(conn, **kwargs):
            execute_calls.append(kwargs)

        fake_get_query = MagicMock()
        fake_get_query.execute = AsyncMock(side_effect=_fake_get_checklist)
        fake_update_query = MagicMock()
        fake_update_query.execute = AsyncMock(side_effect=_fake_dql_execute)

        with (
            patch(
                "dynastore.modules.catalog.catalog_service._provisioning_write_with_retry",
                new=_fake_provision_write,
            ),
            patch(
                "dynastore.modules.catalog.catalog_service._get_provisioning_checklist_query",
                fake_get_query,
            ),
            patch(
                "dynastore.modules.catalog.catalog_service.DQLQuery",
                return_value=fake_update_query,
            ),
            patch(
                "dynastore.modules.catalog.catalog_service.get_catalog_engine",
                return_value=MagicMock(),
            ),
            patch.object(svc, "get_catalog_model", new=AsyncMock(return_value=None)),
            patch(
                "dynastore.modules.catalog.catalog_service._invalidate_catalog_model_cache",
            ),
        ):
            result = await svc.drain_pending_checklist_steps(
                "cat-hard-fail", terminal_status=STEP_FAILED,
            )

        assert result is True
        update_call = execute_calls[0]
        assert update_call.get("st") == STATUS_FAILED
        written_checklist = json.loads(update_call.get("cl", "{}"))
        assert written_checklist["gcp_bucket"] == STEP_FAILED
        assert written_checklist["gcp_eventing"] == STEP_FAILED


# ---------------------------------------------------------------------------
# Acceptance criterion: provisioner raises mid-checklist → catalog not wedged
# ---------------------------------------------------------------------------

class TestProvisionerRaisesBeforeMarkingStep:
    """A provisioner that raises without marking its checklist step must not
    leave the catalog wedged in 'provisioning' indefinitely (#1902).

    This is the structural enforcement test: after the task exits (via
    drain_pending_checklist_steps) the catalog reaches a terminal status.
    """

    @pytest.mark.asyncio
    async def test_unhandled_raise_leaves_no_pending_step(self):
        """A provisioner that raises before mark_provisioning_step is called
        → drain_pending_checklist_steps marks it terminal → catalog not stuck.

        The test simulates:
          1. Checklist built: {"gcp_bucket": "pending", "gcp_eventing": "pending"}
          2. Provisioner raises unexpectedly (does NOT mark any step).
          3. drain_pending_checklist_steps is called (the structural backstop).
          4. Assert: both steps are terminal and the catalog has a non-provisioning status.
        """
        initial_checklist = {"gcp_bucket": STEP_PENDING, "gcp_eventing": STEP_PENDING}
        svc = _make_service()

        # Simulate the DB state after the provisioner raised (steps still pending).
        execute_calls = []

        async def _fake_provision_write(engine, fn):
            conn = MagicMock()
            return await fn(conn)

        async def _fake_get_checklist(conn, **kwargs):
            return _make_checklist_row(initial_checklist)

        async def _fake_dql_execute(conn, **kwargs):
            execute_calls.append(kwargs)

        fake_get_query = MagicMock()
        fake_get_query.execute = AsyncMock(side_effect=_fake_get_checklist)
        fake_update_query = MagicMock()
        fake_update_query.execute = AsyncMock(side_effect=_fake_dql_execute)

        with (
            patch(
                "dynastore.modules.catalog.catalog_service._provisioning_write_with_retry",
                new=_fake_provision_write,
            ),
            patch(
                "dynastore.modules.catalog.catalog_service._get_provisioning_checklist_query",
                fake_get_query,
            ),
            patch(
                "dynastore.modules.catalog.catalog_service.DQLQuery",
                return_value=fake_update_query,
            ),
            patch(
                "dynastore.modules.catalog.catalog_service.get_catalog_engine",
                return_value=MagicMock(),
            ),
            patch.object(svc, "get_catalog_model", new=AsyncMock(return_value=None)),
            patch(
                "dynastore.modules.catalog.catalog_service._invalidate_catalog_model_cache",
            ),
        ):
            # This is the call the reconciler / task runner makes after a crash.
            result = await svc.drain_pending_checklist_steps(
                "cat-crashed", terminal_status=STEP_DEGRADED,
            )

        # Drain must have found and fixed the pending steps.
        assert result is True, "drain_pending_checklist_steps must return True when steps were fixed"

        # The catalog must have received a terminal (non-provisioning) status update.
        assert execute_calls, "An UPDATE must have been issued to the DB"
        update_kwargs = execute_calls[0]
        written_status = update_kwargs.get("st")
        assert written_status is not None, "A catalog status transition must have been written"
        assert written_status != "provisioning", (
            f"Catalog must not stay in 'provisioning' after drain; got {written_status!r}"
        )

        # Both steps must be terminal in the written checklist.
        written_checklist = json.loads(update_kwargs.get("cl", "{}"))
        for key, val in written_checklist.items():
            assert val != STEP_PENDING, (
                f"Step '{key}' must not be 'pending' after drain; got {val!r}"
            )
