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

"""Unit tests for CatalogProvisionTask (#2329).

Covers:
  - task_type registration.
  - Priority-group ordering: higher-priority groups execute before lower.
  - Within-group concurrency: hooks run concurrently but bounded by semaphore.
  - Step marking: each successful hook → mark_provisioning_step("complete").
  - None hook: skipped with no step mark.
  - deprovision_soft/hard selects .deprovision hook.
  - Failing hook marks step "failed" and aborts the run (re-raises).
  - catalog_provision is in _PROVISIONING_TASK_TYPES.
  - Routing matrix: under the cloud preset catalog_provision routes to gcp_cloud_run + OFFLOAD
    (offload_required returns True); under onprem it stays background (no Cloud Run Job).
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_task() -> Any:
    from dynastore.tasks.catalog_provision.task import CatalogProvisionTask

    return CatalogProvisionTask()


def _make_payload(
    catalog_id: str = "c_test",
    scope: str = "catalog",
    operation: str = "provision",
    collection_id: str | None = None,
    force: bool = False,
) -> Any:
    from dynastore.tasks.catalog_provision.task import CatalogProvisionInputs
    from dynastore.models.tasks import TaskPayload

    inputs = CatalogProvisionInputs(
        catalog_id=catalog_id,
        scope=scope,
        operation=operation,
        collection_id=collection_id,
        force=force,
    )
    return TaskPayload(task_id=uuid.uuid4(), caller_id="test", inputs=inputs)


def _make_provisioner(
    key: str,
    priority: int = 100,
    provision_fn: Any = None,
    deprovision_fn: Any = None,
) -> Any:
    """Build a minimal Provisioner-like object for testing."""
    p = MagicMock()
    p.key = key
    p.priority = priority
    p.provision = provision_fn
    p.deprovision = deprovision_fn
    return p


def _mock_catalogs(checklist: Any = None) -> Any:
    mock = AsyncMock()
    mock.get_catalog_model = AsyncMock(return_value=MagicMock(external_id="ext-id"))
    mock.mark_provisioning_step = AsyncMock()
    # Reprovision skip-filter (#2395) reads the checklist; an empty checklist
    # models the fresh-create case (nothing satisfied yet → nothing skipped).
    mock.get_provisioning_checklist = AsyncMock(return_value=checklist or {})
    return mock


def _txn_ctx(conn: Any = None) -> Any:
    conn = conn or AsyncMock()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestCatalogProvisionTaskRegistration:
    def test_task_type(self):
        from dynastore.tasks.catalog_provision.task import CatalogProvisionTask

        assert CatalogProvisionTask.task_type == "catalog_provision"

    def test_importable_from_package(self):
        from dynastore.tasks.catalog_provision import CatalogProvisionTask  # noqa: F401

        assert CatalogProvisionTask is not None

    def test_in_provisioning_task_types(self):
        from dynastore.modules.tasks.execution import _PROVISIONING_TASK_TYPES

        assert "catalog_provision" in _PROVISIONING_TASK_TYPES


# ---------------------------------------------------------------------------
# Priority ordering
# ---------------------------------------------------------------------------


class TestPriorityGroupOrdering:
    @pytest.mark.asyncio
    async def test_lower_priority_group_executes_before_higher(self):
        """Groups must execute in ascending priority order."""
        call_order: List[str] = []

        async def hook_a(**ctx):
            call_order.append("a")

        async def hook_b(**ctx):
            call_order.append("b")

        p_a = _make_provisioner("a", priority=10, provision_fn=hook_a)
        p_b = _make_provisioner("b", priority=20, provision_fn=hook_b)

        # active_provisioners returns groups sorted ascending by priority
        groups = [[p_a], [p_b]]

        task = _make_task()
        payload = _make_payload()
        mock_catalogs = _mock_catalogs()

        with patch(
            "dynastore.tasks.catalog_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ), patch(
            "dynastore.tasks.catalog_provision.task.managed_transaction",
            return_value=_txn_ctx(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.get_catalog_engine",
            return_value=MagicMock(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.provisioning_registry",
        ) as mock_reg, patch(
            "dynastore.tasks.catalog_provision.task._get_group_concurrency",
            new=AsyncMock(return_value=4),
        ):
            mock_reg.active_provisioners = AsyncMock(return_value=groups)
            result = await task.run(payload)

        assert call_order == ["a", "b"], (
            f"Expected ['a', 'b'] but got {call_order} — "
            "priority-10 group must run before priority-20"
        )
        assert result["groups_run"] == 2
        assert result["steps_completed"] == 2

    @pytest.mark.asyncio
    async def test_ordering_with_three_priorities(self):
        """Three distinct priority groups must execute in ascending order."""
        call_order: List[str] = []

        def make_hook(name: str):
            async def hook(**ctx):
                call_order.append(name)
            return hook

        provisioners = [
            _make_provisioner("x", priority=5, provision_fn=make_hook("x")),
            _make_provisioner("y", priority=50, provision_fn=make_hook("y")),
            _make_provisioner("z", priority=100, provision_fn=make_hook("z")),
        ]
        groups = [[provisioners[0]], [provisioners[1]], [provisioners[2]]]

        task = _make_task()
        payload = _make_payload()
        mock_catalogs = _mock_catalogs()

        with patch(
            "dynastore.tasks.catalog_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ), patch(
            "dynastore.tasks.catalog_provision.task.managed_transaction",
            return_value=_txn_ctx(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.get_catalog_engine",
            return_value=MagicMock(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.provisioning_registry",
        ) as mock_reg, patch(
            "dynastore.tasks.catalog_provision.task._get_group_concurrency",
            new=AsyncMock(return_value=4),
        ):
            mock_reg.active_provisioners = AsyncMock(return_value=groups)
            result = await task.run(payload)

        assert call_order == ["x", "y", "z"]
        assert result["steps_completed"] == 3


# ---------------------------------------------------------------------------
# Within-group concurrency
# ---------------------------------------------------------------------------


class TestGroupConcurrency:
    @pytest.mark.asyncio
    async def test_hooks_in_same_group_run_concurrently(self):
        """Two hooks in the same priority group must start before either finishes."""
        started: List[str] = []
        finished: List[str] = []
        gate = asyncio.Event()

        async def hook_a(**ctx):
            started.append("a")
            await gate.wait()
            finished.append("a")

        async def hook_b(**ctx):
            started.append("b")
            await gate.wait()
            finished.append("b")

        p_a = _make_provisioner("a", provision_fn=hook_a)
        p_b = _make_provisioner("b", provision_fn=hook_b)
        groups = [[p_a, p_b]]

        task = _make_task()
        payload = _make_payload()
        mock_catalogs = _mock_catalogs()

        async def run_with_gate():
            run_coro = task.run(payload)
            t = asyncio.create_task(run_coro)
            # Give both hooks time to reach their await gate.wait()
            for _ in range(20):
                await asyncio.sleep(0)
            assert "a" in started and "b" in started, (
                "Both hooks must have started before either finishes "
                "(within-group concurrency)"
            )
            gate.set()
            return await t

        with patch(
            "dynastore.tasks.catalog_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ), patch(
            "dynastore.tasks.catalog_provision.task.managed_transaction",
            return_value=_txn_ctx(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.get_catalog_engine",
            return_value=MagicMock(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.provisioning_registry",
        ) as mock_reg, patch(
            "dynastore.tasks.catalog_provision.task._get_group_concurrency",
            new=AsyncMock(return_value=4),
        ):
            mock_reg.active_provisioners = AsyncMock(return_value=groups)
            result = await run_with_gate()

        assert result["steps_completed"] == 2

    @pytest.mark.asyncio
    async def test_semaphore_bounds_concurrency(self):
        """With concurrency=1 two hooks in the same group must run serially."""
        max_concurrent = 0
        current = 0

        async def hook(**ctx):
            nonlocal max_concurrent, current
            current += 1
            max_concurrent = max(max_concurrent, current)
            await asyncio.sleep(0)
            current -= 1

        p_a = _make_provisioner("a", provision_fn=hook)
        p_b = _make_provisioner("b", provision_fn=hook)
        groups = [[p_a, p_b]]

        task = _make_task()
        payload = _make_payload()
        mock_catalogs = _mock_catalogs()

        with patch(
            "dynastore.tasks.catalog_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ), patch(
            "dynastore.tasks.catalog_provision.task.managed_transaction",
            return_value=_txn_ctx(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.get_catalog_engine",
            return_value=MagicMock(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.provisioning_registry",
        ) as mock_reg, patch(
            "dynastore.tasks.catalog_provision.task._get_group_concurrency",
            new=AsyncMock(return_value=1),
        ):
            mock_reg.active_provisioners = AsyncMock(return_value=groups)
            await task.run(payload)

        assert max_concurrent == 1, (
            f"Semaphore bound=1 must prevent >1 concurrent hook; saw {max_concurrent}"
        )


# ---------------------------------------------------------------------------
# Step marking
# ---------------------------------------------------------------------------


class TestStepMarking:
    @pytest.mark.asyncio
    async def test_successful_hook_marks_complete(self):
        """Each provisioner's step is marked 'complete' after its hook succeeds."""
        async def hook(**ctx):
            pass

        p = _make_provisioner("step_x", provision_fn=hook)
        groups = [[p]]

        task = _make_task()
        payload = _make_payload(catalog_id="c_abc")
        mock_catalogs = _mock_catalogs()
        mock_catalogs.get_catalog_model = AsyncMock(
            return_value=MagicMock(external_id="ext")
        )

        with patch(
            "dynastore.tasks.catalog_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ), patch(
            "dynastore.tasks.catalog_provision.task.managed_transaction",
            return_value=_txn_ctx(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.get_catalog_engine",
            return_value=MagicMock(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.provisioning_registry",
        ) as mock_reg, patch(
            "dynastore.tasks.catalog_provision.task._get_group_concurrency",
            new=AsyncMock(return_value=4),
        ):
            mock_reg.active_provisioners = AsyncMock(return_value=groups)
            await task.run(payload)

        mock_catalogs.mark_provisioning_step.assert_awaited_once_with(
            "c_abc", "step_x", "complete"
        )

    @pytest.mark.asyncio
    async def test_none_hook_is_skipped_no_step_mark(self):
        """A provisioner with provision=None is skipped; no step is marked."""
        p = _make_provisioner("skip_me", provision_fn=None)
        groups = [[p]]

        task = _make_task()
        payload = _make_payload()
        mock_catalogs = _mock_catalogs()

        with patch(
            "dynastore.tasks.catalog_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ), patch(
            "dynastore.tasks.catalog_provision.task.managed_transaction",
            return_value=_txn_ctx(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.get_catalog_engine",
            return_value=MagicMock(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.provisioning_registry",
        ) as mock_reg, patch(
            "dynastore.tasks.catalog_provision.task._get_group_concurrency",
            new=AsyncMock(return_value=4),
        ):
            mock_reg.active_provisioners = AsyncMock(return_value=groups)
            result = await task.run(payload)

        mock_catalogs.mark_provisioning_step.assert_not_awaited()
        assert result["steps_completed"] == 0

    @pytest.mark.asyncio
    async def test_multiple_steps_all_marked_complete(self):
        """All provisioners' steps are marked complete when all hooks succeed."""
        async def hook(**ctx):
            pass

        p1 = _make_provisioner("s1", provision_fn=hook)
        p2 = _make_provisioner("s2", provision_fn=hook)
        p3 = _make_provisioner("s3", provision_fn=hook)
        groups = [[p1, p2], [p3]]

        task = _make_task()
        payload = _make_payload(catalog_id="c_multi")
        mock_catalogs = _mock_catalogs()

        with patch(
            "dynastore.tasks.catalog_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ), patch(
            "dynastore.tasks.catalog_provision.task.managed_transaction",
            return_value=_txn_ctx(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.get_catalog_engine",
            return_value=MagicMock(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.provisioning_registry",
        ) as mock_reg, patch(
            "dynastore.tasks.catalog_provision.task._get_group_concurrency",
            new=AsyncMock(return_value=4),
        ):
            mock_reg.active_provisioners = AsyncMock(return_value=groups)
            result = await task.run(payload)

        assert result["steps_completed"] == 3
        assert mock_catalogs.mark_provisioning_step.await_count == 3
        calls = {
            (call.args[0], call.args[1], call.args[2])
            for call in mock_catalogs.mark_provisioning_step.await_args_list
        }
        assert calls == {
            ("c_multi", "s1", "complete"),
            ("c_multi", "s2", "complete"),
            ("c_multi", "s3", "complete"),
        }


# ---------------------------------------------------------------------------
# Operation selection
# ---------------------------------------------------------------------------


class TestOperationSelection:
    @pytest.mark.asyncio
    async def test_provision_selects_provision_hook(self):
        provision_called = []
        deprovision_called = []

        async def prov(**ctx):
            provision_called.append(True)

        async def deprov(**ctx):
            deprovision_called.append(True)

        p = _make_provisioner("op_test", provision_fn=prov, deprovision_fn=deprov)
        groups = [[p]]

        task = _make_task()
        payload = _make_payload(operation="provision")
        mock_catalogs = _mock_catalogs()

        with patch(
            "dynastore.tasks.catalog_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ), patch(
            "dynastore.tasks.catalog_provision.task.managed_transaction",
            return_value=_txn_ctx(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.get_catalog_engine",
            return_value=MagicMock(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.provisioning_registry",
        ) as mock_reg, patch(
            "dynastore.tasks.catalog_provision.task._get_group_concurrency",
            new=AsyncMock(return_value=4),
        ):
            mock_reg.active_provisioners = AsyncMock(return_value=groups)
            await task.run(payload)

        assert provision_called == [True]
        assert deprovision_called == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("operation", ["deprovision_soft", "deprovision_hard"])
    async def test_deprovision_selects_deprovision_hook(self, operation: str):
        provision_called = []
        deprovision_called = []

        async def prov(**ctx):
            provision_called.append(True)

        async def deprov(**ctx):
            deprovision_called.append(True)

        p = _make_provisioner("op_test", provision_fn=prov, deprovision_fn=deprov)
        groups = [[p]]

        task = _make_task()
        payload = _make_payload(operation=operation)
        mock_catalogs = _mock_catalogs()

        with patch(
            "dynastore.tasks.catalog_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ), patch(
            "dynastore.tasks.catalog_provision.task.managed_transaction",
            return_value=_txn_ctx(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.get_catalog_engine",
            return_value=MagicMock(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.provisioning_registry",
        ) as mock_reg, patch(
            "dynastore.tasks.catalog_provision.task._get_group_concurrency",
            new=AsyncMock(return_value=4),
        ):
            mock_reg.active_provisioners = AsyncMock(return_value=groups)
            await task.run(payload)

        assert deprovision_called == [True]
        assert provision_called == []


# ---------------------------------------------------------------------------
# Failure handling
# ---------------------------------------------------------------------------


class TestFailureHandling:
    @pytest.mark.asyncio
    async def test_failing_hook_marks_step_failed_and_aborts(self):
        """A hook that raises must mark its step 'failed' and re-raise."""
        async def bad_hook(**ctx):
            raise ValueError("provisioner error")

        p = _make_provisioner("bad_step", provision_fn=bad_hook)
        groups = [[p]]

        task = _make_task()
        payload = _make_payload(catalog_id="c_fail")
        mock_catalogs = _mock_catalogs()

        with patch(
            "dynastore.tasks.catalog_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ), patch(
            "dynastore.tasks.catalog_provision.task.managed_transaction",
            return_value=_txn_ctx(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.get_catalog_engine",
            return_value=MagicMock(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.provisioning_registry",
        ) as mock_reg, patch(
            "dynastore.tasks.catalog_provision.task._get_group_concurrency",
            new=AsyncMock(return_value=4),
        ):
            mock_reg.active_provisioners = AsyncMock(return_value=groups)
            with pytest.raises(ValueError, match="provisioner error"):
                await task.run(payload)

        mock_catalogs.mark_provisioning_step.assert_awaited_once_with(
            "c_fail", "bad_step", "failed"
        )

    @pytest.mark.asyncio
    async def test_failing_hook_in_first_group_aborts_second_group(self):
        """A failure in group 1 must abort before group 2 runs."""
        group2_called = []

        async def bad_hook(**ctx):
            raise RuntimeError("group1 failed")

        async def hook2(**ctx):
            group2_called.append(True)

        p1 = _make_provisioner("step1", priority=10, provision_fn=bad_hook)
        p2 = _make_provisioner("step2", priority=20, provision_fn=hook2)
        groups = [[p1], [p2]]

        task = _make_task()
        payload = _make_payload()
        mock_catalogs = _mock_catalogs()

        with patch(
            "dynastore.tasks.catalog_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ), patch(
            "dynastore.tasks.catalog_provision.task.managed_transaction",
            return_value=_txn_ctx(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.get_catalog_engine",
            return_value=MagicMock(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.provisioning_registry",
        ) as mock_reg, patch(
            "dynastore.tasks.catalog_provision.task._get_group_concurrency",
            new=AsyncMock(return_value=4),
        ):
            mock_reg.active_provisioners = AsyncMock(return_value=groups)
            with pytest.raises(RuntimeError, match="group1 failed"):
                await task.run(payload)

        assert group2_called == [], "Group 2 must not run after group 1 fails"

    @pytest.mark.asyncio
    async def test_sync_hook_is_called(self):
        """Sync (non-coroutine) hooks must also be invoked."""
        called = []

        def sync_hook(**ctx):
            called.append("sync")

        p = _make_provisioner("sync_step", provision_fn=sync_hook)
        groups = [[p]]

        task = _make_task()
        payload = _make_payload()
        mock_catalogs = _mock_catalogs()

        with patch(
            "dynastore.tasks.catalog_provision.task._get_catalog_protocol",
            return_value=mock_catalogs,
        ), patch(
            "dynastore.tasks.catalog_provision.task.managed_transaction",
            return_value=_txn_ctx(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.get_catalog_engine",
            return_value=MagicMock(),
        ), patch(
            "dynastore.tasks.catalog_provision.task.provisioning_registry",
        ) as mock_reg, patch(
            "dynastore.tasks.catalog_provision.task._get_group_concurrency",
            new=AsyncMock(return_value=4),
        ):
            mock_reg.active_provisioners = AsyncMock(return_value=groups)
            result = await task.run(payload)

        assert called == ["sync"]
        assert result["steps_completed"] == 1


# ---------------------------------------------------------------------------
# Routing matrix
# ---------------------------------------------------------------------------


class TestRoutingMatrix:
    def _task_item(self, key: str) -> Any:
        from dynastore.modules.tasks.routing.matrix import InventoryItem

        return InventoryItem(task_key=key, kind="task", affinity_tier="catalog")

    def test_cloud_offloadable_task_routes_to_gcp_cloud_run(self):
        """Under the cloud preset, catalog_provision must route to gcp_cloud_run
        with OFFLOAD hint so offload_required() returns True and the dispatcher
        routes to the Cloud Run Job instead of running in-process."""
        from dynastore.modules.tasks.routing.matrix import build_routing_matrix
        from dynastore.modules.tasks.routing.exec_hints import ExecHint

        tasks, _ = build_routing_matrix(
            [self._task_item("catalog_provision")], preset="cloud"
        )
        targets = tasks["catalog_provision"]
        assert len(targets) == 1
        assert targets[0].runner == "gcp_cloud_run"
        assert ExecHint.OFFLOAD in targets[0].hints
        assert ExecHint.BACKGROUND not in targets[0].hints

    def test_onprem_offloadable_task_stays_background(self):
        """Under the onprem preset, catalog_provision stays in-process
        (no Cloud Run Job available in that topology)."""
        from dynastore.modules.tasks.routing.matrix import build_routing_matrix
        from dynastore.modules.tasks.routing.exec_hints import ExecHint

        tasks, _ = build_routing_matrix(
            [self._task_item("catalog_provision")], preset="onprem"
        )
        targets = tasks["catalog_provision"]
        assert len(targets) == 1
        assert targets[0].runner == "background"
        assert ExecHint.BACKGROUND in targets[0].hints
        assert ExecHint.OFFLOAD not in targets[0].hints

    def test_non_offloadable_task_always_single_background(self):
        """A regular system task (not in OFFLOADABLE_SYSTEM_TASKS) stays single background."""
        from dynastore.modules.tasks.routing.matrix import build_routing_matrix

        for preset in ("cloud", "onprem"):
            tasks, _ = build_routing_matrix(
                [self._task_item("gcp_provision_catalog")], preset=preset
            )
            targets = tasks["gcp_provision_catalog"]
            assert len(targets) == 1, (
                f"gcp_provision_catalog must have 1 target under {preset}; "
                f"got {len(targets)}"
            )
            assert targets[0].runner == "background"

    def test_offloadable_system_tasks_contains_catalog_provision(self):
        from dynastore.modules.tasks.routing.matrix import OFFLOADABLE_SYSTEM_TASKS

        assert "catalog_provision" in OFFLOADABLE_SYSTEM_TASKS



# ---------------------------------------------------------------------------
# Reprovision: checklist-aware skip + force (#2395)
# ---------------------------------------------------------------------------


async def _run_with(groups, *, checklist=None, force=False, operation="provision"):
    """Invoke run() with the given provisioner groups and checklist.

    Returns (result, mock_catalogs, emit_mock).
    """
    from unittest.mock import patch as _patch

    task = _make_task()
    payload = _make_payload(force=force, operation=operation)
    mock_catalogs = _mock_catalogs(checklist=checklist)

    with _patch(
        "dynastore.tasks.catalog_provision.task._get_catalog_protocol",
        return_value=mock_catalogs,
    ), _patch(
        "dynastore.tasks.catalog_provision.task.managed_transaction",
        return_value=_txn_ctx(),
    ), _patch(
        "dynastore.tasks.catalog_provision.task.get_catalog_engine",
        return_value=MagicMock(),
    ), _patch(
        "dynastore.tasks.catalog_provision.task.provisioning_registry",
    ) as mock_reg, _patch(
        "dynastore.tasks.catalog_provision.task._get_group_concurrency",
        new=AsyncMock(return_value=4),
    ), _patch.object(
        task, "_emit_catalog_created_events", new=AsyncMock()
    ) as emit_mock:
        mock_reg.active_provisioners = AsyncMock(return_value=groups)
        result = await task.run(payload)
    return result, mock_catalogs, emit_mock


class TestReprovisionChecklistAware:
    @pytest.mark.asyncio
    async def test_skips_satisfied_steps_runs_unsatisfied(self):
        """complete/skipped steps are skipped; failed/degraded/pending run."""
        ran: List[str] = []

        def make_hook(name: str):
            async def hook(**ctx):
                ran.append(name)
            return hook

        done = _make_provisioner("catalog_core", priority=0, provision_fn=make_hook("catalog_core"))
        bucket = _make_provisioner("gcp_bucket", priority=1, provision_fn=make_hook("gcp_bucket"))
        eventing = _make_provisioner("gcp_eventing", priority=1, provision_fn=make_hook("gcp_eventing"))
        groups = [[done], [bucket, eventing]]
        checklist = {
            "catalog_core": "complete",
            "gcp_bucket": "skipped",
            "gcp_eventing": "failed",
        }

        result, _cat, _emit = await _run_with(groups, checklist=checklist)

        assert ran == ["gcp_eventing"], f"only the failed step should run, got {ran}"
        assert result["groups_run"] == 1
        assert result["steps_completed"] == 1

    @pytest.mark.asyncio
    async def test_degraded_step_is_rerun(self):
        """A degraded step (best-effort that did not complete) is re-run."""
        ran: List[str] = []

        async def hook(**ctx):
            ran.append("gcp_eventing")

        eventing = _make_provisioner("gcp_eventing", priority=1, provision_fn=hook)
        groups = [[eventing]]
        checklist = {"gcp_eventing": "degraded"}

        await _run_with(groups, checklist=checklist)
        assert ran == ["gcp_eventing"]

    @pytest.mark.asyncio
    async def test_force_runs_all_steps_regardless_of_checklist(self):
        """force=True ignores the checklist and replays every provisioner."""
        ran: List[str] = []

        def make_hook(name: str):
            async def hook(**ctx):
                ran.append(name)
            return hook

        core = _make_provisioner("catalog_core", priority=0, provision_fn=make_hook("catalog_core"))
        bucket = _make_provisioner("gcp_bucket", priority=1, provision_fn=make_hook("gcp_bucket"))
        groups = [[core], [bucket]]
        checklist = {"catalog_core": "complete", "gcp_bucket": "complete"}

        result, _cat, _emit = await _run_with(groups, checklist=checklist, force=True)

        assert sorted(ran) == ["catalog_core", "gcp_bucket"]
        assert result["steps_completed"] == 2

    @pytest.mark.asyncio
    async def test_noop_reprovision_does_not_re_emit_creation_events(self):
        """When every step is already satisfied, nothing runs and no
        CATALOG_CREATION events are re-emitted."""
        ran: List[str] = []

        async def hook(**ctx):
            ran.append("x")

        core = _make_provisioner("catalog_core", priority=0, provision_fn=hook)
        groups = [[core]]
        checklist = {"catalog_core": "complete"}

        result, _cat, emit_mock = await _run_with(groups, checklist=checklist)

        assert ran == []
        assert result["groups_run"] == 0
        emit_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fresh_create_empty_checklist_runs_everything(self):
        """A fresh create (empty/all-pending checklist) skips nothing."""
        ran: List[str] = []

        def make_hook(name: str):
            async def hook(**ctx):
                ran.append(name)
            return hook

        core = _make_provisioner("catalog_core", priority=0, provision_fn=make_hook("catalog_core"))
        bucket = _make_provisioner("gcp_bucket", priority=1, provision_fn=make_hook("gcp_bucket"))
        groups = [[core], [bucket]]

        result, _cat, emit_mock = await _run_with(groups, checklist={})

        assert sorted(ran) == ["catalog_core", "gcp_bucket"]
        assert result["groups_run"] == 2
        emit_mock.assert_awaited_once()
